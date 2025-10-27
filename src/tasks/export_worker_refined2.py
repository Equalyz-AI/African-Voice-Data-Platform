import asyncio
from functools import partial
from typing import AsyncIterable
import logging
from zipstream import ZipStream, ZIP_DEFLATED
import aiobotocore.client

from src.core.celery_app import celery_app
from src.config import settings
from src.download.s3_config_async import get_async_s3_client_factory
from src.crud.crud_export import get_export_job, update_export_job_status
from src.db.db import get_async_session_maker
from src.db.models import DownloadStatusEnum
from src.download.s3_config import map_category_to_folder
from .export_helpers import generate_readme
from src.download.service import DownloadService

MIN_PART_SIZE = 5 * 1024 * 1024  # 5 MB
CONCURRENT_FILES = 10             # Number of files streamed concurrently
logger = logging.getLogger(__name__)


async def async_get_s3_body_stream_with_retry(
    async_s3_client: aiobotocore.client.AioBaseClient,
    bucket_name: str,
    key: str
) -> AsyncIterable[bytes]:
    """Asynchronously stream S3 object with retry logic."""
    response = await async_s3_client.get_object(Bucket=bucket_name, Key=key)
    s3_body = response["Body"]
    async with s3_body as stream:
        async for chunk in stream:
            yield chunk


async def async_zip_stream_to_s3_multipart(
    zs: ZipStream,
    async_s3_client: aiobotocore.client.AioBaseClient,
    bucket: str,
    key: str,
    min_part_size: int = MIN_PART_SIZE
):
    """Stream ZipStream to S3 with zero in-memory accumulation."""
    resp = await async_s3_client.create_multipart_upload(Bucket=bucket, Key=key)
    upload_id = resp["UploadId"]
    parts = []
    part_number = 1
    buf = bytearray()

    try:
        async for chunk in zs:
            buf.extend(chunk)
            while len(buf) >= min_part_size:
                part_bytes = bytes(buf[:min_part_size])
                buf = buf[min_part_size:]
                resp = await async_s3_client.upload_part(
                    Bucket=bucket,
                    Key=key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=part_bytes,
                    ContentLength=len(part_bytes)
                )
                parts.append({"PartNumber": part_number, "ETag": resp["ETag"]})
                part_number += 1

        if buf:
            resp = await async_s3_client.upload_part(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=bytes(buf),
                ContentLength=len(buf)
            )
            parts.append({"PartNumber": part_number, "ETag": resp["ETag"]})

        await async_s3_client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts}
        )
    except Exception as e:
        await async_s3_client.abort_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id
        )
        raise e


async def async_create_dataset_zip_s3_impl(
    task,
    job_id: str,
    language: str,
    pct: float | None = None,
    category: str | None = None,
    gender: str | None = None,
    age_group: str | None = None,
    education: str | None = None,
    split: str | None = None,
    domain: str | None = None,
    fresh_session_maker=None
):
    """Fully streaming, zero in-memory, async dataset export."""
    session_maker = fresh_session_maker() if fresh_session_maker else get_async_session_maker()

    # 1. Initial status update
    async with session_maker() as session:
        job = await get_export_job(session, job_id)
        if not job:
            raise ValueError(f"Job not found: {job_id}")

        await update_export_job_status(session, job_id, DownloadStatusEnum.PROCESSING, progress_pct=0)

    export_filename = f"exports/{language}_{pct}pct_{job_id}.zip"

    async_s3_client_factory = get_async_s3_client_factory(settings, is_obs=True)
    loop = asyncio.get_event_loop()

    async with async_s3_client_factory() as async_s3_client:
        # 2. Stream samples
        download_service = DownloadService(s3_bucket_name=settings.OBS_BUCKET_NAME)
        async with session_maker() as session:
            samples_stream, total_to_process = await download_service.filter_core_stream(
                session=session,
                language=language,
                pct=pct,
                category=category,
                gender=gender,
                age_group=age_group,
                education=education,
                split=split,
                domain=domain
            )

        if total_to_process == 0:
            async with session_maker() as session:
                await update_export_job_status(
                    session, job_id, DownloadStatusEnum.FAILED,
                    error_message="No audio samples found for the selected criteria.",
                    progress_pct=0
                )
            return {"job_id": job_id, "download_url": None, "total_samples": 0}

        # 3. Setup ZipStream
        zs = ZipStream(compress_type=ZIP_DEFLATED, compress_level=5)
        all_metadata_rows = ["speaker_id,transcript_id,transcript,audio_path,gender,age_group,education,duration,language,snr,domain\n"]
        processed_count = 0
        semaphore = asyncio.Semaphore(CONCURRENT_FILES)

        async def add_sample_to_zip(sample):
            nonlocal processed_count
            key = f"{sample.language.lower()}-test/{map_category_to_folder(sample.language, sample.category)}/{sample.sentence_id}.wav"
            arcname = f"audio/{sample.sentence_id}.wav"

            async def file_stream():
                async for chunk in async_get_s3_body_stream_with_retry(async_s3_client, settings.OBS_BUCKET_NAME, key):
                    yield chunk

            async with semaphore:
                zs.add(file_stream(), arcname=arcname)

                # Add metadata row
                row = (
                    f'"{sample.speaker_id}","{sample.sentence_id}","{sample.sentence or ""}","{arcname}",'
                    f'"{sample.gender}","{sample.age_group}","{sample.edu_level}","{sample.duration}",'
                    f'"{sample.language}","{sample.snr}","{sample.domain}"\n'
                )
                all_metadata_rows.append(row)

                processed_count += 1
                if processed_count % 10 == 0:
                    progress = int((processed_count / total_to_process) * 95)
                    async with session_maker() as session:
                        await update_export_job_status(session, job_id, DownloadStatusEnum.PROCESSING, progress_pct=progress)

        # Gather all tasks concurrently
        tasks = [add_sample_to_zip(sample) async for sample in samples_stream]
        await asyncio.gather(*tasks)

        # 4. Add metadata.csv and README.txt
        zs.add(iter(["".join(all_metadata_rows).encode("utf-8")]), arcname="metadata.csv")
        readme_content = generate_readme(language, pct, False, processed_count, samples_stream[-1].sentence_id if processed_count else "N/A")
        zs.add(iter([readme_content.encode("utf-8")]), arcname="README.txt")

        # 5. Upload to S3 (fully streaming)
        await async_zip_stream_to_s3_multipart(zs, async_s3_client, settings.S3_BUCKET_NAME, export_filename)

        # 6. Generate presigned URL
        download_url = await asyncio.to_thread(
            settings.s3_aws.generate_presigned_url,
            "get_object",
            Params={"Bucket": settings.S3_BUCKET_NAME, "Key": export_filename},
            ExpiresIn=604800
        )

        async with session_maker() as session:
            await update_export_job_status(session, job_id, DownloadStatusEnum.READY, progress_pct=100, download_url=download_url)

        return {
          "job_id": job_id, 
          "download_url": download_url, 
          "total_samples": processed_count
        }





@celery_app.task(bind=True, name="exports.create_dataset_zip_s3_task_new", acks_late=True)
def create_dataset_zip_s3_task_new(
  self, 
  job_id: str,
  language: str,
  pct: float | None = None,
  category: str | None = None,
  gender: str | None = None,
  age_group: str | None = None,
  education: str | None = None,
  split: str | None = None,
  domain: str | None = None
):

    def fresh_session_maker():
      return get_async_session_maker(force_new=True)
    # ... (original logic) ...
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(
            async_create_dataset_zip_s3_impl(
              self, job_id, language, pct, category,
              gender, age_group, education, split, domain,
              fresh_session_maker=fresh_session_maker
            )
        )
    except Exception as e:
        logger.exception(f"Job {job_id} failed: {e}")
        raise e
    finally:
        loop.close()