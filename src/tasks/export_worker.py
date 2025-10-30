
import logging
from math import log
from zipstream.ng import ZipStream, ZIP_DEFLATED
import asyncio
from typing import Iterable
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from botocore.exceptions import BotoCoreError
from botocore.exceptions import ResponseStreamingError

from src.core.celery_app import celery_app
from src.db.db import get_async_session_maker
from src.db.models import DownloadStatusEnum
from src.crud.crud_export import get_export_job, update_export_job_status
from src.download.s3_config import  s3_obs, s3_aws, map_category_to_folder
from src.config import settings





logger = logging.getLogger(__name__)
MIN_PART_SIZE = 5 * 1024 * 1024

SAMPLE_RATE = 48000
CHANNELS = 1
BYTES_PER_SAMPLE = 2


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((ResponseStreamingError, BotoCoreError))
)
def get_s3_stream_for_file_with_retry(bucket_name, key):
    """
    Attempts to download the S3 object. If streaming fails, the entire
    operation (including the initial API call) will be retried up to 5 times.
    """
    obj = s3_obs.get_object(Bucket=bucket_name, Key=key)
    s3_body = obj["Body"]
    return s3_stream_bytes(s3_body)


def download_s3_bytes(bucket, key):
    obj = s3_obs.get_object(Bucket=bucket, Key=key)
    return obj['Body'].read()



def s3_stream_bytes(s3_body, chunk_size=64 * 1024) -> Iterable[bytes]:
    """Generator to yield bytes from S3 StreamingBody."""
    while True:
        chunk = s3_body.read(chunk_size)
        if not chunk:
            break
        yield chunk

def buffered_zip_chunks(zip_gen, min_size=5*1024*1024):
    """Yield fixed-size byte chunks from a zipstream generator for S3 multipart upload."""
    buf = bytearray()
    for chunk in zip_gen:
        buf.extend(chunk)
        while len(buf) >= min_size:
            yield bytes(buf[:min_size])
            buf = buf[min_size:]
    if buf:
        yield bytes(buf)

def zip_bytes_generator(zs, min_size=5*1024*1024):
    """Yield contiguous byte chunks suitable for S3 multipart."""
    buf = bytearray()
    for chunk in zs:
        buf.extend(chunk)
        while len(buf) >= min_size:
            yield bytes(buf[:min_size])
            buf = buf[min_size:]
    if buf:
        yield bytes(buf)



def stream_zip_to_s3_blocking(zip_gen, bucket: str, key: str):
    """Upload a zip generator to S3 safely, skipping empty chunks."""
    resp = s3_aws.create_multipart_upload(Bucket=bucket, Key=key)
    upload_id = resp['UploadId']
    parts = []
    part_number = 1

    try:
        for part_bytes in zip_bytes_generator(zip_gen, MIN_PART_SIZE):
            if not part_bytes:  # Skip empty chunks
                continue

            resp = s3_aws.upload_part(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=part_bytes,
                ContentLength=len(part_bytes)
            )
            parts.append({'PartNumber': part_number, 'ETag': resp['ETag']})
            part_number += 1

        # Only complete upload if at least one part was uploaded
        if parts:
            s3_aws.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
        else:
            # No parts uploaded, abort the upload
            s3_aws.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
            raise ValueError(f"No valid parts to upload for S3 key={key}")

    except Exception as e:
        s3_aws.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        raise



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
    """
    Synchronous wrapper that runs async logic in an isolated event loop.
    """
    import asyncio
    from src.tasks.export_worker import get_async_session_maker, async_create_dataset_zip_s3_impl

    # Always re-create engine + session maker inside each run
    def fresh_session_maker():
        return get_async_session_maker(force_new=True)

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
    """Main async implementation."""
    logger.info(f"Starting export job {job_id}")
    
    session_maker = fresh_session_maker() if fresh_session_maker else get_async_session_maker()

    language = language
    pct = pct

    async with session_maker() as session:
        job = await get_export_job(session, job_id)
        if not job:
            logger.error(f"Job not found: {job_id}")
            # task.update_state(state='FAILURE', meta={'error': 'Job not found'})
            return
        
        await update_export_job_status(
            session, job_id, DownloadStatusEnum.PROCESSING, progress_pct=0
        )

    export_filename = f"exports/{language}_{pct}pct_{job_id}.zip"
    
    try:
        from src.download.service import DownloadService
        download_service = DownloadService(s3_bucket_name=settings.OBS_BUCKET_NAME)
        
        logger.warning(f"\nThe filter paramaters are {language}, {pct}, {category}, {gender}, {age_group}, {education}, {split}, {domain}")
        
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
                # No samples found → mark as failed gracefully
                logger.warning(f"No audio samples found for job {job_id}.")
                async with session_maker() as session:
                    await update_export_job_status(
                        session, job_id, DownloadStatusEnum.FAILED,
                        error_message="No audio samples found for the selected criteria.",
                        progress_pct=0
                    )

                return {
                    'job_id': job_id, 
                    'download_url': None, 
                    'total_samples': 0
                }
            
            zs = ZipStream(compress_type=ZIP_DEFLATED, compress_level=9)
            processed_count = 0
            last_sentence_id = "N/A"
            all_metadata_rows = [
                "speaker_id,transcript_id,transcript,audio_path,gender,age_group,education,duration,language,snr,domain\n"
            ]

            async for sample in samples_stream:
                last_sentence_id = sample.sentence_id
                arcname = f"audio/{sample.sentence_id}.wav"

                folder = map_category_to_folder(sample.language, sample.category)
                key = f"{sample.language.lower()}-test/{folder}/{sample.sentence_id}.wav"
                
                try:
                    file_bytes = await asyncio.to_thread(download_s3_bytes, settings.OBS_BUCKET_NAME, key)
                    zs.add(iter([file_bytes]), arcname=arcname)
                except Exception as e:
                    logger.warning(f"Failed to download {key}: {e}")
                    continue
                
                row = (
                    f'"{sample.speaker_id}","{sample.sentence_id}","{sample.sentence or ""}","{arcname}",'
                    f'"{sample.gender}","{sample.age_group}","{sample.edu_level}","{sample.duration}",'
                    f'"{sample.language}","{sample.snr}","{sample.domain}"\n'
                )
                all_metadata_rows.append(row)
                
                processed_count += 1
                
                # Update progress every 10 samples
                if processed_count % 3 == 0:
                    progress = int((processed_count / total_to_process) * 95)
            
                    
                    async with session_maker() as progress_session:
                        await update_export_job_status(
                            progress_session, job_id, 
                            DownloadStatusEnum.PROCESSING,
                            progress_pct=progress
                        )

            # Finalize zip
            metadata_content = "".join(all_metadata_rows).encode('utf-8')
            zs.add(iter([metadata_content]), arcname="metadata.csv")
            
            
            from .export_helpers import generate_readme
            readme_content = generate_readme(language, pct, False, processed_count, last_sentence_id)
            zs.add(iter([readme_content.encode("utf-8")]), arcname="README.txt")
            
            # stream_zip_to_s3_blocking(zs, bucket=settings.S3_BUCKET_NAME, key=export_filename)
            await asyncio.to_thread(
                stream_zip_to_s3_blocking, 
                zs, 
                bucket=settings.S3_BUCKET_NAME, 
                key=export_filename
            )

        # Generate presigned URL
        download_url = await asyncio.to_thread(
            s3_aws.generate_presigned_url,
            'get_object',
            Params={'Bucket': settings.S3_BUCKET_NAME, 'Key': export_filename},
            ExpiresIn=86400
        )
        
        async with session_maker() as session:
            await update_export_job_status(
                session, job_id, DownloadStatusEnum.READY, 
                download_url=download_url, progress_pct=100
            )
        logger.info(f"\n\nGREAT!.==========\n\nExport Job {job_id} completed: {download_url}\n\n")
        return {
            'job_id': job_id, 
            'download_url': download_url, 
            'total_samples': processed_count
        }

    except Exception as e:
        logger.exception(f"Job {job_id} failed: {e}")
        async with session_maker() as session:
            await update_export_job_status(
                session, job_id, DownloadStatusEnum.FAILED, 
                error_message=str(e), progress_pct=0
            )
        raise
