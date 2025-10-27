import logging
import asyncio
from operator import is_
from typing import AsyncIterable, Iterable, Optional
from functools import partial

# External dependencies used:
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from botocore.exceptions import BotoCoreError, ResponseStreamingError
from zipstream import ZIP_STORED, ZipStream, ZIP_DEFLATED
from aiobotocore.session import get_session
import aiobotocore.client

# --- Imports from original file (assumed to be available) ---
from src.core.celery_app import celery_app
from src.db.db import get_async_session_maker
from src.db.models import DownloadStatusEnum
from src.crud.crud_export import get_export_job, update_export_job_status
from src.download.s3_config import s3_obs, s3_aws, map_category_to_folder
from src.download.s3_config_async import get_async_s3_client_factory
from src.config import settings
from .export_helpers import generate_readme
from src.db.db import get_async_session_maker


logger = logging.getLogger(__name__)
MIN_PART_SIZE = 5 * 1024 * 1024



@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((ResponseStreamingError, BotoCoreError))
)
async def async_get_s3_body_stream_with_retry(async_s3_client: aiobotocore.client.AioBaseClient, bucket_name: str, key: str) -> AsyncIterable[bytes]:
    """
    Asynchronously streams bytes from an S3 object body with retry logic using aiobotocore.
    This is a fully non-blocking operation.
    """
    response = await async_s3_client.get_object(Bucket=bucket_name, Key=key)
    s3_body = response["Body"]
    
    # aiobotocore's StreamingBody is an async context manager
    async with s3_body as stream:
        async for chunk in stream:
            yield chunk

# --- Thread-Async Bridge for Synchronous ZipStream Consumption ---

def sync_s3_stream_wrapper(
    async_s3_client: aiobotocore.client.AioBaseClient, 
    bucket_name: str, 
    key: str, 
    loop: asyncio.AbstractEventLoop,
    is_obs: Optional[bool] = False,
):
    """
    Synchronous generator wrapper to bridge the synchronous ZipStream's read request
    to the asynchronous S3 stream. This must be run in the same thread as ZipStream.
    """
    if is_obs:
        bucket_name = settings.OBS_BUCKET_NAME

    # Create the async generator for the S3 file content
    async_gen = async_get_s3_body_stream_with_retry(
        async_s3_client, bucket_name, key
    )
    
    # Get the async generator's iterator
    aiter_gen = async_gen.__aiter__()
    
    while True:
        future = asyncio.run_coroutine_threadsafe(aiter_gen.__anext__(), loop)
        try:
            chunk = future.result() 
            yield chunk
        except StopAsyncIteration:
            break
        except Exception as e:
            logger.error(f"Error in sync S3 stream wrapper for {key}: {e}")
            raise


async def async_zip_stream_to_s3_multipart(
    zip_stream_generator: AsyncIterable[bytes], 
    async_s3_client: aiobotocore.client.AioBaseClient, 
    bucket: str, 
    key: str, 
    is_obs: Optional[bool] = False, 
    min_part_size: int = MIN_PART_SIZE
):
    """
    Uploads an asynchronous byte stream (from ZipStream) to S3 using multipart upload.
    This is a fully non-blocking implementation using aiobotocore.
    """
    if is_obs:
        bucket = settings.OBS_BUCKET_NAME

    resp = await async_s3_client.create_multipart_upload(Bucket=bucket, Key=key)
    upload_id = resp['UploadId']
    parts = []
    part_number = 1
    buf = bytearray()
    
    try:
        async for chunk in zip_stream_generator:
            buf.extend(chunk)
            
            # Process buffer to yield fixed-size parts for S3 multipart upload
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
                parts.append({'PartNumber': part_number, 'ETag': resp['ETag']})
                part_number += 1

        # Upload the final, smaller part if the buffer is not empty
        if buf:
            part_bytes = bytes(buf)
            resp = await async_s3_client.upload_part(
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
            await async_s3_client.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
        else:
            await async_s3_client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
            raise ValueError(f"No valid parts to upload for S3 key={key}")

    except Exception as e:
        await async_s3_client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
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
    """
    Main async implementation, refactored for non-blocking streaming.
    
    NOTE: This function assumes the S3 client factory is available via a global 
    or a passed-in argument. For this example, we'll assume it's available 
    as `get_async_s3_client_factory` and `settings` is imported.
    """
    logger.info(f"Starting refactored export job {job_id}")
    
    session_maker = fresh_session_maker() if fresh_session_maker else get_async_session_maker()
    
    # 1. Initial status update (non-blocking)
    async with session_maker() as session:
        job = await get_export_job(session, job_id)
        if not job:
            logger.error(f"Job not found: {job_id}")
            return
        
        await update_export_job_status(
            session, job_id, DownloadStatusEnum.PROCESSING, progress_pct=0
        )

    export_filename = f"exports/{language}_{pct}pct_{job_id}_{category}_{domain}_{split}.zip"
    
    try:
        # Initialize the S3 client factory and get the current event loop
        async_s3_client_factory = get_async_s3_client_factory(settings, is_obs=True)
        loop = asyncio.get_event_loop()
        
        # Use aiobotocore for the S3 upload and download operations
        async with async_s3_client_factory() as async_s3_client:
            
            # 2. Setup data stream
            from src.download.service import DownloadService
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
                logger.warning(f"No samples found for filter parameters: {language}, {pct}, {category}")
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

            # 3. Setup ZipStream and metadata
            # zs = ZipStream(compress_type=ZIP_DEFLATED, compress_level=5)
            zs = ZipStream(compress_type=ZIP_STORED)
            processed_count = 0
            # ... (metadata setup - original lines 251-254) ...
            all_metadata_rows = [
                "speaker_id,transcript_id,transcript,audio_path,gender,age_group,education,duration,language,snr,domain\n"
            ]
            
            # 4. Iterate over samples and add to ZipStream
            async for sample in samples_stream:
                # ... (S3 key construction - original lines 256-261) ...
                arcname = f"audio/{sample.sentence_id}.wav"
                folder = map_category_to_folder(sample.language, sample.category)
                key = f"{sample.language.lower()}-test/{folder}/{sample.sentence_id}.wav"
                
                try:
                    # Create the synchronous generator that fetches chunks asynchronously
                    sync_stream_gen = partial(
                        sync_s3_stream_wrapper, 
                        async_s3_client, 
                        settings.OBS_BUCKET_NAME, 
                        is_obs=True, 
                        key=key, 
                        loop=loop
                    )
                    
                    zs.add(sync_stream_gen(), arcname=arcname)
                    
                    # ... (metadata row append - original lines 293-298) ...
                    row = (
                        f'"{sample.speaker_id}","{sample.sentence_id}","{sample.sentence or ""}","{arcname}",'
                        f'"{sample.gender}","{sample.age_group}","{sample.edu_level}","{sample.duration}",'
                        f'"{sample.language}","{sample.snr}","{sample.domain}"\n'
                    )
                    all_metadata_rows.append(row)
                    
                    processed_count += 1
                    
                    # 5. Progress update (non-blocking)
                    if processed_count % 3 == 0:
                        progress = int((processed_count / total_to_process) * 95)
                        async with session_maker() as progress_session:
                            await update_export_job_status(
                                progress_session, job_id, 
                                DownloadStatusEnum.PROCESSING,
                                progress_pct=progress
                            )
                except Exception as e:
                    logger.warning(f"Failed to stream and add {key}: {e}. Skipping file.")
                    continue
            
            # 6. Finalize zip with metadata and README
            metadata_content = "".join(all_metadata_rows).encode('utf-8')
            zs.add(iter([metadata_content]), arcname="metadata.csv")
            

            last_sentence_id = sample.sentence_id if 'sample' in locals() else "N/A"
            readme_content = generate_readme(language, pct, False, processed_count, last_sentence_id)
            zs.add(iter([readme_content.encode("utf-8")]), arcname="README.txt")
            

            # Run the synchronous ZipStream generator in a thread
            zip_stream_output = await asyncio.to_thread(iter, zs)
            
            # Create an async generator from the synchronous generator output
            async def async_zip_output_generator(sync_gen: Iterable[bytes]) -> AsyncIterable[bytes]:
                for chunk in sync_gen:
                    yield chunk
            
            # Upload the async stream to S3 using aiobotocore
            await async_zip_stream_to_s3_multipart(
                async_zip_output_generator(zip_stream_output), 
                async_s3_client, 
                settings.S3_BUCKET_NAME, 
                export_filename,
            )
            
            # 8. Final status update and return
            download_url = await asyncio.to_thread(
                s3_aws.generate_presigned_url,
                'get_object',
                Params={'Bucket': settings.S3_BUCKET_NAME, 'Key': export_filename},
                ExpiresIn=604800 # 7 days
            )
            
            async with session_maker() as session:
                await update_export_job_status(
                    session, job_id, DownloadStatusEnum.READY,
                    progress_pct=100, download_url=download_url
                )
            
            return {
                'job_id': job_id, 
                'download_url': download_url, 
                'total_samples': processed_count
            }
            
    except Exception as e:
        logger.exception(f"Job {job_id} failed during streaming: {e}")
        async with session_maker() as session:
            await update_export_job_status(
                session, job_id, DownloadStatusEnum.FAILED,
                error_message=str(e),
                progress_pct=0
            )
        raise e



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