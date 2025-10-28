import logging
import asyncio
import os
import aiofiles
import zipfile
import tempfile
import shutil
from typing import AsyncIterable, Iterable, Optional
from functools import partial

# External dependencies
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from botocore.exceptions import BotoCoreError, ResponseStreamingError
# We are NO LONGER using zipstream
# from zipstream import ZIP_STORED, ZipStream, ZIP_DEFLATED
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

# --- NEW: Concurrency and Upload Settings ---
# We no longer need MIN_PART_SIZE for streaming the zip
# We define how many files to download from S3 at once.
# Adjust this based on your server's network/CPU. 20-50 is a good range.
CONCURRENT_DOWNLOADS = 20


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((ResponseStreamingError, BotoCoreError))
)
async def async_get_s3_body_stream_with_retry(async_s3_client: aiobotocore.client.AioBaseClient, bucket_name: str, key: str) -> AsyncIterable[bytes]:
    """
    Asynchronously streams bytes from an S3 object body with retry logic using aiobotocore.
    (This function is unchanged, it's already correct).
    """
    response = await async_s3_client.get_object(Bucket=bucket_name, Key=key)
    s3_body = response["Body"]
    
    async with s3_body as stream:
        async for chunk in stream:
            yield chunk

# --- DELETED: sync_s3_stream_wrapper (No longer needed) ---
# --- DELETED: async_zip_stream_to_s3_multipart (No longer needed) ---

# --- NEW: Helper function to download one file to disk ---
async def download_sample_to_temp_file(
    sample, 
    async_s3_client: aiobotocore.client.AioBaseClient, 
    temp_dir_path: str, 
    semaphore: asyncio.Semaphore
) -> Optional[tuple[str, str, str]]:
    """
    Downloads a single S3 file to a temporary directory using a semaphore.
    Returns (local_file_path, zip_archive_name, metadata_row)
    """
    arcname = f"audio/{sample.sentence_id}.wav"
    folder = map_category_to_folder(sample.language, sample.category)
    key = f"{sample.language.lower()}-test/{folder}/{sample.sentence_id}.wav"
    local_file_path = os.path.join(temp_dir_path, f"{sample.sentence_id}.wav")

    async with semaphore:  # Acquire the semaphore
        try:
            # Stream from S3 and write to local temp file
            async with aiofiles.open(local_file_path, 'wb') as f:
                async_gen = async_get_s3_body_stream_with_retry(
                    async_s3_client, settings.OBS_BUCKET_NAME, key
                )
                async for chunk in async_gen:
                    await f.write(chunk)
            
            # Prepare metadata row
            row = (
                f'"{sample.speaker_id}","{sample.sentence_id}","{sample.sentence or ""}","{arcname}",'
                f'"{sample.gender}","{sample.age_group}","{sample.edu_level}","{sample.duration}",'
                f'"{sample.language}","{sample.snr}","{sample.domain}"\n'
            )
            
            return (local_file_path, arcname, row)
            
        except Exception as e:
            logger.warning(f"Failed to download and save {key}: {e}. Skipping file.")
            return None


# --- NEW: Helper function to create the zip file on disk ---
def create_zip_file_from_temp(
    zip_path: str,
    files_to_add: list[tuple[str, str]],
    metadata_path: str,
    readme_path: str
):
    """
    Synchronous function (run in a thread) to create a zip file.
    """
    logger.info(f"Creating zip file at {zip_path}...")
    # Use ZIP_STORED for high speed (no compression)
    with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_STORED) as zf:
        # Add metadata.csv
        zf.write(metadata_path, arcname="metadata.csv")
        # Add README.txt
        zf.write(readme_path, arcname="README.txt")
        
        # Add all the audio files
        for local_path, arcname in files_to_add:
            zf.write(local_path, arcname=arcname)
    logger.info(f"Finished creating zip file.")


# --- NEW: Helper function to upload the final zip file ---
async def upload_final_zip_to_s3(
    async_s3_client: aiobotocore.client.AioBaseClient,
    local_zip_path: str,
    bucket: str,
    key: str
):
    """
    Uploads a single file from disk to S3 using aiobotocore's managed uploader.
    """
    logger.info(f"Uploading {local_zip_path} to S3 bucket {bucket} as {key}...")
    await async_s3_client.upload_file(local_zip_path, bucket, key)
    logger.info("Upload complete.")


# --- REWRITTEN: Main Implementation ---
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
    logger.info(f"Starting REFACTORED export job {job_id}")
    
    session_maker = fresh_session_maker() if fresh_session_maker else get_async_session_maker()
    
    # 1. Initial status update
    async with session_maker() as session:
        job = await get_export_job(session, job_id)
        if not job:
            logger.error(f"Job not found: {job_id}")
            return
        await update_export_job_status(
            session, job_id, DownloadStatusEnum.PROCESSING, progress_pct=0
        )

    export_filename = f"exports/{language}_{pct}pct_{job_id}_{category}_{domain}_{split}.zip"
    
    # Create a temporary directory to store downloaded files
    temp_dir_path = tempfile.mkdtemp()
    logger.info(f"Created temporary directory: {temp_dir_path}")

    files_for_zip: list[tuple[str, str]] = [] # (local_path, arcname)
    processed_count = 0
    
    try:
        async_s3_client_factory = get_async_s3_client_factory(settings, is_obs=True)
        
        async with async_s3_client_factory() as async_s3_client:
            
            # 2. Setup data stream
            from src.download.service import DownloadService
            download_service = DownloadService(s3_bucket_name=settings.OBS_BUCKET_NAME)
            
            async with session_maker() as session:
                samples_stream, total_to_process = await download_service.filter_core_stream(
                    session=session,
                    language=language, pct=pct, category=category, gender=gender,
                    age_group=age_group, education=education, split=split, domain=domain
                )

            if total_to_process == 0:
                logger.warning(f"No samples found for filter parameters.")
                # ... (error handling code remains the same) ...
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

            # 3. --- STAGE 1: Concurrent Download ---
            logger.info(f"Starting concurrent download of {total_to_process} files...")
            
            semaphore = asyncio.Semaphore(CONCURRENT_DOWNLOADS)
            tasks = []
            
            # Prepare metadata file path
            metadata_temp_path = os.path.join(temp_dir_path, "metadata.csv")
            
            async with aiofiles.open(metadata_temp_path, 'w', encoding='utf-8') as f_meta:
                # Write header
                await f_meta.write("speaker_id,transcript_id,transcript,audio_path,gender,age_group,education,duration,language,snr,domain\n")

                async for sample in samples_stream:
                    # Create a task for each file download
                    tasks.append(
                        download_sample_to_temp_file(
                            sample, async_s3_client, temp_dir_path, semaphore
                        )
                    )

                    # Update progress periodically
                    if len(tasks) % 100 == 0 or len(tasks) == total_to_process:
                        progress = int((len(tasks) / total_to_process) * 75) # Download is 75% of the job
                        async with session_maker() as progress_session:
                            await update_export_job_status(
                                progress_session, job_id, 
                                DownloadStatusEnum.PROCESSING,
                                progress_pct=progress
                            )
                
                # Run all download tasks concurrently
                results = await asyncio.gather(*tasks)

                # Process results: write metadata and collect file paths
                for result in results:
                    if result:
                        local_path, arcname, metadata_row = result
                        files_for_zip.append((local_path, arcname))
                        await f_meta.write(metadata_row)
                        processed_count += 1
            
            logger.info(f"Finished concurrent download. {processed_count} files processed.")

            # 4. --- STAGE 2: Create local README ---
            readme_temp_path = os.path.join(temp_dir_path, "README.txt")
            last_sentence_id = "N/A"
            if 'sample' in locals():
                last_sentence_id = sample.sentence_id
                
            readme_content = generate_readme(language, pct, False, processed_count, last_sentence_id)
            async with aiofiles.open(readme_temp_path, 'w', encoding='utf-8') as f_readme:
                await f_readme.write(readme_content)

            # 5. --- STAGE 3: Create Zip File (in a thread) ---
            logger.info("Moving to zip creation stage...")
            final_zip_path = os.path.join(temp_dir_path, f"{job_id}.zip")
            
            await asyncio.to_thread(
                create_zip_file_from_temp,
                final_zip_path,
                files_for_zip,
                metadata_temp_path,
                readme_temp_path
            )
            
            async with session_maker() as progress_session:
                await update_export_job_status(
                    progress_session, job_id, 
                    DownloadStatusEnum.PROCESSING,
                    progress_pct=90 # Zipping complete
                )

            # 6. --- STAGE 4: Upload Final Zip to S3 ---
            # We need a *new* S3 client for the *destination* bucket
            async_s3_dest_factory = get_async_s3_client_factory(settings, is_obs=False) # Assuming is_obs=False is destination
            
            async with async_s3_dest_factory() as async_s3_dest_client:
                await upload_final_zip_to_s3(
                    async_s3_dest_client,
                    final_zip_path,
                    settings.S3_BUCKET_NAME,
                    export_filename
                )

            # 7. Final status update and return
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
    finally:
        # 8. --- FINAL STAGE: Clean up temporary directory ---
        if os.path.exists(temp_dir_path):
            logger.info(f"Cleaning up temporary directory: {temp_dir_path}")
            await asyncio.to_thread(shutil.rmtree, temp_dir_path)





# --- Celery Task (Unchanged) ---
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








# import logging
# import asyncio
# from operator import is_
# from typing import AsyncIterable, Iterable, Optional
# from functools import partial

# # External dependencies used:
# from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
# from botocore.exceptions import BotoCoreError, ResponseStreamingError
# from zipstream import ZIP_STORED, ZipStream, ZIP_DEFLATED
# from aiobotocore.session import get_session
# import aiobotocore.client

# # --- Imports from original file (assumed to be available) ---
# from src.core.celery_app import celery_app
# from src.db.db import get_async_session_maker
# from src.db.models import DownloadStatusEnum
# from src.crud.crud_export import get_export_job, update_export_job_status
# from src.download.s3_config import s3_obs, s3_aws, map_category_to_folder
# from src.download.s3_config_async import get_async_s3_client_factory
# from src.config import settings
# from .export_helpers import generate_readme
# from src.db.db import get_async_session_maker


# logger = logging.getLogger(__name__)
# MIN_PART_SIZE = 25 * 1024 * 1024



# @retry(
#     stop=stop_after_attempt(5),
#     wait=wait_exponential(multiplier=1, min=2, max=30),
#     retry=retry_if_exception_type((ResponseStreamingError, BotoCoreError))
# )
# async def async_get_s3_body_stream_with_retry(async_s3_client: aiobotocore.client.AioBaseClient, bucket_name: str, key: str) -> AsyncIterable[bytes]:
#     """
#     Asynchronously streams bytes from an S3 object body with retry logic using aiobotocore.
#     This is a fully non-blocking operation.
#     """
#     response = await async_s3_client.get_object(Bucket=bucket_name, Key=key)
#     s3_body = response["Body"]
    
#     # aiobotocore's StreamingBody is an async context manager
#     async with s3_body as stream:
#         async for chunk in stream:
#             yield chunk

# # --- Thread-Async Bridge for Synchronous ZipStream Consumption ---

# def sync_s3_stream_wrapper(
#     async_s3_client: aiobotocore.client.AioBaseClient, 
#     bucket_name: str, 
#     key: str, 
#     loop: asyncio.AbstractEventLoop,
#     is_obs: Optional[bool] = False,
# ):
#     """
#     Synchronous generator wrapper to bridge the synchronous ZipStream's read request
#     to the asynchronous S3 stream. This must be run in the same thread as ZipStream.
#     """
#     if is_obs:
#         bucket_name = settings.OBS_BUCKET_NAME

#     # Create the async generator for the S3 file content
#     async_gen = async_get_s3_body_stream_with_retry(
#         async_s3_client, bucket_name, key
#     )
    
#     # Get the async generator's iterator
#     aiter_gen = async_gen.__aiter__()
    
#     while True:
#         future = asyncio.run_coroutine_threadsafe(aiter_gen.__anext__(), loop)
#         try:
#             chunk = future.result() 
#             yield chunk
#         except StopAsyncIteration:
#             break
#         except Exception as e:
#             logger.error(f"Error in sync S3 stream wrapper for {key}: {e}")
#             raise


# async def async_zip_stream_to_s3_multipart(
#     zip_stream_generator: AsyncIterable[bytes], 
#     async_s3_client: aiobotocore.client.AioBaseClient, 
#     bucket: str, 
#     key: str, 
#     is_obs: Optional[bool] = False, 
#     min_part_size: int = MIN_PART_SIZE
# ):
#     """
#     Uploads an asynchronous byte stream (from ZipStream) to S3 using multipart upload.
#     This is a fully non-blocking implementation using aiobotocore.
#     """
#     if is_obs:
#         bucket = settings.OBS_BUCKET_NAME

#     resp = await async_s3_client.create_multipart_upload(Bucket=bucket, Key=key)
#     upload_id = resp['UploadId']
#     parts = []
#     part_number = 1
#     buf = bytearray()
    
#     try:
#         async for chunk in zip_stream_generator:
#             buf.extend(chunk)
            
#             # Process buffer to yield fixed-size parts for S3 multipart upload
#             while len(buf) >= min_part_size:
#                 part_bytes = bytes(buf[:min_part_size])
#                 buf = buf[min_part_size:]
                
#                 resp = await async_s3_client.upload_part(
#                     Bucket=bucket,
#                     Key=key,
#                     UploadId=upload_id,
#                     PartNumber=part_number,
#                     Body=part_bytes,
#                     ContentLength=len(part_bytes)
#                 )
#                 parts.append({'PartNumber': part_number, 'ETag': resp['ETag']})
#                 part_number += 1

#         # Upload the final, smaller part if the buffer is not empty
#         if buf:
#             part_bytes = bytes(buf)
#             resp = await async_s3_client.upload_part(
#                 Bucket=bucket,
#                 Key=key,
#                 UploadId=upload_id,
#                 PartNumber=part_number,
#                 Body=part_bytes,
#                 ContentLength=len(part_bytes)
#             )
#             parts.append({'PartNumber': part_number, 'ETag': resp['ETag']})
#             part_number += 1

#         # Only complete upload if at least one part was uploaded
#         if parts:
#             await async_s3_client.complete_multipart_upload(
#                 Bucket=bucket,
#                 Key=key,
#                 UploadId=upload_id,
#                 MultipartUpload={'Parts': parts}
#             )
#         else:
#             await async_s3_client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
#             raise ValueError(f"No valid parts to upload for S3 key={key}")

#     except Exception as e:
#         await async_s3_client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
#         raise e


# async def async_create_dataset_zip_s3_impl(
#     task, 
#     job_id: str, 
#     language: str,
#     pct: float | None = None,
#     category: str | None = None,
#     gender: str | None = None,
#     age_group: str | None = None,
#     education: str | None = None,
#     split: str | None = None,
#     domain: str | None = None,
#     fresh_session_maker=None
#     ):
#     """
#     Main async implementation, refactored for non-blocking streaming.
    
#     NOTE: This function assumes the S3 client factory is available via a global 
#     or a passed-in argument. For this example, we'll assume it's available 
#     as `get_async_s3_client_factory` and `settings` is imported.
#     """
#     logger.info(f"Starting refactored export job {job_id}")
    
#     session_maker = fresh_session_maker() if fresh_session_maker else get_async_session_maker()
    
#     # 1. Initial status update (non-blocking)
#     async with session_maker() as session:
#         job = await get_export_job(session, job_id)
#         if not job:
#             logger.error(f"Job not found: {job_id}")
#             return
        
#         await update_export_job_status(
#             session, job_id, DownloadStatusEnum.PROCESSING, progress_pct=0
#         )

#     export_filename = f"exports/{language}_{pct}pct_{job_id}_{category}_{domain}_{split}.zip"
    
#     try:
#         # Initialize the S3 client factory and get the current event loop
#         async_s3_client_factory = get_async_s3_client_factory(settings, is_obs=True)
#         loop = asyncio.get_event_loop()
        
#         # Use aiobotocore for the S3 upload and download operations
#         async with async_s3_client_factory() as async_s3_client:
            
#             # 2. Setup data stream
#             from src.download.service import DownloadService
#             download_service = DownloadService(s3_bucket_name=settings.OBS_BUCKET_NAME)
            
#             async with session_maker() as session:
#                 samples_stream, total_to_process = await download_service.filter_core_stream(
#                     session=session,
#                     language=language,
#                     pct=pct,
#                     category=category,
#                     gender=gender,
#                     age_group=age_group,
#                     education=education,
#                     split=split,
#                     domain=domain
#                 )

#             if total_to_process == 0:
#                 logger.warning(f"No samples found for filter parameters: {language}, {pct}, {category}")
#                 async with session_maker() as session:
#                     await update_export_job_status(
#                         session, job_id, DownloadStatusEnum.FAILED,
#                         error_message="No audio samples found for the selected criteria.",
#                         progress_pct=0
#                     )
#                 return {
#                     'job_id': job_id, 
#                     'download_url': None, 
#                     'total_samples': 0
#                 }

#             # 3. Setup ZipStream and metadata
#             # zs = ZipStream(compress_type=ZIP_DEFLATED, compress_level=5)
#             zs = ZipStream(compress_type=ZIP_STORED)
#             processed_count = 0
#             # ... (metadata setup - original lines 251-254) ...
#             all_metadata_rows = [
#                 "speaker_id,transcript_id,transcript,audio_path,gender,age_group,education,duration,language,snr,domain\n"
#             ]
            
#             # 4. Iterate over samples and add to ZipStream
#             async for sample in samples_stream:

#                 arcname = f"audio/{sample.sentence_id}.wav"
#                 folder = map_category_to_folder(sample.language, sample.category)
#                 key = f"{sample.language.lower()}-test/{folder}/{sample.sentence_id}.wav"
                
#                 try:

#                     # NEW IMPLEMENTATION: Use aiobotocore to stream the S3 file content directly into ZipStream
#                     async_s3_getter = partial(
#                         async_get_s3_body_stream_with_retry, 
#                         async_s3_client, 
#                         settings.OBS_BUCKET_NAME, 
#                         key
#                     )

#                     def thread_bridge_s3_getter(async_s3_getter_partial):
#                         """Runs the async S3 fetcher in a separate thread and yields chunks."""
#                         # This blocks the calling thread (ZipStream's thread), but runs the async
#                         # S3 call efficiently in the background thread managed by asyncio.
#                         for chunk in loop.run_until_complete(
#                             asyncio.to_thread(async_s3_getter_partial)
#                         ):
#                             yield chunk

#                     # 3. Add the synchronous generator (thread_bridge_s3_getter) to ZipStream
#                     # NOTE: zs.add expects an iterable. Pass the generator object (callable)
#                     # or an iterable created by calling it.
#                     zs.add(
#                         thread_bridge_s3_getter(async_s3_getter), 
#                         arcname=arcname
#                     )


#                     # Create the synchronous generator that fetches chunks asynchronously
#                     # sync_stream_gen = partial(
#                     #     sync_s3_stream_wrapper, 
#                     #     async_s3_client, 
#                     #     settings.OBS_BUCKET_NAME, 
#                     #     is_obs=True, 
#                     #     key=key, 
#                     #     loop=loop
#                     # )

#                     # zs.add(sync_stream_gen(), arcname=arcname)
                    
#                     # ... (metadata row append - original lines 293-298) ...
#                     row = (
#                         f'"{sample.speaker_id}","{sample.sentence_id}","{sample.sentence or ""}","{arcname}",'
#                         f'"{sample.gender}","{sample.age_group}","{sample.edu_level}","{sample.duration}",'
#                         f'"{sample.language}","{sample.snr}","{sample.domain}"\n'
#                     )
#                     all_metadata_rows.append(row)
                    
#                     processed_count += 1
                    
#                     # 5. Progress update (non-blocking)
#                     if processed_count % 3 == 0:
#                         progress = int((processed_count / total_to_process) * 95)
#                         async with session_maker() as progress_session:
#                             await update_export_job_status(
#                                 progress_session, job_id, 
#                                 DownloadStatusEnum.PROCESSING,
#                                 progress_pct=progress
#                             )
#                 except Exception as e:
#                     logger.warning(f"Failed to stream and add {key}: {e}. Skipping file.")
#                     continue
            
#             # 6. Finalize zip with metadata and README
#             metadata_content = "".join(all_metadata_rows).encode('utf-8')
#             zs.add(iter([metadata_content]), arcname="metadata.csv")
            

#             last_sentence_id = sample.sentence_id if 'sample' in locals() else "N/A"
#             readme_content = generate_readme(language, pct, False, processed_count, last_sentence_id)
#             zs.add(iter([readme_content.encode("utf-8")]), arcname="README.txt")
            

#             # Run the synchronous ZipStream generator in a thread
#             zip_stream_output = await asyncio.to_thread(iter, zs)
            
#             # Create an async generator from the synchronous generator output
#             async def async_zip_output_generator(sync_gen: Iterable[bytes]) -> AsyncIterable[bytes]:
#                 for chunk in sync_gen:
#                     yield chunk
            
#             # Upload the async stream to S3 using aiobotocore
#             await async_zip_stream_to_s3_multipart(
#                 async_zip_output_generator(zip_stream_output), 
#                 async_s3_client, 
#                 settings.S3_BUCKET_NAME, 
#                 export_filename,
#             )
            
#             # 8. Final status update and return
#             download_url = await asyncio.to_thread(
#                 s3_aws.generate_presigned_url,
#                 'get_object',
#                 Params={'Bucket': settings.S3_BUCKET_NAME, 'Key': export_filename},
#                 ExpiresIn=604800 # 7 days
#             )
            
#             async with session_maker() as session:
#                 await update_export_job_status(
#                     session, job_id, DownloadStatusEnum.READY,
#                     progress_pct=100, download_url=download_url
#                 )
            
#             return {
#                 'job_id': job_id, 
#                 'download_url': download_url, 
#                 'total_samples': processed_count
#             }
            
#     except Exception as e:
#         logger.exception(f"Job {job_id} failed during streaming: {e}")
#         async with session_maker() as session:
#             await update_export_job_status(
#                 session, job_id, DownloadStatusEnum.FAILED,
#                 error_message=str(e),
#                 progress_pct=0
#             )
#         raise e



# @celery_app.task(bind=True, name="exports.create_dataset_zip_s3_task_new", acks_late=True)
# def create_dataset_zip_s3_task_new(
#   self, 
#   job_id: str,
#   language: str,
#   pct: float | None = None,
#   category: str | None = None,
#   gender: str | None = None,
#   age_group: str | None = None,
#   education: str | None = None,
#   split: str | None = None,
#   domain: str | None = None
# ):

#     def fresh_session_maker():
#       return get_async_session_maker(force_new=True)
#     # ... (original logic) ...
#     loop = asyncio.new_event_loop()
#     asyncio.set_event_loop(loop)
#     try:
#         return loop.run_until_complete(
#             async_create_dataset_zip_s3_impl(
#               self, job_id, language, pct, category,
#               gender, age_group, education, split, domain,
#               fresh_session_maker=fresh_session_maker
#             )
#         )
#     except Exception as e:
#         logger.exception(f"Job {job_id} failed: {e}")
#         raise e
#     finally:
#         loop.close()