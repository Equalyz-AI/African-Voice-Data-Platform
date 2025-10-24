# import logging
# import asyncio
# from typing import Optional, Iterable
# from uuid import uuid4

# from zipstream import ZipStream, ZIP_DEFLATED  # uses zipstream-ng library (low memory) :contentReference[oaicite:0]{index=0}
# from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
# from botocore.exceptions import BotoCoreError, ResponseStreamingError

# from src.core.celery_app import celery_app
# from src.db.db import get_async_session_maker
# from src.db.models import DownloadStatusEnum
# from src.crud.crud_export import get_export_job, update_export_job_status
# from src.download.s3_config import s3_obs, s3_aws
# from src.config import settings

# logger = logging.getLogger(__name__)

# # Constants
# MIN_PART_SIZE = 5 * 1024 * 1024  # 5 MB upload part size
# CHUNK_SIZE = 64 * 1024          # 64 KB read from S3 streams
# MAX_CONCURRENT_STREAMS = 10     # limit concurrent S3 downloads per worker
# COMPRESSION_LEVEL = 6           # moderate compression for CPU balance

# # Semaphore for concurrency control
# _download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_STREAMS)

# @retry(
#     stop=stop_after_attempt(5),
#     wait=wait_exponential(multiplier=1, min=2, max=30),
#     retry=retry_if_exception_type((ResponseStreamingError, BotoCoreError))
# )
# def get_s3_stream_for_file_with_retry(bucket_name: str, key: str) -> Iterable[bytes]:
#     obj = s3_obs.get_object(Bucket=bucket_name, Key=key)
#     body = obj["Body"]
#     return s3_stream_bytes(body)

# def s3_stream_bytes(s3_body, chunk_size: int = CHUNK_SIZE) -> Iterable[bytes]:
#     """Generator yielding bytes from S3 StreamingBody."""
#     while True:
#         chunk = s3_body.read(chunk_size)
#         if not chunk:
#             break
#         yield chunk

# def zip_bytes_generator(zs: ZipStream, min_size: int = MIN_PART_SIZE) -> Iterable[bytes]:
#     """Yield contiguous byte chunks suitable for S3 multipart upload."""
#     buf = bytearray()
#     for part in zs:
#         buf.extend(part)
#         while len(buf) >= min_size:
#             yield bytes(buf[:min_size])
#             buf = buf[min_size:]
#     if buf:
#         yield bytes(buf)

# def stream_zip_to_s3_blocking(zip_gen: Iterable[bytes], bucket: str, key: str):
#     """Perform S3 multipart upload from the zip generator (blocking)."""
#     resp = s3_aws.create_multipart_upload(Bucket=bucket, Key=key)
#     upload_id = resp['UploadId']
#     parts = []
#     part_number = 1
#     try:
#         for part_bytes in zip_bytes_generator(zip_gen, MIN_PART_SIZE):
#             if not part_bytes:
#                 continue
#             resp = s3_aws.upload_part(
#                 Bucket=bucket,
#                 Key=key,
#                 UploadId=upload_id,
#                 PartNumber=part_number,
#                 Body=part_bytes,
#                 ContentLength=len(part_bytes)
#             )
#             parts.append({'PartNumber': part_number, 'ETag': resp['ETag']})
#             part_number += 1
#         if parts:
#             s3_aws.complete_multipart_upload(
#                 Bucket=bucket,
#                 Key=key,
#                 UploadId=upload_id,
#                 MultipartUpload={'Parts': parts}
#             )
#         else:
#             s3_aws.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
#             raise ValueError(f"No valid parts uploaded for {key}")
#     except Exception:
#         s3_aws.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
#         raise

# @celery_app.task(bind=True, name="exports.create_dataset_zip_s3_task_stream", acks_late=True)
# def create_dataset_zip_s3_task_stream(
#     self,
#     job_id: str,
#     language: str,
#     pct: float | None = None,
#     category: str | None = None,
#     gender: str | None = None,
#     age_group: str | None = None,
#     education: str | None = None,
#     split: str | None = None,
#     domain: str | None = None
# ):
#     """Synchronous wrapper to run the async logic."""
#     import asyncio
#     from src.tasks.export_worker_streaming import async_create_dataset_zip_s3_impl_stream, get_async_session_maker

#     def fresh_session_maker():
#         return get_async_session_maker(force_new=True)

#     loop = asyncio.new_event_loop()
#     asyncio.set_event_loop(loop)
#     try:
#         return loop.run_until_complete(
#             async_create_dataset_zip_s3_impl_stream(
#                 self, job_id, language, pct,
#                 category, gender, age_group,
#                 education, split, domain,
#                 fresh_session_maker=fresh_session_maker
#             )
#         )
#     finally:
#         loop.close()

# async def async_create_dataset_zip_s3_impl_stream(
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
#     fresh_session_maker = None
# ):
#     logger.info(f"🚀 Starting export job {job_id}")
#     session_maker = fresh_session_maker() if fresh_session_maker else get_async_session_maker()

#     # Step 1: mark job as processing
#     async with session_maker() as session:
#         job = await get_export_job(session, job_id)
#         if not job:
#             logger.error(f"Job not found: {job_id}")
#             return
#         await update_export_job_status(session, job_id, DownloadStatusEnum.PROCESSING, progress_pct=0)

#     export_filename = f"exports/{language}_{pct}pct_{job_id}.zip"
#     manifest_parts = []

#     from src.download.service import DownloadService
#     download_service = DownloadService(s3_bucket_name=settings.OBS_BUCKET_NAME)

#     # Step 2: get sample stream and count
#     async with session_maker() as session:
#         samples_stream, total_to_process = await download_service.filter_core_stream(
#             session=session,
#             language=language,
#             pct=pct,
#             category=category,
#             gender=gender,
#             age_group=age_group,
#             education=education,
#             split=split,
#             domain=domain
#         )

#     if total_to_process == 0:
#         async with session_maker() as session:
#             await update_export_job_status(
#                 session, job_id, DownloadStatusEnum.FAILED,
#                 error_message="No audio samples found for selected criteria",
#                 progress_pct=0
#             )
#         return {'job_id': job_id, 'download_url': None, 'total_samples': 0}

#     # Step 3: build the streaming ZIP
#     zs = ZipStream(compress_type=ZIP_DEFLATED, compress_level=COMPRESSION_LEVEL)

#     processed_count = 0
#     last_sentence_id = None

#     async for sample in samples_stream:
#         last_sentence_id = sample.sentence_id
#         arcname = f"audio/{sample.sentence_id}.wav"
#         folder = map_category_to_folder(sample.language, sample.category)
#         key = f"{sample.language.lower()}-test/{folder}/{sample.sentence_id}.wav"

#         # limit concurrency for S3 downloads
#         async with _download_semaphore:
#             try:
#                 s3_stream_gen = await asyncio.to_thread(
#                     get_s3_stream_for_file_with_retry,
#                     settings.OBS_BUCKET_NAME,
#                     key
#                 )
#                 zs.add(s3_stream_gen, arcname=arcname)
#             except Exception as e:
#                 logger.warning(f"⚠️ Skipping file {key} due to stream error: {e}")
#                 continue

#         processed_count += 1
#         if processed_count % 10 == 0 or processed_count == total_to_process:
#             progress_pct = int((processed_count / total_to_process) * 95)
#             async with session_maker() as prog_sess:
#                 await update_export_job_status(
#                     prog_sess, job_id, DownloadStatusEnum.PROCESSING,
#                     progress_pct=progress_pct
#                 )

#     # add metadata and README
#     metadata_content = build_metadata_content(...)  # you implement
#     zs.add(iter([metadata_content]), arcname="metadata.csv")
#     readme_content = generate_readme(language, pct, False, processed_count, last_sentence_id)
#     zs.add(iter([readme_content.encode('utf-8')]), arcname="README.txt")

#     # Step 4: stream upload ZIP to S3
#     await asyncio.to_thread(
#         stream_zip_to_s3_blocking,
#         zs,
#         settings.S3_BUCKET_NAME,
#         export_filename
#     )

#     # Step 5: generate presigned URL
#     download_url = await asyncio.to_thread(
#         s3_aws.generate_presigned_url,
#         'get_object',
#         Params={'Bucket': settings.S3_BUCKET_NAME, 'Key': export_filename},
#         ExpiresIn=settings.PRESIGNED_URL_EXPIRATION
#     )

#     # Step 6: update job as ready
#     async with session_maker() as session:
#         await update_export_job_status(
#             session, job_id, DownloadStatusEnum.READY,
#             download_url=download_url, progress_pct=100
#         )

#     logger.info(f"✅ Job {job_id} completed, URL: {download_url}")
#     return {'job_id': job_id, 'download_url': download_url, 'total_samples': processed_count}

# def map_category_to_folder(language: str, category: Optional[str] = None) -> str:
#     language = language.lower()
#     category = (category or "spontaneous").lower()
#     if category == "spontaneous":
#         if language in ["yoruba","naija","hausa"]:
#             return "read-as-spontaneous"
#     elif category == "read" and language in ["yoruba"]:
#         return "read-as-spontaneous"
#     elif category == "read":
#         return "read"
#     return category
