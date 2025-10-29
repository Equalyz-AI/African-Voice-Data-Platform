import logging, asyncio, aiofiles, os, tempfile, shutil, zipfile
from typing import AsyncIterable
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from botocore.exceptions import BotoCoreError, ResponseStreamingError

from src.db.db import get_async_session_maker
from src.download.s3_config import s3_aws, map_category_to_folder
from src.download.s3_config_async import get_async_s3_client_factory
from src.config import settings
from src.download.utils import generate_readme

logger = logging.getLogger(__name__)
CONCURRENT_DOWNLOADS = 20


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((ResponseStreamingError, BotoCoreError))
)
async def async_get_s3_body_stream_with_retry(async_s3_client, bucket_name, key) -> AsyncIterable[bytes]:
    """
    Fetches the S3 object stream.
    
    The 'Body' of the response from aiobotocore's get_object is 
    an aiohttp.ClientResponse object. This object is an async context
    manager, but it is NOT async iterable.
    
    We must iterate over its .content attribute, which is the StreamReader.
    """
    response = await async_s3_client.get_object(Bucket=bucket_name, Key=key)
    
    async with response["Body"] as http_response:
        
        async for chunk in http_response.content:
            yield chunk


async def download_sample_to_temp_file(sample, async_s3_client, temp_dir_path, semaphore):
    arcname = f"audio/{sample.sentence_id}.wav"
    folder = map_category_to_folder(sample.language, sample.category)

    print(f"Downloading {sample.sentence_id} to {temp_dir_path}") 
    key = f"{sample.language.lower()}-test/{folder}/{sample.sentence_id}.wav"

    local_file_path = os.path.join(temp_dir_path, f"{sample.sentence_id}.wav")

    async with semaphore:
        try:
            async with aiofiles.open(local_file_path, 'wb') as f:
                async for chunk in async_get_s3_body_stream_with_retry(async_s3_client, settings.OBS_BUCKET_NAME, key):
                    await f.write(chunk)

            row = (
                f'"{sample.speaker_id}","{sample.sentence_id}","{sample.sentence or ""}","{arcname}",'
                f'"{sample.gender}","{sample.age_group}","{sample.edu_level}","{sample.duration}",'
                f'"{sample.language}","{sample.snr}","{sample.domain}"\n'
            )
            return (local_file_path, arcname, row)
        except Exception as e:
            logger.warning(f"Failed to download {key}: {e}")
            return None


def create_zip_file_from_temp(zip_path, files_to_add, metadata_path, readme_path):
    logger.info(f"Creating zip file at {zip_path}...")
    with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_STORED) as zf:
        zf.write(metadata_path, arcname="metadata.csv")
        zf.write(readme_path, arcname="README.txt")
        for local_path, arcname in files_to_add:
            zf.write(local_path, arcname=arcname)
    logger.info(f"Finished creating zip file.")




async def upload_final_zip_to_s3(async_s3_client, local_zip_path, bucket, key):
    logger.info(f"Uploading {local_zip_path} → s3://{bucket}/{key}")
    
    file_size = os.path.getsize(local_zip_path)
    
    async with aiofiles.open(local_zip_path, mode='rb') as f:
        file_content = await f.read()
        
    # 3. Use the low-level put_object method
    await async_s3_client.put_object(
        Bucket=bucket, 
        Key=key, 
        Body=file_content, 
        ContentLength=file_size,
        ContentType='application/zip'
    )

    logger.info("Upload complete.")




async def prezip_dataset_zip_s3(
    language: str,
    pct: float | None = None,
    category: str | None = None,
    gender: str | None = None,
    age_group: str | None = None,
    education: str | None = None,
    split: str | None = None,
    domain: str | None = None,
):
    """Runs the dataset pre-zipping process directly (no Celery)."""
    logger.info(f"Starting dataset pre-zip for langauge {language}")

    session_maker = get_async_session_maker()

    the_lang = language
    export_filename = f"exports/{language}_{pct}pct_{split}.zip"
    temp_dir_path = tempfile.mkdtemp()
    logger.info(f"Temporary dir: {temp_dir_path}")

    files_for_zip = []
    processed_count = 0

    try:
        async_s3_client_factory = get_async_s3_client_factory(settings, is_obs=True)
        async with async_s3_client_factory() as async_s3_client:
            from src.download.service import DownloadService
            download_service = DownloadService(s3_bucket_name=settings.OBS_BUCKET_NAME)

            async with session_maker() as session:
              samples_stream, total_to_process = await download_service.filter_core_stream(
                session=session, language=language, pct=pct, category=category,
                gender=gender, age_group=age_group, education=education,
                split=split, domain=domain
              )

              if total_to_process == 0:
                  logger.warning("No samples found.")
                  print(f"The download is not possible. No data")
                  return

              semaphore = asyncio.Semaphore(CONCURRENT_DOWNLOADS)
              metadata_temp_path = os.path.join(temp_dir_path, "metadata.csv")

              async with aiofiles.open(metadata_temp_path, 'w', encoding='utf-8') as f_meta:
                  await f_meta.write("speaker_id,transcript_id,transcript,audio_path,gender,age_group,education,duration,language,snr,domain\n")

                  # =========================================
                  tasks = [
                      download_sample_to_temp_file(sample, async_s3_client, temp_dir_path, semaphore)
                      async for sample in samples_stream
                  ]
                  # ===========================================

                  results = await asyncio.gather(*tasks)

                  for result in results:
                      if result:
                          local_path, arcname, metadata_row = result
                          files_for_zip.append((local_path, arcname))
                          await f_meta.write(metadata_row)
                          processed_count += 1

            readme_temp_path = os.path.join(temp_dir_path, "README.txt")
            readme_content = generate_readme(language, pct, False, processed_count, "N/A")
            async with aiofiles.open(readme_temp_path, 'w', encoding='utf-8') as f_readme:
                await f_readme.write(readme_content)


            final_zip_path = os.path.join(temp_dir_path, f"{pct}{the_lang}.zip")
            await asyncio.to_thread(create_zip_file_from_temp, final_zip_path, files_for_zip, metadata_temp_path, readme_temp_path)

            async_s3_dest_factory = get_async_s3_client_factory(settings, is_obs=False)
            async with async_s3_dest_factory() as async_s3_dest_client:
                await upload_final_zip_to_s3(async_s3_dest_client, final_zip_path, settings.S3_BUCKET_NAME, export_filename)

            download_url = await asyncio.to_thread(
                s3_aws.generate_presigned_url, 'get_object',
                Params={'Bucket': settings.S3_BUCKET_NAME, 'Key': export_filename},
                ExpiresIn=604800
            )
            
            print(f"The download is complete.", download_url)

            return {
              "download_url": download_url, 
              "total_samples": processed_count
            }

    except Exception as e:
        logger.exception(f"Job failed: {e}")
        print(f"The download is not possible.", e)
        raise e
    finally:
        await asyncio.to_thread(shutil.rmtree, temp_dir_path)








if __name__ == "__main__":
    import asyncio

    language = "naija"
    pct = 0.05
    category = "spontaneous"
    gender = None
    age_group = None
    education = None
    split = "train"
    domain = "AG"

    asyncio.run(prezip_dataset_zip_s3(
        language=language,
        pct=pct,
        category=category,
        gender=gender,
        age_group=age_group,
        education=education,
        split=split,
        domain=domain,
    ))
