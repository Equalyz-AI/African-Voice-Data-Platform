import logging
import asyncio
import aiofiles
import os
import tempfile
import shutil
import zipfile
import time
import hmac
import hashlib
import base64
import urllib.parse
from typing import AsyncIterable, Optional

# New required import for direct URL download
import aiohttp 

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from botocore.exceptions import BotoCoreError, ResponseStreamingError
from aiohttp.client_exceptions import ClientError as AiohttpClientError

# NOTE: You will need to ensure these imports match your project structure
from src.db.db import get_async_session_maker
from src.download.s3_config import s3_aws, map_category_to_folder, generate_obs_signed_url
from src.download.s3_config_async import get_async_s3_client_factory
from src.config import settings
from src.download.utils import generate_readme

logger = logging.getLogger(__name__)
CONCURRENT_DOWNLOADS = 20
DOWNLOAD_TIMEOUT_SECONDS = 300
CHUNK_SIZE_BYTES = 8192 # 8 KB for reliable streaming


# --- DOWNLOAD LOGIC ---
async def download_sample_to_temp_file(sample, temp_dir_path, semaphore):
    """
    Downloads a single sample using the OBS signed URL for maximum reliability.
    The async_s3_client parameter has been removed.
    """
    arcname = f"audio/{sample.sentence_id}.wav"
    
    print(f"Downloading {sample.sentence_id}...") 
    
    # 1. Generate the signed, direct HTTP URL
    try:
        url = generate_obs_signed_url(
            language=sample.language.lower(), 
            category=sample.category, 
            filename=f"{sample.sentence_id}.wav"
        )
    except Exception as e:
        logger.error(f"Failed to generate signed URL for {sample.sentence_id}: {e}")
        return None

    local_file_path = os.path.join(temp_dir_path, f"{sample.sentence_id}.wav")

    async with semaphore:
        try:
            # 2. Use standard aiohttp for reliable HTTP streaming download
            timeout_config = aiohttp.ClientTimeout(total=DOWNLOAD_TIMEOUT_SECONDS)
            async with aiohttp.ClientSession(timeout=timeout_config) as session:
                async with session.get(url) as response:
                    # Raise an exception for bad status codes (4xx/5xx)
                    response.raise_for_status() 
                    
                    async with aiofiles.open(local_file_path, 'wb') as f:
                        # Stream the content in controlled chunks to prevent "Chunk too big" error
                        async for chunk in response.content.iter_chunked(CHUNK_SIZE_BYTES):
                            await f.write(chunk)
            
            # --- METADATA GENERATION ---
            row = (
                f'"{sample.speaker_id}","{sample.sentence_id}","{sample.sentence or ""}","{arcname}",'
                f'"{sample.gender}","{sample.age_group}","{sample.edu_level}","{sample.duration}",'
                f'"{sample.language}","{sample.snr}","{sample.domain}"\n'
            )
            return (local_file_path, arcname, row)
            
        except Exception as e:
            # Catch exceptions from aiohttp/aiofiles/etc.
            logger.warning(f"Failed to download {sample.sentence_id} from URL: {e}")
            return None


# --- ZIP AND UPLOAD FUNCTIONS (UNCHANGED) ---

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
        
    await async_s3_client.put_object(
        Bucket=bucket, 
        Key=key, 
        Body=file_content, 
        ContentLength=file_size,
        ContentType='application/zip'
    )

    logger.info("Upload complete.")


# --- MAIN ORCHESTRATION FUNCTION (MODIFIED) ---

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
        # We only need the S3 client for the final upload, not the downloads
        async_s3_client_factory = get_async_s3_client_factory(settings, is_obs=True) 
        
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
                    download_sample_to_temp_file(sample, temp_dir_path, semaphore)
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

            # S3 client is only used here for the final upload
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


# --- EXECUTION BLOCK (UNCHANGED) ---

if __name__ == "__main__":
    import asyncio

    language = "yoruba"
    pct = 100
    category = None
    gender = None
    age_group = None
    education = None
    split = "dev_test"
    domain = None

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



















