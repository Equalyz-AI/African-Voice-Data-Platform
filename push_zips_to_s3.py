import logging
import asyncio
import aiofiles
import os
import tempfile
import zipfile
import csv
from typing import Optional
from tqdm.asyncio import tqdm
import aiohttp
from botocore.exceptions import BotoCoreError, ClientError

from src.db.db import get_async_session_maker
from src.download.s3_config import generate_obs_signed_url
from src.download.s3_config_async import get_async_s3_client_factory
from src.config import settings
from src.download.utils import generate_readme


# ----------------------------- CONFIG -----------------------------
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
CONCURRENT_DOWNLOADS = 20
DOWNLOAD_TIMEOUT_SECONDS = 300
CHUNK_SIZE_BYTES = 8192
CHUNK_SIZE_UPLOAD = 64 * 1024 * 1024  # 64MB
MAX_PARALLEL_UPLOADS = 8
MAX_SINGLE_RUN = 8000  # Maximum files per batch
TRACKING_FILE = "processed_files.csv"
# ------------------------------------------------------------------

# ----------------------------- UTILS -----------------------------
async def download_sample_to_temp_file(sample, temp_dir_path, semaphore):
    """Download a single file efficiently."""
    # Ensure the sentence_id ends with .wav
    filename = sample.sentence_id
    if not filename.lower().endswith(".wav"):
        filename += ".wav"

    arcname = f"audio/{filename}"
    local_file_path = os.path.join(temp_dir_path, filename)

    if os.path.exists(local_file_path):
        logger.debug(f"File exists, skipping: {sample.sentence_id}")
        return local_file_path, arcname, sample
        

    url = generate_obs_signed_url(
        language=sample.language.lower(),
        category=sample.category,
        filename=filename,
    )

    async with semaphore:
        try:
            timeout_config = aiohttp.ClientTimeout(total=DOWNLOAD_TIMEOUT_SECONDS)
            async with aiohttp.ClientSession(timeout=timeout_config) as session:
                async with session.get(url) as response:
                    response.raise_for_status()
                    async with aiofiles.open(local_file_path, "wb") as f:
                        async for chunk in response.content.iter_chunked(CHUNK_SIZE_BYTES):
                            await f.write(chunk)
            return local_file_path, arcname, sample
        except Exception as e:
            logger.warning(f"Failed download {sample.sentence_id}: {e}")
            return None

def create_zip_file(zip_path, files, metadata_path, readme_path):
    """Create ZIP file on disk without loading all files in memory."""
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_STORED) as zf:
        zf.write(metadata_path, arcname="metadata.csv")
        zf.write(readme_path, arcname="README.txt")
        for local_path, arcname in files:
            zf.write(local_path, arcname=arcname)

async def multipart_upload_to_s3(async_s3_client, local_path, bucket, key):
    """Optimized parallel multipart upload to S3."""
    file_size = os.path.getsize(local_path)
    total_parts = (file_size + CHUNK_SIZE_UPLOAD - 1) // CHUNK_SIZE_UPLOAD
    logger.info(f"Starting multipart upload: {key} ({file_size/1e6:.2f} MB, {total_parts} parts)")

    mpu = await async_s3_client.create_multipart_upload(
        Bucket=bucket,
        Key=key,
        ContentType="application/zip"
    )
    upload_id = mpu["UploadId"]
    sem = asyncio.Semaphore(MAX_PARALLEL_UPLOADS)
    tasks = []

    async def upload_part(part_number, chunk_data):
        async with sem:
            for attempt in range(3):
                try:
                    response = await async_s3_client.upload_part(
                        Bucket=bucket,
                        Key=key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=chunk_data
                    )
                    logger.info(f"✅ Uploaded part {part_number}/{total_parts}")
                    return {"PartNumber": part_number, "ETag": response["ETag"]}
                except (BotoCoreError, ClientError) as e:
                    if attempt < 2:
                        logger.warning(f"Retry part {part_number} attempt {attempt+1}: {e}")
                        await asyncio.sleep(2 ** attempt)
                    else:
                        raise

    async with aiofiles.open(local_path, "rb") as f:
        part_number = 1
        while True:
            chunk = await f.read(CHUNK_SIZE_UPLOAD)
            if not chunk:
                break
            tasks.append(upload_part(part_number, chunk))
            part_number += 1

    uploaded_parts = await asyncio.gather(*tasks)
    uploaded_parts = sorted(uploaded_parts, key=lambda p: p["PartNumber"])

    await async_s3_client.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": uploaded_parts},
    )
    logger.info(f"🎉 Multipart upload complete: {key}")

# ------------------------- MAIN FUNCTION -------------------------
async def prezip_dataset(language: str, pct: float = 100, split: Optional[str] = None):
    """Pre-zip dataset with dynamic batching based on MAX_SINGLE_RUN."""
    session_maker = get_async_session_maker()

    # Load processed files
    processed_files = set()
    if os.path.exists(TRACKING_FILE):
        with open(TRACKING_FILE, newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                processed_files.add(row[0])
        logger.info(f"Loaded {len(processed_files)} already processed files.")

    from src.download.service import DownloadService
    download_service = DownloadService(s3_bucket_name=settings.OBS_BUCKET_NAME)

    async with session_maker() as session:
        samples_stream, total = await download_service.filter_core_stream(
            session=session,
            language=language,
            pct=pct,
            split=split,
        )

        # Collect samples excluding already processed
        all_samples = [s async for s in samples_stream if s.sentence_id not in processed_files]
        total_remaining = len(all_samples)
        if total_remaining == 0:
            logger.info("No new samples to process.")
            return

        # Determine batches
        if total_remaining <= MAX_SINGLE_RUN:
            batches = [all_samples]
            logger.info(f"Processing all {total_remaining} files in a single batch.")
        else:
            batches = [all_samples[i:i+MAX_SINGLE_RUN] for i in range(0, total_remaining, MAX_SINGLE_RUN)]
            logger.info(f"Processing {total_remaining} files in {len(batches)} batches of up to {MAX_SINGLE_RUN} files each.")

        batch_index = 1
        for batch_samples in batches:
            logger.info(f"--- Batch {batch_index}: {len(batch_samples)} files ---")
            temp_dir = tempfile.mkdtemp(prefix=f"{language}_batch{batch_index}_")
            files_for_zip = []
            meta_path = os.path.join(temp_dir, "metadata.csv")
            readme_path = os.path.join(temp_dir, "README.txt")
            semaphore = asyncio.Semaphore(CONCURRENT_DOWNLOADS)

            # Write metadata CSV
            async with aiofiles.open(meta_path, "w", encoding="utf-8") as f_meta:
                await f_meta.write("speaker_id,transcript_id,transcript,audio_path,gender,age_group,education,duration,language,snr,domain\n")
                pbar = tqdm(total=len(batch_samples), desc=f"Downloading batch {batch_index}", ncols=100)

                async def wrapped(s):
                    res = await download_sample_to_temp_file(s, temp_dir, semaphore)
                    pbar.update(1)
                    return res

                results = await asyncio.gather(*[wrapped(s) for s in batch_samples])
                pbar.close()

                for r in results:
                    if r:
                        local_path, arcname, sample = r
                        files_for_zip.append((local_path, arcname))
                        row = f'"{sample.speaker_id}","{sample.sentence_id}","{sample.sentence or ""}","{arcname}",'
                        row += f'"{sample.gender}","{sample.age_group}","{sample.edu_level}","{sample.duration}",'
                        row += f'"{sample.language}","{sample.snr}","{sample.domain}"\n'
                        await f_meta.write(row)

            # Write README
            async with aiofiles.open(readme_path, "w") as f_r:
                await f_r.write(generate_readme(language, pct, False, len(batch_samples), f"Batch {batch_index}, split={split}"))

            # Create ZIP
            final_zip = os.path.join(temp_dir, f"{language}_split{split}_pct{pct}_batch{batch_index}.zip")
            await asyncio.to_thread(create_zip_file, final_zip, files_for_zip, meta_path, readme_path)

            # Upload
            async_s3_factory = get_async_s3_client_factory(settings, is_obs=False)
            async with async_s3_factory() as async_s3_client:
                s3_key = f"exports/{language}_{split}_{pct}%_batch[{batch_index}].zip"
                await multipart_upload_to_s3(async_s3_client, final_zip, settings.S3_BUCKET_NAME, s3_key)

            # Update tracking
            with open(TRACKING_FILE, "a", newline="") as f:
                writer = csv.writer(f)
                for s in batch_samples:
                    writer.writerow([s.sentence_id, s.speaker_id, s.category, s.language])

            # Clean temp folder
            try:
                for file in os.listdir(temp_dir):
                    os.remove(os.path.join(temp_dir, file))
                os.rmdir(temp_dir)
                logger.info(f"Cleaned temp folder for batch {batch_index}")
            except Exception as e:
                logger.warning(f"Failed to clean temp folder: {e}")

            batch_index += 1

# ------------------------- RUN -------------------------
# if __name__ == "__main__":
#     import sys
#     language = "yoruba"
#     pct = 100
#     split = "train"
#     asyncio.run(prezip_dataset(language, pct, split))


if __name__ == "__main__":
    import sys

    languages = ["igbo", "naija"]
    splits = ["train", "dev", "dev_test"]
    pct = 100
    BASE_CONCURRENT = 2  # default parallel combinations

    from src.download.service import DownloadService
    download_service = DownloadService(s3_bucket_name=settings.OBS_BUCKET_NAME)
    session_maker = get_async_session_maker()

    async def estimate_total_files(language, split):
        """Quickly get total files for a language+split without downloading."""
        async with session_maker() as session:
            try:
                _, total = await download_service.filter_core_stream(
                    session=session,
                    language=language,
                    pct=pct,
                    split=split,
                )
                return total
            except ValueError:
                logger.warning(f"No audio samples found for Language={language}, Split={split}. Skipping.")
                return 0

    async def process_combination(language, split):
        logger.info(f"==== Starting: Language={language}, Split={split}, Pct={pct} ====")
        try:
            await prezip_dataset(language=language, pct=pct, split=split)
        except Exception as e:
            logger.error(f"Error processing Language={language}, Split={split}: {e}", exc_info=True)
        logger.info(f"==== Finished: Language={language}, Split={split} ====")

    async def main_loop():
        # Step 1: Estimate totals for all combinations
        estimates = []
        for language in languages:
            for split in splits:
                total_files = await estimate_total_files(language, split)
                if total_files > 0:
                    estimates.append((language, split, total_files))
                    logger.info(f"Estimate: {language}-{split} -> {total_files} files\n\n")
                else:
                    logger.info(f"Skipping {language}-{split} because there are no files.\n\n")

        if not estimates:
            logger.info("No valid language/split combinations found. Exiting.\n\n")
            return

        # Step 2: Adjust max concurrent runs dynamically
        semaphore = asyncio.Semaphore(BASE_CONCURRENT)
        if any(total > MAX_SINGLE_RUN for _, _, total in estimates):
            semaphore = asyncio.Semaphore(1)
            logger.info("Detected large batch (>50k files). Limiting to 1 concurrent combination.\n")

        # Step 3: Launch tasks with semaphore
        tasks = []
        for language, split, _ in estimates:
            async def wrapped(lang=language, sp=split):
                async with semaphore:
                    await process_combination(lang, sp)
            tasks.append(wrapped())

        await asyncio.gather(*tasks)

    asyncio.run(main_loop())
