import logging
import asyncio
import aiofiles
import os, re
import tempfile
import zipfile
import csv
from typing import Optional
from tqdm.asyncio import tqdm
import aiohttp
from botocore.exceptions import BotoCoreError, ClientError

from src.core.main_one_config import container_client
from src.db.db import get_async_session_maker
from src.download.main_one import upload_to_azure
from src.download.s3_config import generate_obs_signed_url
from src.download.s3_config_async import get_async_s3_client_factory
from src.config import settings
from src.download.utils import generate_readme
import tarfile
import zstandard as zstd

def create_tar_zst_file(tar_zst_path, files, metadata_path, readme_path):
    """Create a .tar.zst archive on disk."""
    # .tar.zst
    # First, create a plain tar in memory
    with tempfile.NamedTemporaryFile(delete=False) as tmp_tar_file:
        tmp_tar_path = tmp_tar_file.name

    with tarfile.open(tmp_tar_path, "w") as tar:
        # Add metadata and README
        tar.add(metadata_path, arcname="metadata.csv")
        tar.add(readme_path, arcname="README.txt")
        # Add all audio files
        for local_path, arcname in files:
            tar.add(local_path, arcname=arcname)

    # Compress tar with Zstandard
    cctx = zstd.ZstdCompressor(level=3)  # Level 1–22, 3 is a good balance
    with open(tmp_tar_path, "rb") as ifh, open(tar_zst_path, "wb") as ofh:
        cctx.copy_stream(ifh, ofh)

    # Remove temporary tar
    os.remove(tmp_tar_path)


def create_tar_gz_file(tar_path, files, metadata_path, readme_path):
    """
    Create a .tar.gz archive instead of zip.
    - files: list of tuples (local_path, arcname)
    - metadata_path, readme_path: paths to include in archive
    """

    # .tar.gz
    with tarfile.open(tar_path, "w:gz") as tar:
        # Add metadata.csv
        tar.add(metadata_path, arcname="metadata.csv")
        # Add README.txt
        tar.add(readme_path, arcname="README.txt")
        # Add all audio files
        for local_path, arcname in files:
            tar.add(local_path, arcname=arcname)



def create_zip_file(zip_path, files, metadata_path, readme_path):
    """Create ZIP file on disk without loading all files in memory."""
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_STORED) as zf:
        zf.write(metadata_path, arcname="metadata.csv")
        zf.write(readme_path, arcname="README.txt")
        for local_path, arcname in files:
            zf.write(local_path, arcname=arcname)



def normalize_wav_filename(raw_name: str) -> str:
    """
    Normalize filename to ensure:
    - No prefixes like 'record', 'recorder', 'mic', etc. interfere
    - Ends with exactly one .wav
    - Safe lowercase handling
    """
    name = raw_name.strip()

    # Remove all leading prefixes that look like "record", "recorder", "recorder45-", etc.
    name = re.sub(r'^(record(er)?\d*-?)', '', name, flags=re.IGNORECASE)

    # Remove all trailing .wav (in case of double or triple)
    while name.lower().endswith(".wav"):
        name = name[:-4]

    # Add back exactly one .wav
    name = f"{name}.wav"

    return name


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
    """Download one file, handling .wav issues and recorder-prefixed names."""
    
    file_to_download = normalize_wav_filename(sample.sentence_id)
    filename = normalize_wav_filename(sample.audio_id)

    print(f"filename of this audio is {filename}\n\n\n")

    arcname = f"audio/{filename}"
    local_file_path = os.path.join(temp_dir_path, filename)

    if os.path.exists(local_file_path):
        logger.debug(f"Already exists: {filename}")
        return local_file_path, arcname, sample

    async def try_download(fname):
        url = generate_obs_signed_url(
            language=sample.language.lower(),
            category=sample.category,
            filename=fname,
        )
        timeout = aiohttp.ClientTimeout(total=600)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as response:
                if response.status == 404:
                    return False
                response.raise_for_status()
                async with aiofiles.open(local_file_path, "wb") as f:
                    async for chunk in response.content.iter_chunked(8192):
                        await f.write(chunk)
        return True

    async with semaphore:
        try:
            ok = await try_download(file_to_download)

            # If not found, try recorderXX- prefixed versions
            if not ok:
                # Try recorder1–99 prefixes
                for i in range(1, 100):
                    alt = f"recorder{i}-{file_to_download}"
                    ok = await try_download(alt)
                    if ok:
                        logger.info(f"Recovered with prefix: {alt}")
                        break

            if not ok:
                logger.warning(f"❌ Not found even after prefix attempts: {file_to_download}")
                return None

            return local_file_path, arcname, sample

        except Exception as e:
            logger.warning(f"⚠️ Error downloading {file_to_download}: {e}")
            return None



async def prezip_dataset_to_main_one(language: str, pct: float = 100, split: Optional[str] = None):
    """Pre-zip dataset and upload to Azure with clean folder structure."""
    session_maker = get_async_session_maker()

    # Load already processed files
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

        # Filter out already processed samples
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
                await f_meta.write("speaker_id,audio_id,transcript,audio_path,gender,age_group,education,duration,language,snr,domain\n")
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
                        row = f'"{sample.speaker_id}","{sample.audio_id}","{sample.sentence or ""}","{arcname}",'
                        row += f'"{sample.gender}","{sample.age_group}","{sample.edu_level}","{sample.duration}",'
                        row += f'"{sample.language}","{sample.snr}","{sample.domain}"\n'
                        await f_meta.write(row)

            # Write README
            async with aiofiles.open(readme_path, "w") as f_r:
                await f_r.write(generate_readme(language, pct, False, len(batch_samples), f"Batch {batch_index}, split={split}"))

            # Create ZIP
            final_zip = os.path.join(temp_dir, f"Batch_{batch_index}.tar.zst")
            await asyncio.to_thread(create_tar_zst_file, final_zip, files_for_zip, meta_path, readme_path)

            # Upload to Azure
            blob_name = f"exports/{language}/{split}/Batch_{batch_index}.tar.zst"

            logger.info(f"Uploading batch {batch_index} to Azure: {blob_name}")
            await upload_to_azure(container_client, final_zip, blob_name)

            # Update tracking CSV
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



if __name__ == "__main__":

    languages = ["hausa"]
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
            await prezip_dataset_to_main_one(language=language, pct=pct, split=split)
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













# async def prezip_dataset_to_main_one(language: str, pct: float = 100, split: str = None):
#     session_maker = get_async_session_maker()

#     from src.download.service import DownloadService
#     # Load already processed IDs for resume
#     processed = set()
#     if os.path.exists(TRACKING_FILE):
#         with open(TRACKING_FILE, "r", encoding="utf-8-sig") as f:
#             for row in f:
#                 processed.add(row.strip())
#         logger.info(f"Loaded {len(processed)} processed IDs")

#     download_service = DownloadService(s3_bucket_name=settings.OBS_BUCKET_NAME)

#     async with session_maker() as session:
#         stream, total = await download_service.filter_core_stream(
#             session=session,
#             language=language,
#             pct=pct,
#             split=split,
#         )

#         all_samples = [s async for s in stream]
#         logger.info(f"Total samples in DB = {len(all_samples)}")

#         # Skip batches if resuming
#         # samples_to_process = skip_logic(all_samples)

#         samples_to_process = all_samples
#         # Split into batches
#         batches = [
#             samples_to_process[i:i+MAX_SINGLE_RUN]
#             for i in range(0, len(samples_to_process), MAX_SINGLE_RUN)
#         ]

#         batch_index = START_BATCH

#         for batch in batches:
#             logger.info(f"========= BATCH {batch_index} | {len(batch)} FILES =========")
#             temp_dir = tempfile.mkdtemp(prefix=f"{language}_b{batch_index}_")
#             semaphore = asyncio.Semaphore(CONCURRENT_DOWNLOADS)

#             files_for_zip = []
#             metadata_rows = []

#             pbar = tqdm(total=len(batch), desc=f"Batch {batch_index}")

#             async def wrap(s):
#                 res = await download_sample_to_temp_file(s, temp_dir, semaphore)
#                 pbar.update(1)
#                 return res

#             results = await asyncio.gather(*[wrap(s) for s in batch])
#             pbar.close()

#             for r in results:
#                 if not r:
#                     continue
#                 local_path, arcname, sample = r

#                 flac_path = convert_wav_to_flac(local_path)
#                 arcname = arcname.replace(".wav", ".flac")
#                 files_for_zip.append((flac_path, arcname))

#                 flac_path = convert_wav_to_flac(local_path, sample)

#                 if not flac_path:
#                     # already logged → just skip
#                     continue

#                 arcname = arcname.replace(".wav", ".flac")
#                 files_for_zip.append((flac_path, arcname))


#                 metadata_rows.append({
#                     "speaker_id": sample.speaker_id,
#                     "audio_id": sample.audio_id,
#                     "transcript": sample.sentence or "",
#                     "audio_path": arcname,
#                     "gender": sample.gender,
#                     "age_group": sample.age_group,
#                     "education": sample.edu_level,
#                     "duration": sample.duration,
#                     "language": sample.language,
#                     "snr": sample.snr,
#                     "domain": sample.domain
#                 })

#             # Write metadata as proper Excel
#             meta_path = os.path.join(temp_dir, "metadata.xlsx")
#             df = pd.DataFrame(metadata_rows)
#             df.to_excel(meta_path, index=False, engine="openpyxl")

#             # Write README
#             readme_path = os.path.join(temp_dir, "README.txt")
#             async with aiofiles.open(readme_path, "w", encoding="utf-8") as f_r:
#                 from src.download.utils import generate_readme
#                 await f_r.write(generate_readme(language, pct, False, len(batch), f"Batch {batch_index}, split={split}"))

#             # Create zip
#             # zip_path = os.path.join(temp_dir, f"Batch_{batch_index}.zip")
#             # await asyncio.to_thread(create_zip_file, zip_path, files_for_zip, meta_path, readme_path)

#             final_tar_zst = os.path.join(temp_dir, f"Batch_{batch_index}.tar.zst")
#             await asyncio.to_thread(create_tar_zst_file, final_tar_zst, files_for_zip, meta_path, readme_path)


#             # Upload
#             # blob = f"exports2/{language}/{split}/Batch_{batch_index}.zip"
#             # logger.info(f"Uploading: {blob}")
#             # await upload_to_azure(container_client, zip_path, blob)

#              # Upload to Azure
#             blob_name = f"exports2/{language}/{split}/Batch_{batch_index}.tar.zst"
#             logger.info(f"Uploading batch {batch_index} to Azure: {blob_name}")
#             await upload_to_azure(container_client, final_tar_zst, blob_name)


#             # Track processed
#             with open(TRACKING_FILE, "a", encoding="utf-8-sig") as f:
#                 for s in batch:
#                     f.write(f"{s.sentence_id}\n")

#             batch_index += 1
