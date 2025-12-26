import logging
import asyncio
import math
import aiofiles
import os, re
import tempfile
import zipfile
import pandas as pd
from typing import Optional
from tqdm.asyncio import tqdm
from pydub import AudioSegment
import aiohttp, shutil, gc

from pydub.exceptions import CouldntDecodeError
from botocore.exceptions import BotoCoreError, ClientError

from src.core.main_one_config import container_client
from src.db.db import get_async_session_maker
from src.download.main_one import upload_to_azure
from src.download.s3_config import generate_obs_signed_url
from src.download.s3_config_async import get_async_s3_client_factory
from src.config import settings
from src.download.utils import generate_readme


# ============================================================
# NORMALIZE WAV FILENAMES
# ============================================================


# ============================================================
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

CONCURRENT_DOWNLOADS = 20
MAX_SINGLE_RUN = 8000       # max per batch
TRACKING_FILE = "processed_files.csv"
SKIPPED_LOG_FILE = "skipped_audio.log"


START_BATCH = 1
def skip_logic(all_samples, processed):
    all_ids = {s.sentence_id for s in all_samples}
    valid_processed_ids = processed & all_ids

    if not valid_processed_ids:
        logger.info("No valid processed IDs for this dataset. Starting fresh.")
        return all_samples, 1

    processed_count = len(valid_processed_ids)

    completed_batches = math.ceil(processed_count / MAX_SINGLE_RUN)
    start_batch = completed_batches + 1
    skip_count = completed_batches * MAX_SINGLE_RUN

    if skip_count >= len(all_samples):
        logger.info("Nothing to resume, skip count exceeds dataset.")
        return [], start_batch

    samples_to_process = [
        s for s in all_samples
        if s.sentence_id not in valid_processed_ids
    ]

    logger.info(f"Valid processed count = {processed_count}")
    logger.info(f"Completed batches = {completed_batches}")
    logger.info(f"Resuming from batch = {start_batch}")
    logger.info(f"Remaining to process = {len(samples_to_process)}")

    return samples_to_process, start_batch



def normalize_wav_filename(raw_name: str) -> str:
    name = raw_name.strip()
    name = re.sub(r'^(record(er)?\d*-?)', '', name, flags=re.IGNORECASE)

    while name.lower().endswith(".wav"):
        name = name[:-4]

    return f"{name}.wav"


def convert_wav_to_flac(wav_path: str, sample) -> Optional[str]:
    """
    Returns flac path if successful, else None
    """
    try:
        audio = AudioSegment.from_file(wav_path)  # auto-detect format
        flac_path = wav_path.replace(".wav", ".flac")
        audio.export(flac_path, format="flac")

        os.remove(wav_path)

        return flac_path

    except CouldntDecodeError:
        log_skipped(sample, "decode_error_not_wav_or_corrupt")
        return None

    except Exception as e:
        log_skipped(sample, f"conversion_error:{str(e)}")
        return None



def log_skipped(sample, reason: str):
    with open(SKIPPED_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(
            f"audio_id={sample.audio_id}, "
            f"sentence_id={sample.sentence_id}, "
            f"reason={reason}\n"
        )




def safe_cleanup(path: str):
    try:
        shutil.rmtree(path, ignore_errors=True)
        logger.info(f"Cleaned temp dir: {path}")
    except Exception as e:
        logger.warning(f"Failed to cleanup {path}: {e}")




def create_tar_zst_file(tar_zst_path, files, metadata_path, readme_path):
    """Create a highly compressed .tar.zst archive on disk."""
    import zstandard as zstd
    import tarfile
    import tempfile
    import os

    # Step 1: Create a temporary tar file
    with tempfile.NamedTemporaryFile(delete=False) as tmp_tar_file:
        tmp_tar_path = tmp_tar_file.name

    with tarfile.open(tmp_tar_path, "w") as tar:
        tar.add(metadata_path, arcname="metadata.csv")
        tar.add(readme_path, arcname="README.txt")
        for local_path, arcname in files:
            tar.add(local_path, arcname=arcname)

    # Step 2: Compress tar using Zstandard with maximum compression
    cctx = zstd.ZstdCompressor(level=22, write_content_size=True)
    with open(tmp_tar_path, "rb") as ifh, open(tar_zst_path, "wb") as ofh:
        cctx.copy_stream(ifh, ofh)

    # Step 3: Remove temporary tar
    os.remove(tmp_tar_path)


# ============================================================
# DOWNLOAD SINGLE SAMPLE
# ============================================================
async def download_sample_to_temp_file(sample, temp_dir_path, semaphore):
    file_to_download = normalize_wav_filename(sample.sentence_id)
    filename = normalize_wav_filename(sample.audio_id)

    arcname = f"audio/{filename}"
    local_file_path = os.path.join(temp_dir_path, filename)

    if os.path.exists(local_file_path):
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
        ok = await try_download(file_to_download)

        # try recorder prefixes
        if not ok:
            for i in range(1, 100):
                alt = f"recorder{i}-{file_to_download}"
                ok = await try_download(alt)
                if ok:
                    logger.info(f"Recovered with prefix: {alt}")
                    break

        if not ok:
            logger.warning(f"Missing file: {file_to_download}")
            return None

        return local_file_path, arcname, sample


# ============================================================
# ZIP CREATION
# ============================================================
def create_zip_file(zip_path, files, metadata_path, readme_path):
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.write(metadata_path, arcname="metadata.csv")
        zf.write(readme_path, arcname="README.txt")
        for local_path, arcname in files:
            zf.write(local_path, arcname=arcname)


# ============================================================
# MAIN BATCH PRE-ZIPPER
# ============================================================
async def prezip_dataset_to_main_one(language: str, pct: float = 100, split: str = None):
    session_maker = get_async_session_maker()
    from src.download.service import DownloadService

    # Load already processed IDs for resume
    processed = set()
    if os.path.exists(TRACKING_FILE):
        with open(TRACKING_FILE, "r", encoding="utf-8-sig") as f:
            for row in f:
                row = row.strip()
                if row:
                    processed.add(row)
        logger.info(f"Loaded {len(processed)} processed IDs")

    download_service = DownloadService(s3_bucket_name=settings.OBS_BUCKET_NAME)

    async with session_maker() as db_session:   # <-- DB session
        stream, total = await download_service.filter_core_stream(
            session=db_session,
            language=language,
            pct=pct,
            split=split,
        )

        all_samples = [s async for s in stream]
        logger.info(f"Total samples in DB = {len(all_samples)}")

        # samples_to_process = all_samples
        samples_to_process, batch_index = skip_logic(all_samples, processed)

        # Split into batches
        batches = [
            samples_to_process[i:i+MAX_SINGLE_RUN]
            for i in range(0, len(samples_to_process), MAX_SINGLE_RUN)
        ]

        for batch in batches:
            logger.info(f"========= BATCH {batch_index} | {len(batch)} FILES =========")
            temp_dir = tempfile.mkdtemp(prefix=f"{language}_b{batch_index}_")
            semaphore = asyncio.Semaphore(CONCURRENT_DOWNLOADS)

            try:
                files_for_zip = []
                metadata_rows = []

                pbar = tqdm(total=len(batch), desc=f"Batch {batch_index}")

                timeout = aiohttp.ClientTimeout(total=600)
                async with aiohttp.ClientSession(timeout=timeout) as http_session:

                    async def wrap(s):
                        res = await download_sample_to_temp_file(
                            s,
                            temp_dir,
                            semaphore
                        )
                        pbar.update(1)
                        return res

                    results = await asyncio.gather(*[wrap(s) for s in batch])

                pbar.close()

                for r in results:
                    if not r:
                        continue

                    local_path, arcname, sample = r

                    flac_path = convert_wav_to_flac(local_path, sample)
                    if not flac_path:
                        continue

                    arcname = arcname.replace(".wav", ".flac")
                    files_for_zip.append((flac_path, arcname))

                    metadata_rows.append({
                        "speaker_id": sample.speaker_id,
                        "audio_id": sample.audio_id,
                        "transcript": sample.sentence or "",
                        "audio_path": arcname,
                        "gender": sample.gender,
                        "age_group": sample.age_group,
                        "education": sample.edu_level,
                        "duration": sample.duration,
                        "language": sample.language,
                        "snr": sample.snr,
                        "domain": sample.domain
                    })

                meta_path = os.path.join(temp_dir, "metadata.xlsx")
                df = pd.DataFrame(metadata_rows)
                df.to_excel(meta_path, index=False, engine="openpyxl")

                readme_path = os.path.join(temp_dir, "README.txt")
                async with aiofiles.open(readme_path, "w", encoding="utf-8") as f_r:
                    await f_r.write(
                        generate_readme(language, pct, False, len(batch),
                                        f"Batch {batch_index}, split={split}")
                    )

                final_tar_zst = os.path.join(
                    temp_dir, f"Batch_{batch_index}.tar.zst"
                )
                await asyncio.to_thread(
                    create_tar_zst_file,
                    final_tar_zst,
                    files_for_zip,
                    meta_path,
                    readme_path
                )

                blob_name = f"exports2/{language}/{split}/Batch_{batch_index}.tar.zst"
                logger.info(f"Uploading batch {batch_index} to Azure: {blob_name}")
                await upload_to_azure(container_client, final_tar_zst, blob_name)

                with open(TRACKING_FILE, "a", encoding="utf-8-sig") as f:
                    for s in batch:
                        f.write(f"{s.sentence_id}\n")

                # batch_index += 1

            finally:
                safe_cleanup(temp_dir)
                batch_index += 1


# ============================================================
# MAIN LAUNCHER
# ============================================================
if __name__ == "__main__":

    languages = ["igbo", "hausa"]
    splits = ["train"]
    pct = 100

    async def run():
        for lang in languages:
            for sp in splits:
                await prezip_dataset_to_main_one(lang, pct, sp)

    asyncio.run(run())