from src.db.models import AudioSample
import  io
import pandas as pd
from typing import Optional, List
import datetime
import requests
import aiohttp
import asyncio, os, aioboto3
from fastapi import HTTPException
from src.db.models import AudioSample, Category
from src.download.s3_config import  SUPPORTED_LANGUAGES
from src.download.s3_config import s3_aws
from src.download.s3_config import generate_obs_signed_url, map_sentence_id_to_transcript_obs
from sqlmodel import select, and_
from src.config import settings
from zipstream import ZipStream, ZIP_DEFLATED

s3 = s3_aws

# ================================================================================
semaphore = asyncio.Semaphore(5)

async def fetch_audio_stream(session, sample, retries=3):
    print(f"Fetching {sample.sentence_id}")
    for attempt in range(1, retries + 1):
        try:
            async with session.get(sample.storage_link, timeout=10) as resp:
                if resp.status == 200:
                    audio_data = bytearray()
                    async for chunk in resp.content.iter_chunked(1024):
                        audio_data.extend(chunk)
                    print(f"✅ Fetched {sample.sentence_id}")
                    return sample.sentence_id, bytes(audio_data)
                else:
                    print(f"❌ Non-200 status for {sample.sentence_id}: {resp.status}")
        except Exception as e:
            print(f"[Attempt {attempt}] Error streaming {sample.sentence_id}: {e}")
            await asyncio.sleep(2 ** attempt)  # exponential backoff
    print(f"❌ Failed to fetch {sample.sentence_id} after {retries} attempts")
    return sample.sentence_id, None


async def fetch_audio_limited(session, sample):
    async with semaphore:
        return await fetch_audio_stream(session, sample)
    
async def fetch_all(samples):
    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_audio_limited(session, s) for s in samples]
        print(f"Downloading {len(samples)} samples")
        return await asyncio.gather(*tasks)


async def fetch_size(session, url):
    try:
        async with session.head(url, timeout=5) as resp:
            size = int(resp.headers.get("Content-Length", 0))
            return size
    except Exception:
        return 0

async def estimate_total_size(urls):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_size(session, url) for url in urls]
        sizes = await asyncio.gather(*tasks)
        return sum(sizes)


def generate_metadata_buffer(samples: List[AudioSample], as_excel=True):
    """Create metadata buffer in either Excel or CSV."""
    df = pd.DataFrame([{
        "speaker_id": s.speaker_id,
        "transcript_id": s.sentence_id,
        "transcript": s.sentence or "",
        "audio_path": f"audio/{s.sentence_id}.wav",
        "gender": s.gender,
        "age_group": s.age_group,
        "edu_level": s.edu_level,
        "durations": s.duration,
        "language": s.language,
        "edu_level": s.edu_level,
        "snr": s.snr,
        "domain": s.domain,
    } for idx, s in enumerate(samples)])

    buf = io.BytesIO()
    if as_excel:
        df.to_excel(buf, index=False)
        return buf, "metadata.xlsx"
    else:
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        return io.BytesIO(buf.getvalue().encode()), "metadata.csv"



def generate_readme(language: str, pct: int, as_excel: bool, num_samples: int, sentence_id: Optional[str]=None) -> str:
    return f"""\

        📘 Dataset Export Summary
        =========================
        Language         : {language.upper()}
        Percentage       : {pct}%
        Total Samples    : {num_samples}
        File Format      : {"Excel (.xlsx)" if as_excel else "CSV (.csv)"}
        Date             : {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

        📁 Folder Structure
        ===================
        {language}_{pct}pct_<date>/
        ├── metadata.{"xlsx" if as_excel else "csv"}   - Tabular data with metadata
        ├── README.txt                                 - This file
        └── audio/                                     - Folder with audio clips
            ├── {sentence_id}.wav
            ├── {sentence_id}.wav
            └── ...

        📌 Notes
        ========
        - All audio filenames match the metadata rows.
        - File and folder names include language code, percentage, and date.
        - Use Excel or CSV-compatible software to open metadata.
        - If Excel is not supported, a CSV fallback will be provided.

        ✅ Contact
        ==========
        For feedback or support, reach out to the dataset team.
        """





async def stream_zip_with_metadata(samples, bucket: str, as_excel=True, language='hausa', pct=10, category: Optional[str] = "read"):
    import zipstream
    import datetime

    sentence_id = None
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    zip_folder = f"{language}_{pct}pct_{today}"
    zip_name = f"{zip_folder}_dataset.zip"

    z = zipstream.ZipFile(mode="w", compression=zipstream.ZIP_DEFLATED)

    # global 
    # # 1. Add audio files into /audio/
    for idx, s in enumerate(samples):
        audio_filename = f"{zip_folder}/audio/{s.sentence_id}.wav"
        print("the language is: ", language, "\n\n")
        print("the category is: ", category, "\n\n")


        key = f"{language.lower()}/{category.lower()}/{s.sentence_id}.wav"

        print(f"\nDownloading {audio_filename}", "\n", key)
        s3_stream = s3.get_object(Bucket=settings.OBS_BUCKET_NAME, Key=key)['Body']
        print(f"\nDownloading {audio_filename}", "\n", s3_stream)
        z.write_iter(audio_filename, s3_stream)
        sentence_id=s.sentence_id

    # 2. Add metadata (Excel or CSV)
    metadata_buf, metadata_filename = generate_metadata_buffer(samples, as_excel=as_excel)
    metadata_buf.seek(0)
    z.write_iter(f"{zip_folder}/{metadata_filename}", metadata_buf)

    # 3. Add README
    readme_text = generate_readme(language, pct, as_excel, len(samples), sentence_id)
    z.write_iter(f"{zip_folder}/README.txt", io.BytesIO(readme_text.encode("utf-8")))

    return z, zip_name




CHUNK_SIZE = 5 * 1024 * 1024  # 5MB (min size for S3 multipart parts)


async def stream_zip_to_s3(language: str, samples, as_excel: bool = True):
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    zip_folder = f"{language}_{today}"
    zip_name = f"{zip_folder}_dataset.zip"
    object_key = f"exports/{zip_name}"

    # zs = zipstream.ZipFile(mode="w", compression=zipstream.ZIP_DEFLATED)
    zs = ZipStream(compress_type=ZIP_DEFLATED, compress_level=9)

    async with aiohttp.ClientSession() as http_session:
        for s in samples:
            try:
                link = generate_obs_signed_url(
                    language=s.language.lower(),
                    category=s.category,
                    filename=f"{s.sentence_id}.wav"
                )
                async with http_session.get(link) as resp:
                    if resp.status != 200:
                        print(f"⚠️ Skipping {s.sentence_id}, HTTP {resp.status}")
                        continue

                    # read the audio file bytes asynchronously
                    print(f"This is the download details: {s.sentence_id}, {s.sentence}")
                    file_bytes = bytearray()
                    async for chunk in resp.content.iter_chunked(1024 * 1024):
                        file_bytes.extend(chunk)

                    # write the collected bytes as a single iterator for zipstream
                    zs.add(iter([bytes(file_bytes)]), arcname=f"{zip_folder}/audio/{s.sentence_id}.wav")

                    


            except Exception as e:
                print(f"❌ Error fetching {s.sentence_id}: {e}")
                continue

    # Add metadata
    metadata_buf, metadata_filename = generate_metadata_buffer(samples, as_excel)
    metadata_buf.seek(0)
    zs.add(iter([metadata_buf.read()]), arcname=f"{zip_folder}/{metadata_filename}")
    

    # Add README
    readme_text = generate_readme(language, 100, as_excel, len(samples), samples[-1].sentence_id)
    zs.add(iter([readme_text.encode()]), arcname=f"{zip_folder}/README.txt")

    # --- STREAM UPLOAD TO S3 ---
    session = aioboto3.Session()
    async with session.client(
        "s3",
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        region_name=settings.AWS_REGION,
        endpoint_url=settings.AWS_ENDPOINT_URL,
    ) as s3_client:
        # Start multipart upload
        mpu = await s3_client.create_multipart_upload(
            Bucket=settings.S3_BUCKET_NAME,
            Key=object_key,
        )

        parts = []
        part_number = 1
        buffer = b""

        try:
            for chunk in zs:
                buffer += chunk
                if len(buffer) >= CHUNK_SIZE:
                    part = await s3_client.upload_part(
                        Bucket=settings.S3_BUCKET_NAME,
                        Key=object_key,
                        PartNumber=part_number,
                        UploadId=mpu["UploadId"],
                        Body=buffer,
                    )
                    parts.append({"ETag": part["ETag"], "PartNumber": part_number})
                    part_number += 1
                    buffer = b""

            # Upload last remaining chunk
            if buffer:
                part = await s3_client.upload_part(
                    Bucket=settings.S3_BUCKET_NAME,
                    Key=object_key,
                    PartNumber=part_number,
                    UploadId=mpu["UploadId"],
                    Body=buffer,
                )
                parts.append({"ETag": part["ETag"], "PartNumber": part_number})

            # Complete multipart upload
            await s3_client.complete_multipart_upload(
                Bucket=settings.S3_BUCKET_NAME,
                Key=object_key,
                MultipartUpload={"Parts": parts},
                UploadId=mpu["UploadId"],
            )

            signed_url = await s3_client.generate_presigned_url(
                ClientMethod="get_object",
                Params={"Bucket": settings.S3_BUCKET_NAME, "Key": object_key},
                ExpiresIn=3600,
            )

            print(f"✅ Streamed directly to s3://{settings.S3_BUCKET_NAME}/{object_key}")
            return {"download_url": signed_url}

        except Exception as e:
            await s3_client.abort_multipart_upload(
                Bucket=settings.S3_BUCKET_NAME,
                Key=object_key,
                UploadId=mpu["UploadId"],
            )
            raise HTTPException(500, f"Streaming upload failed: {e}")
