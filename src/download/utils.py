from src.db.models import AudioSample
import  io
import pandas as pd
from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select
from .s3_config import s3
from typing import Optional
import datetime
import requests
import requests
from io import BytesIO
from math import floor
import aiohttp
import asyncio, os
from enum import Enum
import botocore.exceptions
from fastapi import HTTPException
from src.db.models import AudioSample, Categroy
from src.download.s3_config import  SUPPORTED_LANGUAGES
from sqlmodel import select, and_



# =========================================================================
# async def fetch_audio(session, sample):
#     async with session.get(sample.storage_link) as resp:
#         if resp.status == 200:
#             return sample.sentence_id, await resp.read()
#         return sample.sentence_id, None

# async def fetch_all(samples):
#     async with aiohttp.ClientSession() as session:
#         tasks = [fetch_audio(session, s) for s in samples]
#         return await asyncio.gather(*tasks)
    


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

# =========================================================================


async def fetch_subset(
    session: AsyncSession, 
    language: str, 
    pct: int | float,

    category: str | None = Categroy.read,
    gender: str | None = None,
    age_group: str | None = None,
    education: str | None = None,
    domain: str | None = None,
        
    ):
    # Count total number of samples
    if language not in SUPPORTED_LANGUAGES:
            raise HTTPException(400, f"Unsupported language: {language}. Only 'Naija', Yoruba', 'Igbo', and 'Hausa' are supported")
    if category not in  [Categroy.read, Categroy.read_as_spontaneous]:
        print(Categroy.read, Categroy.read_as_spontaneous)
        raise HTTPException(400, f"Unavailable category: {category}. Only 'Read' and 'Read_as_Spontanueos' are available")

    filters = [AudioSample.language == language]

    if gender:
        filters.append(AudioSample.gender == gender)
    if category:
        filters.append(AudioSample.category == category)
    if age_group:
        filters.append(AudioSample.age_group == age_group)
    if education:
        filters.append(AudioSample.edu_level == education)
    if domain:
        filters.append(AudioSample.domain == domain)

    total_stmt = select(func.count()).select_from(AudioSample).where(and_(*filters))
    total_result = await session.execute(total_stmt)
    total = total_result.scalar_one()

    print(f"Total samples for {language}: {total}")

    if total == 0:
        return []

    count = max(1, int(floor(total * pct / 100)))

    stmt = (
        select(AudioSample)
        .where(and_(*filters))
        .order_by(AudioSample.id)
        .limit(count)
    )
    result = await session.execute(stmt)
    response = result.scalars().all()

    if not response:
        raise HTTPException(404, "No audio samples found. There might not be enough data for the selected filters")
    print(response)
    return response




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



# def estimate_total_size(samples, bucket, language, category):
#     total_size = 0
#     for s in samples:
#         # key = f"{language.lower()}/{category}/{s.sentence_id}"
#         key = f"{language.lower()}/{category.value if isinstance(category, Enum) else category}/{s.sentence_id}.wav"

#         try:
#             head = s3.head_object(Bucket=bucket, Key=key)
#             total_size += head['ContentLength']
#         except botocore.exceptions.ClientError as e:
#             error_code = e.response['Error']['Code']
#             if error_code == '404':
#                 print(f"File not found: {key}")
#             else:
#                 print(f"Error accessing {key}: {e}")
#             continue
#     return total_size



def generate_metadata_buffer(samples, as_excel=True):
    """Create metadata buffer in either Excel or CSV."""
    df = pd.DataFrame([{
        "speaker_id": s.annotator_id,
        "transcript_id": s.sentence_id,
        "transcript": s.sentence,
        "audio_path": f"audio/{s.sentence_id}.wav",
        "gender": s.gender,
        "age_group": s.age_group,
        "edu_level": s.edu_level,
        "durations": s.durations,
        "language": s.language,
        "edu_level": s.edu_level,
        "snr": s.snr,
        "domain": s.domain,
        "category": s.category,
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



async def stream_zip_with_metadata_links(samples, bucket: str, as_excel=True, language='hausa', pct=10, category: Optional[str] = "read"):
    import zipstream
    import datetime

    today = datetime.datetime.now().strftime("%Y-%m-%d")
    zip_folder = f"{language}_{pct}pct_{today}"
    zip_name = f"{zip_folder}_dataset.zip"

    audio_contents = await fetch_all(samples)
    valid_results = [(sid, data) for sid, data in audio_contents if data is not None]
    print(f"✅ Fetched {len(valid_results)} / {len(samples)}")

    valid_ids = {sid for sid, _ in valid_results}
    filtered_samples = [s for s in samples if s.sentence_id in valid_ids]

    z = zipstream.ZipFile(mode="w", compression=zipstream.ZIP_DEFLATED)
    
    sentence_id_new = None
    for sentence_id, audio_data in valid_results:
        z.write_iter(f"{zip_folder}/audio/{sentence_id}.wav", [audio_data])
        sentence_id_new=sentence_id

    metadata_buf, metadata_filename = generate_metadata_buffer(filtered_samples, as_excel=as_excel)
    metadata_buf.seek(0)
    z.write_iter(f"{zip_folder}/{metadata_filename}", metadata_buf)

    readme_text = generate_readme(language, pct, as_excel, len(filtered_samples), sentence_id_new)
    z.write_iter(f"{zip_folder}/README.txt", io.BytesIO(readme_text.encode("utf-8")))

    return z, zip_name


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
        key = f"{language.lower()}/{category.value if isinstance(category, Enum) else category}/{s.sentence_id}.wav"
        s3_stream = s3.get_object(Bucket=bucket, Key=key)['Body']
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