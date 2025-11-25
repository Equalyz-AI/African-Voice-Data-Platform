from src.db.models import AudioSample
import  io
import pandas as pd
from typing import Optional, List
from datetime import datetime
import aiohttp
import asyncio
from src.db.models import AudioSample
from src.download.s3_config import s3_aws

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
        "audio_id": s.audio_id,
        "transcript": s.sentence or "",
        "audio_path": f"audio/{s.audio_id}.wav",
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
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    return f"""\

        📘 Dataset Export Summary
        =========================
        Language         : {language.upper()}
        Percentage       : {pct}%
        Total Samples    : {num_samples}
        File Format      : {"Excel (.xlsx)" if as_excel else "CSV (.csv)"}
        Date             : {timestamp}

        📁 Folder Structure
        ===================
        {language}_{pct}pct_<{timestamp}>/
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
