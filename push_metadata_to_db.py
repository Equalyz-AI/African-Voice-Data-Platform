import asyncio
from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd
from sqlmodel import select
from src.db.db import get_async_session_maker
from src.db.models import AudioSample, Category, Split


# === CONFIG ===
DATA_DIR = Path("data")
BATCH_SIZE = 50 
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds


# === HELPERS ===
def get_split_from_filename(filename: str) -> Split:
    """Infer dataset split (train, dev, dev_test) from filename."""
    fname = filename.lower()
    if "train" in fname:
        return Split.train
    elif "dev_test" in fname or "test" in fname:
        return Split.dev_test
    elif "dev" in fname:
        return Split.dev
    return Split.train  # default fallback


def map_category(type_str: str) -> Category:
    """Map type column to Category enum."""
    t = str(type_str or "").strip().lower()
    if "spontaneous" in t:
        return Category.spontaneous
    return Category.read


def load_excel_records(file_path: Path) -> List[AudioSample]:
    """Load and parse rows from Excel/CSV file into AudioSample models."""
    if file_path.name.startswith("~$"):
        print(f"⚠️ Skipping temp file: {file_path.name}")
        return []

    suffix = file_path.suffix.lower()
    if suffix == ".xlsx":
        df = pd.read_excel(file_path, engine="openpyxl")
    elif suffix == ".xls":
        df = pd.read_excel(file_path, engine="xlrd")
    elif suffix == ".csv":
        df = pd.read_csv(file_path)
    else:
        raise ValueError(f"Unsupported file type: {suffix}")

    language = file_path.stem.split(".")[0]
    # split = get_split_from_filename(file_path.name)

    records = []
    for _, row in df.iterrows():
        sentence_id = str(row.get("audio_id") or "").strip()
        if not sentence_id:
            print(f"This row will be skipped because it has no audio ID: {sentence_id}")
            continue  # Skip rows without unique 

        records.append(
            AudioSample(
                sentence_id=sentence_id,
                sentence=str(row.get("transcript") or row.get("text") or "").strip(),
                storage_link=str(row.get("audio_path") or "").strip(),
                speaker_id=str(row.get("speaker_id") or "").strip(),
                gender=str(row.get("gender") or "").strip().lower(),
                split=str(row.get("split") or "").strip(),
                age_group=str(row.get("age_group") or "").strip(),
                edu_level=str(row.get("education") or "").strip(),
                duration=str(row.get("duration") or "").strip(),
                language=language,
                domain=str(row.get("domain") or "").strip(),
                category=map_category(row.get("type")),
                created_at=datetime.now(),
            )
        )
    return records


# === DATABASE INSERT ===
async def insert_voice_data(records: List[AudioSample]):
    """Insert audio records in safe, retrying batches (skipping existing IDs)."""
    total_records = len(records)
    print(f"🔍 Preparing {total_records} records for insertion...")

    if not total_records:
        print("⚠️ No records found in this file.")
        return

    # Get all existing sentence_ids to skip duplicates
    all_ids = {r.sentence_id for r in records}
    existing_ids = set()
    
    # Batch the ID check queries (avoid parameter limit)
    ID_CHECK_BATCH = 1000  # Check 1000 IDs at a time
    SessionLocal = get_async_session_maker()
    async with SessionLocal() as session:
        for i in range(0, len(all_ids), ID_CHECK_BATCH):
            batch_ids = list(all_ids)[i:i + ID_CHECK_BATCH]
            stmt = select(AudioSample.sentence_id).where(
                AudioSample.sentence_id.in_(batch_ids)
            )
            result = await session.execute(stmt)
            existing_ids.update({row[0] for row in result.all()})

    records_to_insert = [r for r in records if r.sentence_id not in existing_ids]
    total_new = len(records_to_insert)
    print(f"📊 {total_new} new records to insert (skipping {len(existing_ids)} existing).")

    if not total_new:
        print("✅ No new data to insert.")
        return

    # Insert in batches (existing code is fine)
    for i in range(0, total_new, BATCH_SIZE):
        batch = records_to_insert[i:i + BATCH_SIZE]
        
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                SessionLocal = get_async_session_maker()
                async with SessionLocal() as local_session:
                    for record in batch:
                        local_session.add(record)
                    await local_session.commit()
                
                print(f"✅ Batch {i // BATCH_SIZE + 1} — inserted {len(batch)} records.")
                break
            except Exception as e:
                print(f"⚠️ Batch {i // BATCH_SIZE + 1} failed (attempt {attempt}): {e}")
                if attempt < MAX_RETRIES:
                    print(f"↻ Retrying in {RETRY_DELAY}s...")
                    await asyncio.sleep(RETRY_DELAY)
                else:
                    print(f"❌ Giving up on batch {i // BATCH_SIZE + 1}.")



# === MAIN RUNNER ===
async def run():
    """Main orchestrator — loops through all files in /data."""
    if not DATA_DIR.exists():
        print(f"❌ Data folder not found: {DATA_DIR.resolve()}")
        return

    excel_files = [f for f in DATA_DIR.glob("*") if f.suffix.lower() in [".xlsx", ".xls", ".csv"]]
    print(f"📂 Found {len(excel_files)} Excel/CSV files in {DATA_DIR.resolve()}")

    for file_path in excel_files:
        print(f"\n📘 Processing {file_path.name} ...")
        records = load_excel_records(file_path)
        if not records:
            print(f"⚠️ No valid records in {file_path.name}. Skipping.")
            continue

        await insert_voice_data(records)
        print(f"🎯 Completed: {file_path.name}")

    print("\n🎉 All files processed successfully!")


if __name__ == "__main__":
    asyncio.run(run())