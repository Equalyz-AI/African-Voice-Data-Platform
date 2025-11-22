from sqlmodel import select, func
from sqlmodel.ext.asyncio.session import AsyncSession
from typing import List
import uuid, io, pandas as pd
from src.db.models import AudioSample, Feedback, DownloadLog
from src.download.s3_config import  SUPPORTED_LANGUAGES, s3_aws
from src.config import settings

REQUIRED_COLUMNS = {
    "transcript", 
    "sample_rate", 
    "snr", 
    "audio_path", 
    "language", 
    "gender", 
    "duration"
}

s3 = s3_aws

class AdminService:

  def __init__(self, s3_bucket_name: str = settings.S3_BUCKET_NAME):
    self.s3_bucket_name = s3_bucket_name

  @staticmethod
  async def aggregate_engagement(session: AsyncSession, language: str | None = None):
    stmt = select(
        AudioSample.language,
        func.count(DownloadLog.id).label("download_count"),
        func.count(Feedback.id).label("feedback_count"),
    ).join(
        DownloadLog, DownloadLog.dataset_id == AudioSample.dataset_id
    ).group_by(AudioSample.language)

    if language:
        stmt = stmt.where(AudioSample.language == language)

    result = await session.execute(stmt)
    return result.all()

  @staticmethod
  async def get_download_progress(session: AsyncSession, dataset_id: str):
    total_stmt = select(func.count()).where(AudioSample.dataset_id == dataset_id)
    total_result = await session.execute(total_stmt)
    total = total_result.scalar_one()

    progress_stmt = (
        select(DownloadLog.percentage, func.count())
        .where(DownloadLog.dataset_id == dataset_id)
        .group_by(DownloadLog.percentage)
    )
    progress_result = await session.execute(progress_stmt)
    downloads = dict(progress_result.all())

    return {"total": total, "breakdown": downloads}

  @staticmethod
  async def list_feedback(session: AsyncSession, language: str | None = None, limit: int = 100):
    stmt = (
      select(Feedback)
      .order_by(Feedback.created_at.desc())
      .limit(limit)
    )

    if language:
      stmt = stmt.where(AudioSample.language == language)

    result = await session.execute(stmt)
    return result.all()

  @staticmethod
  async def upload_bulk_with_excel(
      dataset_id: str,
      excel_bytes: bytes,
      files_map: dict[str, io.BytesIO],
      session: AsyncSession
  ) -> List[AudioSample]:
      df = pd.read_excel(io.BytesIO(excel_bytes), engine="openpyxl")

      if not REQUIRED_COLUMNS.issubset(df.columns):
          raise ValueError(f"Missing columns: {REQUIRED_COLUMNS - set(df.columns)}")

      uploaded = []

      for _, row in df.iterrows():
          key_in_excel = row["audio_path"]
          if key_in_excel not in files_map:
              raise ValueError(f"File not found in upload: {key_in_excel}")
          file_obj = files_map[key_in_excel]

          if row["language"] not in SUPPORTED_LANGUAGES:
              raise ValueError(f"Unsupported language: {row['language']}")

          sample_id = str(uuid.uuid4())
          s3_key = f"datasets/{dataset_id}/{sample_id}.wav"
          s3.upload_fileobj(file_obj, settings.S3_BUCKET_NAME, s3_key)

          sample = AudioSample(
              id=sample_id,
              dataset_id=dataset_id,
              audio_path=s3_key,
              transcription=row["transcript"],
              duration=float(row["duration"]),
              language=row["language"],
              sample_rate=int(row["sample_rate"]),
              snr=float(row["snr"]),
              gender=row["gender"],
          )
          session.add(sample)
          uploaded.append(sample)

      await session.commit()
      return uploaded
