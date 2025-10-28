# app/crud/crud_export.py

import uuid
from typing import Optional
from sqlmodel import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.models import DownloadLog, DownloadStatusEnum
from src.schemas.export import ExportJobCreate

async def get_export_job(session: AsyncSession, job_id: str) -> Optional[DownloadLog]:
    """Reads a single export job from the database by its ID."""
    return await session.get(DownloadLog, job_id)

async def create_export_job(session: AsyncSession, job_create: ExportJobCreate) -> DownloadLog:
    """Creates a new export job record in the database."""
    # Create a database model instance from the schema data
    db_job = DownloadLog(
        user_id=job_create.user_id,
        language=job_create.language,
        percentage=job_create.percentage,
        status=DownloadStatusEnum.QUEUED # Set initial status
    )
    session.add(db_job)
    await session.commit()
    await session.refresh(db_job)
    print(db_job)
    return db_job



    
async def update_export_job_status(
    session: AsyncSession,
    job_id: str,
    status: str,
    download_url: Optional[str] = None,
    error_message: Optional[str] = None,
    progress_pct: Optional[int] = None
) -> Optional[DownloadLog]:
    """Updates the status, progress, and other details of an export job."""
    db_job = await get_export_job(session, job_id)
    if db_job:
        db_job.status = status
        if download_url:
            db_job.download_url = download_url
        if error_message:
            db_job.error_message = error_message
        if progress_pct is not None:
            db_job.progress_pct = progress_pct
        await session.commit()
        await session.refresh(db_job)
    print(db_job)
    return db_job
