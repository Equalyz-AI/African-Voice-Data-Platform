from fastapi import APIRouter, Depends, HTTPException
from src.auth.utils import get_current_user
from src.auth.utils import TokenUser
from src.admin.utils import generate_excel_template
from fastapi.responses import Response
from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException
from typing import List, Annotated
from sqlmodel.ext.asyncio.session import AsyncSession
from src.db.db import get_session
from src.admin.service import AdminService
from .schemas import (
    EngagementStats, DownloadProgress, FeedbackListResponse, UploadResult, ResponseSuccess
)

admin_router = APIRouter()



@admin_router.get("/download-template")
async def download_template(current_user: TokenUser = Depends(get_current_user)) -> Response:
    if current_user.role != "admin":
        raise HTTPException(status_code=403, detail="Admins only")
    return await generate_excel_template()



@admin_router.get("/engagement", response_model=List[EngagementStats])
async def get_engagement_stats(
    session: Annotated[AsyncSession, Depends(get_session)],
    language: str | None = None,
):
    return await AdminService.aggregate_engagement(session, language)


@admin_router.get("/download-progress/{dataset_id}", response_model=DownloadProgress)
async def get_progress(
    dataset_id: str,
    session: Annotated[AsyncSession, Depends(get_session)]
):
    return await AdminService.get_download_progress(session, dataset_id)


@admin_router.get("/feedback", response_model=FeedbackListResponse)
async def get_feedbacks(
    session: Annotated[AsyncSession, Depends(get_session)],
    language: str | None = None,
):
    rows = await AdminService.list_feedback(session, language)
    response = [
        {
            "audio_id": audio.id,
            "transcript": audio.transcription,
            "submitted_at": fb.submitted_at,
            "language": audio.language,
            "gender": audio.gender,
            "duration": audio.duration,
        }
        for fb, audio in rows
    ]
    return {"feedbacks": response}


@admin_router.post("/upload-audio-excel", response_model=UploadResult)
async def upload_audio_with_excel(
    dataset_id: Annotated[str, Form()],
    excel: Annotated[UploadFile, File()],
    files: Annotated[List[UploadFile], File()],
    session: Annotated[AsyncSession, Depends(get_session)]
):
    try:
        files_map = {file.filename: await file.read() for file in files}
        excel_bytes = await excel.read()
        uploaded_samples = await AdminService.upload_bulk_with_excel(
            dataset_id, excel_bytes, files_map, session
        )
        return {
            "uploaded_count": len(uploaded_samples),
            "sample_ids": [s.id for s in uploaded_samples]
        }
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
