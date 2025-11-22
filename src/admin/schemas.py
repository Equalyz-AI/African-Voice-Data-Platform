# src/admin/schemas.py
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime


class EngagementStats(BaseModel):
    language: str
    download_count: int
    feedback_count: int


class DownloadProgress(BaseModel):
    total: int
    breakdown: dict[int, int]  # percentage -> count


class FeedbackItem(BaseModel):
    audio_id: str
    transcript: str
    submitted_at: datetime
    language: str
    gender: str
    duration: float


class FeedbackListResponse(BaseModel):
    feedbacks: List[FeedbackItem]


class UploadResult(BaseModel):
    uploaded_count: int
    sample_ids: List[str]



class ResponseSuccess(BaseModel):
    success: bool = True
    message: str = "Request successful"