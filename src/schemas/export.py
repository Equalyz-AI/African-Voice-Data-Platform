# app/schemas/export.py

from email import message
from typing import Optional
from pydantic import BaseModel, Field
from datetime import datetime

# Schema for creating a job (validates incoming data)
class ExportJobCreate(BaseModel):
    user_id: str
    language: str
    percentage: float = Field(..., gt=0, le=100) # Percentage must be between 1-100

# Schema for returning job status (formats outgoing data)
class ExportJobStatus(BaseModel):
    id: Optional[str] = None
    status: Optional[str] = None
    message: Optional[str] = "Sit tight, we're zipping your files up and sending them to you."
    language: Optional[str] = None
    percentage: Optional[float] = None
    progress_pct: Optional[int] = None
    download_url: Optional[str] = None
    error_message: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True