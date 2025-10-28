from pydantic import BaseModel
from typing import List, Optional
from pydantic import BaseModel, Field


class AudioSamplePreview(BaseModel):
    id: Optional[str] = Field(default=None)
    annotator_id: str
    sentence_id: Optional[str] = Field(default=None)
    sentence: Optional[str] = Field(default=None)
    storage_link: Optional[str] = Field(default=None)
    audio_url_obs: Optional[str] = Field(default=None)
    transcript_url_obs: Optional[str] = Field(default=None)
    gender: Optional[str] = Field(default=None)
    age_group: Optional[str] = Field(default=None)
    edu_level: Optional[str] = Field(default=None)
    durations: Optional[str] = Field(default=None)
    language: Optional[str] = Field(default="naija")
    snr: Optional[int] = Field(default=40)
    domain: Optional[str] = Field(default=None)
    category: str


class AudioPreviewResponse(BaseModel):
    samples: List[AudioSamplePreview]



class EstimatedSizeResponse(BaseModel):
    estimated_size_bytes: int
    estimated_size_mb: float
    sample_count: int
    # total_durations: Optional[str] = None