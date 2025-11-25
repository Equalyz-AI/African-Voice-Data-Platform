from pydantic import BaseModel
from typing import Dict, List, Optional
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
    estimated_size_in_bytes: Optional[float] = None
    estimated_size_in_mb: Optional[float] = None
    estimated_size_in_gb: Optional[float] = None
    number_of_audios: Optional[float] = None
    total_duration_in_seconds: Optional[float] = None
    number_of_males: Optional[float] = None
    number_of_females: Optional[float] = None
    domains: Optional[list] = None






class AudioItem(BaseModel):
    audio_id: str
    signed_url: str
    transcript: Optional[str] = None
    duration: Optional[float] = None
    gender: Optional[str] = None
    education: Optional[str] = None
    split: Optional[str] = None
    type: Optional[str] = None





class DomainDistributionItem(BaseModel):
    count: int
    pct: float


class EstimatedSizeResponse(BaseModel):
    estimated_size_in_bytes: int
    estimated_size_in_mb: float
    estimated_size_in_gb: float
    sample_count: int
    total_duration_seconds: float

    male_voicing_count: int
    female_voicing_count: int
    pct_male_voicings: float
    pct_female_voicings: float

    unique_male_speakers: int
    unique_female_speakers: int

    domain_distribution: Dict[str, DomainDistributionItem]
