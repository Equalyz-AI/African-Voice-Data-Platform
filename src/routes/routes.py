from ast import Not
from fastapi import APIRouter, Depends, BackgroundTasks, Query
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi.responses import StreamingResponse
from src.db.db import get_session
from src.auth.utils import get_current_user
from src.auth.schemas import TokenUser
from src.download.service import DownloadService
from src.download.schemas import AudioPreviewResponse, EstimatedSizeResponse
from src.db.models import  Category, GenderEnum, Split
from typing import Optional
from src.config import settings

download_router = APIRouter()
download_service = DownloadService(
    s3_bucket_name=settings.S3_BUCKET_NAME
)


from typing import Optional

def map_all_to_none(value: Optional[str], language: Optional[str] = None) -> Optional[str]:
    if not value:
        return None
    val = value.lower()
    lang = language.lower() if language else None

    if val == "all":
        return None

    # For value and language going into DB
    if val == "read":
        if lang in ["yoruba"]:
            print(f"Mapping the category {val} to spontaneous")
            return "spontaneous"
                
    print(f"This is the language coming from the backend")
    return val


def map_EV_to_EV(category: str | None, language: str | None = None) -> str | None:
    if language.lower() in ["hausa"]:
        if category is None:
            return None
        if category.lower() == "all":
            return None
        if category.lower() == "ec":
            print("Mapping EC to EV for Hausa")
            return "EV"
    return category



@download_router.get(
    "/samples/{language}/preview",
    response_model=AudioPreviewResponse,
    summary="Preview audio samples",
    description="Returns a list of audio samples with presigned URLs for playback.",
)
async def preview_audio_samples(
    language: str,
    limit: int = Query(10, ge=1, le=50),
    gender: str | None = Query(None, alias="gender"),
    age: str | None = Query(None),
    education: str | None = Query(None),
    domain: str | None = Query(None),
    category: str | None = Query(None),
    split: str | None = Query(None),

    session: AsyncSession = Depends(get_session),
):
    gender = map_all_to_none(value=gender)
    age = map_all_to_none(value=age)
    education = map_all_to_none(value=education)
    domain = map_EV_to_EV(domain, language)
    category = map_all_to_none(category, language)

    gender = GenderEnum(gender) if gender else None
    category = Category(category) if category else None
    language = language.lower()

    print("This is the category and language after the mapping: ", category, language, "\n\n")
    return await download_service.preview_audio_samples(
        session=session, 
        language=language, 
        limit=limit, 
        gender=gender, 
        age_group=age, 
        education=education, 
        split=split,
        domain=domain, 
        category=category
    )


@download_router.get("/zip/estimate-size/{language}/{pct}", response_model=EstimatedSizeResponse)
async def estimate_zip_size(
    language: str,
    pct: int | float,
    gender: str | None = Query(None),
    age: str | None = Query(None),
    education: str | None = Query(None),
    domain: str | None = Query(None),
    category: str | None = Query(None),
    split: str | None = Query(None),
    session: AsyncSession = Depends(get_session),
):

    gender = map_all_to_none(value=gender)
    age = map_all_to_none(value=age)
    education = map_all_to_none(value=education)
    domain = map_EV_to_EV(domain, language)
    category = map_all_to_none(category, language)

    gender = GenderEnum(gender) if gender else None
    category = Category(category) if category else None
    language = language.lower()

    print("This is the category after the mapping: ", category, language)

    return await download_service.estimate_zip_size_only(
        session=session,
        language=language,
        pct=pct,
        category=category,
        gender=gender,
        age_group=age,
        education=education,
        split=split,
        domain=domain,
        
    )



@download_router.get("/zip/{language}/{pct}", response_model=dict)
async def download_zip(
    language: str,
    pct: int | float,
    split: Split | None = Query(default=Split.train, description="Split type: train, dev, or dev_test"),
    as_excel: bool = True,
    current_user: TokenUser = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
):
    """Download pre-zipped data batches for a specific language and split."""

    # enforce the only allowed pct
    if pct != 100:
        return {"error": "Only 100% zips are available."}

    language = language.lower()

    return await download_service.download_zip_with_from_s3(
        language=language,
        pct=pct,
        session=session,
        split=split.value if split else None,
    )