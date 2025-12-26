from ast import Not
from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, Union, Literal

from src.db.redis import make_cache_key
from src.db.db import get_session
from src.auth.utils import get_current_user
from src.auth.schemas import TokenUser
from src.download.service import DownloadService
from src.download.schemas import AudioItem, AudioPreviewResponse, AudioSamplePreview, DownloadZipResponseUnion, EstimatedSizeResponse
from src.db.models import  Category, GenderEnum, Split
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
                
    print(f"This is the language coming from the backend\n\n {val}")
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





# @download_router.get(
#     "/samples/{language}/preview",
#     response_model=AudioPreviewResponse,
#     summary="Preview audio samples",
#     description="Returns a list of audio samples with presigned URLs for playback.",
#     include_in_schema=False
# )
# async def preview_audio_samples(
#     language: str,
#     limit: int = Query(10, ge=1, le=50),
#     gender: str | None = Query(None, alias="gender"),
#     age: str | None = Query(None),
#     education: str | None = Query(None),
#     domain: str | None = Query(None),
#     category: str | None = Query(None),
#     split: Split = Query(default=Split.train, description="Split type: train, dev, or dev_test"),
#     session: AsyncSession = Depends(get_session),
# ):

#     gender = map_all_to_none(value=gender)
#     age = map_all_to_none(value=age)
#     education = map_all_to_none(value=education)
#     domain = map_EV_to_EV(domain, language)
#     category = map_all_to_none(category, language)
#     split = split.value


#     gender = GenderEnum(gender) if gender else None
#     category = Category(category) if category else None
#     language = language.lower()

#     return await download_service.preview_audio_samples(
#         session=session, 
#         language=language, 
#         limit=limit, 
#         gender=gender, 
#         age_group=age, 
#         education=education, 
#         split=split,
#         domain=domain, 
#         category=category
#     )



@download_router.get(
    "/samples/{language}/preview",
    response_model=AudioPreviewResponse,
    summary="Preview audio samples",
    description="Returns a list of audio samples with presigned URLs for playback.",
)
async def preview_audio_samples(
    language: str,
    request: Request,
    category: Literal["read", "spontaneous"] = Query(default="spontaneous"),
    split: Split = Query(default=Split.train, description="Split type: train, dev, or dev_test"),
    session: AsyncSession = Depends(get_session),
):
    split = split.value
    language = language.lower()

    redis = request.app.state.redis if request.app.state.redis else None


    result =  await download_service.get_all_signed_audio(
        language=language,
        split=split,
        category=category,
        redis=redis
    )

    samples = [
        AudioSamplePreview(
            annotator_id=audio.get("annotator_id"),
            sentence_id=audio.get("annotator_id"),
            audio_url_obs=audio.get("audio_url_obs"),
            sentence=audio.get("sentence"),
            storage_link=audio.get("storage_link"),
            duration=audio.get("duration"),
            gender=audio.get("gender"),
            edu_level=audio.get("education"),
            category=audio.get("category"),
        )
        for audio in result
    ]
    return AudioPreviewResponse(samples=samples)



@download_router.get("/zip/estimate-size/{language}/{pct}", response_model=Union[EstimatedSizeResponse, dict])
async def estimate_zip_size(
    request: Request,
    language: str,
    pct: int | float,
    gender: str | None = Query(None),
    age: str | None = Query(None),
    education: str | None = Query(None),
    domain: str | None = Query(None),
    category: str | None = Query(None),
    split: Split = Query(default=Split.train, description="Split type: train, dev, or dev_test"),
    session: AsyncSession = Depends(get_session),
):
    gender = map_all_to_none(value=gender)
    age = map_all_to_none(value=age)
    education = map_all_to_none(value=education)
    domain = map_EV_to_EV(domain, language)
    category = map_all_to_none(category, language)
    split = split.value
    language = language.lower()

    gender = GenderEnum(gender) if gender else None
    category = Category(category) if category else None

    # Get the existing Azure batch listing
    if pct != 100:
        return {"error": "Only 100% zips are available."}

    redis = request.app.state.redis if request.app.state.redis else None

    response = await download_service.download_zip_from_azure(
        language=language,
        pct=pct,
        session=session,
        split=split,
        redis=redis,
    )

    #  Handle error or empty result
    if "error" in response:
        return response

    batches = response.get("batches", [])
    if not batches:
        return {
            "message": f"No ZIP batches found for {language}-{split}-{pct}%"
        }

    total_mb = round(sum(b["size_mb"] for b in batches), 2)
    total_bytes = int(total_mb * 1024 * 1024)
    total_gb = round(total_mb / 1024, 2)

    result = await download_service.estimate_zip_size_only(
        session=session,
        language=language,
        pct=pct,
        split=split,
        category=category,
        gender=gender,
        education=education,
        domain=domain,
        age_group=age,

        total_mb=total_mb,
        total_bytes=total_bytes,
        total_gb=total_gb,

        redis=redis
    )

    return result





@download_router.get("/zip/{language}/{pct}", response_model=DownloadZipResponseUnion)
async def download_zip(
    language: str,
    pct: int | float,
    request: Request,
    split: Split = Query(default=Split.train, description="Split type: train, dev, or dev_test"),
    current_user: TokenUser = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
):
    """Download pre-zipped data batches for a specific language and split."""

    redis = request.app.state.redis if request.app.state.redis else None
    # enforce the only allowed pct
    if pct != 100:
        return {
            "error": "Only 100% zips are available."
        }

    language = language.lower()
    split = split.value

    return await download_service.download_zip_from_azure(
        language=language,
        pct=pct,
        session=session,
        split=split,
        redis=redis
    )