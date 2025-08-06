from fastapi import APIRouter, Depends, BackgroundTasks, Query
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi.responses import StreamingResponse
from src.db.db import get_session
from src.auth.utils import get_current_user
from src.auth.schemas import TokenUser
from src.download.service import DownloadService
from src.download.schemas import AudioPreviewResponse, EstimatedSizeResponse
from src.db.models import  Categroy, GenderEnum


download_router = APIRouter()
download_service = DownloadService()


def map_all_to_none(value: str | None) -> str | None:
    return None if value == "all" else value


# @download_router.get(
#     "/samples/{language}/preview",
#     response_model=AudioPreviewResponse,
#     summary="Preview audio samples",
#     description="Returns a list of audio samples with presigned URLs for playback.",
# )
# async def preview_audio_samples(
#     language: str,
#     limit: int = Query(10, ge=1, le=50),
#     gender: GenderEnum | None = Query(GenderEnum.male, alias="gender"),
#     age: str | None = Query(None),
#     education: str | None = Query(None),
#     domain: str | None = Query(None),
#     category: Categroy | None = Query(Categroy.read, alias="category"),
#     session: AsyncSession = Depends(get_session),
# ):
    
#     """
#      Get a list of audio samples for preview.
#     """
#     return await download_service.preview_audio_samples(
#         session=session, 
#         language=language, 
#         limit=limit, 
#         gender=gender, 
#         age_group=age, 
#         education=education, 
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
    limit: int = Query(10, ge=1, le=50),
    gender: str = Query("male", alias="gender"),  # Accept as string
    age: str | None = Query(None),
    education: str | None = Query(None),
    domain: str | None = Query(None),
    category: str = Query("read", alias="category"),

    session: AsyncSession = Depends(get_session),
):
    gender = map_all_to_none(gender)
    age = map_all_to_none(age)
    education = map_all_to_none(education)
    domain = map_all_to_none(domain)
    category = map_all_to_none(category)

    gender = GenderEnum(gender) if gender else None
    category = Categroy(category) if category else None

    return await download_service.preview_audio_samples(
        session=session, 
        language=language, 
        limit=limit, 
        gender=gender, 
        age_group=age, 
        education=education, 
        domain=domain, 
        category=category
    )



# @download_router.get("/zip/estimate-size/{language}/{pct}", response_model=EstimatedSizeResponse)
# async def estimate_zip_size(
#     language: str,
#     pct: int | float,

#     gender: GenderEnum | None = Query(GenderEnum.male, alias="gender"),
#     age: str | None = Query(None),
#     education: str | None = Query(None),
#     domain: str | None = Query(None),
#     category: Categroy | None = Query(Categroy.read, alias="category"),

#     session: AsyncSession = Depends(get_session),
# ):
#     return await download_service.estimate_zip_size_only(
#         language=language,
#         pct=pct,
#         session=session,

#         gender=gender,
#         age_group=age,
#         education=education,
#         domain=domain,
#         category=category
        
#     )

@download_router.get("/zip/estimate-size/{language}/{pct}", response_model=EstimatedSizeResponse)
async def estimate_zip_size(
    language: str,
    pct: int | float,
    gender: str | None = Query(None),
    age: str | None = Query(None),
    education: str | None = Query(None),
    domain: str | None = Query(None),
    category: str | None = Query(),
    session: AsyncSession = Depends(get_session),
):

    gender = map_all_to_none(gender)
    age = map_all_to_none(age)
    education = map_all_to_none(education)
    domain = map_all_to_none(domain)
    category = map_all_to_none(category)

    gender = GenderEnum(gender) if gender else None
    category = Categroy(category) if category else None

    return await download_service.estimate_zip_size_only(
        session=session,
        language=language,
        pct=pct,
        category=category,
        gender=gender,
        age_group=age,
        education=education,
        domain=domain,
        
    )



# @download_router.get("/zip/{language}/{pct}", response_class=StreamingResponse)
# async def download_zip(
#     language: str,
#     pct: int | float,
#     background_tasks: BackgroundTasks,

#     gender: GenderEnum | None = Query(GenderEnum.male, alias="gender"),
#     age: str | None = Query(None),
#     education: str | None = Query(None),
#     domain: str | None = Query(None),
#     category: Categroy | None = Query(Categroy.read, alias="category"),

#     as_excel: bool = True,
#     current_user: TokenUser = Depends(get_current_user),
#     session: AsyncSession = Depends(get_session),
# ):
#     """
#     Download a zip file of audio datasets for a given language and percentage.
#     """
#     return await download_service.download_zip_with_metadata(
#         language=language, 
#         pct=pct, 
#         session=session, 
#         background_tasks=background_tasks, 
#         current_user=current_user, 
#         as_excel=as_excel,

#         gender=gender, 
#         age_group=age, 
#         education=education, 
#         domain=domain, 
#         category=category
#     )


@download_router.get("/zip/{language}/{pct}", response_class=StreamingResponse)
async def download_zip(
    language: str,
    background_tasks: BackgroundTasks,
    pct: int | float,
    
    gender: str | None = Query(None),
    age: str | None = Query(None),
    education: str | None = Query(None),
    domain: str | None = Query(None),
    category: str | None = Query(None),
    
    as_excel: bool = True,
    current_user: TokenUser = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
):

    gender = map_all_to_none(gender)
    age = map_all_to_none(age)
    education = map_all_to_none(education)
    domain = map_all_to_none(domain)
    category = map_all_to_none(category)

    gender = GenderEnum(gender) if gender else None
    category = Categroy(category) if category else None

    return await download_service.download_zip_with_metadata(
        language=language, 
        pct=pct, 
        session=session, 
        background_tasks=background_tasks, 
        current_user=current_user, 
        as_excel=as_excel,

        gender=gender, 
        age_group=age, 
        education=education, 
        domain=domain, 
        category=category
    )
