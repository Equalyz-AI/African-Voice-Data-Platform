from fastapi import HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse
from sqlmodel import select, and_
from src.db.models import AudioSample, DownloadLog, Categroy, GenderEnum
from src.auth.schemas import TokenUser
from sqlalchemy.ext.asyncio import AsyncSession
from .utils import fetch_subset
from src.download.s3_config import BUCKET, SUPPORTED_LANGUAGES
from src.download.utils import stream_zip_with_metadata, estimate_total_size, stream_zip_with_metadata_links
from typing import List, Optional





class DownloadService:
    def __init__(self, s3_bucket_name: str = BUCKET):
    
        self.s3_bucket_name = s3_bucket_name
    
    async def preview_audio_samples(
        self,
        session: AsyncSession,
        language: str,
        limit: int = 10,
        category: str = Categroy.read,
        gender: str | None = None,
        age_group: str | None = None,
        education: str | None = None,
        domain: str | None = None,
    ):
        samples, _ = await self.filter_core(
            session=session,
            language=language,
            limit=limit,
            category=category,
            gender=gender,
            age_group=age_group,
            education=education,
            domain=domain,
        )

        urls = [
            {
                "id": str(s.id),
                "annotator_id": s.annotator_id,
                "sentence_id": s.sentence_id,
                "sentence": s.sentence,
                "storage_link": s.storage_link,
                "gender": s.gender,
                "age_group": s.age_group,
                "edu_level": s.edu_level,
                "durations": s.durations,
                "language": s.language,
                "edu_level": s.edu_level,
                "snr": s.snr,
                "domain": s.domain,
                "category": s.category,

            }
            for s in samples
        ]
        return {"samples": urls}


    async def filter_core(
        self,
        session: AsyncSession,
        language: str,
        limit: Optional[int] = None,
        category: str = Categroy.read,
        gender: str | None = None,
        age_group: str | None = None,
        education: str | None = None,
        domain: str | None = None,
            
    ):
        
        if language not in SUPPORTED_LANGUAGES:
            raise HTTPException(400, f"Unsupported language: {language}. Only 'Naija', Yoruba', 'Igbo', and 'Hausa' are supported")
        if category == Categroy.spontaneous:
            raise HTTPException(400, f"Unavailable category: {category}. Only 'Read' and 'Read_as_Spontanueos' are available")

        filters = [AudioSample.language == language]

        if gender:
            filters.append(AudioSample.gender == gender)
        if category:
            filters.append(AudioSample.category == category)
        if age_group:
            filters.append(AudioSample.age_group == age_group)
        if education:
            filters.append(AudioSample.edu_level == education)
        if domain:
            filters.append(AudioSample.domain == domain)

        if limit:
            stmt = (
                select(AudioSample)
                .where(and_(*filters))
                .order_by(AudioSample.id)
                .limit(limit)
            )
        else:
            stmt = (
                select(AudioSample)
                .where(and_(*filters))
                .order_by(AudioSample.id)
            )

        result = await session.execute(stmt)
        samples = result.scalars().all()

        if not samples:
            raise HTTPException(404, "No audio samples found. There might not be enough data for the selected filters")
        
        total = len(samples)
        
        return samples, total
    

    async def estimate_zip_size_only(
        self,
        session: AsyncSession,
        language: str,
        pct: int | float,
        category: str = Categroy.read,
        gender: GenderEnum | None = None,
        age_group: str | None = None,
        education: str | None = None,
        domain: str | None = None,

    ) -> dict:

        samples = await fetch_subset(
            session=session, 
            language=language,
            category=category,
            gender=gender,
            age_group=age_group,
            education=education,
            domain=domain,
            pct=pct
        )

        total_size = await estimate_total_size([s.storage_link for s in samples])
        # total_size = estimate_total_size(samples, self.s3_bucket_name, language, category)
        return {
            "estimated_size_bytes": total_size,
            "estimated_size_mb": round(total_size / (1024**2), 2),
            "sample_count": len(samples)
        }


  
    async def download_zip_with_metadata(
        self, 
        language: str, 
        pct: int | float, 

        session: AsyncSession, 
        background_tasks: BackgroundTasks, 
        current_user: TokenUser,

        category: str | None = Categroy.read,
        gender: GenderEnum | None = None,
        age_group: str | None = None,
        education: str | None = None,
        domain: str | None = None,

        
        as_excel: bool = True
    ):

        samples = await fetch_subset(
            session=session, 
            language=language,
            category=category,
            gender=gender,
            age_group=age_group,
            education=education,
            domain=domain,
            pct=pct
        )

        if not samples:
            raise HTTPException(404, "No audio samples found. There might not be enough data for the selected filters")
        background_tasks.add_task(
            session.add,
            DownloadLog(
                user_id=current_user.id,
                dataset_id=samples[0].dataset_id,
                percentage=pct,
            ),
        )
        await session.commit() 

        # Stream ZIP
        if language.lower() == "hausa":
            zip_stream, zip_filename = await stream_zip_with_metadata_links(
                samples, self.s3_bucket_name, as_excel=as_excel, language=language, pct=pct
            )
        else:
            zip_stream, zip_filename = await stream_zip_with_metadata(
                samples, self.s3_bucket_name, as_excel=as_excel, language=language, pct=pct
            )

        return StreamingResponse(
            zip_stream,
            media_type="application/zip",
            headers={
                "Content-Disposition": f"attachment; filename={zip_filename}"
            }
        )