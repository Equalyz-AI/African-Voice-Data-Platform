from datetime import datetime, timedelta
import os, json
from re import split
from azure.storage.blob import BlobSasPermissions, generate_blob_sas
from fastapi import HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse
from sqlmodel import select, and_
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional, Tuple
import math
from redis.asyncio import Redis
from pathlib import PurePosixPath
from botocore.exceptions import NoCredentialsError
from sqlalchemy import select, and_, func
from sqlalchemy.ext.asyncio import AsyncScalarResult
import logging

from src.core.main_one_config import ACCOUNT_KEY, API_VERSION, container_client
from src.db.models import AudioSample, DownloadLog, GenderEnum, Split
from src.config import settings
from src.download.s3_config import (
    SUPPORTED_LANGUAGES,
    generate_obs_signed_url,
    s3_aws,
)

from src.utils.audio_lookup import get_audio_filename

logger = logging.getLogger(__name__)


AUDIO_SAMPLE_RATE = 48000  # Hz
AUDIO_BIT_DEPTH = 16       # bits
AUDIO_CHANNELS = 1         # mono
COMPRESSION_RATIO = 0.65 


def upload_to_s3(local_path: str, bucket_name: str, object_name: str):
    """Upload file to S3 and return a signed URL."""
    try:
        s3_aws.upload_file(local_path, bucket_name, object_name)
        print(f"✅ Uploaded {local_path} to s3://{bucket_name}/{object_name}")
    except NoCredentialsError:
        raise HTTPException(status_code=500, detail="AWS credentials not available")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload to S3: {e}")

    # Generate signed URL (valid for 1 hour)
    url = s3_aws.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket_name, "Key": object_name},
        ExpiresIn=3600
    )
    return url



class DownloadService:
    def __init__(self, s3_bucket_name: str = settings.S3_BUCKET_NAME):
    
        self.s3_bucket_name = s3_bucket_name
    
    async def preview_audio_samples(
        self,
        session: AsyncSession,
        language: str,
        limit: int = 10,
        category: str = None,
        gender: str | None = None,
        age_group: str | None = None,
        education: str | None = None,
        split: str | None = None,
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
            split=split,
            domain=domain,
        )



        urls = [
            {
                "id": str(s.id),
                "annotator_id": s.speaker_id,
                "sentence_id": s.sentence_id,
                "sentence": s.sentence,
                "storage_link": s.storage_link,
                "gender": s.gender,
                "audio_url_obs": generate_obs_signed_url(
                    language=s.language.lower(),
                    category=s.category,
                    filename=f"{s.sentence_id}.wav",
                    storage_link=s.storage_link,
                ),
                "age_group": s.age_group,
                "edu_level": s.edu_level,
                "durations": s.duration,
                "language": s.language,
                "edu_level": s.edu_level,
                "snr": s.snr,
                "domain": s.domain,
                "category": s.category,

            }
            
            for s in samples
        ]
        return {
            "samples": urls
        }
    

    async def get_all_signed_audio(
        self, 
        language: str, 
        redis: Optional[Redis] = None, 
        split: Optional[str] = "train",
        category: Optional[str] = "spontaneous"
    ):
        """
        Returns a list of all audio files for a language with signed OBS URLs.
        """
        cache_key = f"preview_audios:{language}"

        # If redis available, try to get cached data
        if redis:
            cached = await redis.get(cache_key)
            if cached:
                logger.info(
                    f"Fetching from redis: {cache_key}\n\n{json.loads(cached)}\n\n"
                )
                return json.loads(cached)

        result = []

        for audio in get_audio_filename(language=language):
            filename = f"{audio['audio_id']}.wav"
            type = f"{audio['type']}"

            signed_url = generate_obs_signed_url(
                language=language, 
                category=type, 
                filename=filename
            )

            result.append({
                "annotator_id": filename,
                "audio_url_obs": signed_url,
                "sentence": audio.get("transcript"),
                "storage_link": signed_url,
                "duration": audio.get("duration"),
                "gender": audio.get("gender"),
                "education": audio.get("education"),
                "split": audio.get("split"),
                "category": audio.get("type")
            })
        
        # Save result in Redis for 1 hour
        if redis:
            await redis.set(cache_key, json.dumps(result), ex=3600)

        return result


    async def filter_core(
        self,
        session: AsyncSession,
        language: str,
        limit: Optional[int] = None,   # absolute count
        pct: Optional[float] = None,   # percentage (0–100)
        category: str = None,
        gender: str | None = None,
        age_group: str | None = None,
        education: str | None = None,
        split: str | None = None,
        domain: str | None = None,
    ):
        filters = [AudioSample.language == language]

        
        try:
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
            if split:
                filters.append(AudioSample.split == split)

            # Always fetch total first
            total_stmt = select(AudioSample.id).where(and_(*filters))
            total_result = await session.execute(total_stmt)
            total = len(total_result.scalars().all())
        except Exception as e:
            raise HTTPException(500, f"Failed to count samples: {e}")   

        if total == 0:
            raise HTTPException(
                404,
                "No audio samples found. There might not be enough data for the selected filters",
            )

        # Compute effective limit
        effective_limit = None
        if pct is not None:
            if not (0 < pct <= 100):
                raise HTTPException(400, "Percentage must be between 0 and 100")
            effective_limit = math.ceil((pct / 100) * total)
        elif limit is not None:
            effective_limit = limit

        stmt = (
            select(AudioSample)
            .where(and_(*filters))
            .order_by(func.random())
        )
        if effective_limit:
            stmt = stmt.limit(effective_limit)

        result = await session.execute(stmt)
        samples = result.scalars().all()

        return samples, total

    

    async def filter_core_stream(
        self,
        session: AsyncSession,
        language: str,
        pct: Optional[float] = None,
        category: str | None = None,
        gender: str | None = None,
        age_group: str | None = None,
        education: str | None = None,
        split: str | None = None,
        domain: str | None = None,
    ) -> Tuple[AsyncScalarResult[AudioSample], int]:
        """
        Returns a memory-efficient async stream of AudioSample records and the total count.
        """
        print(f"This is all the filter parameters {language}, {pct}, {category}, {gender}, {age_group}, {education}, {split}, {domain}")
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
        if split:
            filters.append(AudioSample.split == split)

        # Efficiently count the total matching rows without loading them
        count_query = select(func.count(AudioSample.id)).where(and_(*filters))
        total_available_result = await session.execute(count_query)
        total_available = total_available_result.scalar_one()

        if total_available == 0:
            raise ValueError("No audio samples found for the selected criteria.")

        # Determine how many records to fetch
        if pct is not None:
            if not (0 < pct <= 100):
                raise ValueError("Percentage must be between 0 and 100")
            num_to_fetch = math.ceil((pct / 100) * total_available)
        else:
            # Default to all if no percentage is given
            num_to_fetch = total_available

        # Build the main query that will be streamed
        query = (
            select(AudioSample)
            .where(and_(*filters))
            .order_by(AudioSample.id) # Consistent ordering is good practice
            .limit(num_to_fetch)
        )

        # Use session.stream_scalars to get an async iterator. This is the key change.
        result_stream = await session.stream_scalars(query)
        
        return result_stream, num_to_fetch



    async def estimate_zip_size_only(
        self,
        session: AsyncSession,
        language: str,
        pct: int | float,
        category: str = None,
        gender: GenderEnum | None = None,
        age_group: str | None = None,
        education: str | None = None,
        split: Split | None = None,
        domain: str | None = None,

        total_mb: float | None = None,
        total_bytes: float | None = None,
        total_gb: float | None = None,
        redis: Optional[Redis] = None, 
    ) -> dict:

        print(f"estimate_zip_size:{language}:{split}:{total_gb}\n\n\n")

        cache_key = f"estimate_zip_size:{language}:{split}:{total_gb}"

        # If redis available, try to get cached data
        if redis:
            cached = await redis.get(cache_key)
            if cached:
                logger.info(
                    f"Fetching from redis: {cache_key}\n\n{json.loads(cached)}\n\n"
                )
                return json.loads(cached)

        
        
        samples, total = await self.filter_core(
            session=session,
            language=language,
            category=category,
            gender=gender,
            split=split,
            age_group=age_group,
            education=education,
            domain=domain,
            pct=pct
        )

        # # -----------------------------
        # # 1. Core duration → zip size
        # # -----------------------------
        total_duration = sum(float(s.duration) for s in samples if s.duration)

        logger.info(f"\n\nThis is the total duration: {total_duration}\n\n")
        logger.info(f"\n\nThe samples: {samples[:1]}\n\n")

        # -----------------------------
        # 2. Voicing counts
        # -----------------------------
        male_voicing_count = sum(1 for s in samples if getattr(s, "gender", "").lower() == "male")
        female_voicing_count = sum(1 for s in samples if getattr(s, "gender", "").lower() == "female")

        total_voicings = male_voicing_count + female_voicing_count

        pct_male_voicings = round((male_voicing_count / total_voicings) * 100, 2) if total_voicings > 0 else 0
        pct_female_voicings = round((female_voicing_count / total_voicings) * 100, 2) if total_voicings > 0 else 0

        # -----------------------------
        # 3. Unique speakers
        # -----------------------------
        unique_male_speakers = len({s.speaker_id for s in samples if getattr(s, "gender", "").lower() == "male"})
        unique_female_speakers = len({s.speaker_id for s in samples if getattr(s, "gender", "").lower() == "female"})

        # -----------------------------
        # 4. Domain distribution
        # -----------------------------
        domain_counts = {}

        for s in samples:
            d = getattr(s, "domain", None)
            if not d:
                continue
            domain_counts[d] = domain_counts.get(d, 0) + 1

        domain_distribution = {
            d: {
                "count": count,
                "pct": round((count / total_voicings) * 100, 2)
            }
            for d, count in domain_counts.items()
        }

        # -----------------------------
        # Final enriched summary
        # -----------------------------
        result =  {
            "estimated_size__in_bytes": total_bytes,
            "estimated_size__in_mb": total_mb,
            "estimated_size_in_gb": total_gb,
            "sample_count": len(samples),
            "total_duration_seconds": round(total_duration, 2),
            "male_voicing_count": male_voicing_count,
            "female_voicing_count": female_voicing_count,
            "pct_male_voicings": pct_male_voicings,
            "pct_female_voicings": pct_female_voicings,

            "unique_male_speakers": unique_male_speakers,
            "unique_female_speakers": unique_female_speakers,

            "domain_distribution": domain_distribution,
        }

        # Save result in Redis for 72 hour
        if redis:
            await redis.set(cache_key, json.dumps(result), ex=259200)

        return result


    
    async def download_zip_from_azure(
        self,
        language: str,
        pct: float,
        session,
        split: Optional[Split] = None,
        redis: Optional[Redis] = None, 
    ):  
        cache_key = f"download_zip_from_azure:{language}:{split}:{pct}"

        # If redis available, try to get cached data
        if redis:
            cached = await redis.get(cache_key)
            if cached:
                logger.info(
                    f"Fetching from redis: {cache_key}\n\n{json.loads(cached)}\n\n"
                )
                return json.loads(cached)

        prefix = f"exports2/{language}/{split}/"
        print(f"This is the listing: {prefix}\n\n")
        logger.info(f"Listing Azure blobs under prefix: {prefix}")

        try:
            blob_list = container_client.list_blobs(name_starts_with=prefix)

            results = []

            for blob in blob_list:
                name = blob.name

                if not name.endswith(".zip"):
                    continue

                try:
                    batch_num = int(name.split("Batch_")[-1].split(".zip")[0])
                except:
                    batch_num = 99999

                sas_token = generate_blob_sas(
                    account_name=container_client.account_name,
                    container_name=container_client.container_name,
                    blob_name=name,
                    account_key=ACCOUNT_KEY,
                    permission=BlobSasPermissions(read=True),
                    expiry=datetime.utcnow() + timedelta(hours=6),
                    protocol="https",
                    version=API_VERSION
                )

                # FIX: Remove container name from path
                blob_path = name
                prefix_to_strip = f"{container_client.container_name}/"

                if blob_path.startswith(prefix_to_strip):
                    blob_path = blob_path[len(prefix_to_strip):]

                blob_name_only = PurePosixPath(name).name

                container_base_url = container_client.url
                download_url = f"{container_base_url}/{blob_path}?{sas_token}"

                results.append({
                    "key": blob_name_only,
                    "batch": batch_num,
                    "size_mb": round(blob.size / (1024 * 1024), 2),
                    "last_modified": blob.last_modified.isoformat(),
                    "download_url": await self.strip_sas_token(download_url),
                })

            if not results:
                return {"message": f"No zip files found for {language}-{split}-{pct}%"}

            results.sort(key=lambda x: x["batch"])

            response =  {
                "language": language,
                "split": split,
                "pct": pct,
                "total_batches": len(results),
                "batches": results,
            }


            # Save result in Redis for 72 hour
            if redis:
                await redis.set(cache_key, json.dumps(response), ex=259200)
            print(f"This is the: {response}")
            return response

        except Exception as e:
            logger.error(f"Azure listing/signing failed: {e}", exc_info=True)
            return {"error": str(e)}


    async def strip_sas_token(self, url: str) -> str:
        """
        Remove SAS query parameters from a blob URL.

        Args:
            url (str): Full blob URL with SAS token.

        Returns:
            str: Base blob URL without SAS token.
        """
        return url.split("?", 1)[0]
