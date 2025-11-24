from datetime import datetime, timedelta
import os
from re import split
from azure.storage.blob import BlobSasPermissions, generate_blob_sas
from fastapi import HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse
from sqlmodel import select, and_
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional, Tuple
import math
from pathlib import PurePosixPath
from botocore.exceptions import NoCredentialsError
from sqlalchemy import select, and_, func
from sqlalchemy.ext.asyncio import AsyncScalarResult
import logging

from src.core.main_one_config import ACCOUNT_KEY, API_VERSION, container_client
from src.db.models import AudioSample, DownloadLog, GenderEnum
from src.auth.schemas import TokenUser
from src.config import settings
from src.download.s3_config import (
    SUPPORTED_LANGUAGES,
    generate_obs_signed_url,
    s3_aws,
)
from src.download.utils import (
    stream_zip_with_metadata,
    generate_metadata_buffer,
    generate_readme,
    stream_zip_to_s3,
)
import aioboto3

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
                # "transcript_url_obs": map_sentence_id_to_transcript_obs(s.sentence_id, s.language, s.category, s.sentence),
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
            .order_by(AudioSample.id)
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
        split: str | None = None,
        domain: str | None = None,
    ) -> dict:
        """
        Estimate total dataset ZIP size using durations instead of actual file sizes.
        """
        # Reuse your filter logic
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

        total_duration = sum(float(s.duration) for s in samples if s.duration)

        # Compute total size in bytes based on PCM WAV assumption
        bytes_per_sample = AUDIO_BIT_DEPTH / 8
        total_bytes = total_duration * AUDIO_SAMPLE_RATE * bytes_per_sample * AUDIO_CHANNELS

        # Apply compression ratio
        estimated_zip_bytes = total_bytes * COMPRESSION_RATIO

        return {
            "estimated_size_bytes": int(estimated_zip_bytes),
            "estimated_size_mb": round(estimated_zip_bytes / (1024 ** 2), 2),
            "sample_count": len(samples),
            "total_duration_seconds": round(total_duration, 2)
        }



    async def download_zip_with_metadata_s3(
        self,
        language: str,
        pct: int | float,
        session: AsyncSession,
        background_tasks: BackgroundTasks,
        current_user: TokenUser,
        category: str = None,
        gender: GenderEnum | None = None,
        split: str | None = None,
        age_group: str | None = None,
        education: str | None = None,
        domain: str | None = None,
        as_excel: bool = True,
    ):
        # 1. Fetch samples
        samples, total = await self.filter_core(
            session=session,
            language=language,
            category=category,
            gender=gender,
            age_group=age_group,
            education=education,
            split=split,
            domain=domain,
            pct=pct
        )

        if not samples:
            raise HTTPException(404, "No audio samples found for selected filters")

        print("the total number of samples is ", total)
        # 2. Log download
        background_tasks.add_task(
            session.add,
            DownloadLog(
                user_id=current_user.id,
                dataset_id=samples[0].dataset_id,
                percentage=pct,
            ),
        )
        await session.commit()

        return await stream_zip_to_s3(
            language=language,
            samples=samples,
            as_excel=as_excel
        )
        

    async def download_zip_with_from_s3(
        self,
        language: str,
        pct: float,
        session,
        split: Optional[str] = None,
    ):
        """
        Returns a list of signed S3 URLs for zip batches matching the pattern:
        exports/{language}_{split}_{pct}%_batch[i].zip
        """

        prefix = f"exports/{language}_{split}_{pct}%_batch"
        logger.info(f"Listing zips from bucket={self.s3_bucket_name}, prefix={prefix}")

        try:
            resp = s3_aws.list_objects_v2(Bucket=self.s3_bucket_name, Prefix=prefix)
            files = resp.get("Contents", [])
            if not files:
                return {"message": f"No zip files found for {language}-{split}-{pct}%"}

            results = []
            for obj in files:
                key = obj["Key"]
                if key.endswith(".zip"):
                    signed_url = s3_aws.generate_presigned_url(
                        "get_object",
                        Params={"Bucket": self.s3_bucket_name, "Key": key},
                        ExpiresIn=3600 * 6,  # 6 hours expiry
                    )
                    results.append({
                        "key": key,
                        "size_mb": round(obj["Size"] / (1024 * 1024), 2),
                        "last_modified": obj["LastModified"].isoformat(),
                        "download_url": signed_url,
                    })

            # Sort by batch number
            results.sort(key=lambda x: int(x["key"].split("batch[")[-1].split("]")[0]))

            return {
                "language": language,
                "split": split,
                "pct": pct,
                "total_batches": len(results),
                "batches": results,
            }

        except Exception as e:
            logger.error(f"Failed to list or sign zips: {e}", exc_info=True)
            return {"error": str(e)}

    

    
    async def download_zip_from_azure(
        self,
        language: str,
        pct: float,
        session,
        split: Optional[str] = None,
    ):
        """
        Returns signed Azure URLs for zip batches stored under:
            exports/{language}/{split}/Batch_X.zip
        """

        prefix = f"exports/{language}/{split}/"
        logger.info(f"Listing Azure blobs under prefix: {prefix}")

        try:
            # NORMAL SYNC ITERATOR
            blob_list = container_client.list_blobs(name_starts_with=prefix)

            results = []

            for blob in blob_list:   # ✅ FIXED
                name = blob.name

                if not name.endswith(".zip"):
                    continue

                # Extract batch number
                try:
                    batch_num = int(name.split("Batch_")[-1].split(".zip")[0])
                except:
                    batch_num = 99999

                # Generate SAS token
                sas_token = generate_blob_sas(
                    account_name=container_client.account_name,
                    container_name=container_client.container_name,
                    blob_name=name,
                    account_key=ACCOUNT_KEY,
                    permission=BlobSasPermissions(read=True),
                    expiry=datetime.utcnow() + timedelta(hours=6),
                    protocol="https", 
                    version="2019-07-07"
                )

                download_url = f"{container_client.url}/{name}?{sas_token}"

                blob_name_only = PurePosixPath(name).name

                results.append({
                    "key": blob_name_only,
                    "batch": batch_num,
                    "size_mb": round(blob.size / (1024 * 1024), 2),
                    "last_modified": blob.last_modified.isoformat(),
                    "download_url": download_url,
                })

            if not results:
                return {"message": f"No zip files found for {language}-{split}-{pct}%"}

            # Sort by batch
            results.sort(key=lambda x: x["batch"])

            return {
                "language": language,
                "split": split,
                "pct": pct,
                "total_batches": len(results),
                "batches": results,
            }

        except Exception as e:
            logger.error(f"Azure listing/signing failed: {e}", exc_info=True)
            return {"error": str(e)}