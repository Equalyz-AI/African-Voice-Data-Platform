import base64
from azure.storage.blob.aio import BlobServiceClient
from azure.storage.blob import BlobBlock
import aiofiles, asyncio
import logging
import uuid

from src.core.main_one_config import container_client

logger = logging.getLogger(__name__)


async def multipart_upload_to_azure(local_path, container_client, blob_name):
    block_size = 64 * 1024 * 1024  # 64MB
    blocks = []

    blob_client = container_client.get_blob_client(blob_name)

    async with aiofiles.open(local_path, "rb") as f:
        part_number = 0
        while True:
            chunk = await f.read(block_size)
            if not chunk:
                break

            block_id = base64.b64encode(uuid.uuid4().bytes).decode("utf-8")
            await blob_client.stage_block(block_id=block_id, data=chunk)

            blocks.append(BlobBlock(block_id))
            part_number += 1
            logger.info(f"Uploaded block {part_number}")

    await blob_client.commit_block_list(blocks)
    logger.info(f"🎉 Multipart block upload complete: {blob_name}")





async def upload_to_azure_blob(local_path, container_name, blob_name):
    """Upload final ZIP to Azure Blob Storage."""
    try:
        blob_client = container_client.get_blob_client(blob_name)

        async with aiofiles.open(local_path, "rb") as f:
            data = await f.read()

        await blob_client.upload_blob(
            data,
            overwrite=True,
            content_type="application/zip"
        )

        logger.info(f"🎉 Uploaded to Azure Blob: {blob_name}")

    except Exception as e:
        logger.error(f"Azure upload failed: {e}")
        raise



async def upload_to_azure(container_client, local_path, blob_name):
    """Upload a local file to Azure Blob Storage asynchronously."""
    blob_client = container_client.get_blob_client(blob_name)
    
    # For async uploading, you can use a thread since Azure SDK isn't fully async for files
    logger.info(f"Uploading {local_path} to container  as {blob_name}")
    try:
        await asyncio.to_thread(
          blob_client.upload_blob,
          data=open(local_path, "rb"),
          overwrite=True
      )
        logger.info(f"Upload successful: {blob_name}")
    except Exception as e:
        logger.error(f"Upload failed for {blob_name}: {e}")
