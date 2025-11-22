import aiobotocore.session
from functools import partial
from aiohttp.client import ClientTimeout
from botocore.config import Config
from typing import Optional

DOWNLOAD_TIMEOUT_SECONDS = 300

def get_async_s3_client_factory(settings, is_obs: Optional[bool] = False):
    """
        Factory that returns an async S3 client creator using aiobotocore.
        It automatically routes between AWS S3 and OBS (S3-compatible) 
        depending on `settings.STORAGE_PROVIDER`.

        Example:
            async_s3_client_factory = get_async_s3_client_factory(settings)
            async with async_s3_client_factory() as s3:
                await s3.get_object(Bucket=..., Key=...)
    """

    timeout_config = ClientTimeout(total=DOWNLOAD_TIMEOUT_SECONDS)

    session = aiobotocore.session.get_session()
    # session.set_default_client_config(
    #     client_config=
    # )

    if is_obs:
        aws_access_key_id = settings.OBS_ACCESS_KEY_ID
        aws_secret_access_key = settings.OBS_SECRET_ACCESS_KEY
        endpoint_url = "https://obsv3.cn-global-1.gbbcloud.com"
        region_name = "cn-global-1"

        config_kwargs = getattr(settings, "OBS_CONFIG_KWARGS", {
            "s3": {"addressing_style": "virtual"},
            "signature_version": "s3v4",
            "max_pool_connections": 50
        })

    else:
        # Default to AWS S3
        aws_access_key_id = getattr(settings, "AWS_ACCESS_KEY_ID", None)
        aws_secret_access_key = getattr(settings, "AWS_SECRET_ACCESS_KEY", None)
        endpoint_url = getattr(settings, "AWS_ENDPOINT_URL", None)
        region_name = getattr(settings, "AWS_REGION", "us-east-1")

        config_kwargs = getattr(settings, "AWS_CONFIG_KWARGS", {
            "s3": {"addressing_style": "path"},
            "max_pool_connections": 50
        })

    config = Config(**config_kwargs)

    # Return a callable factory (async context manager)
    return partial(
        session.create_client,
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=endpoint_url,
        region_name=region_name,
        config=config
    )
