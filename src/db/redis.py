from redis.asyncio import Redis
from typing import Optional, Dict


def init_redis_client(
        REDIS_HOST,
        REDIS_PORT,
        REDIS_USERNAME,
        REDIS_PASSWORD,
) -> Redis:
    """
    Initialize a Redis client conditionally with auth credentials if provided.
    """
    redis_config: Dict = {
        "host": REDIS_HOST,
        "port": REDIS_PORT,
        "decode_responses": True,
    }

    if REDIS_USERNAME:
        redis_config["username"] = REDIS_USERNAME 

    if REDIS_PASSWORD:
        redis_config["password"] = REDIS_PASSWORD

    return Redis(**redis_config)


def make_cache_key(prefix: str, user_id: str, context: Optional[str] = None) -> str:
    return f"{prefix}:{user_id}:{context}" if context else f"{prefix}:{user_id}"






