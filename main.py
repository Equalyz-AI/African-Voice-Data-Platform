from fastapi import FastAPI
from src.auth.routes import auth_router
from src.auth.google import google_login
from src.admin.routes import admin_router
from src.routes.routes import download_router
from src.routes.celery import  celery_router
from src.middleware import register_middleware
from src.errors import register_all_errors
import uvicorn, os
from src.db.db import create_tables
from contextlib import asynccontextmanager
from redis.asyncio import Redis
from fastapi.requests import Request
from typing import cast
# from src.db.redis import init_redis_client
from src.config import settings
from src.logging_config import setup_logging
setup_logging()



version = "v1"

description = """
A REST API for downloading African voices data.
"""

version_prefix = f"/api/{version}"



@asynccontextmanager
async def lifespan(app: FastAPI):

    try:
        # app.state.redis = await init_redis_client(
        #     settings.REDIS_HOST,
        #     settings.REDIS_PORT,
        #     settings.REDIS_USERNAME,
        #     settings.REDIS_PASSWORD,
        # )
        
        # redis = cast(Redis, app.state.redis)
        # await redis.set("somekey", "Redis is working")
        # await redis.flushdb()
        pass
    except Exception as e:
        print(f"Error connecting to Redis: {e}")

    await create_tables()
    yield


app = FastAPI(
    lifespan=lifespan,
    title="African Voices API",
    description=description,
    version=version,
    license_info={"name": "MIT License", "url": "https://opensource.org/license/mit"},
    contact={
        "name": "EqualyzAI",
        "url": "https://equalyz.ai/",
        "email": "uche@equalyz.ai",
    },
    terms_of_service="https://equalyz.ai/about-us/",
    openapi_url=f"{version_prefix}/openapi.json",
    docs_url=f"{version_prefix}/docs",
    redoc_url=f"{version_prefix}/redoc"
)


# Register error handlers and middleware
register_all_errors(app)
register_middleware(app)



@app.get("/")
async def root():
    return {"message": "Welcome to Afrocan Voices API"}


@app.get("/debug")
async def debug_redis(request: Request):
    """Test Redis connection."""
    redis = cast(Redis, request.app.state.redis)
    val = await redis.get("somekey")
    return {"val": val}

# Include authentication router
app.include_router(
    auth_router, 
    prefix=f"{version_prefix}/auth", 
    tags=["Auth"]
)

app.include_router(
    google_login, 
    prefix=f"{version_prefix}/auth", 
    tags=["Google"]
)


app.include_router(
    admin_router,
    prefix=f"{version_prefix}/admin",
    tags=["Admin"]
)

app.include_router(
    download_router,
    prefix=f"{version_prefix}/download",
    tags=["Download"]
)

app.include_router(
    celery_router,
    prefix=f"{version_prefix}/celery",
    tags=["Celery"]
)



# if __name__ == "__main__":
#     ENV = os.getenv("ENV", "development")
#     PORT = int(os.getenv("PORT", 8000))
#     HOST = "localhost" if ENV == "production" else "localhost"

#     uvicorn.run(
#         app="main:app",
#         host="localhost",
#         port=PORT,
#         reload=True if ENV == "development" else False,
#         proxy_headers=True
#     )


if __name__ == "__main__":
    import os
    import platform
    import uvicorn

    ENV = os.getenv("ENV", "development")
    PORT = int(os.getenv("PORT", 8000))
    HOST = "localhost"

    run_kwargs = {
        "app": "main:app",
        "host": HOST,
        "port": PORT,
        "reload": ENV == "development",
        "proxy_headers": True,
    }

    # Only add uvloop/httptools on non-Windows systems
    if platform.system() != "Windows":
        run_kwargs.update({
            "loop": "uvloop",
            "http": "httptools"
        })

    uvicorn.run(**run_kwargs)
