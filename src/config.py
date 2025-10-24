from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv
from typing import Optional

load_dotenv()

class Settings(BaseSettings):
    DATABASE_URL: str
    JWT_SECRET: str
    JWT_ALGORITHM: str
    GOOGLE_CLIENT_ID: str
    GOOGLE_CLIENT_SECRET: str
    GOOGLE_REDIRECT_URI: str
    RESEND_API_KEY: str

    FRONTEND_URL: str 
    BACKEND_URL: str
    EMAIL_FROM: str


    OBS_ACCESS_KEY_ID: str
    OBS_SECRET_ACCESS_KEY: str
    OBS_ENDPOINT_URL: str
    OBS_REGION: str
    OBS_BUCKET_NAME: str = "dsn"

    S3_BUCKET_NAME: str
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_REGION: str
    AWS_ENDPOINT_URL: str

    PGDATABASE: str
    PGUSER: str
    PGPASSWORD: str
    PGHOST: str
    PGPORT: int

    REDIS_PORT: int
    REDIS_HOST: str
    REDIS_PASSWORD: str
    REDIS_USERNAME: str
    SESSION_SECRET_KEY: str


    CELERY_BROKER_URL: Optional[str] = None
    CELERY_RESULT_BACKEND: Optional[str] = None

    model_config = SettingsConfigDict(
        env_file=".env", 
        extra="ignore",
        env_ignore_existing=True,
    )


settings = Settings()


broker_connection_retry_on_startup = True
