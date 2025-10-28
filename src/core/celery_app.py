# app/celery.py
from celery import Celery
from src.config import settings


CELERY_BROKER_URL = settings.CELERY_BROKER_URL
CELERY_RESULT_BACKEND = settings.CELERY_RESULT_BACKEND

celery_app = Celery(
    'data_export_tasks',  # Name of the Celery app
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND, # Or a separate result backend
    include=['src.tasks.export_worker'] # List of modules containing tasks
)

# Optional: Configuration for timezones, etc.
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    # Configure retry behavior for transient S3 errors
    task_acks_late=True,
    worker_prefetch_multiplier=1
)