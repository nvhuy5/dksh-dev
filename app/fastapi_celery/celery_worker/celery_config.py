from celery import Celery
import config_loader
from config_loader import CELERY_RESULT_EXPIRES, CELERY_TASK_SOFT_TIME_LIMIT, CELERY_TASK_TIME_LIMIT
# Create celery_app
celery_app = Celery("File Processor")

# Celery configuration
redis_url = (
    f"redis://"
    f"{config_loader.get_env_variable('REDIS_HOST')}"
    f":{config_loader.get_env_variable('REDIS_PORT')}/0"
)

celery_app.conf.update(
    broker_url=redis_url,
    result_backend=redis_url,
    broker_connection_retry_on_startup=True,
    task_track_started=True,
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    task_serializer="json",
    worker_prefetch_multiplier=3,
    task_acks_late=True,
    result_expires=CELERY_RESULT_EXPIRES,
    task_soft_time_limit=CELERY_TASK_SOFT_TIME_LIMIT,
    task_time_limit=CELERY_TASK_TIME_LIMIT,
    task_default_retry_delay=5,
    task_max_retries=3,
    task_reject_on_worker_lost=True,
    worker_hijack_root_logger=False,
    task_retry_backoff=True,
    task_retry_backoff_max=30,
    task_retry_jitter=True,
)
