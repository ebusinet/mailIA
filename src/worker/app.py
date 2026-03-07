from celery import Celery
from celery.schedules import crontab
from src.config import get_settings

settings = get_settings()

app = Celery("mailia", broker=settings.celery_broker_url)

app.conf.update(
    result_backend=settings.redis_url,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="Europe/Paris",
    beat_schedule={
        "sync-all-accounts": {
            "task": "src.worker.tasks.sync_all_accounts",
            "schedule": crontab(minute="*/5"),  # every 5 minutes
        },
    },
)

app.autodiscover_tasks(["src.worker"])
