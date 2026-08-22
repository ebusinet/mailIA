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
    # A sync that hangs (slow IMAP, LLM call per email) must never hold a worker
    # slot forever — without these limits the pool silently stops syncing.
    task_soft_time_limit=1500,  # 25 min: raises SoftTimeLimitExceeded
    task_time_limit=1800,  # 30 min: hard kill of the worker process
    beat_schedule={
        "sync-all-accounts": {
            "task": "src.worker.tasks.sync_all_accounts",
            "schedule": crontab(minute="*/5"),  # every 5 minutes
            # Drop a scheduled run that beat could not deliver in time instead of
            # queueing it — otherwise missed runs pile up without bound.
            "options": {"expires": 240},
        },
    },
)

app.autodiscover_tasks(["src.worker"])


def worker_online(timeout: float = 1.0) -> bool:
    """Is at least one Celery worker consuming the queue?

    Queuing a task nobody consumes looks like success to the caller and silently
    piles up: that is how three months of sync backlog went unnoticed.
    """
    try:
        return bool(app.control.inspect(timeout=timeout).ping())
    except Exception:
        return False

