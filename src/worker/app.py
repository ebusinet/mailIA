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
    # Measured, not guessed: a full catch-up on the real account is ~12.5 min once the
    # redundant SELECT is gone. 2700 leaves 3.6x headroom for latency spikes and heavier
    # messages; the hard limit adds 300s to unwind rather than to work.
    task_soft_time_limit=2700,  # 45 min: raises SoftTimeLimitExceeded
    task_time_limit=3000,  # 50 min: hard kill of the worker process
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

