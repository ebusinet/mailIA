"""Persistent import job tracking.

Jobs are stored as JSON files in /data/imports/jobs/<id>.json.
Uploaded files go to /data/imports/files/<id>/.
This persists across container restarts via Docker volume.
"""

import json
import logging
import os
import threading
import time
import uuid
from pathlib import Path

logger = logging.getLogger(__name__)

IMPORTS_ROOT = Path("/data/imports")
JOBS_DIR = IMPORTS_ROOT / "jobs"
FILES_DIR = IMPORTS_ROOT / "files"

# In-memory cache of active jobs for fast polling
_active_jobs: dict[str, dict] = {}
_lock = threading.Lock()


def _ensure_dirs():
    JOBS_DIR.mkdir(parents=True, exist_ok=True)
    FILES_DIR.mkdir(parents=True, exist_ok=True)


def _job_path(job_id: str) -> Path:
    return JOBS_DIR / f"{job_id}.json"


def _save_job(job: dict):
    """Persist job state to disk."""
    _ensure_dirs()
    tmp = _job_path(job["id"]).with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(job, f, ensure_ascii=False)
    tmp.rename(_job_path(job["id"]))


def _load_job(job_id: str) -> dict | None:
    path = _job_path(job_id)
    if not path.exists():
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def create_job(user_id: int, account_id: int, filename: str, source: str = "upload") -> dict:
    """Create a new import job."""
    job = {
        "id": str(uuid.uuid4())[:8],
        "user_id": user_id,
        "account_id": account_id,
        "filename": filename,
        "source": source,
        "status": "uploading",
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "progress": {
            "current": 0,
            "total": 0,
            "imported": 0,
            "skipped": 0,
            "errors": 0,
            "current_folder": "",
        },
        "folders_done": [],
        "error": None,
    }
    with _lock:
        _active_jobs[job["id"]] = job
    _save_job(job)
    return job


def update_job(job_id: str, **kwargs):
    """Update job fields and persist."""
    with _lock:
        job = _active_jobs.get(job_id)
        if not job:
            job = _load_job(job_id)
            if not job:
                return
            _active_jobs[job_id] = job
        for k, v in kwargs.items():
            if k == "progress":
                job["progress"].update(v)
            else:
                job[k] = v
        job["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    _save_job(job)


def add_folder_done(job_id: str, folder_info: dict):
    """Record a completed folder."""
    with _lock:
        job = _active_jobs.get(job_id) or _load_job(job_id)
        if not job:
            return
        _active_jobs[job_id] = job
        job["folders_done"].append(folder_info)
        job["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    _save_job(job)


def get_job(job_id: str) -> dict | None:
    """Get job status (from memory or disk)."""
    with _lock:
        job = _active_jobs.get(job_id)
        if job:
            return dict(job)
    return _load_job(job_id)


def list_jobs(user_id: int) -> list[dict]:
    """List all jobs for a user, most recent first."""
    _ensure_dirs()
    jobs = []
    for f in JOBS_DIR.glob("*.json"):
        try:
            with open(f) as fh:
                job = json.load(fh)
            if job.get("user_id") == user_id:
                jobs.append(job)
        except Exception:
            continue
    jobs.sort(key=lambda j: j.get("created_at", ""), reverse=True)
    return jobs


def get_job_file_dir(job_id: str) -> Path:
    """Get the directory for a job's uploaded files."""
    d = FILES_DIR / job_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def cleanup_job_files(job_id: str):
    """Remove uploaded files for a completed job."""
    import shutil
    d = FILES_DIR / job_id
    if d.exists():
        shutil.rmtree(d, ignore_errors=True)


def resume_interrupted_jobs():
    """Find jobs that were interrupted (status=importing) and mark them for resume."""
    _ensure_dirs()
    interrupted = []
    for f in JOBS_DIR.glob("*.json"):
        try:
            with open(f) as fh:
                job = json.load(fh)
            if job.get("status") in ("importing", "extracting", "uploading"):
                job["status"] = "interrupted"
                job["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
                with open(f, "w") as fh:
                    json.dump(job, fh, ensure_ascii=False)
                interrupted.append(job)
        except Exception:
            continue
    if interrupted:
        logger.info(f"Found {len(interrupted)} interrupted import jobs")
    return interrupted
