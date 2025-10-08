# services/worker/notifications.py
import os
import json
import redis  # sync library for Celery tasks

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

_redis: redis.Redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True, socket_timeout=5)

from typing import Optional, Dict, Any

def publish_progress(job_id: str, status: str, progress: int, extra: Optional[Dict[str, Any]] = None) -> None:
    """
    Publish a small JSON message to a Redis pubsub channel for the job.
    Channel name: job:{job_id}:progress
    """
    channel = f"job:{job_id}:progress"
    payload: dict[str, str | int] = {"status": status, "progress": int(progress)}
    if extra:
        payload.update(extra)
    message: str = json.dumps(payload)
    _redis.publish(channel, message)
