import os
import json
from pathlib import Path
from typing import Any, Dict, Optional, List
from celery import Celery
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from kombu import Queue
import mlflow
import redis
from fastapi import FastAPI

from .services_common.db_models import Job
from .services_common.db_session import DATABASE_URL
from .runners import cheui_runner, m6anet_runner, xpore_runner
from .notifications import publish_progress

# Broker configuration
BROKER: str = os.getenv(
    "RABBITMQ_URL",
    f"pyamqp://{os.getenv('RABBITMQ_USER','guest')}:{os.getenv('RABBITMQ_PASS','guest')}@{os.getenv('RABBITMQ_HOST','rabbitmq')}//"
)
RESULT_BACKEND: None = None
celery_app: Celery = Celery('epimod', broker=BROKER, backend=RESULT_BACKEND)

# Database setup
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

# Initialize Redis client
REDIS_HOST: str = os.getenv("REDIS_HOST", "redis")
REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
redis_client: redis.StrictRedis = redis.StrictRedis(
    host=REDIS_HOST, 
    port=REDIS_PORT, 
    decode_responses=True
)

# Set expiration time for Redis cache entries (in seconds)
REDIS_EXPIRATION: int = int(os.getenv("REDIS_EXPIRATION", "3600"))  # Default: 1 hour

# Configure Celery queues
celery_app.conf.task_queues = (
    Queue("high_priority", routing_key="high"),
    Queue("default", routing_key="default"),
    Queue("low_priority", routing_key="low"),
)

# Define default routing
celery_app.conf.task_default_queue = "default"
celery_app.conf.task_default_exchange = "default"
celery_app.conf.task_default_routing_key = "default"


def _execute_job(job_id: str, session: Session) -> None:
    """Common job execution logic."""
    job: Optional[Job] = session.get(Job, job_id)
    if not job:
        return

    # Cache job metadata in Redis with expiration
    # Convert SQLAlchemy Column values to strings for Redis
    redis_client.hset(f"job:{job_id}", mapping={
        "status": str(job.status),
        "payload": str(job.payload)
    })
    redis_client.expire(f"job:{job_id}", REDIS_EXPIRATION)

    payload: Dict[str, Any] = json.loads(str(job.payload))
    workdir: Path = Path("/work") / job_id
    workdir.mkdir(parents=True, exist_ok=True)

    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    
    with mlflow.start_run(run_name=job_id):
        try:
            publish_progress(job_id, "queued", 5)
            
            # 1) Download data
            publish_progress(job_id, "downloading", 10)
            from pipeline.ingest import sra_download
            fast5_dir: Path
            fastq: Path
            fast5_dir, fastq = sra_download.download(payload["accession"], workdir)
            
            publish_progress(job_id, "preprocessing", 20)

            # 2) Index/reference
            ref_fa: str = payload["reference_fasta"]

            # Optional conversion and basecalling
            publish_progress(job_id, "convert_slow5", 30)
            # convert if possible...
            publish_progress(job_id, "basecalling", 40)
            # basecall if needed...

            # 3) Run models
            results: Dict[str, Any] = {}
            models: List[str] = payload["models"]
            
            if "cheui" in models:
                publish_progress(job_id, "model_cheui_start", 45)
                results["cheui"] = cheui_runner.run(fast5_dir, fastq, ref_fa, workdir)
                publish_progress(job_id, "model_cheui_done", 65)
                
            if "m6anet" in models:
                publish_progress(job_id, "model_m6anet_start", 66)
                results["m6anet"] = m6anet_runner.run(fast5_dir, fastq, ref_fa, workdir)
                publish_progress(job_id, "model_m6anet_done", 85)
                
            if payload.get("analysis", {}).get("xpore_differential"):
                results["xpore"] = xpore_runner.run(fast5_dir, ref_fa, workdir)

            # 4) Summarize/normalize
            publish_progress(job_id, "postprocess", 90)
            from pipeline.postprocess import summarize
            summary: Dict[str, Any] = summarize.collect(results, workdir) 

            # Update job status - assign to underlying column value
            session.execute(
                Job.__table__.update()
                .where(Job.id == job_id)
                .values(status="succeeded", result=json.dumps(summary))
            )
            
            redis_client.hset(f"job:{job_id}", "status", "succeeded")
            redis_client.hset(f"job:{job_id}", "result", json.dumps(summary))
            redis_client.expire(f"job:{job_id}", REDIS_EXPIRATION)
            
            publish_progress(job_id, "completed", 100)
            session.commit()
            
        except Exception as e:
            # Update job status - assign to underlying column value
            session.execute(
                Job.__table__.update()
                .where(Job.id == job_id)
                .values(status="failed", result=json.dumps({"error": str(e)}))
            )
            
            redis_client.hset(f"job:{job_id}", "status", "failed")
            redis_client.hset(f"job:{job_id}", "result", json.dumps({"error": str(e)}))
            redis_client.expire(f"job:{job_id}", REDIS_EXPIRATION)
            publish_progress(job_id, "failed", 100, {"error": str(e)})
            session.commit()
            raise
        finally:
            session.close()


@celery_app.task(name="epimod.run")  # type: ignore[misc]
def run_job(job_id: str) -> None:
    """Default task for running jobs."""
    session: Session = SessionLocal()
    _execute_job(job_id, session)


@celery_app.task(name="epimod.run_critical", queue="high_priority")  # type: ignore[misc]
def run_critical_task(job_id: str) -> None:
    """High priority task for critical jobs."""
    session: Session = SessionLocal()
    _execute_job(job_id, session)


@celery_app.task(name="epimod.run_default", queue="default")  # type: ignore[misc]
def run_default_task(job_id: str) -> None:
    """Default priority task."""
    session: Session = SessionLocal()
    _execute_job(job_id, session)


@celery_app.task(name="epimod.run_low", queue="low_priority")  # type: ignore[misc]
def run_low_priority_task(job_id: str) -> None:
    """Low priority task for non-urgent jobs."""
    session: Session = SessionLocal()
    _execute_job(job_id, session)


# Lightweight enqueue shim for local compose
app: FastAPI = FastAPI()


@app.post("/enqueue")
def enqueue(body: Dict[str, str]) -> Dict[str, str]:
    """Enqueue a job for asynchronous processing."""
    run_job.delay(body["job_id"])
    return {"queued": body["job_id"]}