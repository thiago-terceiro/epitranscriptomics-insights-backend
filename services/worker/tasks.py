"""
Celery worker tasks for epitranscriptomics analysis pipeline.

Handles job execution with:
- Retry logic with exponential backoff
- Connection pooling for DB and Redis
- Structured logging
- MLflow experiment tracking
- Priority queues (high/default/low)
- MinIO storage cleanup tasks
"""
import os
import json
import logging
import shutil
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional, List, cast
from urllib.parse import urlparse

import httpx

from celery import Celery
from celery.schedules import crontab
from kombu import Queue
from sqlalchemy import create_engine, delete, select
from sqlalchemy.orm import sessionmaker, Session
from fastapi import FastAPI
from fastapi.middleware.trustedhost import TrustedHostMiddleware
import mlflow
import redis

from worker.services_common.db_models import Job
from worker.services_common.db_session import DATABASE_URL
from worker.runners import cheui_runner, m6anet_runner, xpore_runner, dorado_runner
from worker.notifications import publish_progress

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Celery Configuration
# -----------------------------------------------------------------------------
BROKER: str = os.getenv(
    "RABBITMQ_URL",
    f"pyamqp://{os.getenv('RABBITMQ_USER','guest')}:{os.getenv('RABBITMQ_PASS','guest')}@{os.getenv('RABBITMQ_HOST','rabbitmq')}//"
)
RESULT_BACKEND: str = os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/1")

celery_app: Celery = Celery('epimod', broker=BROKER, backend=RESULT_BACKEND)

# Production-ready Celery settings
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=7200,  # 2 hours max
    task_soft_time_limit=6900,  # 1h 55m soft limit
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    worker_disable_rate_limits=False,
    task_default_retry_delay=60,
    task_max_retries=3,
    worker_max_tasks_per_child=100,
)

# Priority queues
celery_app.conf.task_queues = (
    Queue("high_priority", routing_key="high"),
    Queue("default", routing_key="default"),
    Queue("low_priority", routing_key="low"),
    Queue("dorado", routing_key="dorado"),
)
celery_app.conf.task_default_queue = "default"
celery_app.conf.task_default_exchange = "default"
celery_app.conf.task_default_routing_key = "default"

celery_app.conf.task_routes = {
    'epimod.run_critical': {'queue': 'high_priority'},
    'epimod.run': {'queue': 'default'},
    'epimod.run_low': {'queue': 'low_priority'},
    'epimod.cleanup_stale_uploads': {'queue': 'low_priority'},
    'epimod.cleanup_expired_sessions': {'queue': 'low_priority'},
    'epimod.run_pod5': {'queue': 'dorado'},
}

# Periodic task schedule (Celery Beat)
celery_app.conf.beat_schedule = {
    'cleanup-stale-uploads-every-6-hours': {
        'task': 'epimod.cleanup_stale_uploads',
        'schedule': crontab(minute=0, hour='*/6'),  # Every 6 hours
        'args': (),
        'kwargs': {'max_age_hours': 24},
    },
    'cleanup-expired-sessions-daily': {
        'task': 'epimod.cleanup_expired_sessions',
        'schedule': crontab(minute=30, hour=3),  # Daily at 3:30 AM UTC
        'args': (),
        'kwargs': {'ttl_days': 7},
    },
}

# -----------------------------------------------------------------------------
# Database (with connection pooling)
# -----------------------------------------------------------------------------
engine = create_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    pool_recycle=3600
)
SessionLocal = sessionmaker(bind=engine)

# -----------------------------------------------------------------------------
# Redis (with connection pooling and health checks)
# -----------------------------------------------------------------------------
REDIS_HOST: str = os.getenv("REDIS_HOST", "redis")
REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
REDIS_EXPIRATION: int = int(os.getenv("REDIS_EXPIRATION", "3600"))

redis_client: redis.StrictRedis = redis.StrictRedis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
    socket_connect_timeout=5,
    socket_timeout=5,
    retry_on_timeout=True,
    health_check_interval=30
)

# -----------------------------------------------------------------------------
# Mock Mode Detection
# -----------------------------------------------------------------------------
def _env_flag(name: str, default: str = "false") -> bool:
    """Normalize boolean-like environment variables."""
    return os.getenv(name, default).lower() in {"1", "true", "yes", "on"}


PIPELINE_MOCK_MODE: bool = _env_flag("PIPELINE_MOCK_MODE", "false")


def _should_mock(analysis_section: Any, accession: str) -> bool:
    """Compute whether the current job should run in mock mode."""
    if PIPELINE_MOCK_MODE:
        return True
    if isinstance(analysis_section, dict):
        analysis_dict = cast(Dict[str, Any], analysis_section)
        if bool(analysis_dict.get("mock")):
            return True
    return accession.startswith("mock")


def _resolve_reference_path(reference_value: str, workdir: Path, mock_ref: Optional[Path]) -> Optional[Path]:
    """Return a local reference path for a job.

    Accepts a pre-existing local path or a remote HTTP(S) URL and stores the
    downloaded reference under the job workdir.
    """
    if mock_ref:
        return mock_ref

    if not reference_value:
        return None

    parsed = urlparse(reference_value)
    if parsed.scheme in {"http", "https"}:
        reference_dir = workdir / "reference"
        reference_dir.mkdir(parents=True, exist_ok=True)
        filename = Path(parsed.path).name or "reference.fa"
        local_reference = reference_dir / filename

        logger.info("Downloading reference FASTA from %s", reference_value)
        with httpx.stream("GET", reference_value, follow_redirects=True, timeout=120.0) as response:
            response.raise_for_status()
            with local_reference.open("wb") as handle:
                for chunk in response.iter_bytes():
                    if chunk:
                        handle.write(chunk)

        return local_reference

    reference_path = Path(reference_value)
    return reference_path if reference_path.exists() else None


def _has_fast5_inputs(fast5_dir: Path) -> bool:
    return any(fast5_dir.rglob("*.fast5"))


# -----------------------------------------------------------------------------
# Custom Exceptions
# -----------------------------------------------------------------------------
class JobExecutionError(Exception):
    """Custom exception for job execution errors (non-retryable)."""
    pass


# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
def update_job_status(
    job_id: str,
    status: str,
    result: Optional[Dict[str, Any]] = None,
    session: Optional[Session] = None
) -> None:
    """Update job status in database and Redis cache."""
    close_session = session is None
    if close_session:
        session = SessionLocal()

    try:
        job = session.get(Job, job_id)
        if job:
            # Use raw SQL update to avoid SQLAlchemy Column assignment issues
            update_values: Dict[str, Any] = {"status": status}
            if result:
                update_values["result"] = json.dumps(result)

            session.execute(
                Job.__table__.update()
                .where(Job.id == job_id)
                .values(**update_values)
            )
            session.commit()

            # Update Redis cache
            cache_data: Dict[str, str] = {
                "status": status,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            if result:
                cache_data["result"] = json.dumps(result)

            redis_client.hset(f"job:{job_id}", mapping=cache_data)
            redis_client.expire(f"job:{job_id}", REDIS_EXPIRATION)

            logger.info(f"Job {job_id} status updated to {status}")
    except Exception as e:
        logger.error(f"Failed to update job {job_id} status: {e}")
        if close_session:
            session.rollback()
        raise
    finally:
        if close_session:
            session.close()


# -----------------------------------------------------------------------------
# Main Task Execution
# -----------------------------------------------------------------------------
@celery_app.task(bind=True, name="epimod.run", max_retries=3, default_retry_delay=60)  # type: ignore[misc]
def run_job(self: Any, job_id: str) -> Dict[str, Any]:
    """
    Execute bioinformatics analysis job.

    Pipeline steps:
    1. Download data (SRA/MinIO/mock)
    2. Preprocessing (basecall → align → eventalign)
    3. Run modification detection models (CHEUI, m6Anet, xPore)
    4. Post-process and summarize results

    Supports automatic retries with exponential backoff for infrastructure errors.
    """
    start_time = time.time()
    session: Session = SessionLocal()

    try:
        logger.info(f"Starting job {job_id}")

        # Get job from database
        job: Optional[Job] = session.get(Job, job_id)
        if not job:
            raise JobExecutionError(f"Job {job_id} not found")

        # Parse job payload
        try:
            payload: Dict[str, Any] = json.loads(str(job.payload))
        except json.JSONDecodeError as e:
            raise JobExecutionError(f"Invalid job payload: {e}")

        # Validate required fields (reference_fasta not required in mock mode)
        accession = payload.get("accession", "")
        analysis_cfg = payload.get("analysis")
        mock_run = _should_mock(analysis_cfg, accession)

        required_fields = ["accession", "models"]
        if not mock_run:
            required_fields.append("reference_fasta")
        missing_fields = [f for f in required_fields if f not in payload]
        if missing_fields:
            raise JobExecutionError(f"Missing required fields: {missing_fields}")

        # Cache initial job metadata
        owner_value = str(getattr(job, "owner")) if getattr(job, "owner", None) else "anonymous"
        redis_client.hset(f"job:{job_id}", mapping={
            "status": "running",
            "payload": str(job.payload),
            "owner": owner_value,
        })
        redis_client.expire(f"job:{job_id}", REDIS_EXPIRATION)

        # Update status to running
        update_job_status(job_id, "running", session=session)
        publish_progress(job_id, "running", 5)

        # Create work directory
        workdir: Path = Path("/work") / job_id
        workdir.mkdir(parents=True, exist_ok=True)

        # Set up MLflow tracking
        mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))

        with mlflow.start_run(run_name=job_id):
            try:
                # Log job parameters
                mlflow.log_params({
                    "accession": accession,
                    "models": payload["models"],
                    "reference": payload.get("reference_fasta", "mock"),
                    "mock_mode": mock_run,
                })

                # -----------------------------------------------------------------
                # 1) Download data
                # -----------------------------------------------------------------
                publish_progress(job_id, "downloading", 10)
                logger.info(f"Job {job_id}: Downloading data for accession {accession}")

                from worker.pipeline.ingest import sra_download

                user_id = str(getattr(job, "owner")) if getattr(job, "owner", None) else None
                upload_session_id = payload.get("upload_session_id")

                fast5_dir, fastq, mock_ref = sra_download.download(
                    accession,
                    workdir,
                    mock=mock_run,
                    user_id=user_id,
                    upload_session_id=upload_session_id,
                )

                if not mock_run and not _has_fast5_inputs(fast5_dir):
                    raise JobExecutionError(
                        "Real model jobs require uploaded FAST5 signal files. "
                        "SRA-only and POD5-only generic /jobs submissions are not supported; "
                        "use FAST5 + FASTQ for /jobs or the dedicated /jobs/pod5 Dorado route."
                    )

                publish_progress(job_id, "preprocessing", 20)

                # -----------------------------------------------------------------
                # 2) Preprocessing: basecall → align → eventalign
                # -----------------------------------------------------------------
                ref_fa: str = payload.get("reference_fasta", "")
                ref_path = _resolve_reference_path(ref_fa, workdir, mock_ref)

                if not mock_run and ref_path is None:
                    raise JobExecutionError(
                        "reference_fasta must be a local file path inside the worker or a downloadable HTTP(S) URL"
                    )

                eventalign_txt: Optional[Path] = None

                if ref_path:
                    from worker.pipeline.preprocess import basecall, align, eventalign as ea

                    logger.info(f"Job {job_id}: Running preprocessing pipeline")

                    if mock_run or not fastq.exists() or fastq.stat().st_size == 0:
                        publish_progress(job_id, "basecalling", 30)
                        fastq, _ = basecall.basecall(fast5_dir, workdir, reference=ref_path, mock=mock_run)
                    else:
                        logger.info("Job %s: Reusing uploaded FASTQ %s", job_id, fastq)
                        publish_progress(job_id, "basecalling_skipped", 30)

                    publish_progress(job_id, "aligning", 35)
                    aligned_bam = align.align(fastq, ref_path, workdir, preset="splice", mock=mock_run)

                    publish_progress(job_id, "eventalign", 40)
                    eventalign_txt = ea.eventalign(
                        fast5_dir, fastq, aligned_bam, ref_path, workdir, rna=True, mock=mock_run
                    )

                # -----------------------------------------------------------------
                # 3) Run modification detection models
                # -----------------------------------------------------------------
                models: List[str] = payload["models"]
                results: Dict[str, Any] = {}
                model_progress_step = 40 // len(models) if models else 40
                current_progress = 45

                for model in models:
                    try:
                        logger.info(f"Job {job_id}: Running {model} analysis")
                        publish_progress(job_id, f"model_{model}_start", current_progress)

                        if model == "cheui":
                            result = cheui_runner.run(
                                fast5_dir, fastq, ref_fa, workdir,
                                eventalign_txt=eventalign_txt if not mock_run else None,
                                mock=mock_run
                            )
                        elif model == "m6anet":
                            result = m6anet_runner.run(fast5_dir, fastq, ref_fa, workdir, mock=mock_run)
                        elif model == "xpore":
                            result = xpore_runner.run(fast5_dir, ref_fa, workdir, mock=mock_run)
                        else:
                            logger.warning(f"Job {job_id}: Unknown model {model}, skipping")
                            continue

                        results[model] = result
                        current_progress += model_progress_step
                        publish_progress(job_id, f"model_{model}_done", current_progress)

                    except Exception as model_error:
                        logger.error(f"Job {job_id}: Model {model} failed: {model_error}")
                        results[model] = {"error": str(model_error)}

                # Handle xpore differential if requested
                if payload.get("analysis", {}).get("xpore_differential") and "xpore" not in results:
                    results["xpore"] = xpore_runner.run(fast5_dir, ref_fa, workdir, mock=mock_run)

                # -----------------------------------------------------------------
                # 4) Post-process and summarize
                # -----------------------------------------------------------------
                publish_progress(job_id, "postprocess", 90)
                from worker.pipeline.postprocess import summarize

                summary: Dict[str, Any] = summarize.collect(results, workdir)

                # Log artifacts to MLflow
                mlflow.log_dict(results, "model_results.json")
                mlflow.log_dict(summary, "summary_files.json")

                execution_time = time.time() - start_time
                mlflow.log_metric("execution_time_seconds", execution_time)

                # Final result
                final_result: Dict[str, Any] = {
                    "status": "succeeded",
                    "models": results,
                    "summary": summary,
                    "execution_time": execution_time,
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                }

                # Update job status
                update_job_status(job_id, "succeeded", final_result, session=session)
                publish_progress(job_id, "completed", 100)

                logger.info(f"Job {job_id} completed successfully in {execution_time:.2f}s")

                # Cleanup workdir on success
                if workdir.exists():
                    try:
                        shutil.rmtree(workdir)
                    except Exception:
                        pass

                # Schedule cleanup of upload session files (async, non-blocking)
                if upload_session_id:
                    try:
                        cleanup_job_session.delay(
                            session_id=upload_session_id,
                            user_id=user_id or "anonymous"
                        )
                        logger.info(f"Job {job_id}: Scheduled cleanup for upload session {upload_session_id}")
                    except Exception as cleanup_exc:
                        # Don't fail the job if cleanup scheduling fails
                        logger.warning(
                            f"Job {job_id}: Failed to schedule upload cleanup for {upload_session_id}: {cleanup_exc}"
                        )

                return final_result

            except Exception as execution_error:
                logger.error(f"Job {job_id} execution failed: {execution_error}")
                mlflow.log_param("error", str(execution_error))
                raise

    except JobExecutionError as job_error:
        # Don't retry on validation errors
        error_result: Dict[str, Any] = {
            "status": "failed",
            "error": str(job_error),
            "failed_at": datetime.now(timezone.utc).isoformat(),
        }
        update_job_status(job_id, "failed", error_result, session=session)
        publish_progress(job_id, "failed", 100, {"error": str(job_error)})
        logger.error(f"Job {job_id} failed with validation error: {job_error}")
        return error_result

    except Exception as exc:
        # Retry on infrastructure errors
        execution_time = time.time() - start_time

        if self.request.retries < self.max_retries:
            logger.warning(f"Job {job_id} failed (attempt {self.request.retries + 1}), retrying: {exc}")

            retry_result: Dict[str, Any] = {
                "status": "retrying",
                "error": str(exc),
                "attempt": self.request.retries + 1,
                "retry_at": (datetime.now(timezone.utc) + timedelta(seconds=self.default_retry_delay)).isoformat(),
            }
            update_job_status(job_id, "retrying", retry_result, session=session)
            publish_progress(job_id, "retrying", 0, {"error": str(exc)})

            # Exponential backoff
            retry_delay = self.default_retry_delay * (2 ** self.request.retries)
            raise self.retry(exc=exc, countdown=retry_delay)
        else:
            # Max retries exceeded
            error_result = {
                "status": "failed",
                "error": str(exc),
                "execution_time": execution_time,
                "failed_at": datetime.now(timezone.utc).isoformat(),
                "retries_exhausted": True,
            }
            update_job_status(job_id, "failed", error_result, session=session)
            publish_progress(job_id, "failed", 100, {"error": str(exc)})
            logger.error(f"Job {job_id} failed after {self.max_retries} retries: {exc}")
            return error_result

    finally:
        session.close()


# -----------------------------------------------------------------------------
# Priority Queue Task Aliases
# -----------------------------------------------------------------------------
@celery_app.task(name="epimod.run_critical", queue="high_priority", bind=True, max_retries=3)  # type: ignore[misc]
def run_critical_task(self: Any, job_id: str) -> Dict[str, Any]:
    """High priority task for critical jobs."""
    return run_job(job_id)


@celery_app.task(name="epimod.run_default", queue="default", bind=True, max_retries=3)  # type: ignore[misc]
def run_default_task(self: Any, job_id: str) -> Dict[str, Any]:
    """Default priority task."""
    return run_job(job_id)


@celery_app.task(name="epimod.run_low", queue="low_priority", bind=True, max_retries=3)  # type: ignore[misc]
def run_low_priority_task(self: Any, job_id: str) -> Dict[str, Any]:
    """Low priority task for non-urgent jobs."""
    return run_job(job_id)


# -----------------------------------------------------------------------------
# MinIO Storage Cleanup Tasks
# -----------------------------------------------------------------------------
@celery_app.task(name="epimod.cleanup_stale_uploads", queue="low_priority", bind=True, max_retries=2)  # type: ignore[misc]
def cleanup_stale_uploads(
    self: Any,
    max_age_hours: int = 24,
    dry_run: bool = False
) -> Dict[str, Any]:
    """
    Clean up incomplete multipart uploads older than max_age_hours.
    
    This task runs periodically (default: every 6 hours) to:
    1. Find all incomplete multipart uploads in MinIO
    2. Abort uploads older than the specified age (default: 24h)
    3. Free up storage space from abandoned upload attempts
    
    Args:
        max_age_hours: Abort uploads older than this many hours (default: 24)
        dry_run: If True, only report what would be cleaned up
        
    Returns:
        Dict with cleanup statistics and details
    """
    start_time = time.time()
    
    try:
        from worker.api.storage.upload_manager import UploadSession
        
        logger.info(
            f"Starting stale multipart upload cleanup (max_age={max_age_hours}h, dry_run={dry_run})"
        )
        
        result = UploadSession.abort_stale_multipart_uploads(
            max_age_hours=max_age_hours,
            prefix="uploads/",
            dry_run=dry_run
        )
        
        execution_time = time.time() - start_time
        result["execution_time_seconds"] = execution_time
        result["completed_at"] = datetime.now(timezone.utc).isoformat()
        
        logger.info(
            f"Stale upload cleanup completed: "
            f"found={result['total_found']}, aborted={result['aborted_count']}, "
            f"failed={result['failed_count']} in {execution_time:.2f}s"
        )
        
        return result
        
    except Exception as exc:
        logger.error(f"Stale upload cleanup failed: {exc}")
        
        if self.request.retries < self.max_retries:
            retry_delay = 300 * (2 ** self.request.retries)  # 5min, 10min
            raise self.retry(exc=exc, countdown=retry_delay)
        
        return {
            "status": "failed",
            "error": str(exc),
            "failed_at": datetime.now(timezone.utc).isoformat(),
        }


@celery_app.task(name="epimod.cleanup_expired_sessions", queue="low_priority", bind=True, max_retries=2)  # type: ignore[misc]
def cleanup_expired_sessions(
    self: Any,
    ttl_days: int = 7,
    dry_run: bool = False
) -> Dict[str, Any]:
    """
    Clean up expired upload sessions from PostgreSQL and their files from MinIO.
    
    This task runs periodically (default: daily at 3:30 AM) to:
    1. Find upload sessions older than ttl_days
    2. Delete associated files from MinIO
    3. Remove session records from PostgreSQL
    
    Args:
        ttl_days: Delete sessions older than this many days (default: 7)
        dry_run: If True, only report what would be cleaned up
        
    Returns:
        Dict with cleanup statistics and details
    """
    start_time = time.time()
    session: Session = SessionLocal()
    
    try:
        from worker.api.storage.upload_manager import UploadSession
        from worker.api.db.models import UploadSessionModel
        
        logger.info(
            f"Starting expired session cleanup (ttl={ttl_days} days, dry_run={dry_run})"
        )
        
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=ttl_days)
        
        # Find expired sessions
        expired_sessions = session.execute(
            select(
                UploadSessionModel.session_id,
                UploadSessionModel.user_id,
                UploadSessionModel.created_at,
                UploadSessionModel.completed_at
            ).where(
                UploadSessionModel.created_at < cutoff_date
            )
        ).fetchall()
        
        minio_cleanup_results: List[Dict[str, Any]] = []
        db_deleted_sessions: List[str] = []
        failed_sessions: List[Dict[str, str]] = []
        
        for row in expired_sessions:
            session_id = row.session_id
            user_id = row.user_id
            
            if dry_run:
                minio_cleanup_results.append({
                    "session_id": session_id,
                    "user_id": user_id,
                    "would_delete": True
                })
                db_deleted_sessions.append(session_id)
                continue
            
            try:
                # Delete files from MinIO
                minio_result = UploadSession.delete_session_files(session_id, user_id)
                minio_cleanup_results.append(minio_result)
                
                # Delete session record from PostgreSQL
                session.execute(
                    delete(UploadSessionModel).where(
                        UploadSessionModel.session_id == session_id
                    )
                )
                db_deleted_sessions.append(session_id)
                
            except Exception as exc:
                logger.error(f"Failed to cleanup session {session_id}: {exc}")
                failed_sessions.append({
                    "session_id": session_id,
                    "error": str(exc)
                })
        
        if not dry_run:
            session.commit()
        
        execution_time = time.time() - start_time
        
        result = {
            "dry_run": dry_run,
            "ttl_days": ttl_days,
            "cutoff_date": cutoff_date.isoformat(),
            "total_expired_sessions": len(expired_sessions),
            "sessions_deleted": len(db_deleted_sessions),
            "sessions_failed": len(failed_sessions),
            "deleted_session_ids": db_deleted_sessions,
            "failed_sessions": failed_sessions,
            "minio_cleanup_details": minio_cleanup_results,
            "execution_time_seconds": execution_time,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
        
        logger.info(
            f"Expired session cleanup completed: "
            f"found={len(expired_sessions)}, deleted={len(db_deleted_sessions)}, "
            f"failed={len(failed_sessions)} in {execution_time:.2f}s"
        )
        
        return result
        
    except Exception as exc:
        logger.error(f"Expired session cleanup failed: {exc}")
        session.rollback()
        
        if self.request.retries < self.max_retries:
            retry_delay = 300 * (2 ** self.request.retries)
            raise self.retry(exc=exc, countdown=retry_delay)
        
        return {
            "status": "failed",
            "error": str(exc),
            "failed_at": datetime.now(timezone.utc).isoformat(),
        }
        
    finally:
        session.close()


@celery_app.task(name="epimod.cleanup_job_session", queue="low_priority")  # type: ignore[misc]
def cleanup_job_session(
    session_id: str,
    user_id: str = "anonymous"
) -> Dict[str, Any]:
    """
    Clean up MinIO files for a specific upload session after job completion.
    
    This task can be called after a job finishes to immediately clean up
    the source upload files that are no longer needed.
    
    Args:
        session_id: The upload session ID to clean up
        user_id: The user ID who owns the session
        
    Returns:
        Dict with cleanup statistics
    """
    try:
        from worker.api.storage.upload_manager import UploadSession
        
        logger.info(f"Cleaning up session files for session_id={session_id}")
        
        result = UploadSession.delete_session_files(session_id, user_id)
        result["completed_at"] = datetime.now(timezone.utc).isoformat()
        
        logger.info(
            f"Session cleanup completed: session_id={session_id}, "
            f"deleted={result['deleted_count']}, failed={result['failed_count']}"
        )
        
        return result
        
    except Exception as exc:
        logger.error(f"Session cleanup failed for {session_id}: {exc}")
        return {
            "status": "failed",
            "session_id": session_id,
            "error": str(exc),
            "failed_at": datetime.now(timezone.utc).isoformat(),
        }


# -----------------------------------------------------------------------------
# Dorado POD5 Basecalling Task
# -----------------------------------------------------------------------------
@celery_app.task(  # type: ignore[misc]
    bind=True,
    name="epimod.run_pod5",
    queue="dorado",
    max_retries=2,
    default_retry_delay=120,
)
def run_pod5_job(self: Any, job_id: str) -> Dict[str, Any]:
    """Execute Dorado basecalling on an uploaded POD5 file.

    Expected payload shape::

        {
            "source": {
                "type": "uploaded_pod5",
                "pod5_object_name": "uploads/anonymous/session-id/file.pod5",
                "reference_object_name": "refs/hg38.fa"   # optional
            }
        }

    Pipeline:
        1. Download POD5 from MinIO to a temp dir
        2. Run Dorado → BAM with ML modification tags
        3. Upload BAM to results/{user_id}/{job_id}/output.bam
        4. Delete source POD5 (ephemeral strategy)
    """
    start_time = time.time()
    session: Session = SessionLocal()

    try:
        job: Optional[Job] = session.get(Job, job_id)
        if not job:
            raise JobExecutionError(f"Job {job_id} not found")

        try:
            payload: Dict[str, Any] = json.loads(str(job.payload))
        except json.JSONDecodeError as exc:
            raise JobExecutionError(f"Invalid job payload: {exc}")

        source = payload.get("source", {})
        if not isinstance(source, dict) or source.get("type") != "uploaded_pod5":
            raise JobExecutionError("Job payload source type is not 'uploaded_pod5'")

        pod5_object_name: Optional[str] = source.get("pod5_object_name")
        if not pod5_object_name:
            raise JobExecutionError("Missing 'pod5_object_name' in payload source")

        user_id: str = str(getattr(job, "owner") or "anonymous")
        reference_object_name: Optional[str] = source.get("reference_object_name")

        # Cache initial state
        redis_client.hset(f"job:{job_id}", mapping={
            "status": "running",
            "payload": str(job.payload),
            "owner": user_id,
        })
        redis_client.expire(f"job:{job_id}", REDIS_EXPIRATION)

        update_job_status(job_id, "running", session=session)
        publish_progress(job_id, "running", 5)

        logger.info("POD5 job %s: starting Dorado on %s", job_id, pod5_object_name)
        publish_progress(job_id, "basecalling", 20)

        result = dorado_runner.run_uploaded_pod5(
            job_id=job_id,
            pod5_object_name=pod5_object_name,
            user_id=user_id,
            reference_object_name=reference_object_name,
        )

        publish_progress(job_id, "uploading_bam", 80)

        execution_time = time.time() - start_time
        final_result: Dict[str, Any] = {
            "status": "succeeded",
            "bam_object_name": result.get("bam_object_name"),
            "bam_size_bytes": result.get("bam_size_bytes"),
            "pod5_deleted": result.get("pod5_deleted", False),
            "execution_time": execution_time,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }

        update_job_status(job_id, "succeeded", final_result, session=session)
        publish_progress(job_id, "completed", 100)
        logger.info("POD5 job %s succeeded in %.2fs", job_id, execution_time)
        return final_result

    except JobExecutionError as job_error:
        error_result: Dict[str, Any] = {
            "status": "failed",
            "error": str(job_error),
            "failed_at": datetime.now(timezone.utc).isoformat(),
        }
        update_job_status(job_id, "failed", error_result, session=session)
        publish_progress(job_id, "failed", 100, {"error": str(job_error)})
        logger.error("POD5 job %s failed (non-retryable): %s", job_id, job_error)
        return error_result

    except Exception as exc:
        if self.request.retries < self.max_retries:
            retry_delay = self.default_retry_delay * (2 ** self.request.retries)
            logger.warning(
                "POD5 job %s failed (attempt %d), retrying in %ds: %s",
                job_id, self.request.retries + 1, retry_delay, exc,
            )
            update_job_status(
                job_id,
                "retrying",
                {"status": "retrying", "error": str(exc), "attempt": self.request.retries + 1},
                session=session,
            )
            raise self.retry(exc=exc, countdown=retry_delay)

        error_result = {
            "status": "failed",
            "error": str(exc),
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "retries_exhausted": True,
        }
        update_job_status(job_id, "failed", error_result, session=session)
        publish_progress(job_id, "failed", 100, {"error": str(exc)})
        logger.error("POD5 job %s failed after %d retries: %s", job_id, self.max_retries, exc)
        return error_result

    finally:
        session.close()

app: FastAPI = FastAPI()

allowed_hosts = [
    host.strip()
    for host in os.getenv(
        "ALLOWED_HOSTS",
        "localhost,127.0.0.1,*.yourdomain.com",
    ).split(",")
    if host.strip()
]

app.add_middleware(TrustedHostMiddleware, allowed_hosts=allowed_hosts)


@app.post("/enqueue")
def enqueue(body: Dict[str, str]) -> Dict[str, str]:
    """Enqueue a job for asynchronous processing."""
    run_job.delay(body["job_id"])
    return {"queued": body["job_id"]}
