"""
Enhanced worker task with better error handling and retry logic.
"""
import os
import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, Optional, List
from celery import Celery
from celery.exceptions import Retry
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from kombu import Queue
import mlflow
import redis
from datetime import datetime, timedelta

from .services_common.db_models import Job
from .services_common.db_session import DATABASE_URL
from .runners import cheui_runner, m6anet_runner, xpore_runner
from .notifications import publish_progress

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Broker configuration
BROKER: str = os.getenv(
    "RABBITMQ_URL",
    f"pyamqp://{os.getenv('RABBITMQ_USER','guest')}:{os.getenv('RABBITMQ_PASS','guest')}@{os.getenv('RABBITMQ_HOST','rabbitmq')}//"
)
RESULT_BACKEND: str = os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/1")

# Enhanced Celery configuration
celery_app: Celery = Celery('epimod', broker=BROKER, backend=RESULT_BACKEND)

# Celery configuration for production
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=7200,  # 2 hours
    task_soft_time_limit=6900,  # 1 hour 55 minutes
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    worker_disable_rate_limits=False,
    task_default_retry_delay=60,
    task_max_retries=3,
    worker_max_tasks_per_child=100,
)

# Database setup with connection pooling
engine = create_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    pool_recycle=3600
)
SessionLocal = sessionmaker(bind=engine)

# Initialize Redis client with connection pooling
REDIS_HOST: str = os.getenv("REDIS_HOST", "redis")
REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
redis_client: redis.StrictRedis = redis.StrictRedis(
    host=REDIS_HOST, 
    port=REDIS_PORT, 
    decode_responses=True,
    socket_connect_timeout=5,
    socket_timeout=5,
    retry_on_timeout=True,
    health_check_interval=30
)

# Set expiration time for Redis cache entries (in seconds)
REDIS_EXPIRATION: int = int(os.getenv("REDIS_EXPIRATION", "3600"))  # Default: 1 hour

# Configure Celery queues with enhanced routing
celery_app.conf.task_queues = (
    Queue("high_priority", routing_key="high", priority=10),
    Queue("default", routing_key="default", priority=5),
    Queue("low_priority", routing_key="low", priority=1),
)

# Define default routing
celery_app.conf.task_default_queue = "default"
celery_app.conf.task_default_exchange = "default"
celery_app.conf.task_default_routing_key = "default"

# Task routing
celery_app.conf.task_routes = {
    'epimod.run_priority': {'queue': 'high_priority'},
    'epimod.run': {'queue': 'default'},
    'epimod.run_batch': {'queue': 'low_priority'},
}

class JobExecutionError(Exception):
    """Custom exception for job execution errors."""
    pass

def update_job_status(job_id: str, status: str, result: Optional[Dict[str, Any]] = None, session: Optional[Session] = None) -> None:
    """Update job status in database and cache."""
    if session is None:
        session = SessionLocal()
        close_session = True
    else:
        close_session = False
    
    try:
        # Update database
        job = session.get(Job, job_id)
        if job:
            job.status = status
            if result:
                job.result = json.dumps(result)
            session.commit()
            
            # Update Redis cache
            cache_data = {
                "status": status,
                "updated_at": datetime.utcnow().isoformat(),
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

@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def run_analysis_job(self, job_id: str) -> Dict[str, Any]:
    """
    Execute bioinformatics analysis job with enhanced error handling and retries.
    """
    start_time = time.time()
    session = SessionLocal()
    
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
        
        # Validate required fields
        required_fields = ["accession", "reference_fasta", "models"]
        missing_fields = [field for field in required_fields if field not in payload]
        if missing_fields:
            raise JobExecutionError(f"Missing required fields: {missing_fields}")
        
        # Update job status to running
        update_job_status(job_id, "running", session=session)
        publish_progress(job_id, "running", 5)
        
        # Set up MLflow tracking
        mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
        
        # Create work directory
        workdir: Path = Path("/work") / job_id
        workdir.mkdir(parents=True, exist_ok=True)
        
        with mlflow.start_run(run_name=job_id):
            try:
                # Log job parameters
                mlflow.log_params({
                    "accession": payload["accession"],
                    "models": payload["models"],
                    "reference": payload["reference_fasta"]
                })
                
                # 1) Download data with progress updates
                publish_progress(job_id, "downloading", 10)
                logger.info(f"Job {job_id}: Downloading data for accession {payload['accession']}")
                
                from .pipeline.ingest import sra_download
                fast5_dir: Path
                fastq: Path
                fast5_dir, fastq = sra_download.download(payload["accession"], workdir)
                
                publish_progress(job_id, "preprocessing", 20)
                
                # 2) Process reference
                ref_fa: str = payload["reference_fasta"]
                models: List[str] = payload["models"]
                
                # 3) Run analysis models
                results: Dict[str, Any] = {}
                model_progress_step = 60 // len(models) if models else 60
                current_progress = 30
                
                for i, model in enumerate(models):
                    try:
                        logger.info(f"Job {job_id}: Running {model} analysis")
                        publish_progress(job_id, f"running_{model}", current_progress)
                        
                        if model == "m6anet":
                            result = m6anet_runner.run(fast5_dir, fastq, ref_fa, workdir)
                        elif model == "xpore":
                            result = xpore_runner.run(fast5_dir, fastq, ref_fa, workdir)
                        elif model == "cheui":
                            result = cheui_runner.run(fast5_dir, fastq, ref_fa, workdir)
                        else:
                            raise JobExecutionError(f"Unknown model: {model}")
                        
                        results[model] = result
                        mlflow.log_artifacts(str(workdir / model))
                        
                        current_progress += model_progress_step
                        publish_progress(job_id, f"completed_{model}", current_progress)
                        
                    except Exception as model_error:
                        logger.error(f"Job {job_id}: Model {model} failed: {model_error}")
                        results[model] = {"error": str(model_error)}
                        # Continue with other models
                
                # 4) Post-processing
                publish_progress(job_id, "postprocessing", 90)
                
                # Log results to MLflow
                mlflow.log_dict(results, "results.json")
                
                # Calculate execution time
                execution_time = time.time() - start_time
                mlflow.log_metric("execution_time_seconds", execution_time)
                
                # Final result
                final_result = {
                    "status": "completed",
                    "results": results,
                    "execution_time": execution_time,
                    "completed_at": datetime.utcnow().isoformat()
                }
                
                # Update job status
                update_job_status(job_id, "completed", final_result, session=session)
                publish_progress(job_id, "completed", 100)
                
                logger.info(f"Job {job_id} completed successfully in {execution_time:.2f} seconds")
                return final_result
                
            except Exception as execution_error:
                logger.error(f"Job {job_id} execution failed: {execution_error}")
                mlflow.log_param("error", str(execution_error))
                raise execution_error
                
    except JobExecutionError as job_error:
        # Don't retry on job validation errors
        error_result = {
            "status": "failed",
            "error": str(job_error),
            "failed_at": datetime.utcnow().isoformat()
        }
        update_job_status(job_id, "failed", error_result, session=session)
        logger.error(f"Job {job_id} failed with validation error: {job_error}")
        return error_result
        
    except Exception as exc:
        # Retry on infrastructure errors
        execution_time = time.time() - start_time
        
        if self.request.retries < self.max_retries:
            logger.warning(f"Job {job_id} failed (attempt {self.request.retries + 1}), retrying: {exc}")
            
            # Update status to retrying
            retry_result = {
                "status": "retrying",
                "error": str(exc),
                "attempt": self.request.retries + 1,
                "retry_at": (datetime.utcnow() + timedelta(seconds=self.default_retry_delay)).isoformat()
            }
            update_job_status(job_id, "retrying", retry_result, session=session)
            
            # Exponential backoff
            retry_delay = self.default_retry_delay * (2 ** self.request.retries)
            raise self.retry(exc=exc, countdown=retry_delay)
        else:
            # Max retries exceeded
            error_result = {
                "status": "failed",
                "error": str(exc),
                "execution_time": execution_time,
                "failed_at": datetime.utcnow().isoformat(),
                "retries_exhausted": True
            }
            update_job_status(job_id, "failed", error_result, session=session)
            logger.error(f"Job {job_id} failed after {self.max_retries} retries: {exc}")
            return error_result
    
    finally:
        session.close()

# Register task with multiple names for different priorities
celery_app.task(run_analysis_job, name="epimod.run")
celery_app.task(run_analysis_job, name="epimod.run_priority")
celery_app.task(run_analysis_job, name="epimod.run_batch")