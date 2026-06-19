import asyncio
import os
import json
import logging
from contextlib import asynccontextmanager
from uuid import uuid4
from typing import Dict, Any, List, Optional, cast

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from celery import Celery

from worker.api.db.session import get_session, init_db
from worker.api.db.models import Job, UploadSessionModel
from worker.api.routes.alphafold3_routes import router as alphafold3_router
from worker.api.routes.collab_routes import router as collab_router
from worker.api.routes.streaming import router as streaming_router
from worker.api.schemas import JobCreate, JobStatus, Pod5JobRequest

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Rate limiting configuration
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
LIMITER_STORAGE_URI = os.getenv(
    "RATE_LIMIT_STORAGE_URI",
    f"redis://{REDIS_HOST}:{REDIS_PORT}/1",
)
limiter = Limiter(key_func=get_remote_address, storage_uri=LIMITER_STORAGE_URI)

@asynccontextmanager
async def app_lifespan(app: FastAPI):
    """Application startup and shutdown events."""
    # Startup: initialize DB and any other resources
    logger.info("Starting up Epimod API...")
    await init_db()
    logger.info("Database initialized")
    try:
        await redis_client.ping()
        logger.info("Redis connection established")
    except Exception as redis_error:
        logger.warning("Redis ping failed during startup: %s", redis_error)
    yield
    # Shutdown: add teardown logic here if needed
    try:
        close_method = getattr(redis_client, "aclose", None)
        if callable(close_method):
            await close_method()  # type: ignore[misc]
        else:
            await redis_client.close()
    except AttributeError:
        await redis_client.connection_pool.disconnect()
    except Exception as redis_error:
        logger.warning("Redis close failed: %s", redis_error)
    logger.info("Shutting down Epimod API...")

# Create FastAPI app with enhanced configuration
app = FastAPI(
    title="Epimod API",
    version="0.1.0",
    description="Epitranscriptomics Analysis Platform API",
    docs_url="/docs" if os.getenv("ENVIRONMENT") != "production" else None,
    redoc_url="/redoc" if os.getenv("ENVIRONMENT") != "production" else None,
    lifespan=app_lifespan
)

# Rate limit handler
async def rate_limit_handler(request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(
        status_code=429,
        content={"detail": "Rate limit exceeded"}
    )

# Rate limiting middleware
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, rate_limit_handler)

# Security middleware
allowed_hosts = [
    host.strip()
    for host in os.getenv(
        "ALLOWED_HOSTS",
        "localhost,127.0.0.1,*.yourdomain.com",
    ).split(",")
    if host.strip()
]

app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=allowed_hosts,
)

# CORS Configuration - restrictive for production
allowed_origins = os.getenv("ALLOWED_ORIGINS", "http://localhost:3000").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["Authorization", "Content-Type", "X-Requested-With"],
    expose_headers=["X-Total-Count", "X-Rate-Limit-Remaining"]
)

# Include routers
app.include_router(alphafold3_router)
app.include_router(collab_router)
app.include_router(streaming_router, prefix="/api")

# Upload routes
from worker.api.routes.upload_routes import router as upload_router
app.include_router(upload_router)

# Celery configuration
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
RABBITMQ_PASS = os.getenv('RABBITMQ_PASS', 'guest')
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')

BROKER = os.getenv(
    'RABBITMQ_URL',
    f"pyamqp://{RABBITMQ_USER}:{RABBITMQ_PASS}@{RABBITMQ_HOST}//"
)
celery_app = Celery('epimod', broker=BROKER)

# Redis configuration
redis_client = aioredis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
    socket_connect_timeout=5,
    socket_timeout=5,
    retry_on_timeout=True,
)

# Health check endpoint
@app.get("/health")
async def health_check() -> Dict[str, Any]:
    """Health check endpoint for load balancers."""
    try:
        # Check Redis connectivity
        await redis_client.ping()
        return {
            "status": "healthy",
            "service": "epimod-api",
            "version": "0.1.0",
            "checks": {
                "redis": "ok"
            }
        }
    except Exception as e:
        logger.error("Health check failed: %s", e)
        raise HTTPException(status_code=503, detail="Service unhealthy")

# Job management endpoints with rate limiting
@app.post("/jobs", response_model=Dict[str, Any])
@limiter.limit("20/minute")  # type: ignore[misc]
async def create_job(
    request: Request,
    req: JobCreate,
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    """Create a new analysis job with enhanced validation and rate limiting."""
    try:
        job_id = str(uuid4())
        owner_id = "anonymous"  # No authentication required
        
        # Enhanced payload validation
        payload_dict = req.model_dump()
        if not payload_dict.get("accession"):
            raise HTTPException(status_code=400, detail="Missing required field: accession")
        
        # Validate accession format
        accession = payload_dict["accession"]
        if len(accession) < 3 or len(accession) > 100:
            raise HTTPException(status_code=400, detail="Accession must be between 3 and 100 characters")
        
        # Validate models list
        models_raw = payload_dict.get("models", [])
        if not models_raw or not isinstance(models_raw, list):
            raise HTTPException(status_code=400, detail="At least one model must be specified")
        
        models = cast(List[str], models_raw)
        valid_models = {"cheui", "m6anet", "xpore"}
        invalid_models = set(models) - valid_models
        if invalid_models:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid models: {invalid_models}. Valid options: {valid_models}"
            )
        
        # Validate reference_fasta
        reference_fasta = payload_dict.get("reference_fasta")
        if not reference_fasta or not isinstance(reference_fasta, str):
            raise HTTPException(status_code=400, detail="reference_fasta is required")

        upload_session_id = payload_dict.get("upload_session_id")
        if upload_session_id:
            session_result = await session.execute(
                select(
                    UploadSessionModel.session_id,
                    UploadSessionModel.user_id,
                    UploadSessionModel.completed_at,
                ).where(UploadSessionModel.session_id == upload_session_id)
            )
            upload_record = session_result.mappings().first()

            if upload_record is None:
                raise HTTPException(status_code=404, detail="Upload session not found")

            if str(upload_record["user_id"]) != owner_id:
                raise HTTPException(status_code=403, detail="Upload session does not belong to user")

            if upload_record.get("completed_at") is None:
                raise HTTPException(status_code=400, detail="Upload session not completed")

            payload_dict["accession"] = f"upload:{upload_session_id}"
        
        job_payload = json.dumps(payload_dict)

        job = Job(
            id=job_id,
            status="queued",
            payload=job_payload,
            owner=owner_id,
        )
        session.add(job)
        await session.commit()
        await session.refresh(job)

        # Cache job in Redis for faster lookups
        try:
            await redis_client.hset(f"job:{job_id}", mapping={
                "status": "queued",
                "created_at": str(job.created_at),
                "payload": job_payload,
                "owner": owner_id,
            })
            await redis_client.expire(f"job:{job_id}", 3600)  # 1 hour TTL
        except Exception as redis_error:
            logger.warning("Redis caching failed for job %s: %s", job_id, redis_error)

        # Publish job to Celery broker
        try:
            await asyncio.to_thread(celery_app.send_task, "epimod.run", args=[job_id])
            logger.info("Job %s queued successfully", job_id)
        except Exception as e:
            logger.error("Failed to enqueue job %s: %s", job_id, e)
            error_payload = json.dumps({"error": f"enqueue_failed: {str(e)}"})
            await session.execute(
                update(Job)
                .where(Job.id == job_id)
                .values(
                    status="failed",
                    result=error_payload
                )
            )
            await session.commit()
            try:
                await redis_client.hset(
                    f"job:{job_id}",
                    mapping={
                        "status": "failed",
                        "result": error_payload,
                        "owner": owner_id,
                    },
                )
                await redis_client.expire(f"job:{job_id}", 3600)
            except Exception as redis_error:
                logger.warning(
                    "Redis update failed for job %s after enqueue error: %s",
                    job_id,
                    redis_error,
                )
            raise HTTPException(status_code=503, detail="Failed to enqueue job")

        return {"job_id": job_id, "status": job.status}
        
    except HTTPException as exc:
        await session.rollback()
        raise exc
    except Exception as e:
        logger.error("Unexpected error creating job: %s", e)
        await session.rollback()
        raise HTTPException(status_code=500, detail="Internal server error")


# ---------------------------------------------------------------------------
# POD5 Basecalling Job Endpoint
# ---------------------------------------------------------------------------





@app.post("/jobs/pod5", response_model=Dict[str, Any])
@limiter.limit("10/minute")  # type: ignore[misc]
async def create_pod5_job(
    request: Request,
    req: Pod5JobRequest,
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    """Submit a Dorado basecalling job for an uploaded POD5 file.

    The client must first upload the POD5 via the presigned multipart upload
    flow (``POST /upload/init`` → per-part URLs → ``POST /upload/complete``),
    then call this endpoint with the completed session ID and the MinIO object
    name of the POD5 file.

    Dorado will:
    1. Download the POD5 from MinIO.
    2. Run basecalling with modification-tag output (ML/MM for m6A).
    3. Upload the resulting BAM to ``results/{user_id}/{job_id}/output.bam``.
    4. Delete the source POD5 (ephemeral processing — no long-term raw storage).
    """
    try:
        owner_id = "anonymous"

        # Validate upload session
        session_result = await session.execute(
            select(
                UploadSessionModel.session_id,
                UploadSessionModel.user_id,
                UploadSessionModel.completed_at,
                UploadSessionModel.files,
            ).where(UploadSessionModel.session_id == req.upload_session_id)
        )
        upload_record = session_result.mappings().first()

        if upload_record is None:
            raise HTTPException(status_code=404, detail="Upload session not found")

        if str(upload_record["user_id"]) != owner_id:
            raise HTTPException(status_code=403, detail="Upload session does not belong to user")

        if upload_record.get("completed_at") is None:
            raise HTTPException(
                status_code=400,
                detail="Upload session not completed — call POST /upload/complete first",
            )

        # Verify the declared object_name is part of the session
        session_files: List[Any] = list(upload_record.get("files") or [])
        known_objects = {
            str(f.get("object_name"))
            for f in session_files
            if isinstance(f, dict)
        }
        if req.pod5_object_name not in known_objects:
            raise HTTPException(
                status_code=400,
                detail="pod5_object_name does not belong to the specified upload session",
            )

        job_id = str(uuid4())
        job_payload = json.dumps({
            "source": {
                "type": "uploaded_pod5",
                "pod5_object_name": req.pod5_object_name,
                "reference_object_name": req.reference_object_name,
                "upload_session_id": req.upload_session_id,
            }
        })

        job = Job(
            id=job_id,
            status="queued",
            payload=job_payload,
            owner=owner_id,
        )
        session.add(job)
        await session.commit()
        await session.refresh(job)

        # Cache in Redis for fast status lookups
        try:
            await redis_client.hset(f"job:{job_id}", mapping={
                "status": "queued",
                "created_at": str(job.created_at),
                "payload": job_payload,
                "owner": owner_id,
            })
            await redis_client.expire(f"job:{job_id}", 3600)
        except Exception as redis_error:
            logger.warning("Redis caching failed for pod5 job %s: %s", job_id, redis_error)

        # Dispatch to Dorado worker queue
        try:
            await asyncio.to_thread(celery_app.send_task, "epimod.run_pod5", args=[job_id])
            logger.info("POD5 job %s queued successfully", job_id)
        except Exception as enqueue_exc:
            logger.error("Failed to enqueue pod5 job %s: %s", job_id, enqueue_exc)
            error_payload = json.dumps({"error": f"enqueue_failed: {enqueue_exc}"})
            await session.execute(
                update(Job).where(Job.id == job_id).values(status="failed", result=error_payload)
            )
            await session.commit()
            raise HTTPException(status_code=503, detail="Failed to enqueue job")

        return {"job_id": job_id, "status": "queued"}

    except HTTPException:
        await session.rollback()
        raise
    except Exception as exc:
        logger.error("Unexpected error creating pod5 job: %s", exc)
        await session.rollback()
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/jobs/list")
@limiter.limit("30/minute")  # type: ignore[misc]
async def list_jobs(
    request: Request,
    session: AsyncSession = Depends(get_session),
    limit: int = 50,
    offset: int = 0
) -> Dict[str, Any]:
    """List all jobs."""
    try:
        # Query jobs from database
        stmt = (
            select(Job)
            .order_by(Job.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        result = await session.execute(stmt)
        jobs = result.scalars().all()

        jobs_list: list[Dict[str, Any]] = []
        for job in jobs:
            result_val = getattr(job, "result")
            parsed_result = json.loads(result_val) if result_val else None
            
            jobs_list.append({
                "id": str(getattr(job, "id")),
                "status": str(getattr(job, "status")),
                "created_at": str(getattr(job, "created_at")),
                "result": parsed_result
            })
        
        return {"jobs": jobs_list, "total": len(jobs_list)}
        
    except Exception as e:
        logger.error("Error listing jobs: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/jobs/{job_id}", response_model=JobStatus)
@limiter.limit("60/minute")  # type: ignore[misc]
async def get_job(
    job_id: str,
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    """Get job status and results."""
    try:
        # Check Redis cache first for better performance
        job_data = await redis_client.hgetall(f"job:{job_id}")
        if job_data:
            result_raw = job_data.get("result")
            parsed_result = json.loads(result_raw) if result_raw else None
            return JobStatus(
                id=job_id, 
                status=str(job_data.get("status")), 
                result=parsed_result
            )

        # Fallback to database if not in Redis
        job = await session.get(Job, job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
            
        job_id_val = getattr(job, "id")
        status_val = getattr(job, "status")
        result_val = getattr(job, "result")
        parsed = json.loads(result_val) if result_val else None
        
        return JobStatus(id=str(job_id_val), status=str(status_val), result=parsed)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error retrieving job %s: %s", job_id, e)
        raise HTTPException(status_code=500, detail="Internal server error")