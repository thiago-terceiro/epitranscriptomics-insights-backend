import os
import json
import logging
from contextlib import asynccontextmanager
from uuid import uuid4
from typing import Dict, Any

import redis
from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from sqlalchemy.orm import Session
from celery import Celery

from db.session import get_session, init_db
from db.models import Job
from routes import auth_routes
from routes.streaming import router as streaming_router
from .schemas import JobCreate, JobStatus
from .auth import get_current_user

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Rate limiting configuration
limiter = Limiter(key_func=get_remote_address)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup and shutdown events."""
    # Startup: initialize DB and any other resources
    logger.info("Starting up Epimod API...")
    init_db()
    logger.info("Database initialized")
    yield
    # Shutdown: add teardown logic here if needed
    logger.info("Shutting down Epimod API...")

# Create FastAPI app with enhanced configuration
app = FastAPI(
    title="Epimod API",
    version="0.1.0",
    description="Epitranscriptomics Analysis Platform API",
    docs_url="/docs" if os.getenv("ENVIRONMENT") != "production" else None,
    redoc_url="/redoc" if os.getenv("ENVIRONMENT") != "production" else None,
    lifespan=lifespan
)

# Rate limiting middleware
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Security middleware
app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=os.getenv("ALLOWED_HOSTS", "localhost,127.0.0.1,*.yourdomain.com").split(",")
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
app.include_router(auth_routes.router)
app.include_router(streaming_router, prefix="/api")

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
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
redis_client = redis.StrictRedis(
    host=REDIS_HOST, 
    port=REDIS_PORT, 
    decode_responses=True,
    socket_connect_timeout=5,
    socket_timeout=5,
    retry_on_timeout=True
)

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint for load balancers."""
    try:
        # Check Redis connectivity
        redis_client.ping()
        return {
            "status": "healthy",
            "service": "epimod-api",
            "version": "0.1.0",
            "checks": {
                "redis": "ok"
            }
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unhealthy")

# Job management endpoints with rate limiting
@app.post("/jobs", response_model=Dict[str, Any])
# @limiter.limit("10/minute")  # Uncomment when slowapi is available
async def create_job(
    # request: Request,  # Uncomment when slowapi is available
    req: JobCreate, 
    session: Session = Depends(get_session),
    # current_user = Depends(get_current_user)  # Uncomment to require auth
) -> Dict[str, Any]:
    """Create a new analysis job."""
    try:
        job_id = str(uuid4())
        
        # Enhanced payload validation
        payload_dict = req.model_dump()
        if not payload_dict.get("accession"):
            raise HTTPException(status_code=400, detail="Missing required field: accession")
        
        job = Job(
            id=job_id, 
            status="queued", 
            payload=req.model_dump_json(),
            # owner=current_user.username  # Uncomment when auth is enabled
        )
        session.add(job)
        session.commit()

        # Cache job in Redis for faster lookups
        try:
            redis_client.hset(f"job:{job_id}", mapping={
                "status": "queued",
                "created_at": str(job.created_at),
                "payload": req.model_dump_json()
            })
            redis_client.expire(f"job:{job_id}", 3600)  # 1 hour TTL
        except Exception as redis_error:
            logger.warning(f"Redis caching failed for job {job_id}: {redis_error}")

        # Publish job to Celery broker
        try:
            celery_app.send_task("epimod.run", args=[job_id])
            logger.info(f"Job {job_id} queued successfully")
        except Exception as e:
            logger.error(f"Failed to enqueue job {job_id}: {e}")
            session.query(Job).filter(Job.id == job_id).update({
                Job.status: "failed",
                Job.result: json.dumps({"error": f"enqueue_failed: {str(e)}"})
            })
            session.commit()
            raise HTTPException(status_code=503, detail="Failed to enqueue job")

        return {"job_id": job_id, "status": job.status}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error creating job: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/jobs/{job_id}", response_model=JobStatus)
# @limiter.limit("30/minute")  # Uncomment when slowapi is available
async def get_job(
    job_id: str, 
    # request: Request,  # Uncomment when slowapi is available
    session: Session = Depends(get_session)
):
    """Get job status and results."""
    try:
        # Check Redis cache first for better performance
        job_data = redis_client.hgetall(f"job:{job_id}")
        if job_data:
            result_raw = job_data.get("result")
            parsed_result = json.loads(result_raw) if result_raw else None
            return JobStatus(
                id=job_id, 
                status=str(job_data.get("status")), 
                result=parsed_result
            )

        # Fallback to database if not in Redis
        job = session.get(Job, job_id)
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
        logger.error(f"Error retrieving job {job_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")