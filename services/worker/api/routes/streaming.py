# services/api/routes/streaming.py
import os
import json
import asyncio
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
import redis.asyncio as aioredis

router = APIRouter()

REDIS_URL = os.getenv("REDIS_URL") or f"redis://{os.getenv('REDIS_HOST','redis')}:{os.getenv('REDIS_PORT',6379)}"
redis_client = None

@router.on_event("startup")
async def startup_redis():
    global redis_client
    if not redis_client:
        redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)

async def event_generator(job_id: str):
    """
    Subscribe to Redis pubsub channel for job progress.
    This generator yields SSE 'data:' frames.
    """
    channel = f"job:{job_id}:progress"
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(channel)
    try:
        # first try to send current DB job state here if desired (optional)
        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=5.0)
            if message:
                # message example: {"type":"message","pattern":None,"channel":"job:...","data":"{\"status\":\"...\"}"}
                data = message.get("data")
                if isinstance(data, (bytes, bytearray)):
                    data = data.decode()
                # yield SSE message
                yield f"data: {data}\n\n"
                # check if job finished
                try:
                    parsed = json.loads(data)
                    if parsed.get("status") in ("completed", "failed"):
                        break
                except Exception:
                    pass
            else:
                # no message -> keep connection alive with comment ping (optional)
                yield ": ping\n\n"
                await asyncio.sleep(1.0)
    finally:
        await pubsub.unsubscribe(channel)
        await pubsub.close()

@router.get("/jobs/{job_id}/stream")
async def stream_job(job_id: str):
    # Optional: validate job exists in DB; else 404
    return StreamingResponse(event_generator(job_id), media_type="text/event-stream")
