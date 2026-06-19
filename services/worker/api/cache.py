"""Redis caching helpers using asyncio-compatible client."""
import json
import logging
import os
from typing import Any, Optional, Dict

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

class CacheService:
    """Redis-based caching service with fallback handling."""
    
    def __init__(self):
        self.redis_host = os.getenv("REDIS_HOST", "redis")
        self.redis_port = int(os.getenv("REDIS_PORT", 6379))
        self.redis_db = int(os.getenv("REDIS_DB", 0))
        self.default_ttl = int(os.getenv("CACHE_DEFAULT_TTL", 300))  # 5 minutes
        
        try:
            self.client = aioredis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                db=self.redis_db,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
                health_check_interval=30,
            )
        except Exception as e:
            logger.error("Failed to initialize Redis cache: %s", e)
            self.client = None

    async def is_available(self) -> bool:
        """Check if Redis is reachable."""
        if not self.client:
            return False
        try:
            await self.client.ping()
            return True
        except Exception:
            return False

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set a value in cache with optional TTL."""
        if not self.client:
            return False

        try:
            serialized_value = json.dumps(value) if not isinstance(value, str) else value
            ttl = ttl or self.default_ttl
            result = await self.client.setex(key, ttl, serialized_value)
            return bool(result)
        except Exception as e:
            logger.warning("Cache set failed for key %s: %s", key, e)
            return False

    async def get(self, key: str, default: Any = None) -> Any:
        """Get a value from cache."""
        if not self.client:
            return default

        try:
            value = await self.client.get(key)
            if value is None:
                return default

            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        except Exception as e:
            logger.warning("Cache get failed for key %s: %s", key, e)
            return default

    async def delete(self, key: str) -> bool:
        """Delete a key from cache."""
        if not self.client:
            return False

        try:
            result = await self.client.delete(key)
            return bool(result)
        except Exception as e:
            logger.warning("Cache delete failed for key %s: %s", key, e)
            return False

    async def hset(self, key: str, mapping: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        """Set hash fields in cache."""
        if not self.client:
            return False

        try:
            serialized_mapping = {
                field: json.dumps(value) if not isinstance(value, str) else value
                for field, value in mapping.items()
            }

            result = await self.client.hset(
                key,
                mapping=serialized_mapping,  # type: ignore[arg-type]
            )
            if ttl:
                await self.client.expire(key, ttl)
            return bool(result)
        except Exception as e:
            logger.warning("Cache hset failed for key %s: %s", key, e)
            return False

    async def hget(self, key: str, field: str, default: Any = None) -> Any:
        """Get a hash field from cache."""
        if not self.client:
            return default

        try:
            value = await self.client.hget(key, field)
            if value is None:
                return default

            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        except Exception as e:
            logger.warning("Cache hget failed for key %s, field %s: %s", key, field, e)
            return default

    async def hgetall(self, key: str) -> Dict[str, Any]:
        """Get all hash fields from cache."""
        if not self.client:
            return {}

        try:
            data = await self.client.hgetall(key)
            parsed: Dict[str, Any] = {}
            for field, value in data.items():
                try:
                    parsed[field] = json.loads(value)
                except json.JSONDecodeError:
                    parsed[field] = value
            return parsed
        except Exception as e:
            logger.warning("Cache hgetall failed for key %s: %s", key, e)
            return {}

    async def cache_job_data(self, job_id: str, status: str, payload: str, result: Any = None) -> bool:
        """Cache job data for faster retrieval."""
        cache_data = {
            "status": status,
            "payload": payload
        }
        if result:
            cache_data["result"] = result

        return await self.hset(f"job:{job_id}", cache_data, ttl=3600)

    async def get_job_data(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get cached job data."""
        data = await self.hgetall(f"job:{job_id}")
        return data if data else None

    async def invalidate_job_cache(self, job_id: str) -> bool:
        """Invalidate job cache."""
        return await self.delete(f"job:{job_id}")

# Global cache instance
cache = CacheService()
