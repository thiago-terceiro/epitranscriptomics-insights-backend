"""
Redis caching service for improved API performance.
"""
import json
import os
import redis
from typing import Any, Optional, Dict
from datetime import timedelta
import logging

logger = logging.getLogger(__name__)

class CacheService:
    """Redis-based caching service with fallback handling."""
    
    def __init__(self):
        self.redis_host = os.getenv("REDIS_HOST", "redis")
        self.redis_port = int(os.getenv("REDIS_PORT", 6379))
        self.redis_db = int(os.getenv("REDIS_DB", 0))
        self.default_ttl = int(os.getenv("CACHE_DEFAULT_TTL", 300))  # 5 minutes
        
        try:
            self.client = redis.StrictRedis(
                host=self.redis_host,
                port=self.redis_port,
                db=self.redis_db,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
                health_check_interval=30
            )
            # Test connection
            self.client.ping()
            logger.info("Redis cache service initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Redis cache: {e}")
            self.client = None
    
    def is_available(self) -> bool:
        """Check if Redis is available."""
        if not self.client:
            return False
        try:
            self.client.ping()
            return True
        except Exception:
            return False
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set a value in cache with optional TTL."""
        if not self.is_available():
            return False
        
        try:
            serialized_value = json.dumps(value) if not isinstance(value, str) else value
            if ttl is None:
                ttl = self.default_ttl
            
            result = self.client.setex(key, ttl, serialized_value)
            return bool(result)
        except Exception as e:
            logger.warning(f"Cache set failed for key {key}: {e}")
            return False
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get a value from cache."""
        if not self.is_available():
            return default
        
        try:
            value = self.client.get(key)
            if value is None:
                return default
            
            # Try to deserialize JSON, fallback to string
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        except Exception as e:
            logger.warning(f"Cache get failed for key {key}: {e}")
            return default
    
    def delete(self, key: str) -> bool:
        """Delete a key from cache."""
        if not self.is_available():
            return False
        
        try:
            result = self.client.delete(key)
            return bool(result)
        except Exception as e:
            logger.warning(f"Cache delete failed for key {key}: {e}")
            return False
    
    def hset(self, key: str, mapping: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        """Set hash fields in cache."""
        if not self.is_available():
            return False
        
        try:
            # Serialize complex values
            serialized_mapping = {}
            for k, v in mapping.items():
                serialized_mapping[k] = json.dumps(v) if not isinstance(v, str) else v
            
            result = self.client.hset(key, mapping=serialized_mapping)
            if ttl:
                self.client.expire(key, ttl)
            return bool(result)
        except Exception as e:
            logger.warning(f"Cache hset failed for key {key}: {e}")
            return False
    
    def hget(self, key: str, field: str, default: Any = None) -> Any:
        """Get a hash field from cache."""
        if not self.is_available():
            return default
        
        try:
            value = self.client.hget(key, field)
            if value is None:
                return default
            
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        except Exception as e:
            logger.warning(f"Cache hget failed for key {key}, field {field}: {e}")
            return default
    
    def hgetall(self, key: str) -> Dict[str, Any]:
        """Get all hash fields from cache."""
        if not self.is_available():
            return {}
        
        try:
            data = self.client.hgetall(key)
            result = {}
            for k, v in data.items():
                try:
                    result[k] = json.loads(v)
                except json.JSONDecodeError:
                    result[k] = v
            return result
        except Exception as e:
            logger.warning(f"Cache hgetall failed for key {key}: {e}")
            return {}
    
    def cache_job_data(self, job_id: str, status: str, payload: str, result: Any = None) -> bool:
        """Cache job data for faster retrieval."""
        cache_data = {
            "status": status,
            "payload": payload
        }
        if result:
            cache_data["result"] = result
        
        return self.hset(f"job:{job_id}", cache_data, ttl=3600)  # 1 hour TTL
    
    def get_job_data(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get cached job data."""
        return self.hgetall(f"job:{job_id}")
    
    def invalidate_job_cache(self, job_id: str) -> bool:
        """Invalidate job cache."""
        return self.delete(f"job:{job_id}")

# Global cache instance
cache = CacheService()