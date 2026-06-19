"""AlphaFold3 client integration layer.

This module abstracts calls to an external AlphaFold3 service. It supports two
modes:

1. Real mode — forward requests to an upstream API defined via environment
   variables.
2. Mock mode — synthesize deterministic responses so the backend can be tested
   locally without the heavy AlphaFold3 stack.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, cast
from uuid import uuid4

import httpx
import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


def _env_flag(name: str, default: str = "false") -> bool:
    """Normalize boolean-like environment variables."""
    return os.getenv(name, default).lower() in {"1", "true", "yes", "on"}


class AlphaFold3Client:
    """Client wrapper around a remote AlphaFold3 prediction service."""

    def __init__(self) -> None:
        self.base_url: Optional[str] = os.getenv("ALPHAFOLD3_API_URL")
        self.api_key: Optional[str] = os.getenv("ALPHAFOLD3_API_KEY")
        self.timeout: float = float(os.getenv("ALPHAFOLD3_TIMEOUT", "60"))
        self.mock_mode: bool = _env_flag("ALPHAFOLD3_MOCK_MODE", "true") or not self.base_url

        redis_host = os.getenv("REDIS_HOST", "redis")
        redis_port = int(os.getenv("REDIS_PORT", "6379"))
        self._redis: aioredis.Redis[str] = aioredis.Redis(
            host=redis_host,
            port=redis_port,
            decode_responses=True,
        )

    async def submit(self, sequence: str, name: Optional[str] = None) -> Dict[str, Any]:
        """Submit a prediction request.

        Returns a payload that mirrors the AlphaFold3 job metadata contract used by
        the frontend. In mock mode we create a synthetic job and mark it as
        completed after a short delay.
        """

        target_name = name.strip() if name else None

        if self.mock_mode:
            job_id = str(uuid4())
            created_at = datetime.now(timezone.utc).isoformat()
            metadata: Dict[str, Any] = {
                "job_id": job_id,
                "id": job_id,
                "status": "queued",
                "target_name": target_name or f"AlphaFold3 mock target ({len(sequence)} aa)",
                "sequence_length": len(sequence),
                "created_at": created_at,
            }

            key = f"alphafold3:job:{job_id}"
            cache_map: Dict[str, str] = {
                "id": metadata["id"],
                "status": metadata["status"],
                "target_name": metadata["target_name"],
                "sequence_length": str(metadata["sequence_length"]),
                "created_at": metadata["created_at"],
                "sequence": sequence,
            }
            await self._redis.hset(key, mapping=cache_map)  # type: ignore[arg-type]
            await self._redis.expire(key, 3600)
            asyncio.create_task(self._complete_mock_job(job_id))
            return {"result": metadata}

        if not self.base_url:
            raise RuntimeError("AlphaFold3 API URL is not configured")

        payload: Dict[str, Any] = {"sequence": sequence}
        if target_name:
            payload["name"] = target_name

        headers: Dict[str, str] = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        url = self._build_url("predict")
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()

        # Optionally cache job metadata for faster lookups if an ID is present.
        if not isinstance(data, dict):
            raise ValueError("AlphaFold3 predict response must be a JSON object")

        data_dict: Dict[str, Any] = cast(Dict[str, Any], data)
        result_data_value = data_dict.get("result")
        if not isinstance(result_data_value, dict):
            return data_dict

        result_data: Dict[str, Any] = cast(Dict[str, Any], result_data_value)
        job_id_value = result_data.get("job_id")
        job_id = str(job_id_value) if job_id_value is not None else None
        if job_id:
            key = f"alphafold3:job:{job_id}"
            cache_map: Dict[str, str] = {
                "id": str(result_data.get("id", job_id)),
                "status": str(result_data.get("status", "submitted")),
                "target_name": str(result_data.get("target_name", target_name or "AlphaFold3 target")),
                "sequence_length": str(result_data.get("sequence_length", len(sequence))),
                "created_at": str(result_data.get("created_at", datetime.now(timezone.utc).isoformat())),
            }
            outputs = result_data.get("outputs")
            if outputs:
                cache_map["outputs"] = json.dumps(outputs)
            await self._redis.hset(key, mapping=cache_map)  # type: ignore[arg-type]
            await self._redis.expire(key, 3600)

        return data_dict

    async def get_job(self, job_id: str) -> Dict[str, Any]:
        """Fetch the job status and (if available) the result."""

        key = f"alphafold3:job:{job_id}"
        cached = await self._redis.hgetall(key)
        if cached:
            status = cached.get("status", "unknown")
            payload: Dict[str, Any] = {
                "job_id": job_id,
                "status": status,
                "target_name": cached.get("target_name"),
                "sequence_length": int(cached.get("sequence_length", "0")),
                "created_at": cached.get("created_at"),
            }
            structure = cached.get("structure")
            if structure:
                payload["structure"] = json.loads(structure)
            return {"result": payload}

        if self.mock_mode:
            raise KeyError(f"No mock AlphaFold3 job found for id={job_id}")

        url = self._build_url(f"jobs/{job_id}")
        headers: Dict[str, str] = {"Accept": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(url, headers=headers)
            if response.status_code == 404:
                raise KeyError(f"AlphaFold3 job {job_id} not found") from None
            response.raise_for_status()
            return response.json()

    async def list_jobs(self, limit: int = 50) -> Dict[str, Any]:
        """Return recent AlphaFold3 jobs."""

        limit = max(1, min(limit, 200))

        if not self.mock_mode and self.base_url:
            url = self._build_url("results")
            headers: Dict[str, str] = {"Accept": "application/json"}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
            params = {"limit": limit}
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(url, headers=headers, params=params)
                response.raise_for_status()
                return response.json()

        keys = await self._redis.keys("alphafold3:job:*")
        records: List[Dict[str, Any]] = []
        for key in keys[:limit]:
            data = await self._redis.hgetall(key)
            if not data:
                continue
            job_id = key.rsplit(":", 1)[-1]
            record: Dict[str, Any] = {
                "job_id": job_id,
                "status": data.get("status", "unknown"),
                "target_name": data.get("target_name"),
                "sequence_length": int(data.get("sequence_length", "0")),
                "created_at": data.get("created_at"),
                "completed_at": data.get("completed_at"),
            }
            structure = data.get("structure")
            if structure:
                try:
                    record["structure"] = json.loads(structure)
                except json.JSONDecodeError:
                    logger.debug("Failed to decode cached AlphaFold3 structure for %s", job_id)
            records.append(record)

        records.sort(key=lambda item: item.get("created_at") or "", reverse=True)
        return {"results": records}

    async def get_structure(self, job_id: str) -> Dict[str, Any]:
        """Retrieve the predicted structure artifact for a job."""

        key = f"alphafold3:job:{job_id}"
        cached = await self._redis.hget(key, "structure")
        if cached:
            return json.loads(cached)

        if self.mock_mode:
            raise KeyError(f"Mock structure for job {job_id} not available")

        url = self._build_url(f"jobs/{job_id}/structure")
        headers: Dict[str, str] = {"Accept": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(url, headers=headers)
            if response.status_code == 404:
                raise KeyError(f"AlphaFold3 structure for job {job_id} not found") from None
            response.raise_for_status()
            data = response.json()

        await self._redis.hset(key, mapping={"structure": json.dumps(data)})
        await self._redis.expire(key, 3600)
        return data

    async def _complete_mock_job(self, job_id: str) -> None:
        """Simulate asynchronous AlphaFold3 processing in mock mode."""

        await asyncio.sleep(2)
        key = f"alphafold3:job:{job_id}"
        exists = await self._redis.exists(key)
        if not exists:
            return

        structure_payload: Dict[str, Any] = {
            "pdb": f"https://mock-alphafold3.local/jobs/{job_id}/model.pdb",
            "confidence": 0.85,
        }
        await self._redis.hset(
            key,
            mapping={
                "status": "succeeded",
                "structure": json.dumps(structure_payload),
                "completed_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        await self._redis.expire(key, 3600)

    def _build_url(self, suffix: str) -> str:
        base = (self.base_url or "").rstrip("/")
        return f"{base}/{suffix.lstrip('/')}"


alphafold3_client = AlphaFold3Client()

__all__ = ["AlphaFold3Client", "alphafold3_client"]
