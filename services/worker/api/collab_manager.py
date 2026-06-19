"""Real-time collaboration session manager using Redis for persistence."""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, cast
from uuid import uuid4

from fastapi import WebSocket
from redis.asyncio import Redis

from worker.api.cache import cache

ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime(ISO_FORMAT)


class CollaborationError(RuntimeError):
    """Base exception for collaboration errors."""


class SessionNotFoundError(CollaborationError):
    """Raised when a session cannot be found."""


class SessionCapacityError(CollaborationError):
    """Raised when a session reaches its participant limit."""


class CollabManager:
    """Manage collaborative session state and websocket connections."""

    def __init__(self) -> None:
        self._redis: Optional[Redis[Any]] = None
        self._locks: Dict[str, asyncio.Lock] = {}
        self._connections: Dict[str, List[WebSocket]] = {}
        self._connection_meta: Dict[str, Dict[WebSocket, str]] = {}

        self._session_prefix = os.getenv("COLLAB_SESSION_PREFIX", "collab:session")
        self._participants_prefix = os.getenv("COLLAB_PARTICIPANTS_PREFIX", "collab:participants")
        self._session_ttl = int(os.getenv("COLLAB_SESSION_TTL", "86400"))
        self._max_participants = int(os.getenv("COLLAB_MAX_PARTICIPANTS", "16"))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @property
    def max_participants(self) -> int:
        return self._max_participants

    def _session_key(self, session_id: str) -> str:
        return f"{self._session_prefix}:{session_id}"

    def _participants_key(self, session_id: str) -> str:
        return f"{self._participants_prefix}:{session_id}"

    def _redis_client(self) -> Redis[Any]:
        if self._redis is None:
            if cache.client is not None:
                self._redis = cast(Redis[Any], cache.client)
            else:
                host = os.getenv("COLLAB_REDIS_HOST", os.getenv("REDIS_HOST", "redis"))
                port = int(os.getenv("COLLAB_REDIS_PORT", os.getenv("REDIS_PORT", "6379")))
                db = int(os.getenv("COLLAB_REDIS_DB", os.getenv("REDIS_DB", "0")))
                self._redis = Redis(host=host, port=port, db=db, decode_responses=True)
        assert self._redis is not None
        return self._redis

    def _ensure_lock(self, session_id: str) -> asyncio.Lock:
        lock = self._locks.get(session_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[session_id] = lock
        return lock

    async def _ensure_redis(self) -> None:
        try:
            client = self._redis_client()
        except Exception as exc:  # pragma: no cover - defensive
            raise CollaborationError("Unable to initialise collaboration backend") from exc

        try:
            await client.ping()
        except Exception as exc:
            raise CollaborationError("Collaboration backend unavailable") from exc

    async def _load_session(self, session_id: str) -> Dict[str, Any]:
        await self._ensure_redis()
        redis = self._redis_client()
        data = await redis.get(self._session_key(session_id))
        if data is None:
            raise SessionNotFoundError(session_id)

        if isinstance(data, bytes):
            data = data.decode("utf-8")

        if not isinstance(data, str):
            raise CollaborationError("Invalid session payload")

        session = cast(Dict[str, Any], json.loads(data))
        session.setdefault("stickies", {})
        session.setdefault("shapes", {})
        return session

    async def save_session(self, session_id: str, session: Dict[str, Any]) -> None:
        await self._ensure_redis()
        redis = self._redis_client()
        session["updated_at"] = _utc_now_iso()
        await redis.set(self._session_key(session_id), json.dumps(session), ex=self._session_ttl)

    async def delete_session(self, session_id: str) -> None:
        await self._ensure_redis()
        redis = self._redis_client()
        await redis.delete(self._session_key(session_id))
        await redis.delete(self._participants_key(session_id))

    # ------------------------------------------------------------------
    # Public session API
    # ------------------------------------------------------------------
    async def create_session(
        self,
        owner_id: str,
        owner_name: Optional[str] = None,
        title: Optional[str] = None,
    ) -> Dict[str, Any]:
        await self._ensure_redis()
        session_id = uuid4().hex
        now = _utc_now_iso()
        session: Dict[str, Any] = {
            "session_id": session_id,
            "title": title or "Shared analysis workspace",
            "created_at": now,
            "owner_id": owner_id,
            "owner_name": owner_name,
            "updated_at": now,
            "stickies": {},
            "shapes": {},
        }

        redis = self._redis_client()
        await redis.set(self._session_key(session_id), json.dumps(session), ex=self._session_ttl)

        return {
            "session_id": session_id,
            "title": session["title"],
            "created_at": session["created_at"],
            "max_participants": self._max_participants,
        }

    async def get_session(self, session_id: str) -> Dict[str, Any]:
        session = await self._load_session(session_id)
        await self._redis_client().expire(self._session_key(session_id), self._session_ttl)
        return session

    async def list_participants(self, session_id: str) -> List[Dict[str, Any]]:
        await self._ensure_redis()
        redis = self._redis_client()
        participants_key = self._participants_key(session_id)
        data = await redis.hgetall(participants_key)

        participants: List[Dict[str, Any]] = []
        for raw in data.values():
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            if not isinstance(raw, str):
                continue
            try:
                participants.append(cast(Dict[str, Any], json.loads(raw)))
            except json.JSONDecodeError:
                continue

        if participants:
            await redis.expire(participants_key, self._session_ttl)
        return participants

    async def _set_participant(self, session_id: str, user_id: str, payload: Dict[str, Any]) -> None:
        await self._ensure_redis()
        redis = self._redis_client()
        participants_key = self._participants_key(session_id)
        await redis.hset(participants_key, user_id, json.dumps(payload))
        await redis.expire(participants_key, self._session_ttl)

    async def _remove_participant(self, session_id: str, user_id: str) -> None:
        if not self._redis:
            return
        redis = self._redis_client()
        participants_key = self._participants_key(session_id)
        await redis.hdel(participants_key, user_id)
        await redis.expire(participants_key, self._session_ttl)

    async def _enforce_capacity(self, session_id: str, user_id: str) -> None:
        await self._ensure_redis()
        redis = self._redis_client()
        participants_key = self._participants_key(session_id)
        count = await redis.hlen(participants_key)
        if count >= self._max_participants:
            exists = await redis.hexists(participants_key, user_id)
            if not exists:
                raise SessionCapacityError(session_id)

    # ------------------------------------------------------------------
    # WebSocket coordination
    # ------------------------------------------------------------------
    async def attach(
        self,
        session_id: str,
        websocket: WebSocket,
        participant: Dict[str, Any],
        *,
        accept_connection: bool = True,
    ) -> Dict[str, Any]:
        if accept_connection:
            await websocket.accept()
        lock = self._ensure_lock(session_id)
        async with lock:
            session = await self.get_session(session_id)
            await self._enforce_capacity(session_id, participant["user_id"])
            await self._set_participant(session_id, participant["user_id"], participant)
            connections = self._connections.setdefault(session_id, [])
            if websocket not in connections:
                connections.append(websocket)
            self._connection_meta.setdefault(session_id, {})[websocket] = participant["user_id"]
            return session

    async def detach(self, session_id: str, websocket: WebSocket) -> Optional[str]:
        user_id: Optional[str] = None
        if session_id in self._connections and websocket in self._connections[session_id]:
            self._connections[session_id].remove(websocket)
            if not self._connections[session_id]:
                self._connections.pop(session_id, None)
        if session_id in self._connection_meta and websocket in self._connection_meta[session_id]:
            user_id = self._connection_meta[session_id].pop(websocket)
            if not self._connection_meta[session_id]:
                self._connection_meta.pop(session_id, None)
        if user_id:
            await self._remove_participant(session_id, user_id)
        return user_id

    async def broadcast(self, session_id: str, message: Dict[str, Any]) -> None:
        for connection in list(self._connections.get(session_id, [])):
            try:
                await connection.send_json(message)
            except Exception:
                continue

    async def broadcast_presence(self, session_id: str) -> None:
        participants = await self.list_participants(session_id)
        payload: Dict[str, Any] = {
            "type": "presence",
            "participants": participants,
        }
        await self.broadcast(session_id, payload)

    # ------------------------------------------------------------------
    # Event handling
    # ------------------------------------------------------------------
    async def handle_event(
        self,
        session_id: str,
        event_type: str,
        payload: Dict[str, Any],
        actor: Dict[str, Any],
    ) -> Dict[str, Any]:
        lock = self._ensure_lock(session_id)
        async with lock:
            session = await self.get_session(session_id)
            now = _utc_now_iso()
            broadcast_payload: Dict[str, Any] = payload
            changed = False

            if event_type == "sticky.create":
                sticky_id = payload.get("id") or uuid4().hex
                sticky: Dict[str, Any] = {
                    "id": sticky_id,
                    "row_id": payload.get("row_id"),
                    "text": payload.get("text", ""),
                    "x": float(payload.get("x", 0.0)),
                    "y": float(payload.get("y", 0.0)),
                    "color": payload.get("color", "#facc15"),
                    "created_by": actor["user_id"],
                    "created_by_name": actor.get("name"),
                    "created_at": now,
                    "updated_at": now,
                }
                session.setdefault("stickies", {})[sticky_id] = sticky
                broadcast_payload = sticky
                payload["id"] = sticky_id
                changed = True

            elif event_type == "sticky.update":
                sticky_id = payload.get("id")
                sticky = session.get("stickies", {}).get(sticky_id or "")
                if sticky:
                    for field in ("text", "x", "y", "color", "row_id"):
                        if field in payload:
                            if field in {"x", "y"}:
                                sticky[field] = float(payload[field])
                            else:
                                sticky[field] = payload[field]
                    sticky["updated_at"] = now
                    broadcast_payload = sticky
                    changed = True

            elif event_type == "sticky.delete":
                sticky_id = payload.get("id")
                if sticky_id and sticky_id in session.get("stickies", {}):
                    session["stickies"].pop(sticky_id, None)
                    broadcast_payload = {"id": sticky_id}
                    changed = True

            elif event_type == "shape.create":
                shape_id = payload.get("id") or uuid4().hex
                shape_data = payload.get("data")
                if shape_data is None and "points" in payload:
                    shape_data = {"points": payload.get("points", [])}
                shape: Dict[str, Any] = {
                    "id": shape_id,
                    "type": payload.get("type", "circle"),
                    "data": shape_data or {},
                    "color": payload.get("color", "#f97316"),
                    "stroke_width": float(payload.get("stroke_width", 2.0)),
                    "row_id": payload.get("row_id"),
                    "created_by": actor["user_id"],
                    "created_by_name": actor.get("name"),
                    "created_at": now,
                    "updated_at": now,
                }
                session.setdefault("shapes", {})[shape_id] = shape
                if shape_data is not None and "data" not in payload:
                    payload["data"] = shape_data
                payload["id"] = shape_id
                broadcast_payload = shape
                changed = True

            elif event_type == "shape.update":
                shape_id = payload.get("id")
                shape = session.get("shapes", {}).get(shape_id or "")
                if shape:
                    for field in ("data", "color", "stroke_width", "row_id"):
                        if field in payload:
                            if field == "stroke_width":
                                shape[field] = float(payload[field])
                            elif field == "data" and isinstance(payload[field], dict):
                                shape[field] = payload[field]
                            else:
                                shape[field] = payload[field]
                    shape["updated_at"] = now
                    broadcast_payload = shape
                    changed = True

            elif event_type == "shape.delete":
                shape_id = payload.get("id")
                if shape_id and shape_id in session.get("shapes", {}):
                    session["shapes"].pop(shape_id, None)
                    broadcast_payload = {"id": shape_id}
                    changed = True

            if changed:
                await self.save_session(session_id, session)

            return {
                "type": event_type,
                "payload": broadcast_payload,
                "actor": actor,
                "timestamp": now,
            }


collab_manager = CollabManager()
"""Singleton collaboration manager."""
