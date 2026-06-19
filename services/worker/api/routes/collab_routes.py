"""HTTP and WebSocket routes for collaboration sessions."""
from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect, status
from pydantic import BaseModel

from worker.api.auth import (
    COLLAB_WEBSOCKET_TOKEN_EXPIRE_SECONDS,
    create_collab_websocket_token,
    decode_token,
    get_current_active_user,
)
from worker.api.collab_manager import (
    CollaborationError,
    SessionCapacityError,
    SessionNotFoundError,
    collab_manager,
)
from worker.api.schemas import (
    CollabSessionCreateResponse,
    CollabSessionDetailResponse,
    CollabSessionState,
)

router = APIRouter(prefix="/collab", tags=["collaboration"])

COLOR_PALETTE = [
    "#38bdf8",
    "#f97316",
    "#a855f7",
    "#22c55e",
    "#eab308",
    "#ef4444",
    "#ec4899",
    "#0ea5e9",
    "#14b8a6",
    "#f973ab",
]


class CollabSessionCreateRequest(BaseModel):
    title: Optional[str] = None


class CollabWebSocketTicketResponse(BaseModel):
    ticket: str
    expires_in: int


def _select_color(user_id: str) -> str:
    digest = hashlib.sha256(user_id.encode("utf-8")).hexdigest()
    index = int(digest[:8], 16) % len(COLOR_PALETTE)
    return COLOR_PALETTE[index]


def _presence_payload(user_id: str, name: Optional[str]) -> Dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "user_id": user_id,
        "name": name,
        "color": _select_color(user_id),
        "joined_at": now,
    }


def _participant_from_token_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    user_id_raw = payload.get("sub")
    if not user_id_raw:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token subject")

    user_id = str(user_id_raw)
    display_name = payload.get("preferred_username") or payload.get("username") or payload.get("name") or user_id
    return _presence_payload(user_id, display_name)


async def _receive_websocket_ticket(websocket: WebSocket, session_id: str) -> Optional[Dict[str, Any]]:
    try:
        auth_message = await asyncio.wait_for(websocket.receive_json(), timeout=5)
    except asyncio.TimeoutError:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Authentication required")
        return None
    except WebSocketDisconnect:
        return None
    except Exception:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Invalid authentication payload")
        return None

    if auth_message.get("type") != "auth" or not isinstance(auth_message.get("ticket"), str):
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Authentication required")
        return None

    try:
        payload = decode_token(auth_message["ticket"])
    except HTTPException:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Invalid ticket")
        return None

    if payload.get("type") != "collab_ws":
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Invalid ticket type")
        return None

    if payload.get("session_id") != session_id:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Invalid ticket scope")
        return None

    return payload


@router.post("/sessions", response_model=CollabSessionCreateResponse)
async def create_collab_session(
    payload: CollabSessionCreateRequest,
    user: Any = Depends(get_current_active_user),
) -> CollabSessionCreateResponse:
    try:
        response = await collab_manager.create_session(
            owner_id=str(getattr(user, "id", getattr(user, "username", "anonymous"))),
            owner_name=getattr(user, "username", None),
            title=payload.title,
        )
    except CollaborationError as error:  # pragma: no cover - defensive logging path
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(error)) from error

    return CollabSessionCreateResponse(**response)


@router.get("/sessions/{session_id}", response_model=CollabSessionDetailResponse)
async def get_collab_session(
    session_id: str,
    _user: Any = Depends(get_current_active_user),
) -> CollabSessionDetailResponse:
    try:
        session = await collab_manager.get_session(session_id)
        participants = await collab_manager.list_participants(session_id)
    except SessionNotFoundError as error:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session not found") from error
    except CollaborationError as error:  # pragma: no cover - defensive logging path
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(error)) from error

    return CollabSessionDetailResponse(
        session=CollabSessionState(**session),
        participants=participants,
        max_participants=collab_manager.max_participants,
    )


@router.post("/sessions/{session_id}/ws-ticket", response_model=CollabWebSocketTicketResponse)
async def create_collab_ws_ticket(
    session_id: str,
    user: Any = Depends(get_current_active_user),
) -> CollabWebSocketTicketResponse:
    try:
        await collab_manager.get_session(session_id)
    except SessionNotFoundError as error:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session not found") from error
    except CollaborationError as error:  # pragma: no cover - defensive logging path
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(error)) from error

    username = str(getattr(user, "username", getattr(user, "id", "anonymous")))
    display_name = getattr(user, "username", None) or getattr(user, "full_name", None) or username
    ticket = create_collab_websocket_token(
        {
            "sub": username,
            "preferred_username": display_name,
            "session_id": session_id,
        }
    )
    return CollabWebSocketTicketResponse(ticket=ticket, expires_in=COLLAB_WEBSOCKET_TOKEN_EXPIRE_SECONDS)


@router.websocket("/ws/{session_id}")
async def collab_session_websocket(websocket: WebSocket, session_id: str) -> None:
    await websocket.accept()

    payload = await _receive_websocket_ticket(websocket, session_id)
    if payload is None:
        return

    try:
        participant = _participant_from_token_payload(payload)
    except HTTPException:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Invalid participant")
        return

    try:
        session = await collab_manager.attach(session_id, websocket, participant, accept_connection=False)
        participants = await collab_manager.list_participants(session_id)
        await websocket.send_json(
            {
                "type": "session:init",
                "session": session,
                "participants": participants,
                "max_participants": collab_manager.max_participants,
            }
        )
        await collab_manager.broadcast_presence(session_id)
    except SessionNotFoundError:
        await websocket.close(code=status.WS_1000_NORMAL_CLOSURE, reason="Session not found")
        return
    except SessionCapacityError:
        await websocket.close(code=status.WS_1013_TRY_AGAIN_LATER, reason="Session full")
        return
    except CollaborationError:
        await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason="Collaboration unavailable")
        return

    try:
        while True:
            message = await websocket.receive_json()
            event_type = message.get("type")
            if not event_type:
                continue

            if event_type in {"ping", "heartbeat"}:
                await websocket.send_json({"type": "heartbeat", "timestamp": datetime.now(timezone.utc).isoformat()})
                continue

            payload_data = message.get("payload", {})
            try:
                broadcast_message = await collab_manager.handle_event(
                    session_id=session_id,
                    event_type=event_type,
                    payload=payload_data,
                    actor=participant,
                )
            except SessionNotFoundError:
                await websocket.close(code=status.WS_1000_NORMAL_CLOSURE, reason="Session ended")
                return
            except CollaborationError:
                await websocket.send_json(
                    {
                        "type": "error",
                        "message": "Unable to process collaboration event",
                    }
                )
                continue

            await collab_manager.broadcast(session_id, broadcast_message)
    except WebSocketDisconnect:
        pass
    except Exception:  # pragma: no cover - defensive catch
        await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason="Unexpected error")
    finally:
        await collab_manager.detach(session_id, websocket)
        try:
            await collab_manager.broadcast_presence(session_id)
        except CollaborationError:
            # Presence fail should not crash disconnect handling
            return
