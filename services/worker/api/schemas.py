from __future__ import annotations

from pydantic import BaseModel, Field, EmailStr
from typing import Optional, List, Dict, Any, Literal


def _empty_job_results() -> List["AlphaFoldJobResult"]:
    return []


def _empty_sticky_map() -> Dict[str, "CollabSticky"]:
    return {}


def _empty_shape_map() -> Dict[str, "CollabShape"]:
    return {}


def _empty_participant_list() -> List[Dict[str, Any]]:
    return []

class UserCreate(BaseModel):
    username: str = Field(min_length=3, max_length=50)
    password: str = Field(min_length=6, max_length=128)
    email: Optional[EmailStr] = None

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"

class JobCreate(BaseModel):
    accession: str  # Can be SRA accession OR upload session_id (format: "upload-{timestamp}-{hash}")
    reference_fasta: str
    reference_gtf: Optional[str] = None
    models: List[str] = Field(default_factory=lambda: ["m6anet"])
    analysis: Dict[str, Any] = Field(default_factory=dict)
    upload_session_id: Optional[str] = None  # If provided, indicates user uploaded files via MinIO

class JobStatus(BaseModel):
    id: str
    status: str
    result: Optional[Dict[str, Any]] = None


class Pod5JobRequest(BaseModel):
    """Request body for submitting a Dorado basecalling job."""

    upload_session_id: str
    pod5_object_name: str
    reference_object_name: Optional[str] = None


class AlphaFoldPredictRequest(BaseModel):
    sequence: str = Field(min_length=1)
    name: Optional[str] = Field(default=None, max_length=128)


class AlphaFoldJobResult(BaseModel):
    job_id: str
    id: Optional[str] = None
    status: str
    target_name: Optional[str] = None
    sequence_length: Optional[int] = None
    created_at: Optional[str] = None
    completed_at: Optional[str] = None
    outputs: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None
    structure: Optional[Dict[str, Any]] = None


class AlphaFoldPredictResponse(BaseModel):
    result: AlphaFoldJobResult


class AlphaFoldJobResponse(BaseModel):
    result: AlphaFoldJobResult


class AlphaFoldJobListResponse(BaseModel):
    results: List[AlphaFoldJobResult] = Field(default_factory=_empty_job_results)


class CollabSticky(BaseModel):
    id: str
    row_id: Optional[str] = None
    text: str
    x: float
    y: float
    color: str
    created_by: str
    created_by_name: Optional[str] = None
    created_at: str
    updated_at: Optional[str] = None


class CollabShape(BaseModel):
    id: str
    type: Literal["circle", "path", "rectangle"]
    data: Dict[str, Any] = Field(default_factory=dict)
    color: str
    stroke_width: float
    row_id: Optional[str] = None
    created_by: str
    created_by_name: Optional[str] = None
    created_at: str
    updated_at: Optional[str] = None


class CollabSessionState(BaseModel):
    session_id: str
    title: str
    created_at: str
    owner_id: str
    owner_name: Optional[str] = None
    stickies: Dict[str, CollabSticky] = Field(default_factory=_empty_sticky_map)
    shapes: Dict[str, CollabShape] = Field(default_factory=_empty_shape_map)


class CollabSessionCreateResponse(BaseModel):
    session_id: str
    title: str
    created_at: str
    max_participants: int


class CollabSessionDetailResponse(BaseModel):
    session: CollabSessionState
    participants: List[Dict[str, Any]] = Field(default_factory=_empty_participant_list)
    max_participants: int
