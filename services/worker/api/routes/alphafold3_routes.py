from __future__ import annotations

import re
from typing import Any

from fastapi import APIRouter, HTTPException, status

from worker.api.alphafold3_client import alphafold3_client
from worker.api.schemas import (
    AlphaFoldJobListResponse,
    AlphaFoldJobResponse,
    AlphaFoldPredictRequest,
    AlphaFoldPredictResponse,
)

AMINO_ACIDS = re.compile(r"^[ACDEFGHIKLMNPQRSTVWY]+$", re.IGNORECASE)

router = APIRouter(prefix="/alphafold3", tags=["alphafold3"])


def validate_sequence(sequence: str) -> str:
    sanitized = sequence.replace("\n", "").replace("\r", "").replace(" ", "")
    sanitized = sanitized.upper()
    if not sanitized:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Sequence is empty")
    if not AMINO_ACIDS.fullmatch(sanitized):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Sequence must contain only standard amino acid characters",
        )
    return sanitized


@router.post("/predict", response_model=AlphaFoldPredictResponse, status_code=status.HTTP_202_ACCEPTED)
async def predict(payload: AlphaFoldPredictRequest) -> AlphaFoldPredictResponse:
    sequence = validate_sequence(payload.sequence)
    try:
        result = await alphafold3_client.submit(sequence=sequence, name=payload.name)
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - network failures or config errors
        raise HTTPException(status_code=502, detail=f"AlphaFold3 submission failed: {exc}") from exc
    return AlphaFoldPredictResponse.model_validate(result)


@router.get("/jobs/{job_id}", response_model=AlphaFoldJobResponse)
async def job_status(job_id: str) -> AlphaFoldJobResponse:
    try:
        result: Any = await alphafold3_client.get_job(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="AlphaFold3 job not found")
    except Exception as exc:  # pragma: no cover - network failures
        raise HTTPException(status_code=502, detail=f"AlphaFold3 status query failed: {exc}") from exc
    return AlphaFoldJobResponse.model_validate(result)


@router.get("/jobs/{job_id}/structure")
async def job_structure(job_id: str) -> Any:
    try:
        result = await alphafold3_client.get_structure(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="AlphaFold3 structure not found")
    except Exception as exc:  # pragma: no cover - network failures
        raise HTTPException(status_code=502, detail=f"AlphaFold3 structure fetch failed: {exc}") from exc
    return result


@router.get("/results", response_model=AlphaFoldJobListResponse)
async def list_results(limit: int = 50) -> AlphaFoldJobListResponse:
    try:
        result = await alphafold3_client.list_jobs(limit=limit)
    except Exception as exc:  # pragma: no cover - network failures
        raise HTTPException(status_code=502, detail=f"AlphaFold3 listing failed: {exc}") from exc
    return AlphaFoldJobListResponse.model_validate(result)
