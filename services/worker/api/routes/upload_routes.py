"""
Upload Routes - Pre-signed URL generation and upload session management
"""
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, cast

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import select, update, delete
from sqlalchemy.ext.asyncio import AsyncSession

from worker.api.db.models import UploadSessionModel
from worker.api.db.session import get_session
from worker.api.storage.upload_manager import PRESIGN_EXPIRY_SECONDS, UploadSession

router = APIRouter(prefix="/upload", tags=["upload"])


class FileMetadata(BaseModel):
    """File metadata for upload"""
    filename: str = Field(..., description="Name of the file to upload")
    size: int = Field(..., description="File size in bytes", ge=0)


class InitUploadRequest(BaseModel):
    """Request to initialize upload session"""
    files: List[FileMetadata] = Field(..., description="List of files to upload")


class UploadFileEntry(BaseModel):
    """File entry returned to the client when initiating an upload."""

    filename: str
    object_name: str
    upload_id: str
    size: int
    part_size: int


class InitUploadResponse(BaseModel):
    """Response with multipart metadata for upload"""

    session_id: str
    user_id: str
    expires_at: str
    part_size: int
    files: List[UploadFileEntry]


class UploadPart(BaseModel):
    part_number: int = Field(..., description="Multipart part number (1-indexed)", ge=1)
    etag: str = Field(..., description="ETag returned by MinIO for the part")


class CompleteUploadFile(BaseModel):
    object_name: str
    upload_id: str
    parts: List[UploadPart]


class CompleteUploadRequest(BaseModel):
    """Request to complete upload session"""
    session_id: str = Field(..., description="Upload session ID")
    files: List[CompleteUploadFile] = Field(..., description="Files with part metadata to finalize")


class CompleteUploadResponse(BaseModel):
    """Response after completing upload"""
    session_id: str
    verified_files: List[str]
    failed_files: List[str]
    status: str


class PartUploadUrlRequest(BaseModel):
    """Request presigned URL for a specific multipart part."""

    session_id: str
    object_name: str
    upload_id: str
    part_number: int = Field(..., ge=1)


class PartUploadUrlResponse(BaseModel):
    """Pre-signed URL details for uploading a single part."""

    upload_url: str
    part_number: int
    expires_at: str


@router.post("/init", response_model=InitUploadResponse, status_code=status.HTTP_201_CREATED)
async def init_upload(
    request: InitUploadRequest,
    session: AsyncSession = Depends(get_session)
):
    """
    Initialize upload session and prepare multipart metadata for direct MinIO upload
    
    This endpoint:
    1. Creates a unique upload session
    2. Starts a multipart upload per file and returns upload_id + part_size
    3. Client then requests per-part pre-signed URLs via /upload/part-url and uploads directly to MinIO
    
    No authentication required - all users are anonymous
    """
    try:
        user_id = "anonymous"
        
        # Validate file count and sizes
        if len(request.files) == 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="At least one file is required"
            )
        
        if len(request.files) > 1000:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Maximum 1000 files per upload session"
            )
        
        # Create upload session with pre-signed URLs
        session_payload = UploadSession.create_session(
            user_id=user_id,
            files=[{"filename": f.filename, "size": f.size} for f in request.files],
            expiration_hours=1
        )

        try:
            expires_at = datetime.fromisoformat(session_payload["expires_at"])
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Invalid session expiry timestamp"
            ) from exc

        expires_at = expires_at if expires_at.tzinfo else expires_at.replace(tzinfo=timezone.utc)

        files_with_part_size: List[Dict[str, Any]] = [
            {
                **file_info,
                "part_size": session_payload.get("part_size"),
                "verified": False,
                "verified_at": None,
            }
            for file_info in session_payload["files"]
        ]

        record = UploadSessionModel(
            session_id=session_payload["session_id"],
            user_id=user_id,
            expires_at=expires_at,
            files=files_with_part_size,
        )

        session_payload["files"] = [UploadFileEntry(**file_info) for file_info in files_with_part_size]

        session.add(record)
        await session.commit()
        
        return InitUploadResponse(**session_payload)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to initialize upload session: {str(e)}"
        )


@router.post("/part-url", response_model=PartUploadUrlResponse)
async def get_part_upload_url(
    request: PartUploadUrlRequest,
    session: AsyncSession = Depends(get_session),
):
    """
    Provide a pre-signed URL for a specific multipart part.

    The client must present the session_id, object_name, upload_id, and desired part number.
    """

    try:
        user_id = "anonymous"

        columns = (
            UploadSessionModel.session_id,
            UploadSessionModel.user_id,
            UploadSessionModel.expires_at,
            UploadSessionModel.files,
        )
        result = await session.execute(
            select(*columns).where(UploadSessionModel.session_id == request.session_id)
        )
        record = result.mappings().first()

        if record is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Upload session not found")

        record_user_id = str(record["user_id"])
        if record_user_id != user_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Upload session does not belong to user")

        expires_at_value = record.get("expires_at")
        if not expires_at_value:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Upload session missing expiry")

        record_expiry = (
            expires_at_value
            if getattr(expires_at_value, "tzinfo", None)
            else expires_at_value.replace(tzinfo=timezone.utc)
        )
        if record_expiry < datetime.now(timezone.utc):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Upload session expired")

        raw_files = cast(List[Any], record.get("files") or [])
        record_files: List[Dict[str, Any]] = [cast(Dict[str, Any], item.copy()) for item in raw_files if isinstance(item, dict)]

        matched_file = next(
            (f for f in record_files if f.get("object_name") == request.object_name),
            None,
        )

        if not matched_file:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found in session")

        if str(matched_file.get("upload_id")) != request.upload_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Upload ID does not match session file")

        # Enforce a reasonable upper bound for part numbers based on declared size
        part_size = int(matched_file.get("part_size") or 0)
        file_size = int(matched_file.get("size") or 0)
        if part_size > 0 and file_size > 0:
            estimated_parts = max(1, math.ceil(file_size / part_size))
            if request.part_number > estimated_parts:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="part_number exceeds expected part count for file size",
                )

        try:
            upload_url = UploadSession.get_part_upload_url(
                object_name=request.object_name,
                upload_id=request.upload_id,
                part_number=request.part_number,
            )
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc

        expiry_ts = datetime.now(timezone.utc) + timedelta(seconds=PRESIGN_EXPIRY_SECONDS)

        return PartUploadUrlResponse(
            upload_url=upload_url,
            part_number=request.part_number,
            expires_at=expiry_ts.isoformat(),
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate part upload URL: {str(e)}"
        )


@router.post("/complete", response_model=CompleteUploadResponse)
async def complete_upload(
    request: CompleteUploadRequest,
    session: AsyncSession = Depends(get_session)
):
    """
    Complete upload session and verify all files were uploaded successfully
    
    This endpoint:
    1. Finalizes each multipart upload using provided ETags
    2. Verifies objects exist in MinIO
    3. Returns lists of verified and failed files
    4. Marks session as ready for job submission
    No authentication required
    """
    try:
        user_id = "anonymous"

        columns = (
            UploadSessionModel.session_id,
            UploadSessionModel.user_id,
            UploadSessionModel.expires_at,
            UploadSessionModel.files,
            UploadSessionModel.completed_at,
        )
        result = await session.execute(
            select(*columns).where(UploadSessionModel.session_id == request.session_id)
        )
        record = result.mappings().first()

        if record is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Upload session not found")

        record_user_id = str(record["user_id"])
        if record_user_id != user_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Upload session does not belong to user")

        now = datetime.now(timezone.utc)
        expires_at_value = record.get("expires_at")
        if expires_at_value is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Upload session missing expiry")

        record_expiry = (
            expires_at_value
            if getattr(expires_at_value, "tzinfo", None)
            else expires_at_value.replace(tzinfo=timezone.utc)
        )
        if record_expiry < now:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Upload session expired")

        raw_files = cast(List[Any], record.get("files") or [])
        record_files: List[Dict[str, Any]] = [cast(Dict[str, Any], item.copy()) for item in raw_files if isinstance(item, dict)]

        expected_files = {
            file_info["object_name"]: file_info
            for file_info in record_files
            if "object_name" in file_info
        }

        verified_files: List[str] = []
        failed_files: List[str] = []
        expected_prefix = UploadSession.get_object_path(request.session_id, record_user_id)

        for file_payload in request.files:
            object_name = file_payload.object_name
            upload_id = file_payload.upload_id

            if not object_name.startswith(expected_prefix):
                failed_files.append(object_name)
                continue

            file_entry = expected_files.get(object_name)
            if not file_entry:
                failed_files.append(object_name)
                continue

            if str(file_entry.get("upload_id")) != upload_id:
                failed_files.append(object_name)
                continue

            if not file_payload.parts:
                failed_files.append(object_name)
                continue

            try:
                UploadSession.complete_multipart_upload(
                    object_name=object_name,
                    upload_id=upload_id,
                    parts=[part.model_dump() for part in file_payload.parts],
                )
            except HTTPException:
                raise
            except Exception:
                failed_files.append(object_name)
                continue

            # Verify the completed object exists
            if UploadSession.verify_upload(object_name):
                verified_files.append(object_name)
                file_entry["verified"] = True
                file_entry["verified_at"] = now.isoformat()
                file_entry["parts"] = [part.model_dump() for part in file_payload.parts]
            else:
                failed_files.append(object_name)

        missing_files = [name for name in expected_files if name not in verified_files]
        for missing in missing_files:
            if missing not in failed_files:
                failed_files.append(missing)

        if not failed_files:
            status_str = "completed"
        elif verified_files:
            status_str = "partial"
        else:
            status_str = "failed"

        updated_files = list(expected_files.values())

        await session.execute(
            update(UploadSessionModel)
            .where(UploadSessionModel.session_id == request.session_id)
            .values(files=updated_files, completed_at=now)
        )
        await session.commit()

        return CompleteUploadResponse(
            session_id=request.session_id,
            verified_files=verified_files,
            failed_files=failed_files,
            status=status_str
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to complete upload session: {str(e)}"
        )


@router.get("/session/{session_id}", response_model=dict)
async def get_session_files(
    session_id: str,
    session: AsyncSession = Depends(get_session)
):
    """
    List all files in an upload session
    
    Useful for checking upload progress and verifying completeness
    No authentication required
    """
    try:
        user_id = "anonymous"

        columns = (
            UploadSessionModel.session_id,
            UploadSessionModel.user_id,
            UploadSessionModel.expires_at,
            UploadSessionModel.files,
            UploadSessionModel.completed_at,
        )
        result = await session.execute(
            select(*columns).where(UploadSessionModel.session_id == session_id)
        )
        record = result.mappings().first()

        if record is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Upload session not found")

        record_user_id = str(record["user_id"])
        if record_user_id != user_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Upload session does not belong to user")

        try:
            uploaded_objects = UploadSession.list_session_files(session_id, record_user_id)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve session files: {exc}"
            ) from exc

        record_expires_at = record.get("expires_at")
        record_completed_at = record.get("completed_at")
        raw_files = cast(List[Any], record.get("files") or [])
        record_files = [dict(item) for item in raw_files if isinstance(item, dict)]

        part_size = None
        if record_files:
            part_size = record_files[0].get("part_size")

        response: Dict[str, Any] = {
            "session_id": session_id,
            "user_id": user_id,
            "expires_at": record_expires_at.isoformat() if record_expires_at else None,
            "completed_at": record_completed_at.isoformat() if record_completed_at else None,
            "expected_files": record_files,
            "uploaded_objects": uploaded_objects,
            "file_count": len(record_files),
            "part_size": part_size,
        }
        return response

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve session files: {str(e)}"
        )


# -----------------------------------------------------------------------------
# Admin Cleanup Endpoints
# -----------------------------------------------------------------------------

class CleanupStaleUploadsRequest(BaseModel):
    """Request to clean up stale multipart uploads"""
    max_age_hours: int = Field(default=24, description="Abort uploads older than this many hours", ge=1)
    dry_run: bool = Field(default=False, description="If true, only report what would be cleaned up")


class CleanupStaleUploadsResponse(BaseModel):
    """Response from stale upload cleanup"""
    dry_run: bool
    max_age_hours: int
    total_found: int
    aborted_count: int
    failed_count: int
    aborted: List[Dict[str, str]]
    failed: List[Dict[str, str]]


class CleanupExpiredSessionsRequest(BaseModel):
    """Request to clean up expired upload sessions"""
    ttl_days: int = Field(default=7, description="Delete sessions older than this many days", ge=1)
    dry_run: bool = Field(default=False, description="If true, only report what would be cleaned up")


class CleanupExpiredSessionsResponse(BaseModel):
    """Response from expired session cleanup"""
    dry_run: bool
    ttl_days: int
    cutoff_date: str
    total_expired_sessions: int
    sessions_deleted: int
    sessions_failed: int
    deleted_session_ids: List[str]
    failed_sessions: List[Dict[str, str]]


class StorageStatsResponse(BaseModel):
    """Response with storage statistics"""
    prefix: str
    total_objects: int
    total_size_bytes: int
    total_size_human: str
    objects_by_age: Dict[str, Dict[str, Any]]


class DeleteSessionResponse(BaseModel):
    """Response from deleting a specific session"""
    session_id: str
    prefix: str
    deleted_count: int
    failed_count: int
    deleted: List[str]
    failed: List[Dict[str, str]]


@router.post("/admin/cleanup-stale-uploads", response_model=CleanupStaleUploadsResponse)
async def admin_cleanup_stale_uploads(
    request: CleanupStaleUploadsRequest,
):
    """
    [Admin] Clean up incomplete multipart uploads older than max_age_hours.
    
    This endpoint immediately aborts stale multipart uploads in MinIO.
    For background cleanup, use the Celery task instead.
    
    Set dry_run=true to preview what would be cleaned up without making changes.
    """
    try:
        result = UploadSession.abort_stale_multipart_uploads(
            max_age_hours=request.max_age_hours,
            prefix="uploads/",
            dry_run=request.dry_run
        )
        
        return CleanupStaleUploadsResponse(**result)
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to cleanup stale uploads: {str(e)}"
        )


@router.post("/admin/cleanup-expired-sessions", response_model=CleanupExpiredSessionsResponse)
async def admin_cleanup_expired_sessions(
    request: CleanupExpiredSessionsRequest,
    session: AsyncSession = Depends(get_session)
):
    """
    [Admin] Clean up expired upload sessions from PostgreSQL and MinIO.
    
    This endpoint:
    1. Finds sessions older than ttl_days
    2. Deletes their files from MinIO
    3. Removes session records from PostgreSQL
    
    Set dry_run=true to preview what would be cleaned up without making changes.
    """
    try:
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=request.ttl_days)
        
        # Find expired sessions
        result = await session.execute(
            select(
                UploadSessionModel.session_id,
                UploadSessionModel.user_id,
                UploadSessionModel.created_at,
            ).where(
                UploadSessionModel.created_at < cutoff_date
            )
        )
        expired_sessions = result.fetchall()
        
        deleted_session_ids: List[str] = []
        failed_sessions: List[Dict[str, str]] = []
        
        for row in expired_sessions:
            session_id = row.session_id
            user_id = row.user_id
            
            if request.dry_run:
                deleted_session_ids.append(session_id)
                continue
            
            try:
                # Delete files from MinIO
                UploadSession.delete_session_files(session_id, user_id)
                
                # Delete session record from PostgreSQL
                await session.execute(
                    delete(UploadSessionModel).where(
                        UploadSessionModel.session_id == session_id
                    )
                )
                deleted_session_ids.append(session_id)
                
            except Exception as exc:
                failed_sessions.append({
                    "session_id": session_id,
                    "error": str(exc)
                })
        
        if not request.dry_run:
            await session.commit()
        
        return CleanupExpiredSessionsResponse(
            dry_run=request.dry_run,
            ttl_days=request.ttl_days,
            cutoff_date=cutoff_date.isoformat(),
            total_expired_sessions=len(expired_sessions),
            sessions_deleted=len(deleted_session_ids),
            sessions_failed=len(failed_sessions),
            deleted_session_ids=deleted_session_ids,
            failed_sessions=failed_sessions,
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to cleanup expired sessions: {str(e)}"
        )


@router.get("/admin/storage-stats", response_model=StorageStatsResponse)
async def admin_get_storage_stats(
    prefix: str = Query(default="uploads/", description="Object prefix to scan")
):
    """
    [Admin] Get storage statistics for uploaded files.
    
    Returns object count, total size, and breakdown by age.
    """
    try:
        stats = UploadSession.get_bucket_stats(prefix=prefix)
        return StorageStatsResponse(**stats)
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get storage stats: {str(e)}"
        )


@router.delete("/admin/session/{session_id}", response_model=DeleteSessionResponse)
async def admin_delete_session(
    session_id: str,
    session: AsyncSession = Depends(get_session)
):
    """
    [Admin] Delete a specific upload session and its files.
    
    This endpoint:
    1. Deletes all files for the session from MinIO
    2. Removes the session record from PostgreSQL
    """
    try:
        # Get session to find user_id
        result = await session.execute(
            select(UploadSessionModel.user_id).where(
                UploadSessionModel.session_id == session_id
            )
        )
        row = result.scalar_one_or_none()
        
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Upload session not found"
            )
        
        user_id = row
        
        # Delete files from MinIO
        minio_result = UploadSession.delete_session_files(session_id, user_id)
        
        # Delete session record from PostgreSQL
        await session.execute(
            delete(UploadSessionModel).where(
                UploadSessionModel.session_id == session_id
            )
        )
        await session.commit()
        
        return DeleteSessionResponse(**minio_result)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete session: {str(e)}"
        )


@router.post("/admin/trigger-cleanup")
async def admin_trigger_cleanup(
    cleanup_type: str = Query(
        ...,
        description="Type of cleanup: 'stale_uploads', 'expired_sessions', or 'both'"
    ),
    max_age_hours: int = Query(default=24, description="For stale uploads: max age in hours"),
    ttl_days: int = Query(default=7, description="For expired sessions: TTL in days"),
):
    """
    [Admin] Trigger cleanup tasks asynchronously via Celery.
    
    This queues cleanup tasks to run in the background instead of
    waiting for the periodic schedule.
    
    cleanup_type options:
    - 'stale_uploads': Clean incomplete multipart uploads
    - 'expired_sessions': Clean old upload sessions
    - 'both': Run both cleanup tasks
    """
    try:
        from worker.tasks import cleanup_stale_uploads, cleanup_expired_sessions
        
        queued_tasks: List[Dict[str, str]] = []
        
        if cleanup_type in ("stale_uploads", "both"):
            task = cleanup_stale_uploads.delay(max_age_hours=max_age_hours)
            queued_tasks.append({
                "task": "cleanup_stale_uploads",
                "task_id": task.id,
                "max_age_hours": str(max_age_hours)
            })
        
        if cleanup_type in ("expired_sessions", "both"):
            task = cleanup_expired_sessions.delay(ttl_days=ttl_days)
            queued_tasks.append({
                "task": "cleanup_expired_sessions",
                "task_id": task.id,
                "ttl_days": str(ttl_days)
            })
        
        if not queued_tasks:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid cleanup_type. Use 'stale_uploads', 'expired_sessions', or 'both'"
            )
        
        return {
            "status": "queued",
            "message": f"Cleanup tasks queued successfully",
            "tasks": queued_tasks
        }
        
    except HTTPException:
        raise
    except ImportError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Celery tasks not available: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to trigger cleanup: {str(e)}"
        )
