"""
MinIO Upload Session Management
Generates pre-signed URLs for direct client->MinIO uploads.

This module now supports S3-compatible multipart uploads to keep memory usage
low for large files. The flow is:
1) Backend creates a multipart upload and returns an upload_id plus part_size.
2) Client requests pre-signed URLs per part and uploads parts directly to MinIO.
3) Backend completes the multipart upload using provided ETags.
"""
import math
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Sequence

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from minio import Minio
from minio.error import S3Error

# MinIO client configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_PUBLIC_ENDPOINT = os.getenv("MINIO_PUBLIC_ENDPOINT", "localhost:9000")  # Browser-accessible endpoint
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "epimod")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
PRESIGN_EXPIRY_SECONDS = int(os.getenv("MINIO_PRESIGN_EXPIRY_SECONDS", "3600"))

SCHEME = "https" if MINIO_SECURE else "http"

# Initialize MinIO client (uses internal endpoint for worker operations)
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE
)

# Initialize public MinIO client for generating browser-accessible pre-signed URLs
public_minio_client = Minio(
    MINIO_PUBLIC_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE
)


def _make_s3_client(endpoint: str) -> "boto3.client":
    """Create a boto3 S3 client for MinIO with SigV4 signing."""

    return boto3.client(
        "s3",
        endpoint_url=f"{SCHEME}://{endpoint}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name=MINIO_REGION,
        config=Config(signature_version="s3v4"),
    )


# Use internal endpoint for control-plane calls and public for presigned URLs
s3_internal_client = _make_s3_client(MINIO_ENDPOINT)
s3_public_client = _make_s3_client(MINIO_PUBLIC_ENDPOINT)

# Multipart rules
MIN_MULTIPART_SIZE = 5 * 1024 * 1024  # 5MB per S3 spec
DEFAULT_PART_SIZE = 8 * 1024 * 1024  # 8MB preferred
MAX_PARTS = 10_000  # S3 upper bound
MAX_PART_SIZE = 5 * 1024 * 1024 * 1024  # 5GB per part ceiling

# Ensure bucket exists
def ensure_bucket():
    """Create bucket if it doesn't exist"""
    try:
        if not minio_client.bucket_exists(MINIO_BUCKET):
            minio_client.make_bucket(MINIO_BUCKET)
    except S3Error as e:
        print(f"Error ensuring bucket: {e}")
    except Exception as e:
        print(f"Warning: Could not connect to MinIO: {e}")

# Don't call ensure_bucket() at import time - let it fail gracefully
# ensure_bucket() will be called on first upload attempt


class UploadSession:
    """Manages file upload sessions with pre-signed URLs."""

    @staticmethod
    def _calculate_part_size(file_size: int) -> int:
        """Pick a part size that respects S3 limits and caps part count.

        Uses a preferred baseline of 8MB, never below 5MB, and scales up to
        keep part count under 10k.
        """

        base_size = max(DEFAULT_PART_SIZE, MIN_MULTIPART_SIZE)
        if file_size <= 0:
            return base_size

        max_supported_size = MAX_PART_SIZE * MAX_PARTS
        if file_size > max_supported_size:
            raise ValueError(
                "File too large for multipart upload; exceeds 10k parts at 5GB each"
            )

        required_part_size = math.ceil(file_size / MAX_PARTS)
        part_size = max(base_size, required_part_size)

        # Stay under the 5GB per part ceiling; if exceeded, callers should split files earlier
        return min(part_size, MAX_PART_SIZE)

    @staticmethod
    def create_session(
        user_id: str,
        files: List[Dict[str, Any]],
        expiration_hours: int = 1
    ) -> Dict[str, Any]:
        """Create a multipart upload session and generate upload IDs.

        Returns upload_id and part_size per session; clients request pre-signed
        part URLs on demand to avoid precomputing thousands of links.
        """

        ensure_bucket()

        session_id = str(uuid.uuid4())
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

        # Compute a safe part size for the largest file
        largest_file = max((int(f.get("size", 0)) for f in files), default=0)
        part_size = UploadSession._calculate_part_size(largest_file)

        presigned_entries = []
        created_uploads: List[Dict[str, str]] = []

        try:
            for file_info in files:
                filename = file_info["filename"]
                size = int(file_info.get("size", 0))
                object_name = f"uploads/{user_id}/{session_id}/{timestamp}_{filename}"

                upload_id = UploadSession._create_multipart_upload(object_name)
                created_uploads.append({"object_name": object_name, "upload_id": upload_id})

                presigned_entries.append(
                    {
                        "filename": filename,
                        "object_name": object_name,
                        "upload_id": upload_id,
                        "size": size,
                    }
                )
        except Exception as exc:  # boto errors wrapped
            # Abort any uploads created before the failure to avoid stranding parts
            for created in created_uploads:
                try:
                    UploadSession.abort_multipart_upload(
                        created["object_name"], created["upload_id"]
                    )
                except Exception:
                    # Best-effort cleanup; log to stdout since we lack structured logging here
                    print(
                        f"Warning: failed to abort multipart upload {created.get('upload_id')} "
                        f"for object {created.get('object_name')} after init failure"
                    )
            raise Exception(f"Failed to start multipart upload: {exc}")

        expires_at = datetime.now(timezone.utc) + timedelta(hours=expiration_hours)

        return {
            "session_id": session_id,
            "user_id": user_id,
            "expires_at": expires_at.isoformat(),
            "part_size": part_size,
            "files": presigned_entries,
        }

    @staticmethod
    def get_part_upload_url(
        object_name: str,
        upload_id: str,
        part_number: int,
        expires_seconds: int = PRESIGN_EXPIRY_SECONDS,
    ) -> str:
        """Generate a pre-signed URL for a single multipart part."""

        ensure_bucket()

        if part_number <= 0:
            raise ValueError("part_number must be >= 1")
        if part_number > MAX_PARTS:
            raise ValueError("part_number exceeds maximum allowed parts")

        try:
            return s3_public_client.generate_presigned_url(
                "upload_part",
                Params={
                    "Bucket": MINIO_BUCKET,
                    "Key": object_name,
                    "UploadId": upload_id,
                    "PartNumber": part_number,
                },
                ExpiresIn=expires_seconds,
                HttpMethod="PUT",
            )
        except (ClientError, BotoCoreError) as exc:
            raise Exception(f"Failed to generate part URL: {exc}")

    @staticmethod
    def complete_multipart_upload(
        object_name: str,
        upload_id: str,
        parts: Sequence[Dict[str, Any]],
    ) -> None:
        """Complete multipart upload using provided parts list."""

        ensure_bucket()

        cleaned_parts: List[Dict[str, Any]] = []
        seen_numbers = set()

        for part in parts:
            if "etag" not in part or "part_number" not in part:
                raise ValueError("Each part entry must include etag and part_number")

            part_number = int(part["part_number"])
            if part_number <= 0 or part_number > MAX_PARTS:
                raise ValueError("Invalid part_number in parts list")
            if part_number in seen_numbers:
                raise ValueError("Duplicate part_number in parts list")

            seen_numbers.add(part_number)
            cleaned_parts.append({"ETag": str(part["etag"]), "PartNumber": part_number})

        if not cleaned_parts:
            raise ValueError("No parts provided to complete multipart upload")

        sorted_parts = sorted(cleaned_parts, key=lambda item: item["PartNumber"])

        try:
            s3_internal_client.complete_multipart_upload(
                Bucket=MINIO_BUCKET,
                Key=object_name,
                UploadId=upload_id,
                MultipartUpload={"Parts": sorted_parts},
            )
        except (ClientError, BotoCoreError) as exc:
            raise Exception(f"Failed to complete multipart upload: {exc}")

    @staticmethod
    def abort_multipart_upload(object_name: str, upload_id: str) -> None:
        """Abort an incomplete multipart upload to reclaim storage."""

        ensure_bucket()
        try:
            s3_internal_client.abort_multipart_upload(
                Bucket=MINIO_BUCKET, Key=object_name, UploadId=upload_id
            )
        except (ClientError, BotoCoreError) as exc:
            raise Exception(f"Failed to abort multipart upload: {exc}")
    
    @staticmethod
    def verify_upload(object_name: str) -> bool:
        """
        Verify that a file was successfully uploaded to MinIO
        
        Args:
            object_name: Full path to object in MinIO
            
        Returns:
            True if file exists, False otherwise
        """
        try:
            ensure_bucket()
            minio_client.stat_object(MINIO_BUCKET, object_name)
            return True
        except S3Error:
            return False
    
    @staticmethod
    def get_object_path(session_id: str, user_id: str) -> str:
        """
        Get the MinIO path for a session
        
        Returns:
            Path prefix for all files in this session
        """
        return f"uploads/{user_id}/{session_id}/"
    
    @staticmethod
    def list_session_files(session_id: str, user_id: str) -> List[str]:
        """
        List all uploaded files in a session
        
        Returns:
            List of object names
        """
        prefix = UploadSession.get_object_path(session_id, user_id)
        try:
            ensure_bucket()
            objects = minio_client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True)
            return [obj.object_name for obj in objects if obj.object_name]
        except S3Error as e:
            raise Exception(f"Failed to list session files: {e}")
    
    @staticmethod
    def download_to_worker(object_name: str, local_path: str) -> str:
        """
        Download file from MinIO to worker's local filesystem
        
        Args:
            object_name: Full path to object in MinIO
            local_path: Local filesystem path to save file
            
        Returns:
            Path to downloaded file
        """
        try:
            ensure_bucket()
            minio_client.fget_object(MINIO_BUCKET, object_name, local_path)
            return local_path
        except S3Error as e:
            raise Exception(f"Failed to download {object_name}: {e}")

    @staticmethod
    def _create_multipart_upload(object_name: str) -> str:
        """Start a multipart upload and return the upload_id."""

        ensure_bucket()
        try:
            response = s3_internal_client.create_multipart_upload(
                Bucket=MINIO_BUCKET,
                Key=object_name,
                ContentType="application/octet-stream",
            )
            upload_id = response.get("UploadId")
            if not upload_id:
                raise ValueError("No UploadId returned from create_multipart_upload")
            return upload_id
        except (ClientError, BotoCoreError) as exc:
            raise Exception(f"Failed to create multipart upload: {exc}")

    # -------------------------------------------------------------------------
    # Cleanup Methods
    # -------------------------------------------------------------------------

    @staticmethod
    def list_incomplete_multipart_uploads(
        max_age_hours: int = 24,
        prefix: str = "uploads/"
    ) -> List[Dict[str, Any]]:
        """
        List incomplete multipart uploads older than max_age_hours.
        
        Args:
            max_age_hours: Only return uploads older than this many hours
            prefix: Object key prefix to filter (default: "uploads/")
            
        Returns:
            List of dicts with upload_id, key, initiated timestamp
        """
        ensure_bucket()
        stale_uploads: List[Dict[str, Any]] = []
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
        
        try:
            # Use pagination to handle large numbers of uploads
            paginator = s3_internal_client.get_paginator("list_multipart_uploads")
            
            for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix=prefix):
                uploads = page.get("Uploads", [])
                
                for upload in uploads:
                    initiated = upload.get("Initiated")
                    if initiated and initiated < cutoff_time:
                        stale_uploads.append({
                            "upload_id": upload.get("UploadId"),
                            "key": upload.get("Key"),
                            "initiated": initiated.isoformat() if initiated else None,
                            "owner": upload.get("Owner", {}).get("DisplayName"),
                        })
            
            return stale_uploads
            
        except (ClientError, BotoCoreError) as exc:
            raise Exception(f"Failed to list incomplete multipart uploads: {exc}")

    @staticmethod
    def abort_stale_multipart_uploads(
        max_age_hours: int = 24,
        prefix: str = "uploads/",
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Abort all incomplete multipart uploads older than max_age_hours.
        
        Args:
            max_age_hours: Abort uploads older than this many hours
            prefix: Object key prefix to filter
            dry_run: If True, only list but don't actually abort
            
        Returns:
            Dict with counts and details of aborted/failed uploads
        """
        stale_uploads = UploadSession.list_incomplete_multipart_uploads(
            max_age_hours=max_age_hours,
            prefix=prefix
        )
        
        aborted: List[Dict[str, str]] = []
        failed: List[Dict[str, str]] = []
        
        for upload in stale_uploads:
            upload_id = upload.get("upload_id")
            key = upload.get("key")
            
            if not upload_id or not key:
                continue
                
            if dry_run:
                aborted.append({"key": key, "upload_id": upload_id})
                continue
                
            try:
                UploadSession.abort_multipart_upload(key, upload_id)
                aborted.append({"key": key, "upload_id": upload_id})
            except Exception as exc:
                failed.append({
                    "key": key,
                    "upload_id": upload_id,
                    "error": str(exc)
                })
        
        return {
            "dry_run": dry_run,
            "max_age_hours": max_age_hours,
            "total_found": len(stale_uploads),
            "aborted_count": len(aborted),
            "failed_count": len(failed),
            "aborted": aborted,
            "failed": failed,
        }

    @staticmethod
    def delete_session_files(session_id: str, user_id: str) -> Dict[str, Any]:
        """
        Delete all files associated with an upload session.
        
        Args:
            session_id: The upload session ID
            user_id: The user ID who owns the session
            
        Returns:
            Dict with counts of deleted/failed objects
        """
        ensure_bucket()
        prefix = UploadSession.get_object_path(session_id, user_id)
        
        deleted: List[str] = []
        failed: List[Dict[str, str]] = []
        
        try:
            # List all objects with the session prefix
            objects = minio_client.list_objects(
                MINIO_BUCKET,
                prefix=prefix,
                recursive=True
            )
            
            for obj in objects:
                if not obj.object_name:
                    continue
                    
                try:
                    minio_client.remove_object(MINIO_BUCKET, obj.object_name)
                    deleted.append(obj.object_name)
                except S3Error as exc:
                    failed.append({
                        "object_name": obj.object_name,
                        "error": str(exc)
                    })
            
            return {
                "session_id": session_id,
                "prefix": prefix,
                "deleted_count": len(deleted),
                "failed_count": len(failed),
                "deleted": deleted,
                "failed": failed,
            }
            
        except S3Error as exc:
            raise Exception(f"Failed to delete session files: {exc}")

    @staticmethod
    def delete_objects_by_prefix(prefix: str, dry_run: bool = False) -> Dict[str, Any]:
        """
        Delete all objects matching a prefix.
        
        Args:
            prefix: Object key prefix (e.g., "uploads/anonymous/session-id/")
            dry_run: If True, only list but don't actually delete
            
        Returns:
            Dict with counts and details of deleted/failed objects
        """
        ensure_bucket()
        
        to_delete: List[str] = []
        deleted: List[str] = []
        failed: List[Dict[str, str]] = []
        
        try:
            objects = minio_client.list_objects(
                MINIO_BUCKET,
                prefix=prefix,
                recursive=True
            )
            
            for obj in objects:
                if not obj.object_name:
                    continue
                to_delete.append(obj.object_name)
            
            if dry_run:
                return {
                    "dry_run": True,
                    "prefix": prefix,
                    "would_delete_count": len(to_delete),
                    "objects": to_delete,
                }
            
            for object_name in to_delete:
                try:
                    minio_client.remove_object(MINIO_BUCKET, object_name)
                    deleted.append(object_name)
                except S3Error as exc:
                    failed.append({
                        "object_name": object_name,
                        "error": str(exc)
                    })
            
            return {
                "dry_run": False,
                "prefix": prefix,
                "deleted_count": len(deleted),
                "failed_count": len(failed),
                "deleted": deleted,
                "failed": failed,
            }
            
        except S3Error as exc:
            raise Exception(f"Failed to delete objects by prefix: {exc}")

    @staticmethod
    def get_bucket_stats(prefix: str = "uploads/") -> Dict[str, Any]:
        """
        Get storage statistics for objects matching a prefix.
        
        Args:
            prefix: Object key prefix to scan
            
        Returns:
            Dict with object count, total size, and breakdown by age
        """
        ensure_bucket()
        
        now = datetime.now(timezone.utc)
        stats = {
            "prefix": prefix,
            "total_objects": 0,
            "total_size_bytes": 0,
            "objects_by_age": {
                "last_24h": {"count": 0, "size_bytes": 0},
                "1_7_days": {"count": 0, "size_bytes": 0},
                "7_30_days": {"count": 0, "size_bytes": 0},
                "over_30_days": {"count": 0, "size_bytes": 0},
            }
        }
        
        try:
            objects = minio_client.list_objects(
                MINIO_BUCKET,
                prefix=prefix,
                recursive=True
            )
            
            for obj in objects:
                if not obj.object_name:
                    continue
                    
                stats["total_objects"] += 1
                size = obj.size or 0
                stats["total_size_bytes"] += size
                
                if obj.last_modified:
                    age = now - obj.last_modified.replace(tzinfo=timezone.utc)
                    
                    if age < timedelta(hours=24):
                        bucket = "last_24h"
                    elif age < timedelta(days=7):
                        bucket = "1_7_days"
                    elif age < timedelta(days=30):
                        bucket = "7_30_days"
                    else:
                        bucket = "over_30_days"
                    
                    stats["objects_by_age"][bucket]["count"] += 1
                    stats["objects_by_age"][bucket]["size_bytes"] += size
            
            # Add human-readable sizes
            stats["total_size_human"] = _format_bytes(stats["total_size_bytes"])
            for bucket in stats["objects_by_age"]:
                stats["objects_by_age"][bucket]["size_human"] = _format_bytes(
                    stats["objects_by_age"][bucket]["size_bytes"]
                )
            
            return stats
            
        except S3Error as exc:
            raise Exception(f"Failed to get bucket stats: {exc}")


def _format_bytes(size_bytes: int) -> str:
    """Format bytes into human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(size_bytes) < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"
