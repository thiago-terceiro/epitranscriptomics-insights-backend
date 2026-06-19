#!/usr/bin/env python3
"""
End-to-end integration test: Researcher uploads POD5, gets BAM.

Simulates the exact journey of a real researcher using this app:
  1. Stack health checks
  2. Upload a mock POD5 file (like a researcher would)
  3. Submit basecalling job (POST /jobs/pod5)
  4. Poll until job succeeds
  5. Verify BAM is in MinIO and POD5 is deleted

Run from backend/ directory after `make local-up`:
    python scripts/integration_test_pod5.py

Exit code 0 = all green, 1 = failure.
"""

import json
import sys
import time
from typing import Any, Dict, List, Optional

import boto3
import httpx
from botocore.config import Config

# ---------------------------------------------------------------------------
# Config (matches docker-compose.local.yml defaults)
# ---------------------------------------------------------------------------
API: str = "http://localhost:8080"
MINIO_ENDPOINT: str = "http://localhost:9000"
MINIO_KEY: str = "minioadmin"
MINIO_SECRET: str = "minioadmin"
MINIO_BUCKET: str = "epimod"
JOB_POLL_TIMEOUT: int = 120   # seconds
JOB_POLL_INTERVAL: int = 3    # seconds

# ---------------------------------------------------------------------------
# ANSI colours
# ---------------------------------------------------------------------------
GREEN:  str = "\033[92m"
RED:    str = "\033[91m"
YELLOW: str = "\033[93m"
BOLD:   str = "\033[1m"
RESET:  str = "\033[0m"

def ok(msg: str)   -> None: print(f"  {GREEN}[OK]{RESET} {msg}")
def fail(msg: str) -> None: print(f"  {RED}[FAIL]{RESET} {msg}")
def info(msg: str) -> None: print(f"  {YELLOW}-->{RESET} {msg}")
def step(msg: str) -> None: print(f"\n{BOLD}{msg}{RESET}")

# ---------------------------------------------------------------------------
# Minimal synthetic POD5 (enough to be a valid file for storage testing)
# A real POD5 is HDF5-based; for mock mode we just need bytes the worker
# accepts -- the mock mode never calls dorado, so any non-empty file works.
# ---------------------------------------------------------------------------

def make_mock_pod5(size_bytes: int = 64 * 1024) -> bytes:
    """Return a fake-but-named POD5 payload (64 KB by default)."""
    # POD5 files start with a specific magic; we replicate that for realism.
    # Magic: b'\x8aPOD5\r\n\x1a\n'  (8 bytes)
    magic: bytes = b"\x8aPOD5\r\n\x1a\n"
    payload: bytes = magic + b"\x00" * (size_bytes - len(magic))
    return payload


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def s3_client() -> Any:
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_KEY,
        aws_secret_access_key=MINIO_SECRET,
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )


def object_exists(s3: Any, key: str) -> bool:
    try:
        s3.head_object(Bucket=MINIO_BUCKET, Key=key)
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Test steps
# ---------------------------------------------------------------------------

def check_health(client: httpx.Client) -> None:
    step("Step 1 -- Health check")
    r: httpx.Response = client.get(f"{API}/health", timeout=10)
    assert r.status_code == 200, f"API not healthy: {r.status_code} {r.text}"
    ok(f"API healthy: {r.json().get('status')}")


def init_upload(client: httpx.Client, filename: str, size: int) -> Dict[str, Any]:
    step("Step 2 -- Init multipart upload (researcher picks file)")
    payload: Dict[str, Any] = {"files": [{"filename": filename, "size": size}]}
    r: httpx.Response = client.post(f"{API}/upload/init", json=payload, timeout=15)
    assert r.status_code == 201, f"Init failed: {r.status_code} {r.text}"
    data: Dict[str, Any] = r.json()
    ok(f"Session created: {data['session_id']}")
    info(f"Object will be: {data['files'][0]['object_name']}")
    return data


def upload_file(client: httpx.Client, init_data: Dict[str, Any], file_bytes: bytes) -> List[Dict[str, Any]]:
    """Request presigned URL for each part and PUT directly to MinIO."""
    step("Step 3 -- Upload POD5 chunks to MinIO (presigned multipart)")
    file_entry: Dict[str, Any] = init_data["files"][0]
    session_id: str  = init_data["session_id"]
    object_name: str = file_entry["object_name"]
    upload_id: str   = file_entry["upload_id"]
    part_size: int   = int(file_entry["part_size"])

    parts: List[Dict[str, Any]] = []
    offset: int = 0
    part_number: int = 1

    while offset < len(file_bytes):
        chunk: bytes = file_bytes[offset : offset + part_size]
        offset += part_size

        # Get presigned URL for this part
        url_resp: httpx.Response = client.post(
            f"{API}/upload/part-url",
            json={
                "session_id": session_id,
                "object_name": object_name,
                "upload_id": upload_id,
                "part_number": part_number,
            },
            timeout=15,
        )
        assert url_resp.status_code == 200, f"Part URL failed: {url_resp.status_code} {url_resp.text}"
        presigned_url: str = url_resp.json()["upload_url"]

        # PUT directly to MinIO (as a browser would)
        put_resp: httpx.Response = httpx.put(presigned_url, content=chunk, timeout=30)
        assert put_resp.status_code in (200, 204), f"PUT part {part_number} failed: {put_resp.status_code}"
        etag: str = put_resp.headers.get("ETag", "").strip('"')
        parts.append({"part_number": part_number, "etag": etag})
        ok(f"Part {part_number} uploaded ({len(chunk):,} bytes) -- ETag: {etag[:12]}...")
        part_number += 1

    return parts


def complete_upload(
    client: httpx.Client,
    session_id: str,
    object_name: str,
    upload_id: str,
    parts: List[Dict[str, Any]],
) -> None:
    step("Step 4 -- Complete upload (tell the API all parts are done)")
    payload: Dict[str, Any] = {
        "session_id": session_id,
        "files": [{"object_name": object_name, "upload_id": upload_id, "parts": parts}],
    }
    r: httpx.Response = client.post(f"{API}/upload/complete", json=payload, timeout=30)
    assert r.status_code == 200, f"Complete failed: {r.status_code} {r.text}"
    data: Dict[str, Any] = r.json()
    assert data["status"] in ("completed", "partial"), f"Unexpected status: {data}"
    ok(f"Upload finalised -- verified files: {data.get('verified_files')}")


def submit_job(client: httpx.Client, session_id: str, object_name: str) -> str:
    step("Step 5 -- Submit basecalling job (researcher clicks 'Analyse')")
    payload: Dict[str, str] = {
        "upload_session_id": session_id,
        "pod5_object_name": object_name,
    }
    r: httpx.Response = client.post(f"{API}/jobs/pod5", json=payload, timeout=15)
    assert r.status_code == 200, f"Job submit failed: {r.status_code} {r.text}"
    job_id: str = r.json()["job_id"]
    ok(f"Job queued: {job_id}")
    return job_id


def poll_job(client: httpx.Client, job_id: str) -> Dict[str, Any]:
    step("Step 6 -- Poll job status (researcher watches progress bar)")
    deadline: float = time.time() + JOB_POLL_TIMEOUT
    last_status: str = ""
    while time.time() < deadline:
        r: httpx.Response = client.get(f"{API}/jobs/{job_id}", timeout=10)
        assert r.status_code == 200, f"Poll failed: {r.status_code} {r.text}"
        data: Dict[str, Any] = r.json()
        status: str = data.get("status", "unknown")
        if status != last_status:
            info(f"Job status: {status}")
            last_status = status
        if status == "succeeded":
            ok(f"Job completed in mock mode [OK]")
            return data
        if status == "failed":
            error_detail: Any = data.get("result") or data.get("error") or ""
            raise AssertionError(f"Job failed: {error_detail}")
        time.sleep(JOB_POLL_INTERVAL)
    raise AssertionError(f"Job did not complete within {JOB_POLL_TIMEOUT}s -- last status: {last_status}")


def verify_storage(s3: Any, job_id: str, pod5_key: str) -> None:
    step("Step 7 -- Verify storage (BAM exists, POD5 gone)")
    bam_key: str = f"results/anonymous/{job_id}/output.bam"

    # BAM must exist
    assert object_exists(s3, bam_key), f"BAM not found at s3://{MINIO_BUCKET}/{bam_key}"
    bam_size: int = s3.head_object(Bucket=MINIO_BUCKET, Key=bam_key)["ContentLength"]
    ok(f"BAM found: {bam_key} ({bam_size:,} bytes)")

    # POD5 must be deleted (ephemeral strategy)
    if object_exists(s3, pod5_key):
        fail(f"POD5 NOT deleted -- still at s3://{MINIO_BUCKET}/{pod5_key}")
        raise AssertionError("Ephemeral deletion failed: POD5 still present")
    ok(f"POD5 deleted (ephemeral [OK]): {pod5_key}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run() -> None:
    print(f"\n{BOLD}{'='*60}{RESET}")
    print(f"{BOLD}  Epitranscriptomics -- Researcher E2E Integration Test{RESET}")
    print(f"{BOLD}{'='*60}{RESET}")
    print(f"  Researcher uploads a POD5, gets a modification-tagged BAM.")
    print(f"  (MOCK mode -- no GPU required)\n")

    s3: Any = s3_client()
    failures: List[str] = []

    try:
        mock_pod5: bytes = make_mock_pod5()
        filename: str = "sample_reads.pod5"

        with httpx.Client(timeout=30) as client:
            check_health(client)
            init_data: Dict[str, Any] = init_upload(client, filename, len(mock_pod5))
            session_id: str           = init_data["session_id"]
            file_entry: Dict[str, Any] = init_data["files"][0]
            object_name: str          = file_entry["object_name"]
            upload_id: str            = file_entry["upload_id"]

            parts: List[Dict[str, Any]] = upload_file(client, init_data, mock_pod5)
            complete_upload(client, session_id, object_name, upload_id, parts)
            job_id: str               = submit_job(client, session_id, object_name)
            job_result: Dict[str, Any] = poll_job(client, job_id)

        verify_storage(s3, job_id, object_name)

        print(f"\n{GREEN}{BOLD}{'='*60}{RESET}")
        print(f"{GREEN}{BOLD}  ALL CHECKS PASSED [OK]{RESET}")
        print(f"{GREEN}{BOLD}{'='*60}{RESET}")
        result_data: Optional[Any] = job_result.get("result")
        if isinstance(result_data, str):
            try:
                result_data = json.loads(result_data)
            except Exception:
                pass
        if isinstance(result_data, dict):
            print(f"\n  BAM : {result_data.get('bam_object_name')}")
            print(f"  Size: {result_data.get('bam_size_bytes', '?'):,} bytes")
            print(f"  Time: {result_data.get('execution_time', '?'):.2f}s")
        print()

    except AssertionError as exc:
        failures.append(str(exc))
        fail(str(exc))
    except Exception as exc:
        failures.append(str(exc))
        fail(f"Unexpected error: {exc}")

    if failures:
        print(f"\n{RED}{BOLD}FAILED -- {len(failures)} error(s){RESET}\n")
        sys.exit(1)


if __name__ == "__main__":
    run()
