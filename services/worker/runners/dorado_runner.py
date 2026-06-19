"""
Dorado basecaller runner.

Downloads a POD5 from MinIO, runs Dorado basecalling to produce a
modification-tagged BAM (ML tags for m6A etc.), uploads the BAM back to
MinIO, then deletes the source POD5.

This implements the "ephemeral processing" strategy:
  POD5 (large) ──Dorado──> BAM (10-50× smaller, keeps mod probabilities)
  POD5 is deleted immediately after basecalling.
"""
import logging
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration (all override-able via environment)
# ---------------------------------------------------------------------------
DORADO_BIN: str = os.getenv("DORADO_BIN", "/opt/dorado/bin/dorado")
DORADO_MODEL: str = os.getenv("DORADO_MODEL", "dna_r10.4.1_e8.2_400bps_hac@v4.3.0")
DORADO_DEVICE: str = os.getenv("DORADO_DEVICE", "cuda:all")

# When true, skip real Dorado and write a minimal synthetic BAM (local testing without GPU)
MOCK_MODE: bool = os.getenv("PIPELINE_MOCK_MODE", "false").lower() == "true"

# Minimal valid BAM: magic + empty gzip block (samtools-parseable header)
_MOCK_BAM_BYTES = (
    b"BAM\x01"          # magic
    + b"\x1f\x8b\x08\x04\x00\x00\x00\x00\x00\xff\x06\x00BC\x02\x00\x1b\x00"
    + b"\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00"  # empty BGZF block
)

_MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "minio:9000")
_MINIO_ACCESS_KEY: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
_MINIO_SECRET_KEY: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
_MINIO_REGION: str = os.getenv("MINIO_REGION", "us-east-1")
_MINIO_SECURE: bool = os.getenv("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET: str = os.getenv("MINIO_BUCKET", "epimod")

_SCHEME: str = "https" if _MINIO_SECURE else "http"

# boto3 client for worker-side MinIO operations (no presigned URLs needed here)
_s3 = boto3.client(
    "s3",
    endpoint_url=f"{_SCHEME}://{_MINIO_ENDPOINT}",
    aws_access_key_id=_MINIO_ACCESS_KEY,
    aws_secret_access_key=_MINIO_SECRET_KEY,
    region_name=_MINIO_REGION,
    config=Config(signature_version="s3v4"),
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _ensure_bucket() -> None:
    """Create the bucket if it does not yet exist (idempotent)."""
    try:
        _s3.head_bucket(Bucket=MINIO_BUCKET)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchBucket", "NotFound"}:
            _s3.create_bucket(Bucket=MINIO_BUCKET)
        else:
            raise


def _download_object(object_name: str, dest: Path) -> None:
    """Download a MinIO object to a local path."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Downloading s3://%s/%s → %s", MINIO_BUCKET, object_name, dest)
    _s3.download_file(MINIO_BUCKET, object_name, str(dest))


def _upload_object(local_path: Path, object_name: str) -> None:
    """Upload a local file to MinIO."""
    _ensure_bucket()
    logger.info("Uploading %s → s3://%s/%s", local_path, MINIO_BUCKET, object_name)
    _s3.upload_file(str(local_path), MINIO_BUCKET, object_name)


def _delete_object(object_name: str) -> None:
    """Delete an object from MinIO (best-effort; logs but does not raise)."""
    try:
        _s3.delete_object(Bucket=MINIO_BUCKET, Key=object_name)
        logger.info("Deleted s3://%s/%s", MINIO_BUCKET, object_name)
    except (ClientError, BotoCoreError) as exc:
        logger.warning("Could not delete s3://%s/%s: %s", MINIO_BUCKET, object_name, exc)


# ---------------------------------------------------------------------------
# Command builder
# ---------------------------------------------------------------------------

def build_dorado_command(
    pod5_path: Path,
    output_dir: Path,
    reference_path: Optional[Path] = None,
    modified_bases: bool = True,
) -> List[str]:
    """Build the Dorado basecaller CLI command.

    Args:
        pod5_path: Path to the input POD5 file.
        output_dir: Directory where Dorado writes the output BAM.
        reference_path: Optional reference FASTA for aligned basecalling.
        modified_bases: Whether to emit modification tags (ML/MM).

    Returns:
        Command as a list of strings ready for subprocess.
    """
    cmd: List[str] = [
        DORADO_BIN,
        "basecaller",
        "--device", DORADO_DEVICE,
        "--emit-moves",
        DORADO_MODEL,
        str(pod5_path),
        "--output-dir", str(output_dir),
    ]
    if modified_bases:
        cmd += ["--modified-bases", "m6A_DRACH"]
    if reference_path is not None:
        cmd += ["--reference", str(reference_path)]
    return cmd


# ---------------------------------------------------------------------------
# Basecalling orchestration
# ---------------------------------------------------------------------------

def run_dorado_basecall(
    pod5_path: Path,
    workdir: Path,
    reference_path: Optional[Path] = None,
) -> Path:
    """Run Dorado on a local POD5 file and return the output BAM path.

    Args:
        pod5_path: Local POD5 file.
        workdir: Directory for Dorado output.
        reference_path: Optional reference FASTA.

    Returns:
        Path to the produced BAM file.

    Raises:
        subprocess.CalledProcessError: If Dorado exits non-zero.
        FileNotFoundError: If no BAM is found after Dorado runs.
    """
    bam_dir = workdir / "bam_out"
    bam_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Mock mode: skip real Dorado, write a synthetic BAM for local tests
    # ------------------------------------------------------------------
    if MOCK_MODE:
        mock_bam = bam_dir / "mock_output.bam"
        mock_bam.write_bytes(_MOCK_BAM_BYTES)
        logger.info("MOCK_MODE: synthetic BAM written to %s", mock_bam)
        return mock_bam

    cmd = build_dorado_command(pod5_path, bam_dir, reference_path=reference_path)
    logger.info("Running Dorado: %s", " ".join(cmd))

    result = subprocess.run(
        cmd,
        check=True,
        capture_output=True,
        text=True,
    )
    if result.stdout:
        logger.debug("Dorado stdout: %s", result.stdout[:2000])
    if result.stderr:
        logger.debug("Dorado stderr: %s", result.stderr[:2000])

    bam_files = list(bam_dir.glob("*.bam"))
    if not bam_files:
        raise FileNotFoundError(f"Dorado produced no BAM file in {bam_dir}")

    # Dorado typically writes a single BAM; if multiple, take the largest.
    bam_path = max(bam_files, key=lambda p: p.stat().st_size)
    logger.info("Dorado produced BAM: %s (%.1f MB)", bam_path, bam_path.stat().st_size / 1e6)
    return bam_path


# ---------------------------------------------------------------------------
# Public entry point (called by Celery task)
# ---------------------------------------------------------------------------

def run_uploaded_pod5(
    job_id: str,
    pod5_object_name: str,
    user_id: str,
    reference_object_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Full pipeline: download POD5 → basecall → upload BAM → delete POD5.

    Args:
        job_id: Job identifier (used to namespace MinIO output path).
        pod5_object_name: MinIO object key of the uploaded POD5.
        user_id: Owner identifier for output path namespacing.
        reference_object_name: Optional MinIO object key for reference FASTA.

    Returns:
        Dict with ``bam_object_name`` and basic stats.
    """
    workdir = Path(tempfile.mkdtemp(prefix=f"dorado_{job_id}_"))
    pod5_deleted = False

    try:
        # 1. Download POD5
        pod5_filename = Path(pod5_object_name).name
        local_pod5 = workdir / pod5_filename
        _download_object(pod5_object_name, local_pod5)

        # 2. Optionally download reference
        local_ref: Optional[Path] = None
        if reference_object_name:
            ref_filename = Path(reference_object_name).name
            local_ref = workdir / ref_filename
            _download_object(reference_object_name, local_ref)

        # 3. Run Dorado
        bam_path = run_dorado_basecall(local_pod5, workdir, reference_path=local_ref)

        # 4. Upload BAM to MinIO
        bam_object_name = f"results/{user_id}/{job_id}/output.bam"
        _upload_object(bam_path, bam_object_name)

        # 5. Delete source POD5 (ephemeral strategy - free storage)
        _delete_object(pod5_object_name)
        pod5_deleted = True

        return {
            "bam_object_name": bam_object_name,
            "pod5_deleted": pod5_deleted,
            "bam_size_bytes": bam_path.stat().st_size,
        }

    except Exception:
        # Best-effort POD5 cleanup even on failure to free storage
        if not pod5_deleted:
            _delete_object(pod5_object_name)
        raise

    finally:
        # Always clean up local temp dir
        try:
            shutil.rmtree(workdir, ignore_errors=True)
        except Exception as cleanup_exc:
            logger.warning("Failed to remove workdir %s: %s", workdir, cleanup_exc)
