import gzip
import subprocess
import tarfile
from pathlib import Path
from typing import Optional, Tuple


def _write_mock_payload(accession: str, fast5_dir: Path, fastq: Path) -> Tuple[Path, Path, Path]:
    """Create lightweight mock inputs so the pipeline can run end-to-end locally."""
    mock_fast5 = fast5_dir / f"{accession}_read.fast5"
    mock_fast5.write_text("MOCK_FAST5_CONTENT\n")

    fastq.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(fastq, "wt") as handle:
        handle.write("@mock_read\n")
        handle.write("ACGTACGTACGT\n")
        handle.write("+\n")
        handle.write("!!!!!!!!!!!!\n")

    # Create mock reference FASTA so preprocessing can run
    mock_ref = fastq.parent / "mock_reference.fa"
    mock_ref.write_text(">chrMock\nACGTACGTACGTACGTACGT\n")

    return fast5_dir, fastq, mock_ref


def _download_from_minio(session_id: str, user_id: str, outdir: Path) -> Tuple[Path, Path]:
    """
    Download user-uploaded files from MinIO storage
    
    Args:
        session_id: Upload session ID
        user_id: User identifier
        outdir: Local directory to download files to
        
    Returns:
        Tuple of (fast5_dir, fastq_path)
    """
    from worker.api.storage.upload_manager import UploadSession
    
    fast5_dir = outdir / "fast5"
    staging_dir = outdir / "uploads"
    extract_dir = outdir / "extracted"
    fast5_dir.mkdir(parents=True, exist_ok=True)
    staging_dir.mkdir(parents=True, exist_ok=True)
    extract_dir.mkdir(parents=True, exist_ok=True)
    
    # List all files in the upload session
    object_names = UploadSession.list_session_files(session_id, user_id)
    
    if not object_names:
        raise ValueError(f"No files found in upload session {session_id}")
    
    # Download each file
    fastq_path: Optional[Path] = None
    
    def register_extracted_files(root: Path) -> Optional[Path]:
        nonlocal fastq_path

        for extracted_file in root.rglob("*"):
            if not extracted_file.is_file():
                continue

            suffixes = extracted_file.name.lower()
            if suffixes.endswith(".fast5"):
                destination = fast5_dir / extracted_file.name
                if destination != extracted_file:
                    destination.write_bytes(extracted_file.read_bytes())
            elif suffixes.endswith((".fastq", ".fastq.gz", ".fq", ".fq.gz")) and fastq_path is None:
                fastq_path = outdir / extracted_file.name
                if fastq_path != extracted_file:
                    fastq_path.write_bytes(extracted_file.read_bytes())

        return fastq_path

    for object_name in object_names:
        filename = Path(object_name).name
        lower_name = filename.lower()
        
        # Determine file type and destination
        if lower_name.endswith(".fast5"):
            local_path = fast5_dir / filename
        elif lower_name.endswith((".fastq", ".fastq.gz", ".fq", ".fq.gz")):
            local_path = outdir / filename
            fastq_path = local_path
        elif lower_name.endswith((".tar", ".tar.gz", ".tgz")):
            local_path = staging_dir / filename
        else:
            # Skip unsupported files
            continue
        
        # Download from MinIO
        UploadSession.download_to_worker(object_name, str(local_path))

        if lower_name.endswith((".tar", ".tar.gz", ".tgz")):
            archive_extract_dir = extract_dir / filename.replace(".tar.gz", "").replace(".tgz", "").replace(".tar", "")
            archive_extract_dir.mkdir(parents=True, exist_ok=True)
            with tarfile.open(local_path, "r:*") as archive:
                archive.extractall(archive_extract_dir)
            register_extracted_files(archive_extract_dir)
    
    # If no FASTQ found, create empty one for compatibility
    if not fastq_path:
        fastq_path = outdir / "reads.fastq.gz"
        with gzip.open(fastq_path, "wt") as handle:
            handle.write("")
    
    return fast5_dir, fastq_path


def download(
    accession: str,
    outdir: Path,
    mock: bool = False,
    user_id: Optional[str] = None,
    upload_session_id: Optional[str] = None,
) -> Tuple[Path, Path, Optional[Path]]:
    """
    Download or retrieve sequencing data files
    
    Supports three modes:
    1. Mock mode - Creates dummy files for testing (includes mock reference)
    2. User uploads - Downloads from MinIO (accession starts with "upload-")
    3. SRA downloads - Downloads from NCBI SRA
    
    Args:
        accession: SRA accession (e.g., "SRR000001") or upload session ID (e.g., "upload-12345-abc")
        outdir: Output directory for downloaded files
        mock: If True, create mock files instead of downloading
        user_id: Required when accession is an upload session ID
        
    Returns:
        Tuple of (fast5_dir, fastq_path, mock_reference_path or None)
    """
    fast5_dir = outdir / "fast5"
    fastq = outdir / "reads.fastq.gz"
    fast5_dir.mkdir(parents=True, exist_ok=True)

    # Mock mode
    if mock or accession.startswith("mock"):
        return _write_mock_payload(accession, fast5_dir, fastq)
    
    # User upload mode
    if upload_session_id or accession.startswith("upload:") or accession.startswith("upload-"):
        if not user_id:
            raise ValueError("user_id is required when downloading from upload session")
        session_identifier = upload_session_id or accession
        if session_identifier.startswith("upload:"):
            session_identifier = session_identifier.split(":", 1)[1]
        fast5, fq = _download_from_minio(session_identifier, user_id, outdir)
        return fast5, fq, None

    # SRA download mode
    # Prefetch & fasterq-dump (SRA tools) – falls back to ENA if needed
    subprocess.run(["prefetch", accession], check=True)
    subprocess.run(["fasterq-dump", accession, "-O", str(outdir)], check=True)
    # gzip fastq
    subprocess.run(["pigz", "-f", str(outdir / f"{accession}.fastq")], check=True)
    (outdir / f"{accession}.fastq.gz").replace(fastq)
    return fast5_dir, fastq, None
