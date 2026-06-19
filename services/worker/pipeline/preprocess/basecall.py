"""
Basecalling Module - Converts raw FAST5 signals to sequences

Uses Dorado (ONT's latest basecaller) to convert FAST5 files to FASTQ/BAM.
This is the FIRST mandatory step before any modification calling.
"""
import os
import shutil
from pathlib import Path
import subprocess
from typing import Tuple, Optional


DORADO_BIN = os.getenv("DORADO_BASECALL_BIN", os.getenv("DORADO_BIN", "dorado"))
DORADO_DEVICE = os.getenv("DORADO_DEVICE", "cuda:all")


def _mock_basecall(fast5_dir: Path, outdir: Path) -> Tuple[Path, Path]:
    """Create mock basecalling outputs for testing"""
    import gzip
    
    fastq = outdir / "calls.fastq.gz"
    bam = outdir / "calls.bam"
    
    # Create minimal FASTQ
    with gzip.open(fastq, "wt") as f:
        f.write("@mock_read_001\n")
        f.write("ACGTACGTACGTACGTACGT\n")
        f.write("+\n")
        f.write("!!!!!!!!!!!!!!!!!!!!\n")
    
    # Create empty BAM placeholder
    bam.write_text("MOCK_BAM_CONTENT\n")
    
    return fastq, bam


def basecall(
    fast5_dir: Path,
    workdir: Path,
    model: str = "rna004_130bps_sup@v5.0.0",
    reference: Optional[Path] = None,
    mock: bool = False
) -> Tuple[Path, Path]:
    """
    Basecall FAST5 files using Dorado
    
    Args:
        fast5_dir: Directory containing FAST5 files
        workdir: Working directory for outputs
        model: Dorado model name (default: RNA004 super-accurate)
        reference: Optional reference for alignment during basecalling
        mock: If True, create mock outputs instead of running Dorado
        
    Returns:
        Tuple of (fastq_path, bam_path)
        
    Note:
        - Dorado must be installed in the worker image or available on PATH.
        - Requires ~8GB RAM and GPU for fast basecalling.
        - Output is both FASTQ (sequences) and BAM (if reference provided).
    """
    outdir = workdir / "basecall"
    outdir.mkdir(exist_ok=True, parents=True)
    
    if mock:
        return _mock_basecall(fast5_dir, outdir)
    
    dorado_binary = shutil.which(DORADO_BIN) or DORADO_BIN
    cmd = [
        dorado_binary,
        "basecaller",
        "--device", DORADO_DEVICE,
        model,
        str(fast5_dir),
        "--output-dir", str(outdir),
        "--emit-fastq",
    ]
    
    # Add reference alignment if provided
    if reference and reference.exists():
        cmd.extend(["--reference", str(reference)])
    
    print(f"Running Dorado basecalling with model {model}...")
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
        )
        print(f"Dorado output: {result.stdout}")
    except subprocess.CalledProcessError as e:
        print(f"Dorado stderr: {e.stderr}")
        raise RuntimeError(f"Dorado basecalling failed: {e.stderr}")
    
    # Locate output files
    fastq = outdir / "calls.fastq"
    bam = outdir / "calls.bam"
    
    # Dorado may output uncompressed - compress if needed
    if not fastq.exists() and not (outdir / "calls.fastq.gz").exists():
        # Look for any FASTQ in output dir
        fastq_files = list(outdir.glob("*.fastq"))
        if fastq_files:
            fastq = fastq_files[0]
    
    # Compress FASTQ if not already compressed
    if fastq.exists() and not str(fastq).endswith(".gz"):
        print("Compressing FASTQ...")
        subprocess.run(["gzip", str(fastq)], check=True)
        fastq = Path(str(fastq) + ".gz")
    
    if not fastq.exists():
        raise FileNotFoundError(f"Basecalling completed but FASTQ not found in {outdir}")
    
    return fastq, bam if bam.exists() else None
