"""
Eventalign Module - Re-attaches raw signal to genomic positions

Uses f5c (or nanopolish) to create eventalign output that connects:
- Raw FAST5 signal values
- To specific genomic positions from the alignment

This is the THIRD and MOST CRITICAL step - without this, modification
callers have no signal data to compare against expected currents.
"""
from pathlib import Path
import subprocess
from typing import Optional


def _mock_eventalign(outdir: Path) -> Path:
    """Create mock eventalign output for testing"""
    eventalign = outdir / "eventalign.txt"
    
    # Minimal eventalign format
    eventalign.write_text(
        "contig\tposition\treference_kmer\tread_index\tstrand\tevent_index\t"
        "event_level_mean\tevent_stdv\tevent_length\tmodel_kmer\tmodel_mean\t"
        "model_stdv\tstandardized_level\tstart_idx\tend_idx\n"
        "chrMock\t100\tAAAAA\t0\tt\t5\t65.2\t1.2\t0.005\tAAAAA\t64.8\t2.1\t0.15\t100\t110\n"
        "chrMock\t101\tAAAAC\t0\tt\t6\t68.1\t1.5\t0.006\tAAAAC\t67.5\t2.0\t0.20\t110\t120\n"
    )
    
    return eventalign


def eventalign(
    fast5_dir: Path,
    fastq: Path,
    bam: Path,
    reference: Path,
    workdir: Path,
    threads: int = 4,
    rna: bool = True,
    mock: bool = False
) -> Path:
    """
    Run f5c eventalign to re-attach signal to genomic positions
    
    Args:
        fast5_dir: Directory containing original FAST5 files
        fastq: Basecalled FASTQ file
        bam: Aligned BAM file (sorted and indexed)
        reference: Reference genome/transcriptome FASTA
        workdir: Working directory for outputs
        threads: Number of CPU threads
        rna: True for RNA data (use --rna flag)
        mock: If True, create mock output instead of running f5c
        
    Returns:
        Path to eventalign.txt file
        
    Note:
        - f5c is much faster than nanopolish (10-50x speedup)
        - Output file can be very large (GBs for full datasets)
        - This step requires the ORIGINAL FAST5 files - you cannot skip them!
    """
    outdir = workdir / "eventalign"
    outdir.mkdir(exist_ok=True, parents=True)
    
    if mock:
        return _mock_eventalign(outdir)
    
    eventalign_output = outdir / "eventalign.txt"
    
    # Step 1: Index FAST5 files with f5c
    print("Indexing FAST5 files...")
    index_cmd = [
        "f5c", "index",
        "-d", str(fast5_dir),
        str(fastq)
    ]
    
    try:
        subprocess.run(index_cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"f5c index failed: {e.stderr}")
    
    # Step 2: Run eventalign
    print(f"Running f5c eventalign (this may take a while)...")
    eventalign_cmd = [
        "f5c", "eventalign",
        "-r", str(fastq),
        "-b", str(bam),
        "-g", str(reference),
        "-t", str(threads),
        "--signal-index",  # Use signal index for speed
        "--scale-events"   # Normalize events
    ]
    
    if rna:
        eventalign_cmd.append("--rna")
    
    try:
        # Run and redirect stdout to file
        with open(eventalign_output, "w") as outfile:
            result = subprocess.run(
                eventalign_cmd,
                stdout=outfile,
                stderr=subprocess.PIPE,
                text=True,
                check=True
            )
        
        print(f"f5c completed: {eventalign_output}")
        
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"f5c eventalign failed: {e.stderr}")
    
    if not eventalign_output.exists() or eventalign_output.stat().st_size == 0:
        raise FileNotFoundError(f"Eventalign output not created or empty: {eventalign_output}")
    
    return eventalign_output


def eventalign_docker(
    fast5_dir: Path,
    fastq: Path,
    bam: Path,
    reference: Path,
    workdir: Path,
    threads: int = 4,
    rna: bool = True,
    mock: bool = False
) -> Path:
    """Backward-compatible wrapper for the previous Docker-based API."""
    return eventalign(
        fast5_dir=fast5_dir,
        fastq=fastq,
        bam=bam,
        reference=reference,
        workdir=workdir,
        threads=threads,
        rna=rna,
        mock=mock,
    )
