"""
Alignment Module - Maps basecalled reads to reference genome/transcriptome

Uses minimap2 to align FASTQ reads to a reference, producing a sorted BAM file.
This is the SECOND mandatory step - modification callers need genomic coordinates.
"""
from pathlib import Path
import subprocess
from typing import Optional


def _mock_align(fastq: Path, outdir: Path) -> Path:
    """Create mock alignment output for testing"""
    bam = outdir / "aligned.bam"
    bam.write_text("MOCK_ALIGNED_BAM_CONTENT\n")
    
    bai = outdir / "aligned.bam.bai"
    bai.write_text("MOCK_BAM_INDEX\n")
    
    return bam


def align(
    fastq: Path,
    reference: Path,
    workdir: Path,
    preset: str = "map-ont",
    threads: int = 4,
    mock: bool = False
) -> Path:
    """
    Align basecalled reads to reference using minimap2
    
    Args:
        fastq: Basecalled FASTQ file (can be .gz)
        reference: Reference genome/transcriptome FASTA
        workdir: Working directory for outputs
        preset: minimap2 preset (map-ont for DNA, splice for RNA)
        threads: Number of CPU threads
        mock: If True, create mock output instead of running minimap2
        
    Returns:
        Path to sorted, indexed BAM file
        
    Note:
        - For RNA data, use preset="splice" or "splice:hq"
        - Output BAM is automatically sorted and indexed
        - Requires samtools for BAM processing
    """
    outdir = workdir / "align"
    outdir.mkdir(exist_ok=True, parents=True)
    
    if mock:
        return _mock_align(fastq, outdir)
    
    # Ensure reference exists
    if not reference.exists():
        raise FileNotFoundError(f"Reference file not found: {reference}")
    
    bam_unsorted = outdir / "aligned_unsorted.bam"
    bam_sorted = outdir / "aligned.bam"
    
    print(f"Aligning {fastq.name} to {reference.name} with minimap2...")
    
    # Run minimap2 + samtools view in pipeline
    # minimap2 -ax map-ont ref.fa reads.fq | samtools view -bS > aligned.bam
    minimap_cmd = [
        "minimap2",
        "-ax", preset,
        "-t", str(threads),
        str(reference),
        str(fastq)
    ]
    
    samtools_cmd = [
        "samtools", "view",
        "-bS",  # BAM output
        "-@", str(threads),
        "-o", str(bam_unsorted)
    ]
    
    try:
        # Run minimap2
        minimap_proc = subprocess.Popen(
            minimap_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=False
        )
        
        # Pipe to samtools
        samtools_proc = subprocess.Popen(
            samtools_cmd,
            stdin=minimap_proc.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        minimap_proc.stdout.close()  # Allow minimap2 to receive SIGPIPE
        samtools_proc.communicate()
        
        if samtools_proc.returncode != 0:
            raise RuntimeError(f"Alignment failed with code {samtools_proc.returncode}")
            
    except Exception as e:
        raise RuntimeError(f"Alignment failed: {e}")
    
    # Sort BAM
    print("Sorting BAM...")
    subprocess.run([
        "samtools", "sort",
        "-@", str(threads),
        "-o", str(bam_sorted),
        str(bam_unsorted)
    ], check=True)
    
    # Index BAM
    print("Indexing BAM...")
    subprocess.run([
        "samtools", "index",
        "-@", str(threads),
        str(bam_sorted)
    ], check=True)
    
    # Clean up unsorted BAM
    bam_unsorted.unlink(missing_ok=True)
    
    if not bam_sorted.exists():
        raise FileNotFoundError(f"Alignment completed but BAM not found: {bam_sorted}")
    
    print(f"Alignment complete: {bam_sorted}")
    return bam_sorted
