import subprocess
from pathlib import Path

def download(accession: str, outdir: Path):
    fast5_dir = outdir/"fast5"
    fastq = outdir/"reads.fastq.gz"
    fast5_dir.mkdir(parents=True, exist_ok=True)
    # Prefetch & fasterq-dump (SRA tools) – falls back to ENA if needed
    subprocess.run(["prefetch", accession], check=True)
    subprocess.run(["fasterq-dump", accession, "-O", str(outdir)], check=True)
    # gzip fastq
    subprocess.run(["pigz", "-f", str(outdir/f"{accession}.fastq")], check=True)
    (outdir/f"{accession}.fastq.gz").replace(fastq)
    return fast5_dir, fastq
