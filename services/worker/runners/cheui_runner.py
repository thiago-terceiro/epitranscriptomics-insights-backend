from pathlib import Path
import subprocess

def run(fast5_dir: Path, fastq_gz: Path, ref_fa: str, workdir: Path):
    outdir = workdir/"cheui"
    outdir.mkdir(exist_ok=True)
    # Assume a CHEUI container is available as ghcr.io/yourorg/cheui:1.0
    cmd = [
        "docker","run","--rm",
        "-v",f"{workdir}:{workdir}",
        "ghcr.io/yourorg/cheui:1.0",
        "cheui","call",
        "--fast5", str(fast5_dir),
        "--fastq", str(fastq_gz),
        "--reference", ref_fa,
        "--outdir", str(outdir)
    ]
    subprocess.run(cmd, check=True)
    return {
        "sites_tsv": str(outdir/"sites.tsv"),
        "reads_tsv": str(outdir/"reads.tsv")
    }
