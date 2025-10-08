from pathlib import Path
import subprocess

def run(fast5_dir: Path, fastq_gz: Path, ref_fa: str, workdir: Path):
    outdir = workdir/"m6anet"
    outdir.mkdir(exist_ok=True)
    cmd = [
        "m6anet", "infer",
        "--fastq", str(fastq_gz),
        "--fast5", str(fast5_dir),
        "--reference", ref_fa,
        "--out", str(outdir)
    ]
    subprocess.run(cmd, check=True)
    return {
        "sites_tsv": str(outdir/"m6A_sites.tsv")
    }
