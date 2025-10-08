from pathlib import Path

def run(fast5_dir: Path, ref_fa: str, workdir: Path):
    # Placeholder: xPore expects per-condition BAM/fast5 indexes; implement when two conditions provided
    outdir = workdir/"xpore"
    outdir.mkdir(exist_ok=True)
    return {"note":"xpore requires two conditions; runner stub returns outdir"}
