from pathlib import Path
import subprocess
from typing import Dict


def _mock_outputs(outdir: Path) -> Dict[str, str]:
    sites = outdir / "m6A_sites.tsv"
    # Include comprehensive columns for research use
    sites.write_text(
        "chrom\tpos\tref_base\tprobability\tmodification_type\tcoverage\tstrand\n"
        "chrMock\t200\tA\t0.88\tm6A\t32\t+\n"
        "chrMock\t350\tA\t0.76\tm6A\t21\t-\n"
    )
    return {"sites_tsv": str(sites)}


def run(fast5_dir: Path, fastq_gz: Path, ref_fa: str, workdir: Path, mock: bool = False) -> Dict[str, str]:
    outdir = workdir/"m6anet"
    outdir.mkdir(exist_ok=True)
    if mock:
        return _mock_outputs(outdir)
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
