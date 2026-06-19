from pathlib import Path
from typing import Dict


def run(fast5_dir: Path, ref_fa: str, workdir: Path, mock: bool = False) -> Dict[str, str]:
    # Placeholder: xPore expects per-condition BAM/fast5 indexes; implement when two conditions provided
    outdir = workdir / "xpore"
    outdir.mkdir(exist_ok=True)
    if mock:
        summary = outdir / "xpore_summary.json"
        summary.write_text("{\"status\": \"mocked\"}\n")
        return {"note": "mock xpore run", "summary": str(summary)}
    return {"note": "xpore requires two conditions; runner stub returns outdir"}
