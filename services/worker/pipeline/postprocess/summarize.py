import pandas as pd
from pathlib import Path
from typing import Dict, Any, List

def collect(results: Dict[str, Any], workdir: Path) -> Dict[str, str]:
    combined: List[pd.DataFrame] = []
    if "cheui" in results and results["cheui"]["sites_tsv"]:
        df = pd.read_csv(results["cheui"]["sites_tsv"], sep='\t')  # type: ignore[call-overload]
        combined.append(df)
    if "m6anet" in results and results["m6anet"]["sites_tsv"]:
        df = pd.read_csv(results["m6anet"]["sites_tsv"], sep='\t')  # type: ignore[call-overload]
        combined.append(df)
    if combined:
        merged = pd.concat(combined, ignore_index=True)
        out = workdir/"summary.sites.tsv"
        merged.to_csv(out, sep='\t', index=False)
        return {"summary_sites": str(out)}
    return {"message": "no site files found"}