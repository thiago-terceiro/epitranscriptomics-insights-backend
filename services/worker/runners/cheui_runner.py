from pathlib import Path
import os
import subprocess
from typing import Dict, Optional
import pandas as pd


CHEUI_PYTHON = os.getenv("CHEUI_PYTHON", "python")
CHEUI_PREPROCESS_SCRIPT = Path(os.getenv("CHEUI_PREPROCESS_SCRIPT", "/opt/cheui/scripts/CHEUI_preprocess_m6A.py"))
CHEUI_PREDICT_SCRIPT = Path(os.getenv("CHEUI_PREDICT_SCRIPT", "/opt/cheui/scripts/CHEUI_predict_model1.py"))
CHEUI_KMER_MODEL = Path(os.getenv("CHEUI_KMER_MODEL", "/opt/cheui/kmer_models/model_kmer.csv"))
CHEUI_MODEL1 = Path(os.getenv("CHEUI_MODEL1", "/opt/cheui/CHEUI_trained_models/CHEUI_m6A_model1.h5"))


def _ensure_cheui_assets() -> None:
    required = [CHEUI_PREPROCESS_SCRIPT, CHEUI_PREDICT_SCRIPT, CHEUI_KMER_MODEL, CHEUI_MODEL1]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise FileNotFoundError(
            "CHEUI assets are not available in the worker container. Missing: " + ", ".join(missing)
        )


def _mock_outputs(outdir: Path) -> Dict[str, str]:
    """Mock outputs for testing - same format as before"""
    sites = outdir / "sites.tsv"
    reads = outdir / "reads.tsv"
    # Include comprehensive columns for research use
    sites.write_text(
        "chrom\tpos\tref_base\tprobability\tmodification_type\tcoverage\tstrand\n"
        "chrMock\t100\tA\t0.95\tm6A\t25\t+\n"
        "chrMock\t450\tC\t0.88\tm5C\t18\t+\n"
    )
    reads.write_text(
        "read_id\tprobability\tmodification_type\tposition\n"
        "mock_read_001\t0.92\tm6A\t100\n"
        "mock_read_002\t0.85\tm5C\t450\n"
    )
    return {"sites_tsv": str(sites), "reads_tsv": str(reads)}


def run(
    fast5_dir: Path,
    fastq_gz: Optional[Path],
    ref_fa: str,
    workdir: Path,
    eventalign_txt: Optional[Path] = None,
    mock: bool = False
) -> Dict[str, str]:
    """
    Run CHEUI modification caller with proper preprocessing
    
    Args:
        fast5_dir: Directory with FAST5 files (REQUIRED - contains raw signal)
        fastq_gz: Basecalled FASTQ (will be generated if None)
        ref_fa: Reference transcriptome FASTA
        workdir: Working directory
        eventalign_txt: Pre-computed eventalign file (will be generated if None)
        mock: If True, skip processing and return mock data
        
    Returns:
        Dict with paths to sites.tsv and reads.tsv
        
    Note:
        This is the PROPER implementation that runs all required steps:
        FAST5 → basecall → align → eventalign → CHEUI preprocess → CHEUI predict
    """
    outdir = workdir / "cheui"
    outdir.mkdir(exist_ok=True)
    
    if mock:
        return _mock_outputs(outdir)
    
    ref_path = Path(ref_fa)
    if not ref_path.exists():
        raise FileNotFoundError(f"Reference not found: {ref_fa}")
    
    # If eventalign not provided, we need to run the full preprocessing pipeline
    if eventalign_txt is None:
        print("Running full preprocessing pipeline (basecall → align → eventalign)...")
        
        # Import preprocessing modules
        from worker.pipeline.preprocess import basecall, align, eventalign as ea
        
        # Step 1: Basecalling (if FASTQ not provided)
        if fastq_gz is None:
            print("Step 1/3: Basecalling with Dorado...")
            fastq_gz, _ = basecall.basecall(
                fast5_dir=fast5_dir,
                workdir=workdir,
                reference=ref_path,
                mock=False
            )
        
        # Step 2: Alignment
        print("Step 2/3: Aligning with minimap2...")
        aligned_bam = align.align(
            fastq=fastq_gz,
            reference=ref_path,
            workdir=workdir,
            preset="splice",  # Use splice for RNA
            mock=False
        )
        
        # Step 3: Eventalign
        print("Step 3/3: Eventalign with f5c...")
        eventalign_txt = ea.eventalign(
            fast5_dir=fast5_dir,
            fastq=fastq_gz,
            bam=aligned_bam,
            reference=ref_path,
            workdir=workdir,
            rna=True,
            mock=False
        )
    
    # Now run CHEUI with the eventalign output
    print("Running CHEUI preprocessing...")
    cheui_input = outdir / "cheui_input.p"
    _ensure_cheui_assets()
    
    # CHEUI step 1: Preprocess eventalign into kmer format
    subprocess.run([
        CHEUI_PYTHON,
        str(CHEUI_PREPROCESS_SCRIPT),
        "-i", str(eventalign_txt),
        "-m", str(CHEUI_KMER_MODEL),
        "-o", str(cheui_input),
        "-n", "4"  # threads
    ], check=True)
    
    print("Running CHEUI prediction...")
    probs_file = outdir / "probs.txt"
    
    # CHEUI step 2: Run prediction model
    subprocess.run([
        CHEUI_PYTHON,
        str(CHEUI_PREDICT_SCRIPT),
        "-i", str(cheui_input),
        "-m", str(CHEUI_MODEL1),
        "-o", str(probs_file)
    ], check=True)
    
    # Convert CHEUI output to standardized format
    print("Converting CHEUI output to standard format...")
    sites_tsv = outdir / "sites.tsv"
    reads_tsv = outdir / "reads.tsv"
    
    # Parse CHEUI probabilities
    df = pd.read_csv(probs_file, sep='\t', comment='#')
    df['modification_type'] = 'm6A'
    df['model'] = 'CHEUI'
    
    # Aggregate per-position probabilities
    sites = df.groupby('position').agg(
        probability=('probability', 'mean'),
        coverage=('read_id', 'count'),
        ref_base=('kmer', lambda x: x.str[2].iloc[0] if len(x) > 0 else 'N'),
        strand=('strand', lambda x: x.iloc[0] if len(x) > 0 else '+'),
        modification_type=('modification_type', 'first')
    ).reset_index()
    
    sites['chrom'] = df['contig'].iloc[0] if 'contig' in df.columns else 'unknown'
    sites = sites[['chrom', 'position', 'ref_base', 'probability', 
                   'modification_type', 'coverage', 'strand']]
    sites.columns = ['chrom', 'pos', 'ref_base', 'probability', 
                     'modification_type', 'coverage', 'strand']
    sites.to_csv(sites_tsv, sep='\t', index=False)
    
    # Per-read probabilities
    reads = df[['read_id', 'probability', 'modification_type', 'position']]
    reads.to_csv(reads_tsv, sep='\t', index=False)
    
    print(f"CHEUI complete! Output:\n  {sites_tsv}\n  {reads_tsv}")
    
    return {
        "sites_tsv": str(sites_tsv),
        "reads_tsv": str(reads_tsv)
    }
