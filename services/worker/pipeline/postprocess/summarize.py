import pandas as pd
from pathlib import Path
from typing import Dict, Any, List

def collect(results: Dict[str, Any], workdir: Path) -> Dict[str, str]:
    """
    Collect and merge results from multiple models while preserving provenance.
    
    Returns paths to both individual model outputs and a merged summary with 
    model source tracking for research reproducibility.
    """
    output_files: Dict[str, str] = {}
    combined: List[pd.DataFrame] = []
    
    # Process each model's results
    for model_name in ["cheui", "m6anet", "xpore"]:
        if model_name not in results:
            continue
            
        model_results = results[model_name]
        
        # Preserve individual model outputs
        if "sites_tsv" in model_results and model_results["sites_tsv"]:
            output_files[f"{model_name}_sites"] = model_results["sites_tsv"]
            
            # Load and add model provenance
            try:
                df = pd.read_csv(model_results["sites_tsv"], sep='\t')  # type: ignore[call-overload]
                
                # Add model source column if not present
                if 'model' not in df.columns:
                    df['model'] = model_name
                
                # Ensure modification_type exists (for older outputs)
                if 'modification_type' not in df.columns:
                    if model_name == "m6anet":
                        df['modification_type'] = 'm6A'
                    elif model_name == "cheui":
                        # CHEUI detects both, keep as-is or mark as 'unknown' if not specified
                        df['modification_type'] = df.get('modification_type', 'm6A/m5C')
                    else:
                        df['modification_type'] = 'unknown'
                
                combined.append(df)
            except Exception as e:
                # Log but continue - don't fail entire job for one model
                print(f"Warning: Could not process {model_name} results: {e}")
        
        # Preserve additional outputs (reads, BED files, etc.)
        if "reads_tsv" in model_results and model_results["reads_tsv"]:
            output_files[f"{model_name}_reads"] = model_results["reads_tsv"]
    
    # Create merged summary with full provenance
    if combined:
        merged = pd.concat(combined, ignore_index=True)
        
        # Sort by chromosome and position for easier analysis
        if 'chrom' in merged.columns and 'pos' in merged.columns:
            merged = merged.sort_values(['chrom', 'pos'], ignore_index=True)
        
        # Add summary metadata
        summary_out = workdir / "summary.sites.tsv"
        merged.to_csv(summary_out, sep='\t', index=False)
        output_files["summary_sites"] = str(summary_out)
        
        # Create a metadata file documenting the merge
        metadata_out = workdir / "summary.metadata.json"
        metadata = {
            "models_used": list(results.keys()),
            "total_sites": len(merged),
            "sites_per_model": merged['model'].value_counts().to_dict() if 'model' in merged.columns else {},
            "modification_types": merged['modification_type'].value_counts().to_dict() if 'modification_type' in merged.columns else {},
            "columns": list(merged.columns)
        }
        
        import json
        with open(metadata_out, 'w') as f:
            json.dump(metadata, f, indent=2)
        output_files["summary_metadata"] = str(metadata_out)
        
        return output_files
    
    return {"message": "no site files found"}