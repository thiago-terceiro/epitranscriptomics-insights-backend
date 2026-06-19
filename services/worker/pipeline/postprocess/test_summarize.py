"""Test suite for the improved summarize module with model provenance tracking."""

import tempfile
from pathlib import Path
import pandas as pd
from worker.pipeline.postprocess.summarize import collect


def test_provenance_tracking():
    """Test that model source is properly tracked in merged results."""
    with tempfile.TemporaryDirectory() as tmpdir:
        workdir = Path(tmpdir)
        
        # Create mock CHEUI output
        cheui_dir = workdir / "cheui"
        cheui_dir.mkdir()
        cheui_sites = cheui_dir / "sites.tsv"
        cheui_sites.write_text(
            "chrom\tpos\tref_base\tprobability\tmodification_type\tcoverage\tstrand\n"
            "chr1\t100\tA\t0.95\tm6A\t25\t+\n"
            "chr1\t450\tC\t0.88\tm5C\t18\t+\n"
        )
        
        # Create mock m6Anet output
        m6anet_dir = workdir / "m6anet"
        m6anet_dir.mkdir()
        m6anet_sites = m6anet_dir / "sites.tsv"
        m6anet_sites.write_text(
            "chrom\tpos\tref_base\tprobability\tmodification_type\tcoverage\tstrand\n"
            "chr1\t200\tA\t0.88\tm6A\t32\t+\n"
        )
        
        # Simulate results structure
        results = {
            "cheui": {"sites_tsv": str(cheui_sites)},
            "m6anet": {"sites_tsv": str(m6anet_sites)}
        }
        
        # Run collection
        output = collect(results, workdir)
        
        # Verify outputs
        assert "cheui_sites" in output
        assert "m6anet_sites" in output
        assert "summary_sites" in output
        assert "summary_metadata" in output
        
        # Check merged file has model provenance
        merged_df = pd.read_csv(output["summary_sites"], sep='\t')
        
        assert "model" in merged_df.columns, "Model column missing!"
        assert "modification_type" in merged_df.columns, "Modification type missing!"
        
        # Verify model tracking
        assert set(merged_df["model"].unique()) == {"cheui", "m6anet"}
        
        # Verify 3 total sites (2 CHEUI + 1 m6Anet)
        assert len(merged_df) == 3
        
        # Check sorting by position
        assert merged_df["pos"].tolist() == [100, 200, 450]
        
        print("✓ All provenance tracking tests passed!")
        print(f"\nMerged summary preview:\n{merged_df.to_string()}")
        
        # Check metadata file
        import json
        with open(output["summary_metadata"], 'r') as f:
            metadata = json.load(f)
        
        assert metadata["models_used"] == ["cheui", "m6anet"]
        assert metadata["total_sites"] == 3
        assert metadata["sites_per_model"]["cheui"] == 2
        assert metadata["sites_per_model"]["m6anet"] == 1
        
        print(f"\nMetadata:\n{json.dumps(metadata, indent=2)}")
        print("\n✓ All tests passed! Research-ready output confirmed.")


if __name__ == "__main__":
    test_provenance_tracking()
