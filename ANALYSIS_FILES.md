# Required Files for Analysis

## Mock Mode (No Files Needed)

To run the pipeline without real data, use an accession starting with `mock`:

- `mock` — basic mock run
- `mock-m6a` — mock with m6A test data
- `mockSRR123` — any string prefixed with "mock"

The system **automatically generates** all required files at runtime:

- Mock FAST5 and FASTQ files
- Mock reference FASTA

This exercises the **full pipeline** (download → basecall → align → eventalign → models) using mock functions at each step. See `worker/pipeline/ingest/sra_download.py::_write_mock_payload`.

### UI Dev Mode Testing

In local development (`NODE_ENV=development`), the Upload page shows a **Dev Mode** toggle:

1. Enable "Dev Mode — Use Mock Data"
2. File upload is disabled, reference URL auto-fills
3. Sign in first (e.g., `testuser` / `testpass123`)
4. Click Submit → backend generates mock data

## Real Data Input Files

| Type | Extensions | Required | Description |
|------|------------|----------|-------------|
| FAST5 | `.fast5` | Yes* | Raw nanopore signal data (legacy ONT format) |
| POD5 | `.pod5` | Yes* | Raw nanopore signal data (newer, more compact ONT format) |
| FASTQ | `.fastq`, `.fq`, `.fastq.gz`, `.fq.gz` | Yes* | Sequencing reads (can be gzipped) |
| Reference FASTA | URL | Yes | Reference genome/transcriptome |

\* Upload requires: (FAST5 or POD5) **AND** FASTQ — or a single archive containing both

## Supported Archives

`.tar`, `.tar.gz` (bundles of FAST5/POD5/FASTQ)

### Optional: Generate Test FAST5

If you need a realistic FAST5 file for debugging tools like `h5py`, run:

```bash
python make_test_fast5.py
```

This creates `test_fast5_realistic.fast5` with proper ONT structure.

### Real Data

For production runs, provide either:

1. **SRA Accession** — e.g., `SRR12345678` (downloads from NCBI SRA)
2. **Upload Session** — upload your own files via the web UI, which creates an upload session ID automatically
