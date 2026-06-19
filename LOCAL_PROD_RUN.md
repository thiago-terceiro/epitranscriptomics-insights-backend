# Local Production-Like Run

This backend now supports two production-like local paths without auth:

1. FAST5 + FASTQ uploads for modification-model analysis.
2. POD5-only uploads for GPU Dorado basecalling.

## Prerequisites

- Docker Desktop with Linux containers enabled.
- NVIDIA Container Toolkit / Docker GPU support.
- A populated `.env` file copied from `.env.example`.

Recommended `.env` overrides:

```env
RABBITMQ_USER=guest
RABBITMQ_PASS=guest
RABBITMQ_HOST=rabbitmq
JWT_SECRET=replace-with-a-long-random-string
PIPELINE_MOCK_MODE=false
ALPHAFOLD3_MOCK_MODE=true
DORADO_DEVICE=cuda:all
```

## Start The Stack

```bash
cd backend
make prod-local-up
```

Useful follow-ups:

```bash
make prod-local-status
make prod-local-logs
```

## Supported Flow A: FAST5 + FASTQ Model Analysis

Use the frontend upload page or submit directly to `/jobs` with:

- one or more `.fast5` files plus a `.fastq`, `.fq`, `.fastq.gz`, or `.fq.gz`
- a downloadable HTTP(S) reference FASTA URL or a local path available inside the worker
- `m6anet` selected by default

Current behavior:

- uploaded FASTQ is reused instead of forcing a new basecall
- archives (`.tar`, `.tar.gz`, `.tgz`) are extracted in the worker before scanning for FAST5 and FASTQ
- missing real signal input fails fast with a clear error

## Supported Flow B: POD5 GPU Dorado Basecalling

Upload a single `.pod5` file.

The frontend now routes that submission to `/jobs/pod5`, which:

- queues the `worker_dorado` queue
- runs Dorado with GPU access
- uploads `results/{user}/{job_id}/output.bam`
- deletes the source POD5 from object storage after processing

This path does not require a reference FASTA.

## Known Limits

- Generic `/jobs` real analysis is intentionally limited to FAST5 + FASTQ inputs.
- CHEUI local execution now expects pre-provisioned assets under `/opt/cheui` or equivalent env overrides.
- xPore remains a stub and is not production-ready in this repository.
- AlphaFold and auth were intentionally left out of this local production run path.