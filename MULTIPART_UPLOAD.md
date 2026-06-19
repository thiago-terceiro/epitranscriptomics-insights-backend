# Multipart Upload Flow (MinIO/S3)

This document explains the new multipart upload pipeline used to keep memory low when handling large sequencing files.

## Overview
- Clients upload directly to MinIO using S3-compatible multipart uploads with presigned part URLs.
- The backend creates and completes multipart uploads; the frontend/Next.js API slices files into parts and streams them to the presigned URLs.
- Default part size is 8MB (never below 5MB), scaled up automatically to stay under the 10k-part S3 limit.

## Key Endpoints (backend)
- `POST /upload/init`: creates an upload session, one multipart upload per file, returns `session_id`, `part_size`, and per-file `{object_name, upload_id, filename, size}`.
- `POST /upload/part-url`: returns a presigned URL for a given `{session_id, object_name, upload_id, part_number}`.
- `POST /upload/complete`: accepts `{session_id, files: [{object_name, upload_id, parts:[{part_number, etag}]}]}` and finalizes each multipart upload.
- `GET /upload/session/{session_id}`: lists expected/uploaded objects and includes `part_size` for resume tools.

## Frontend/Next.js Flow
- `app/api/jobs/submit/route.ts` now:
  1. Validates form data (size, extension, non-empty files, models array).
  2. Calls `/upload/init` to obtain `session_id`, `part_size`, and file metadata.
  3. For each file, slices it into `part_size` chunks, fetches a presigned part URL (`/upload/part-url`) per chunk, uploads via `PUT`, and captures the returned ETag.
  4. Calls `/upload/complete` with the part list for each file.
  5. Submits the job with `upload_session_id` so workers can fetch data from MinIO.
- Proxy helpers under `app/api/upload/part-url` and `app/api/upload/complete` forward requests to the backend.

## Libraries
- **boto3**: used in the backend to create/complete multipart uploads and generate presigned part URLs against MinIO (S3-compatible with SigV4).
- **minio** Python SDK: still used for bucket existence checks, listing objects, and verification.
- **Next.js fetch**: used server-side to request presigned URLs and stream file slices directly to MinIO.

## Design Rationale
- Multipart uploads avoid loading whole files into memory; each chunk is uploaded directly to storage.
- Presigned URLs keep the app server out of the data path while preserving control over upload/session state.
- Part-size negotiation keeps part counts within S3 limits (10k parts, 5MB minimum).

## Known Limitations / Follow-ups
- Uploads run sequentially; adding bounded parallelism would improve throughput.
- Failed uploads are not auto-aborted; an abort step could reclaim storage sooner.
- Endpoints are anonymous; if user scoping is needed, tie sessions to auth and enforce ownership.
- The server assumes non-empty files; empty files are rejected early.
