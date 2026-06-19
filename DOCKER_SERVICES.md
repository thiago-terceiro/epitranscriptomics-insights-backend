# Docker Services Overview

Purpose: quick orientation for anyone maintaining the stack without deep infra knowledge. Sources: infra/docker-compose.yml (prod-ish), infra/docker-compose.local.yml (dev), Dockerfile.api, Dockerfile.worker, infra/helm/values.yaml.

## Runtime services

| Service | Image (tag) | What it does | How we use it |
| --- | --- | --- | --- |
| api | built from Dockerfile.api (python:3.11-slim base) 

| FastAPI + Gunicorn HTTP API | Exposes port 8080, depends on DB/cache/queue/object store/MLflow. Prod replicas: 1 (compose) / 2 (helm). Healthcheck on /health. |

| worker | built from Dockerfile.worker (python:3.11-slim base) | Celery workers for pipelines | In compose: scaled (3) plus priority variants; in local: single worker handling all queues. Concurrency via `CELERY_WORKER_CONCURRENCY`. Mounts /tmp volume. |

| worker_high_priority | same as worker | Celery queue for urgent tasks | Runs on `high_priority` queue with lower concurrency. |

| worker_low_priority | same as worker | Celery queue for background/slow tasks | Runs on `low_priority` queue with minimal resources. |

| rabbitmq | rabbitmq:3.13-management (-alpine in local) | Message broker for Celery | Exposes 5672 (AMQP) and 15672 (management UI). Persists to `rabbitmq_data`. Healthcheck via diagnostics ping. |

| postgres | postgres:16 (-alpine in local) | Primary relational DB | Exposes 5432. Init scripts in infra/postgres/init. Persists to `postgres_data`. Tuned basic memory settings. |

| redis | redis:7.2-alpine | Cache + Celery backend | Exposes 6379. Configures maxmemory + AOF (prod) / no persistence (local). Stores data on `redis_data`. |

| minio | minio/minio:RELEASE.2024-06-13T22-53-53Z | S3-compatible object storage | Exposes 9000 (API) and 9001 (console). Credentials from env. Persists to `minio_data`. |

| create-bucket | minio/mc | One-shot bucket bootstrapper | Creates `${MINIO_BUCKET}` and `mlflow` buckets against MinIO; runs once and exits. |

| mlflow | ghcr.io/mlflow/mlflow:v3.12.0 | Experiment tracking UI/API | Runs built-in server on 5000 backed by sqlite file; artifacts stored in MinIO via S3 endpoint. Persists to `mlflow_data`. |


## Networks & volumes

- Networks: `epimod-network` (prod-ish) and `epimod-local` (dev) for internal service discovery.
- Named volumes: `postgres_data`, `minio_data`, `mlflow_data`, `rabbitmq_data`, `redis_data`, `worker_temp` hold stateful data; safe to remove only if you want a clean slate.

## How to run

- Local lightweight: `docker compose -f infra/docker-compose.local.yml up` (single worker, mock modes allowed, alpine images where possible).
- Full stack: `docker compose -f infra/docker-compose.yml up` (scaled workers, stricter healthchecks, production-ish settings).
- Production-like no-auth local flows now supported:
	- FAST5 + FASTQ uploads go through `/jobs` and run the generic worker model pipeline.
	- Single POD5 uploads go through `/jobs/pod5` and run the GPU Dorado worker.
	- Generic `/jobs` submissions do not support POD5-only or SRA-only real analysis; those fail fast instead of entering a broken path.
- Kubernetes: images referenced in infra/helm/values.yaml (`image.api`, `image.worker`); replace `PLACEHOLDER_ORG_NAME` and configure ingress/secrets as needed.

## Maintenance tips

- Update images: bump tags in compose/helm and rebuild local images for `api`/`worker` when code or requirements.txt change.
- Storage safety: keep MinIO/Postgres volumes during upgrades unless you intend to wipe data.
- Health: all core services define healthchecks; if a container restarts repeatedly, inspect logs and verify dependent services are healthy first.
- Credentials/env: set .env for broker/DB/MinIO secrets; compose files use sensible defaults for local use.
