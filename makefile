SHELL := /bin/bash

.PHONY: env compose-up compose-down local-up local-down local-logs local-test prod-local-up prod-local-down prod-local-logs prod-local-status fmt lint test build-images push-images

REGISTRY ?= docker.io/PLACEHOLDER_ORG_NAME
VERSION  ?= 0.1.0

# Setup .env file from example
env:
	@if [ ! -f .env ]; then cp .env.example .env; echo "Created .env"; fi

# Production deployment (full resources)
compose-up:
	docker compose --env-file .env -f infra/docker-compose.yml up -d --build

compose-down:
	docker compose -f infra/docker-compose.yml down -v

# Production-like local run (no auth, real services, GPU Dorado path available)
prod-local-up: env
	@echo "Starting production-like local environment..."
	@echo "This path supports:"
	@echo "  - FAST5 + FASTQ uploads for model analysis"
	@echo "  - POD5 uploads via /jobs/pod5 for GPU Dorado basecalling"
	docker compose --env-file .env -f infra/docker-compose.yml up -d --build

prod-local-down:
	docker compose -f infra/docker-compose.yml down -v

prod-local-logs:
	docker compose -f infra/docker-compose.yml logs -f --tail=100

prod-local-status:
	@docker compose -f infra/docker-compose.yml ps

# Local development (lightweight, single worker)
local-up: env
	@echo "Starting local development environment..."
	@echo "This may take 5-10 minutes on first run (downloading images + building)"
	docker compose --env-file .env -f infra/docker-compose.local.yml up -d --build
	@echo ""
	@echo "✅ Services starting! Access points:"
	@echo "   API:        http://localhost:8080"
	@echo "   API Docs:   http://localhost:8080/docs"
	@echo "   RabbitMQ:   http://localhost:15672 (guest/guest)"
	@echo "   MinIO:      http://localhost:9001 (minioadmin/minioadmin)"
	@echo "   MLflow:     http://localhost:5000"
	@echo ""
	@echo "Check status with: make local-logs"

local-down:
	docker compose -f infra/docker-compose.local.yml down -v

local-logs:
	docker compose -f infra/docker-compose.local.yml logs -f --tail=50

local-test:
	@bash scripts/health-check.sh

integration-test:
	@echo "Running researcher E2E integration test (requires make local-up first)..."
	epi-venv/bin/python scripts/integration_test_pod5.py

local-status:
	@docker compose -f infra/docker-compose.local.yml ps

fmt:
	docker run --rm -v $$(pwd):/code ghcr.io/psf/black:26.5.1 black .

lint:
	docker run --rm -v $$(pwd):/code cytopia/pylint -E services
	docker run --rm -v $$(pwd):/code hadolint/hadolint hadolint -V Dockerfile.*
	docker run --rm -v $$(pwd):/code koalaman/shellcheck:stable sh -c 'shellcheck scripts/*.sh || true'

build-images:
	docker build -t $(REGISTRY)/epimod:api-$(VERSION) -f Dockerfile.api .
	docker build -t $(REGISTRY)/epimod:worker-$(VERSION) -f Dockerfile.worker .

push-images:
	docker push $(REGISTRY)/epimod:api-$(VERSION)
	docker push $(REGISTRY)/epimod:worker-$(VERSION)