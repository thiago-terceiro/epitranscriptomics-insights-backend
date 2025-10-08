SHELL := /bin/bash

.PHONY: env compose-up compose-down fmt lint test build-images push-images

REGISTRY ?= ghcr.io/yourorg
VERSION  ?= 0.1.0

env:
	@if [ ! -f .env ]; then cp .env.example .env; echo "Created .env"; fi

compose-up:
	docker compose --env-file .env -f infra/docker-compose.yml up -d --build

compose-down:
	docker compose -f infra/docker-compose.yml down -v

fmt:
	docker run --rm -v $$(pwd):/code ghcr.io/psf/black:24.4.2 black .

lint:
	docker run --rm -v $$(pwd):/code cytopia/pylint -E services
	docker run --rm -v $$(pwd):/code hadolint/hadolint hadolint -V Dockerfile.*
	docker run --rm -v $$(pwd):/code koalaman/shellcheck:stable sh -c 'shellcheck scripts/*.sh || true'

build-images:
	docker build -t $(REGISTRY)/epimod-api:$(VERSION) -f Dockerfile.api .
	docker build -t $(REGISTRY)/epimod-worker:$(VERSION) -f Dockerfile.worker .

push-images:
	docker push $(REGISTRY)/epimod-api:$(VERSION)
	docker push $(REGISTRY)/epimod-worker:$(VERSION)