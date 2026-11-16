# Every target used by CI is defined here, so a CI failure reproduces locally with
# the same command. `make help` lists them.
SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

PYTHON ?= python3
VENV ?= .venv
BIN := $(VENV)/bin
COMPOSE ?= docker compose
# Services with a container healthcheck (Loki and Alloy have none: distroless images).
HEALTHCHECKED := seaweedfs postgres costlake-code dagster-webserver dagster-daemon costlake-exporter pushgateway prometheus grafana
TF_DIRS := infrastructure/terraform/modules/lake-buckets infrastructure/terraform/modules/aws-focus-export infrastructure/terraform/modules/pipeline-access infrastructure/terraform/envs/local infrastructure/terraform/envs/aws

.PHONY: help
help: ## List targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------- development
$(BIN)/python:
	$(PYTHON) -m venv $(VENV)
	$(BIN)/pip install --upgrade pip

.PHONY: install
install: $(BIN)/python ## Install the project and dev tools (hash-pinned lock) in .venv
	$(BIN)/pip install --require-hashes --no-deps -r requirements/dev.lock
	$(BIN)/pip install --no-deps -e .

.PHONY: lock
lock: ## Re-resolve requirements/*.lock (needs Docker)
	docker run --rm -v "$$PWD":/src -w /src python:3.12.16-slim-trixie sh -c '\
	  pip install -q --root-user-action=ignore uv==0.13.8 && \
	  uv pip compile pyproject.toml --extra orchestration --universal --python-version 3.11 --generate-hashes --no-header -q -o requirements/runtime.lock && \
	  uv pip compile pyproject.toml --extra orchestration --group dev --universal --python-version 3.11 --generate-hashes --no-header -q -o requirements/dev.lock'

.PHONY: fmt
fmt: ## Format Python and Terraform
	$(BIN)/ruff format src tests
	$(BIN)/ruff check --fix src tests
	terraform fmt -recursive infrastructure/terraform

.PHONY: lint
lint: ## Formatting check, lint, type check
	$(BIN)/ruff format --check src tests
	$(BIN)/ruff check src tests
	$(BIN)/mypy src

.PHONY: test
test: ## Unit tests with coverage
	COSTLAKE_ENV=ci $(BIN)/pytest tests/unit --cov --cov-report=term --cov-report=xml:coverage.xml --junitxml=reports/unit.xml

.PHONY: test-integration
test-integration: ## Integration tests (pipeline + real dbt; PostgreSQL if COSTLAKE_TEST_POSTGRES_URL); coverage adds to the unit run
	COSTLAKE_ENV=ci $(BIN)/pytest tests/integration --cov --cov-append --cov-report=term --cov-report=xml:coverage.xml --junitxml=reports/integration.xml

.PHONY: dbt-parse
dbt-parse: ## Compile the dbt project (needed by Dagster outside Docker)
	COSTLAKE_ENV=ci $(BIN)/dbt parse --project-dir dbt --profiles-dir dbt --quiet

.PHONY: tf-check
tf-check: ## terraform fmt, validate and test (offline, mocked providers)
	terraform fmt -recursive -check infrastructure/terraform
	@for d in $(TF_DIRS); do \
	  echo "--- $$d"; \
	  terraform -chdir=$$d init -backend=false -input=false -no-color >/dev/null; \
	  terraform -chdir=$$d validate -no-color; \
	  if [ -d $$d/tests ]; then terraform -chdir=$$d test -no-color; fi; \
	done

.PHONY: scan
scan: ## Security scans: locked dependencies, secrets, IaC misconfigurations
	$(BIN)/pip-audit --strict --disable-pip --no-deps -r requirements/runtime.lock
	# Git history only: the git-ignored .env legitimately contains local secrets.
	docker run --rm -v "$$PWD":/repo zricethezav/gitleaks:v8.30.1 git /repo --redact --exit-code 1
	docker run --rm -v "$$PWD":/src aquasec/trivy:0.76.2 config --exit-code 1 --severity HIGH,CRITICAL \
	  --skip-dirs /src/.github --skip-dirs /src/.venv --skip-dirs /src/infrastructure/terraform/envs/local /src

.PHONY: promtool
promtool: ## Validate Prometheus config and unit-test alerting rules (Docker)
	docker run --rm -v "$$PWD/monitoring/prometheus":/etc/prometheus:ro --entrypoint promtool prom/prometheus:v3.16.0 check config /etc/prometheus/prometheus.yml
	docker run --rm -v "$$PWD/monitoring/prometheus":/etc/prometheus:ro -w /etc/prometheus/tests --entrypoint promtool prom/prometheus:v3.16.0 test rules costlake_test.yml

.PHONY: check
check: lint test test-integration tf-check ## Everything CI runs without Docker

# ---------------------------------------------------------------- local platform
.env:
	./scripts/init-env.sh

.PHONY: build
build: .env ## Build the costlake image
	$(COMPOSE) build

.PHONY: up
up: .env ## Build, start the platform, provision buckets with Terraform, wait until healthy
	$(COMPOSE) build
	$(COMPOSE) up -d --wait seaweedfs postgres
	$(MAKE) bootstrap
	$(COMPOSE) up -d
	$(COMPOSE) up -d --wait --wait-timeout 600 $(HEALTHCHECKED)

.PHONY: bootstrap
bootstrap: ## terraform apply of envs/local against SeaweedFS (idempotent)
	$(COMPOSE) --profile bootstrap run --rm terraform init -input=false -lockfile=readonly -backend-config=path=/state/terraform.tfstate
	$(COMPOSE) --profile bootstrap run --rm terraform apply -input=false -auto-approve

.PHONY: demo-data
demo-data: ## Drop 120 days of synthetic exports (4 sources) into the landing bucket
	$(COMPOSE) --profile tools run --rm generator

.PHONY: run
run: ## Trigger the pipeline now instead of waiting for the sensor
	$(COMPOSE) exec costlake-code dagster job launch -m costlake.orchestration.definitions -j costlake_pipeline

.PHONY: e2e
e2e: ## End-to-end test of the running platform
	./scripts/e2e.sh

.PHONY: logs
logs: ## Follow logs of the costlake services
	$(COMPOSE) logs -f costlake-code dagster-daemon costlake-exporter

.PHONY: down
down: ## Stop the platform (keeps data volumes)
	$(COMPOSE) --profile bootstrap --profile tools down

.PHONY: destroy
destroy: ## terraform destroy of the local buckets, then remove containers AND volumes
	-$(COMPOSE) --profile bootstrap run --rm terraform destroy -input=false -auto-approve
	$(COMPOSE) --profile bootstrap --profile tools down --volumes --remove-orphans

.PHONY: clean
clean: ## Remove local build and test artefacts
	rm -rf data reports coverage.xml .pytest_cache .mypy_cache .ruff_cache dbt/target dbt/logs
