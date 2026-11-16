# Contributing

## Setup

```bash
make install        # .venv with hash-pinned dependencies (Python 3.11 to 3.13)
make check          # lint + type check + unit + integration + Terraform tests
```

The platform tests need Docker: `make up && make e2e`.

## Conventions

* **Every CI step is a Make target.** A red CI job is reproduced locally with the same command (`make lint`, `make test`, `make tf-check`, `make promtool`, `make e2e`, `make scan`).
* **Pipeline logic lives in `src/costlake/pipeline.py` and below, never in Dagster assets.** Assets only call pipeline functions, so every step is testable without an orchestrator.
* **New source format**: add an adapter in `src/costlake/normalization/adapters/` returning the silver contract, register it in `adapters/__init__.py` and `SourceFormat`, then add a fixture-based test that checks the reconciliation rule passes.
* **New quality rule**: add it to `quality/checks.py` with a severity. `error` blocks publication of the whole delivery, so use it only for problems that make totals wrong.
* **New gold model**: add the dbt model with a grain test (`unique_combination_of_columns`), then add it to `PUBLISHED_TABLES` if dashboards need it.
* **Dependencies**: edit `pyproject.toml`, run `make lock`, commit both lock files.
* **Terraform**: `make fmt`, keep `terraform test` files next to the module, no provider credentials in tests.
* Python is formatted and linted with ruff and type-checked with `mypy --strict`.

## Pull requests

Describe the behaviour change and how it was verified. Changes to the anomaly detector or the quality rules need a test showing the case they handle.
