from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from costlake.config import PROJECT_ROOT, Settings, StorageBackend, load_pipeline_config
from costlake.pipeline import Runtime


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        _env_file=None,  # type: ignore[call-arg]
        env="ci",
        config_dir=PROJECT_ROOT / "config",
        storage_backend=StorageBackend.LOCAL,
        local_root=tmp_path / "object-store",
        database_url=f"sqlite:///{tmp_path / 'ledger.sqlite'}",
        warehouse_path=tmp_path / "warehouse" / "costlake.duckdb",
        pushgateway_url=None,
        serving_database_url=None,
    )


@pytest.fixture
def runtime(settings: Settings) -> Runtime:
    return Runtime.from_settings(settings)


@pytest.fixture
def pipeline_config(settings: Settings):  # type: ignore[no-untyped-def]
    return load_pipeline_config(settings)


END_DAY = date(2026, 11, 16)
EXPORT_TIME = datetime(2026, 11, 16, 6, 0, tzinfo=UTC)
