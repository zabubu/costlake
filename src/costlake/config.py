"""Runtime configuration.

Two layers, deliberately separated:

* ``Settings``: endpoints, credentials and paths. Read from environment variables
  (prefix ``COSTLAKE_``) so that secrets never live in versioned files.
* ``PipelineConfig``: tunables of the data pipeline (quality thresholds, anomaly
  detection parameters, sources). Read from versioned YAML files, one per
  environment, in ``config/environments/<env>.yaml``.
"""

from __future__ import annotations

import re
from enum import StrEnum
from functools import lru_cache
from itertools import pairwise
from pathlib import Path
from typing import Annotated, Literal

import yaml
from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class StorageBackend(StrEnum):
    S3 = "s3"
    LOCAL = "local"


class SourceFormat(StrEnum):
    FOCUS = "focus"
    AWS_CUR2 = "aws_cur2"


class Settings(BaseSettings):
    """Infrastructure settings, resolved from the environment."""

    model_config = SettingsConfigDict(
        env_prefix="COSTLAKE_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    env: Literal["local", "ci", "prod"] = "local"
    config_dir: Path = PROJECT_ROOT / "config"

    storage_backend: StorageBackend = StorageBackend.LOCAL
    local_root: Path = PROJECT_ROOT / "data" / "object-store"
    s3_endpoint_url: str | None = None
    s3_region: str = "us-east-1"
    s3_access_key_id: SecretStr | None = None
    s3_secret_access_key: SecretStr | None = None
    landing_bucket: str = "costlake-landing"
    lake_bucket: str = "costlake-lake"

    database_url: str = f"sqlite:///{PROJECT_ROOT / 'data' / 'ledger.sqlite'}"
    serving_database_url: str | None = None
    warehouse_path: Path = PROJECT_ROOT / "data" / "warehouse" / "costlake.duckdb"
    dbt_project_dir: Path = PROJECT_ROOT / "dbt"

    pushgateway_url: str | None = None
    log_level: str = "INFO"
    log_format: Literal["json", "console"] = "console"

    @field_validator("landing_bucket", "lake_bucket")
    @classmethod
    def _bucket_name(cls, value: str) -> str:
        if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", value):
            raise ValueError(f"invalid bucket name: {value!r}")
        return value

    @model_validator(mode="after")
    def _s3_requires_credentials(self) -> Settings:
        # A custom endpoint (MinIO) has no instance-profile fallback: credentials are mandatory.
        if (
            self.storage_backend is StorageBackend.S3
            and self.s3_endpoint_url
            and (self.s3_access_key_id is None or self.s3_secret_access_key is None)
        ):
            raise ValueError("s3_access_key_id and s3_secret_access_key are required with s3")
        return self

    @property
    def lake_uri(self) -> str:
        """Root URI of the lake as seen by DuckDB."""
        if self.storage_backend is StorageBackend.S3:
            return f"s3://{self.lake_bucket}"
        return str((self.local_root / self.lake_bucket).resolve())


class SourceConfig(BaseModel):
    """A billing export feed dropped into the landing bucket."""

    id: Annotated[str, Field(pattern=r"^[a-z0-9][a-z0-9-]{1,40}$")]
    format: SourceFormat
    prefix: str
    description: str = ""

    @field_validator("prefix")
    @classmethod
    def _prefix(cls, value: str) -> str:
        value = value.strip("/")
        if not value or ".." in value.split("/"):
            raise ValueError("prefix must be a non-empty relative key prefix")
        return value + "/"


class QualityConfig(BaseModel):
    reconciliation_tolerance: float = Field(0.01, ge=0)
    min_tag_coverage: float = Field(0.8, ge=0, le=1)
    allocation_tag: Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]{0,62}$")] = "team"


class IngestionConfig(BaseModel):
    settle_seconds: int = Field(
        60,
        ge=0,
        description="Ignore a delivery folder until its newest object is this old, so that an "
        "export still being written is never picked up half-way.",
    )


class AnomalyConfig(BaseModel):
    window_days: int = Field(42, ge=14, le=180)
    min_history_days: int = Field(14, ge=5)
    z_threshold: float = Field(3.5, gt=0)
    min_abs_delta: float = Field(25.0, ge=0)
    min_relative_delta: float = Field(0.3, ge=0)

    @model_validator(mode="after")
    def _history_fits_window(self) -> AnomalyConfig:
        if self.min_history_days > self.window_days:
            raise ValueError("min_history_days cannot exceed window_days")
        return self


class PipelineConfig(BaseModel):
    sources: list[SourceConfig]
    ingestion: IngestionConfig = IngestionConfig()
    quality: QualityConfig = QualityConfig()
    anomaly: AnomalyConfig = AnomalyConfig()

    @model_validator(mode="after")
    def _unique_sources(self) -> PipelineConfig:
        ids = [s.id for s in self.sources]
        if len(ids) != len(set(ids)):
            raise ValueError("source ids must be unique")
        prefixes = sorted(s.prefix for s in self.sources)
        for a, b in pairwise(prefixes):
            if b.startswith(a):
                raise ValueError(f"source prefixes overlap: {a!r} and {b!r}")
        return self

    def source(self, source_id: str) -> SourceConfig:
        for source in self.sources:
            if source.id == source_id:
                return source
        raise KeyError(source_id)


def load_pipeline_config(settings: Settings) -> PipelineConfig:
    path = settings.config_dir / "environments" / f"{settings.env}.yaml"
    with path.open(encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    if not isinstance(raw, dict):
        raise TypeError(f"{path} must contain a mapping")
    return PipelineConfig.model_validate(raw)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
