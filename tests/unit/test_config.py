from __future__ import annotations

import pytest
from pydantic import SecretStr, ValidationError

from costlake.config import (
    PROJECT_ROOT,
    AnomalyConfig,
    PipelineConfig,
    Settings,
    SourceConfig,
    StorageBackend,
    load_pipeline_config,
)


@pytest.mark.parametrize("env", ["local", "ci", "prod"])
def test_every_environment_file_is_valid(env: str) -> None:
    settings = Settings(_env_file=None, env=env, config_dir=PROJECT_ROOT / "config")  # type: ignore[call-arg]
    config = load_pipeline_config(settings)
    assert {s.id for s in config.sources} == {
        "aws-focus",
        "azure-focus",
        "gcp-focus",
        "aws-cur2-legacy",
    }


def test_prefix_is_normalised_and_traversal_rejected() -> None:
    assert SourceConfig(id="aws", format="focus", prefix="/aws/focus/").prefix == "aws/focus/"
    with pytest.raises(ValidationError):
        SourceConfig(id="aws", format="focus", prefix="aws/../secrets")


def test_overlapping_prefixes_are_rejected() -> None:
    with pytest.raises(ValidationError, match="overlap"):
        PipelineConfig(
            sources=[
                SourceConfig(id="src-a", format="focus", prefix="aws"),
                SourceConfig(id="src-b", format="focus", prefix="aws/cur2"),
            ]
        )


def test_duplicate_source_ids_are_rejected() -> None:
    with pytest.raises(ValidationError, match="unique"):
        PipelineConfig(
            sources=[
                SourceConfig(id="src-a", format="focus", prefix="x"),
                SourceConfig(id="src-a", format="focus", prefix="y"),
            ]
        )


def test_anomaly_history_cannot_exceed_window() -> None:
    with pytest.raises(ValidationError):
        AnomalyConfig(window_days=14, min_history_days=20)


def test_invalid_bucket_name() -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, landing_bucket="Not_A_Bucket")  # type: ignore[call-arg]


def test_custom_s3_endpoint_requires_credentials() -> None:
    with pytest.raises(ValidationError, match="required"):
        Settings(
            _env_file=None, storage_backend=StorageBackend.S3, s3_endpoint_url="http://minio:9000"
        )  # type: ignore[call-arg]
    ok = Settings(  # type: ignore[call-arg]
        _env_file=None,
        storage_backend=StorageBackend.S3,
        s3_endpoint_url="http://minio:9000",
        s3_access_key_id=SecretStr("a"),
        s3_secret_access_key=SecretStr("very-secret-value"),
    )
    assert ok.lake_uri == "s3://costlake-lake"
    assert "very-secret-value" not in repr(ok)
