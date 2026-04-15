from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from kedro.config import OmegaConfigLoader
from kedro.io import AbstractDataset, DataCatalog, MemoryDataset
from kedro.pipeline import node, pipeline
from kedro.runner import SequentialRunner

from meridian_data.config import bootstrap_env, load_kedro_credentials
from meridian_data.pipelines.credentials_context import create_pipeline

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _set_s3_env(monkeypatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-2")
    monkeypatch.setenv("S3_BUCKET_NAME", "meridian-test")
    monkeypatch.setenv("FRED_API_KEY", "fred-test-key")


def test_bootstrap_env_preserves_os_env_values(tmp_path, monkeypatch) -> None:
    (tmp_path / ".env").write_text("TEST_ENV_PRIORITY=from-dotenv\n", encoding="utf-8")
    monkeypatch.setenv("TEST_ENV_PRIORITY", "from-os")

    bootstrap_env(project_root=tmp_path)

    assert os.getenv("TEST_ENV_PRIORITY") == "from-os"


def test_bootstrap_env_loads_dotenv_values_when_missing(tmp_path, monkeypatch) -> None:
    (tmp_path / ".env").write_text("TEST_ENV_FALLBACK=from-dotenv\n", encoding="utf-8")
    monkeypatch.delenv("TEST_ENV_FALLBACK", raising=False)

    bootstrap_env(project_root=tmp_path)

    assert os.getenv("TEST_ENV_FALLBACK") == "from-dotenv"


def test_load_kedro_credentials_returns_s3_payload(monkeypatch) -> None:
    _set_s3_env(monkeypatch)

    credentials = load_kedro_credentials(env="local")

    assert credentials["key"] == "test-key"
    assert credentials["secret"] == "test-secret"
    assert credentials["client_kwargs"]["region_name"] == "us-east-2"
    assert "bucket_name" not in credentials


def test_credentials_pipeline_produces_output_for_downstream_node(monkeypatch) -> None:
    _set_s3_env(monkeypatch)

    def _capture_region_name(s3_credentials: dict[str, Any]) -> str:
        return s3_credentials["client_kwargs"]["region_name"]

    pipeline_under_test = create_pipeline() + pipeline(
        [
            node(
                func=_capture_region_name,
                inputs="s3_credentials",
                outputs="captured_region",
                name="capture_region_name_node",
            )
        ]
    )
    catalog = DataCatalog({"captured_region": MemoryDataset()})

    SequentialRunner().run(pipeline_under_test, catalog)

    assert catalog.load("captured_region") == "us-east-2"


class CredentialEchoDataset(AbstractDataset):
    def __init__(self, credentials: dict[str, Any]):
        self._credentials = credentials

    def _load(self) -> dict[str, Any]:
        return self._credentials

    def _save(self, data: dict[str, Any]) -> None:  # noqa: ARG002
        return None

    def _describe(self) -> dict[str, Any]:
        return {"credentials_keys": sorted(self._credentials.keys())}


def test_catalog_credentials_resolve_without_credentials_node(monkeypatch) -> None:
    _set_s3_env(monkeypatch)

    conf_path = PROJECT_ROOT / "conf"
    loader = OmegaConfigLoader(
        conf_source=str(conf_path),
        env="local",
        base_env="base",
        default_run_env="local",
    )
    credentials = loader["credentials"]

    catalog_config = {
        "catalog_credentials_dataset": {
            "type": "tests.pipelines.credentials_context.test_credentials_context.CredentialEchoDataset",
            "credentials": "s3",
        }
    }
    catalog = DataCatalog.from_config(
        catalog=catalog_config,
        credentials=credentials,
    )

    resolved_credentials = catalog.load("catalog_credentials_dataset")

    assert resolved_credentials["key"] == "test-key"
    assert resolved_credentials["secret"] == "test-secret"
    assert resolved_credentials["client_kwargs"]["region_name"] == "us-east-2"
    assert "bucket_name" not in resolved_credentials


def test_parameters_resolve_s3_bucket_name(monkeypatch) -> None:
    _set_s3_env(monkeypatch)

    conf_path = PROJECT_ROOT / "conf"
    loader = OmegaConfigLoader(
        conf_source=str(conf_path),
        env="local",
        base_env="base",
        default_run_env="local",
        custom_resolvers={"oc.env": lambda key, default=None: os.getenv(key, default)},
    )
    parameters = loader["parameters"]

    assert parameters["s3"]["bucket_name"] == "meridian-test"


def test_prod_env_resolves_fred_catalog_s3_paths_and_credentials(monkeypatch) -> None:
    _set_s3_env(monkeypatch)

    conf_path = PROJECT_ROOT / "conf"
    loader = OmegaConfigLoader(
        conf_source=str(conf_path),
        env="prod",
        base_env="base",
        default_run_env="local",
        custom_resolvers={"oc.env": lambda key, default=None: os.getenv(key, default)},
    )

    credentials = loader["credentials"]
    catalog = loader["catalog"]

    assert credentials["s3"]["key"] == "test-key"
    assert credentials["s3"]["secret"] == "test-secret"
    assert credentials["s3"]["client_kwargs"]["region_name"] == "us-east-2"
    assert credentials["fred"]["api_key"] == "fred-test-key"

    assert (
        catalog["macro__fred__raw__series"]["path"]
        == "s3://meridian-test/meridian/01_raw/macro/fred/series"
    )
    assert (
        catalog["macro__fred__raw__series"]["type"] == "partitions.PartitionedDataset"
    )
    assert (
        catalog["macro__fred__primary__series_latest"]["path"]
        == "s3://meridian-test/meridian/03_primary/macro/fred/series_latest"
    )
    assert (
        catalog["macro__fred__primary__series_latest"]["type"]
        == "partitions.PartitionedDataset"
    )
