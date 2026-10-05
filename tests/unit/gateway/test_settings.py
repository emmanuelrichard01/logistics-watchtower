"""The gateway refuses to boot on invalid configuration."""

import json
from pathlib import Path

import pytest
from pydantic import ValidationError
from watchtower_gateway.settings import Settings, load_device_keys

KEYS_OK = {"EDGE-0101": "dev-only-key-EDGE-0101"}


def keys_file(tmp_path: Path, content: object) -> Path:
    path = tmp_path / "keys.json"
    path.write_text(json.dumps(content), encoding="utf-8")
    return path


def test_valid_config_loads(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WT_GATEWAY_BOOTSTRAP_SERVERS", "redpanda:9092")
    monkeypatch.setenv("WT_GATEWAY_SCHEMA_REGISTRY_URL", "http://redpanda:8081")
    monkeypatch.setenv("WT_GATEWAY_DEVICE_KEYS_FILE", str(keys_file(tmp_path, KEYS_OK)))
    s = Settings()  # type: ignore[call-arg]
    assert s.input_topic == "wt.input.v1"
    assert load_device_keys(s.device_keys_file) == {"EDGE-0101": b"dev-only-key-EDGE-0101"}


@pytest.mark.parametrize(
    ("env", "keys"),
    [
        ({"WT_GATEWAY_SCHEMA_REGISTRY_URL": "http://r:8081"}, KEYS_OK),  # missing bootstrap
        (
            {
                "WT_GATEWAY_BOOTSTRAP_SERVERS": "r:9092",
                "WT_GATEWAY_SCHEMA_REGISTRY_URL": "not a url",
            },
            KEYS_OK,
        ),
        (
            {
                "WT_GATEWAY_BOOTSTRAP_SERVERS": "r:9092",
                "WT_GATEWAY_SCHEMA_REGISTRY_URL": "http://r:8081",
                "WT_GATEWAY_MAX_BATCH": "0",
            },
            KEYS_OK,
        ),
        (
            {
                "WT_GATEWAY_BOOTSTRAP_SERVERS": "r:9092",
                "WT_GATEWAY_SCHEMA_REGISTRY_URL": "http://r:8081",
            },
            {"EDGE 01": "dev-only-key-EDGE-0101"},
        ),
        (
            {
                "WT_GATEWAY_BOOTSTRAP_SERVERS": "r:9092",
                "WT_GATEWAY_SCHEMA_REGISTRY_URL": "http://r:8081",
            },
            {"EDGE-0101": "short"},
        ),
        (
            {
                "WT_GATEWAY_BOOTSTRAP_SERVERS": "r:9092",
                "WT_GATEWAY_SCHEMA_REGISTRY_URL": "http://r:8081",
            },
            {},
        ),
    ],
)
def test_invalid_config_refuses_to_boot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, env: dict[str, str], keys: object
) -> None:
    for name in (
        "WT_GATEWAY_BOOTSTRAP_SERVERS",
        "WT_GATEWAY_SCHEMA_REGISTRY_URL",
        "WT_GATEWAY_MAX_BATCH",
    ):
        monkeypatch.delenv(name, raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("WT_GATEWAY_DEVICE_KEYS_FILE", str(keys_file(tmp_path, keys)))
    with pytest.raises(ValidationError):
        Settings()  # type: ignore[call-arg]


def test_missing_keys_file_refuses_to_boot(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WT_GATEWAY_BOOTSTRAP_SERVERS", "r:9092")
    monkeypatch.setenv("WT_GATEWAY_SCHEMA_REGISTRY_URL", "http://r:8081")
    monkeypatch.setenv("WT_GATEWAY_DEVICE_KEYS_FILE", "/does/not/exist.json")
    with pytest.raises(ValidationError):
        Settings()  # type: ignore[call-arg]
