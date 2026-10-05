"""Gateway configuration. Invalid configuration stops the process at boot."""

import json
import uuid
from pathlib import Path

from pydantic import AnyHttpUrl, Field, FilePath, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from watchtower_contracts.identity import ID_PATTERN


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="WT_GATEWAY_", frozen=True)

    bootstrap_servers: str = Field(min_length=1)
    schema_registry_url: AnyHttpUrl
    device_keys_file: FilePath
    org_id: uuid.UUID = uuid.UUID("00000000-0000-4000-8000-000000000001")
    input_topic: str = "wt.input.v1"
    quarantine_topic: str = "telemetry.quarantine.v1"
    max_batch: int = Field(500, ge=1, le=5000)
    delivery_timeout_s: float = Field(10.0, gt=0, le=120)
    host: str = "0.0.0.0"
    port: int = Field(8000, ge=1, le=65535)

    @field_validator("device_keys_file")
    @classmethod
    def _keys_parse(cls, path: Path) -> Path:
        load_device_keys(path)  # raises on a malformed registry
        return path


def load_device_keys(path: Path) -> dict[str, bytes]:
    """Device key registry: a JSON object mapping device_id to its HMAC key (UTF-8).

    Development keys only; a real deployment loads them from a secrets store.
    """
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or not raw:
        raise ValueError("device key registry must be a non-empty JSON object")
    keys: dict[str, bytes] = {}
    for device_id, key in raw.items():
        if not isinstance(device_id, str) or not ID_PATTERN.fullmatch(device_id):
            raise ValueError(f"invalid device_id in key registry: {device_id!r}")
        if not isinstance(key, str) or len(key) < 16:
            raise ValueError(f"key for {device_id} must be a string of at least 16 characters")
        keys[device_id] = key.encode("utf-8")
    return keys
