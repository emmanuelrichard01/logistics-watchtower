"""Watchtower platform adapters: the only package that imports infrastructure clients."""

from watchtower_platform.kafka import PINNED_PRODUCER_CONFIG, make_producer, producer_config
from watchtower_platform.schema_registry import (
    RegistryUnavailableError,
    SchemaRegistry,
    decode,
    encode,
)

__all__ = [
    "PINNED_PRODUCER_CONFIG",
    "RegistryUnavailableError",
    "SchemaRegistry",
    "decode",
    "encode",
    "make_producer",
    "producer_config",
]
