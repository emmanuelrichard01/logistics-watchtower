"""Watchtower platform adapters: the only package that imports infrastructure clients."""

from watchtower_platform.kafka import PINNED_PRODUCER_CONFIG, make_producer, producer_config

__all__ = ["PINNED_PRODUCER_CONFIG", "make_producer", "producer_config"]
