"""Kafka producer factory. Every Watchtower producer is built here."""

from typing import Any

from confluent_kafka import Producer

# Settings no caller may change:
# - murmur2_random is the Java client's partitioner. librdkafka's default
#   (consistent_random, CRC32) sends the same key to a different partition, so
#   producers in different languages would split one vehicle's records across
#   partitions and break per-vehicle ordering.
# - Idempotence with acks=all removes duplicates from producer retries and loses
#   nothing on a leader change (plan section 10).
PINNED_PRODUCER_CONFIG: dict[str, str | bool] = {
    "partitioner": "murmur2_random",
    "enable.idempotence": True,
    "acks": "all",
}


def producer_config(bootstrap_servers: str, **overrides: Any) -> dict[str, Any]:
    """Build a producer config; tuning keys may be overridden, pinned keys may not."""
    clashes = sorted(set(overrides) & set(PINNED_PRODUCER_CONFIG))
    if clashes:
        raise ValueError(f"pinned producer settings cannot be overridden: {', '.join(clashes)}")
    return {"bootstrap.servers": bootstrap_servers, **overrides, **PINNED_PRODUCER_CONFIG}


def make_producer(bootstrap_servers: str, **overrides: Any) -> Producer:
    return Producer(producer_config(bootstrap_servers, **overrides))
