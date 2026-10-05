import pytest
from confluent_kafka import Producer
from watchtower_platform import PINNED_PRODUCER_CONFIG, make_producer, producer_config


def test_pinned_settings_are_always_present() -> None:
    config = producer_config("localhost:9092", **{"linger.ms": 5})
    assert config["partitioner"] == "murmur2_random"
    assert config["enable.idempotence"] is True
    assert config["acks"] == "all"
    assert config["linger.ms"] == 5


@pytest.mark.parametrize("key", sorted(PINNED_PRODUCER_CONFIG))
def test_pinned_settings_cannot_be_overridden(key: str) -> None:
    with pytest.raises(ValueError, match=key):
        producer_config("localhost:9092", **{key: "anything"})


def test_librdkafka_accepts_the_config() -> None:
    # Construction validates every key and value; no broker is contacted.
    assert isinstance(make_producer("127.0.0.1:1"), Producer)
