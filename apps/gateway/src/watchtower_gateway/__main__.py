"""Entrypoint: `python -m watchtower_gateway` or `wt-gateway`. Refuses to boot on invalid config."""

import sys

import uvicorn
from pydantic import ValidationError
from watchtower_platform import SchemaRegistry

from watchtower_gateway.app import create_app
from watchtower_gateway.publisher import KafkaPublisher
from watchtower_gateway.settings import Settings, load_device_keys


def main() -> None:
    try:
        settings = Settings()  # type: ignore[call-arg]  # values come from WT_GATEWAY_* env
    except ValidationError as exc:
        print(f"wt-gateway: invalid configuration, refusing to start\n{exc}", file=sys.stderr)
        raise SystemExit(2) from exc

    publisher = KafkaPublisher(
        bootstrap_servers=settings.bootstrap_servers,
        registry=SchemaRegistry(str(settings.schema_registry_url)),
        input_topic=settings.input_topic,
        quarantine_topic=settings.quarantine_topic,
        org_id=str(settings.org_id),
        delivery_timeout_s=settings.delivery_timeout_s,
    )
    app = create_app(
        publisher=publisher,
        keys=load_device_keys(settings.device_keys_file),
        max_batch=settings.max_batch,
    )
    uvicorn.run(app, host=settings.host, port=settings.port, log_level="info")


if __name__ == "__main__":
    main()
