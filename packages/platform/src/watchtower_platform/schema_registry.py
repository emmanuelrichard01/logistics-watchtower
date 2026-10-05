"""Minimal Schema Registry client and Confluent wire format.

Standard library only: confluent-kafka's registry extra pulls in httpx, attrs and
cachetools for the three calls Watchtower needs.
"""

import io
import json
import struct
import threading
import urllib.error
import urllib.request
from typing import Any

import fastavro

MAGIC = 0
_CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"


class RegistryUnavailableError(Exception):
    """The registry could not be reached or answered with an error: retry later."""


class SchemaRegistry:
    def __init__(self, url: str, timeout_s: float = 5.0) -> None:
        self._url = url.rstrip("/")
        self._timeout = timeout_s
        self._ids: dict[str, int] = {}
        self._schemas: dict[int, Any] = {}
        self._lock = threading.Lock()

    def _call(self, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
        request = urllib.request.Request(
            self._url + path,
            data=json.dumps(body).encode() if body is not None else None,
            method=method,
            headers={"Content-Type": _CONTENT_TYPE, "Accept": _CONTENT_TYPE},
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                return json.load(response)
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            raise RegistryUnavailableError(f"{method} {path}: {exc}") from exc

    def ping(self) -> bool:
        try:
            self._call("GET", "/subjects")
        except RegistryUnavailableError:
            return False
        return True

    def register(self, subject: str, schema: dict[str, Any]) -> int:
        """Register (idempotently) and return the schema ID; cached per subject."""
        with self._lock:
            cached = self._ids.get(subject)
        if cached is not None:
            return cached
        schema_id = int(
            self._call("POST", f"/subjects/{subject}/versions", {"schema": json.dumps(schema)})[
                "id"
            ]
        )
        with self._lock:
            self._ids[subject] = schema_id
            self._schemas[schema_id] = fastavro.parse_schema(schema)
        return schema_id

    def parsed(self, schema_id: int) -> Any:
        with self._lock:
            cached = self._schemas.get(schema_id)
        if cached is not None:
            return cached
        schema = json.loads(self._call("GET", f"/schemas/ids/{schema_id}")["schema"])
        parsed = fastavro.parse_schema(schema)
        with self._lock:
            self._schemas[schema_id] = parsed
        return parsed


def encode(schema_id: int, parsed_schema: Any, record: dict[str, Any]) -> bytes:
    """Confluent wire format: magic byte 0, big-endian 4-byte schema ID, Avro body."""
    body = io.BytesIO()
    fastavro.schemaless_writer(body, parsed_schema, record)
    return struct.pack(">bI", MAGIC, schema_id) + body.getvalue()


def decode(
    data: bytes, registry: SchemaRegistry, *, return_record_name: bool = False
) -> tuple[int, Any]:
    if len(data) < 5 or data[0] != MAGIC:
        raise ValueError("not Confluent wire format")
    schema_id = struct.unpack(">I", data[1:5])[0]
    schema = registry.parsed(schema_id)
    return schema_id, fastavro.schemaless_reader(
        io.BytesIO(data[5:]), schema, None, return_record_name=return_record_name
    )
