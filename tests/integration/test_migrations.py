"""The core schema migrates both ways and enforces the plan's invariants in the database."""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from alembic import command
from alembic.config import Config
from psycopg import errors
from sqlalchemy import Connection, Engine, create_engine, inspect, text
from sqlalchemy.exc import DBAPIError

pytestmark = pytest.mark.integration

CORE_TABLES = {
    "organizations",
    "vehicles",
    "sensors",
    "cargo_profiles",
    "shipments",
    "shipment_assignments",
    "vehicle_state",
    "risk_assessments",
    "alerts",
    "interventions",
    "rule_sets",
    "processed_events",
    "outbox",
    "audit_log",
    "minute_series",
}
T0 = datetime(2026, 10, 5, 7, 0, tzinfo=UTC)
# Not "{vehicle}:{shipment|-}:{type}": the shipment segment is missing.
MISSING_SHIPMENT_FIELD = "TRK-101-CARGO_TEMP_BREACH"


def public_tables(url: str) -> set[str]:
    engine = create_engine(url)
    try:
        return set(inspect(engine).get_table_names(schema="public"))
    finally:
        engine.dispose()


def role_exists(url: str) -> bool:
    engine = create_engine(url)
    try:
        with engine.connect() as conn:
            query = text("SELECT 1 FROM pg_roles WHERE rolname = 'watchtower_app'")
            return conn.execute(query).scalar() is not None
    finally:
        engine.dispose()


def test_migration_applies_to_an_empty_database_and_downgrades_cleanly(
    database_url: str, alembic_cfg: Config
) -> None:
    config = alembic_cfg
    command.upgrade(config, "head")
    assert public_tables(database_url) >= CORE_TABLES
    assert role_exists(database_url)

    command.downgrade(config, "base")
    assert public_tables(database_url) & CORE_TABLES == set()
    assert not role_exists(database_url)


def insert(conn: Connection, table: str, key: str = "id", **values: Any) -> Any:
    if key == "id":
        values.setdefault("id", uuid.uuid4())
    columns = ", ".join(values)
    params = ", ".join(f":{name}" for name in values)
    conn.execute(text(f"INSERT INTO {table} ({columns}) VALUES ({params})"), values)
    return values[key]


def add_vehicle(conn: Connection, org: uuid.UUID, vehicle_id: str) -> str:
    device = vehicle_id.replace("TRK", "EDGE")
    return insert(
        conn, "vehicles", key="vehicle_id", vehicle_id=vehicle_id, org_id=org, device_id=device
    )


def seed(engine: Engine) -> dict[str, Any]:
    with engine.begin() as conn:
        org = insert(conn, "organizations", name="Synthetic Cold Chain Ltd")
        vehicle = add_vehicle(conn, org, "TRK-101")
        profile = insert(
            conn,
            "cargo_profiles",
            org_id=org,
            name="Frozen fish",
            min_temp_c=-25.0,
            max_temp_c=-18.0,
            allowed_excursion_minutes=30,
            value_per_kg_ngn=4500,
        )
        shipment = insert(
            conn,
            "shipments",
            org_id=org,
            reference="SHP-0001",
            cargo_profile_id=profile,
            cargo_weight_kg=9000.0,
            origin="Lagos",
            destination="Abuja",
            status="IN_TRANSIT",
        )
    return {"org": org, "vehicle": vehicle, "shipment": shipment}


def open_alert(
    conn: Connection, ids: dict[str, Any], state: str = "OPEN", dedup_key: str | None = None
) -> uuid.UUID:
    return insert(
        conn,
        "alerts",
        org_id=ids["org"],
        vehicle_id=ids["vehicle"],
        alert_type="CARGO_TEMP_BREACH",
        severity="CRITICAL",
        state=state,
        dedup_key=dedup_key or f"{ids['vehicle']}:{ids['shipment']}:CARGO_TEMP_BREACH",
        rule_version=1,
        version=1,
        opened_at=T0,
        last_seen_at=T0,
        evidence="{}",
    )


def sqlstate_error(exc: pytest.ExceptionInfo[DBAPIError]) -> type[object]:
    return type(exc.value.orig)


def test_only_one_live_alert_per_dedup_key(migrated: Engine) -> None:
    ids = seed(migrated)
    with migrated.begin() as conn:
        first = open_alert(conn, ids)

    with pytest.raises(DBAPIError) as exc, migrated.begin() as conn:
        open_alert(conn, ids)
    assert sqlstate_error(exc) is errors.UniqueViolation

    # The index is partial: once resolved, the same key may open again.
    with migrated.begin() as conn:
        conn.execute(text("UPDATE alerts SET state = 'RESOLVED' WHERE id = :id"), {"id": first})
        open_alert(conn, ids)


def test_duplicate_idempotency_key_is_rejected(migrated: Engine) -> None:
    ids = seed(migrated)
    actor = uuid.uuid4()
    with migrated.begin() as conn:
        alert = open_alert(conn, ids)
        insert(
            conn,
            "interventions",
            org_id=ids["org"],
            alert_id=alert,
            actor_id=actor,
            action="ACK",
            idempotency_key="click-1",
        )

    with pytest.raises(DBAPIError) as exc, migrated.begin() as conn:
        insert(
            conn,
            "interventions",
            org_id=ids["org"],
            alert_id=alert,
            actor_id=actor,
            action="ACK",
            idempotency_key="click-1",
        )
    assert sqlstate_error(exc) is errors.UniqueViolation


def test_a_shipment_is_on_one_truck_at_a_time(migrated: Engine) -> None:
    ids = seed(migrated)
    with migrated.begin() as conn:
        other = add_vehicle(conn, ids["org"], "TRK-102")
        insert(
            conn,
            "shipment_assignments",
            org_id=ids["org"],
            shipment_id=ids["shipment"],
            vehicle_id=ids["vehicle"],
            valid_from=T0,
            valid_to=T0 + timedelta(hours=4),
        )

    with pytest.raises(DBAPIError) as exc, migrated.begin() as conn:
        insert(
            conn,
            "shipment_assignments",
            org_id=ids["org"],
            shipment_id=ids["shipment"],
            vehicle_id=other,
            valid_from=T0 + timedelta(hours=2),
            valid_to=None,
        )
    assert sqlstate_error(exc) is errors.ExclusionViolation


def test_app_role_cannot_rewrite_append_only_tables(migrated: Engine) -> None:
    ids = seed(migrated)
    with migrated.begin() as conn:
        conn.execute(text("SET LOCAL ROLE watchtower_app"))
        assessment = insert(
            conn,
            "risk_assessments",
            org_id=ids["org"],
            shipment_id=ids["shipment"],
            vehicle_id=ids["vehicle"],
            assessed_at=T0,
            score=0.4,
            confidence=0.8,
            rule_version=1,
            evidence="{}",
        )

    for statement in (
        "UPDATE risk_assessments SET score = 0 WHERE id = :id",
        "DELETE FROM risk_assessments WHERE id = :id",
    ):
        with pytest.raises(DBAPIError) as exc:
            run_as_app(migrated, statement, id=assessment)
        assert sqlstate_error(exc) is errors.InsufficientPrivilege


def run_as_app(engine: Engine, statement: str, **params: Any) -> None:
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL ROLE watchtower_app"))
        conn.execute(text(statement), params)


def test_malformed_dedup_key_is_rejected(migrated: Engine) -> None:
    ids = seed(migrated)
    with pytest.raises(DBAPIError) as exc, migrated.begin() as conn:
        open_alert(conn, ids, dedup_key=MISSING_SHIPMENT_FIELD)
    assert sqlstate_error(exc) is errors.CheckViolation


def test_minute_series_routes_rows_to_utc_day_partitions(migrated: Engine) -> None:
    ids = seed(migrated)
    row = {
        "org_id": ids["org"],
        "vehicle_id": ids["vehicle"],
        "probe": "CARGO",
        "sample_count": 2,
        "total_centi": -3760,
        "low_centi": -1910,
        "high_centi": -1850,
        "bucket_version": 1,
    }
    with migrated.begin() as conn:
        for _ in range(2):  # idempotent
            conn.execute(text("SELECT ensure_minute_series_partitions('2026-10-05', 2)"))
        insert(
            conn,
            "minute_series",
            key="minute",
            minute=datetime(2026, 10, 5, 23, 59, tzinfo=UTC),
            **row,
        )
        partition = conn.execute(
            text("SELECT tableoid::regclass::text FROM minute_series WHERE minute = :m"),
            {"m": datetime(2026, 10, 5, 23, 59, tzinfo=UTC)},
        ).scalar()
    assert partition == "minute_series_20261005"

    # A day with no partition is refused, never silently stored elsewhere.
    with pytest.raises(DBAPIError) as exc, migrated.begin() as conn:
        insert(
            conn,
            "minute_series",
            key="minute",
            minute=datetime(2026, 10, 7, 0, 0, tzinfo=UTC),
            **row,
        )
    assert sqlstate_error(exc) is errors.CheckViolation
