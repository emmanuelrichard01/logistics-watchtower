"""Every gateway rule, as a matrix, plus properties over random valid and corrupted readings."""

import copy
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from watchtower_gateway.validation import Accepted, Reason, Rejected, validate_reading

from .conftest import KEYS, NOW, NOW_MS, reading, resign


def check(r: Any) -> Accepted | Rejected:
    return validate_reading(r, ingest_ms=NOW_MS, keys=KEYS)


def test_valid_reading_is_accepted_and_stamped() -> None:
    out = check(reading())
    assert isinstance(out, Accepted)
    assert out.vehicle_id == "TRK-101"
    assert out.telemetry["ingest_time"] == NOW
    assert out.telemetry["event_time"].tzinfo is not None


def test_epoch_millisecond_event_time_is_accepted() -> None:
    r = reading()
    r["event_time"] = NOW_MS - 30_000
    assert isinstance(check(resign(r)), Accepted)


def mutate(path: str, value: Any) -> Callable[[dict[str, Any]], None]:
    def apply(r: dict[str, Any]) -> None:
        *parents, leaf = path.split(".")
        target = r
        for p in parents:
            target = target[p]
        target[leaf] = value

    return apply


def drop(name: str) -> Callable[[dict[str, Any]], None]:
    return lambda r: r.pop(name)


# (case, mutation applied before re-signing, expected reason)
MATRIX: list[tuple[str, Callable[[dict[str, Any]], None], Reason]] = [
    ("unknown field", mutate("extra", 1), Reason.MALFORMED),
    ("missing field", drop("reefer"), Reason.MALFORMED),
    ("wrong type", mutate("seq", "many"), Reason.MALFORMED),
    ("bad enum", mutate("reefer.door", "AJAR"), Reason.MALFORMED),
    ("naive event_time", mutate("event_time", "2026-10-05T07:12:00"), Reason.MALFORMED),
    ("bad event_id", mutate("event_id", "not-a-uuid"), Reason.MALFORMED),
    ("negative seq", mutate("seq", -1), Reason.MALFORMED),
    ("vehicle id charset", mutate("vehicle_id", "TRK 101"), Reason.BAD_ID),
    ("boot id charset", mutate("boot_id", "b/1"), Reason.BAD_ID),
    ("unregistered device", mutate("device_id", "EDGE-9999"), Reason.UNKNOWN_DEVICE),
    ("event_id for another seq", mutate("seq", 1), Reason.EVENT_ID_MISMATCH),
    ("cargo probe too hot", mutate("reefer.cargo_probe_c", 95.0), Reason.OUT_OF_RANGE),
    ("setpoint too cold", mutate("reefer.setpoint_c", -90.0), Reason.OUT_OF_RANGE),
    ("ambient impossible", mutate("vehicle.ambient_c", 81.0), Reason.OUT_OF_RANGE),
    ("outside Nigeria", mutate("position.lat", 51.5), Reason.OUT_OF_RANGE),
    ("too fast", mutate("position.speed_kmh", 170.0), Reason.OUT_OF_RANGE),
    ("negative speed", mutate("position.speed_kmh", -1.0), Reason.OUT_OF_RANGE),
    (
        "from the future",
        mutate("event_time", (NOW + timedelta(minutes=6)).isoformat()),
        Reason.FUTURE_EVENT,
    ),
    (
        "older than 7 days",
        mutate("event_time", (NOW - timedelta(days=8)).isoformat()),
        Reason.STALE_EVENT,
    ),
]


@pytest.mark.parametrize(("case", "corrupt", "reason"), MATRIX, ids=[m[0] for m in MATRIX])
def test_each_rule_quarantines_with_its_reason(
    case: str, corrupt: Callable[[dict[str, Any]], None], reason: Reason
) -> None:
    r = reading()
    corrupt(r)
    out = check(resign(r))
    assert isinstance(out, Rejected), case
    assert out.reason is reason, (case, out.detail)


def test_tampered_reading_fails_the_signature() -> None:
    r = reading()
    r["reefer"]["cargo_probe_c"] = -25.0  # plausible value, not re-signed
    out = check(r)
    assert isinstance(out, Rejected)
    assert out.reason is Reason.BAD_SIGNATURE


def test_signature_from_another_devices_key_fails() -> None:
    r = reading(device_id="EDGE-0102")
    r["device_id"] = "EDGE-0101"  # claims to be another device, keeps its own signature
    out = check(r)
    assert isinstance(out, Rejected)
    assert out.reason in {Reason.BAD_SIGNATURE, Reason.EVENT_ID_MISMATCH}


@pytest.mark.parametrize("raw", [None, 42, "reading", [1, 2]])
def test_non_objects_are_malformed(raw: Any) -> None:
    out = check(raw)
    assert isinstance(out, Rejected)
    assert out.reason is Reason.MALFORMED


def test_missing_signature_is_malformed() -> None:
    r = reading()
    del r["signature"]
    out = check(r)
    assert isinstance(out, Rejected)
    assert out.reason is Reason.MALFORMED


def test_null_probes_and_no_gps_fix_are_valid() -> None:
    r = reading()
    r["reefer"]["cargo_probe_c"] = None
    r["position"] = None
    assert isinstance(check(resign(r)), Accepted)


def test_quarantine_key_prefers_vehicle_then_device() -> None:
    r = reading()
    r["reefer"]["cargo_probe_c"] = 99.0
    out = check(resign(r))
    assert isinstance(out, Rejected)
    assert out.key == "TRK-101"
    assert check({"device_id": "EDGE-0101"}).key == "EDGE-0101"  # type: ignore[union-attr]


# ---- Properties -------------------------------------------------------------

valid_readings = st.builds(
    reading,
    seq=st.integers(min_value=0, max_value=2**40),
    device_id=st.sampled_from(sorted(KEYS)),
    age=st.timedeltas(min_value=timedelta(0), max_value=timedelta(days=6, hours=23)),
    vehicle_id=st.from_regex(r"[A-Za-z0-9._:-]{1,64}", fullmatch=True),
)


@settings(max_examples=200, deadline=None)
@given(
    valid_readings, st.floats(min_value=-80, max_value=80), st.floats(min_value=0, max_value=160)
)
def test_any_valid_reading_is_accepted(r: dict[str, Any], cargo: float, speed: float) -> None:
    r = copy.deepcopy(r)
    r["reefer"]["cargo_probe_c"] = cargo
    r["position"]["speed_kmh"] = speed
    assert isinstance(check(resign(r)), Accepted)


@settings(max_examples=300, deadline=None)
@given(valid_readings, st.sampled_from(MATRIX), st.booleans())
def test_any_single_corruption_is_quarantined_with_the_right_reason(
    r: dict[str, Any], case: tuple[str, Callable[[dict[str, Any]], None], Reason], re_sign: bool
) -> None:
    name, corrupt, reason = case
    r = copy.deepcopy(r)
    corrupt(r)
    out = check(resign(r) if re_sign else r)
    assert isinstance(out, Rejected), name
    if re_sign:
        assert out.reason is reason, (name, out.detail)
    else:
        # Not re-signed: caught at the first check it fails, at the latest the signature.
        assert out.reason in {
            Reason.MALFORMED,
            Reason.BAD_ID,
            Reason.UNKNOWN_DEVICE,
            Reason.BAD_SIGNATURE,
        }, (name, out.reason)


def test_time_limits_are_inclusive_at_the_edges() -> None:
    edge_future = reading()
    edge_future["event_time"] = datetime.fromtimestamp(
        (NOW_MS + 5 * 60_000) / 1000, UTC
    ).isoformat()
    assert isinstance(check(resign(edge_future)), Accepted)
    edge_old = reading()
    edge_old["event_time"] = datetime.fromtimestamp(
        (NOW_MS - 7 * 86_400_000) / 1000, UTC
    ).isoformat()
    assert isinstance(check(resign(edge_old)), Accepted)
