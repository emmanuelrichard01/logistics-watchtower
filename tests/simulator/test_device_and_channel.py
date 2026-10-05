from typing import Any

import pytest
from watchtower_simulator.channel import Channel, per_step
from watchtower_simulator.clock import rng
from watchtower_simulator.device import DeliveryPolicy, Device


def reading() -> dict[str, Any]:
    return {"link": {"signal_dbm": None, "buffered": False}}


def device(**policy: Any) -> Device:
    return Device("EDGE-0001", "20261005", rng(1, "test"), DeliveryPolicy(**policy))


def test_per_step_probability_matches_per_minute_over_a_minute() -> None:
    p = 0.2
    assert 1 - (1 - per_step(p, 5.0)) ** 12 == pytest.approx(p)


def test_forced_outage_is_always_down_and_does_not_shift_the_stream() -> None:
    a, b = Channel(rng(7, "c")), Channel(rng(7, "c"))
    assert not any(a.step(0.01, 0.3, 5.0, forced_down=True) for _ in range(100))
    for _ in range(100):
        b.step(0.01, 0.3, 5.0, forced_down=False)
    assert a.rng.random() == b.rng.random()


def test_sequence_numbers_restart_on_reboot_with_a_new_boot_id() -> None:
    d = device()
    first = [d.stamp(reading(), i * 1000) for i in range(3)]
    boot = d.boot_id
    d.reboot()
    after = d.stamp(reading(), 9000)
    assert [r["seq"] for r in first] == [1, 2, 3]
    assert after["seq"] == 1
    assert after["boot_id"] != boot
    assert len({r["event_id"] for r in [*first, after]}) == 4


def test_buffer_replays_in_order_flagged_buffered_while_live_continues() -> None:
    d = device(replay_per_step=1)
    for i in range(5):
        assert (
            d.handle(d.stamp(reading(), i * 30_000), i * 30_000, link_up=False, excursion=False)
            == []
        )
    out = d.handle(d.stamp(reading(), 200_000), 200_000, link_up=True, excursion=False)
    assert [x.reading["seq"] for x in out] == [6, 1]
    assert [x.reading["link"]["buffered"] for x in out] == [False, True]
    drained = [
        x.reading["seq"] for t in range(4) for x in d.drain(205_000 + t * 5000, link_up=True)
    ]
    assert drained == [2, 3, 4, 5]


def test_full_buffer_drops_routine_readings_before_excursion_evidence() -> None:
    d = device(buffer_capacity=3)
    d.handle(d.stamp(reading(), 0), 0, link_up=False, excursion=True)
    for i in range(1, 5):
        d.handle(d.stamp(reading(), i), i, link_up=False, excursion=False)
    kept = [(r["seq"], exc) for r, exc in d.buffer]
    assert kept[0] == (1, True)
    assert d.dropped == 2


def test_duplicates_share_the_event_id_and_arrive_later() -> None:
    d = device(duplicate_probability=1.0, max_copies=3)
    out = d.handle(d.stamp(reading(), 0), 0, link_up=True, excursion=False)
    assert len(out) >= 2
    assert len({x.reading["event_id"] for x in out}) == 1
    assert [x.ingest_ms for x in out] == sorted(x.ingest_ms for x in out)
