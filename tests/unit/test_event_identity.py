import uuid

import pytest
from hypothesis import given
from hypothesis import strategies as st
from watchtower_contracts import event_id
from watchtower_contracts.identity import ID_PATTERN

ids = st.from_regex(ID_PATTERN, fullmatch=True)
seqs = st.integers(min_value=0, max_value=2**63 - 1)


def test_identity_is_pinned() -> None:
    # If this fails, every stored event_id is invalidated and replays stop
    # deduplicating. Never update the expected value; fix the code instead.
    assert event_id("EDGE-0101", "b-20261005-0412", 18273) == uuid.UUID(
        "e6970414-38f9-566d-86aa-fa98b086dc4e"
    )


@given(ids, ids, seqs)
def test_same_inputs_give_the_same_uuid5(device: str, boot: str, seq: int) -> None:
    first = event_id(device, boot, seq)
    assert first == event_id(device, boot, seq)
    assert first.version == 5


@given(st.tuples(ids, ids, seqs), st.tuples(ids, ids, seqs))
def test_different_inputs_give_different_ids(
    a: tuple[str, str, int], b: tuple[str, str, int]
) -> None:
    if a != b:
        assert event_id(*a) != event_id(*b)


@pytest.mark.parametrize(
    ("device", "boot", "seq"),
    [
        ("", "b", 1),
        ("d", "", 1),
        ("d/x", "b", 1),
        ("d", "b/x", 1),
        ("d", "\ud800", 1),
        ("d" * 65, "b", 1),
        ("d", "b", -1),
    ],
)
def test_ambiguous_or_invalid_inputs_are_rejected(device: str, boot: str, seq: int) -> None:
    with pytest.raises(ValueError, match=r"device_id|boot_id|seq"):
        event_id(device, boot, seq)
