from datetime import UTC, datetime

import msgspec
import pytest

from alma.decision_contract import (
    parse_decision_contract,
    parse_decision_with_repair,
    validate_decision_expiry,
    validate_decision_state,
)

VALID_DECISION = b"""{
    "policy_version": "alma-v1",
    "state_id": "01JSTATE",
    "decision_id": "01JDECISION",
    "created_at": "2026-07-31T05:00:00Z",
    "venue": "MT5",
    "symbol": "XAUUSDC",
    "action": "INCREASE_LONG",
    "target": {"side": "LONG", "volume": "0.25"},
    "entry": {
        "mode": "ADAPTIVE",
        "preferred_low": "3284.80",
        "preferred_high": "3286.20",
        "max_acceptable_price": "3287.40",
        "ttl_seconds": 45,
        "on_missed": "WAIT_RETEST",
        "on_partial_fill": "REPRICE_REMAINDER"
    },
    "invalidation_price": "3279.60",
    "targets": [
        {"price": "3292.00", "close_fraction": "0.40"},
        {"price": "3298.50", "close_fraction": "0.60"}
    ],
    "review_triggers": ["FLOW_REVERSAL"],
    "evidence": ["sweep_reclaim"],
    "uncertainty": "0.31"
}"""

DECIMAL_FIELDS = [
    pytest.param(b'"volume": "0.25"', b'"volume": "%s"', id="target-volume"),
    pytest.param(
        b'"preferred_low": "3284.80"',
        b'"preferred_low": "%s"',
        id="preferred-low",
    ),
    pytest.param(
        b'"preferred_high": "3286.20"',
        b'"preferred_high": "%s"',
        id="preferred-high",
    ),
    pytest.param(
        b'"max_acceptable_price": "3287.40"',
        b'"max_acceptable_price": "%s"',
        id="max-acceptable-price",
    ),
    pytest.param(
        b'"invalidation_price": "3279.60"',
        b'"invalidation_price": "%s"',
        id="invalidation-price",
    ),
    pytest.param(b'"price": "3292.00"', b'"price": "%s"', id="target-price"),
    pytest.param(b'"uncertainty": "0.31"', b'"uncertainty": "%s"', id="uncertainty"),
]


def test_repairs_invalid_decision_once() -> None:
    repair_calls = []

    def repair(payload: bytes) -> bytes:
        repair_calls.append(payload)
        return VALID_DECISION

    contract = parse_decision_with_repair(b"{", repair)

    assert contract.decision_id == "01JDECISION"
    assert repair_calls == [b"{"]


def test_does_not_repair_semantically_invalid_decision() -> None:
    payload = VALID_DECISION.replace(b'"uncertainty": "0.31"', b'"uncertainty": "NaN"')
    repaired = False

    def repair(_: bytes) -> bytes:
        nonlocal repaired
        repaired = True
        return VALID_DECISION

    with pytest.raises(ValueError, match="decision decimals must be finite"):
        parse_decision_with_repair(payload, repair)

    assert not repaired


def test_rejects_mismatched_snapshot_state() -> None:
    contract = parse_decision_contract(VALID_DECISION)

    with pytest.raises(ValueError, match="state_id does not match expected snapshot"):
        validate_decision_state(contract, expected_state_id="01JOTHER")


def test_rejects_expired_decision() -> None:
    contract = parse_decision_contract(VALID_DECISION)

    with pytest.raises(ValueError, match="decision contract has expired"):
        validate_decision_expiry(
            contract, now=datetime(2026, 7, 31, 5, 0, 46, tzinfo=UTC)
        )

    with pytest.raises(ValueError, match="decision contract has expired"):
        validate_decision_expiry(
            contract, now=datetime(2026, 7, 31, 5, 0, 45, tzinfo=UTC)
        )


def test_rejects_decision_created_beyond_clock_skew() -> None:
    contract = parse_decision_contract(VALID_DECISION)

    with pytest.raises(ValueError, match="created_at is in the future"):
        validate_decision_expiry(
            contract, now=datetime(2026, 7, 31, 4, 59, 57, tzinfo=UTC)
        )

    validate_decision_expiry(contract, now=datetime(2026, 7, 31, 4, 59, 58, tzinfo=UTC))


@pytest.mark.parametrize(
    ("old", "new"),
    [
        pytest.param(b'"alma-v1"', b'"alma-v2"', id="policy-version"),
        pytest.param(b'"INCREASE_LONG"', b'"HOLD_FOREVER"', id="action"),
        pytest.param(b'"ADAPTIVE"', b'"CHASE"', id="entry-mode"),
        pytest.param(b'"WAIT_RETEST"', b'"CHASE"', id="on-missed"),
        pytest.param(
            b'"REPRICE_REMAINDER"',
            b'"IGNORE_REMAINDER"',
            id="on-partial-fill",
        ),
    ],
)
def test_rejects_unknown_literal_or_enum_value(old: bytes, new: bytes) -> None:
    payload = VALID_DECISION.replace(old, new, 1)

    with pytest.raises(msgspec.ValidationError, match="Invalid enum value"):
        parse_decision_contract(payload)


def test_rejects_non_utc_created_at() -> None:
    payload = VALID_DECISION.replace(
        b'"2026-07-31T05:00:00Z"', b'"2026-07-31T05:00:00+01:00"'
    )

    with pytest.raises(ValueError, match="created_at must be UTC RFC3339"):
        parse_decision_contract(payload)


def test_rejects_target_fractions_above_one() -> None:
    payload = VALID_DECISION.replace(
        b'"close_fraction": "0.60"', b'"close_fraction": "0.61"'
    )

    with pytest.raises(ValueError, match="target fractions must sum to at most 1"):
        parse_decision_contract(payload)


def test_rejects_exposure_change_without_positive_protection_target() -> None:
    payload = VALID_DECISION.replace(
        b'"targets": [\n        {"price": "3292.00", "close_fraction": "0.40"},\n        {"price": "3298.50", "close_fraction": "0.60"}\n    ]',
        b'"targets": []',
    )

    with pytest.raises(ValueError, match="protection target"):
        parse_decision_contract(payload)


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        (b'"volume": "0.25"', b'"volume": "-0.01"', "target volume"),
        (b'"ttl_seconds": 45', b'"ttl_seconds": 0', "TTL"),
        (b'"preferred_low": "3284.80"', b'"preferred_low": "0"', "prices"),
        (b'"close_fraction": "0.40"', b'"close_fraction": "-0.01"', "fractions"),
        (b'"uncertainty": "0.31"', b'"uncertainty": "1.01"', "uncertainty"),
    ],
)
def test_rejects_invalid_economic_ranges(old: bytes, new: bytes, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        parse_decision_contract(VALID_DECISION.replace(old, new, 1))


def test_rejects_reversed_entry_envelope() -> None:
    payload = VALID_DECISION.replace(
        b'"preferred_high": "3286.20"',
        b'"preferred_high": "3284.00"',
    )
    with pytest.raises(ValueError, match="entry envelope"):
        parse_decision_contract(payload)


def test_rejects_unknown_target_side() -> None:
    payload = VALID_DECISION.replace(b'"side": "LONG"', b'"side": "BOTH"')
    with pytest.raises(msgspec.ValidationError, match="Invalid enum value"):
        parse_decision_contract(payload)


def test_rejects_malformed_target_fraction() -> None:
    payload = VALID_DECISION.replace(
        b'"close_fraction": "0.60"', b'"close_fraction": "not-a-decimal"'
    )

    with pytest.raises(ValueError, match="decision decimals must be valid"):
        parse_decision_contract(payload)


@pytest.mark.parametrize("fraction", ["NaN", "sNaN", "Infinity"])
def test_rejects_non_finite_target_fraction(fraction: str) -> None:
    payload = VALID_DECISION.replace(
        b'"close_fraction": "0.60"',
        f'"close_fraction": "{fraction}"'.encode(),
    )

    with pytest.raises(ValueError, match="target fractions must be finite"):
        parse_decision_contract(payload)


@pytest.mark.parametrize(("original", "replacement"), DECIMAL_FIELDS)
def test_rejects_malformed_decimal_fields(original: bytes, replacement: bytes) -> None:
    payload = VALID_DECISION.replace(original, replacement % b"not-a-decimal", 1)

    with pytest.raises(ValueError, match="decision decimals must be valid"):
        parse_decision_contract(payload)


@pytest.mark.parametrize(("original", "replacement"), DECIMAL_FIELDS)
@pytest.mark.parametrize("non_finite", ["NaN", "sNaN", "Infinity"])
def test_rejects_non_finite_decimal_fields(
    original: bytes, replacement: bytes, non_finite: str
) -> None:
    payload = VALID_DECISION.replace(original, replacement % non_finite.encode(), 1)

    with pytest.raises(ValueError, match="decision decimals must be finite"):
        parse_decision_contract(payload)


@pytest.mark.parametrize(
    ("old", "new"),
    [
        pytest.param(
            b'"volume": "0.25"',
            b'"volume": "0.25", "unexpected": true',
            id="target",
        ),
        pytest.param(
            b'"on_partial_fill": "REPRICE_REMAINDER"',
            b'"on_partial_fill": "REPRICE_REMAINDER", "unexpected": true',
            id="entry",
        ),
        pytest.param(
            b'"close_fraction": "0.40"',
            b'"close_fraction": "0.40", "unexpected": true',
            id="targets-item",
        ),
        pytest.param(
            b'"uncertainty": "0.31"',
            b'"uncertainty": "0.31", "unexpected": true',
            id="top-level",
        ),
    ],
)
def test_rejects_unknown_field(old: bytes, new: bytes) -> None:
    payload = VALID_DECISION.replace(old, new, 1)

    with pytest.raises(msgspec.ValidationError, match="unknown field"):
        parse_decision_contract(payload)


@pytest.mark.parametrize(
    ("action", "side", "volume"),
    [
        ("OPEN_LONG", "SHORT", "0.25"),
        ("INCREASE_SHORT", "LONG", "0.25"),
        ("CLOSE", "LONG", "0.25"),
        ("REVERSE", "FLAT", "0"),
        ("NO_CHANGE", "FLAT", "0.25"),
        ("NO_CHANGE", "LONG", "0"),
    ],
)
def test_rejects_contradictory_action_and_target(
    action: str, side: str, volume: str
) -> None:
    payload = VALID_DECISION.replace(b'"INCREASE_LONG"', f'"{action}"'.encode())
    payload = payload.replace(b'"side": "LONG"', f'"side": "{side}"'.encode())
    payload = payload.replace(b'"volume": "0.25"', f'"volume": "{volume}"'.encode())
    with pytest.raises(ValueError, match="inconsistent|must not be flat"):
        parse_decision_contract(payload)
