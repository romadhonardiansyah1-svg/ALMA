from collections.abc import Callable
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Literal

import msgspec


class DecisionAction(StrEnum):
    NO_CHANGE = "NO_CHANGE"
    OPEN_LONG = "OPEN_LONG"
    OPEN_SHORT = "OPEN_SHORT"
    INCREASE_LONG = "INCREASE_LONG"
    INCREASE_SHORT = "INCREASE_SHORT"
    REDUCE = "REDUCE"
    CLOSE = "CLOSE"
    REVERSE = "REVERSE"


class EntryMode(StrEnum):
    PASSIVE = "PASSIVE"
    AGGRESSIVE_LIMIT = "AGGRESSIVE_LIMIT"
    STOP_ENTRY = "STOP_ENTRY"
    MARKET_PROTECTED = "MARKET_PROTECTED"
    ADAPTIVE = "ADAPTIVE"
    WAIT_RETEST = "WAIT_RETEST"


class OnMissed(StrEnum):
    ABORT = "ABORT"
    WAIT_RETEST = "WAIT_RETEST"
    REQUEST_REVIEW = "REQUEST_REVIEW"


class OnPartialFill(StrEnum):
    KEEP_REMAINDER = "KEEP_REMAINDER"
    REPRICE_REMAINDER = "REPRICE_REMAINDER"
    CANCEL_REMAINDER = "CANCEL_REMAINDER"


class DecisionTarget(msgspec.Struct, forbid_unknown_fields=True):
    side: Literal["LONG", "SHORT", "FLAT"]
    volume: str


class DecisionEntry(msgspec.Struct, forbid_unknown_fields=True):
    mode: EntryMode
    preferred_low: str
    preferred_high: str
    max_acceptable_price: str
    ttl_seconds: int
    on_missed: OnMissed
    on_partial_fill: OnPartialFill


class DecisionExitTarget(msgspec.Struct, forbid_unknown_fields=True):
    price: str
    close_fraction: str


class DecisionContract(msgspec.Struct, forbid_unknown_fields=True):
    policy_version: Literal["alma-v1"]
    state_id: str
    decision_id: str
    created_at: datetime
    venue: str
    symbol: str
    action: DecisionAction
    target: DecisionTarget
    entry: DecisionEntry
    invalidation_price: str
    targets: list[DecisionExitTarget]
    review_triggers: list[str]
    evidence: list[str]
    uncertainty: str


def _coerce_decimal_strings(payload: bytes) -> bytes:
    # ponytail: models sometimes emit numbers (0.01) where the contract
    # mandates strings ("0.01") — coerce at the decode boundary, not per-caller
    import json
    try:
        obj = json.loads(payload)
    except json.JSONDecodeError:
        return payload
    if not isinstance(obj, dict):
        return payload
    # known decimal-string field paths in the DecisionContract shape
    _STRINGIFY_PATHS = {
        ("target", "volume"),
        ("entry", "preferred_low"),
        ("entry", "preferred_high"),
        ("entry", "max_acceptable_price"),
        ("invalidation_price",),
        ("uncertainty",),
    }
    _STRINGIFY_TARGETS = {"price", "close_fraction"}
    for path in _STRINGIFY_PATHS:
        ref = obj
        for key in path[:-1]:
            if not isinstance(ref, dict):
                break
            ref = ref.get(key, {})
        if isinstance(ref, dict):
            k = path[-1]
            if k in ref and isinstance(ref[k], (int, float)):
                ref[k] = str(ref[k])
    targets = obj.get("targets", [])
    if isinstance(targets, list):
        for t in targets:
            if isinstance(t, dict):
                for k in _STRINGIFY_TARGETS:
                    if k in t and isinstance(t[k], (int, float)):
                        t[k] = str(t[k])
    return json.dumps(obj, separators=(",", ":")).encode()


def parse_decision_contract(payload: bytes) -> DecisionContract:
    contract = msgspec.json.decode(_coerce_decimal_strings(payload), type=DecisionContract)
    if contract.created_at.utcoffset() != timedelta(0):
        raise ValueError("created_at must be UTC RFC3339")
    decimal_strings = (
        contract.target.volume,
        contract.entry.preferred_low,
        contract.entry.preferred_high,
        contract.entry.max_acceptable_price,
        contract.invalidation_price,
        *(target.price for target in contract.targets),
        contract.uncertainty,
    )
    try:
        decimal_values = tuple(map(Decimal, decimal_strings))
        target_fractions = [
            Decimal(target.close_fraction) for target in contract.targets
        ]
    except InvalidOperation:
        raise ValueError("decision decimals must be valid") from None
    if not all(value.is_finite() for value in decimal_values):
        raise ValueError("decision decimals must be finite")
    if not all(fraction.is_finite() for fraction in target_fractions):
        raise ValueError("target fractions must be finite")
    target_volume = Decimal(contract.target.volume)
    preferred_low = Decimal(contract.entry.preferred_low)
    preferred_high = Decimal(contract.entry.preferred_high)
    max_acceptable_price = Decimal(contract.entry.max_acceptable_price)
    invalidation_price = Decimal(contract.invalidation_price)
    target_prices = [Decimal(target.price) for target in contract.targets]
    uncertainty = Decimal(contract.uncertainty)
    if target_volume < 0:
        raise ValueError("target volume must be non-negative")
    if (contract.target.side == "FLAT") != (target_volume == 0):
        raise ValueError("target side and volume are inconsistent")
    required_side = {
        DecisionAction.OPEN_LONG: "LONG",
        DecisionAction.INCREASE_LONG: "LONG",
        DecisionAction.OPEN_SHORT: "SHORT",
        DecisionAction.INCREASE_SHORT: "SHORT",
        DecisionAction.CLOSE: "FLAT",
    }.get(contract.action)
    if required_side is not None and contract.target.side != required_side:
        raise ValueError("decision action and target are inconsistent")
    if contract.action is DecisionAction.REVERSE and contract.target.side == "FLAT":
        raise ValueError("reverse target must not be flat")
    if contract.entry.ttl_seconds <= 0:
        raise ValueError("entry TTL must be positive")
    if any(
        price <= 0
        for price in (
            preferred_low,
            preferred_high,
            max_acceptable_price,
            invalidation_price,
            *target_prices,
        )
    ):
        raise ValueError("decision prices must be positive")
    if preferred_low > preferred_high:
        raise ValueError("entry envelope low exceeds high")
    if any(fraction < 0 for fraction in target_fractions):
        raise ValueError("target fractions must be non-negative")
    if sum(target_fractions, start=Decimal(0)) > 1:
        raise ValueError("target fractions must sum to at most 1")
    protected_actions = {
        DecisionAction.OPEN_LONG,
        DecisionAction.OPEN_SHORT,
        DecisionAction.INCREASE_LONG,
        DecisionAction.INCREASE_SHORT,
        DecisionAction.REVERSE,
    }
    if contract.action in protected_actions and not any(
        fraction > 0 for fraction in target_fractions
    ):
        raise ValueError("exposure change requires a positive protection target")
    if not 0 <= uncertainty <= 1:
        raise ValueError("uncertainty must be between 0 and 1")
    return contract


def parse_decision_with_repair(
    payload: bytes, repair: Callable[[bytes], bytes]
) -> DecisionContract:
    try:
        return parse_decision_contract(payload)
    except msgspec.DecodeError:
        return parse_decision_contract(repair(payload))


def validate_decision_state(
    contract: DecisionContract, *, expected_state_id: str
) -> None:
    if contract.state_id != expected_state_id:
        raise ValueError("state_id does not match expected snapshot")


def validate_decision_expiry(
    contract: DecisionContract,
    *,
    now: datetime,
    max_future_skew: timedelta = timedelta(seconds=2),
) -> None:
    if max_future_skew < timedelta(0):
        raise ValueError("future skew must be non-negative")
    if contract.created_at > now + max_future_skew:
        raise ValueError("decision created_at is in the future")
    expires_at = contract.created_at + timedelta(seconds=contract.entry.ttl_seconds)
    if now >= expires_at:
        raise ValueError("decision contract has expired")
