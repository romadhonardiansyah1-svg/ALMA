from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum


class VenueMode(StrEnum):
    OFF = "OFF"
    MONITOR = "MONITOR"
    MANAGE_ONLY = "MANAGE_ONLY"
    TRADE = "TRADE"


class OpenPositionPolicy(StrEnum):
    MANAGE = "MANAGE"
    FREEZE = "FREEZE"
    CLOSE_AND_DISABLE = "CLOSE_AND_DISABLE"


@dataclass(frozen=True)
class ModeTransitionPlan:
    active_mode: VenueMode
    desired_quantity: Decimal
    ensure_protection: bool
    final_mode: VenueMode


def plan_mode_transition(
    *,
    current: VenueMode,
    requested: VenueMode,
    actual: Decimal,
    policy: OpenPositionPolicy | None,
) -> ModeTransitionPlan:
    if not actual.is_finite():
        raise ValueError("actual position must be finite")
    if current is not VenueMode.TRADE or requested is VenueMode.TRADE or actual == 0:
        return ModeTransitionPlan(requested, actual, False, requested)
    if policy is None:
        raise ValueError("open position policy is required")
    if policy is OpenPositionPolicy.CLOSE_AND_DISABLE:
        return ModeTransitionPlan(VenueMode.MANAGE_ONLY, Decimal(0), True, requested)
    if policy is OpenPositionPolicy.MANAGE:
        return ModeTransitionPlan(
            VenueMode.MANAGE_ONLY,
            actual,
            True,
            VenueMode.MANAGE_ONLY,
        )
    return ModeTransitionPlan(requested, actual, True, requested)


def allows_new_exposure(mode: VenueMode) -> bool:
    return mode is VenueMode.TRADE


def allows_quantity_change(
    mode: VenueMode,
    *,
    actual: Decimal,
    desired: Decimal,
) -> bool:
    return (
        actual.is_finite()
        and desired.is_finite()
        and actual != desired
        and (
            mode is VenueMode.TRADE
            or (
                mode is VenueMode.MANAGE_ONLY
                and abs(desired) < abs(actual)
                and actual * desired >= 0
            )
        )
    )
