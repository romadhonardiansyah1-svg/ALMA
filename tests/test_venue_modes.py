from decimal import Decimal

from alma.venue_modes import (
    OpenPositionPolicy,
    VenueMode,
    allows_new_exposure,
    allows_quantity_change,
    plan_mode_transition,
)


def test_venue_mode_values_match_contract() -> None:
    assert tuple(VenueMode) == (
        VenueMode.OFF,
        VenueMode.MONITOR,
        VenueMode.MANAGE_ONLY,
        VenueMode.TRADE,
    )
    assert tuple(mode.value for mode in VenueMode) == (
        "OFF",
        "MONITOR",
        "MANAGE_ONLY",
        "TRADE",
    )


def test_only_trade_mode_allows_new_exposure() -> None:
    assert {mode: allows_new_exposure(mode) for mode in VenueMode} == {
        VenueMode.OFF: False,
        VenueMode.MONITOR: False,
        VenueMode.MANAGE_ONLY: False,
        VenueMode.TRADE: True,
    }


def test_manage_only_allows_only_exposure_reduction() -> None:
    assert allows_quantity_change(
        VenueMode.MANAGE_ONLY,
        actual=Decimal(2),
        desired=Decimal(1),
    )
    assert not allows_quantity_change(
        VenueMode.MANAGE_ONLY,
        actual=Decimal(2),
        desired=Decimal(2),
    )
    assert not allows_quantity_change(
        VenueMode.MANAGE_ONLY,
        actual=Decimal(2),
        desired=Decimal(3),
    )


def test_manage_only_rejects_reversal_to_smaller_opposite_position() -> None:
    assert not allows_quantity_change(
        VenueMode.MANAGE_ONLY,
        actual=Decimal(2),
        desired=Decimal(-1),
    )


def test_quantity_change_policy_matches_venue_modes() -> None:
    assert {
        mode: (
            allows_quantity_change(mode, actual=Decimal(2), desired=Decimal(3)),
            allows_quantity_change(mode, actual=Decimal(2), desired=Decimal(2)),
        )
        for mode in VenueMode
    } == {
        VenueMode.OFF: (False, False),
        VenueMode.MONITOR: (False, False),
        VenueMode.MANAGE_ONLY: (False, False),
        VenueMode.TRADE: (True, False),
    }


def test_quantity_change_rejects_non_finite_quantities() -> None:
    for mode in VenueMode:
        assert not allows_quantity_change(
            mode,
            actual=Decimal("NaN"),
            desired=Decimal(1),
        )
        assert not allows_quantity_change(
            mode,
            actual=Decimal(1),
            desired=Decimal("Infinity"),
        )


def test_open_position_transition_requires_explicit_policy() -> None:
    with __import__("pytest").raises(ValueError, match="policy"):
        plan_mode_transition(
            current=VenueMode.TRADE,
            requested=VenueMode.OFF,
            actual=Decimal(1),
            policy=None,
        )


def test_non_finite_position_is_rejected_during_mode_transition() -> None:
    with __import__("pytest").raises(ValueError, match="finite"):
        plan_mode_transition(
            current=VenueMode.TRADE,
            requested=VenueMode.OFF,
            actual=Decimal("NaN"),
            policy=OpenPositionPolicy.FREEZE,
        )
