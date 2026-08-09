import pytest

from alma.f9 import CanaryEvidence, assess_canary, freeze_required, scale_allowed


def evidence(**changes) -> CanaryEvidence:
    values = {
        "replay_oos_passed": True,
        "shadow_passed": True,
        "binance_testnet_passed": True,
        "mt5_demo_passed": True,
        "recovery_passed": True,
        "protection_passed": True,
        "emergency_stop_passed": True,
        "operator_approved": True,
        "exposure_policy_selected": True,
        "venues": ("BINANCE",),
        "symbols": ("BTCUSDT-PERP",),
    }
    values.update(changes)
    return CanaryEvidence(**values)


def test_canary_is_fail_closed_and_single_venue_symbol() -> None:
    assert assess_canary(evidence()).ready
    blocked = assess_canary(
        evidence(
            mt5_demo_passed=False,
            operator_approved=False,
            venues=("BINANCE", "MT5"),
        )
    )
    assert not blocked.ready
    assert blocked.blockers == (
        "MT5_DEMO_PENDING",
        "OPERATOR_APPROVAL_REQUIRED",
        "EXACTLY_ONE_VENUE_REQUIRED",
    )
    assert not assess_canary(evidence(venues=("",), symbols=("",))).ready
    assert not assess_canary(
        evidence(venues=(1,), symbols=("BTCUSDT-PERP",))  # type: ignore[arg-type]
    ).ready


def test_freeze_and_scale_require_measured_evidence() -> None:
    assert freeze_required(
        stale_state=False,
        divergence=False,
        protection_failure=True,
        unexplained_loss=False,
    )
    assert not scale_allowed(
        live_samples=29,
        minimum_samples=30,
        execution_confirmed=True,
        costs_confirmed=True,
        recovery_confirmed=True,
        risk_confirmed=True,
        unexplained_loss=False,
    )
    assert scale_allowed(
        live_samples=30,
        minimum_samples=30,
        execution_confirmed=True,
        costs_confirmed=True,
        recovery_confirmed=True,
        risk_confirmed=True,
        unexplained_loss=False,
    )
    with pytest.raises(ValueError, match="sample"):
        scale_allowed(
            live_samples=0,
            minimum_samples=0,
            execution_confirmed=True,
            costs_confirmed=True,
            recovery_confirmed=True,
            risk_confirmed=True,
            unexplained_loss=False,
        )
