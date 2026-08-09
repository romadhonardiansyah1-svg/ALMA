from dataclasses import dataclass


@dataclass(frozen=True)
class CanaryEvidence:
    replay_oos_passed: bool
    shadow_passed: bool
    binance_testnet_passed: bool
    mt5_demo_passed: bool
    recovery_passed: bool
    protection_passed: bool
    emergency_stop_passed: bool
    operator_approved: bool
    exposure_policy_selected: bool
    venues: tuple[str, ...]
    symbols: tuple[str, ...]


@dataclass(frozen=True)
class CanaryReadiness:
    ready: bool
    blockers: tuple[str, ...]


def assess_canary(evidence: CanaryEvidence) -> CanaryReadiness:
    def one_named(values: tuple[str, ...]) -> bool:
        return (
            len(values) == 1 and isinstance(values[0], str) and bool(values[0].strip())
        )

    checks = (
        (evidence.replay_oos_passed is True, "REPLAY_OOS_PENDING"),
        (evidence.shadow_passed is True, "SHADOW_PENDING"),
        (evidence.binance_testnet_passed is True, "BINANCE_TESTNET_PENDING"),
        (evidence.mt5_demo_passed is True, "MT5_DEMO_PENDING"),
        (evidence.recovery_passed is True, "RECOVERY_PENDING"),
        (evidence.protection_passed is True, "PROTECTION_PENDING"),
        (evidence.emergency_stop_passed is True, "EMERGENCY_STOP_PENDING"),
        (evidence.operator_approved is True, "OPERATOR_APPROVAL_REQUIRED"),
        (evidence.exposure_policy_selected is True, "EXPOSURE_POLICY_REQUIRED"),
        (one_named(evidence.venues), "EXACTLY_ONE_VENUE_REQUIRED"),
        (one_named(evidence.symbols), "EXACTLY_ONE_SYMBOL_REQUIRED"),
    )
    blockers = tuple(reason for passed, reason in checks if not passed)
    return CanaryReadiness(not blockers, blockers)


def scale_allowed(
    *,
    live_samples: int,
    minimum_samples: int,
    execution_confirmed: bool,
    costs_confirmed: bool,
    recovery_confirmed: bool,
    risk_confirmed: bool,
    unexplained_loss: bool,
) -> bool:
    if minimum_samples <= 0 or live_samples < 0:
        raise ValueError("sample counts must be non-negative with a positive minimum")
    return (
        live_samples >= minimum_samples
        and execution_confirmed is True
        and costs_confirmed is True
        and recovery_confirmed is True
        and risk_confirmed is True
        and unexplained_loss is False
    )


def freeze_required(
    *,
    stale_state: bool,
    divergence: bool,
    protection_failure: bool,
    unexplained_loss: bool,
) -> bool:
    return any(
        value is not False
        for value in (stale_state, divergence, protection_failure, unexplained_loss)
    )
