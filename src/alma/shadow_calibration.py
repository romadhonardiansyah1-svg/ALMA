import sqlite3
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from alma.database import immediate_transaction
from alma.decision_contract import parse_decision_contract


@dataclass(frozen=True)
class CalibrationSummary:
    count: int
    weighted_count: Decimal
    win_rate: Decimal | None
    net_expectancy: Decimal | None
    calibration_error: Decimal | None


def confidence_bucket(confidence: Decimal) -> int:
    if not confidence.is_finite() or not 0 <= confidence <= 1:
        raise ValueError("confidence must be finite and within [0, 1]")
    return min(9, int(confidence * 10))


def record_shadow_outcome(
    connection: sqlite3.Connection,
    *,
    decision_id: str,
    won: bool,
    net_return: Decimal,
    observed_at: datetime,
) -> None:
    if not net_return.is_finite():
        raise ValueError("net return must be finite")
    if observed_at.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    with immediate_transaction(connection):
        eligible = connection.execute(
            "SELECT d.raw_contract, r.venue, r.symbol, r.setup, r.regime, "
            "r.session, r.news_state FROM decisions d JOIN shadow_runs r "
            "ON r.decision_id = d.decision_id "
            "WHERE d.decision_id = ? AND d.validation_result = 'ACCEPTED' "
            "AND d.provenance = 'SHADOW' AND r.status = 'ACCEPTED'",
            (decision_id,),
        ).fetchone()
        if eligible is None:
            raise ValueError("outcome requires an accepted shadow decision")
        contract = parse_decision_contract(eligible[0])
        uncertainty = Decimal(contract.uncertainty)
        venue, symbol, setup, regime, session, news_state = eligible[1:]
        if not all((venue, symbol, setup, regime, session, news_state)):
            raise ValueError("accepted shadow run has incomplete dimensions")
        connection.execute(
            "INSERT INTO shadow_outcomes "
            "(decision_id, venue, symbol, setup, regime, session, news_state, "
            "confidence_bucket, uncertainty, won, net_return, observed_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                decision_id,
                venue,
                symbol,
                setup,
                regime,
                session,
                news_state,
                confidence_bucket(Decimal(1) - uncertainty),
                str(uncertainty),
                int(won),
                str(net_return),
                observed_at.isoformat(),
            ),
        )


def calibration_summary(
    connection: sqlite3.Connection,
    *,
    model: str,
    venue: str,
    symbol: str,
    setup: str,
    regime: str,
    session: str,
    news_state: str,
    bucket: int,
    now: datetime,
    half_life_days: Decimal,
) -> CalibrationSummary:
    if not 0 <= bucket <= 9:
        raise ValueError("confidence bucket must be within [0, 9]")
    if not half_life_days.is_finite() or half_life_days <= 0:
        raise ValueError("half-life must be finite and positive")
    if now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    rows = connection.execute(
        "SELECT o.won, o.net_return, o.uncertainty, o.observed_at "
        "FROM shadow_outcomes o JOIN decisions d ON d.decision_id = o.decision_id "
        "WHERE d.model_id = ? AND o.venue = ? AND o.symbol = ? AND o.setup = ? "
        "AND o.regime = ? AND o.session = ? AND o.news_state = ? "
        "AND o.confidence_bucket = ? ORDER BY o.seq",
        (model, venue, symbol, setup, regime, session, news_state, bucket),
    ).fetchall()
    if not rows:
        return CalibrationSummary(0, Decimal(0), None, None, None)

    weighted_count = weighted_wins = weighted_return = weighted_confidence = Decimal(0)
    for won, net_return, uncertainty, observed_at in rows:
        observed = datetime.fromisoformat(observed_at)
        age_seconds = max(Decimal(0), Decimal(str((now - observed).total_seconds())))
        age_days = age_seconds / Decimal(86_400)
        weight = Decimal(2) ** (-(age_days / half_life_days))
        weighted_count += weight
        weighted_wins += weight * Decimal(won)
        weighted_return += weight * Decimal(net_return)
        weighted_confidence += weight * (Decimal(1) - Decimal(uncertainty))
    win_rate = weighted_wins / weighted_count
    expectancy = weighted_return / weighted_count
    calibrated_confidence = weighted_confidence / weighted_count
    return CalibrationSummary(
        count=len(rows),
        weighted_count=weighted_count,
        win_rate=win_rate,
        net_expectancy=expectancy,
        calibration_error=abs(win_rate - calibrated_confidence),
    )


def promotion_allowed(
    summary: CalibrationSummary,
    *,
    minimum_samples: int,
    replay_passed: bool,
    no_regression: bool,
) -> bool:
    if minimum_samples <= 0:
        raise ValueError("minimum samples must be positive")
    return summary.count >= minimum_samples and replay_passed and no_regression
