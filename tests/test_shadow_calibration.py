import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from alma.ledger import open_ledger
from alma.shadow_calibration import (
    calibration_summary,
    confidence_bucket,
    promotion_allowed,
    record_shadow_outcome,
)

NOW = datetime(2026, 7, 31, 10, tzinfo=UTC)


def contract(decision_id: str, uncertainty: str) -> bytes:
    return json.dumps(
        {
            "policy_version": "alma-v1",
            "state_id": "state-1",
            "decision_id": decision_id,
            "created_at": NOW.isoformat(),
            "venue": "BINANCE",
            "symbol": "BTCUSDT-PERP",
            "action": "INCREASE_LONG",
            "target": {"side": "LONG", "volume": "0.25"},
            "entry": {
                "mode": "ADAPTIVE",
                "preferred_low": "60000",
                "preferred_high": "61000",
                "max_acceptable_price": "61500",
                "ttl_seconds": 300,
                "on_missed": "ABORT",
                "on_partial_fill": "CANCEL_REMAINDER",
            },
            "invalidation_price": "59000",
            "targets": [{"price": "62000", "close_fraction": "1"}],
            "review_triggers": ["FLOW_SHIFT"],
            "evidence": ["shadow"],
            "uncertainty": uncertainty,
        },
        separators=(",", ":"),
    ).encode()


def add_shadow_decision(
    connection,
    decision_id: str,
    uncertainty: str,
    validation_result: str = "ACCEPTED",
) -> None:
    connection.execute(
        "INSERT INTO decisions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            decision_id,
            "state-1",
            NOW.isoformat(),
            contract(decision_id, uncertainty),
            validation_result,
            "model-a",
            "prompt",
            "policy",
            "code",
            "SHADOW",
        ),
    )
    connection.execute(
        "INSERT INTO shadow_runs "
        "(request_id, state_id, decision_id, status, validation_error, requested_model, "
        "actual_model, prompt_tokens, completion_tokens, latency_ms, hooks, venue, symbol, "
        "setup, regime, session, news_state, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            f"request:{decision_id}",
            "state-1",
            decision_id,
            validation_result,
            None,
            "model-a",
            "model-a",
            10,
            5,
            1.0,
            "SETUP",
            "BINANCE",
            "BTCUSDT-PERP",
            "LIQUIDITY_SWEEP_REVERSAL",
            "BULL_LOW_VOL",
            "LONDON",
            "NONE",
            NOW.isoformat(),
        ),
    )
    connection.commit()


def test_outcomes_are_derived_append_only_and_calibrated(tmp_path) -> None:
    connection = open_ledger(tmp_path / "alma.db")
    add_shadow_decision(connection, "decision-1", "0.2")
    add_shadow_decision(connection, "decision-2", "0.2")

    record_shadow_outcome(
        connection,
        decision_id="decision-1",
        won=True,
        net_return=Decimal("0.01"),
        observed_at=NOW - timedelta(days=10),
    )
    record_shadow_outcome(
        connection,
        decision_id="decision-2",
        won=False,
        net_return=Decimal("-0.02"),
        observed_at=NOW,
    )

    summary = calibration_summary(
        connection,
        model="model-a",
        venue="BINANCE",
        symbol="BTCUSDT-PERP",
        setup="LIQUIDITY_SWEEP_REVERSAL",
        regime="BULL_LOW_VOL",
        session="LONDON",
        news_state="NONE",
        bucket=8,
        now=NOW,
        half_life_days=Decimal(10),
    )
    assert summary.count == 2
    assert summary.weighted_count == Decimal("1.5")
    assert summary.win_rate == Decimal(1) / Decimal(3)
    assert summary.net_expectancy == Decimal("-0.01")
    assert summary.calibration_error == abs(Decimal(1) / Decimal(3) - Decimal("0.8"))

    with pytest.raises(Exception, match="append-only"):
        connection.execute(
            "UPDATE shadow_outcomes SET won = 1 WHERE decision_id = 'decision-2'"
        )
    with pytest.raises(Exception, match="UNIQUE"):
        record_shadow_outcome(
            connection,
            decision_id="decision-1",
            won=True,
            net_return=Decimal("0.01"),
            observed_at=NOW,
        )

    add_shadow_decision(
        connection, "decision-rejected", "0.2", validation_result="REJECTED"
    )
    with pytest.raises(ValueError, match="accepted shadow decision"):
        record_shadow_outcome(
            connection,
            decision_id="decision-rejected",
            won=True,
            net_return=Decimal(1),
            observed_at=NOW,
        )
    connection.close()


def test_confidence_buckets_and_input_boundaries() -> None:
    assert confidence_bucket(Decimal(0)) == 0
    assert confidence_bucket(Decimal("0.84")) == 8
    assert confidence_bucket(Decimal(1)) == 9
    for invalid in (Decimal("-0.1"), Decimal("1.1"), Decimal("NaN")):
        with pytest.raises(ValueError, match="confidence"):
            confidence_bucket(invalid)


def test_promotion_requires_samples_replay_and_no_regression() -> None:
    class Summary:
        count = 30

    summary = Summary()
    assert promotion_allowed(
        summary, minimum_samples=30, replay_passed=True, no_regression=True
    )
    assert not promotion_allowed(
        summary, minimum_samples=31, replay_passed=True, no_regression=True
    )
    assert not promotion_allowed(
        summary, minimum_samples=30, replay_passed=False, no_regression=True
    )
    assert not promotion_allowed(
        summary, minimum_samples=30, replay_passed=True, no_regression=False
    )
    with pytest.raises(ValueError, match="minimum"):
        promotion_allowed(
            summary, minimum_samples=0, replay_passed=True, no_regression=True
        )
