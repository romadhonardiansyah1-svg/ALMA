from dataclasses import replace
from decimal import Decimal

import pytest

from alma.bars import MINUTE_NS, Bar
from alma.features import FeatureConfig, FeatureState
from alma.market_state import MarketState


def bar(minutes: int, index: int, open_: str, high: str, low: str, close: str) -> Bar:
    start = index * minutes * MINUTE_NS
    return Bar(
        minutes=minutes,
        start_ns=start,
        end_ns=start + minutes * MINUTE_NS,
        open=Decimal(open_),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=Decimal(1),
    )


def base_snapshot(now_ns: int):
    state = MarketState("BINANCE", "BTCUSDT-PERP")
    state.on_quote(
        now_ns,
        Decimal(100),
        Decimal("100.1"),
        Decimal(3),
        Decimal(1),
    )
    state.on_trade(now_ns, Decimal("100.05"), Decimal(1), aggressor=1)
    return state.snapshot(now_ns)


def test_features_use_closed_bars_and_are_replay_deterministic() -> None:
    state = FeatureState(FeatureConfig(acceptance_bars=2))
    first = base_snapshot(60 * MINUTE_NS)
    first = replace(
        first,
        h1=bar(60, 0, "100", "110", "90", "105"),
        m15=bar(15, 0, "100", "105", "95", "101"),
        m5=bar(5, 0, "100", "103", "97", "101"),
        m1=bar(1, 0, "100", "102", "99", "101"),
    )
    state.update(first)

    second = replace(
        base_snapshot(75 * MINUTE_NS),
        m15=bar(15, 1, "101", "106", "100", "105.5"),
        m5=bar(5, 1, "101", "102", "100", "101.5"),
        m1=bar(1, 1, "101", "102", "100", "101.5"),
    )
    features = state.update(second)

    assert features.liquidity_high == Decimal(105)
    assert features.liquidity_low == Decimal(95)
    assert features.h1_return == Decimal("0.05")
    assert features.h1_volatility == Decimal("0.05")
    assert features.h1_regime == 1
    assert features.m15_position == Decimal("1.05")
    assert features.m15_displacement == Decimal("0.45")
    assert features.m15_body_bps == Decimal("4.5") / Decimal(101) * 10_000
    assert features.m5_compression == Decimal(1) / Decimal(3)
    assert features.m1_return == Decimal("0.5") / Decimal(101)
    assert features.spread_bps == Decimal(10)
    assert features.top_book_imbalance == Decimal("0.5")
    assert len(features.feature_id) == 64

    twin = FeatureState(FeatureConfig(acceptance_bars=2))
    twin.update(first)
    assert twin.update(second).feature_id == features.feature_id
    assert state.update(second).feature_id == features.feature_id


def test_feature_state_rejects_future_bar_and_closed_bar_revision() -> None:
    state = FeatureState()
    snapshot = base_snapshot(10 * MINUTE_NS)
    future = replace(snapshot, m15=bar(15, 0, "100", "101", "99", "100"))
    with pytest.raises(ValueError, match="not closed"):
        state.update(future)

    closed = replace(
        base_snapshot(15 * MINUTE_NS),
        m15=bar(15, 0, "100", "101", "99", "100"),
    )
    state.update(closed)
    revised = replace(
        base_snapshot(16 * MINUTE_NS),
        m15=bar(15, 0, "100", "102", "99", "101"),
    )
    with pytest.raises(ValueError, match="revised"):
        state.update(revised)


def test_liquidity_level_stays_fixed_during_acceptance_window() -> None:
    state = FeatureState(FeatureConfig(acceptance_bars=2))
    state.update(
        replace(
            base_snapshot(15 * MINUTE_NS),
            m15=bar(15, 0, "95", "100", "90", "98"),
        )
    )
    state.update(
        replace(
            base_snapshot(30 * MINUTE_NS),
            m15=bar(15, 1, "99", "103", "98", "102"),
        )
    )
    features = state.update(
        replace(
            base_snapshot(45 * MINUTE_NS),
            m15=bar(15, 2, "102", "104", "101", "103"),
        )
    )

    assert features.liquidity_high == Decimal(100)
    assert features.acceptance_above == 2


def test_feature_config_rejects_invalid_thresholds() -> None:
    with pytest.raises(ValueError, match="history"):
        FeatureConfig(history_size=1)
    with pytest.raises(ValueError, match="acceptance"):
        FeatureConfig(acceptance_bars=0)
    with pytest.raises(ValueError, match="threshold"):
        FeatureConfig(flow_threshold=Decimal("NaN"))
