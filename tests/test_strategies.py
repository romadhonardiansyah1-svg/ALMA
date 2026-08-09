from dataclasses import replace
from decimal import Decimal

from alma.features import FeatureConfig, FeatureSnapshot
from alma.strategies import detect_liquidity_sweep, detect_liquidity_vacuum


def features(**changes) -> FeatureSnapshot:
    base = FeatureSnapshot(
        feature_id="f" * 64,
        source_state_id="s" * 64,
        observed_at_ns=1,
        market_age_ms=0,
        price=Decimal(101),
        spread_bps=Decimal(1),
        top_book_imbalance=Decimal("0.5"),
        flow_imbalance=Decimal("0.5"),
        tick_velocity_1s=10,
        realized_volatility=Decimal("0.001"),
        funding_rate=Decimal("0.0001"),
        book_valid=True,
        h1_return=Decimal("0.01"),
        h1_volatility=Decimal("0.01"),
        h1_regime=1,
        liquidity_high=Decimal(110),
        liquidity_low=Decimal(100),
        m15_open=Decimal("99.5"),
        m15_high=Decimal(102),
        m15_low=Decimal(99),
        m15_close=Decimal(101),
        m15_position=Decimal("0.1"),
        m15_displacement=Decimal("0.15"),
        m15_body_bps=Decimal(150),
        acceptance_above=0,
        acceptance_below=0,
        m5_compression=Decimal("0.4"),
        m1_return=Decimal("0.002"),
    )
    return replace(base, **changes)


def test_liquidity_sweep_reversal_returns_evidence_not_order() -> None:
    config = FeatureConfig(sweep_distance_ratio=Decimal("0.05"))

    bullish = detect_liquidity_sweep(features(), config)
    bearish = detect_liquidity_sweep(
        features(
            price=Decimal(109),
            flow_imbalance=Decimal("-0.5"),
            top_book_imbalance=Decimal("-0.5"),
            h1_regime=-1,
            m15_open=Decimal("110.5"),
            m15_high=Decimal(111),
            m15_low=Decimal(108),
            m15_close=Decimal(109),
            m1_return=Decimal("-0.002"),
        ),
        config,
    )

    assert bullish is not None
    assert bullish.setup == "LIQUIDITY_SWEEP_REVERSAL"
    assert bullish.direction == 1
    assert bullish.invalidation == Decimal(99)
    assert bullish.target_reference == Decimal(110)
    assert bullish.estimated_edge_bps > bullish.cost_bps
    assert not hasattr(bullish, "quantity")

    assert bearish is not None
    assert bearish.direction == -1
    assert bearish.invalidation == Decimal(111)
    assert bearish.target_reference == Decimal(100)


def test_sweep_rejects_no_reclaim_bad_flow_cost_and_invalid_feed() -> None:
    config = FeatureConfig()

    assert detect_liquidity_sweep(features(m15_close=Decimal("99.8")), config) is None
    assert (
        detect_liquidity_sweep(features(flow_imbalance=Decimal("-0.5")), config) is None
    )
    assert detect_liquidity_sweep(features(spread_bps=Decimal(2_000)), config) is None
    assert detect_liquidity_sweep(features(book_valid=False), config) is None


def test_liquidity_vacuum_continuation_requires_acceptance_and_thin_side() -> None:
    config = FeatureConfig(acceptance_bars=2)
    bullish = features(
        price=Decimal(112),
        m15_open=Decimal(109),
        m15_high=Decimal(113),
        m15_low=Decimal(108),
        m15_close=Decimal(112),
        m15_position=Decimal("1.2"),
        m15_displacement=Decimal("0.3"),
        m15_body_bps=Decimal(275),
        acceptance_above=2,
        m5_compression=Decimal("0.4"),
    )
    bearish = replace(
        bullish,
        price=Decimal(98),
        flow_imbalance=Decimal("-0.5"),
        top_book_imbalance=Decimal("-0.5"),
        h1_regime=-1,
        m15_open=Decimal(101),
        m15_high=Decimal(102),
        m15_low=Decimal(97),
        m15_close=Decimal(98),
        m15_position=Decimal("-0.2"),
        acceptance_above=0,
        acceptance_below=2,
        m1_return=Decimal("-0.002"),
    )

    long_setup = detect_liquidity_vacuum(bullish, config)
    short_setup = detect_liquidity_vacuum(bearish, config)

    assert long_setup is not None and long_setup.direction == 1
    assert long_setup.setup == "LIQUIDITY_VACUUM_CONTINUATION"
    assert long_setup.invalidation == Decimal(110)
    assert long_setup.target_reference == Decimal(120)
    assert long_setup.estimated_edge_bps > long_setup.cost_bps
    assert short_setup is not None and short_setup.direction == -1
    assert short_setup.invalidation == Decimal(100)
    assert short_setup.target_reference == Decimal(90)


def test_vacuum_rejects_missing_acceptance_compression_flow_and_crowding() -> None:
    config = FeatureConfig(acceptance_bars=2)
    candidate = features(
        price=Decimal(112),
        m15_close=Decimal(112),
        m15_displacement=Decimal("0.3"),
        m15_body_bps=Decimal(275),
        acceptance_above=2,
    )

    assert (
        detect_liquidity_vacuum(replace(candidate, acceptance_above=1), config) is None
    )
    assert (
        detect_liquidity_vacuum(
            replace(candidate, m5_compression=Decimal("0.8")), config
        )
        is None
    )
    assert (
        detect_liquidity_vacuum(
            replace(candidate, flow_imbalance=Decimal("0.1")), config
        )
        is None
    )
    assert (
        detect_liquidity_vacuum(
            replace(candidate, funding_rate=Decimal("0.002")), config
        )
        is None
    )


def test_detectors_reject_stale_market_state() -> None:
    stale = features(market_age_ms=2_001)
    assert detect_liquidity_sweep(stale) is None
    assert (
        detect_liquidity_vacuum(
            replace(
                stale,
                price=Decimal(112),
                m15_close=Decimal(112),
                m15_displacement=Decimal("0.3"),
                m15_body_bps=Decimal(275),
                acceptance_above=2,
            )
        )
        is None
    )
