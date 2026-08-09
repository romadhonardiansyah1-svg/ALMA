from dataclasses import dataclass
from decimal import Decimal

from alma.features import FeatureConfig, FeatureSnapshot


@dataclass(frozen=True)
class SetupEvidence:
    setup: str
    direction: int
    feature_id: str
    observed_at_ns: int
    entry_reference: Decimal
    invalidation: Decimal
    target_reference: Decimal | None
    cost_bps: Decimal
    estimated_edge_bps: Decimal
    evidence: tuple[str, ...]
    event_index: int | None = None


def _cost(features: FeatureSnapshot, config: FeatureConfig) -> Decimal | None:
    if features.spread_bps is None or features.spread_bps < 0:
        return None
    return features.spread_bps + config.modeled_cost_bps


def _ready(features: FeatureSnapshot, config: FeatureConfig) -> bool:
    return (
        features.book_valid
        and 0 <= features.market_age_ms <= config.max_market_age_ms
        and features.price is not None
        and features.liquidity_high is not None
        and features.liquidity_low is not None
        and features.liquidity_high > features.liquidity_low
        and features.m15_high is not None
        and features.m15_low is not None
        and features.m15_close is not None
        and features.m1_return is not None
    )


def detect_liquidity_sweep(
    features: FeatureSnapshot,
    config: FeatureConfig | None = None,
) -> SetupEvidence | None:
    config = config or FeatureConfig()
    if not _ready(features, config):
        return None
    price = features.price
    high = features.liquidity_high
    low = features.liquidity_low
    assert price is not None and high is not None and low is not None
    assert features.m15_high is not None and features.m15_low is not None
    assert features.m15_close is not None and features.m1_return is not None
    cost = _cost(features, config)
    if cost is None:
        return None

    threshold = (high - low) * config.sweep_distance_ratio
    bullish = (
        features.m15_low <= low - threshold
        and features.m15_close > low
        and features.m1_return > 0
        and features.flow_imbalance >= config.flow_threshold
        and features.top_book_imbalance >= config.book_imbalance_threshold
    )
    bearish = (
        features.m15_high >= high + threshold
        and features.m15_close < high
        and features.m1_return < 0
        and features.flow_imbalance <= -config.flow_threshold
        and features.top_book_imbalance <= -config.book_imbalance_threshold
    )
    if bullish:
        direction = 1
        invalidation = features.m15_low
        target = high
        edge = max(Decimal(0), (target - price) / price * 10_000)
    elif bearish:
        direction = -1
        invalidation = features.m15_high
        target = low
        edge = max(Decimal(0), (price - target) / price * 10_000)
    else:
        return None
    if edge <= cost:
        return None
    return SetupEvidence(
        setup="LIQUIDITY_SWEEP_REVERSAL",
        direction=direction,
        feature_id=features.feature_id,
        observed_at_ns=features.observed_at_ns,
        entry_reference=price,
        invalidation=invalidation,
        target_reference=target,
        cost_bps=cost,
        estimated_edge_bps=edge,
        evidence=(
            "liquidity_range_normalized_cross",
            "m15_reclaim",
            "m1_flow_reversal",
            "edge_above_modeled_cost",
        ),
    )


def detect_liquidity_vacuum(
    features: FeatureSnapshot,
    config: FeatureConfig | None = None,
) -> SetupEvidence | None:
    config = config or FeatureConfig()
    if (
        not _ready(features, config)
        or features.m15_displacement is None
        or features.m15_body_bps is None
        or features.m5_compression is None
        or features.funding_rate is None
        or abs(features.funding_rate) > config.funding_limit
        or features.m15_displacement < config.displacement_threshold
        or features.m5_compression > config.compression_threshold
    ):
        return None
    price = features.price
    high = features.liquidity_high
    low = features.liquidity_low
    assert price is not None and high is not None and low is not None
    assert features.m15_close is not None and features.m1_return is not None
    cost = _cost(features, config)
    if cost is None or features.m15_body_bps <= cost:
        return None

    bullish = (
        features.m15_close > high
        and features.acceptance_above >= config.acceptance_bars
        and features.m1_return > 0
        and features.flow_imbalance >= config.flow_threshold
        and features.top_book_imbalance >= config.book_imbalance_threshold
        and features.h1_regime >= 0
    )
    bearish = (
        features.m15_close < low
        and features.acceptance_below >= config.acceptance_bars
        and features.m1_return < 0
        and features.flow_imbalance <= -config.flow_threshold
        and features.top_book_imbalance <= -config.book_imbalance_threshold
        and features.h1_regime <= 0
    )
    if bullish:
        direction = 1
        invalidation = high
        target = high + (high - low)
    elif bearish:
        direction = -1
        invalidation = low
        target = low - (high - low)
    else:
        return None
    if target <= 0:
        return None
    edge = max(
        Decimal(0),
        (target - price) / price * 10_000
        if direction > 0
        else (price - target) / price * 10_000,
    )
    if edge <= cost:
        return None
    return SetupEvidence(
        setup="LIQUIDITY_VACUUM_CONTINUATION",
        direction=direction,
        feature_id=features.feature_id,
        observed_at_ns=features.observed_at_ns,
        entry_reference=price,
        invalidation=invalidation,
        target_reference=target,
        cost_bps=cost,
        estimated_edge_bps=edge,
        evidence=(
            "m5_volatility_compression",
            "m15_displacement_acceptance",
            "directional_top_book_and_flow",
            "funding_within_limit",
            "measured_range_projection_above_modeled_cost",
        ),
    )
