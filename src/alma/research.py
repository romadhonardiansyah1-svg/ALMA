import hashlib
import json
from dataclasses import asdict, dataclass, replace
from decimal import Decimal

from alma.features import FeatureConfig, FeatureState
from alma.market_recording import MarketEvent, read_events
from alma.market_state import MarketState
from alma.strategies import (
    SetupEvidence,
    detect_liquidity_sweep,
    detect_liquidity_vacuum,
)
from alma.strategy_replay import (
    BenchmarkResult,
    ReplayConfig,
    ReplayTrade,
    benchmark,
    purged_walk_forward,
)


@dataclass(frozen=True)
class ResearchResult:
    candidates: tuple[SetupEvidence, ...]
    benchmark: BenchmarkResult


@dataclass(frozen=True)
class FoldReport:
    test_start: int
    test_stop: int
    filled: int
    missed: int
    net_pnl: Decimal
    expectancy: Decimal | None
    profit_factor: Decimal | None
    max_drawdown: Decimal
    costs: Decimal


@dataclass(frozen=True)
class OOSReport:
    status: str
    folds: tuple[FoldReport, ...]
    filled: int
    missed: int
    net_pnl: Decimal
    expectancy: Decimal | None
    profit_factor: Decimal | None
    max_drawdown: Decimal
    costs: Decimal
    report_hash: str


def _bar_key(snapshot) -> tuple[int | None, ...]:
    return tuple(
        bar.start_ns if bar is not None else None
        for bar in (snapshot.m1, snapshot.m5, snapshot.m15, snapshot.h1)
    )


def detect_candidates(
    events: list[MarketEvent],
    config: FeatureConfig | None = None,
) -> tuple[SetupEvidence, ...]:
    if not events:
        return ()
    config = config or FeatureConfig()
    venue, symbol = events[0].venue, events[0].symbol
    state = MarketState(venue, symbol)
    features = FeatureState(config)
    candidates: list[SetupEvidence] = []
    previous_key: tuple[int | None, ...] | None = None
    observed_at_ns = 0

    for event_index, event in enumerate(events):
        event.apply(state)
        observed_at_ns = max(observed_at_ns, event.ts_event_ns)
        snapshot = state.snapshot(observed_at_ns)
        key = _bar_key(snapshot)
        if key == previous_key:
            continue
        previous_key = key
        feature_snapshot = features.update(snapshot)
        for detector in (detect_liquidity_sweep, detect_liquidity_vacuum):
            candidate = detector(feature_snapshot, config)
            if candidate is not None:
                candidates.append(replace(candidate, event_index=event_index))

    return tuple(candidates)


def research_replay(
    events: list[MarketEvent],
    *,
    feature_config: FeatureConfig | None = None,
    replay_config: ReplayConfig | None = None,
) -> ResearchResult:
    candidates = detect_candidates(events, feature_config)
    result = benchmark(events, list(candidates), replay_config)
    return ResearchResult(candidates=candidates, benchmark=result)


def _metrics(
    trades: tuple[ReplayTrade, ...],
) -> tuple[
    int,
    int,
    Decimal,
    Decimal | None,
    Decimal | None,
    Decimal,
    Decimal,
]:
    filled = tuple(trade for trade in trades if trade.status == "FILLED")
    missed = len(trades) - len(filled)
    net = sum((trade.net_pnl for trade in filled), Decimal(0))
    expectancy = net / len(filled) if filled else None
    gains = sum((trade.net_pnl for trade in filled if trade.net_pnl > 0), Decimal(0))
    losses = -sum(
        (trade.net_pnl for trade in filled if trade.net_pnl < 0),
        Decimal(0),
    )
    profit_factor = gains / losses if losses else None
    equity = peak = drawdown = Decimal(0)
    for trade in filled:
        equity += trade.net_pnl
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    costs = sum((trade.fees + trade.funding for trade in filled), Decimal(0))
    return len(filled), missed, net, expectancy, profit_factor, drawdown, costs


def oos_report(
    events: list[MarketEvent],
    candidates: tuple[SetupEvidence, ...],
    *,
    replay_config: ReplayConfig | None = None,
    train_size: int,
    test_size: int,
    purge: int,
    minimum_fills: int = 30,
) -> OOSReport:
    if minimum_fills <= 0:
        raise ValueError("minimum fills must be positive")
    if not candidates:
        folds: tuple[tuple[range, range], ...] = ()
    else:
        folds = purged_walk_forward(
            total=len(candidates),
            train_size=train_size,
            test_size=test_size,
            purge=purge,
        )
    reports: list[FoldReport] = []
    all_trades: list[ReplayTrade] = []
    if folds:
        start = folds[0][0].start
        stop = folds[-1][1].stop
        # ponytail: one chronological pass preserves open-position state across
        # train/purge/test boundaries; only test trades enter reported metrics.
        simulated = benchmark(
            events, list(candidates[start:stop]), replay_config
        ).trades
        for _, test in folds:
            trades = simulated[test.start - start : test.stop - start]
            metrics = _metrics(trades)
            all_trades.extend(trades)
            reports.append(FoldReport(test.start, test.stop, *metrics))
    metrics = _metrics(tuple(all_trades))
    status = "MEASURED" if metrics[0] >= minimum_fills else "INSUFFICIENT_SAMPLE"
    payload = {
        "status": status,
        "folds": [asdict(report) for report in reports],
        "metrics": metrics,
    }
    report_hash = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()
    return OOSReport(status, tuple(reports), *metrics, report_hash)


def research_parquet(
    root: str,
    venue: str,
    symbol: str,
    *,
    feature_config: FeatureConfig | None = None,
    replay_config: ReplayConfig | None = None,
) -> ResearchResult:
    return research_replay(
        read_events(root, venue, symbol),
        feature_config=feature_config,
        replay_config=replay_config,
    )
