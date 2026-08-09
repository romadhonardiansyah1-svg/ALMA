from decimal import Decimal

from alma.features import FeatureConfig
from alma.market_recording import MarketEvent
from alma.research import detect_candidates, oos_report, research_replay
from alma.strategies import SetupEvidence
from alma.strategy_replay import ReplayConfig

MINUTE = 60_000_000_000


def trade(minute: int, second: int, price: str) -> MarketEvent:
    return MarketEvent.trade(
        minute * MINUTE + second * 1_000_000_000,
        "BINANCE",
        "BTCUSDT-PERP",
        Decimal(price),
        Decimal(1),
        1,
    )


def sweep_stream() -> list[MarketEvent]:
    return [
        MarketEvent.book_snapshot(0, "BINANCE", "BTCUSDT-PERP", 1),
        MarketEvent.quote(
            0,
            "BINANCE",
            "BTCUSDT-PERP",
            Decimal("104.9"),
            Decimal(105),
            Decimal(3),
            Decimal(1),
        ),
        trade(0, 0, "105"),
        trade(0, 30, "110"),
        trade(1, 0, "100"),
        trade(14, 0, "105"),
        trade(15, 0, "105"),
        trade(16, 0, "99"),
        trade(16, 30, "100"),
        trade(29, 0, "100.5"),
        trade(29, 30, "101"),
        MarketEvent.book_snapshot(
            29 * MINUTE + 59_000_000_000, "BINANCE", "BTCUSDT-PERP", 2
        ),
        MarketEvent.quote(
            29 * MINUTE + 59_000_000_000,
            "BINANCE",
            "BTCUSDT-PERP",
            Decimal("100.9"),
            Decimal(101),
            Decimal(3),
            Decimal(1),
        ),
        trade(30, 0, "101"),
    ]


def test_candidate_generation_is_chronological_and_evidence_only() -> None:
    events = sweep_stream()
    candidates = detect_candidates(events, FeatureConfig(acceptance_bars=1))

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.setup == "LIQUIDITY_SWEEP_REVERSAL"
    assert candidate.direction == 1
    assert candidate.event_index == len(events) - 1
    assert candidate.observed_at_ns == 30 * MINUTE
    assert candidate.invalidation == Decimal(99)
    assert not hasattr(candidate, "quantity")

    future_changed = events + [
        MarketEvent.quote(
            31 * MINUTE,
            "BINANCE",
            "BTCUSDT-PERP",
            Decimal(50),
            Decimal("50.1"),
            Decimal(1),
            Decimal(3),
        )
    ]
    assert (
        detect_candidates(future_changed, FeatureConfig(acceptance_bars=1))[0]
        == candidate
    )


def test_research_replay_uses_only_quotes_after_candidate() -> None:
    events = sweep_stream()
    events.extend(
        [
            MarketEvent.quote(
                31 * MINUTE,
                "BINANCE",
                "BTCUSDT-PERP",
                Decimal(101),
                Decimal("101.1"),
                Decimal(3),
                Decimal(1),
            ),
            MarketEvent.quote(
                32 * MINUTE,
                "BINANCE",
                "BTCUSDT-PERP",
                Decimal(110),
                Decimal("110.1"),
                Decimal(3),
                Decimal(1),
            ),
        ]
    )

    result = research_replay(
        events,
        feature_config=FeatureConfig(acceptance_bars=1),
        replay_config=ReplayConfig(
            latency_ns=0,
            slippage_bps=Decimal(0),
            fee_bps=Decimal(0),
            max_entry_deviation_bps=Decimal(100),
            max_entry_delay_ns=2 * MINUTE,
        ),
    )

    assert len(result.candidates) == 1
    assert result.benchmark.trades[0].entry_ns == 31 * MINUTE
    assert result.benchmark.trades[0].entry_price == Decimal("101.1")
    assert result.benchmark.trades[0].exit_reason == "TARGET"
    assert (
        result.benchmark.result_hash
        == research_replay(
            events,
            feature_config=FeatureConfig(acceptance_bars=1),
            replay_config=ReplayConfig(
                latency_ns=0,
                slippage_bps=Decimal(0),
                fee_bps=Decimal(0),
                max_entry_deviation_bps=Decimal(100),
                max_entry_delay_ns=2 * MINUTE,
            ),
        ).benchmark.result_hash
    )


def test_oos_report_uses_only_purged_test_folds_and_is_honest() -> None:
    events = [
        MarketEvent.book_snapshot(0, "BINANCE", "BTCUSDT-PERP", 1),
        MarketEvent.quote(
            10 * MINUTE,
            "BINANCE",
            "BTCUSDT-PERP",
            Decimal(100),
            Decimal("100.1"),
            Decimal(1),
            Decimal(1),
        ),
        MarketEvent.quote(
            11 * MINUTE,
            "BINANCE",
            "BTCUSDT-PERP",
            Decimal(110),
            Decimal("110.1"),
            Decimal(1),
            Decimal(1),
        ),
    ]
    candidates = tuple(
        SetupEvidence(
            setup="LIQUIDITY_SWEEP_REVERSAL",
            direction=1,
            feature_id=str(index).zfill(64),
            observed_at_ns=index * MINUTE,
            entry_reference=Decimal(100),
            invalidation=Decimal(90),
            target_reference=Decimal(110),
            cost_bps=Decimal(0),
            estimated_edge_bps=Decimal(100),
            evidence=("test",),
        )
        for index in range(8)
    )
    config = ReplayConfig(
        max_entry_delay_ns=20 * MINUTE,
        max_entry_deviation_bps=Decimal(20),
        fee_bps=Decimal(1),
    )

    report = oos_report(
        events,
        candidates,
        replay_config=config,
        train_size=3,
        test_size=2,
        purge=1,
        minimum_fills=30,
    )

    assert tuple((fold.test_start, fold.test_stop) for fold in report.folds) == (
        (4, 6),
        (6, 8),
    )
    # The train-side position remains open through both test folds; it must not
    # be reset and counted again as OOS profit at either boundary.
    assert report.filled == 0
    assert report.missed == 4
    assert report.status == "INSUFFICIENT_SAMPLE"
    assert report.costs == 0
    assert report.expectancy is None
    assert len(report.report_hash) == 64
    assert (
        oos_report(
            events,
            candidates,
            replay_config=config,
            train_size=3,
            test_size=2,
            purge=1,
            minimum_fills=30,
        ).report_hash
        == report.report_hash
    )


def test_empty_oos_report_is_insufficient_not_profitable() -> None:
    report = oos_report(
        [],
        (),
        train_size=3,
        test_size=2,
        purge=1,
    )

    assert report.status == "INSUFFICIENT_SAMPLE"
    assert report.filled == 0
    assert report.net_pnl == 0
    assert report.expectancy is None
    assert report.profit_factor is None
