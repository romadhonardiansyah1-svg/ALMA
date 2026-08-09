from dataclasses import replace
from decimal import Decimal

import pytest

from alma.market_recording import MarketEvent
from alma.strategies import SetupEvidence
from alma.strategy_replay import ReplayConfig, benchmark, purged_walk_forward

SECOND = 1_000_000_000


def setup(**changes) -> SetupEvidence:
    base = SetupEvidence(
        setup="LIQUIDITY_SWEEP_REVERSAL",
        direction=1,
        feature_id="f" * 64,
        observed_at_ns=SECOND,
        entry_reference=Decimal(100),
        invalidation=Decimal(98),
        target_reference=Decimal(104),
        cost_bps=Decimal(6),
        estimated_edge_bps=Decimal(400),
        evidence=("test",),
    )
    return replace(base, **changes)


def quote(ts: int, bid: str, ask: str) -> MarketEvent:
    return MarketEvent.quote(
        ts,
        "BINANCE",
        "BTCUSDT-PERP",
        Decimal(bid),
        Decimal(ask),
        Decimal(1),
        Decimal(1),
    )


def events() -> list[MarketEvent]:
    return [
        MarketEvent.book_snapshot(0, "BINANCE", "BTCUSDT-PERP", 100),
        quote(SECOND, "99.9", "100.0"),
        MarketEvent.funding(
            SECOND + SECOND // 2,
            "BINANCE",
            "BTCUSDT-PERP",
            Decimal("0.0008"),
        ),
        quote(2 * SECOND, "100.0", "100.1"),
        MarketEvent.funding(
            3 * SECOND,
            "BINANCE",
            "BTCUSDT-PERP",
            Decimal("0.0008"),
        ),
        MarketEvent.funding_settlement(
            3 * SECOND + 1,
            "BINANCE",
            "BTCUSDT-PERP",
            Decimal("0.0008"),
            Decimal("100.5"),
        ),
        quote(4 * SECOND, "104.0", "104.1"),
    ]


def test_benchmark_models_latency_bid_ask_costs_partial_fill_and_funding() -> None:
    config = ReplayConfig(
        latency_ns=SECOND,
        slippage_bps=Decimal(10),
        fee_bps=Decimal(4),
        partial_fill_fraction=Decimal("0.5"),
        max_entry_deviation_bps=Decimal(50),
        max_entry_delay_ns=3 * SECOND,
        max_holding_ns=10 * SECOND,
    )

    result = benchmark(events(), [setup()], config)
    trade = result.trades[0]

    assert trade.status == "FILLED"
    assert trade.exit_reason == "TARGET"
    assert trade.entry_ns == 2 * SECOND
    assert trade.exit_ns == 4 * SECOND
    assert trade.quantity == Decimal("0.5")
    assert trade.entry_price == Decimal("100.2001")
    assert trade.exit_price == Decimal("103.8960")
    assert trade.gross_pnl == (trade.exit_price - trade.entry_price) * trade.quantity
    assert trade.fees > 0
    assert trade.funding > 0
    assert trade.net_pnl == trade.gross_pnl - trade.fees - trade.funding
    assert result.total_net_pnl == trade.net_pnl
    assert len(result.result_hash) == 64

    assert benchmark(events(), [setup()], config).result_hash == result.result_hash


def test_funding_only_applies_at_explicit_settlement() -> None:
    stream = [
        MarketEvent.book_snapshot(0, "BINANCE", "BTCUSDT-PERP", 1),
        quote(2 * SECOND, "100", "100.1"),
        MarketEvent.funding(
            3 * SECOND,
            "BINANCE",
            "BTCUSDT-PERP",
            Decimal("0.01"),
        ),
        MarketEvent.funding_settlement(
            3 * SECOND + 1,
            "BINANCE",
            "BTCUSDT-PERP",
            Decimal("0.01"),
            Decimal(100),
        ),
        quote(4 * SECOND, "101", "101.1"),
    ]

    trade = benchmark(
        stream,
        [setup(target_reference=Decimal(110), invalidation=Decimal(90))],
        ReplayConfig(
            slippage_bps=Decimal(0),
            fee_bps=Decimal(0),
        ),
    ).trades[0]

    assert trade.funding == Decimal(1)


def test_rate_update_without_settlement_has_no_funding_charge() -> None:
    stream = [
        MarketEvent.book_snapshot(0, "BINANCE", "BTCUSDT-PERP", 1),
        quote(2 * SECOND, "100", "100.1"),
        MarketEvent.funding(3 * SECOND, "BINANCE", "BTCUSDT-PERP", Decimal("0.01")),
        quote(4 * SECOND, "101", "101.1"),
    ]
    trade = benchmark(
        stream,
        [setup(target_reference=Decimal(110), invalidation=Decimal(90))],
        ReplayConfig(slippage_bps=Decimal(0), fee_bps=Decimal(0)),
    ).trades[0]
    assert trade.funding == 0


def test_disconnect_closes_at_last_executable_quote() -> None:
    stream = [
        MarketEvent.book_snapshot(0, "BINANCE", "BTCUSDT-PERP", 1),
        quote(2 * SECOND, "100", "100.1"),
        quote(3 * SECOND, "95", "95.1"),
        MarketEvent.book_invalidate(4 * SECOND, "BINANCE", "BTCUSDT-PERP"),
    ]

    trade = benchmark(
        stream,
        [setup(target_reference=Decimal(110), invalidation=Decimal(90))],
        ReplayConfig(slippage_bps=Decimal(0), fee_bps=Decimal(0)),
    ).trades[0]

    assert trade.exit_reason == "DISCONNECT"
    assert trade.exit_price == Decimal(95)
    assert trade.gross_pnl == Decimal("-5.1")


def test_short_uses_bid_entry_ask_exit_and_invalidation() -> None:
    candidate = setup(
        direction=-1,
        entry_reference=Decimal(100),
        invalidation=Decimal(102),
        target_reference=Decimal(96),
    )
    stream = [
        MarketEvent.book_snapshot(0, "BINANCE", "BTCUSDT-PERP", 1),
        quote(2 * SECOND, "99.9", "100.0"),
        quote(3 * SECOND, "102.0", "102.1"),
    ]

    trade = benchmark(
        stream,
        [candidate],
        ReplayConfig(latency_ns=0, slippage_bps=Decimal(0), fee_bps=Decimal(0)),
    ).trades[0]

    assert trade.entry_price == Decimal("99.9")
    assert trade.exit_price == Decimal("102.1")
    assert trade.exit_reason == "INVALIDATION"
    assert trade.gross_pnl < 0


def test_missed_entry_and_disconnect_fail_closed() -> None:
    stale = [
        MarketEvent.book_snapshot(0, "BINANCE", "BTCUSDT-PERP", 1),
        MarketEvent.book_invalidate(SECOND, "BINANCE", "BTCUSDT-PERP"),
        quote(2 * SECOND, "100", "100.1"),
    ]
    config = ReplayConfig(max_entry_delay_ns=2 * SECOND)

    missed = benchmark(stale, [setup()], config).trades[0]
    too_far = benchmark(
        [
            MarketEvent.book_snapshot(0, "BINANCE", "BTCUSDT-PERP", 1),
            quote(2 * SECOND, "102", "102.1"),
        ],
        [setup()],
        ReplayConfig(max_entry_deviation_bps=Decimal(5)),
    ).trades[0]

    assert (missed.status, missed.exit_reason) == ("MISSED", "NO_VALID_QUOTE")
    assert (too_far.status, too_far.exit_reason) == ("MISSED", "PRICE_ENVELOPE")


def test_pre_latency_quote_cannot_change_entry() -> None:
    config = ReplayConfig(latency_ns=SECOND)
    original = events()
    changed = list(original)
    changed[1] = quote(SECOND, "50", "50.1")

    first = benchmark(original, [setup()], config).trades[0]
    second = benchmark(changed, [setup()], config).trades[0]

    assert first.entry_price == second.entry_price
    assert first.net_pnl == second.net_pnl


def test_candidate_event_index_blocks_same_event_fill_with_cross_stream_time() -> None:
    stream = [
        MarketEvent.book_snapshot(0, "BINANCE", "BTCUSDT-PERP", 1),
        quote(2 * SECOND, "100", "100.1"),
        MarketEvent.funding(
            SECOND,
            "BINANCE",
            "BTCUSDT-PERP",
            Decimal("0.0001"),
        ),
        quote(3 * SECOND, "101", "101.1"),
    ]

    trade = benchmark(
        stream,
        [setup(observed_at_ns=2 * SECOND, event_index=1)],
        ReplayConfig(
            slippage_bps=Decimal(0),
            fee_bps=Decimal(0),
            max_entry_deviation_bps=Decimal(200),
        ),
    ).trades[0]

    assert trade.entry_ns == 3 * SECOND
    assert trade.entry_price == Decimal("101.1")


def test_end_of_data_marks_to_last_executable_quote() -> None:
    stream = [
        MarketEvent.book_snapshot(0, "BINANCE", "BTCUSDT-PERP", 1),
        quote(2 * SECOND, "100", "100.1"),
        quote(3 * SECOND, "101", "101.1"),
    ]

    trade = benchmark(
        stream,
        [setup(target_reference=Decimal(110), invalidation=Decimal(90))],
        ReplayConfig(slippage_bps=Decimal(0), fee_bps=Decimal(0)),
    ).trades[0]

    assert trade.exit_reason == "END_OF_DATA"
    assert trade.exit_price == Decimal(101)
    assert trade.gross_pnl == Decimal("0.9")


def test_end_of_data_exit_never_precedes_entry_across_streams() -> None:
    stream = [
        MarketEvent.book_snapshot(0, "BINANCE", "BTCUSDT-PERP", 1),
        quote(2 * SECOND, "100", "100.1"),
        MarketEvent.funding(SECOND, "BINANCE", "BTCUSDT-PERP", Decimal("0.01")),
    ]
    trade = benchmark(
        stream,
        [setup(target_reference=Decimal(110), invalidation=Decimal(90))],
        ReplayConfig(slippage_bps=Decimal(0), fee_bps=Decimal(0)),
    ).trades[0]
    assert trade.exit_ns == trade.entry_ns == 2 * SECOND


def test_benchmark_rejects_candidate_while_position_is_open() -> None:
    candidates = [
        setup(target_reference=Decimal(110), invalidation=Decimal(90)),
        setup(
            feature_id="g" * 64,
            observed_at_ns=3 * SECOND,
            target_reference=Decimal(110),
            invalidation=Decimal(90),
        ),
    ]

    result = benchmark(events(), candidates, ReplayConfig())

    assert result.trades[0].status == "FILLED"
    assert (result.trades[1].status, result.trades[1].exit_reason) == (
        "MISSED",
        "POSITION_OPEN",
    )


def test_purged_walk_forward_is_chronological_and_validated() -> None:
    folds = purged_walk_forward(total=12, train_size=4, test_size=2, purge=1)

    assert folds == (
        (range(4), range(5, 7)),
        (range(2, 6), range(7, 9)),
        (range(4, 8), range(9, 11)),
    )
    assert all(train.stop + 1 == test.start for train, test in folds)

    with pytest.raises(ValueError, match="positive"):
        purged_walk_forward(total=12, train_size=0, test_size=2, purge=1)
    with pytest.raises(ValueError, match="purge"):
        purged_walk_forward(total=12, train_size=4, test_size=2, purge=-1)
