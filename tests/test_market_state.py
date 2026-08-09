from decimal import Decimal

import pytest

from alma.market_state import MarketState

SECOND = 1_000_000_000
MINUTE = 60 * SECOND


def test_trade_quote_events_build_compact_immutable_snapshot() -> None:
    state = MarketState("BINANCE", "BTCUSDT-PERP")

    state.on_quote(
        0,
        bid=Decimal(100),
        ask=Decimal(101),
        bid_size=Decimal(3),
        ask_size=Decimal(1),
    )
    state.on_trade(SECOND, Decimal("100.5"), Decimal(2), aggressor=1)
    state.on_trade(2 * SECOND, Decimal(101), Decimal(1), aggressor=-1)
    snapshot = state.snapshot(2 * SECOND)

    assert (snapshot.bid, snapshot.ask, snapshot.spread) == (
        Decimal(100),
        Decimal(101),
        Decimal(1),
    )
    assert (snapshot.bid_size, snapshot.ask_size) == (Decimal(3), Decimal(1))
    assert snapshot.top_book_imbalance == Decimal("0.5")
    assert snapshot.market_age_ms == 2_000
    assert snapshot.tick_velocity_1s == 2
    assert snapshot.flow_imbalance == Decimal(1) / Decimal(3)
    assert snapshot.realized_volatility > 0
    assert snapshot.session == "ASIA"
    assert snapshot.book_valid is False
    assert len(snapshot.state_id) == 64


def test_closed_bars_are_retained_by_timeframe() -> None:
    state = MarketState("BINANCE", "BTCUSDT-PERP")

    for minute in range(6):
        state.on_trade(minute * MINUTE, Decimal(100 + minute), Decimal(1))
    state.advance(6 * MINUTE)
    snapshot = state.snapshot(6 * MINUTE)

    assert snapshot.m1 is not None and snapshot.m1.start_ns == 5 * MINUTE
    assert snapshot.m5 is not None and snapshot.m5.start_ns == 0
    assert snapshot.m15 is None
    assert snapshot.h1 is None


def test_state_id_is_stable_for_same_event_stream() -> None:
    def build() -> MarketState:
        state = MarketState("BINANCE", "BTCUSDT-PERP")
        state.on_quote(0, Decimal(100), Decimal(101), Decimal(2), Decimal(1))
        state.on_trade(SECOND, Decimal("100.5"), Decimal("0.2"), aggressor=1)
        state.on_mark(SECOND, Decimal("100.4"))
        state.on_funding(SECOND, Decimal("0.0001"))
        return state

    assert (
        build().snapshot(2 * SECOND).state_id == build().snapshot(2 * SECOND).state_id
    )


def test_raw_book_gap_invalidates_until_new_snapshot() -> None:
    state = MarketState("BINANCE", "BTCUSDT-PERP")

    state.on_book_snapshot(0, 100)
    assert (
        state.on_book_delta(0, first=99, final=105, previous_final=98).value
        == "APPLIED"
    )
    assert state.snapshot(0).book_valid is True
    assert (
        state.on_book_delta(0, first=106, final=110, previous_final=104).value == "GAP"
    )
    assert state.snapshot(0).book_valid is False
    assert state.metrics.gap_count == 1
    state.on_book_snapshot(0, 200)
    assert state.snapshot(0).book_valid is True


def test_reconnect_invalidates_depth_and_counts_reconnect() -> None:
    state = MarketState("BINANCE", "BTCUSDT-PERP")
    state.on_book_snapshot(0, 100)

    state.on_reconnect(0)

    assert state.snapshot(0).book_valid is False
    assert state.metrics.reconnect_count == 1


def test_invalid_or_out_of_order_market_events_fail_closed() -> None:
    state = MarketState("BINANCE", "BTCUSDT-PERP", future_tolerance_ns=0)
    state.on_quote(SECOND, Decimal(100), Decimal(101), Decimal(1), Decimal(1))

    with pytest.raises(ValueError, match="out of order"):
        state.on_quote(0, Decimal(100), Decimal(101), Decimal(1), Decimal(1))
    state.on_trade(0, Decimal(100), Decimal(1))
    with pytest.raises(ValueError, match="ask"):
        state.on_quote(2 * SECOND, Decimal(102), Decimal(101), Decimal(1), Decimal(1))
    with pytest.raises(ValueError, match="now"):
        state.snapshot(0)


def test_small_future_exchange_timestamp_is_clamped_to_zero_age() -> None:
    state = MarketState("BINANCE", "BTCUSDT-PERP", future_tolerance_ns=20)
    state.on_trade(10, Decimal(100), Decimal(1))

    assert state.snapshot(0).market_age_ms == 0


def test_fresh_mark_does_not_mask_stale_quote_or_book() -> None:
    state = MarketState("BINANCE", "BTCUSDT-PERP")
    state.on_quote(0, Decimal(100), Decimal(101), Decimal(1), Decimal(1))
    state.on_book_snapshot(0, 1)
    state.on_mark(10 * SECOND, Decimal("100.5"))

    snapshot = state.snapshot(10 * SECOND)

    assert snapshot.market_age_ms == 10_000
    assert snapshot.observed_at_ns == 10 * SECOND


def test_metrics_report_nearest_rank_p95_latency() -> None:
    state = MarketState("BINANCE", "BTCUSDT-PERP")
    for latency_ns in range(1, 101):
        state.metrics.observe_latency(latency_ns * 1_000_000)

    assert state.metrics.p95_latency_ms == 95
