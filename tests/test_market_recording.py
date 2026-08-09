from decimal import Decimal

import pyarrow.parquet as pq
import pytest

from alma.market_recording import MarketEvent, ParquetRecorder, read_events, replay
from alma.market_state import MarketState

SECOND = 1_000_000_000


def events() -> list[MarketEvent]:
    return [
        MarketEvent.quote(
            0,
            "BINANCE",
            "BTCUSDT-PERP",
            Decimal(100),
            Decimal(101),
            Decimal(2),
            Decimal(1),
        ),
        MarketEvent.trade(
            SECOND,
            "BINANCE",
            "BTCUSDT-PERP",
            Decimal("100.5"),
            Decimal("0.2"),
            1,
        ),
        MarketEvent.mark(
            SECOND,
            "BINANCE",
            "BTCUSDT-PERP",
            Decimal("100.4"),
        ),
        MarketEvent.funding(
            SECOND,
            "BINANCE",
            "BTCUSDT-PERP",
            Decimal("0.0001"),
        ),
    ]


def test_partitioned_parquet_replay_reproduces_state_hash(tmp_path) -> None:
    live = MarketState("BINANCE", "BTCUSDT-PERP")
    recorder = ParquetRecorder(
        tmp_path,
        "BINANCE",
        "BTCUSDT-PERP",
        session_ns=7,
    )
    for event in events():
        recorder.record(event)
        event.apply(live)
    paths = recorder.flush()

    assert len(paths) == 1
    assert paths[0].relative_to(tmp_path).parts[:3] == (
        "date=1970-01-01",
        "venue=BINANCE",
        "symbol=BTCUSDT-PERP",
    )
    table = pq.read_table(paths[0])
    assert table.num_rows == 4
    assert read_events(tmp_path, "BINANCE", "BTCUSDT-PERP") == events()

    replayed, last_ts = replay(tmp_path, "BINANCE", "BTCUSDT-PERP")
    assert replayed.snapshot(last_ts).state_id == live.snapshot(last_ts).state_id


def test_replay_uses_recording_order_not_event_timestamp_order(tmp_path) -> None:
    recorder = ParquetRecorder(
        tmp_path,
        "BINANCE",
        "BTCUSDT-PERP",
        session_ns=9,
    )
    quote = MarketEvent.quote(
        2 * SECOND,
        "BINANCE",
        "BTCUSDT-PERP",
        Decimal(100),
        Decimal(101),
        Decimal(1),
        Decimal(1),
    )
    trade = MarketEvent.trade(
        SECOND,
        "BINANCE",
        "BTCUSDT-PERP",
        Decimal("100.5"),
        Decimal(1),
        0,
    )
    recorder.record(quote)
    recorder.record(trade)
    recorder.flush()

    state, last_ts = replay(tmp_path, "BINANCE", "BTCUSDT-PERP")
    assert state.snapshot(last_ts).observed_at_ns == 2 * SECOND


def test_recorder_rejects_cross_instrument_and_duplicate_replay_order(tmp_path) -> None:
    recorder = ParquetRecorder(
        tmp_path,
        "BINANCE",
        "BTCUSDT-PERP",
        session_ns=11,
    )
    wrong = MarketEvent.mark(0, "BINANCE", "ETHUSDT-PERP", Decimal(100))
    with pytest.raises(ValueError, match="instrument"):
        recorder.record(wrong)

    recorder.record(events()[0])
    path = recorder.flush()[0]
    table = pq.read_table(path)
    duplicate = table.take([0, 0])
    pq.write_table(duplicate, path)
    with pytest.raises(ValueError, match="duplicate"):
        replay(tmp_path, "BINANCE", "BTCUSDT-PERP")


def test_market_event_validation_fails_closed() -> None:
    with pytest.raises(ValueError, match="aggressor"):
        MarketEvent.trade(
            0,
            "BINANCE",
            "BTCUSDT-PERP",
            Decimal(100),
            Decimal(1),
            2,
        )
    with pytest.raises(ValueError, match="path"):
        ParquetRecorder(".", "../BINANCE", "BTCUSDT-PERP")


def test_recorder_auto_flushes_bounded_rows(tmp_path) -> None:
    recorder = ParquetRecorder(
        tmp_path,
        "BINANCE",
        "BTCUSDT-PERP",
        session_ns=12,
        max_rows=2,
    )

    recorder.record(events()[0])
    recorder.record(events()[1])

    assert len(list(tmp_path.rglob("*.parquet"))) == 1
    assert recorder.flush() == []


def test_book_invalidation_and_network_reconnect_are_distinct() -> None:
    state = MarketState("BINANCE", "BTCUSDT-PERP")
    MarketEvent.book_snapshot(0, state.venue, state.symbol, 100).apply(state)

    MarketEvent.book_invalidate(1, state.venue, state.symbol).apply(state)
    assert state.snapshot(1).book_valid is False
    assert state.metrics.reconnect_count == 0

    MarketEvent.reconnect(2, state.venue, state.symbol).apply(state)
    assert state.metrics.reconnect_count == 1
