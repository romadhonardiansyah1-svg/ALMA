from decimal import Decimal

import pytest

from alma.bars import MultiTimeframeBars

SECOND = 1_000_000_000
MINUTE = 60 * SECOND


def test_trade_events_build_exact_m1_ohlcv() -> None:
    bars = MultiTimeframeBars()

    assert bars.on_trade(0, Decimal("100.1"), Decimal("0.2")) == []
    assert bars.on_trade(10 * SECOND, Decimal("101.2"), Decimal("0.3")) == []
    assert bars.on_trade(59 * SECOND, Decimal("99.9"), Decimal("0.5")) == []
    closed = bars.on_trade(MINUTE, Decimal("102.0"), Decimal(1))

    assert len(closed) == 1
    bar = closed[0]
    assert (bar.minutes, bar.start_ns, bar.end_ns) == (1, 0, MINUTE)
    assert (bar.open, bar.high, bar.low, bar.close, bar.volume) == (
        Decimal("100.1"),
        Decimal("101.2"),
        Decimal("99.9"),
        Decimal("99.9"),
        Decimal("1.0"),
    )


def test_closed_m1_bars_derive_m5_without_synthetic_gap_bars() -> None:
    bars = MultiTimeframeBars()
    closed = []

    for minute, price in [(0, "100"), (1, "101"), (4, "99")]:
        closed += bars.on_trade(minute * MINUTE, Decimal(price), Decimal(1))
        closed += bars.advance((minute + 1) * MINUTE)

    closed += bars.on_trade(7 * MINUTE, Decimal(105), Decimal(2))
    closed += bars.advance(10 * MINUTE)

    m5 = [bar for bar in closed if bar.minutes == 5]
    assert len(m5) == 2
    assert (m5[0].start_ns, m5[0].end_ns) == (0, 5 * MINUTE)
    assert (m5[0].open, m5[0].high, m5[0].low, m5[0].close, m5[0].volume) == (
        Decimal(100),
        Decimal(101),
        Decimal(99),
        Decimal(99),
        Decimal(3),
    )
    assert (m5[1].start_ns, m5[1].end_ns, m5[1].volume) == (
        5 * MINUTE,
        10 * MINUTE,
        Decimal(2),
    )
    assert not [
        bar
        for bar in closed
        if bar.minutes == 1
        and bar.start_ns
        in {2 * MINUTE, 3 * MINUTE, 5 * MINUTE, 6 * MINUTE, 8 * MINUTE, 9 * MINUTE}
    ]


def test_m1_derives_m15_and_h1_on_utc_boundaries() -> None:
    bars = MultiTimeframeBars()
    closed = []

    for minute in range(61):
        closed += bars.on_trade(
            minute * MINUTE,
            Decimal(minute + 1),
            Decimal(1),
        )
    closed += bars.advance(61 * MINUTE)

    m15 = [bar for bar in closed if bar.minutes == 15]
    h1 = [bar for bar in closed if bar.minutes == 60]
    assert [(bar.start_ns, bar.end_ns) for bar in m15] == [
        (0, 15 * MINUTE),
        (15 * MINUTE, 30 * MINUTE),
        (30 * MINUTE, 45 * MINUTE),
        (45 * MINUTE, 60 * MINUTE),
    ]
    assert len(h1) == 1
    assert (h1[0].start_ns, h1[0].end_ns) == (0, 60 * MINUTE)
    assert (h1[0].open, h1[0].close, h1[0].volume) == (
        Decimal(1),
        Decimal(60),
        Decimal(60),
    )


def test_out_of_order_and_invalid_trade_values_fail_closed() -> None:
    bars = MultiTimeframeBars()
    bars.on_trade(SECOND, Decimal(100), Decimal(1))

    with pytest.raises(ValueError, match="out of order"):
        bars.on_trade(0, Decimal(101), Decimal(1))
    for price, size in [
        (Decimal("NaN"), Decimal(1)),
        (Decimal(100), Decimal(0)),
        (Decimal(100), Decimal("Infinity")),
    ]:
        with pytest.raises(ValueError):
            bars.on_trade(2 * SECOND, price, size)


def test_advance_never_moves_backward_or_emits_empty_bars() -> None:
    bars = MultiTimeframeBars()

    assert bars.advance(10 * MINUTE) == []
    bars.on_trade(10 * MINUTE, Decimal(100), Decimal(1))
    bars.advance(11 * MINUTE)
    with pytest.raises(ValueError, match="out of order"):
        bars.advance(10 * MINUTE)
