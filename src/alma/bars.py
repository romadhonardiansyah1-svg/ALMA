from dataclasses import dataclass
from decimal import Decimal

MINUTE_NS = 60_000_000_000


@dataclass(frozen=True)
class Bar:
    minutes: int
    start_ns: int
    end_ns: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal


@dataclass
class _Builder:
    minutes: int
    start_ns: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal

    @property
    def end_ns(self) -> int:
        return self.start_ns + self.minutes * MINUTE_NS

    def update(
        self, high: Decimal, low: Decimal, close: Decimal, volume: Decimal
    ) -> None:
        self.high = max(self.high, high)
        self.low = min(self.low, low)
        self.close = close
        self.volume += volume

    def finish(self) -> Bar:
        return Bar(
            minutes=self.minutes,
            start_ns=self.start_ns,
            end_ns=self.end_ns,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
        )


class MultiTimeframeBars:
    def __init__(self) -> None:
        self._watermark_ns: int | None = None
        self._m1: _Builder | None = None
        self._derived: dict[int, _Builder | None] = {5: None, 15: None, 60: None}

    def on_trade(self, ts_event_ns: int, price: Decimal, size: Decimal) -> list[Bar]:
        self._validate_time(ts_event_ns)
        if not price.is_finite() or price <= 0:
            raise ValueError("price must be finite and positive")
        if not size.is_finite() or size <= 0:
            raise ValueError("size must be finite and positive")

        closed = self._close_at(ts_event_ns)
        start_ns = ts_event_ns // MINUTE_NS * MINUTE_NS
        if self._m1 is None:
            self._m1 = _Builder(1, start_ns, price, price, price, price, size)
        else:
            self._m1.update(price, price, price, size)
        self._watermark_ns = ts_event_ns
        return closed

    def advance(self, ts_event_ns: int) -> list[Bar]:
        self._validate_time(ts_event_ns)
        closed = self._close_at(ts_event_ns)
        self._watermark_ns = ts_event_ns
        return closed

    def _validate_time(self, ts_event_ns: int) -> None:
        if ts_event_ns < 0:
            raise ValueError("timestamp must be non-negative")
        if self._watermark_ns is not None and ts_event_ns < self._watermark_ns:
            raise ValueError("timestamp is out of order")

    def _close_at(self, watermark_ns: int) -> list[Bar]:
        closed: list[Bar] = []
        if self._m1 is not None and watermark_ns >= self._m1.end_ns:
            m1 = self._m1.finish()
            self._m1 = None
            closed.append(m1)
            self._add_to_derived(m1)

        for minutes in self._derived:
            builder = self._derived[minutes]
            if builder is not None and watermark_ns >= builder.end_ns:
                closed.append(builder.finish())
                self._derived[minutes] = None
        return closed

    def _add_to_derived(self, bar: Bar) -> None:
        for minutes, builder in self._derived.items():
            start_ns = bar.start_ns // (minutes * MINUTE_NS) * minutes * MINUTE_NS
            if builder is None:
                self._derived[minutes] = _Builder(
                    minutes,
                    start_ns,
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    bar.volume,
                )
            elif builder.start_ns == start_ns:
                builder.update(bar.high, bar.low, bar.close, bar.volume)
            else:
                raise RuntimeError("derived bar crossed an unclosed bucket")
