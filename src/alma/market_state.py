import hashlib
import json
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from decimal import Decimal

from alma.bars import Bar, MultiTimeframeBars
from alma.book_sequence import BookSequenceResult, BookSequenceState

SECOND_NS = 1_000_000_000


@dataclass
class MarketMetrics:
    event_count: int = 0
    gap_count: int = 0
    reconnect_count: int = 0
    processing_latency_ns: deque[int] = field(
        default_factory=lambda: deque(maxlen=4096),
        repr=False,
    )

    def observe_latency(self, latency_ns: int) -> None:
        if latency_ns < 0:
            raise ValueError("latency must be non-negative")
        self.processing_latency_ns.append(latency_ns)

    @property
    def p95_latency_ms(self) -> float:
        if not self.processing_latency_ns:
            return 0.0
        ordered = sorted(self.processing_latency_ns)
        index = max(0, (95 * len(ordered) + 99) // 100 - 1)
        return ordered[index] / 1_000_000


@dataclass(frozen=True)
class MarketSnapshot:
    state_id: str
    observed_at_ns: int
    market_age_ms: int
    venue: str
    symbol: str
    bid: Decimal | None
    ask: Decimal | None
    bid_size: Decimal | None
    ask_size: Decimal | None
    spread: Decimal | None
    top_book_imbalance: Decimal
    mark_price: Decimal | None
    funding_rate: Decimal | None
    tick_velocity_1s: int
    realized_volatility: Decimal
    flow_imbalance: Decimal
    session: str
    book_valid: bool
    m1: Bar | None
    m5: Bar | None
    m15: Bar | None
    h1: Bar | None


class MarketState:
    def __init__(
        self,
        venue: str,
        symbol: str,
        *,
        future_tolerance_ns: int = 250_000_000,
    ) -> None:
        if future_tolerance_ns < 0:
            raise ValueError("future_tolerance_ns must be non-negative")
        self.venue = venue
        self.symbol = symbol
        self.future_tolerance_ns = future_tolerance_ns
        self.metrics = MarketMetrics()
        self._bars = MultiTimeframeBars()
        self._book = BookSequenceState()
        self._last_event_ns: int | None = None
        self._last_by_stream: dict[str, int] = {}
        self._bid: Decimal | None = None
        self._ask: Decimal | None = None
        self._bid_size: Decimal | None = None
        self._ask_size: Decimal | None = None
        self._mark_price: Decimal | None = None
        self._funding_rate: Decimal | None = None
        self._trade_times: deque[int] = deque()
        self._trade_prices: deque[Decimal] = deque(maxlen=64)
        self._trade_flow: deque[tuple[int, int, Decimal]] = deque(maxlen=64)
        self._latest_bars: dict[int, Bar] = {}

    def _time(self, ts_event_ns: int, stream: str) -> None:
        if ts_event_ns < 0:
            raise ValueError("timestamp must be non-negative")
        if ts_event_ns < self._last_by_stream.get(stream, 0):
            raise ValueError("timestamp is out of order")
        self._last_by_stream[stream] = ts_event_ns
        self._last_event_ns = max(ts_event_ns, self._last_event_ns or 0)
        self.metrics.event_count += 1

    @staticmethod
    def _positive(value: Decimal, name: str) -> None:
        if not value.is_finite() or value <= 0:
            raise ValueError(f"{name} must be finite and positive")

    def _retain(self, bars: list[Bar]) -> None:
        for bar in bars:
            self._latest_bars[bar.minutes] = bar

    def on_trade(
        self,
        ts_event_ns: int,
        price: Decimal,
        size: Decimal,
        *,
        aggressor: int = 0,
    ) -> None:
        self._positive(price, "price")
        self._positive(size, "size")
        if aggressor not in (-1, 0, 1):
            raise ValueError("aggressor must be -1, 0, or 1")
        self._time(ts_event_ns, "trade")
        self._retain(self._bars.on_trade(ts_event_ns, price, size))
        self._trade_times.append(ts_event_ns)
        cutoff = ts_event_ns - SECOND_NS
        while self._trade_times and self._trade_times[0] < cutoff:
            self._trade_times.popleft()
        self._trade_prices.append(price)
        self._trade_flow.append((ts_event_ns, aggressor, size))

    def on_quote(
        self,
        ts_event_ns: int,
        bid: Decimal,
        ask: Decimal,
        bid_size: Decimal,
        ask_size: Decimal,
    ) -> None:
        self._positive(bid, "bid")
        self._positive(ask, "ask")
        self._positive(bid_size, "bid size")
        self._positive(ask_size, "ask size")
        if ask < bid:
            raise ValueError("ask must not be below bid")
        self._time(ts_event_ns, "quote")
        self._bid = bid
        self._ask = ask
        self._bid_size = bid_size
        self._ask_size = ask_size

    def on_mark(self, ts_event_ns: int, price: Decimal) -> None:
        self._positive(price, "mark price")
        self._time(ts_event_ns, "mark")
        self._mark_price = price

    def on_funding(self, ts_event_ns: int, rate: Decimal) -> None:
        if not rate.is_finite():
            raise ValueError("funding rate must be finite")
        self._time(ts_event_ns, "funding")
        self._funding_rate = rate

    def advance(self, ts_event_ns: int) -> None:
        self._time(ts_event_ns, "clock")
        self._retain(self._bars.advance(ts_event_ns))

    def on_book_snapshot(self, ts_event_ns: int, last_update_id: int) -> None:
        if (
            self._book.last_update_id is not None
            and last_update_id < self._book.last_update_id
        ):
            return
        self._time(ts_event_ns, "book")
        self._book.on_snapshot(last_update_id)

    def on_book_delta(
        self,
        ts_event_ns: int,
        *,
        first: int,
        final: int,
        previous_final: int,
    ) -> BookSequenceResult:
        self._time(ts_event_ns, "book")
        result = self._book.on_delta(
            first=first,
            final=final,
            previous_final=previous_final,
        )
        if result is BookSequenceResult.GAP:
            self.metrics.gap_count += 1
        return result

    def on_reconnect(self, ts_event_ns: int) -> None:
        self._time(ts_event_ns, "connection")
        self.metrics.reconnect_count += 1
        self._book.valid = False

    def invalidate_book(self, ts_event_ns: int) -> None:
        self._time(ts_event_ns, "book")
        self._book.valid = False

    def _volatility(self) -> Decimal:
        if len(self._trade_prices) < 2:
            return Decimal(0)
        returns = [
            self._trade_prices[index] / self._trade_prices[index - 1] - 1
            for index in range(1, len(self._trade_prices))
        ]
        return (
            sum((value * value for value in returns), Decimal(0)) / len(returns)
        ).sqrt()

    def _snapshot_values(self, now_ns: int) -> dict[str, object]:
        if self._last_event_ns is None:
            raise ValueError("market state has no events")
        if now_ns + self.future_tolerance_ns < self._last_event_ns:
            raise ValueError("now precedes latest market event")
        execution_times = [
            self._last_by_stream[stream]
            for stream in ("quote", "book")
            if stream in self._last_by_stream
        ]
        execution_at_ns = (
            min(execution_times) if execution_times else self._last_event_ns
        )
        observed_at_ns = min(self._last_event_ns, now_ns)
        cutoff = now_ns - SECOND_NS
        buy_volume = sum(
            (
                size
                for ts_event_ns, aggressor, size in self._trade_flow
                if ts_event_ns >= cutoff and aggressor > 0
            ),
            Decimal(0),
        )
        sell_volume = sum(
            (
                size
                for ts_event_ns, aggressor, size in self._trade_flow
                if ts_event_ns >= cutoff and aggressor < 0
            ),
            Decimal(0),
        )
        total_flow = buy_volume + sell_volume
        imbalance = (
            (buy_volume - sell_volume) / total_flow if total_flow else Decimal(0)
        )
        spread = (
            self._ask - self._bid
            if self._ask is not None and self._bid is not None
            else None
        )
        top_size = (self._bid_size or Decimal(0)) + (self._ask_size or Decimal(0))
        top_book_imbalance = (
            ((self._bid_size or Decimal(0)) - (self._ask_size or Decimal(0))) / top_size
            if top_size
            else Decimal(0)
        )
        hour = datetime.fromtimestamp(self._last_event_ns / 1_000_000_000, UTC).hour
        session = (
            "ASIA"
            if hour < 7
            else "LONDON"
            if hour < 13
            else "NEW_YORK"
            if hour < 21
            else "OFF_HOURS"
        )
        return {
            "observed_at_ns": observed_at_ns,
            "market_age_ms": (now_ns - min(execution_at_ns, now_ns)) // 1_000_000,
            "venue": self.venue,
            "symbol": self.symbol,
            "bid": self._bid,
            "ask": self._ask,
            "bid_size": self._bid_size,
            "ask_size": self._ask_size,
            "spread": spread,
            "top_book_imbalance": top_book_imbalance,
            "mark_price": self._mark_price,
            "funding_rate": self._funding_rate,
            "tick_velocity_1s": sum(
                ts_event_ns >= cutoff for ts_event_ns in self._trade_times
            ),
            "realized_volatility": self._volatility(),
            "flow_imbalance": imbalance,
            "session": session,
            "book_valid": self._book.valid,
            "m1": self._latest_bars.get(1),
            "m5": self._latest_bars.get(5),
            "m15": self._latest_bars.get(15),
            "h1": self._latest_bars.get(60),
        }

    def snapshot(self, now_ns: int) -> MarketSnapshot:
        values = self._snapshot_values(now_ns)
        encoded = json.dumps(
            asdict(MarketSnapshot(state_id="", **values)),
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode()
        return MarketSnapshot(
            state_id=hashlib.sha256(encoded).hexdigest(),
            **values,
        )
