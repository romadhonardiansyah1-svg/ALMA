import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import TypeVar

import pyarrow as pa
import pyarrow.parquet as pq

from alma.book_sequence import BookSequenceResult
from alma.market_state import MarketState

_SAFE_PART = re.compile(r"^[A-Za-z0-9._-]+$")
T = TypeVar("T")
_SCHEMA = pa.schema(
    [
        ("session_ns", pa.int64()),
        ("sequence", pa.int64()),
        ("kind", pa.string()),
        ("ts_event_ns", pa.int64()),
        ("bid", pa.string()),
        ("ask", pa.string()),
        ("bid_size", pa.string()),
        ("ask_size", pa.string()),
        ("price", pa.string()),
        ("size", pa.string()),
        ("rate", pa.string()),
        ("aggressor", pa.int8()),
        ("first_update_id", pa.int64()),
        ("final_update_id", pa.int64()),
        ("previous_final_update_id", pa.int64()),
    ]
)


def _decimal(value: Decimal, name: str, *, positive: bool = False) -> Decimal:
    if not value.is_finite() or (positive and value <= 0):
        raise ValueError(
            f"{name} must be finite" + (" and positive" if positive else "")
        )
    return value


def _component(value: str) -> str:
    if not value or value in {".", ".."} or not _SAFE_PART.fullmatch(value):
        raise ValueError("invalid partition path component")
    return value


@dataclass(frozen=True)
class MarketEvent:
    kind: str
    ts_event_ns: int
    venue: str
    symbol: str
    bid: Decimal | None = None
    ask: Decimal | None = None
    bid_size: Decimal | None = None
    ask_size: Decimal | None = None
    price: Decimal | None = None
    size: Decimal | None = None
    rate: Decimal | None = None
    aggressor: int = 0
    first_update_id: int | None = None
    final_update_id: int | None = None
    previous_final_update_id: int | None = None

    def __post_init__(self) -> None:
        if self.ts_event_ns < 0:
            raise ValueError("timestamp must be non-negative")
        _component(self.venue)
        _component(self.symbol)

    @classmethod
    def quote(
        cls,
        ts_event_ns: int,
        venue: str,
        symbol: str,
        bid: Decimal,
        ask: Decimal,
        bid_size: Decimal,
        ask_size: Decimal,
    ) -> "MarketEvent":
        return cls(
            "quote",
            ts_event_ns,
            venue,
            symbol,
            bid=_decimal(bid, "bid", positive=True),
            ask=_decimal(ask, "ask", positive=True),
            bid_size=_decimal(bid_size, "bid size", positive=True),
            ask_size=_decimal(ask_size, "ask size", positive=True),
        )

    @classmethod
    def trade(
        cls,
        ts_event_ns: int,
        venue: str,
        symbol: str,
        price: Decimal,
        size: Decimal,
        aggressor: int,
    ) -> "MarketEvent":
        if aggressor not in (-1, 0, 1):
            raise ValueError("aggressor must be -1, 0, or 1")
        return cls(
            "trade",
            ts_event_ns,
            venue,
            symbol,
            price=_decimal(price, "price", positive=True),
            size=_decimal(size, "size", positive=True),
            aggressor=aggressor,
        )

    @classmethod
    def mark(
        cls, ts_event_ns: int, venue: str, symbol: str, price: Decimal
    ) -> "MarketEvent":
        return cls(
            "mark",
            ts_event_ns,
            venue,
            symbol,
            price=_decimal(price, "mark price", positive=True),
        )

    @classmethod
    def funding(
        cls, ts_event_ns: int, venue: str, symbol: str, rate: Decimal
    ) -> "MarketEvent":
        return cls(
            "funding",
            ts_event_ns,
            venue,
            symbol,
            rate=_decimal(rate, "funding rate"),
        )

    @classmethod
    def funding_settlement(
        cls,
        ts_event_ns: int,
        venue: str,
        symbol: str,
        rate: Decimal,
        mark_price: Decimal,
    ) -> "MarketEvent":
        return cls(
            "funding_settlement",
            ts_event_ns,
            venue,
            symbol,
            rate=_decimal(rate, "funding rate"),
            price=_decimal(mark_price, "mark price", positive=True),
        )

    @classmethod
    def book_snapshot(
        cls, ts_event_ns: int, venue: str, symbol: str, last_update_id: int
    ) -> "MarketEvent":
        if last_update_id < 0:
            raise ValueError("update ID must be non-negative")
        return cls(
            "book_snapshot",
            ts_event_ns,
            venue,
            symbol,
            final_update_id=last_update_id,
        )

    @classmethod
    def book_delta(
        cls,
        ts_event_ns: int,
        venue: str,
        symbol: str,
        *,
        first: int,
        final: int,
        previous_final: int,
    ) -> "MarketEvent":
        if min(first, final, previous_final) < 0:
            raise ValueError("update IDs must be non-negative")
        return cls(
            "book_delta",
            ts_event_ns,
            venue,
            symbol,
            first_update_id=first,
            final_update_id=final,
            previous_final_update_id=previous_final,
        )

    @classmethod
    def reconnect(cls, ts_event_ns: int, venue: str, symbol: str) -> "MarketEvent":
        return cls("reconnect", ts_event_ns, venue, symbol)

    @classmethod
    def book_invalidate(
        cls, ts_event_ns: int, venue: str, symbol: str
    ) -> "MarketEvent":
        return cls("book_invalidate", ts_event_ns, venue, symbol)

    def apply(self, state: MarketState) -> BookSequenceResult | None:
        if (self.venue, self.symbol) != (state.venue, state.symbol):
            raise ValueError("event instrument does not match state")
        if self.kind == "quote":
            state.on_quote(
                self.ts_event_ns,
                self._required(self.bid, "bid"),
                self._required(self.ask, "ask"),
                self._required(self.bid_size, "bid size"),
                self._required(self.ask_size, "ask size"),
            )
        elif self.kind == "trade":
            state.on_trade(
                self.ts_event_ns,
                self._required(self.price, "price"),
                self._required(self.size, "size"),
                aggressor=self.aggressor,
            )
        elif self.kind == "mark":
            state.on_mark(self.ts_event_ns, self._required(self.price, "price"))
        elif self.kind in {"funding", "funding_settlement"}:
            state.on_funding(self.ts_event_ns, self._required(self.rate, "rate"))
        elif self.kind == "book_snapshot":
            state.on_book_snapshot(
                self.ts_event_ns,
                self._required(self.final_update_id, "final update ID"),
            )
        elif self.kind == "book_delta":
            return state.on_book_delta(
                self.ts_event_ns,
                first=self._required(self.first_update_id, "first update ID"),
                final=self._required(self.final_update_id, "final update ID"),
                previous_final=self._required(
                    self.previous_final_update_id, "previous final update ID"
                ),
            )
        elif self.kind == "reconnect":
            state.on_reconnect(self.ts_event_ns)
        elif self.kind == "book_invalidate":
            state.invalidate_book(self.ts_event_ns)
        else:
            raise ValueError(f"unknown market event kind: {self.kind}")
        return None

    @staticmethod
    def _required(value: T | None, name: str) -> T:
        if value is None:
            raise ValueError(f"missing {name}")
        return value


class ParquetRecorder:
    def __init__(
        self,
        root: str | Path,
        venue: str,
        symbol: str,
        *,
        session_ns: int | None = None,
        max_rows: int = 10_000,
    ) -> None:
        self.root = Path(root)
        self.venue = _component(venue)
        self.symbol = _component(symbol)
        self.session_ns = time.time_ns() if session_ns is None else session_ns
        if self.session_ns < 0:
            raise ValueError("session_ns must be non-negative")
        if max_rows <= 0:
            raise ValueError("max_rows must be positive")
        self.max_rows = max_rows
        self._rows: list[dict[str, object]] = []
        self._sequence = 0

    def record(self, event: MarketEvent) -> None:
        if (event.venue, event.symbol) != (self.venue, self.symbol):
            raise ValueError("event instrument does not match recorder")
        self._rows.append(
            {
                "session_ns": self.session_ns,
                "sequence": self._sequence,
                "kind": event.kind,
                "ts_event_ns": event.ts_event_ns,
                "bid": _text(event.bid),
                "ask": _text(event.ask),
                "bid_size": _text(event.bid_size),
                "ask_size": _text(event.ask_size),
                "price": _text(event.price),
                "size": _text(event.size),
                "rate": _text(event.rate),
                "aggressor": event.aggressor,
                "first_update_id": event.first_update_id,
                "final_update_id": event.final_update_id,
                "previous_final_update_id": event.previous_final_update_id,
            }
        )
        self._sequence += 1
        if len(self._rows) >= self.max_rows:
            self.flush()

    def flush(self) -> list[Path]:
        if not self._rows:
            return []
        by_date: dict[str, list[dict[str, object]]] = {}
        for row in self._rows:
            date = (
                datetime.fromtimestamp(int(row["ts_event_ns"]) / 1e9, UTC)
                .date()
                .isoformat()
            )
            by_date.setdefault(date, []).append(row)
        paths: list[Path] = []
        for date, rows in sorted(by_date.items()):
            directory = (
                self.root
                / f"date={date}"
                / f"venue={self.venue}"
                / f"symbol={self.symbol}"
            )
            directory.mkdir(parents=True, exist_ok=True)
            path = directory / f"events-{self.session_ns}-{rows[0]['sequence']}.parquet"
            pq.write_table(pa.Table.from_pylist(rows, schema=_SCHEMA), path)
            paths.append(path)
        self._rows.clear()
        return paths


def read_events(root: str | Path, venue: str, symbol: str) -> list[MarketEvent]:
    venue = _component(venue)
    symbol = _component(symbol)
    paths = sorted(Path(root).glob(f"date=*/venue={venue}/symbol={symbol}/*.parquet"))
    if not paths:
        raise FileNotFoundError("no market recordings")
    rows: list[dict[str, object]] = []
    for path in paths:
        rows.extend(pq.ParquetFile(path).read().to_pylist())
    rows.sort(key=lambda row: (int(row["session_ns"]), int(row["sequence"])))
    keys = [(int(row["session_ns"]), int(row["sequence"])) for row in rows]
    if len(keys) != len(set(keys)):
        raise ValueError("duplicate replay order")
    return [_event_from_row(row, venue, symbol) for row in rows]


def replay(root: str | Path, venue: str, symbol: str) -> tuple[MarketState, int]:
    events = read_events(root, venue, symbol)
    state = MarketState(venue, symbol)
    last_ts = 0
    for event in events:
        event.apply(state)
        last_ts = max(last_ts, event.ts_event_ns)
    return state, last_ts


def _event_from_row(row: dict[str, object], venue: str, symbol: str) -> MarketEvent:
    return MarketEvent(
        kind=str(row["kind"]),
        ts_event_ns=int(row["ts_event_ns"]),
        venue=venue,
        symbol=symbol,
        bid=_from_text(row["bid"]),
        ask=_from_text(row["ask"]),
        bid_size=_from_text(row["bid_size"]),
        ask_size=_from_text(row["ask_size"]),
        price=_from_text(row["price"]),
        size=_from_text(row["size"]),
        rate=_from_text(row["rate"]),
        aggressor=int(row["aggressor"]),
        first_update_id=_optional_int(row["first_update_id"]),
        final_update_id=_optional_int(row["final_update_id"]),
        previous_final_update_id=_optional_int(row["previous_final_update_id"]),
    )


def _text(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _from_text(value: object) -> Decimal | None:
    return None if value is None else Decimal(str(value))


def _optional_int(value: object) -> int | None:
    return None if value is None else int(value)
