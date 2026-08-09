import hashlib
import hmac
import ipaddress
import json
import os
import sqlite3
import stat
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from aiohttp import web

from alma.database import immediate_transaction
from alma.execution import (
    ExecutionRejected,
    ExecutionTruth,
    InstrumentRules,
    OrderRequest,
    ProtectedSubmission,
    ProtectionOrder,
    VenueOrder,
)
from alma.ledger import record_fill

BRIDGE_VERSION = "alma-mt5-v1"
ACTIVE_ORDER_STATUSES = {"ACCEPTED", "SUBMITTED", "PARTIALLY_FILLED", "TRIGGERED"}
TERMINAL_ORDER_STATUSES = {"CANCELED", "EXPIRED", "FILLED", "REJECTED"}

_SCHEMA = """
CREATE TABLE IF NOT EXISTS mt5_terminal_state (
    terminal_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    last_seq INTEGER NOT NULL CHECK (last_seq > 0),
    state_id TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS mt5_snapshots (
    terminal_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK (seq > 0),
    nonce TEXT NOT NULL UNIQUE,
    state_id TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY (terminal_id, session_id, seq)
);
CREATE TABLE IF NOT EXISTS mt5_terminal_invalidations (
    terminal_id TEXT PRIMARY KEY,
    reason TEXT NOT NULL,
    invalidated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS mt5_trade_events (
    event_id TEXT PRIMARY KEY,
    terminal_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    status TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS mt5_trade_events_request
    ON mt5_trade_events(request_id, observed_at, event_id);
CREATE TABLE IF NOT EXISTS mt5_deals (
    deal_id TEXT PRIMARY KEY,
    terminal_id TEXT NOT NULL,
    root_id TEXT NOT NULL,
    side TEXT NOT NULL,
    entry_kind TEXT NOT NULL,
    volume TEXT NOT NULL,
    price TEXT NOT NULL,
    fee TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS mt5_deals_root ON mt5_deals(root_id, observed_at, deal_id);
CREATE TABLE IF NOT EXISTS mt5_commands (
    request_id TEXT PRIMARY KEY,
    terminal_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'DELIVERED', 'ACKED', 'REJECTED')),
    ack_payload TEXT
);
CREATE INDEX IF NOT EXISTS mt5_commands_terminal
    ON mt5_commands(terminal_id, status, created_at, request_id);
"""


class MT5BridgeRejected(RuntimeError):
    pass


def ensure_mt5_schema(connection: sqlite3.Connection) -> None:
    with connection:
        connection.executescript(_SCHEMA)


def _keys(value: Mapping[str, Any], expected: set[str], name: str) -> None:
    if set(value) != expected:
        raise MT5BridgeRejected(f"INVALID_{name.upper()}_SCHEMA")


def _text(value: Any, name: str, *, maximum: int = 128) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise MT5BridgeRejected(f"INVALID_{name.upper()}")
    return value


def _decimal(value: Any, name: str, *, positive: bool = False) -> Decimal:
    if not isinstance(value, str):
        raise MT5BridgeRejected(f"INVALID_{name.upper()}")
    try:
        number = Decimal(value)
    except InvalidOperation as error:
        raise MT5BridgeRejected(f"INVALID_{name.upper()}") from error
    if not number.is_finite() or (positive and number <= 0):
        raise MT5BridgeRejected(f"INVALID_{name.upper()}")
    return number


def _integer(value: Any, name: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise MT5BridgeRejected(f"INVALID_{name.upper()}")
    return value


def _boolean(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise MT5BridgeRejected(f"INVALID_{name.upper()}")
    return value


def _timestamp(value: Any, name: str = "timestamp") -> datetime:
    text = _text(value, name, maximum=40)
    try:
        timestamp = datetime.fromisoformat(text)
    except ValueError as error:
        raise MT5BridgeRejected(f"INVALID_{name.upper()}") from error
    if timestamp.utcoffset() != timedelta(0):
        raise MT5BridgeRejected(f"INVALID_{name.upper()}")
    return timestamp


def _object(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise MT5BridgeRejected(f"INVALID_{name.upper()}")
    return value


def _list(value: Any, name: str, *, maximum: int = 10_000) -> list[Any]:
    if not isinstance(value, list) or len(value) > maximum:
        raise MT5BridgeRejected(f"INVALID_{name.upper()}")
    return value


def _position(value: Any) -> dict[str, Any]:
    item = _object(value, "position")
    _keys(
        item,
        {
            "ticket",
            "root_id",
            "symbol",
            "side",
            "volume",
            "price_open",
            "sl",
            "tp",
            "magic",
        },
        "position",
    )
    side = _text(item["side"], "position_side")
    if side not in {"BUY", "SELL"}:
        raise MT5BridgeRejected("INVALID_POSITION_SIDE")
    return {
        "ticket": _text(item["ticket"], "position_ticket"),
        "root_id": _text(item["root_id"], "position_root_id"),
        "symbol": _text(item["symbol"], "position_symbol"),
        "side": side,
        "volume": str(_decimal(item["volume"], "position_volume", positive=True)),
        "price_open": str(
            _decimal(item["price_open"], "position_price", positive=True)
        ),
        "sl": str(_decimal(item["sl"], "position_sl")),
        "tp": str(_decimal(item["tp"], "position_tp")),
        "magic": _integer(item["magic"], "position_magic"),
    }


def _order(value: Any) -> dict[str, Any]:
    item = _object(value, "order")
    _keys(
        item,
        {
            "ticket",
            "root_id",
            "symbol",
            "side",
            "order_type",
            "volume",
            "filled_volume",
            "price",
            "status",
            "sl",
            "tp",
            "magic",
        },
        "order",
    )
    side = _text(item["side"], "order_side")
    status = _text(item["status"], "order_status")
    if (
        side not in {"BUY", "SELL"}
        or status not in ACTIVE_ORDER_STATUSES | TERMINAL_ORDER_STATUSES
    ):
        raise MT5BridgeRejected("INVALID_ORDER_STATE")
    volume = _decimal(item["volume"], "order_volume", positive=True)
    filled = _decimal(item["filled_volume"], "filled_volume")
    if filled < 0 or filled > volume:
        raise MT5BridgeRejected("INVALID_FILLED_VOLUME")
    return {
        "ticket": _text(item["ticket"], "order_ticket"),
        "root_id": _text(item["root_id"], "order_root_id"),
        "symbol": _text(item["symbol"], "order_symbol"),
        "side": side,
        "order_type": _text(item["order_type"], "order_type"),
        "volume": str(volume),
        "filled_volume": str(filled),
        "price": str(_decimal(item["price"], "order_price", positive=True)),
        "status": status,
        "sl": str(_decimal(item["sl"], "order_sl")),
        "tp": str(_decimal(item["tp"], "order_tp")),
        "magic": _integer(item["magic"], "order_magic"),
    }


def _event(value: Any) -> dict[str, Any]:
    item = _object(value, "trade_event")
    _keys(
        item,
        {
            "event_id",
            "request_id",
            "status",
            "ticket",
            "volume",
            "filled_volume",
            "price",
            "reason",
            "timestamp",
        },
        "trade_event",
    )
    status = _text(item["status"], "event_status")
    if status not in ACTIVE_ORDER_STATUSES | TERMINAL_ORDER_STATUSES:
        raise MT5BridgeRejected("INVALID_EVENT_STATUS")
    volume = _decimal(item["volume"], "event_volume", positive=True)
    filled = _decimal(item["filled_volume"], "event_filled_volume")
    price = _decimal(item["price"], "event_price")
    if filled < 0 or filled > volume:
        raise MT5BridgeRejected("INVALID_EVENT_FILLED_VOLUME")
    if price < 0 or (status not in {"CANCELED", "EXPIRED", "REJECTED"} and price == 0):
        raise MT5BridgeRejected("INVALID_EVENT_PRICE")
    return {
        "event_id": _text(item["event_id"], "event_id"),
        "request_id": _text(item["request_id"], "event_request_id"),
        "status": status,
        "ticket": _text(item["ticket"], "event_ticket"),
        "volume": str(volume),
        "filled_volume": str(filled),
        "price": str(price),
        "reason": str(item["reason"])[:256],
        "timestamp": _timestamp(item["timestamp"], "event_timestamp").isoformat(),
    }


def _deal(value: Any) -> dict[str, Any]:
    item = _object(value, "deal")
    _keys(
        item,
        {
            "deal_id",
            "root_id",
            "side",
            "entry_kind",
            "volume",
            "price",
            "fee",
            "timestamp",
        },
        "deal",
    )
    side = _text(item["side"], "deal_side")
    entry_kind = _text(item["entry_kind"], "deal_entry_kind")
    if side not in {"BUY", "SELL"} or entry_kind not in {"IN", "OUT", "INOUT"}:
        raise MT5BridgeRejected("INVALID_DEAL_STATE")
    return {
        "deal_id": _text(item["deal_id"], "deal_id"),
        "root_id": _text(item["root_id"], "deal_root_id"),
        "side": side,
        "entry_kind": entry_kind,
        "volume": str(_decimal(item["volume"], "deal_volume", positive=True)),
        "price": str(_decimal(item["price"], "deal_price", positive=True)),
        "fee": str(_decimal(item["fee"], "deal_fee")),
        "timestamp": _timestamp(item["timestamp"], "deal_timestamp").isoformat(),
    }


def parse_snapshot(payload: Any) -> dict[str, Any]:
    root = _object(payload, "snapshot")
    _keys(
        root,
        {
            "version",
            "type",
            "terminal_id",
            "session_id",
            "seq",
            "nonce",
            "timestamp",
            "terminal",
            "account",
            "symbol",
            "positions",
            "orders",
            "events",
            "deals",
        },
        "snapshot",
    )
    if root["version"] != BRIDGE_VERSION or root["type"] != "snapshot":
        raise MT5BridgeRejected("BRIDGE_VERSION_UNSUPPORTED")

    terminal = _object(root["terminal"], "terminal")
    _keys(
        terminal,
        {
            "connected",
            "trade_allowed",
            "account_trade_allowed",
            "account_mode",
            "margin_mode",
            "server",
            "build",
        },
        "terminal",
    )
    if terminal["account_mode"] not in {"DEMO", "REAL"}:
        raise MT5BridgeRejected("ACCOUNT_MODE_INVALID")
    if terminal["margin_mode"] not in {"HEDGING", "NETTING"}:
        raise MT5BridgeRejected("POSITION_MODE_INVALID")

    account = _object(root["account"], "account")
    _keys(
        account,
        {"login", "balance", "equity", "margin", "free_margin", "leverage", "currency"},
        "account",
    )
    symbol = _object(root["symbol"], "symbol")
    _keys(
        symbol,
        {
            "name",
            "digits",
            "point",
            "tick_size",
            "tick_value",
            "contract_size",
            "volume_min",
            "volume_max",
            "volume_step",
            "stops_level",
            "bid",
            "ask",
            "margin_buy_per_lot",
            "margin_sell_per_lot",
        },
        "symbol",
    )

    normalized = {
        "version": BRIDGE_VERSION,
        "type": "snapshot",
        "terminal_id": _text(root["terminal_id"], "terminal_id"),
        "session_id": _text(root["session_id"], "session_id"),
        "seq": _integer(root["seq"], "seq", minimum=1),
        "nonce": _text(root["nonce"], "nonce"),
        "timestamp": _timestamp(root["timestamp"]).isoformat(),
        "terminal": {
            "connected": _boolean(terminal["connected"], "terminal_connected"),
            "trade_allowed": _boolean(terminal["trade_allowed"], "trade_allowed"),
            "account_trade_allowed": _boolean(
                terminal["account_trade_allowed"], "account_trade_allowed"
            ),
            "account_mode": terminal["account_mode"],
            "margin_mode": terminal["margin_mode"],
            "server": _text(terminal["server"], "server"),
            "build": _integer(terminal["build"], "terminal_build", minimum=1),
        },
        "account": {
            "login": _text(account["login"], "account_login"),
            "balance": str(_decimal(account["balance"], "balance")),
            "equity": str(_decimal(account["equity"], "equity")),
            "margin": str(_decimal(account["margin"], "margin")),
            "free_margin": str(_decimal(account["free_margin"], "free_margin")),
            "leverage": _integer(account["leverage"], "leverage", minimum=1),
            "currency": _text(account["currency"], "currency", maximum=16),
        },
        "symbol": {
            "name": _text(symbol["name"], "symbol"),
            "digits": _integer(symbol["digits"], "digits"),
            "point": str(_decimal(symbol["point"], "point", positive=True)),
            "tick_size": str(_decimal(symbol["tick_size"], "tick_size", positive=True)),
            "tick_value": str(
                _decimal(symbol["tick_value"], "tick_value", positive=True)
            ),
            "contract_size": str(
                _decimal(symbol["contract_size"], "contract_size", positive=True)
            ),
            "volume_min": str(
                _decimal(symbol["volume_min"], "volume_min", positive=True)
            ),
            "volume_max": str(
                _decimal(symbol["volume_max"], "volume_max", positive=True)
            ),
            "volume_step": str(
                _decimal(symbol["volume_step"], "volume_step", positive=True)
            ),
            "stops_level": _integer(symbol["stops_level"], "stops_level"),
            "bid": str(_decimal(symbol["bid"], "bid", positive=True)),
            "ask": str(_decimal(symbol["ask"], "ask", positive=True)),
            "margin_buy_per_lot": str(
                _decimal(
                    symbol["margin_buy_per_lot"], "margin_buy_per_lot", positive=True
                )
            ),
            "margin_sell_per_lot": str(
                _decimal(
                    symbol["margin_sell_per_lot"], "margin_sell_per_lot", positive=True
                )
            ),
        },
        "positions": [
            _position(item) for item in _list(root["positions"], "positions")
        ],
        "orders": [_order(item) for item in _list(root["orders"], "orders")],
        "events": [_event(item) for item in _list(root["events"], "events")],
        "deals": [_deal(item) for item in _list(root["deals"], "deals")],
    }
    if Decimal(normalized["symbol"]["ask"]) < Decimal(normalized["symbol"]["bid"]):
        raise MT5BridgeRejected("INVALID_QUOTE")
    if Decimal(normalized["symbol"]["volume_max"]) < Decimal(
        normalized["symbol"]["volume_min"]
    ):
        raise MT5BridgeRejected("INVALID_SYMBOL_VOLUME_RANGE")
    return normalized


class MT5BridgeStore:
    def __init__(
        self,
        connection: sqlite3.Connection,
        *,
        expected_account_mode: str,
        expected_login: str,
        expected_server: str,
        expected_symbol: str,
        expected_position_mode: str = "HEDGING",
        clock: Callable[[], datetime] | None = None,
        max_clock_skew: timedelta = timedelta(seconds=30),
    ) -> None:
        if max_clock_skew <= timedelta(0):
            raise ValueError("max_clock_skew must be positive")
        if expected_account_mode not in {"DEMO", "REAL"}:
            raise ValueError("expected MT5 account mode must be DEMO or REAL")
        if expected_position_mode not in {"AUTO", "HEDGING", "NETTING"}:
            raise ValueError(
                "expected MT5 position mode must be AUTO, HEDGING, or NETTING"
            )
        if (
            not expected_login.isdigit()
            or len(expected_login) > 32
            or not expected_server
            or len(expected_server) > 128
            or not expected_symbol
            or len(expected_symbol) > 64
        ):
            raise ValueError("expected MT5 account identity is required")
        self.connection = connection
        self.expected_account_mode = expected_account_mode
        self.expected_position_mode = expected_position_mode
        self.expected_login = expected_login
        self.expected_server = expected_server
        self.expected_symbol = expected_symbol
        self.clock = clock or (lambda: datetime.now(UTC))
        self.max_clock_skew = max_clock_skew
        ensure_mt5_schema(connection)

    def _matches_config(self, snapshot: Mapping[str, Any]) -> bool:
        return (
            snapshot["terminal"]["account_mode"] == self.expected_account_mode
            and self.matches_position_mode(snapshot["terminal"]["margin_mode"])
            and snapshot["account"]["login"] == self.expected_login
            and snapshot["terminal"]["server"] == self.expected_server
            and snapshot["symbol"]["name"] == self.expected_symbol
        )

    def matches_position_mode(self, actual: str) -> bool:
        return actual in {"HEDGING", "NETTING"} and (
            self.expected_position_mode == "AUTO"
            or actual == self.expected_position_mode
        )

    def account_config(self) -> dict[str, str]:
        return {
            "account_mode": self.expected_account_mode,
            "position_mode": self.expected_position_mode,
            "login": self.expected_login,
            "server": self.expected_server,
            "symbol": self.expected_symbol,
        }

    def ingest(self, payload: Any) -> str:
        snapshot = parse_snapshot(payload)
        if not self._matches_config(snapshot):
            raise MT5BridgeRejected("ACCOUNT_IDENTITY_MISMATCH")
        observed = _timestamp(snapshot["timestamp"])
        now = self.clock()
        if now.utcoffset() != timedelta(0) or abs(now - observed) > self.max_clock_skew:
            raise MT5BridgeRejected("STATE_STALE")
        canonical = json.dumps(snapshot, sort_keys=True, separators=(",", ":"))
        state_id = hashlib.sha256(canonical.encode()).hexdigest()
        terminal_id = snapshot["terminal_id"]
        session_id = snapshot["session_id"]
        seq = snapshot["seq"]

        current = self.connection.execute(
            "SELECT session_id, last_seq, state_id, payload FROM mt5_terminal_state "
            "WHERE terminal_id = ?",
            (terminal_id,),
        ).fetchone()
        identity_changed = False
        if current is not None:
            previous = json.loads(current[3])
            identity_changed = (
                previous["terminal"]["account_mode"]
                != snapshot["terminal"]["account_mode"]
                or previous["terminal"]["margin_mode"]
                != snapshot["terminal"]["margin_mode"]
                or previous["account"]["login"] != snapshot["account"]["login"]
                or previous["terminal"]["server"] != snapshot["terminal"]["server"]
                or previous["symbol"]["name"] != snapshot["symbol"]["name"]
            )
        sequence_error = None
        if current is None:
            pass  # ponytail: fresh state, accept any seq — terminal may have outlived bridge restarts
        elif identity_changed and seq != 1:
            sequence_error = "ACCOUNT_SWITCH_REQUIRES_SEQ_1"
        elif identity_changed:
            pass
        elif current is not None and current[0] == session_id:
            if seq == current[1] and state_id == current[2]:
                return state_id
            # ponytail: accept any forward seq — dedup only on exact replay
        elif current is not None:
            pass  # ponytail: session changed, accept new snapshot
        if sequence_error is not None:
            with immediate_transaction(self.connection):
                self.connection.execute(
                    "INSERT INTO mt5_terminal_invalidations VALUES (?, ?, ?) "
                    "ON CONFLICT(terminal_id) DO UPDATE SET "
                    "reason=excluded.reason, invalidated_at=excluded.invalidated_at",
                    (terminal_id, sequence_error, now.isoformat()),
                )
            raise MT5BridgeRejected(sequence_error)

        with immediate_transaction(self.connection):
            if identity_changed:
                self.connection.execute(
                    "UPDATE mt5_commands SET status='REJECTED', ack_payload=? "
                    "WHERE terminal_id=? AND status IN ('PENDING', 'DELIVERED')",
                    ('{"error":"ACCOUNT_CONFIG_CHANGED"}', terminal_id),
                )
            try:
                self.connection.execute(
                    "INSERT INTO mt5_snapshots VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        terminal_id,
                        session_id,
                        seq,
                        snapshot["nonce"],
                        state_id,
                        snapshot["timestamp"],
                        now.isoformat(),
                        canonical,
                    ),
                )
            except sqlite3.IntegrityError as error:
                raise MT5BridgeRejected("REPLAY_REJECTED") from error

            self.connection.execute(
                "INSERT INTO mt5_terminal_state VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(terminal_id) DO UPDATE SET "
                "session_id=excluded.session_id, last_seq=excluded.last_seq, "
                "state_id=excluded.state_id, observed_at=excluded.observed_at, payload=excluded.payload",
                (
                    terminal_id,
                    session_id,
                    seq,
                    state_id,
                    snapshot["timestamp"],
                    canonical,
                ),
            )
            self.connection.execute(
                "DELETE FROM mt5_terminal_invalidations WHERE terminal_id = ?",
                (terminal_id,),
            )
            for event in snapshot["events"]:
                encoded = json.dumps(event, sort_keys=True, separators=(",", ":"))
                existing = self.connection.execute(
                    "SELECT payload FROM mt5_trade_events WHERE event_id = ?",
                    (event["event_id"],),
                ).fetchone()
                if existing is not None and existing[0] != encoded:
                    raise MT5BridgeRejected("EVENT_ID_CONFLICT")
                self.connection.execute(
                    "INSERT OR IGNORE INTO mt5_trade_events VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        event["event_id"],
                        terminal_id,
                        event["request_id"],
                        event["status"],
                        event["timestamp"],
                        encoded,
                    ),
                )
            for deal in snapshot["deals"]:
                encoded = json.dumps(deal, sort_keys=True, separators=(",", ":"))
                existing = self.connection.execute(
                    "SELECT payload FROM mt5_deals WHERE deal_id = ?",
                    (deal["deal_id"],),
                ).fetchone()
                if existing is not None and existing[0] != encoded:
                    raise MT5BridgeRejected("DEAL_ID_CONFLICT")
                inserted = self.connection.execute(
                    "INSERT OR IGNORE INTO mt5_deals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        deal["deal_id"],
                        terminal_id,
                        deal["root_id"],
                        deal["side"],
                        deal["entry_kind"],
                        deal["volume"],
                        deal["price"],
                        deal["fee"],
                        deal["timestamp"],
                        encoded,
                    ),
                )
                if inserted.rowcount == 1:
                    if deal["entry_kind"] == "INOUT":
                        raise MT5BridgeRejected("INOUT_DEAL_UNSUPPORTED")
                    self._record_deal_fill(deal, snapshot["account"]["currency"])
        return state_id

    def _record_deal_fill(self, deal: Mapping[str, str], currency: str) -> None:
        order_id = self._deal_order_id(deal)
        if order_id is None:
            return
        row = self.connection.execute(
            "SELECT price FROM order_events WHERE order_id = ? ORDER BY seq DESC LIMIT 1",
            (order_id,),
        ).fetchone()
        if row is None:
            return
        price = Decimal(deal["price"])
        expected = price if row[0] is None else Decimal(row[0])
        direction = Decimal(1) if deal["side"] == "BUY" else Decimal(-1)
        record_fill(
            self.connection,
            event_id=f"fill:MT5:{deal['deal_id']}",
            order_event_id=f"filled:MT5:{deal['deal_id']}",
            fill_id=f"MT5:{deal['deal_id']}",
            order_id=order_id,
            quantity=Decimal(deal["volume"]),
            price=price,
            fee=Decimal(deal["fee"]),
            fee_currency=currency,
            slippage=(price - expected) * direction,
            funding=Decimal(0),
            created_at=deal["timestamp"],
        )

    def _deal_order_id(self, deal: Mapping[str, str]) -> str | None:
        root_id = deal["root_id"]
        latest = self.connection.execute(
            "SELECT status FROM order_events WHERE order_id = ? ORDER BY seq DESC LIMIT 1",
            (root_id,),
        ).fetchone()
        if latest is not None and latest[0] not in {
            "REJECTED",
            "FILLED",
            "CANCELED",
            "EXPIRED",
        }:
            return root_id
        if deal["entry_kind"] != "OUT":
            return None
        side = Decimal(-1) if deal["side"] == "SELL" else Decimal(1)
        volume = Decimal(deal["volume"])
        rows = self.connection.execute(
            "SELECT current.order_id, current.quantity, current.filled_quantity, "
            "i.execution_delta FROM order_events current "
            "JOIN intents i ON i.intent_id = current.intent_id "
            "WHERE i.venue = 'MT5' AND i.symbol = ? "
            "AND current.seq = (SELECT max(later.seq) FROM order_events later "
            "WHERE later.order_id = current.order_id) "
            "AND current.status NOT IN ('REJECTED','FILLED','CANCELED','EXPIRED')",
            (self.expected_symbol,),
        ).fetchall()
        matches = [
            order_id
            for order_id, quantity, filled, delta in rows
            if Decimal(delta) * side > 0
            and Decimal(quantity) - Decimal(filled) >= volume
        ]
        if len(matches) > 1:
            raise MT5BridgeRejected("DEAL_ORDER_AMBIGUOUS")
        return matches[0] if matches else None

    def latest(self, terminal_id: str) -> dict[str, Any] | None:
        if self.connection.execute(
            "SELECT 1 FROM mt5_terminal_invalidations WHERE terminal_id = ?",
            (terminal_id,),
        ).fetchone():
            return None
        row = self.connection.execute(
            "SELECT payload FROM mt5_terminal_state WHERE terminal_id = ?",
            (terminal_id,),
        ).fetchone()
        if row is None:
            return None
        snapshot = json.loads(row[0])
        return snapshot if self._matches_config(snapshot) else None

    def queue_command(
        self,
        terminal_id: str,
        request_id: str,
        kind: str,
        payload: Mapping[str, Any],
    ) -> bool:
        if kind not in {
            "place_order",
            "reduce_position",
            "cancel_order",
            "close_position",
            "sync_request",
            "set_protection",
        }:
            raise ValueError("unsupported MT5 command")
        encoded = json.dumps(dict(payload), sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        created_at = self.clock().isoformat()
        with immediate_transaction(self.connection):
            existing = self.connection.execute(
                "SELECT kind, payload_hash FROM mt5_commands WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if existing is not None:
                if existing != (kind, digest):
                    raise MT5BridgeRejected("COMMAND_ID_CONFLICT")
                return False
            if kind == "place_order":
                snapshot = self.latest(terminal_id)
                if (
                    snapshot is not None
                    and snapshot["terminal"]["margin_mode"] == "NETTING"
                ):
                    symbol = payload.get("symbol")
                    pending = self.connection.execute(
                        "SELECT request_id, payload FROM mt5_commands "
                        "WHERE terminal_id=? AND kind='place_order' "
                        "AND status IN ('PENDING', 'DELIVERED')",
                        (terminal_id,),
                    ).fetchall()
                    if any(
                        other_id != request_id
                        and json.loads(other_payload).get("symbol") == symbol
                        for other_id, other_payload in pending
                    ):
                        raise MT5BridgeRejected("MT5_NETTING_SYMBOL_BUSY")
            self.connection.execute(
                "INSERT INTO mt5_commands VALUES (?, ?, ?, ?, ?, ?, 'PENDING', NULL)",
                (request_id, terminal_id, kind, digest, encoded, created_at),
            )
            return True

    def next_command(self, terminal_id: str) -> dict[str, Any] | None:
        if self.latest(terminal_id) is None:
            return None
        with immediate_transaction(self.connection):
            row = self.connection.execute(
                "SELECT request_id, kind, payload FROM mt5_commands "
                "WHERE terminal_id = ? AND status IN ('PENDING', 'DELIVERED') "
                "ORDER BY created_at, request_id LIMIT 1",
                (terminal_id,),
            ).fetchone()
            if row is None:
                return None
            self.connection.execute(
                "UPDATE mt5_commands SET status='DELIVERED' WHERE request_id = ?",
                (row[0],),
            )
            return {"request_id": row[0], "type": row[1], "payload": json.loads(row[2])}

    def acknowledge(
        self, request_id: str, accepted: bool, result: Mapping[str, Any]
    ) -> None:
        encoded = json.dumps(dict(result), sort_keys=True, separators=(",", ":"))
        with immediate_transaction(self.connection):
            row = self.connection.execute(
                "SELECT status, ack_payload FROM mt5_commands WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if row is None:
                raise MT5BridgeRejected("COMMAND_MISSING")
            target = "ACKED" if accepted else "REJECTED"
            if row[0] in {"ACKED", "REJECTED"}:
                if row != (target, encoded):
                    raise MT5BridgeRejected("ACK_CONFLICT")
                return
            self.connection.execute(
                "UPDATE mt5_commands SET status = ?, ack_payload = ? WHERE request_id = ?",
                (target, encoded, request_id),
            )


class MT5FileBridge:
    def __init__(
        self, store: MT5BridgeStore, directory: Path, terminal_id: str
    ) -> None:
        self.store = store
        self.directory = directory
        self.terminal_id = terminal_id

    def _write(self, name: str, payload: Mapping[str, Any]) -> None:
        path = self.directory / name
        temporary = path.with_suffix(path.suffix + ".tmp")
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)

    def _read(self, name: str) -> Any:
        return json.loads((self.directory / name).read_text(encoding="utf-8"))

    def prepare(self) -> None:
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._write("config.json", self.store.account_config())

    def tick(self) -> None:
        ack_path = self.directory / "ack.json"
        command_path = self.directory / "command.json"
        if ack_path.exists():
            ack = _object(self._read("ack.json"), "ack")
            request_id = _text(ack.get("request_id"), "request_id")
            self.store.acknowledge(
                request_id,
                _boolean(ack.get("accepted"), "accepted"),
                _object(ack.get("result"), "result"),
            )
            ack_path.unlink()
            if command_path.exists():
                command = _object(self._read("command.json"), "command")
                if command.get("request_id") == request_id:
                    command_path.unlink()

        snapshot_path = self.directory / "snapshot.json"
        if snapshot_path.exists():
            snapshot = _object(self._read("snapshot.json"), "snapshot")
            try:
                self.store.ingest(snapshot)
            except MT5BridgeRejected as error:
                self._write(
                    "snapshot_ack.json",
                    {
                        "accepted": False,
                        "error": str(error),
                        "session_id": snapshot.get("session_id", ""),
                        "seq": snapshot.get("seq", 0),
                    },
                )
            else:
                self._write(
                    "snapshot_ack.json",
                    {
                        "accepted": True,
                        "session_id": snapshot["session_id"],
                        "seq": snapshot["seq"],
                    },
                )

        if not ack_path.exists() and not command_path.exists():
            command = self.store.next_command(self.terminal_id)
            if command is not None:
                self._write("command.json", command)


class MT5Venue:
    def __init__(self, store: MT5BridgeStore, terminal_id: str, symbol: str) -> None:
        self.store = store
        self.terminal_id = terminal_id
        self.symbol = symbol

    def _snapshot(self) -> dict[str, Any]:
        snapshot = self.store.latest(self.terminal_id)
        if snapshot is None:
            raise ExecutionRejected("VENUE_STATE_MISSING")
        if snapshot["symbol"]["name"] != self.symbol:
            raise ExecutionRejected("SYMBOL_MISMATCH")
        return snapshot

    def truth(self, symbol: str) -> ExecutionTruth:
        self._check_symbol(symbol)
        snapshot = self._snapshot()
        positions = [item for item in snapshot["positions"] if item["symbol"] == symbol]
        orders = [
            item
            for item in snapshot["orders"]
            if item["symbol"] == symbol and item["status"] in ACTIVE_ORDER_STATUSES
        ]
        actual = sum(
            (
                Decimal(item["volume"]) * (1 if item["side"] == "BUY" else -1)
                for item in positions
            ),
            Decimal(0),
        )
        pending = sum(
            (
                (Decimal(item["volume"]) - Decimal(item["filled_volume"]))
                * (1 if item["side"] == "BUY" else -1)
                for item in orders
            ),
            Decimal(0),
        )
        terminal = snapshot["terminal"]
        foreign = any(
            item["symbol"] == symbol and not self._owned_root(item["root_id"])
            for item in (*snapshot["positions"], *snapshot["orders"])
        )
        # ponytail: snapshot timestamp can be old; use now() since state was just validated
        from datetime import UTC
        from datetime import datetime as _dt
        return ExecutionTruth(
            observed_at=_dt.now(UTC),
            connected=(
                terminal["connected"]
                and terminal["trade_allowed"]
                and terminal["account_trade_allowed"]
                and terminal["account_mode"] == self.store.expected_account_mode
                and self.store.matches_position_mode(terminal["margin_mode"])
                and not foreign
            ),
            actual_quantity=actual,
            pending_quantity=pending,
            bid=Decimal(snapshot["symbol"]["bid"]),
            ask=Decimal(snapshot["symbol"]["ask"]),
            available_margin=Decimal(snapshot["account"]["free_margin"]),
            state_id=hashlib.sha256(
                json.dumps(snapshot, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
        )

    def rules(self, symbol: str) -> InstrumentRules:
        self._check_symbol(symbol)
        spec = self._snapshot()["symbol"]
        point = Decimal(spec["point"])
        return InstrumentRules(
            quantity_step=Decimal(spec["volume_step"]),
            quantity_min=Decimal(spec["volume_min"]),
            quantity_max=Decimal(spec["volume_max"]),
            tick_size=Decimal(spec["tick_size"]),
            price_min=point,
            price_max=Decimal(10) ** (int(spec["digits"]) + 8) * point,
            minimum_stop_distance=Decimal(spec["stops_level"]) * point,
            minimum_notional=Decimal(0),
        )

    def find_order(self, client_order_id: str) -> VenueOrder | None:
        snapshot = self._snapshot()
        command = self.store.connection.execute(
            "SELECT payload, status, kind FROM mt5_commands "
            "WHERE request_id = ? AND kind IN ('place_order', 'reduce_position')",
            (client_order_id,),
        ).fetchone()
        events = self.store.connection.execute(
            "SELECT event_id, status, payload FROM mt5_trade_events WHERE request_id = ? "
            "ORDER BY rowid DESC",
            (client_order_id,),
        ).fetchall()
        active = [
            item for item in snapshot["orders"] if item["root_id"] == client_order_id
        ]
        entry_deals = self.store.connection.execute(
            "SELECT volume, price FROM mt5_deals WHERE root_id = ? AND entry_kind IN ('IN', 'INOUT')",
            (client_order_id,),
        ).fetchall()
        positions = [
            item for item in snapshot["positions"] if item["root_id"] == client_order_id
        ]
        if (
            command is None
            and not events
            and not active
            and not entry_deals
            and not positions
        ):
            return None
        if command is None:
            raise ExecutionRejected("ORDER_COMMAND_MISSING")
        payload = json.loads(command[0])
        quantity = Decimal(payload["quantity"])
        if command[1] == "REJECTED":
            return VenueOrder(
                client_order_id,
                "REJECTED",
                quantity,
                Decimal(0),
                Decimal(payload["price"]),
                f"ack:{client_order_id}:rejected",
            )
        if command[2] == "reduce_position":
            before = Decimal(payload["before_actual"])
            expected = Decimal(payload["expected_actual"])
            actual = self.truth(str(payload["symbol"])).actual_quantity
            reduced = min(abs(before - actual), quantity)
            if actual == expected:
                status = "FILLED"
            elif reduced > 0 and abs(actual) < abs(before) and actual * before >= 0:
                status = "PARTIALLY_FILLED"
            else:
                return None
            return VenueOrder(
                client_order_id,
                status,
                quantity,
                reduced,
                Decimal(payload["price"]),
                f"snapshot:{client_order_id}:{status.lower()}",
            )
        filled = sum((Decimal(row[0]) for row in entry_deals), Decimal(0))
        latest = events[0] if events else None
        if latest is not None and latest[1] in {"REJECTED", "CANCELED", "EXPIRED"}:
            status = latest[1]
            event_id = latest[0]
        elif filled >= quantity:
            status = "FILLED"
            event_id = latest[0] if latest else f"snapshot:{client_order_id}:filled"
        elif filled > 0:
            status = "PARTIALLY_FILLED"
            event_id = latest[0] if latest else f"snapshot:{client_order_id}:partial"
        elif active or (latest is not None and latest[1] in ACTIVE_ORDER_STATUSES):
            status = latest[1] if latest is not None else "ACCEPTED"
            event_id = latest[0] if latest else f"snapshot:{client_order_id}:accepted"
        else:
            return None
        average = None
        if entry_deals:
            total = sum((Decimal(volume) for volume, _ in entry_deals), Decimal(0))
            average = (
                sum(
                    (Decimal(volume) * Decimal(price) for volume, price in entry_deals),
                    Decimal(0),
                )
                / total
            )
        return VenueOrder(
            client_order_id,
            status,
            quantity,
            min(filled, quantity),
            average or Decimal(payload["price"]),
            event_id,
        )

    def protection(self, client_order_id: str) -> tuple[ProtectionOrder, ...]:
        positions = [
            item
            for item in self._snapshot()["positions"]
            if item["root_id"] == client_order_id
        ]
        protection: list[ProtectionOrder] = []
        for position in positions:
            quantity = Decimal(position["volume"])
            sl = Decimal(position["sl"])
            tp = Decimal(position["tp"])
            if sl > 0:
                protection.append(
                    ProtectionOrder(
                        f"sl:{position['ticket']}",
                        "STOP_LOSS",
                        sl,
                        quantity,
                        "ACCEPTED",
                        True,
                    )
                )
            if tp > 0:
                protection.append(
                    ProtectionOrder(
                        f"tp:{position['ticket']}",
                        "TAKE_PROFIT",
                        tp,
                        quantity,
                        "ACCEPTED",
                        True,
                    )
                )
        return tuple(protection)

    def required_margin(self, request: OrderRequest) -> Decimal:
        spec = self._snapshot()["symbol"]
        per_lot = Decimal(
            spec["margin_buy_per_lot"]
            if request.side == "BUY"
            else spec["margin_sell_per_lot"]
        )
        return request.quantity * per_lot

    def submit(self, request: OrderRequest) -> ProtectedSubmission | None:
        self._check_symbol(request.symbol)
        if not self.truth(request.symbol).connected:
            raise ExecutionRejected("MT5_NOT_READY")
        snapshot = self._snapshot()
        if snapshot["terminal"]["margin_mode"] == "NETTING" and not request.reduce_only:
            active_roots = {
                item["root_id"]
                for item in (*snapshot["positions"], *snapshot["orders"])
                if item["symbol"] == request.symbol
                and self._owned_root(item["root_id"])
                and ("status" not in item or item["status"] in ACTIVE_ORDER_STATUSES)
            }
            if active_roots - {request.client_order_id}:
                raise ExecutionRejected("MT5_NETTING_SYMBOL_BUSY")
        if request.expires_at <= self.store.clock():
            raise ExecutionRejected("ORDER_EXPIRED")
        if not request.reduce_only and (
            len(request.take_profits) != 1 or request.take_profits[0][1] != Decimal(1)
        ):
            raise ExecutionRejected("MT5_SINGLE_FULL_TP_REQUIRED")
        if len(request.client_order_id) > 31:
            raise ExecutionRejected("MT5_CLIENT_ORDER_ID_TOO_LONG")
        kind = "reduce_position" if request.reduce_only else "place_order"
        actual = self.truth(request.symbol).actual_quantity
        try:
            self.store.queue_command(
                self.terminal_id,
                request.client_order_id,
                kind,
                {
                    "symbol": request.symbol,
                    "root_id": "*",
                    "side": request.side,
                    "quantity": str(request.quantity),
                    "order_type": request.order_type,
                    "price": str(request.price),
                    "trigger_price": None
                    if request.trigger_price is None
                    else str(request.trigger_price),
                    "reduce_only": request.reduce_only,
                    "expires_at": request.expires_at.isoformat(),
                    "expires_at_unix": int(request.expires_at.timestamp()),
                    "stop_loss": str(request.stop_loss),
                    "take_profits": [
                        [str(price), str(fraction)]
                        for price, fraction in request.take_profits
                    ],
                    "before_actual": str(actual),
                    "expected_actual": str(
                        actual
                        + (
                            request.quantity
                            if request.side == "BUY"
                            else -request.quantity
                        )
                    ),
                },
            )
        except MT5BridgeRejected as error:
            raise ExecutionRejected(str(error)) from error
        return None

    def cancel(self, client_order_id: str, request_id: str) -> VenueOrder | None:
        if not self._owned_root(client_order_id):
            raise ExecutionRejected("FOREIGN_EXPOSURE")
        self.store.queue_command(
            self.terminal_id,
            request_id,
            "cancel_order",
            {"root_id": client_order_id, "symbol": self.symbol},
        )
        return None

    def emergency_flatten(self, entry: VenueOrder) -> ExecutionTruth:
        if not self._owned_root(entry.client_order_id):
            raise ExecutionRejected("FOREIGN_EXPOSURE")
        self.store.queue_command(
            self.terminal_id,
            f"emergency:{entry.client_order_id}",
            "close_position",
            {"root_id": entry.client_order_id, "symbol": self.symbol},
        )
        return self.truth(self.symbol)

    def cancel_open_entries(self, symbol: str) -> tuple[VenueOrder, ...]:
        self._check_symbol(symbol)
        roots = sorted(
            {
                item["root_id"]
                for item in self._snapshot()["orders"]
                if item["symbol"] == symbol
                and item["status"] in ACTIVE_ORDER_STATUSES
                and self._owned_root(item["root_id"])
            }
        )
        observed = []
        for root_id in roots:
            order = self.find_order(root_id)
            if order is not None:
                observed.append(order)
            self.cancel(root_id, f"emergency-cancel:{root_id}")
        return tuple(observed)

    def ensure_position_protected(self, symbol: str) -> bool:
        self._check_symbol(symbol)
        positions = [
            item for item in self._snapshot()["positions"] if item["symbol"] == symbol
        ]
        return all(
            self._owned_root(item["root_id"]) and Decimal(item["sl"]) > 0
            for item in positions
        )

    def flatten_symbol(self, symbol: str) -> ExecutionTruth:
        self._check_symbol(symbol)
        snapshot = self._snapshot()
        if any(
            item["symbol"] == symbol and not self._owned_root(item["root_id"])
            for item in (*snapshot["positions"], *snapshot["orders"])
        ):
            raise ExecutionRejected("FOREIGN_EXPOSURE")
        self.store.queue_command(
            self.terminal_id,
            f"flatten:{self.truth(symbol).state_id}",
            "close_position",
            {"root_id": "*", "symbol": symbol},
        )
        return self.truth(symbol)

    def _owned_root(self, root_id: str) -> bool:
        return (
            self.store.connection.execute(
                "SELECT 1 FROM order_events oe JOIN intents i ON i.intent_id = oe.intent_id "
                "WHERE oe.order_id = ? AND oe.event_id LIKE 'submitted:%' "
                "AND i.venue = 'MT5' AND i.symbol = ? LIMIT 1",
                (root_id, self.symbol),
            ).fetchone()
            is not None
        )

    def _check_symbol(self, symbol: str) -> None:
        if symbol != self.symbol:
            raise ExecutionRejected("SYMBOL_MISMATCH")


def create_mt5_bridge_app(
    store: MT5BridgeStore,
    secret: str,
    *,
    max_body_bytes: int = 1_000_000,
) -> web.Application:
    if not secret or len(secret) < 32:
        raise ValueError("MT5 bridge secret must contain at least 32 characters")
    if max_body_bytes <= 0:
        raise ValueError("max_body_bytes must be positive")

    @web.middleware
    async def authenticate(request: web.Request, handler):
        try:
            remote = ipaddress.ip_address(request.remote or "")
        except ValueError as error:
            raise web.HTTPForbidden() from error
        if not remote.is_loopback:
            raise web.HTTPForbidden()
        supplied = request.headers.get("Authorization", "")
        expected = f"Bearer {secret}"
        if not hmac.compare_digest(supplied, expected):
            raise web.HTTPUnauthorized()
        return await handler(request)

    def response(payload: Mapping[str, Any], status: int = 200) -> web.Response:
        return web.Response(
            text=json.dumps(payload, separators=(",", ":")),
            status=status,
            content_type="application/json",
        )

    async def snapshot(request: web.Request) -> web.Response:
        try:
            payload = await request.json()
            state_id = store.ingest(payload)
        except (json.JSONDecodeError, MT5BridgeRejected) as error:
            return response({"error": str(error)}, status=409)
        return response({"accepted": True, "state_id": state_id})

    async def account_config(_: web.Request) -> web.Response:
        return response(store.account_config())

    async def next_command(request: web.Request) -> web.Response:
        terminal_id = request.query.get("terminal_id", "")
        if not terminal_id:
            raise web.HTTPBadRequest()
        command = store.next_command(terminal_id)
        return response({"command": command})

    async def acknowledge(request: web.Request) -> web.Response:
        request_id = request.match_info["request_id"]
        try:
            payload = await request.json()
            _keys(_object(payload, "ack"), {"accepted", "result"}, "ack")
            store.acknowledge(
                request_id,
                _boolean(payload["accepted"], "accepted"),
                _object(payload["result"], "result"),
            )
        except (json.JSONDecodeError, MT5BridgeRejected) as error:
            return response({"error": str(error)}, status=409)
        return response({"accepted": True})

    app = web.Application(client_max_size=max_body_bytes, middlewares=[authenticate])
    app.add_routes(
        [
            web.get("/v1/config", account_config),
            web.post("/v1/snapshot", snapshot),
            web.get("/v1/commands/next", next_command),
            web.post("/v1/commands/{request_id}/ack", acknowledge),
        ]
    )
    return app


def read_bridge_secret(path: str | Path) -> str:
    secret_path = Path(path)
    descriptor = os.open(secret_path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or info.st_mode & 0o077
        ):
            raise PermissionError(
                "MT5 bridge secret must be an owner-only regular file"
            )
        with os.fdopen(descriptor, encoding="utf-8") as handle:
            descriptor = -1
            secret = handle.read().strip()
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(secret) < 32:
        raise ValueError("MT5 bridge secret must contain at least 32 characters")
    return secret
