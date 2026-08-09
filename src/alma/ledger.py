import sqlite3
from datetime import datetime, timedelta
from decimal import Decimal
from os import PathLike

from alma.database import immediate_transaction, open_database

_ORDER_STATUSES = {
    "SUBMITTED",
    "ACCEPTED",
    "REJECTED",
    "PARTIALLY_FILLED",
    "FILLED",
    "CANCELED",
    "EXPIRED",
}
_TERMINAL_STATUSES = {"REJECTED", "FILLED", "CANCELED", "EXPIRED"}
_TRANSITIONS = {
    "SUBMITTED": _ORDER_STATUSES - {"SUBMITTED"},
    "ACCEPTED": {"PARTIALLY_FILLED", "FILLED", "CANCELED", "EXPIRED", "REJECTED"},
    "PARTIALLY_FILLED": {"PARTIALLY_FILLED", "FILLED", "CANCELED", "EXPIRED"},
}
_APPEND_ONLY_TABLES = (
    "decisions",
    "shadow_runs",
    "shadow_outcomes",
    "intents",
    "order_events",
    "fill_events",
    "audit_events",
    "calendar_events",
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS request_ids (
    request_id TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS venue_modes (
    venue_id TEXT PRIMARY KEY,
    mode TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pending_mode_transitions (
    venue TEXT PRIMARY KEY REFERENCES venue_modes(venue_id),
    symbol TEXT NOT NULL,
    state_id TEXT NOT NULL,
    final_mode TEXT NOT NULL,
    created_at TEXT NOT NULL,
    request_id TEXT NOT NULL UNIQUE REFERENCES request_ids(request_id)
);
CREATE TABLE IF NOT EXISTS decisions (
    decision_id TEXT PRIMARY KEY,
    state_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    raw_contract BLOB NOT NULL,
    validation_result TEXT NOT NULL,
    model_id TEXT NOT NULL,
    prompt_hash TEXT NOT NULL,
    policy_hash TEXT NOT NULL,
    code_hash TEXT NOT NULL,
    provenance TEXT NOT NULL CHECK (provenance IN ('EXECUTION', 'SHADOW'))
);
CREATE TABLE IF NOT EXISTS shadow_runs (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL UNIQUE,
    state_id TEXT NOT NULL,
    decision_id TEXT REFERENCES decisions(decision_id),
    status TEXT NOT NULL,
    validation_error TEXT,
    requested_model TEXT NOT NULL,
    actual_model TEXT NOT NULL,
    prompt_tokens INTEGER NOT NULL CHECK (prompt_tokens >= 0),
    completion_tokens INTEGER NOT NULL CHECK (completion_tokens >= 0),
    latency_ms REAL NOT NULL CHECK (latency_ms >= 0),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    failure_classes TEXT NOT NULL DEFAULT '',
    fallback_used INTEGER NOT NULL DEFAULT 0 CHECK (fallback_used IN (0, 1)),
    hooks TEXT NOT NULL,
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    setup TEXT NOT NULL,
    regime TEXT NOT NULL,
    session TEXT NOT NULL,
    news_state TEXT NOT NULL,
    hypothetical_delta TEXT,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS shadow_outcomes (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    decision_id TEXT NOT NULL UNIQUE REFERENCES decisions(decision_id),
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    setup TEXT NOT NULL,
    regime TEXT NOT NULL,
    session TEXT NOT NULL,
    news_state TEXT NOT NULL,
    confidence_bucket INTEGER NOT NULL CHECK (confidence_bucket BETWEEN 0 AND 9),
    uncertainty TEXT NOT NULL,
    won INTEGER NOT NULL CHECK (won IN (0, 1)),
    net_return TEXT NOT NULL,
    observed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS intents (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    intent_id TEXT NOT NULL UNIQUE,
    decision_id TEXT NOT NULL REFERENCES decisions(decision_id),
    request_id TEXT NOT NULL UNIQUE REFERENCES request_ids(request_id),
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    state_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    mode TEXT NOT NULL,
    desired_quantity TEXT NOT NULL,
    actual_quantity TEXT NOT NULL,
    pending_quantity TEXT NOT NULL,
    execution_delta TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS order_events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    intent_id TEXT NOT NULL REFERENCES intents(intent_id),
    order_id TEXT NOT NULL,
    status TEXT NOT NULL,
    quantity TEXT NOT NULL,
    filled_quantity TEXT NOT NULL,
    price TEXT,
    created_at TEXT NOT NULL,
    recovered INTEGER NOT NULL DEFAULT 0 CHECK (recovered IN (0, 1))
);
CREATE INDEX IF NOT EXISTS order_events_order_seq ON order_events(order_id, seq);
CREATE TABLE IF NOT EXISTS fill_events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    order_event_id TEXT NOT NULL UNIQUE REFERENCES order_events(event_id),
    fill_id TEXT NOT NULL UNIQUE,
    order_id TEXT NOT NULL,
    quantity TEXT NOT NULL,
    price TEXT NOT NULL,
    fee TEXT NOT NULL,
    fee_currency TEXT NOT NULL,
    slippage TEXT NOT NULL,
    funding TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit_events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    request_id TEXT,
    created_at TEXT NOT NULL,
    before_summary TEXT NOT NULL,
    after_summary TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS calendar_events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision >= 0),
    release_at TEXT NOT NULL,
    currency TEXT NOT NULL,
    impact TEXT NOT NULL CHECK (impact IN ('LOW', 'MEDIUM', 'HIGH')),
    title TEXT NOT NULL,
    actual TEXT,
    forecast TEXT,
    prior TEXT,
    source TEXT NOT NULL,
    received_at TEXT NOT NULL,
    UNIQUE(event_id, revision)
);
CREATE INDEX IF NOT EXISTS calendar_release ON calendar_events(release_at, event_id, revision);
"""


def open_ledger(path: str | PathLike[str]) -> sqlite3.Connection:
    connection = open_database(path)
    with connection:
        connection.executescript(_SCHEMA)
        connection.execute("INSERT OR IGNORE INTO schema_migrations VALUES (1)")
        for table in _APPEND_ONLY_TABLES:
            for operation in ("UPDATE", "DELETE"):
                connection.execute(
                    f"CREATE TRIGGER IF NOT EXISTS {table}_no_{operation.lower()} "
                    f"BEFORE {operation} ON {table} BEGIN "
                    "SELECT RAISE(ABORT, 'append-only table'); END"
                )
    return connection


def _decimal(value: Decimal, name: str, *, positive: bool = False) -> str:
    if not value.is_finite() or (positive and value <= 0):
        raise ValueError(
            f"{name} must be finite" + (" and positive" if positive else "")
        )
    return str(value)


def append_decision(
    connection: sqlite3.Connection,
    *,
    decision_id: str,
    state_id: str,
    created_at: str,
    raw_contract: bytes,
    validation_result: str,
    model_id: str,
    prompt_hash: str,
    policy_hash: str,
    code_hash: str,
    provenance: str = "EXECUTION",
) -> None:
    if provenance not in {"EXECUTION", "SHADOW"}:
        raise ValueError("unknown decision provenance")
    with connection:
        connection.execute(
            "INSERT INTO decisions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                decision_id,
                state_id,
                created_at,
                raw_contract,
                validation_result,
                model_id,
                prompt_hash,
                policy_hash,
                code_hash,
                provenance,
            ),
        )


def record_shadow_run(
    connection: sqlite3.Connection,
    *,
    request_id: str,
    state_id: str,
    decision: dict[str, object] | None,
    status: str,
    validation_error: str | None,
    requested_model: str,
    actual_model: str,
    prompt_tokens: int,
    completion_tokens: int,
    latency_ms: float,
    attempt_count: int,
    failure_classes: str,
    fallback_used: bool,
    hooks: str,
    venue: str,
    symbol: str,
    setup: str,
    regime: str,
    session: str,
    news_state: str,
    hypothetical_delta: Decimal | None,
    created_at: str,
    provenance: str = "SHADOW",
) -> None:
    if status not in {"ACCEPTED", "REJECTED", "NO_DECISION"}:
        raise ValueError("unknown shadow status")
    if min(prompt_tokens, completion_tokens, attempt_count) < 0 or latency_ms < 0:
        raise ValueError("shadow telemetry must be non-negative")
    if provenance not in {"EXECUTION", "SHADOW"}:
        raise ValueError("unknown decision provenance")
    with immediate_transaction(connection):
        decision_id = None
        if decision is not None:
            decision_id = str(decision["decision_id"])
            connection.execute(
                "INSERT INTO decisions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    decision_id,
                    str(decision["state_id"]),
                    str(decision["created_at"]),
                    decision["raw_contract"],
                    str(decision["validation_result"]),
                    str(decision["model_id"]),
                    str(decision["prompt_hash"]),
                    str(decision["policy_hash"]),
                    str(decision["code_hash"]),
                    provenance,
                ),
            )
        connection.execute(
            "INSERT INTO shadow_runs "
            "(request_id, state_id, decision_id, status, validation_error, requested_model, "
            "actual_model, prompt_tokens, completion_tokens, latency_ms, attempt_count, "
            "failure_classes, fallback_used, hooks, venue, symbol, setup, regime, session, "
            "news_state, hypothetical_delta, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                request_id,
                state_id,
                decision_id,
                status,
                validation_error,
                requested_model,
                actual_model,
                prompt_tokens,
                completion_tokens,
                latency_ms,
                attempt_count,
                failure_classes,
                int(fallback_used),
                hooks,
                venue,
                symbol,
                setup,
                regime,
                session,
                news_state,
                (
                    _decimal(hypothetical_delta, "hypothetical delta")
                    if hypothetical_delta is not None
                    else None
                ),
                created_at,
            ),
        )


def _insert_intent(
    connection: sqlite3.Connection,
    *,
    intent_id: str,
    decision_id: str,
    request_id: str,
    venue: str,
    symbol: str,
    state_id: str,
    created_at: str,
    mode: str,
    desired_quantity: Decimal,
    actual_quantity: Decimal,
    pending_quantity: Decimal,
    execution_delta: Decimal,
) -> None:
    connection.execute(
        "INSERT INTO intents "
        "(intent_id, decision_id, request_id, venue, symbol, state_id, created_at, mode, "
        "desired_quantity, actual_quantity, pending_quantity, execution_delta) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            intent_id,
            decision_id,
            request_id,
            venue,
            symbol,
            state_id,
            created_at,
            mode,
            _decimal(desired_quantity, "desired quantity"),
            _decimal(actual_quantity, "actual quantity"),
            _decimal(pending_quantity, "pending quantity"),
            _decimal(execution_delta, "execution delta"),
        ),
    )


def _latest_order(connection: sqlite3.Connection, order_id: str) -> tuple | None:
    return connection.execute(
        "SELECT intent_id, status, quantity, filled_quantity, price "
        "FROM order_events WHERE order_id = ? ORDER BY seq DESC LIMIT 1",
        (order_id,),
    ).fetchone()


def _insert_order_event(
    connection: sqlite3.Connection,
    *,
    event_id: str,
    intent_id: str,
    order_id: str,
    status: str,
    quantity: Decimal,
    filled_quantity: Decimal,
    price: Decimal | None,
    created_at: str,
    recovered: bool,
) -> None:
    if status not in _ORDER_STATUSES:
        raise ValueError("unknown order status")
    quantity_text = _decimal(quantity, "quantity", positive=True)
    filled_text = _decimal(filled_quantity, "filled quantity")
    if filled_quantity < 0 or filled_quantity > quantity:
        raise ValueError("filled quantity outside order quantity")
    if status == "FILLED" and filled_quantity != quantity:
        raise ValueError("FILLED requires complete quantity")
    if status == "PARTIALLY_FILLED" and not 0 < filled_quantity < quantity:
        raise ValueError("PARTIALLY_FILLED requires partial quantity")
    latest = _latest_order(connection, order_id)
    if latest is None:
        if not recovered and status != "SUBMITTED":
            raise ValueError("first local order event must be SUBMITTED")
    else:
        previous_intent, previous_status, previous_quantity, previous_filled, _ = latest
        if previous_status in _TERMINAL_STATUSES:
            raise ValueError("terminal order cannot transition")
        if status not in _TRANSITIONS[previous_status]:
            raise ValueError("invalid order transition")
        if intent_id != previous_intent or quantity_text != previous_quantity:
            raise ValueError("order identity or quantity changed")
        if filled_quantity < Decimal(previous_filled):
            raise ValueError("filled quantity moved backward")
    connection.execute(
        "INSERT INTO order_events "
        "(event_id, intent_id, order_id, status, quantity, filled_quantity, price, created_at, recovered) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            event_id,
            intent_id,
            order_id,
            status,
            quantity_text,
            filled_text,
            None if price is None else _decimal(price, "price", positive=True),
            created_at,
            int(recovered),
        ),
    )


def append_order_event(
    connection: sqlite3.Connection,
    *,
    event_id: str,
    intent_id: str,
    order_id: str,
    status: str,
    quantity: Decimal,
    filled_quantity: Decimal,
    price: Decimal | None,
    created_at: str,
    recovered: bool = False,
) -> None:
    with immediate_transaction(connection):
        _insert_order_event(
            connection,
            event_id=event_id,
            intent_id=intent_id,
            order_id=order_id,
            status=status,
            quantity=quantity,
            filled_quantity=filled_quantity,
            price=price,
            created_at=created_at,
            recovered=recovered,
        )


def reserve_order_submission(
    connection: sqlite3.Connection,
    *,
    event_id: str,
    intent_id: str,
    order_id: str,
    quantity: Decimal,
    price: Decimal | None,
    created_at: str,
) -> bool:
    with immediate_transaction(connection):
        if (
            connection.execute(
                "SELECT 1 FROM order_events WHERE order_id = ?", (order_id,)
            ).fetchone()
            is not None
        ):
            return False
        _insert_order_event(
            connection,
            event_id=event_id,
            intent_id=intent_id,
            order_id=order_id,
            status="SUBMITTED",
            quantity=quantity,
            filled_quantity=Decimal(0),
            price=price,
            created_at=created_at,
            recovered=False,
        )
        return True


def record_fill(
    connection: sqlite3.Connection,
    *,
    event_id: str,
    order_event_id: str,
    fill_id: str,
    order_id: str,
    quantity: Decimal,
    price: Decimal,
    fee: Decimal,
    fee_currency: str,
    slippage: Decimal,
    funding: Decimal,
    created_at: str,
) -> bool:
    fill_quantity = Decimal(_decimal(quantity, "fill quantity", positive=True))
    fill_price = _decimal(price, "fill price", positive=True)
    fee_text = _decimal(fee, "fee")
    slippage_text = _decimal(slippage, "slippage")
    funding_text = _decimal(funding, "funding")
    with immediate_transaction(connection):
        existing = connection.execute(
            "SELECT order_id, quantity, price, fee, fee_currency, slippage, funding "
            "FROM fill_events WHERE fill_id = ?",
            (fill_id,),
        ).fetchone()
        payload = (
            order_id,
            str(fill_quantity),
            fill_price,
            fee_text,
            fee_currency,
            slippage_text,
            funding_text,
        )
        if existing is not None:
            if existing != payload:
                raise ValueError("fill id payload conflict")
            return False
        latest = _latest_order(connection, order_id)
        if latest is None:
            raise ValueError("order does not exist")
        intent_id, status, order_quantity_text, filled_text, order_price = latest
        if status in _TERMINAL_STATUSES:
            raise ValueError("terminal order cannot receive fill")
        order_quantity = Decimal(order_quantity_text)
        total_filled = Decimal(filled_text) + fill_quantity
        if total_filled > order_quantity:
            raise ValueError("fill exceeds remaining quantity")
        next_status = "FILLED" if total_filled == order_quantity else "PARTIALLY_FILLED"
        _insert_order_event(
            connection,
            event_id=order_event_id,
            intent_id=intent_id,
            order_id=order_id,
            status=next_status,
            quantity=order_quantity,
            filled_quantity=total_filled,
            price=price if order_price is None else Decimal(order_price),
            created_at=created_at,
            recovered=False,
        )
        connection.execute(
            "INSERT INTO fill_events "
            "(event_id, order_event_id, fill_id, order_id, quantity, price, fee, fee_currency, "
            "slippage, funding, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                event_id,
                order_event_id,
                fill_id,
                order_id,
                str(fill_quantity),
                fill_price,
                fee_text,
                fee_currency,
                slippage_text,
                funding_text,
                created_at,
            ),
        )
        return True


def _insert_audit_event(
    connection: sqlite3.Connection,
    *,
    event_id: str,
    actor: str,
    action: str,
    request_id: str | None,
    created_at: str,
    before_summary: str,
    after_summary: str,
) -> None:
    connection.execute(
        "INSERT INTO audit_events "
        "(event_id, actor, action, request_id, created_at, before_summary, after_summary) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            event_id,
            actor,
            action,
            request_id,
            created_at,
            before_summary,
            after_summary,
        ),
    )


def append_audit_event(connection: sqlite3.Connection, **values: object) -> None:
    with connection:
        _insert_audit_event(connection, **values)  # type: ignore[arg-type]


def append_calendar_event(
    connection: sqlite3.Connection,
    *,
    event_id: str,
    revision: int,
    release_at: str,
    currency: str,
    impact: str,
    title: str,
    actual: str | None,
    forecast: str | None,
    prior: str | None,
    source: str,
    received_at: str,
) -> bool:
    values = (event_id, currency, title, source)
    if any(
        not isinstance(value, str) or not value or len(value) > 256 for value in values
    ):
        raise ValueError("calendar text is empty or too long")
    if isinstance(revision, bool) or not isinstance(revision, int):
        raise TypeError("invalid calendar revision")
    if revision < 0 or impact not in {"LOW", "MEDIUM", "HIGH"}:
        raise ValueError("invalid calendar revision or impact")
    for value in (actual, forecast, prior):
        if value is not None and (not isinstance(value, str) or len(value) > 128):
            raise ValueError("calendar value is too long")
    for value in (release_at, received_at):
        try:
            timestamp = datetime.fromisoformat(value)
        except ValueError as error:
            raise ValueError("calendar timestamp must be RFC3339") from error
        if timestamp.utcoffset() != timedelta(0):
            raise ValueError("calendar timestamp must be UTC")
    payload = (
        release_at,
        currency,
        impact,
        title,
        actual,
        forecast,
        prior,
        source,
        received_at,
    )
    with immediate_transaction(connection):
        existing = connection.execute(
            "SELECT release_at, currency, impact, title, actual, forecast, prior, source, "
            "received_at FROM calendar_events WHERE event_id = ? AND revision = ?",
            (event_id, revision),
        ).fetchone()
        if existing is not None:
            if existing != payload:
                raise ValueError("calendar revision payload conflict")
            return False
        latest = connection.execute(
            "SELECT max(revision) FROM calendar_events WHERE event_id = ?", (event_id,)
        ).fetchone()[0]
        if latest is not None and revision <= latest:
            raise ValueError("calendar revision moved backward")
        connection.execute(
            "INSERT INTO calendar_events "
            "(event_id, revision, release_at, currency, impact, title, actual, forecast, prior, "
            "source, received_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (event_id, revision, *payload),
        )
    return True


def record_intent_mutation(
    connection: sqlite3.Connection,
    *,
    audit_event_id: str,
    actor: str,
    before_summary: str,
    after_summary: str,
    **intent: object,
) -> bool:
    request_id = str(intent["request_id"])
    with immediate_transaction(connection):
        cursor = connection.execute(
            "INSERT OR IGNORE INTO request_ids VALUES (?)", (request_id,)
        )
        if cursor.rowcount != 1:
            return False
        _insert_intent(connection, **intent)  # type: ignore[arg-type]
        _insert_audit_event(
            connection,
            event_id=audit_event_id,
            actor=actor,
            action="INTENT_PREPARED",
            request_id=request_id,
            created_at=str(intent["created_at"]),
            before_summary=before_summary,
            after_summary=after_summary,
        )
    return True


def reconstruct_episode(
    connection: sqlite3.Connection, decision_id: str
) -> dict[str, object]:
    def rows(sql: str, parameters: tuple = ()) -> list[dict[str, object]]:
        cursor = connection.execute(sql, parameters)
        names = [column[0] for column in cursor.description]
        return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]

    decision_rows = rows(
        "SELECT * FROM decisions WHERE decision_id = ?", (decision_id,)
    )
    if not decision_rows:
        raise KeyError(decision_id)
    intents = rows(
        "SELECT * FROM intents WHERE decision_id = ? ORDER BY seq", (decision_id,)
    )
    intent_ids = [row["intent_id"] for row in intents]
    if not intent_ids:
        return {
            "decision": decision_rows[0],
            "intents": [],
            "order_events": [],
            "fill_events": [],
            "audit_events": [],
        }
    placeholders = ",".join("?" for _ in intent_ids)
    order_events = rows(
        f"SELECT * FROM order_events WHERE intent_id IN ({placeholders}) ORDER BY seq",
        tuple(intent_ids),
    )
    order_ids = [row["order_id"] for row in order_events]
    fill_events = (
        rows(
            f"SELECT * FROM fill_events WHERE order_id IN ({','.join('?' for _ in order_ids)}) ORDER BY seq",
            tuple(order_ids),
        )
        if order_ids
        else []
    )
    request_ids = [row["request_id"] for row in intents]
    audit_events = rows(
        f"SELECT * FROM audit_events WHERE request_id IN ({','.join('?' for _ in request_ids)}) ORDER BY seq",
        tuple(request_ids),
    )
    return {
        "decision": decision_rows[0],
        "intents": intents,
        "order_events": order_events,
        "fill_events": fill_events,
        "audit_events": audit_events,
    }


def backup_ledger(connection: sqlite3.Connection, path: str | PathLike[str]) -> None:
    destination = sqlite3.connect(path)
    try:
        connection.backup(destination)
    finally:
        destination.close()
