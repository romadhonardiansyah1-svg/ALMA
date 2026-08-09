import hashlib
import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

from nautilus_trader.model.events import OrderFilled

from alma.decision_contract import parse_decision_contract
from alma.ledger import record_fill


def child_order_id(entry_id: str, trade_id: str, kind: str, index: int = 0) -> str:
    digest = hashlib.sha256(
        f"{entry_id}:{trade_id}:{kind}:{index}".encode()
    ).hexdigest()[:24]
    return f"alma-{digest}"


def parent_for_child(connection: sqlite3.Connection, child_id: str) -> str | None:
    rows = connection.execute(
        "SELECT DISTINCT f.order_id, f.fill_id, d.raw_contract "
        "FROM fill_events f JOIN order_events o ON o.order_id = f.order_id "
        "JOIN intents i ON i.intent_id = o.intent_id "
        "JOIN decisions d ON d.decision_id = i.decision_id "
        "WHERE o.event_id LIKE 'submitted:%'",
    ).fetchall()
    for entry_id, fill_id, raw_contract in rows:
        trade_id = fill_id.removeprefix("BINANCE:")
        if child_id == child_order_id(entry_id, trade_id, "sl"):
            return entry_id
        contract = parse_decision_contract(raw_contract)
        target_count = sum(
            Decimal(target.close_fraction) > 0 for target in contract.targets
        )
        if any(
            child_id == child_order_id(entry_id, trade_id, "tp", index)
            for index in range(target_count)
        ):
            return entry_id
    return None


def record_native_fill(
    connection: sqlite3.Connection,
    event: OrderFilled,
) -> bool:
    order_id = str(event.client_order_id)
    row = connection.execute(
        "SELECT price FROM order_events WHERE order_id = ? ORDER BY seq DESC LIMIT 1",
        (order_id,),
    ).fetchone()
    if row is None:
        raise ValueError("native fill order is not reserved")
    price = event.last_px.as_decimal()
    expected = None if row[0] is None else Decimal(row[0])
    slippage = Decimal(0)
    if expected is not None:
        slippage = price - expected if event.is_buy else expected - price
    seconds, nanoseconds = divmod(event.ts_event, 1_000_000_000)
    created_at = datetime.fromtimestamp(seconds, UTC).replace(
        microsecond=nanoseconds // 1_000
    )
    trade_id = str(event.trade_id)
    return record_fill(
        connection,
        event_id=f"fill:BINANCE:{trade_id}",
        order_event_id=f"fill-order:BINANCE:{trade_id}",
        fill_id=f"BINANCE:{trade_id}",
        order_id=order_id,
        quantity=event.last_qty.as_decimal(),
        price=price,
        fee=event.commission.as_decimal(),
        fee_currency=event.commission.currency.code,
        slippage=slippage,
        funding=Decimal(0),
        created_at=created_at.isoformat(),
    )
