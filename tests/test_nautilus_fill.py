from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

from alma.ledger import (
    append_decision,
    open_ledger,
    record_intent_mutation,
    reserve_order_submission,
)
from alma.nautilus_fill import child_order_id, parent_for_child, record_native_fill


class Value:
    def __init__(self, value: str) -> None:
        self.value = Decimal(value)

    def as_decimal(self) -> Decimal:
        return self.value


def test_native_fill_is_exact_and_idempotent(tmp_path) -> None:
    connection = open_ledger(tmp_path / "alma.db")
    try:
        append_decision(
            connection,
            decision_id="decision-1",
            state_id="state-1",
            created_at="2026-07-31T12:00:00+00:00",
            raw_contract=b"{}",
            validation_result="ACCEPTED",
            model_id="model",
            prompt_hash="prompt",
            policy_hash="policy",
            code_hash="code",
        )
        record_intent_mutation(
            connection,
            audit_event_id="audit-1",
            actor="executor",
            before_summary="{}",
            after_summary="{}",
            intent_id="intent-1",
            decision_id="decision-1",
            request_id="request-1",
            venue="BINANCE",
            symbol="BTCUSDT-PERP",
            state_id="state-1",
            created_at="2026-07-31T12:00:00+00:00",
            mode="TRADE",
            desired_quantity=Decimal(1),
            actual_quantity=Decimal(0),
            pending_quantity=Decimal(0),
            execution_delta=Decimal(1),
        )
        reserve_order_submission(
            connection,
            event_id="submitted:alma-order",
            intent_id="intent-1",
            order_id="alma-order",
            quantity=Decimal(1),
            price=Decimal(100),
            created_at="2026-07-31T12:00:00+00:00",
        )
        event = SimpleNamespace(
            client_order_id="alma-order",
            trade_id="trade-1",
            last_qty=Value("0.4"),
            last_px=Value("100.2"),
            commission=SimpleNamespace(
                as_decimal=lambda: Decimal("0.02"),
                currency=SimpleNamespace(code="USDT"),
            ),
            is_buy=True,
            ts_event=1_775_217_600_123_456_789,
        )

        assert record_native_fill(connection, event)
        assert not record_native_fill(connection, event)
        assert connection.execute(
            "SELECT quantity, price, fee, fee_currency, slippage, created_at "
            "FROM fill_events WHERE fill_id = 'BINANCE:trade-1'"
        ).fetchone() == (
            "0.4",
            "100.2",
            "0.02",
            "USDT",
            "0.2",
            datetime.fromtimestamp(1_775_217_600, UTC)
            .replace(microsecond=123_456)
            .isoformat(),
        )
        assert (
            parent_for_child(connection, child_order_id("alma-order", "trade-1", "sl"))
            == "alma-order"
        )
    finally:
        connection.close()
