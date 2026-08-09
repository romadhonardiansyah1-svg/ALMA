import sqlite3
from decimal import Decimal

import pytest

from alma.ledger import (
    append_audit_event,
    append_decision,
    append_order_event,
    backup_ledger,
    open_ledger,
    reconstruct_episode,
    record_fill,
    record_intent_mutation,
)


def add_decision(
    connection, decision_id: str = "decision-1", state_id: str = "state-1"
) -> None:
    append_decision(
        connection,
        decision_id=decision_id,
        state_id=state_id,
        created_at="2026-07-31T08:00:00+00:00",
        raw_contract=b'{"policy_version":"alma-v1"}',
        validation_result="ACCEPTED",
        model_id="model-1",
        prompt_hash="prompt-hash",
        policy_hash="policy-hash",
        code_hash="code-hash",
    )


def add_intent(connection) -> None:
    assert record_intent_mutation(
        connection,
        audit_event_id="audit:request-1",
        actor="test",
        before_summary="{}",
        after_summary="{}",
        intent_id="intent-1",
        decision_id="decision-1",
        request_id="request-1",
        venue="BINANCE",
        symbol="BTCUSDT-PERP",
        state_id="state-1",
        created_at="2026-07-31T08:00:01+00:00",
        mode="TRADE",
        desired_quantity=Decimal(1),
        actual_quantity=Decimal(0),
        pending_quantity=Decimal(0),
        execution_delta=Decimal(1),
    )


def test_ledger_reconstructs_decision_to_partial_and_final_fill_after_restart(
    tmp_path,
) -> None:
    path = tmp_path / "alma.db"
    connection = open_ledger(path)
    add_decision(connection)
    add_intent(connection)
    append_order_event(
        connection,
        event_id="order-event-1",
        intent_id="intent-1",
        order_id="order-1",
        status="SUBMITTED",
        quantity=Decimal(1),
        filled_quantity=Decimal(0),
        price=Decimal(100),
        created_at="2026-07-31T08:00:02+00:00",
    )
    assert record_fill(
        connection,
        event_id="fill-event-1",
        order_event_id="order-event-2",
        fill_id="fill-1",
        order_id="order-1",
        quantity=Decimal("0.4"),
        price=Decimal("100.1"),
        fee=Decimal("0.02"),
        fee_currency="USDT",
        slippage=Decimal("0.1"),
        funding=Decimal(0),
        created_at="2026-07-31T08:00:03+00:00",
    )
    assert not record_fill(
        connection,
        event_id="fill-event-1-replayed",
        order_event_id="order-event-2-replayed",
        fill_id="fill-1",
        order_id="order-1",
        quantity=Decimal("0.4"),
        price=Decimal("100.1"),
        fee=Decimal("0.02"),
        fee_currency="USDT",
        slippage=Decimal("0.1"),
        funding=Decimal(0),
        created_at="2026-07-31T08:00:03+00:00",
    )
    record_fill(
        connection,
        event_id="fill-event-2",
        order_event_id="order-event-3",
        fill_id="fill-2",
        order_id="order-1",
        quantity=Decimal("0.6"),
        price=Decimal("100.2"),
        fee=Decimal("0.03"),
        fee_currency="USDT",
        slippage=Decimal("0.2"),
        funding=Decimal("-0.01"),
        created_at="2026-07-31T08:00:04+00:00",
    )
    append_audit_event(
        connection,
        event_id="audit-1",
        actor="executor",
        action="ORDER_FILLED",
        request_id="request-1",
        created_at="2026-07-31T08:00:04+00:00",
        before_summary='{"status":"PARTIALLY_FILLED"}',
        after_summary='{"status":"FILLED"}',
    )
    connection.close()

    reopened = open_ledger(path)
    try:
        episode = reconstruct_episode(reopened, "decision-1")
        assert episode["decision"]["decision_id"] == "decision-1"
        assert [row["status"] for row in episode["order_events"]] == [
            "SUBMITTED",
            "PARTIALLY_FILLED",
            "FILLED",
        ]
        assert [row["fill_id"] for row in episode["fill_events"]] == [
            "fill-1",
            "fill-2",
        ]
        assert [row["action"] for row in episode["audit_events"]] == [
            "INTENT_PREPARED",
            "ORDER_FILLED",
        ]
    finally:
        reopened.close()


def test_order_transition_and_fill_are_validated_atomically(tmp_path) -> None:
    connection = open_ledger(tmp_path / "alma.db")
    try:
        add_decision(connection)
        add_intent(connection)
        append_order_event(
            connection,
            event_id="submitted",
            intent_id="intent-1",
            order_id="order-1",
            status="SUBMITTED",
            quantity=Decimal(1),
            filled_quantity=Decimal(0),
            price=None,
            created_at="2026-07-31T08:00:02+00:00",
        )
        with pytest.raises(ValueError, match="exceeds remaining"):
            record_fill(
                connection,
                event_id="bad-fill",
                order_event_id="bad-status",
                fill_id="fill-bad",
                order_id="order-1",
                quantity=Decimal("1.1"),
                price=Decimal(100),
                fee=Decimal(0),
                fee_currency="USDT",
                slippage=Decimal(0),
                funding=Decimal(0),
                created_at="2026-07-31T08:00:03+00:00",
            )
        assert connection.execute("SELECT count(*) FROM fill_events").fetchone() == (0,)
        assert connection.execute("SELECT count(*) FROM order_events").fetchone() == (
            1,
        )

        append_order_event(
            connection,
            event_id="rejected",
            intent_id="intent-1",
            order_id="order-1",
            status="REJECTED",
            quantity=Decimal(1),
            filled_quantity=Decimal(0),
            price=None,
            created_at="2026-07-31T08:00:03+00:00",
        )
        with pytest.raises(ValueError, match="terminal"):
            append_order_event(
                connection,
                event_id="accepted-late",
                intent_id="intent-1",
                order_id="order-1",
                status="ACCEPTED",
                quantity=Decimal(1),
                filled_quantity=Decimal(0),
                price=None,
                created_at="2026-07-31T08:00:04+00:00",
            )
    finally:
        connection.close()


def test_recovery_may_start_from_broker_observed_order_state(tmp_path) -> None:
    connection = open_ledger(tmp_path / "alma.db")
    try:
        add_decision(connection)
        add_intent(connection)
        append_order_event(
            connection,
            event_id="recovered",
            intent_id="intent-1",
            order_id="order-1",
            status="PARTIALLY_FILLED",
            quantity=Decimal(1),
            filled_quantity=Decimal("0.25"),
            price=Decimal(100),
            created_at="2026-07-31T08:00:05+00:00",
            recovered=True,
        )
        assert connection.execute(
            "SELECT status, filled_quantity FROM order_events"
        ).fetchone() == ("PARTIALLY_FILLED", "0.25")
    finally:
        connection.close()


def test_ledger_tables_reject_update_and_delete(tmp_path) -> None:
    connection = open_ledger(tmp_path / "alma.db")
    try:
        add_decision(connection)
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute(
                "UPDATE decisions SET state_id = 'changed' WHERE decision_id = 'decision-1'"
            )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute("DELETE FROM decisions WHERE decision_id = 'decision-1'")
    finally:
        connection.close()


def test_consistent_backup_contains_committed_episode(tmp_path) -> None:
    source = open_ledger(tmp_path / "alma.db")
    try:
        add_decision(source)
        backup_ledger(source, tmp_path / "backup.db")
    finally:
        source.close()

    backup = open_ledger(tmp_path / "backup.db")
    try:
        assert (
            reconstruct_episode(backup, "decision-1")["decision"]["state_id"]
            == "state-1"
        )
    finally:
        backup.close()


def test_failed_audit_rolls_back_request_and_intent(tmp_path) -> None:
    connection = open_ledger(tmp_path / "alma.db")
    try:
        add_decision(connection)
        append_audit_event(
            connection,
            event_id="audit:request-crash",
            actor="test",
            action="EXISTING",
            request_id=None,
            created_at="2026-07-31T08:00:00+00:00",
            before_summary="{}",
            after_summary="{}",
        )
        with pytest.raises(sqlite3.IntegrityError):
            record_intent_mutation(
                connection,
                audit_event_id="audit:request-crash",
                actor="brain",
                before_summary="{}",
                after_summary="{}",
                intent_id="intent-crash",
                decision_id="decision-1",
                request_id="request-crash",
                venue="BINANCE",
                symbol="BTCUSDT-PERP",
                state_id="state-1",
                created_at="2026-07-31T08:00:01+00:00",
                mode="TRADE",
                desired_quantity=Decimal(1),
                actual_quantity=Decimal(0),
                pending_quantity=Decimal(0),
                execution_delta=Decimal(1),
            )
        assert connection.execute(
            "SELECT count(*) FROM request_ids WHERE request_id = 'request-crash'"
        ).fetchone() == (0,)
        assert connection.execute(
            "SELECT count(*) FROM intents WHERE intent_id = 'intent-crash'"
        ).fetchone() == (0,)
    finally:
        connection.close()


def test_failed_fill_insert_rolls_back_order_transition(tmp_path) -> None:
    connection = open_ledger(tmp_path / "alma.db")
    try:
        add_decision(connection)
        add_intent(connection)
        append_order_event(
            connection,
            event_id="submitted",
            intent_id="intent-1",
            order_id="order-1",
            status="SUBMITTED",
            quantity=Decimal(1),
            filled_quantity=Decimal(0),
            price=Decimal(100),
            created_at="2026-07-31T08:00:02+00:00",
        )
        record_fill(
            connection,
            event_id="fill-event",
            order_event_id="partial",
            fill_id="fill-1",
            order_id="order-1",
            quantity=Decimal("0.4"),
            price=Decimal(100),
            fee=Decimal(0),
            fee_currency="USDT",
            slippage=Decimal(0),
            funding=Decimal(0),
            created_at="2026-07-31T08:00:03+00:00",
        )
        with pytest.raises(sqlite3.IntegrityError):
            record_fill(
                connection,
                event_id="fill-event",
                order_event_id="must-roll-back",
                fill_id="fill-2",
                order_id="order-1",
                quantity=Decimal("0.6"),
                price=Decimal(100),
                fee=Decimal(0),
                fee_currency="USDT",
                slippage=Decimal(0),
                funding=Decimal(0),
                created_at="2026-07-31T08:00:04+00:00",
            )
        assert connection.execute(
            "SELECT status, filled_quantity FROM order_events "
            "WHERE order_id = 'order-1' ORDER BY seq DESC LIMIT 1"
        ).fetchone() == ("PARTIALLY_FILLED", "0.4")
        assert connection.execute(
            "SELECT count(*) FROM order_events WHERE event_id = 'must-roll-back'"
        ).fetchone() == (0,)
    finally:
        connection.close()


def test_fill_locks_before_reading_latest_order(tmp_path) -> None:
    connection = open_ledger(tmp_path / "alma.db")
    statements: list[str] = []
    try:
        add_decision(connection)
        add_intent(connection)
        append_order_event(
            connection,
            event_id="submitted",
            intent_id="intent-1",
            order_id="order-1",
            status="SUBMITTED",
            quantity=Decimal(1),
            filled_quantity=Decimal(0),
            price=Decimal(100),
            created_at="2026-07-31T08:00:02+00:00",
        )
        connection.set_trace_callback(statements.append)
        record_fill(
            connection,
            event_id="fill-event",
            order_event_id="filled",
            fill_id="fill-1",
            order_id="order-1",
            quantity=Decimal(1),
            price=Decimal(100),
            fee=Decimal(0),
            fee_currency="USDT",
            slippage=Decimal(0),
            funding=Decimal(0),
            created_at="2026-07-31T08:00:03+00:00",
        )
        assert statements.index("BEGIN IMMEDIATE") < next(
            index
            for index, statement in enumerate(statements)
            if statement.startswith("SELECT intent_id, status")
        )
    finally:
        connection.close()
