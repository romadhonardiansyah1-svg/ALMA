import asyncio
import json
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from aiohttp.test_utils import TestClient, TestServer

from alma.execution import ExecutionRejected, OrderRequest
from alma.ledger import (
    append_decision,
    append_order_event,
    open_ledger,
    record_intent_mutation,
    reserve_order_submission,
)
from alma.mt5_bridge import (
    BRIDGE_VERSION,
    MT5BridgeRejected,
    MT5BridgeStore,
    MT5FileBridge,
    MT5Venue,
    create_mt5_bridge_app,
    parse_snapshot,
)

NOW = datetime(2026, 7, 31, 13, 49, 39, tzinfo=UTC)


def bridge_store(connection) -> MT5BridgeStore:
    return MT5BridgeStore(
        connection,
        expected_account_mode="DEMO",
        expected_login="123456",
        expected_server="Broker-Demo",
        expected_symbol="XAUUSD",
        clock=lambda: NOW,
    )


def snapshot(*, seq: int = 1, nonce: str = "nonce-1") -> dict[str, object]:
    return {
        "version": BRIDGE_VERSION,
        "type": "snapshot",
        "terminal_id": "terminal-1",
        "session_id": "session-1",
        "seq": seq,
        "nonce": nonce,
        "timestamp": NOW.isoformat(),
        "terminal": {
            "connected": True,
            "trade_allowed": True,
            "account_trade_allowed": True,
            "account_mode": "DEMO",
            "margin_mode": "HEDGING",
            "server": "Broker-Demo",
            "build": 5000,
        },
        "account": {
            "login": "123456",
            "balance": "10000",
            "equity": "10000",
            "margin": "0",
            "free_margin": "10000",
            "leverage": 100,
            "currency": "USD",
        },
        "symbol": {
            "name": "XAUUSD",
            "digits": 2,
            "point": "0.01",
            "tick_size": "0.01",
            "tick_value": "1",
            "contract_size": "100",
            "volume_min": "0.01",
            "volume_max": "100",
            "volume_step": "0.01",
            "stops_level": 10,
            "bid": "3285.10",
            "ask": "3285.20",
            "margin_buy_per_lot": "3285.20",
            "margin_sell_per_lot": "3285.10",
        },
        "positions": [],
        "orders": [],
        "events": [],
        "deals": [],
    }


def request() -> OrderRequest:
    return OrderRequest(
        client_order_id="alma-order-1",
        intent_id="intent-1",
        symbol="XAUUSD",
        side="BUY",
        quantity=Decimal("0.10"),
        order_type="LIMIT",
        price=Decimal("3285.10"),
        trigger_price=None,
        reduce_only=False,
        expires_at=NOW + timedelta(seconds=30),
        stop_loss=Decimal("3280.00"),
        take_profits=((Decimal("3295.00"), Decimal(1)),),
    )


def reserve_entry(connection, order_id: str = "alma-order-1") -> None:
    append_decision(
        connection,
        decision_id="decision-1",
        state_id="state-1",
        created_at=NOW.isoformat(),
        raw_contract=b"{}",
        validation_result="ACCEPTED",
        model_id="model",
        prompt_hash="prompt",
        policy_hash="policy",
        code_hash="code",
    )
    assert record_intent_mutation(
        connection,
        audit_event_id="audit:intent-1",
        actor="test",
        before_summary="{}",
        after_summary="{}",
        intent_id="intent-1",
        decision_id="decision-1",
        request_id="intent-request-1",
        venue="MT5",
        symbol="XAUUSD",
        state_id="state-1",
        created_at=NOW.isoformat(),
        mode="TRADE",
        desired_quantity=Decimal("0.10"),
        actual_quantity=Decimal(0),
        pending_quantity=Decimal(0),
        execution_delta=Decimal("0.10"),
    )
    assert reserve_order_submission(
        connection,
        event_id=f"submitted:{order_id}",
        intent_id="intent-1",
        order_id=order_id,
        quantity=Decimal("0.10"),
        price=Decimal("3285.10"),
        created_at=NOW.isoformat(),
    )


def test_parser_accepts_configurable_account_and_position_modes() -> None:
    assert parse_snapshot(snapshot())["terminal"]["account_mode"] == "DEMO"

    live = snapshot()
    live["terminal"]["account_mode"] = "REAL"  # type: ignore[index]
    assert parse_snapshot(live)["terminal"]["account_mode"] == "REAL"

    invalid = snapshot()
    invalid["terminal"]["account_mode"] = "CONTEST"  # type: ignore[index]
    with pytest.raises(MT5BridgeRejected, match="ACCOUNT_MODE_INVALID"):
        parse_snapshot(invalid)

    netting = snapshot()
    netting["terminal"]["margin_mode"] = "NETTING"  # type: ignore[index]
    assert parse_snapshot(netting)["terminal"]["margin_mode"] == "NETTING"

    invalid_position_mode = snapshot()
    invalid_position_mode["terminal"]["margin_mode"] = "EXCHANGE"  # type: ignore[index]
    with pytest.raises(MT5BridgeRejected, match="POSITION_MODE_INVALID"):
        parse_snapshot(invalid_position_mode)

    extra = snapshot()
    extra["credential"] = "must-never-be-accepted"
    with pytest.raises(MT5BridgeRejected, match="INVALID_SNAPSHOT_SCHEMA"):
        parse_snapshot(extra)


def test_store_rejects_wrong_account_identity_before_ledger_write(tmp_path) -> None:
    connection = open_ledger(tmp_path / "alma.db")
    store = bridge_store(connection)
    wrong = snapshot()
    wrong["account"]["login"] = "654321"  # type: ignore[index]
    try:
        with pytest.raises(MT5BridgeRejected, match="ACCOUNT_IDENTITY_MISMATCH"):
            store.ingest(wrong)
        assert connection.execute("SELECT count(*) FROM mt5_snapshots").fetchone() == (
            0,
        )
    finally:
        connection.close()


def test_real_config_accepts_only_matching_real_account_before_ledger(tmp_path) -> None:
    connection = open_ledger(tmp_path / "alma.db")
    store = MT5BridgeStore(
        connection,
        expected_account_mode="REAL",
        expected_login="987654",
        expected_server="Broker-Live",
        expected_symbol="XAUUSD.r",
        clock=lambda: NOW,
    )
    demo = snapshot()
    demo["account"]["login"] = "987654"  # type: ignore[index]
    demo["terminal"]["server"] = "Broker-Live"  # type: ignore[index]
    demo["symbol"]["name"] = "XAUUSD.r"  # type: ignore[index]
    real = deepcopy(demo)
    real["terminal"]["account_mode"] = "REAL"  # type: ignore[index]
    try:
        with pytest.raises(MT5BridgeRejected, match="ACCOUNT_IDENTITY_MISMATCH"):
            store.ingest(demo)
        assert connection.execute("SELECT count(*) FROM mt5_snapshots").fetchone() == (
            0,
        )
        assert store.ingest(real)
        assert connection.execute("SELECT count(*) FROM mt5_snapshots").fetchone() == (
            1,
        )
    finally:
        connection.close()


def test_account_switch_hides_old_truth_and_rejects_old_outbox(tmp_path) -> None:
    path = tmp_path / "alma.db"
    connection = open_ledger(path)
    demo_store = bridge_store(connection)
    demo_store.ingest(snapshot())
    assert demo_store.queue_command(
        "terminal-1", "old-account-command", "sync_request", {"full": True}
    )
    connection.close()

    connection = open_ledger(path)
    real_store = MT5BridgeStore(
        connection,
        expected_account_mode="REAL",
        expected_login="987654",
        expected_server="Broker-Live",
        expected_symbol="XAUUSD.r",
        clock=lambda: NOW,
    )
    real = snapshot(nonce="real-account-nonce")
    real["session_id"] = "real-account-session"
    real["terminal"]["account_mode"] = "REAL"  # type: ignore[index]
    real["terminal"]["server"] = "Broker-Live"  # type: ignore[index]
    real["account"]["login"] = "987654"  # type: ignore[index]
    real["symbol"]["name"] = "XAUUSD.r"  # type: ignore[index]
    try:
        assert real_store.latest("terminal-1") is None
        assert real_store.next_command("terminal-1") is None

        not_reset = deepcopy(real)
        not_reset["seq"] = 2
        with pytest.raises(MT5BridgeRejected, match="ACCOUNT_SWITCH_REQUIRES_SEQ_1"):
            real_store.ingest(not_reset)

        real_store.ingest(real)
        assert real_store.latest("terminal-1")["account"]["login"] == "987654"  # type: ignore[index]
        assert real_store.next_command("terminal-1") is None
        assert connection.execute(
            "SELECT status, ack_payload FROM mt5_commands WHERE request_id = ?",
            ("old-account-command",),
        ).fetchone() == ("REJECTED", '{"error":"ACCOUNT_CONFIG_CHANGED"}')
    finally:
        connection.close()


def test_store_requires_valid_expected_account_identity(tmp_path) -> None:
    connection = open_ledger(tmp_path / "alma.db")
    try:
        with pytest.raises(ValueError, match="identity"):
            MT5BridgeStore(
                connection,
                expected_account_mode="DEMO",
                expected_login="not-numeric",
                expected_server="Broker-Demo",
                expected_symbol="XAUUSD",
            )
    finally:
        connection.close()


def test_position_mode_auto_accepts_both_and_explicit_pin_rejects_mismatch(
    tmp_path,
) -> None:
    netting = snapshot()
    netting["terminal"]["margin_mode"] = "NETTING"  # type: ignore[index]
    connection = open_ledger(tmp_path / "auto.db")
    auto = MT5BridgeStore(
        connection,
        expected_account_mode="DEMO",
        expected_position_mode="AUTO",
        expected_login="123456",
        expected_server="Broker-Demo",
        expected_symbol="XAUUSD",
        clock=lambda: NOW,
    )
    try:
        assert auto.ingest(netting)
        assert MT5Venue(auto, "terminal-1", "XAUUSD").truth("XAUUSD").connected
    finally:
        connection.close()

    connection = open_ledger(tmp_path / "pinned.db")
    hedging = MT5BridgeStore(
        connection,
        expected_account_mode="DEMO",
        expected_position_mode="HEDGING",
        expected_login="123456",
        expected_server="Broker-Demo",
        expected_symbol="XAUUSD",
        clock=lambda: NOW,
    )
    try:
        with pytest.raises(MT5BridgeRejected, match="ACCOUNT_IDENTITY_MISMATCH"):
            hedging.ingest(netting)
        assert connection.execute("SELECT count(*) FROM mt5_snapshots").fetchone() == (
            0,
        )
    finally:
        connection.close()


def test_sequence_replay_and_restart_are_durable(tmp_path) -> None:
    path = tmp_path / "alma.db"
    connection = open_ledger(path)
    store = bridge_store(connection)
    state_id = store.ingest(snapshot())
    assert store.ingest(snapshot()) == state_id
    connection.close()

    reopened = open_ledger(path)
    store = bridge_store(reopened)
    try:
        # ponytail: forward seq jumps are accepted (dedup is on exact replay, not seq gaps)
        gap = snapshot(seq=3, nonce="nonce-3")
        store.ingest(gap)
        assert store.latest("terminal-1")["seq"] == 3  # type: ignore[index]

        second = snapshot(seq=2, nonce="nonce-2")
        store.ingest(second)
        assert store.latest("terminal-1")["seq"] == 2  # type: ignore[index]
        replay = snapshot(seq=1, nonce="nonce-1")
        replay["session_id"] = "session-2"
        with pytest.raises(MT5BridgeRejected, match="REPLAY_REJECTED"):
            store.ingest(replay)
    finally:
        reopened.close()


def test_command_outbox_is_idempotent_and_ack_is_not_venue_truth(tmp_path) -> None:
    connection = open_ledger(tmp_path / "alma.db")
    store = bridge_store(connection)
    try:
        store.ingest(snapshot())
        assert store.queue_command(
            "terminal-1", "request-1", "sync_request", {"full": True}
        )
        assert not store.queue_command(
            "terminal-1", "request-1", "sync_request", {"full": True}
        )
        with pytest.raises(MT5BridgeRejected, match="COMMAND_ID_CONFLICT"):
            store.queue_command(
                "terminal-1", "request-1", "sync_request", {"full": False}
            )

        command = store.next_command("terminal-1")
        assert command == {
            "request_id": "request-1",
            "type": "sync_request",
            "payload": {"full": True},
        }
        assert store.next_command("terminal-1") == command
        store.acknowledge("request-1", True, {"accepted": True})
        assert store.next_command("terminal-1") is None
        assert MT5Venue(store, "terminal-1", "XAUUSD").find_order("request-1") is None
    finally:
        connection.close()


def test_rejected_order_is_observable_and_zero_price_does_not_poison_snapshot(
    tmp_path,
) -> None:
    connection = open_ledger(tmp_path / "alma.db")
    store = bridge_store(connection)
    venue = MT5Venue(store, "terminal-1", "XAUUSD")
    try:
        store.ingest(snapshot())
        venue.submit(request())
        assert store.next_command("terminal-1")["request_id"] == "alma-order-1"  # type: ignore[index]
        store.acknowledge("alma-order-1", False, {"retcode": "10018"})
        acknowledged = venue.find_order("alma-order-1")
        assert acknowledged is not None and acknowledged.status == "REJECTED"

        rejected = snapshot(seq=2, nonce="nonce-2")
        rejected["events"] = [
            {
                "event_id": "event-rejected-1",
                "request_id": "alma-order-1",
                "status": "REJECTED",
                "ticket": "0",
                "volume": "0.10",
                "filled_volume": "0",
                "price": "0",
                "reason": "Market closed",
                "timestamp": NOW.isoformat(),
            }
        ]
        store.ingest(rejected)

        observed = venue.find_order("alma-order-1")
        assert observed is not None
        assert observed.status == "REJECTED"
        assert observed.filled_quantity == 0
        assert store.latest("terminal-1")["seq"] == 2  # type: ignore[index]
    finally:
        connection.close()


def test_submit_rejects_expiry_and_fractional_targets_before_outbox(tmp_path) -> None:
    connection = open_ledger(tmp_path / "alma.db")
    store = bridge_store(connection)
    venue = MT5Venue(store, "terminal-1", "XAUUSD")
    try:
        store.ingest(snapshot())
        with pytest.raises(ExecutionRejected, match="ORDER_EXPIRED"):
            venue.submit(replace(request(), expires_at=NOW - timedelta(seconds=1)))
        with pytest.raises(ExecutionRejected, match="MT5_SINGLE_FULL_TP_REQUIRED"):
            venue.submit(
                replace(
                    request(),
                    take_profits=(
                        (Decimal(3290), Decimal("0.4")),
                        (Decimal(3295), Decimal("0.6")),
                    ),
                )
            )
        assert store.next_command("terminal-1") is None
    finally:
        connection.close()


def test_reduce_command_is_filled_only_after_broker_truth_changes(tmp_path) -> None:
    connection = open_ledger(tmp_path / "alma.db")
    reserve_entry(connection, "alma-entry")
    store = MT5BridgeStore(
        connection,
        expected_account_mode="DEMO",
        expected_login="123456",
        expected_server="Broker-Demo",
        expected_symbol="XAUUSD",
        expected_position_mode="NETTING",
        clock=lambda: NOW,
    )
    venue = MT5Venue(store, "terminal-1", "XAUUSD")
    state = snapshot()
    state["terminal"]["margin_mode"] = "NETTING"  # type: ignore[index]
    state["positions"] = [
        {
            "ticket": "601",
            "root_id": "alma-entry",
            "symbol": "XAUUSD",
            "side": "BUY",
            "volume": "0.10",
            "price_open": "3285.10",
            "sl": "3280.00",
            "tp": "3295.00",
            "magic": 260731,
        }
    ]
    reduce = replace(
        request(),
        side="SELL",
        quantity=Decimal("0.04"),
        reduce_only=True,
        stop_loss=Decimal(0),
        take_profits=(),
    )
    try:
        store.ingest(state)
        assert venue.submit(reduce) is None
        command = store.next_command("terminal-1")
        assert command is not None and command["type"] == "reduce_position"
        assert command["payload"]["expected_actual"] == "0.06"
        store.acknowledge("alma-order-1", True, {"retcode": "10009"})
        assert venue.find_order("alma-order-1") is None

        reduced = snapshot(seq=2, nonce="nonce-2")
        reduced["terminal"]["margin_mode"] = "NETTING"  # type: ignore[index]
        reduced["positions"] = [{**state["positions"][0], "volume": "0.06"}]
        store.ingest(reduced)
        observed = venue.find_order("alma-order-1")
        assert observed is not None and observed.status == "FILLED"
        assert observed.filled_quantity == Decimal("0.04")
    finally:
        connection.close()


def test_netting_rejects_second_root_before_outbox(tmp_path) -> None:
    connection = open_ledger(tmp_path / "alma.db")
    reserve_entry(connection, "alma-first-root")
    store = MT5BridgeStore(
        connection,
        expected_account_mode="DEMO",
        expected_position_mode="AUTO",
        expected_login="123456",
        expected_server="Broker-Demo",
        expected_symbol="XAUUSD",
        clock=lambda: NOW,
    )
    venue = MT5Venue(store, "terminal-1", "XAUUSD")
    state = snapshot()
    state["terminal"]["margin_mode"] = "NETTING"  # type: ignore[index]
    state["positions"] = [
        {
            "ticket": "601",
            "root_id": "alma-first-root",
            "symbol": "XAUUSD",
            "side": "BUY",
            "volume": "0.10",
            "price_open": "3285.10",
            "sl": "3280.00",
            "tp": "3295.00",
            "magic": 260731,
        }
    ]
    try:
        store.ingest(state)
        with pytest.raises(ExecutionRejected, match="MT5_NETTING_SYMBOL_BUSY"):
            venue.submit(request())
        assert store.next_command("terminal-1") is None
    finally:
        connection.close()


def test_netting_rejects_second_pending_root_before_next_snapshot(tmp_path) -> None:
    connection = open_ledger(tmp_path / "alma.db")
    store = MT5BridgeStore(
        connection,
        expected_account_mode="DEMO",
        expected_position_mode="AUTO",
        expected_login="123456",
        expected_server="Broker-Demo",
        expected_symbol="XAUUSD",
        clock=lambda: NOW,
    )
    venue = MT5Venue(store, "terminal-1", "XAUUSD")
    state = snapshot()
    state["terminal"]["margin_mode"] = "NETTING"  # type: ignore[index]
    try:
        store.ingest(state)
        venue.submit(request())
        with pytest.raises(ExecutionRejected, match="MT5_NETTING_SYMBOL_BUSY"):
            venue.submit(replace(request(), client_order_id="alma-order-2"))
        assert connection.execute(
            "SELECT request_id FROM mt5_commands WHERE kind='place_order'"
        ).fetchall() == [("alma-order-1",)]
    finally:
        connection.close()


def test_venue_uses_snapshot_events_deals_and_resident_protection(tmp_path) -> None:
    connection = open_ledger(tmp_path / "alma.db")
    reserve_entry(connection)
    store = bridge_store(connection)
    venue = MT5Venue(store, "terminal-1", "XAUUSD")
    try:
        store.ingest(snapshot())
        truth = venue.truth("XAUUSD")
        assert truth.connected and truth.actual_quantity == truth.pending_quantity == 0
        assert truth.available_margin == Decimal(10000)
        assert venue.rules("XAUUSD").minimum_stop_distance == Decimal("0.10")
        assert venue.required_margin(request()) == Decimal("328.520")

        assert venue.submit(request()) is None
        assert venue.find_order("alma-order-1") is None
        assert store.next_command("terminal-1")["request_id"] == "alma-order-1"  # type: ignore[index]
        store.acknowledge("alma-order-1", True, {"retcode": "PLACED"})
        assert venue.find_order("alma-order-1") is None

        accepted = snapshot(seq=2, nonce="nonce-2")
        accepted["orders"] = [
            {
                "ticket": "501",
                "root_id": "alma-order-1",
                "symbol": "XAUUSD",
                "side": "BUY",
                "order_type": "LIMIT",
                "volume": "0.10",
                "filled_volume": "0",
                "price": "3285.10",
                "status": "ACCEPTED",
                "sl": "3280.00",
                "tp": "3295.00",
                "magic": 260731,
            }
        ]
        accepted["events"] = [
            {
                "event_id": "event-accepted-1",
                "request_id": "alma-order-1",
                "status": "ACCEPTED",
                "ticket": "501",
                "volume": "0.10",
                "filled_volume": "0",
                "price": "3285.10",
                "reason": "",
                "timestamp": NOW.isoformat(),
            }
        ]
        store.ingest(accepted)
        observed = venue.find_order("alma-order-1")
        assert observed is not None and observed.status == "ACCEPTED"

        filled = snapshot(seq=3, nonce="nonce-3")
        filled["positions"] = [
            {
                "ticket": "601",
                "root_id": "alma-order-1",
                "symbol": "XAUUSD",
                "side": "BUY",
                "volume": "0.10",
                "price_open": "3285.10",
                "sl": "3280.00",
                "tp": "3295.00",
                "magic": 260731,
            },
        ]
        filled["deals"] = [
            {
                "deal_id": "701",
                "root_id": "alma-order-1",
                "side": "BUY",
                "entry_kind": "IN",
                "volume": "0.04",
                "price": "3285.10",
                "fee": "-0.20",
                "timestamp": NOW.isoformat(),
            },
            {
                "deal_id": "702",
                "root_id": "alma-order-1",
                "side": "BUY",
                "entry_kind": "IN",
                "volume": "0.06",
                "price": "3285.10",
                "fee": "-0.30",
                "timestamp": NOW.isoformat(),
            },
        ]
        store.ingest(filled)
        observed = venue.find_order("alma-order-1")
        assert observed is not None and observed.status == "FILLED"
        assert observed.filled_quantity == Decimal("0.10")
        assert connection.execute(
            "SELECT fill_id, quantity, fee_currency FROM fill_events ORDER BY seq"
        ).fetchall() == [
            ("MT5:701", "0.04", "USD"),
            ("MT5:702", "0.06", "USD"),
        ]
        assert connection.execute(
            "SELECT status, filled_quantity FROM order_events "
            "WHERE order_id = 'alma-order-1' ORDER BY seq"
        ).fetchall() == [
            ("SUBMITTED", "0"),
            ("PARTIALLY_FILLED", "0.04"),
            ("FILLED", "0.10"),
        ]
        protection = venue.protection("alma-order-1")
        assert [(item.kind, item.price, item.quantity) for item in protection] == [
            ("STOP_LOSS", Decimal("3280.00"), Decimal("0.10")),
            ("TAKE_PROFIT", Decimal("3295.00"), Decimal("0.10")),
        ]
        assert venue.ensure_position_protected("XAUUSD")
        assert venue.truth("XAUUSD").actual_quantity == Decimal("0.10")
    finally:
        connection.close()


def test_find_order_uses_ingestion_order_when_event_timestamps_tie(tmp_path) -> None:
    connection = open_ledger(tmp_path / "alma.db")
    reserve_entry(connection)
    store = bridge_store(connection)
    venue = MT5Venue(store, "terminal-1", "XAUUSD")
    try:
        store.ingest(snapshot())
        venue.submit(request())
        for seq, event_id, status in (
            (2, "999-old", "ACCEPTED"),
            (3, "1000-new", "CANCELED"),
        ):
            state = snapshot(seq=seq, nonce=f"nonce-{seq}")
            state["events"] = [
                {
                    "event_id": event_id,
                    "request_id": "alma-order-1",
                    "status": status,
                    "ticket": "501",
                    "volume": "0.10",
                    "filled_volume": "0",
                    "price": "3285.10" if status == "ACCEPTED" else "0",
                    "reason": "",
                    "timestamp": NOW.isoformat(),
                }
            ]
            store.ingest(state)

        observed = venue.find_order("alma-order-1")
        assert observed is not None and observed.status == "CANCELED"
        assert observed.event_id == "1000-new"
    finally:
        connection.close()


def test_out_deal_maps_to_exact_pending_reduction(tmp_path) -> None:
    connection = open_ledger(tmp_path / "alma.db")
    reserve_entry(connection)
    append_order_event(
        connection,
        event_id="filled:entry",
        intent_id="intent-1",
        order_id="alma-order-1",
        status="FILLED",
        quantity=Decimal("0.10"),
        filled_quantity=Decimal("0.10"),
        price=Decimal("3285.10"),
        created_at=NOW.isoformat(),
    )
    append_decision(
        connection,
        decision_id="decision-reduce",
        state_id="state-reduce",
        created_at=NOW.isoformat(),
        raw_contract=b"{}",
        validation_result="ACCEPTED",
        model_id="model",
        prompt_hash="prompt",
        policy_hash="policy",
        code_hash="code",
    )
    assert record_intent_mutation(
        connection,
        audit_event_id="audit:reduce",
        actor="test",
        before_summary="{}",
        after_summary="{}",
        intent_id="intent-reduce",
        decision_id="decision-reduce",
        request_id="request-reduce",
        venue="MT5",
        symbol="XAUUSD",
        state_id="state-reduce",
        created_at=NOW.isoformat(),
        mode="TRADE",
        desired_quantity=Decimal("0.06"),
        actual_quantity=Decimal("0.10"),
        pending_quantity=Decimal(0),
        execution_delta=Decimal("-0.04"),
    )
    assert reserve_order_submission(
        connection,
        event_id="submitted:reduce",
        intent_id="intent-reduce",
        order_id="alma-reduce",
        quantity=Decimal("0.04"),
        price=Decimal("3285.10"),
        created_at=NOW.isoformat(),
    )
    store = bridge_store(connection)

    assert (
        store._deal_order_id(
            {
                "deal_id": "exit-1",
                "root_id": "alma-order-1",
                "side": "SELL",
                "entry_kind": "OUT",
                "volume": "0.04",
                "price": "3285.00",
                "fee": "-0.10",
                "timestamp": NOW.isoformat(),
            }
        )
        == "alma-reduce"
    )
    connection.close()


def test_loopback_api_requires_bearer_and_rejects_duplicate_nonce(tmp_path) -> None:
    async def run() -> None:
        connection = open_ledger(tmp_path / "alma.db")
        store = bridge_store(connection)
        app = create_mt5_bridge_app(store, "s" * 32)
        client = TestClient(TestServer(app))
        await client.start_server()
        try:
            unauthorized = await client.post("/v1/snapshot", json=snapshot())
            assert unauthorized.status == 401
            assert (await client.get("/v1/config")).status == 401
            headers = {"Authorization": f"Bearer {'s' * 32}"}
            config = await client.get("/v1/config", headers=headers)
            assert config.status == 200
            assert await config.json() == {
                "account_mode": "DEMO",
                "position_mode": "HEDGING",
                "login": "123456",
                "server": "Broker-Demo",
                "symbol": "XAUUSD",
            }
            accepted = await client.post(
                "/v1/snapshot", json=snapshot(), headers=headers
            )
            assert accepted.status == 200
            assert (await accepted.json())["accepted"] is True

            conflict = deepcopy(snapshot())
            conflict["account"]["balance"] = "9999"  # type: ignore[index]
            rejected = await client.post("/v1/snapshot", json=conflict, headers=headers)
            assert rejected.status == 409
            assert (await rejected.json())["error"] == "REPLAY_REJECTED"
        finally:
            await client.close()
            connection.close()

    asyncio.run(run())


def test_foreign_exposure_blocks_mutation_without_being_closed(tmp_path) -> None:
    connection = open_ledger(tmp_path / "alma.db")
    store = bridge_store(connection)
    venue = MT5Venue(store, "terminal-1", "XAUUSD")
    state = snapshot()
    state["positions"] = [
        {
            "ticket": "900",
            "root_id": "alma-forged-900",
            "symbol": "XAUUSD",
            "side": "BUY",
            "volume": "0.01",
            "price_open": "3285.10",
            "sl": "0",
            "tp": "0",
            "magic": 42,
        }
    ]
    state["orders"] = [
        {
            "ticket": "901",
            "root_id": "alma-forged-901",
            "symbol": "XAUUSD",
            "side": "BUY",
            "order_type": "LIMIT",
            "volume": "0.01",
            "filled_volume": "0",
            "price": "3285.10",
            "status": "ACCEPTED",
            "sl": "3280.00",
            "tp": "3295.00",
            "magic": 260731,
        }
    ]
    try:
        store.ingest(state)
        assert not venue.truth("XAUUSD").connected
        assert not venue.ensure_position_protected("XAUUSD")
        with pytest.raises(ExecutionRejected, match="MT5_NOT_READY"):
            venue.submit(request())
        with pytest.raises(ExecutionRejected, match="FOREIGN_EXPOSURE"):
            venue.flatten_symbol("XAUUSD")
        assert venue.cancel_open_entries("XAUUSD") == ()
        assert store.next_command("terminal-1") is None
    finally:
        connection.close()


def test_file_bridge_round_trip_is_atomic_and_keeps_store_validation(tmp_path) -> None:
    connection = open_ledger(tmp_path / "alma.db")
    store = bridge_store(connection)
    ipc = MT5FileBridge(store, tmp_path / "ipc", "terminal-1")
    try:
        ipc.prepare()
        assert json.loads((tmp_path / "ipc/config.json").read_text()) == {
            "account_mode": "DEMO",
            "position_mode": "HEDGING",
            "login": "123456",
            "server": "Broker-Demo",
            "symbol": "XAUUSD",
        }

        (tmp_path / "ipc/snapshot.json").write_text(json.dumps(snapshot()))
        ipc.tick()
        assert json.loads((tmp_path / "ipc/snapshot_ack.json").read_text()) == {
            "accepted": True,
            "session_id": "session-1",
            "seq": 1,
        }
        assert store.latest("terminal-1") is not None

        store.queue_command("terminal-1", "alma-order-1", "sync_request", {})
        ipc.tick()
        command = json.loads((tmp_path / "ipc/command.json").read_text())
        assert command["request_id"] == "alma-order-1"

        (tmp_path / "ipc/ack.json").write_text(
            json.dumps(
                {
                    "request_id": "alma-order-1",
                    "accepted": True,
                    "result": {"retcode": "10009"},
                }
            )
        )
        ipc.tick()
        assert not (tmp_path / "ipc/ack.json").exists()
        assert not (tmp_path / "ipc/command.json").exists()
        assert connection.execute(
            "SELECT status FROM mt5_commands WHERE request_id='alma-order-1'"
        ).fetchone() == ("ACKED",)
    finally:
        connection.close()
