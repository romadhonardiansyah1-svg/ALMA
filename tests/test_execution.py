import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest
from nautilus_trader.model.enums import TimeInForce

from alma.binance_native_execution import ENTRY_TIME_IN_FORCE, _record_entry_rejection
from alma.emergency_stop import execute_emergency_stop
from alma.execution import (
    ExecutionRejected,
    ExecutionStatus,
    ExecutionTruth,
    InstrumentRules,
    OrderRequest,
    ProtectedSubmission,
    ProtectionOrder,
    TacticalExecutor,
    VenueOrder,
    client_order_id,
)
from alma.ledger import (
    append_decision,
    open_ledger,
    record_intent_mutation,
    reserve_order_submission,
)
from alma.mutation_gate import MutationGate
from alma.venue_mode_store import initialize_venue_mode
from alma.venue_modes import OpenPositionPolicy, VenueMode

NOW = datetime(2026, 7, 31, 12, tzinfo=UTC)


def contract(*, partial_policy: str = "KEEP_REMAINDER") -> bytes:
    return json.dumps(
        {
            "policy_version": "alma-v1",
            "state_id": "state-1",
            "decision_id": "decision-1",
            "created_at": NOW.isoformat(),
            "venue": "BINANCE",
            "symbol": "BTCUSDT-PERP",
            "action": "OPEN_LONG",
            "target": {"side": "LONG", "volume": "1"},
            "entry": {
                "mode": "PASSIVE",
                "preferred_low": "100.0",
                "preferred_high": "101.0",
                "max_acceptable_price": "102.0",
                "ttl_seconds": 60,
                "on_missed": "ABORT",
                "on_partial_fill": partial_policy,
            },
            "invalidation_price": "95.0",
            "targets": [{"price": "110.0", "close_fraction": "1"}],
            "review_triggers": [],
            "evidence": ["test"],
            "uncertainty": "0.1",
        }
    ).encode()


def ledger(
    path,
    mode: VenueMode = VenueMode.TRADE,
    *,
    partial_policy: str = "KEEP_REMAINDER",
    raw_contract: bytes | None = None,
    quantity: Decimal = Decimal(1),
):
    connection = open_ledger(path)
    initialize_venue_mode(connection, "BINANCE", mode)
    append_decision(
        connection,
        decision_id="decision-1",
        state_id="state-1",
        created_at=NOW.isoformat(),
        raw_contract=raw_contract or contract(partial_policy=partial_policy),
        validation_result="ACCEPTED",
        model_id="model",
        prompt_hash="prompt",
        policy_hash="policy",
        code_hash="code",
    )
    assert record_intent_mutation(
        connection,
        audit_event_id="audit:intent-request",
        actor="test",
        before_summary="{}",
        after_summary="{}",
        intent_id="intent-1",
        decision_id="decision-1",
        request_id="intent-request",
        venue="BINANCE",
        symbol="BTCUSDT-PERP",
        state_id="state-1",
        created_at=NOW.isoformat(),
        mode="TRADE",
        desired_quantity=quantity,
        actual_quantity=Decimal(0),
        pending_quantity=Decimal(0),
        execution_delta=quantity,
    )
    return connection


def test_submission_reservation_has_one_winner_across_connections(tmp_path) -> None:
    path = tmp_path / "alma.db"
    connection = ledger(path)
    connection.close()
    order_id = client_order_id("intent-1")

    def reserve() -> bool:
        worker = open_ledger(path)
        try:
            return reserve_order_submission(
                worker,
                event_id=f"submitted:{order_id}",
                intent_id="intent-1",
                order_id=order_id,
                quantity=Decimal(1),
                price=Decimal(100),
                created_at=NOW.isoformat(),
            )
        finally:
            worker.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(pool.map(lambda _: reserve(), range(2)))

    assert sorted(results) == [False, True]
    verification = open_ledger(path)
    try:
        assert verification.execute(
            "SELECT status FROM order_events WHERE order_id = ?", (order_id,)
        ).fetchall() == [("SUBMITTED",)]
    finally:
        verification.close()


class FakeVenue:
    def __init__(self) -> None:
        self.current_truth = ExecutionTruth(
            NOW,
            True,
            Decimal(0),
            Decimal(0),
            Decimal(100),
            Decimal("100.1"),
            Decimal(1_000),
            "venue-state-1",
        )
        self.current_rules = InstrumentRules(
            quantity_step=Decimal("0.001"),
            quantity_min=Decimal("0.001"),
            quantity_max=Decimal(100),
            tick_size=Decimal("0.1"),
            price_min=Decimal(1),
            price_max=Decimal(1_000_000),
            minimum_stop_distance=Decimal("0.1"),
        )
        self.orders: dict[str, VenueOrder] = {}
        self.protections: dict[str, tuple[ProtectionOrder, ...]] = {}
        self.submissions: list[OrderRequest] = []
        self.timeout_after_accept = False
        self.async_submit = False
        self.cancel_timeout = False
        self.cancellations: list[tuple[str, str]] = []
        self.confirm_protection = True
        self.confirm_emergency_flatten = True
        self.fill_on_submit = Decimal(0)
        self.truth_calls = 0
        self.truth_after_first: ExecutionTruth | None = None
        self.truth_sequence: list[ExecutionTruth] = []

    def truth(self, symbol: str) -> ExecutionTruth:
        assert symbol == "BTCUSDT-PERP"
        self.truth_calls += 1
        if self.truth_sequence:
            return self.truth_sequence.pop(0)
        if self.truth_calls > 1 and self.truth_after_first is not None:
            return self.truth_after_first
        return self.current_truth

    def rules(self, symbol: str) -> InstrumentRules:
        assert symbol == "BTCUSDT-PERP"
        return self.current_rules

    def find_order(self, order_id: str) -> VenueOrder | None:
        return self.orders.get(order_id)

    def protection(self, client_order_id: str) -> tuple[ProtectionOrder, ...]:
        protection = self.protections.get(client_order_id, ())
        order = self.orders.get(client_order_id)
        if order is None or order.filled_quantity == 0:
            return protection
        scale = order.filled_quantity / order.quantity
        return tuple(
            replace(child, quantity=child.quantity * scale) for child in protection
        )

    def required_margin(self, request: OrderRequest) -> Decimal:
        return request.quantity * Decimal(10)

    def submit(self, request: OrderRequest) -> ProtectedSubmission | None:
        self.submissions.append(request)
        order = VenueOrder(
            client_order_id=request.client_order_id,
            status="ACCEPTED",
            quantity=request.quantity,
            filled_quantity=self.fill_on_submit,
            price=request.price,
            event_id=f"accepted:{request.client_order_id}",
        )
        self.orders[request.client_order_id] = order
        protection = ()
        if self.confirm_protection:
            protection = (
                ProtectionOrder(
                    order_id=f"sl:{request.client_order_id}",
                    kind="STOP_LOSS",
                    price=request.stop_loss,
                    quantity=request.quantity,
                    status="ACCEPTED",
                    venue_resident=True,
                ),
                *(
                    ProtectionOrder(
                        order_id=f"tp:{request.client_order_id}:{index}",
                        kind="TAKE_PROFIT",
                        price=price,
                        quantity=request.quantity * fraction,
                        status="ACCEPTED",
                        venue_resident=True,
                    )
                    for index, (price, fraction) in enumerate(request.take_profits)
                ),
            )
        self.protections[request.client_order_id] = protection
        if self.async_submit:
            return None
        if self.timeout_after_accept:
            self.timeout_after_accept = False
            raise TimeoutError
        return ProtectedSubmission(order, protection)

    def cancel(self, client_order_id: str, request_id: str) -> VenueOrder:
        self.cancellations.append((client_order_id, request_id))
        if self.cancel_timeout:
            raise TimeoutError
        order = self.orders[client_order_id]
        canceled = replace(
            order,
            status="CANCELED",
            event_id=f"canceled:{client_order_id}",
        )
        self.orders[client_order_id] = canceled
        return canceled

    def emergency_flatten(self, entry: VenueOrder) -> ExecutionTruth:
        if not self.confirm_emergency_flatten:
            return replace(
                self.current_truth,
                actual_quantity=max(entry.filled_quantity, Decimal("0.1")),
                pending_quantity=Decimal(0),
                state_id="venue-state-emergency-failed",
            )
        self.current_truth = replace(
            self.current_truth,
            actual_quantity=Decimal(0),
            pending_quantity=Decimal(0),
            state_id="venue-state-emergency-flat",
        )
        return self.current_truth

    def cancel_open_entries(self, symbol: str) -> tuple[VenueOrder, ...]:
        assert symbol == "BTCUSDT-PERP"
        canceled = []
        for order_id, order in tuple(self.orders.items()):
            if order.status in {"ACCEPTED", "SUBMITTED", "PARTIALLY_FILLED"}:
                canceled.append(self.cancel(order_id, f"emergency:{order_id}"))
        return tuple(canceled)

    def ensure_position_protected(self, symbol: str) -> bool:
        assert symbol == "BTCUSDT-PERP"
        return self.confirm_protection

    def flatten_symbol(self, symbol: str) -> ExecutionTruth:
        assert symbol == "BTCUSDT-PERP"
        self.current_truth = replace(
            self.current_truth,
            actual_quantity=Decimal(0),
            pending_quantity=Decimal(0),
            state_id="venue-state-emergency-stop-flat",
        )
        return self.current_truth


def test_timeout_after_accept_recovers_same_order_without_second_submit(
    tmp_path,
) -> None:
    path = tmp_path / "alma.db"
    connection = ledger(path)
    venue = FakeVenue()
    venue.timeout_after_accept = True
    expected_id = client_order_id("intent-1")
    try:
        first = TacticalExecutor(connection, venue).execute("intent-1", now=NOW)
        assert first.status is ExecutionStatus.UNKNOWN
        assert first.client_order_id == expected_id
        assert len(venue.submissions) == 1
        assert connection.execute(
            "SELECT status FROM order_events WHERE order_id = ?", (expected_id,)
        ).fetchall() == [("SUBMITTED",)]
    finally:
        connection.close()

    reopened = open_ledger(path)
    try:
        recovered = TacticalExecutor(reopened, venue).execute("intent-1", now=NOW)
        assert recovered.status is ExecutionStatus.ACTIVE
        assert len(venue.submissions) == 1
        assert reopened.execute(
            "SELECT status FROM order_events WHERE order_id = ? ORDER BY seq",
            (expected_id,),
        ).fetchall() == [("SUBMITTED",), ("ACCEPTED",)]
    finally:
        reopened.close()


def test_async_native_submit_stays_unknown_until_venue_callback(tmp_path) -> None:
    connection = ledger(tmp_path / "alma.db")
    venue = FakeVenue()
    venue.async_submit = True
    try:
        result = TacticalExecutor(connection, venue).execute("intent-1", now=NOW)
        assert result.status is ExecutionStatus.UNKNOWN
        assert connection.execute(
            "SELECT status FROM order_events WHERE order_id = ?",
            (client_order_id("intent-1"),),
        ).fetchall() == [("SUBMITTED",)]
    finally:
        connection.close()


def test_confirmed_adapter_rejection_terminalizes_reservation(tmp_path) -> None:
    connection = ledger(tmp_path / "alma.db")
    venue = FakeVenue()

    def reject(_request):
        raise ExecutionRejected("ADAPTER_REJECTED")

    venue.submit = reject
    order_id = client_order_id("intent-1")
    try:
        with pytest.raises(ExecutionRejected, match="ADAPTER_REJECTED"):
            TacticalExecutor(connection, venue).execute("intent-1", now=NOW)
        assert connection.execute(
            "SELECT status FROM order_events WHERE order_id = ? ORDER BY seq",
            (order_id,),
        ).fetchall() == [("SUBMITTED",), ("REJECTED",)]
        assert TacticalExecutor(connection, venue).recover_open_intents(now=NOW) == ()
    finally:
        connection.close()


def test_target_quantity_must_match_native_increment_before_reservation(
    tmp_path,
) -> None:
    raw = (
        contract()
        .replace(
            b'"targets": [{"price": "110.0", "close_fraction": "1"}]',
            b'"targets": [{"price": "110.0", "close_fraction": "0.5"},'
            b'{"price": "111.0", "close_fraction": "0.5"}]',
        )
        .replace(b'"volume": "1"', b'"volume": "0.001"')
    )
    connection = ledger(
        tmp_path / "alma.db", raw_contract=raw, quantity=Decimal("0.001")
    )
    venue = FakeVenue()
    venue.current_rules = replace(
        venue.current_rules,
        quantity_step=Decimal("0.001"),
        quantity_min=Decimal("0.001"),
    )
    try:
        with pytest.raises(ExecutionRejected, match="PROTECTION_INVALID"):
            TacticalExecutor(connection, venue).execute("intent-1", now=NOW)
        assert venue.submissions == []
        assert connection.execute("SELECT count(*) FROM order_events").fetchone() == (
            0,
        )
    finally:
        connection.close()


def test_entry_below_venue_minimum_notional_is_rejected_before_reservation(
    tmp_path,
) -> None:
    raw = contract().replace(b'"volume": "1"', b'"volume": "0.001"')
    connection = ledger(
        tmp_path / "alma.db", raw_contract=raw, quantity=Decimal("0.001")
    )
    venue = FakeVenue()
    venue.current_rules = replace(venue.current_rules, minimum_notional=Decimal(50))
    try:
        with pytest.raises(ExecutionRejected, match="INSTRUMENT_INVALID"):
            TacticalExecutor(connection, venue).execute("intent-1", now=NOW)
        assert venue.submissions == []
        assert connection.execute("SELECT count(*) FROM order_events").fetchone() == (
            0,
        )
    finally:
        connection.close()


def test_binance_short_ttl_entry_uses_gtc_for_local_expiry_management() -> None:
    assert ENTRY_TIME_IN_FORCE is TimeInForce.GTC


def test_binance_parent_rejection_closes_local_submission_once(tmp_path) -> None:
    connection = ledger(tmp_path / "alma.db")
    order_id = client_order_id("intent-1")
    assert reserve_order_submission(
        connection,
        event_id=f"submitted:{order_id}",
        intent_id="intent-1",
        order_id=order_id,
        quantity=Decimal(1),
        price=Decimal(100),
        created_at=NOW.isoformat(),
    )
    event = SimpleNamespace(
        client_order_id=order_id,
        id="venue-rejection-1",
        ts_event=int(NOW.timestamp() * 1_000_000_000),
    )
    try:
        assert _record_entry_rejection(connection, event)
        assert not _record_entry_rejection(connection, event)
        assert connection.execute(
            "SELECT status FROM order_events WHERE order_id = ? ORDER BY seq",
            (order_id,),
        ).fetchall() == [("SUBMITTED",), ("REJECTED",)]
    finally:
        connection.close()


def test_ambiguous_local_submit_never_resubmits_when_broker_query_is_empty(
    tmp_path,
) -> None:
    connection = ledger(tmp_path / "alma.db")
    venue = FakeVenue()
    venue.timeout_after_accept = True
    executor = TacticalExecutor(connection, venue)
    try:
        assert executor.execute("intent-1", now=NOW).status is ExecutionStatus.UNKNOWN
        venue.orders.clear()
        assert executor.execute("intent-1", now=NOW).status is ExecutionStatus.UNKNOWN
        assert len(venue.submissions) == 1
    finally:
        connection.close()


def test_executor_cancels_entry_when_venue_protection_is_unconfirmed(tmp_path) -> None:
    connection = ledger(tmp_path / "alma.db")
    venue = FakeVenue()
    venue.confirm_protection = False
    venue.fill_on_submit = Decimal("0.4")
    try:
        with pytest.raises(ExecutionRejected, match="PROTECTION_UNCONFIRMED"):
            TacticalExecutor(connection, venue).execute("intent-1", now=NOW)
        order_id = client_order_id("intent-1")
        assert len(venue.submissions) == 1
        assert len(venue.cancellations) == 1
        assert venue.orders[order_id].status == "CANCELED"
        assert connection.execute(
            "SELECT mode FROM venue_modes WHERE venue_id = 'BINANCE'"
        ).fetchone() == ("MANAGE_ONLY",)
        assert connection.execute(
            "SELECT status FROM order_events WHERE order_id = ? ORDER BY seq",
            (order_id,),
        ).fetchall() == [("SUBMITTED",), ("CANCELED",)]
    finally:
        connection.close()


def test_failed_emergency_flatten_blocks_new_entries_durably(tmp_path) -> None:
    connection = ledger(tmp_path / "alma.db")
    venue = FakeVenue()
    venue.confirm_protection = False
    venue.confirm_emergency_flatten = False
    venue.fill_on_submit = Decimal("0.4")
    try:
        with pytest.raises(ExecutionRejected, match="EMERGENCY_DERISK_UNCONFIRMED"):
            TacticalExecutor(connection, venue).execute("intent-1", now=NOW)
        assert connection.execute(
            "SELECT mode FROM venue_modes WHERE venue_id = 'BINANCE'"
        ).fetchone() == ("MANAGE_ONLY",)
        assert len(venue.submissions) == 1
    finally:
        connection.close()


def test_emergency_stop_cancels_entries_flattens_and_completes_off(tmp_path) -> None:
    connection = ledger(tmp_path / "alma.db")
    venue = FakeVenue()
    venue.current_truth = replace(
        venue.current_truth,
        actual_quantity=Decimal(1),
        state_id="venue-state-open",
    )
    venue.orders["manual-entry"] = VenueOrder(
        "manual-entry",
        "ACCEPTED",
        Decimal(1),
        Decimal(0),
        Decimal(100),
        "manual-entry-accepted",
    )
    gate = MutationGate(connection, max_age=timedelta(seconds=2), clock=lambda: NOW)
    try:
        result = execute_emergency_stop(
            gate,
            venue,
            request_id="emergency-1",
            completion_request_id="emergency-1-complete",
            venue_id="BINANCE",
            symbol="BTCUSDT-PERP",
            policy=OpenPositionPolicy.CLOSE_AND_DISABLE,
            actor="operator",
            now=NOW,
        )
        assert result.mode is VenueMode.OFF
        assert result.truth.actual_quantity == result.truth.pending_quantity == 0
        assert result.canceled_entries == 1
        assert connection.execute(
            "SELECT mode FROM venue_modes WHERE venue_id = 'BINANCE'"
        ).fetchone() == ("OFF",)
    finally:
        connection.close()


def test_recovery_cancels_entry_when_protection_disappeared(tmp_path) -> None:
    path = tmp_path / "alma.db"
    connection = ledger(path)
    venue = FakeVenue()
    venue.timeout_after_accept = True
    venue.fill_on_submit = Decimal("0.4")
    try:
        assert (
            TacticalExecutor(connection, venue).execute("intent-1", now=NOW).status
            is ExecutionStatus.UNKNOWN
        )
    finally:
        connection.close()

    venue.protections.clear()
    reopened = open_ledger(path)
    try:
        with pytest.raises(ExecutionRejected, match="PROTECTION_UNCONFIRMED"):
            TacticalExecutor(reopened, venue).execute("intent-1", now=NOW)
        assert venue.orders[client_order_id("intent-1")].status == "CANCELED"
        assert len(venue.submissions) == 1
        assert reopened.execute(
            "SELECT mode FROM venue_modes WHERE venue_id = 'BINANCE'"
        ).fetchone() == ("MANAGE_ONLY",)
    finally:
        reopened.close()


def test_recovery_never_treats_filled_entry_without_protection_as_safe(
    tmp_path,
) -> None:
    connection = ledger(tmp_path / "alma.db")
    venue = FakeVenue()
    executor = TacticalExecutor(connection, venue)
    try:
        result = executor.execute("intent-1", now=NOW)
        order_id = result.client_order_id
        assert order_id is not None
        venue.orders[order_id] = replace(
            venue.orders[order_id],
            status="FILLED",
            filled_quantity=Decimal(1),
            event_id=f"filled:{order_id}",
        )
        assert (
            executor.maintain("intent-1", now=NOW + timedelta(seconds=1)).status
            is ExecutionStatus.RECOVERED
        )
        venue.orders.clear()
        venue.protections.clear()
        venue.current_truth = replace(
            venue.current_truth,
            actual_quantity=Decimal(1),
            state_id="state-filled",
        )
        with pytest.raises(ExecutionRejected, match="PROTECTION_UNCONFIRMED"):
            executor.recover_open_intents(now=NOW + timedelta(seconds=2))
        assert venue.current_truth.actual_quantity == 0
    finally:
        connection.close()


def test_executor_rereads_truth_immediately_before_submit(tmp_path) -> None:
    connection = ledger(tmp_path / "alma.db")
    venue = FakeVenue()
    venue.truth_after_first = replace(
        venue.current_truth,
        actual_quantity=Decimal(1),
    )
    try:
        result = TacticalExecutor(connection, venue).execute("intent-1", now=NOW)
        assert result.status is ExecutionStatus.NO_ACTION
        assert venue.truth_calls == 2
        assert venue.submissions == []
        assert connection.execute("SELECT count(*) FROM order_events").fetchone() == (
            0,
        )
    finally:
        connection.close()


def test_executor_reconciles_manual_position_before_submit(tmp_path) -> None:
    connection = ledger(tmp_path / "alma.db")
    venue = FakeVenue()
    venue.current_truth = replace(
        venue.current_truth,
        actual_quantity=Decimal("0.4"),
    )
    try:
        result = TacticalExecutor(connection, venue).execute("intent-1", now=NOW)
        assert result.delta == Decimal("0.6")
        request = venue.submissions[0]
        assert request.quantity == Decimal("0.6")
        assert request.reduce_only is False
        assert request.stop_loss == Decimal("95.0")
        assert request.take_profits == ((Decimal("110.0"), Decimal(1)),)
    finally:
        connection.close()


@pytest.mark.parametrize(
    ("action", "target", "desired"),
    [
        ("REDUCE", b'{"side": "LONG", "volume": "0.4"}', Decimal("0.4")),
        ("CLOSE", b'{"side": "FLAT", "volume": "0"}', Decimal(0)),
    ],
)
def test_reduce_and_close_submit_only_reduce_only_quantity(
    tmp_path, action, target, desired
) -> None:
    raw = contract().replace(b'"action": "OPEN_LONG"', f'"action": "{action}"'.encode())
    raw = raw.replace(b'{"side": "LONG", "volume": "1"}', target)
    raw = raw.replace(
        b'"targets": [{"price": "110.0", "close_fraction": "1"}]',
        b'"targets": []',
    )
    connection = ledger(tmp_path / "alma.db", raw_contract=raw, quantity=desired)
    venue = FakeVenue()
    venue.current_truth = replace(venue.current_truth, actual_quantity=Decimal(1))
    try:
        TacticalExecutor(connection, venue).execute("intent-1", now=NOW)
        request = venue.submissions[0]
        assert request.reduce_only
        assert request.side == "SELL"
        assert request.quantity == Decimal(1) - desired
        assert request.stop_loss == 0 and request.take_profits == ()
    finally:
        connection.close()


def test_reduce_action_cannot_increase_exposure(tmp_path) -> None:
    raw = contract().replace(b'"action": "OPEN_LONG"', b'"action": "REDUCE"')
    raw = raw.replace(b'"volume": "1"', b'"volume": "2"', 1)
    raw = raw.replace(
        b'"targets": [{"price": "110.0", "close_fraction": "1"}]', b'"targets": []'
    )
    connection = ledger(tmp_path / "alma.db", raw_contract=raw, quantity=Decimal(2))
    venue = FakeVenue()
    venue.current_truth = replace(venue.current_truth, actual_quantity=Decimal(1))
    try:
        with pytest.raises(ExecutionRejected, match="ACTION_STATE_MISMATCH"):
            TacticalExecutor(connection, venue).execute("intent-1", now=NOW)
        assert venue.submissions == []
    finally:
        connection.close()


def test_reverse_flattens_before_opening_protected_target(tmp_path) -> None:
    raw = contract().replace(b'"action": "OPEN_LONG"', b'"action": "REVERSE"')
    raw = raw.replace(b'"side": "LONG"', b'"side": "SHORT"', 1)
    raw = raw.replace(b'"invalidation_price": "95.0"', b'"invalidation_price": "105.0"')
    raw = raw.replace(b'"price": "110.0"', b'"price": "90.0"')
    connection = ledger(tmp_path / "alma.db", raw_contract=raw, quantity=Decimal(-1))
    venue = FakeVenue()
    venue.current_truth = replace(venue.current_truth, actual_quantity=Decimal(1))
    executor = TacticalExecutor(connection, venue)
    try:
        first = executor.execute("intent-1", now=NOW)
        assert first.client_order_id == client_order_id("intent-1")
        assert venue.submissions[0].reduce_only
        assert venue.submissions[0].quantity == Decimal(1)

        venue.orders[first.client_order_id] = replace(
            venue.orders[first.client_order_id],
            status="FILLED",
            filled_quantity=Decimal(1),
            event_id="filled:reverse-flat",
        )
        venue.current_truth = replace(venue.current_truth, actual_quantity=Decimal(0))
        second = executor.maintain("intent-1", now=NOW + timedelta(seconds=1))

        assert second.status is ExecutionStatus.REPLACED
        assert second.client_order_id == client_order_id("intent-1", 1)
        assert venue.submissions[1].side == "SELL"
        assert not venue.submissions[1].reduce_only
        assert venue.submissions[1].take_profits == ((Decimal("90.0"), Decimal(1)),)
    finally:
        connection.close()


def test_reverse_partial_flatten_reprices_before_opening(tmp_path) -> None:
    raw = contract(partial_policy="REPRICE_REMAINDER").replace(
        b'"action": "OPEN_LONG"', b'"action": "REVERSE"'
    )
    raw = raw.replace(b'"side": "LONG"', b'"side": "SHORT"', 1)
    raw = raw.replace(b'"invalidation_price": "95.0"', b'"invalidation_price": "105.0"')
    raw = raw.replace(b'"price": "110.0"', b'"price": "90.0"')
    connection = ledger(tmp_path / "alma.db", raw_contract=raw, quantity=Decimal(-1))
    venue = FakeVenue()
    venue.current_truth = replace(venue.current_truth, actual_quantity=Decimal(1))
    executor = TacticalExecutor(connection, venue)
    try:
        first = executor.execute("intent-1", now=NOW)
        first_id = first.client_order_id
        assert first_id is not None
        venue.orders[first_id] = replace(
            venue.orders[first_id],
            status="PARTIALLY_FILLED",
            filled_quantity=Decimal("0.4"),
            event_id=f"partial:{first_id}",
        )
        venue.current_truth = replace(
            venue.current_truth, actual_quantity=Decimal("0.6")
        )
        flatten = executor.maintain("intent-1", now=NOW + timedelta(seconds=1))
        flatten_id = flatten.client_order_id
        assert flatten_id == client_order_id("intent-1", 1)
        assert flatten_id is not None
        assert venue.submissions[-1].reduce_only
        venue.orders[flatten_id] = replace(
            venue.orders[flatten_id],
            status="FILLED",
            filled_quantity=Decimal("0.6"),
            event_id=f"filled:{flatten_id}",
        )
        venue.current_truth = replace(venue.current_truth, actual_quantity=Decimal(0))

        opening = executor.maintain("intent-1", now=NOW + timedelta(seconds=2))

        assert opening.status is ExecutionStatus.REPLACED
        assert opening.client_order_id == client_order_id("intent-1", 2)
        assert not venue.submissions[-1].reduce_only
    finally:
        connection.close()


def test_conflicting_pending_order_blocks_reduction(tmp_path) -> None:
    raw = contract().replace(b'"action": "OPEN_LONG"', b'"action": "REDUCE"')
    raw = raw.replace(b'"volume": "1"', b'"volume": "0.5"', 1)
    raw = raw.replace(
        b'"targets": [{"price": "110.0", "close_fraction": "1"}]', b'"targets": []'
    )
    connection = ledger(tmp_path / "alma.db", raw_contract=raw, quantity=Decimal("0.5"))
    venue = FakeVenue()
    venue.current_truth = replace(
        venue.current_truth,
        actual_quantity=Decimal(1),
        pending_quantity=Decimal("-0.7"),
    )
    try:
        with pytest.raises(ExecutionRejected, match="PENDING_CONFLICT"):
            TacticalExecutor(connection, venue).execute("intent-1", now=NOW)
        assert venue.submissions == []
    finally:
        connection.close()


@pytest.mark.parametrize(
    ("truth", "error"),
    [
        (
            ExecutionTruth(
                NOW - timedelta(seconds=3),
                True,
                Decimal(0),
                Decimal(0),
                Decimal(100),
                Decimal("100.1"),
                Decimal(1_000),
            ),
            "STATE_STALE",
        ),
        (
            ExecutionTruth(
                NOW,
                False,
                Decimal(0),
                Decimal(0),
                Decimal(100),
                Decimal("100.1"),
                Decimal(1_000),
            ),
            "VENUE_UNAVAILABLE",
        ),
    ],
)
def test_executor_rejects_stale_or_disconnected_truth(tmp_path, truth, error) -> None:
    connection = ledger(tmp_path / "alma.db")
    venue = FakeVenue()
    venue.current_truth = truth
    try:
        with pytest.raises(ExecutionRejected, match=error):
            TacticalExecutor(connection, venue).execute("intent-1", now=NOW)
        assert venue.submissions == []
    finally:
        connection.close()


def test_executor_enforces_current_mode_metadata_and_margin(tmp_path) -> None:
    connection = ledger(tmp_path / "alma.db", VenueMode.MONITOR)
    venue = FakeVenue()
    try:
        with pytest.raises(ExecutionRejected, match="MODE_BLOCKED"):
            TacticalExecutor(connection, venue).execute("intent-1", now=NOW)
        connection.execute(
            "UPDATE venue_modes SET mode = 'TRADE' WHERE venue_id = 'BINANCE'"
        )
        connection.commit()

        venue.current_rules = replace(
            venue.current_rules,
            quantity_step=Decimal("0.3"),
        )
        with pytest.raises(ExecutionRejected, match="INSTRUMENT_INVALID"):
            TacticalExecutor(connection, venue).execute("intent-1", now=NOW)

        venue.current_rules = replace(
            venue.current_rules,
            quantity_step=Decimal("0.001"),
        )
        venue.current_truth = replace(venue.current_truth, available_margin=Decimal(1))
        with pytest.raises(ExecutionRejected, match="MARGIN_UNKNOWN"):
            TacticalExecutor(connection, venue).execute("intent-1", now=NOW)
        assert venue.submissions == []
    finally:
        connection.close()


def test_maintain_cancels_expired_order(tmp_path) -> None:
    connection = ledger(tmp_path / "alma.db")
    venue = FakeVenue()
    executor = TacticalExecutor(connection, venue)
    try:
        submitted = executor.execute("intent-1", now=NOW)
        result = executor.maintain("intent-1", now=NOW + timedelta(seconds=61))
        assert result.status is ExecutionStatus.CANCELED
        assert result.client_order_id == submitted.client_order_id
        assert len(venue.cancellations) == 1
        assert connection.execute(
            "SELECT status FROM order_events WHERE order_id = ? ORDER BY seq",
            (submitted.client_order_id,),
        ).fetchall() == [("SUBMITTED",), ("ACCEPTED",), ("CANCELED",)]
    finally:
        connection.close()


def test_execute_after_restart_manages_expired_order_instead_of_rejecting_contract(
    tmp_path,
) -> None:
    path = tmp_path / "alma.db"
    connection = ledger(path)
    venue = FakeVenue()
    try:
        TacticalExecutor(connection, venue).execute("intent-1", now=NOW)
    finally:
        connection.close()

    reopened = open_ledger(path)
    try:
        result = TacticalExecutor(reopened, venue).execute(
            "intent-1", now=NOW + timedelta(seconds=61)
        )
        assert result.status is ExecutionStatus.CANCELED
        assert len(venue.submissions) == 1
        assert len(venue.cancellations) == 1
    finally:
        reopened.close()


def test_startup_recovery_scans_only_latest_nonterminal_orders(tmp_path) -> None:
    connection = ledger(tmp_path / "alma.db")
    venue = FakeVenue()
    executor = TacticalExecutor(connection, venue)
    try:
        executor.execute("intent-1", now=NOW)
        results = executor.recover_open_intents(now=NOW + timedelta(seconds=1))
        assert tuple(result.status for result in results) == (ExecutionStatus.ACTIVE,)
        assert (
            executor.recover_open_intents(
                now=NOW + timedelta(seconds=1), venue="MT5", symbol="XAUUSD"
            )
            == ()
        )

        executor.maintain("intent-1", now=NOW + timedelta(seconds=61))
        assert executor.recover_open_intents(now=NOW + timedelta(seconds=62)) == ()
        assert len(venue.submissions) == 1
    finally:
        connection.close()


def test_recovery_expires_superseded_live_order(tmp_path) -> None:
    connection = ledger(tmp_path / "alma.db")
    venue = FakeVenue()
    executor = TacticalExecutor(connection, venue)
    try:
        executor.execute("intent-1", now=NOW)
        assert record_intent_mutation(
            connection,
            audit_event_id="audit:intent-2",
            actor="test",
            before_summary="{}",
            after_summary="{}",
            intent_id="intent-2",
            decision_id="decision-1",
            request_id="intent-2",
            venue="BINANCE",
            symbol="BTCUSDT-PERP",
            state_id="state-1",
            created_at=NOW.isoformat(),
            mode="TRADE",
            desired_quantity=Decimal(1),
            actual_quantity=Decimal(0),
            pending_quantity=Decimal(0),
            execution_delta=Decimal(1),
        )

        executor.recover_open_intents(now=NOW + timedelta(seconds=61))

        assert venue.orders[client_order_id("intent-1")].status == "CANCELED"
    finally:
        connection.close()


def test_recovery_retries_waiting_intent_once_when_entry_becomes_executable(
    tmp_path,
) -> None:
    raw = contract().replace(b'"mode": "PASSIVE"', b'"mode": "WAIT_RETEST"')
    connection = ledger(tmp_path / "alma.db", raw_contract=raw)
    venue = FakeVenue()
    venue.current_truth = replace(venue.current_truth, ask=Decimal(102))
    executor = TacticalExecutor(connection, venue)
    try:
        assert executor.execute("intent-1", now=NOW).status is ExecutionStatus.WAITING
        assert venue.submissions == []

        venue.current_truth = replace(venue.current_truth, ask=Decimal("100.1"))
        first = executor.recover_open_intents(now=NOW + timedelta(seconds=1))
        replay = executor.recover_open_intents(now=NOW + timedelta(seconds=2))

        assert tuple(result.status for result in first) == (ExecutionStatus.SUBMITTED,)
        assert tuple(result.status for result in replay) == (ExecutionStatus.ACTIVE,)
        assert len(venue.submissions) == 1
    finally:
        connection.close()


def test_recovery_skips_historical_invalid_waiting_contract(tmp_path) -> None:
    raw = contract().replace(
        b'"targets": [{"price": "110.0", "close_fraction": "1"}]',
        b'"targets": []',
    )
    connection = ledger(tmp_path / "alma.db", raw_contract=raw)
    venue = FakeVenue()
    try:
        assert TacticalExecutor(connection, venue).recover_open_intents(now=NOW) == ()
        assert venue.submissions == []
    finally:
        connection.close()


def test_recovery_activates_only_latest_waiting_intent_per_venue_symbol(
    tmp_path,
) -> None:
    raw = contract().replace(b'"mode": "PASSIVE"', b'"mode": "WAIT_RETEST"')
    connection = ledger(tmp_path / "alma.db", raw_contract=raw)
    second = raw.replace(b'"decision-1"', b'"decision-2"')
    append_decision(
        connection,
        decision_id="decision-2",
        state_id="state-1",
        created_at=NOW.isoformat(),
        raw_contract=second,
        validation_result="ACCEPTED",
        model_id="model",
        prompt_hash="prompt",
        policy_hash="policy",
        code_hash="code",
    )
    assert record_intent_mutation(
        connection,
        audit_event_id="audit:intent-request-2",
        actor="test",
        before_summary="{}",
        after_summary="{}",
        intent_id="intent-2",
        decision_id="decision-2",
        request_id="intent-request-2",
        venue="BINANCE",
        symbol="BTCUSDT-PERP",
        state_id="state-1",
        created_at=NOW.isoformat(),
        mode="TRADE",
        desired_quantity=Decimal(1),
        actual_quantity=Decimal(0),
        pending_quantity=Decimal(0),
        execution_delta=Decimal(1),
    )
    venue = FakeVenue()
    try:
        results = TacticalExecutor(connection, venue).recover_open_intents(now=NOW)
        assert tuple(result.status for result in results) == (
            ExecutionStatus.SUBMITTED,
        )
        assert [request.intent_id for request in venue.submissions] == ["intent-2"]
    finally:
        connection.close()


def test_recovery_maintains_older_order_and_submits_latest_intent(tmp_path) -> None:
    connection = ledger(tmp_path / "alma.db")
    venue = FakeVenue()
    executor = TacticalExecutor(connection, venue)
    try:
        assert executor.execute("intent-1", now=NOW).status is ExecutionStatus.SUBMITTED
        venue.submissions.clear()
        raw = contract().replace(b'"decision-1"', b'"decision-2"')
        append_decision(
            connection,
            decision_id="decision-2",
            state_id="state-1",
            created_at=(NOW + timedelta(seconds=1)).isoformat(),
            raw_contract=raw,
            validation_result="ACCEPTED",
            model_id="model",
            prompt_hash="prompt",
            policy_hash="policy",
            code_hash="code",
        )
        assert record_intent_mutation(
            connection,
            audit_event_id="audit:intent-request-2",
            actor="test",
            before_summary="{}",
            after_summary="{}",
            intent_id="intent-2",
            decision_id="decision-2",
            request_id="intent-request-2",
            venue="BINANCE",
            symbol="BTCUSDT-PERP",
            state_id="state-1",
            created_at=(NOW + timedelta(seconds=1)).isoformat(),
            mode="TRADE",
            desired_quantity=Decimal(1),
            actual_quantity=Decimal(0),
            pending_quantity=Decimal(0),
            execution_delta=Decimal(1),
        )

        results = executor.recover_open_intents(now=NOW + timedelta(seconds=1))

        assert tuple(result.status for result in results) == (
            ExecutionStatus.ACTIVE,
            ExecutionStatus.SUBMITTED,
        )
        assert [request.intent_id for request in venue.submissions] == ["intent-2"]
    finally:
        connection.close()


@pytest.mark.parametrize(
    ("policy", "expected"),
    [
        ("KEEP_REMAINDER", ExecutionStatus.ACTIVE),
        ("CANCEL_REMAINDER", ExecutionStatus.CANCELED),
    ],
)
def test_maintain_applies_keep_or_cancel_partial_policy(
    tmp_path, policy, expected
) -> None:
    connection = ledger(tmp_path / "alma.db", partial_policy=policy)
    venue = FakeVenue()
    executor = TacticalExecutor(connection, venue)
    try:
        submitted = executor.execute("intent-1", now=NOW)
        order_id = submitted.client_order_id
        assert order_id is not None
        venue.orders[order_id] = replace(
            venue.orders[order_id],
            status="PARTIALLY_FILLED",
            filled_quantity=Decimal("0.4"),
            event_id=f"partial:{order_id}",
        )
        result = executor.maintain("intent-1", now=NOW + timedelta(seconds=1))
        assert result.status is expected
        assert len(venue.cancellations) == (policy == "CANCEL_REMAINDER")
    finally:
        connection.close()


def test_maintain_reprices_remainder_after_confirmed_cancel_and_resync(
    tmp_path,
) -> None:
    connection = ledger(tmp_path / "alma.db", partial_policy="REPRICE_REMAINDER")
    venue = FakeVenue()
    executor = TacticalExecutor(connection, venue)
    try:
        first = executor.execute("intent-1", now=NOW)
        first_id = first.client_order_id
        assert first_id is not None
        venue.orders[first_id] = replace(
            venue.orders[first_id],
            status="PARTIALLY_FILLED",
            filled_quantity=Decimal("0.4"),
            event_id=f"partial:{first_id}",
        )
        venue.current_truth = replace(
            venue.current_truth,
            actual_quantity=Decimal("0.4"),
        )

        result = executor.maintain("intent-1", now=NOW + timedelta(seconds=1))

        assert result.status is ExecutionStatus.REPLACED
        assert result.client_order_id == client_order_id("intent-1", 1)
        assert len(venue.cancellations) == 1
        assert [request.quantity for request in venue.submissions] == [
            Decimal(1),
            Decimal("0.6"),
        ]
    finally:
        connection.close()


def test_maintain_reprices_remainder_more_than_once(tmp_path) -> None:
    connection = ledger(tmp_path / "alma.db", partial_policy="REPRICE_REMAINDER")
    venue = FakeVenue()
    executor = TacticalExecutor(connection, venue)
    try:
        first = executor.execute("intent-1", now=NOW)
        first_id = first.client_order_id
        assert first_id is not None
        venue.orders[first_id] = replace(
            venue.orders[first_id],
            status="PARTIALLY_FILLED",
            filled_quantity=Decimal("0.4"),
            event_id=f"partial:{first_id}",
        )
        venue.current_truth = replace(
            venue.current_truth, actual_quantity=Decimal("0.4")
        )
        second = executor.maintain("intent-1", now=NOW + timedelta(seconds=1))
        second_id = second.client_order_id
        assert second_id is not None
        venue.orders[second_id] = replace(
            venue.orders[second_id],
            status="PARTIALLY_FILLED",
            filled_quantity=Decimal("0.2"),
            event_id=f"partial:{second_id}",
        )
        venue.current_truth = replace(
            venue.current_truth, actual_quantity=Decimal("0.6")
        )

        third = executor.maintain("intent-1", now=NOW + timedelta(seconds=2))

        assert third.status is ExecutionStatus.REPLACED
        assert third.client_order_id == client_order_id("intent-1", 2)
        assert [request.quantity for request in venue.submissions] == [
            Decimal(1),
            Decimal("0.6"),
            Decimal("0.4"),
        ]
    finally:
        connection.close()


def test_maintain_reprices_after_asynchronous_cancel_confirmation(tmp_path) -> None:
    connection = ledger(tmp_path / "alma.db", partial_policy="REPRICE_REMAINDER")
    venue = FakeVenue()
    executor = TacticalExecutor(connection, venue)
    try:
        first = executor.execute("intent-1", now=NOW)
        first_id = first.client_order_id
        assert first_id is not None
        venue.orders[first_id] = replace(
            venue.orders[first_id],
            status="PARTIALLY_FILLED",
            filled_quantity=Decimal("0.4"),
            event_id=f"partial:{first_id}",
        )
        venue.current_truth = replace(
            venue.current_truth, actual_quantity=Decimal("0.4")
        )

        def asynchronous_cancel(client_order_id, request_id):
            venue.cancellations.append((client_order_id, request_id))

        venue.cancel = asynchronous_cancel
        assert (
            executor.maintain("intent-1", now=NOW + timedelta(seconds=1)).status
            is ExecutionStatus.UNKNOWN
        )
        venue.orders[first_id] = replace(
            venue.orders[first_id],
            status="CANCELED",
            event_id=f"canceled:{first_id}",
        )

        result = executor.maintain("intent-1", now=NOW + timedelta(seconds=2))
        assert result.status is ExecutionStatus.REPLACED
        assert [request.quantity for request in venue.submissions] == [
            Decimal(1),
            Decimal("0.6"),
        ]
    finally:
        connection.close()


def test_maintain_rechecks_truth_immediately_before_replacement_submit(
    tmp_path,
) -> None:
    connection = ledger(tmp_path / "alma.db", partial_policy="REPRICE_REMAINDER")
    venue = FakeVenue()
    executor = TacticalExecutor(connection, venue)
    try:
        first = executor.execute("intent-1", now=NOW)
        first_id = first.client_order_id
        assert first_id is not None
        venue.orders[first_id] = replace(
            venue.orders[first_id],
            status="PARTIALLY_FILLED",
            filled_quantity=Decimal("0.4"),
            event_id=f"partial:{first_id}",
        )
        partial = replace(venue.current_truth, actual_quantity=Decimal("0.4"))
        complete = replace(venue.current_truth, actual_quantity=Decimal(1))
        venue.truth_sequence = [partial, complete]

        result = executor.maintain("intent-1", now=NOW + timedelta(seconds=1))

        assert result.status is ExecutionStatus.NO_ACTION
        assert len(venue.cancellations) == 1
        assert len(venue.submissions) == 1
    finally:
        connection.close()


def test_maintain_never_replaces_when_cancel_is_unconfirmed(tmp_path) -> None:
    connection = ledger(tmp_path / "alma.db", partial_policy="REPRICE_REMAINDER")
    venue = FakeVenue()
    executor = TacticalExecutor(connection, venue)
    try:
        first = executor.execute("intent-1", now=NOW)
        first_id = first.client_order_id
        assert first_id is not None
        venue.orders[first_id] = replace(
            venue.orders[first_id],
            status="PARTIALLY_FILLED",
            filled_quantity=Decimal("0.4"),
            event_id=f"partial:{first_id}",
        )
        venue.cancel_timeout = True
        with pytest.raises(ExecutionRejected, match="CANCEL_UNCONFIRMED"):
            executor.maintain("intent-1", now=NOW + timedelta(seconds=1))
        assert len(venue.submissions) == 1
    finally:
        connection.close()
