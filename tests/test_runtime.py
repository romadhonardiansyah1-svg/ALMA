import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from nautilus_trader.adapters.binance.common.enums import BinanceEnvironment

from alma.bars import Bar
from alma.decision_contract import parse_decision_contract
from alma.execution import (
    ExecutionRejected,
    ExecutionTruth,
    InstrumentRules,
    ProtectedSubmission,
    VenueOrder,
)
from alma.ledger import append_decision, open_ledger
from alma.market_state import MarketSnapshot
from alma.mutation_gate import MutationGate, VenueTruth
from alma.runtime import (
    RuntimeCore,
    _execution_enabled,
    _runtime_healthy,
    _validate_live_profile,
    activate_trade_mode,
    bootstrap_monitor_modes,
    complete_pending_transition,
    execute_accepted_decision,
)
from alma.shadow_service import ShadowResult
from alma.venue_mode_store import initialize_venue_mode
from alma.venue_modes import OpenPositionPolicy, VenueMode

NOW = datetime(2026, 8, 1, 5, tzinfo=UTC)


def test_runtime_health_fails_closed_on_ledger_alert() -> None:
    assert not _runtime_healthy({}, 0, True, {"BINANCE": True}, ["PROVIDER_FAILED"])


def test_main_disposes_node_after_asyncio_loop_closes(monkeypatch) -> None:
    import sys

    from alma import runtime

    class Node:
        disposed_outside_loop = False

        def dispose(self) -> None:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                self.disposed_outside_loop = True

    node = Node()

    async def run_runtime(*args, **kwargs):
        del args, kwargs
        return node

    monkeypatch.setattr(runtime, "run_runtime", run_runtime)
    monkeypatch.setattr(sys, "argv", ["alma.runtime"])

    runtime.main()

    assert node.disposed_outside_loop


def test_one_unavailable_venue_does_not_disable_healthy_sibling() -> None:
    modes = {"BINANCE": "TRADE", "MT5": "TRADE"}
    assert _execution_enabled(modes, {"BINANCE": True, "MT5": False})
    assert not _execution_enabled(modes, {"BINANCE": False, "MT5": False})


def test_live_profiles_are_configurable_but_explicitly_armed() -> None:
    _validate_live_profile(BinanceEnvironment.LIVE, "REAL", "true")
    _validate_live_profile(BinanceEnvironment.TESTNET, "DEMO", "false")
    with pytest.raises(RuntimeError, match="LIVE_PROFILE_NOT_APPROVED"):
        _validate_live_profile(BinanceEnvironment.LIVE, "DEMO", "false")


def test_pending_transition_completes_only_after_flat_truth(tmp_path) -> None:
    connection = open_ledger(tmp_path / "alma.db")
    initialize_venue_mode(connection, "BINANCE", VenueMode.TRADE)
    gate = MutationGate(connection, max_age=timedelta(seconds=2), clock=lambda: NOW)
    gate.sync_venue(
        venue="BINANCE",
        symbol="BTCUSDT-PERP",
        truth=VenueTruth("open", NOW, Decimal(1), Decimal(0)),
    )
    gate.transition_mode(
        request_id="stop",
        state_id="open",
        timestamp=NOW,
        venue="BINANCE",
        symbol="BTCUSDT-PERP",
        requested=VenueMode.OFF,
        policy=OpenPositionPolicy.CLOSE_AND_DISABLE,
        actor="test",
    )
    assert (
        complete_pending_transition(
            connection,
            venue="BINANCE",
            symbol="BTCUSDT-PERP",
            truth=ExecutionTruth(
                NOW,
                True,
                Decimal(1),
                Decimal(0),
                Decimal(100),
                Decimal(101),
                Decimal(1000),
                "still-open",
            ),
            now=NOW,
        )
        is None
    )
    assert (
        complete_pending_transition(
            connection,
            venue="BINANCE",
            symbol="BTCUSDT-PERP",
            truth=ExecutionTruth(
                NOW,
                True,
                Decimal(0),
                Decimal(0),
                Decimal(100),
                Decimal(101),
                Decimal(1000),
                "flat",
            ),
            now=NOW,
        )
        is VenueMode.OFF
    )
    connection.close()


class Venue:
    def __init__(self) -> None:
        self.submissions = []
        self.orders = {}
        self.current_truth = ExecutionTruth(
            NOW,
            True,
            Decimal(0),
            Decimal(0),
            Decimal(100),
            Decimal("100.1"),
            Decimal(1000),
            "broker-1",
        )

    def truth(self, symbol):
        assert symbol == "BTCUSDT-PERP"
        return self.current_truth

    def rules(self, symbol):
        assert symbol == "BTCUSDT-PERP"
        return InstrumentRules(
            Decimal("0.001"),
            Decimal("0.001"),
            Decimal(100),
            Decimal("0.1"),
            Decimal(1),
            Decimal(1_000_000),
            Decimal("0.1"),
        )

    def find_order(self, order_id):
        return self.orders.get(order_id)

    def protection(self, order_id):
        del order_id
        return ()

    def required_margin(self, request):
        return request.quantity * 10

    def submit(self, request):
        self.submissions.append(request)
        order = VenueOrder(
            request.client_order_id,
            "ACCEPTED",
            request.quantity,
            Decimal(0),
            request.price,
            f"accepted:{request.client_order_id}",
        )
        self.orders[request.client_order_id] = order
        return ProtectedSubmission(order, ())

    def cancel(self, client_order_id, request_id):
        del request_id
        return self.orders[client_order_id]

    def emergency_flatten(self, entry):
        del entry
        return self.current_truth

    def cancel_open_entries(self, symbol):
        del symbol
        return ()

    def ensure_position_protected(self, symbol):
        del symbol
        return True

    def flatten_symbol(self, symbol):
        del symbol
        return self.current_truth


def execution_contract() -> bytes:
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
                "preferred_low": "100",
                "preferred_high": "101",
                "max_acceptable_price": "102",
                "ttl_seconds": 60,
                "on_missed": "ABORT",
                "on_partial_fill": "KEEP_REMAINDER",
            },
            "invalidation_price": "95",
            "targets": [{"price": "110", "close_fraction": "1"}],
            "review_triggers": [],
            "evidence": ["test"],
            "uncertainty": "0.1",
        }
    ).encode()


def snapshot(state_id: str = "state-1") -> MarketSnapshot:
    return MarketSnapshot(
        state_id=state_id,
        observed_at_ns=1_785_558_000_000_000_000,
        market_age_ms=10,
        venue="BINANCE",
        symbol="BTCUSDT-PERP",
        bid=Decimal(63000),
        ask=Decimal(63001),
        bid_size=Decimal(1),
        ask_size=Decimal(1),
        spread=Decimal(1),
        top_book_imbalance=Decimal(0),
        mark_price=Decimal("63000.5"),
        funding_rate=Decimal("0.0001"),
        tick_velocity_1s=1,
        realized_volatility=Decimal(0),
        flow_imbalance=Decimal(0),
        session="ASIA",
        book_valid=True,
        m1=None,
        m5=None,
        m15=None,
        h1=None,
    )


def test_runtime_only_calls_ai_for_material_changes(tmp_path, monkeypatch) -> None:
    async def run() -> None:
        connection = open_ledger(tmp_path / "alma.db")
        bootstrap_monitor_modes(connection)
        assert dict(connection.execute("SELECT venue_id, mode FROM venue_modes")) == {
            "BINANCE": VenueMode.MONITOR.value,
            "MT5": VenueMode.MONITOR.value,
        }

        calls = []

        class Shadow:
            async def evaluate(self, request, **context):
                calls.append((request, context))
                return type("Result", (), {"status": "NO_DECISION"})()

        core = RuntimeCore(
            Shadow(), clock_ns=lambda: snapshot().observed_at_ns + 10_000_000
        )
        first = await core.process(snapshot(), VenueMode.MONITOR)
        assert first == "NO_DECISION"
        assert calls[0][0].hooks == ("ACCOUNT_CHANGE",)
        assert calls[0][1]["actual_quantity"] == Decimal(0)

        assert await core.process(snapshot("state-2"), VenueMode.MONITOR) is None
        assert len(calls) == 1

        assert (
            await core.process(
                snapshot("state-account-change"),
                VenueMode.MONITOR,
                actual_quantity=Decimal(1),
            )
            == "NO_DECISION"
        )
        assert calls[-1][0].hooks == ("ACCOUNT_CHANGE",)

        bar = Bar(
            minutes=1,
            start_ns=1_785_557_940_000_000_000,
            end_ns=1_785_558_000_000_000_000,
            open=Decimal(63000),
            high=Decimal(63002),
            low=Decimal(62999),
            close=Decimal(63001),
            volume=Decimal(1),
        )
        calls_before = len(calls)
        assert (
            await core.process(
                replace(snapshot("state-3"), m1=bar),
                VenueMode.MONITOR,
                actual_quantity=Decimal(1),
            )
            is None
        )
        assert len(calls) == calls_before

        setup = type("Setup", (), {"setup": "SWEEP", "direction": 1})()
        monkeypatch.setattr("alma.runtime.detect_liquidity_sweep", lambda _: setup)
        assert await core.process(
            snapshot("setup-1"), VenueMode.MONITOR, actual_quantity=Decimal(1)
        )
        assert calls[-1][0].hooks == ("SETUP",)
        assert (
            await core.process(
                snapshot("setup-2"), VenueMode.MONITOR, actual_quantity=Decimal(1)
            )
            is None
        )
        assert len(calls) == calls_before + 1

        monkeypatch.setattr("alma.runtime.detect_liquidity_sweep", lambda _: None)
        assert (
            await core.process(
                snapshot("setup-clear"), VenueMode.MONITOR, actual_quantity=Decimal(1)
            )
            is None
        )
        monkeypatch.setattr("alma.runtime.detect_liquidity_sweep", lambda _: setup)
        assert await core.process(
            snapshot("setup-3"), VenueMode.MONITOR, actual_quantity=Decimal(1)
        )
        assert len(calls) == calls_before + 2
        connection.close()

    asyncio.run(run())


def test_runtime_never_executes_after_market_snapshot_becomes_stale() -> None:
    async def run() -> None:
        current_ns = [snapshot().observed_at_ns + 10_000_000]
        handled = []

        class Shadow:
            async def evaluate(self, request, **context):
                del request, context
                current_ns[0] = snapshot().observed_at_ns + 3_000_000_000
                return type("Result", (), {"status": "ACCEPTED"})()

        async def handler(*args):
            handled.append(args)

        core = RuntimeCore(Shadow(), handler, clock_ns=lambda: current_ns[0])
        with pytest.raises(ExecutionRejected, match="MARKET_STATE_STALE"):
            await core.process(snapshot(), VenueMode.TRADE)
        assert handled == []

        calls_before = current_ns[0]
        assert (
            await core.process(replace(snapshot(), book_valid=False), VenueMode.TRADE)
            is None
        )
        assert current_ns[0] == calls_before

    asyncio.run(run())


def test_execution_decision_submits_once_and_replay_is_idempotent(tmp_path) -> None:
    connection = open_ledger(tmp_path / "alma.db")
    initialize_venue_mode(connection, "BINANCE", VenueMode.TRADE)
    raw = execution_contract()
    append_decision(
        connection,
        decision_id="decision-1",
        state_id="state-1",
        created_at=NOW.isoformat(),
        raw_contract=raw,
        validation_result="ACCEPTED",
        model_id="model",
        prompt_hash="prompt",
        policy_hash="policy",
        code_hash="code",
        provenance="EXECUTION",
    )
    result = ShadowResult("ACCEPTED", parse_decision_contract(raw), None, Decimal(1))
    venue = Venue()

    first = execute_accepted_decision(
        connection,
        venue,
        result,
        expected_actual=Decimal(0),
        expected_pending=Decimal(0),
        now=NOW,
    )
    second = execute_accepted_decision(
        connection,
        venue,
        result,
        expected_actual=Decimal(0),
        expected_pending=Decimal(0),
        now=NOW,
    )

    assert first.status == "SUBMITTED"
    assert second.status == "ACTIVE"
    assert len(venue.submissions) == 1
    assert connection.execute("SELECT count(*) FROM intents").fetchone() == (1,)
    connection.close()


def test_fresh_venue_truth_preserves_explicit_mode(tmp_path) -> None:
    connection = open_ledger(tmp_path / "alma.db")
    initialize_venue_mode(connection, "BINANCE", VenueMode.MONITOR)
    venue = Venue()

    assert (
        activate_trade_mode(
            connection, venue, venue_id="BINANCE", symbol="BTCUSDT-PERP", now=NOW
        )
        is VenueMode.MONITOR
    )
    assert connection.execute(
        "SELECT mode FROM venue_modes WHERE venue_id='BINANCE'"
    ).fetchone() == ("MONITOR",)
    assert connection.execute(
        "SELECT count(*) FROM audit_events WHERE action='VENUE_MODE_TRANSITION'"
    ).fetchone() == (0,)
    connection.close()


def test_runtime_service_wires_testnet_and_demo_execution() -> None:
    root = Path(__file__).parents[1]
    source = (root / "src/alma/runtime.py").read_text()
    unit = (root / "deploy/alma.service").read_text()
    assert "binance_testnet_runtime" in source
    assert "PublicMarketState" in source
    assert "public_usdm_state_node" not in source
    assert source.count("asyncio.create_task(") == 3
    assert "await binance_core.process(" not in source
    assert "await mt5_core.process(" not in source
    assert 'if decision_tasks["BINANCE"] is None:' in source
    assert 'if decision_tasks["MT5"] is None:' in source
    assert "loop.add_signal_handler(signal.SIGTERM, main_task.cancel)" in source
    assert "asyncio.wait_for(" in source
    assert "MT5Venue" in source
    assert 'provenance="EXECUTION"' in source
    assert "recover_open_intents" in source
    assert '"execution_enabled": execution_enabled' in source
    assert 'cycle_errors["BINANCE"]' in source
    assert 'cycle_errors["MT5"]' in source
    assert "BINANCE_NOT_MONITOR" not in source
    assert "-m alma.runtime" in unit
    assert 'expected_account_mode=os.environ["ALMA_MT5_ACCOUNT_MODE"]' in source
    assert 'os.environ.get("ALMA_BINANCE_ENVIRONMENT", "TESTNET")' in source
    assert 'os.environ.get("ALMA_BINANCE_INSTRUMENT", "BTCUSDT-PERP.BINANCE")' in source
    assert 'os.environ.get("ALMA_MT5_TERMINAL_ID", "mt5-1")' in source
