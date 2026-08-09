import asyncio
from decimal import Decimal
from types import SimpleNamespace

import pytest
from nautilus_trader.adapters.binance import BINANCE
from nautilus_trader.adapters.binance.common.enums import (
    BinanceAccountType,
    BinanceEnvironment,
)
from nautilus_trader.model.identifiers import InstrumentId

from alma.binance_data import PublicMarketState
from alma.binance_native_execution import (
    BinanceNativeVenue,
    _emergency_cache_state,
    _intent_reduces_position,
    _owned_execution_order,
    _owned_order,
    _pending_emergency,
    _record_order_status,
    _signed_position_quantity,
)
from alma.binance_testnet import (
    binance_testnet_node,
    binance_testnet_node_config,
    binance_testnet_read_only_smoke,
    binance_testnet_runtime,
    write_read_only_evidence,
)
from alma.binance_testnet import (
    testnet_credentials_available as credentials_available,
)
from alma.binance_testnet_soak import Observation, validate_observation
from alma.ledger import (
    append_decision,
    open_ledger,
    record_intent_mutation,
    reserve_order_submission,
)
from alma.market_state import MarketState
from alma.nautilus_fill import child_order_id

CREDENTIALS = {
    "BINANCE_FUTURES_TESTNET_API_KEY": "local-key",
    "BINANCE_FUTURES_TESTNET_API_SECRET": "local-secret",
    "ALMA_BINANCE_ACCOUNT_ID": "BINANCE-USDT_FUTURES-master",
}


def test_native_position_quantity_calls_nautilus_method() -> None:
    position = SimpleNamespace(signed_decimal_qty=lambda: Decimal("-0.0009"))
    assert _signed_position_quantity(position) == Decimal("-0.0009")


def test_flatten_symbol_selects_only_binance_intent(tmp_path, monkeypatch) -> None:
    connection = open_ledger(tmp_path / "alma.db")
    for index, venue_id in enumerate(("BINANCE", "MT5"), start=1):
        decision_id = f"decision-{index}"
        intent_id = f"intent-{index}"
        append_decision(
            connection,
            decision_id=decision_id,
            state_id="state",
            created_at="2026-08-01T00:00:00+00:00",
            raw_contract=b"{}",
            validation_result="ACCEPTED",
            model_id="model",
            prompt_hash="prompt",
            policy_hash="policy",
            code_hash="code",
        )
        record_intent_mutation(
            connection,
            audit_event_id=f"audit-{index}",
            actor="test",
            before_summary="{}",
            after_summary="{}",
            intent_id=intent_id,
            decision_id=decision_id,
            request_id=f"request-{index}",
            venue=venue_id,
            symbol="BTCUSDT-PERP",
            state_id="state",
            desired_quantity=Decimal(0),
            actual_quantity=Decimal(1),
            pending_quantity=Decimal(0),
            execution_delta=Decimal(-1),
            created_at="2026-08-01T00:00:00+00:00",
            mode="TRADE",
        )
        reserve_order_submission(
            connection,
            event_id=f"submitted:{venue_id.lower()}",
            intent_id=intent_id,
            order_id=f"{venue_id.lower()}-order",
            quantity=Decimal(1),
            price=Decimal(100),
            created_at="2026-08-01T00:00:00+00:00",
        )

    venue = BinanceNativeVenue.__new__(BinanceNativeVenue)
    venue.connection = connection
    venue.instrument_id = InstrumentId.from_str("BTCUSDT-PERP.BINANCE")
    selected: list[str] = []
    monkeypatch.setattr(
        BinanceNativeVenue,
        "_flatten",
        lambda self, order: selected.append(order),
    )
    monkeypatch.setattr(BinanceNativeVenue, "truth", lambda self, symbol: symbol)

    assert venue.flatten_symbol("BTCUSDT-PERP") == "BTCUSDT-PERP"
    assert selected == ["binance-order"]


def test_native_fill_identifies_durable_reduction_parent(tmp_path) -> None:
    connection = open_ledger(tmp_path / "alma.db")
    append_decision(
        connection,
        decision_id="decision",
        state_id="state",
        created_at="2026-08-01T00:00:00+00:00",
        raw_contract=b"{}",
        validation_result="ACCEPTED",
        model_id="model",
        prompt_hash="prompt",
        policy_hash="policy",
        code_hash="code",
    )
    assert record_intent_mutation(
        connection,
        audit_event_id="audit",
        actor="test",
        before_summary="{}",
        after_summary="{}",
        intent_id="intent",
        decision_id="decision",
        request_id="request",
        venue="BINANCE",
        symbol="BTCUSDT-PERP",
        state_id="state",
        created_at="2026-08-01T00:00:00+00:00",
        mode="TRADE",
        desired_quantity=Decimal("0.1"),
        actual_quantity=Decimal(1),
        pending_quantity=Decimal(0),
        execution_delta=Decimal("-0.9"),
    )
    assert reserve_order_submission(
        connection,
        event_id="submitted:reduce",
        intent_id="intent",
        order_id="reduce",
        quantity=Decimal("0.9"),
        price=Decimal(100),
        created_at="2026-08-01T00:00:00+00:00",
    )
    assert _owned_order(
        connection,
        SimpleNamespace(client_order_id="reduce"),
        venue="BINANCE",
        symbol="BTCUSDT-PERP",
    )
    assert not _owned_order(
        connection,
        SimpleNamespace(client_order_id="alma-unreserved"),
        venue="BINANCE",
        symbol="BTCUSDT-PERP",
    )
    assert _intent_reduces_position(connection, "reduce")
    assert _owned_execution_order(
        connection,
        SimpleNamespace(client_order_id="reduce"),
        venue="BINANCE",
        symbol="BTCUSDT-PERP",
    )
    assert reserve_order_submission(
        connection,
        event_id="protection-submitted:stop",
        intent_id="intent",
        order_id="stop",
        quantity=Decimal("0.1"),
        price=Decimal(90),
        created_at="2026-08-01T00:00:00+00:00",
    )
    assert not _owned_execution_order(
        connection,
        SimpleNamespace(client_order_id="stop"),
        venue="BINANCE",
        symbol="BTCUSDT-PERP",
    )
    event = SimpleNamespace(
        client_order_id="reduce",
        id="event-1",
        ts_event=1_785_557_000_000_000_000,
    )
    assert _record_order_status(connection, event, "ACCEPTED")
    assert not _record_order_status(connection, event, "ACCEPTED")
    event.id = "event-2"
    assert _record_order_status(connection, event, "CANCELED")
    event.id = "event-3"
    assert not _record_order_status(connection, event, "EXPIRED")
    assert connection.execute(
        "SELECT status FROM order_events WHERE order_id='reduce' ORDER BY seq"
    ).fetchall() == [("SUBMITTED",), ("ACCEPTED",), ("CANCELED",)]
    event.client_order_id = "missing"
    assert not _record_order_status(connection, event, "ACCEPTED")
    assert not _intent_reduces_position(connection, "missing")
    assert not _pending_emergency(connection, venue="BINANCE", symbol="BTCUSDT-PERP")
    assert reserve_order_submission(
        connection,
        event_id="emergency-submitted:flat",
        intent_id="intent",
        order_id="flat",
        quantity=Decimal("0.1"),
        price=Decimal(100),
        created_at="2026-08-01T00:00:01+00:00",
    )
    assert (
        _pending_emergency(connection, venue="BINANCE", symbol="BTCUSDT-PERP") == "flat"
    )
    assert _emergency_cache_state(SimpleNamespace(order=lambda _: None), "flat") == (
        "RESUBMIT",
        None,
    )
    active = SimpleNamespace(is_closed=False)
    assert _emergency_cache_state(SimpleNamespace(order=lambda _: active), "flat") == (
        "WAIT",
        active,
    )
    connection.close()


def test_testnet_account_monitor_requires_balance_but_allows_trading() -> None:
    clean: Observation = {
        "nonzero_balance_assets": 1,
        "position_amount": "0",
        "orders_open": 0,
        "algo_orders_open": 0,
    }
    validate_observation(clean)
    with pytest.raises(RuntimeError, match="TESTNET_BALANCE_MISSING"):
        validate_observation({**clean, "nonzero_balance_assets": 0})
    validate_observation(
        {
            **clean,
            "position_amount": "0.0001",
            "orders_open": 1,
            "algo_orders_open": 1,
        }
    )


def test_testnet_config_fails_closed_without_local_credentials() -> None:
    assert credentials_available({}) is False
    with pytest.raises(RuntimeError, match="not installed locally"):
        binance_testnet_node_config({})


def test_testnet_config_uses_native_clients_without_copying_secrets() -> None:
    config = binance_testnet_node_config(CREDENTIALS)
    data = config.data_clients[BINANCE]
    execution = config.exec_clients[BINANCE]

    assert credentials_available(CREDENTIALS) is True
    assert data.account_type is BinanceAccountType.USDT_FUTURES
    assert execution.account_type is BinanceAccountType.USDT_FUTURES
    assert data.environment is BinanceEnvironment.TESTNET
    assert execution.environment is BinanceEnvironment.TESTNET
    assert data.api_key is data.api_secret is None
    assert execution.api_key is execution.api_secret is None
    assert execution.use_reduce_only is True
    assert config.logging.log_level == "WARNING"
    # The adapter's runtime retry behavior is version-owned; duplicate safety is
    # enforced by ALMA's deterministic client-order ID and venue reconciliation.
    assert execution.max_retries is None


def test_testnet_node_registers_native_data_and_execution_factories() -> None:
    asyncio.set_event_loop(asyncio.new_event_loop())
    node = binance_testnet_node(CREDENTIALS)
    try:
        with pytest.raises(KeyError):
            node.add_data_client_factory(BINANCE, object)
        with pytest.raises(KeyError):
            node.add_exec_client_factory(BINANCE, object)
    finally:
        node.dispose()
        asyncio.set_event_loop(None)


def test_testnet_node_registers_configured_data_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factories: list[tuple[str, object]] = []
    fake_node = SimpleNamespace(
        add_data_client_factory=lambda name, factory: factories.append((name, factory)),
        add_exec_client_factory=lambda *_: None,
    )
    monkeypatch.setattr("alma.binance_testnet.TradingNode", lambda config: fake_node)
    configured_factory = object()

    node = binance_testnet_node(CREDENTIALS, data_client_factory=configured_factory)

    assert node is fake_node
    assert factories == [(BINANCE, configured_factory)]


def test_binance_config_accepts_environment_and_instrument_profile() -> None:
    credentials = {
        "BINANCE_API_KEY": "local-key",
        "BINANCE_API_SECRET": "local-secret",
    }
    instrument = InstrumentId.from_str("ETHUSDT-PERP.BINANCE")
    config = binance_testnet_node_config(
        credentials,
        environment=BinanceEnvironment.LIVE,
        instrument_id=instrument,
    )

    assert config.data_clients[BINANCE].environment is BinanceEnvironment.LIVE
    assert config.exec_clients[BINANCE].environment is BinanceEnvironment.LIVE
    assert config.data_clients[BINANCE].instrument_provider.load_ids == frozenset(
        {instrument}
    )


def test_testnet_runtime_attaches_native_execution_strategy(tmp_path) -> None:
    asyncio.set_event_loop(asyncio.new_event_loop())
    connection = open_ledger(tmp_path / "alma.db")
    node, venue = binance_testnet_runtime(connection, CREDENTIALS)
    try:
        assert isinstance(venue, BinanceNativeVenue)
        assert venue.expected_account_id == "BINANCE-USDT_FUTURES-master"
        assert venue.cache is node.cache
        assert venue.trader_id == node.trader_id
    finally:
        node.dispose()
        connection.close()
        asyncio.set_event_loop(None)


def test_public_market_state_has_unique_nonexecution_order_tag() -> None:
    market = PublicMarketState(MarketState("BINANCE", "BTCUSDT-PERP"))
    assert market.order_id_tag == "002"


def test_native_protection_ids_are_deterministic_and_bounded() -> None:
    entry = "alma-58689daf2af4981e2ec765ef"
    assert child_order_id(entry, "trade-1", "sl") == child_order_id(
        entry, "trade-1", "sl"
    )
    assert child_order_id(entry, "trade-1", "sl") != child_order_id(
        entry, "trade-1", "tp", 0
    )
    assert len(child_order_id(entry, "trade-1", "tp", 0)) <= 36


def test_read_only_smoke_reads_native_cache_and_disposes(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []
    instrument_id = type(
        "InstrumentIdStub",
        (),
        {
            "symbol": SimpleNamespace(value="BTCUSDT-PERP"),
            "__str__": lambda self: "BTCUSDT-PERP.BINANCE",
        },
    )()
    currency = type("CurrencyStub", (), {"code": "USDT"})()
    balance = SimpleNamespace(total="1000 USDT", free="900 USDT", locked="100 USDT")
    account = SimpleNamespace(
        id="BINANCE-001",
        balances=lambda: {currency: balance},
        balance=lambda requested: balance if requested is currency else None,
    )
    instrument = SimpleNamespace(id=instrument_id)

    class FakeCache:
        def account_for_venue(self, venue):
            return account

        def instrument(self, requested):
            return instrument

        def positions(self, **kwargs):
            return [object()]

        def positions_open(self, **kwargs):
            return []

        def orders(self, **kwargs):
            return [object(), object()]

        def orders_open(self, **kwargs):
            return [object()]

    class FakeNode:
        cache = FakeCache()
        kernel = SimpleNamespace(
            data_engine=SimpleNamespace(check_connected=lambda: True),
            exec_engine=SimpleNamespace(check_connected=lambda: True),
        )

        def build(self) -> None:
            calls.append("build")

        async def run_async(self) -> None:
            calls.append("run")
            await asyncio.Event().wait()

        async def stop_async(self) -> None:
            calls.append("stop")

        def dispose(self) -> None:
            calls.append("dispose")

    venue = SimpleNamespace(instrument_id=instrument_id)
    monkeypatch.setattr(
        "alma.binance_testnet.binance_testnet_runtime",
        lambda connection, environ: (FakeNode(), venue),
    )
    monkeypatch.setattr(
        "alma.binance_testnet.binance_testnet_open_algo_orders",
        lambda symbol, environ: ({"algoId": 1},),
    )
    connection = open_ledger(tmp_path / "alma.db")
    try:
        result = binance_testnet_read_only_smoke(connection, 1, CREDENTIALS)
    finally:
        connection.close()

    assert result == {
        "environment": "TESTNET",
        "account_type": "USDT_FUTURES",
        "account_id": "BINANCE-001",
        "instrument_id": "BTCUSDT-PERP.BINANCE",
        "balances": (("USDT", "1000 USDT", "900 USDT", "100 USDT"),),
        "positions": 1,
        "positions_open": 0,
        "orders": 2,
        "orders_open": 1,
        "algo_orders_open": 1,
    }
    assert calls == ["build", "run", "stop", "dispose"]


def test_read_only_evidence_is_atomic_private_and_non_mutating(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    observed = {
        "environment": "TESTNET",
        "account_type": "USDT_FUTURES",
        "account_id": "BINANCE-001",
        "instrument_id": "BTCUSDT-PERP.BINANCE",
        "balances": (),
        "positions": 0,
        "positions_open": 0,
        "orders": 0,
        "orders_open": 0,
        "algo_orders_open": 0,
    }
    monkeypatch.setattr(
        "alma.binance_testnet.binance_testnet_read_only_smoke",
        lambda connection, timeout: observed,
    )
    path = tmp_path / "evidence.json"

    evidence = write_read_only_evidence(path, 1)

    assert evidence == {
        "passed": True,
        "read_only": True,
        "mutation_attempted": False,
        "observed": observed,
        "error": None,
    }
    assert path.stat().st_mode & 0o777 == 0o600
    assert not tuple(tmp_path.glob(".*.tmp"))
