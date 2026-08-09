import asyncio
from decimal import Decimal
from types import SimpleNamespace

import pytest
from nautilus_trader.adapters.binance import (
    BINANCE,
    BinanceAccountType,
    BinanceLiveDataClientFactory,
)
from nautilus_trader.adapters.binance.common.enums import BinanceEnvironment
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.model.identifiers import InstrumentId

from alma import binance_data
from alma.binance_data import (
    FirstBookStream,
    FirstTradeTick,
    FundingSettlementTracker,
    PublicMarketState,
    RawBookGuard,
    public_usdm_book_node,
    public_usdm_node,
    public_usdm_node_config,
    public_usdm_state_node,
    public_usdm_trade_node,
)
from alma.market_state import MarketState


def test_public_usdm_node_has_data_client_without_execution_client() -> None:
    config = public_usdm_node_config()

    data_config = config.data_clients[BINANCE]
    assert data_config.account_type == BinanceAccountType.USDT_FUTURES
    assert data_config.environment == BinanceEnvironment.LIVE
    assert data_config.api_key is None
    assert data_config.api_secret is None
    assert data_config.instrument_provider.load_ids == frozenset(
        {InstrumentId.from_str("BTCUSDT-PERP.BINANCE")},
    )
    assert config.exec_clients == {}


def test_public_usdm_node_registers_binance_data_factory() -> None:
    asyncio.set_event_loop(asyncio.new_event_loop())
    node = public_usdm_node()
    try:
        with pytest.raises(KeyError):
            node.add_data_client_factory(BINANCE, BinanceLiveDataClientFactory)
    finally:
        node.dispose()
        asyncio.set_event_loop(None)


def test_first_trade_tick_subscribes_to_btcusdt() -> None:
    tracer = FirstTradeTick()
    subscribed: list[InstrumentId] = []
    tracer.subscribe_trade_ticks = subscribed.append

    tracer.on_start()

    assert subscribed == [InstrumentId.from_str("BTCUSDT-PERP.BINANCE")]


def test_first_trade_tick_keeps_only_first_event() -> None:
    tracer = FirstTradeTick()
    first = object()

    tracer.on_trade_tick(first)
    tracer.on_trade_tick(object())

    assert tracer.first_tick is first


def test_public_usdm_trade_node_registers_tracer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracer = FirstTradeTick()
    added: list[FirstTradeTick] = []
    fake_node = SimpleNamespace(
        trader=SimpleNamespace(add_strategy=added.append),
    )
    monkeypatch.setattr(binance_data, "public_usdm_node", lambda: fake_node)

    node, returned_tracer = public_usdm_trade_node(tracer)

    assert node is fake_node
    assert returned_tracer is tracer
    assert added == [tracer]


def test_first_book_stream_subscribes_to_btcusdt() -> None:
    tracer = FirstBookStream()
    subscribed: list[tuple[InstrumentId, dict[str, object]]] = []
    tracer.subscribe_order_book_deltas = lambda instrument_id, **kwargs: (
        subscribed.append(
            (instrument_id, kwargs),
        )
    )

    tracer.on_start()

    assert subscribed == [
        (InstrumentId.from_str("BTCUSDT-PERP.BINANCE"), {"managed": True}),
    ]


def test_first_book_stream_keeps_first_snapshot_then_first_delta() -> None:
    tracer = FirstBookStream()
    snapshot = SimpleNamespace(is_snapshot=True)
    delta = SimpleNamespace(is_snapshot=False)

    tracer.on_order_book_deltas(snapshot)
    tracer.on_order_book_deltas(SimpleNamespace(is_snapshot=True))
    tracer.on_order_book_deltas(delta)
    tracer.on_order_book_deltas(SimpleNamespace(is_snapshot=False))

    assert tracer.snapshot is snapshot
    assert tracer.delta is delta


def test_public_usdm_book_node_registers_tracer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracer = FirstBookStream()
    added: list[FirstBookStream] = []
    fake_node = SimpleNamespace(trader=SimpleNamespace(add_strategy=added.append))
    monkeypatch.setattr(binance_data, "public_usdm_node", lambda: fake_node)

    node, returned_tracer = public_usdm_book_node(tracer)

    assert node is fake_node
    assert returned_tracer is tracer
    assert added == [tracer]


def test_book_smoke_returns_snapshot_and_delta_then_disposes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    snapshot = object()
    delta = object()
    tracer = SimpleNamespace(snapshot=snapshot, delta=delta)

    class FakeNode:
        def build(self) -> None:
            calls.append("build")

        async def run_async(self) -> None:
            calls.append("run")
            await asyncio.Event().wait()

        async def stop_async(self) -> None:
            calls.append("stop")

        def dispose(self) -> None:
            calls.append("dispose")

    monkeypatch.setattr(
        binance_data,
        "public_usdm_book_node",
        lambda: (FakeNode(), tracer),
    )

    assert binance_data.public_usdm_first_book_smoke(1) == (snapshot, delta)
    assert calls == ["build", "run", "stop", "dispose"]


def test_book_smoke_times_out_and_disposes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    tracer = SimpleNamespace(snapshot=object(), delta=None)

    class FakeNode:
        def build(self) -> None:
            calls.append("build")

        async def run_async(self) -> None:
            calls.append("run")
            await asyncio.Event().wait()

        async def stop_async(self) -> None:
            calls.append("stop")

        def dispose(self) -> None:
            calls.append("dispose")

    monkeypatch.setattr(
        binance_data,
        "public_usdm_book_node",
        lambda: (FakeNode(), tracer),
    )

    assert binance_data.public_usdm_first_book_smoke(0.001) is None
    assert calls == ["build", "run", "stop", "dispose"]


def test_trade_smoke_returns_first_tick_and_disposes_node(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    tick = object()
    tracer = SimpleNamespace(first_tick=tick)

    class FakeNode:
        def build(self) -> None:
            calls.append("build")

        async def run_async(self) -> None:
            calls.append("run")
            await asyncio.Event().wait()

        async def stop_async(self) -> None:
            calls.append("stop")

        def dispose(self) -> None:
            calls.append("dispose")

    monkeypatch.setattr(
        binance_data,
        "public_usdm_trade_node",
        lambda: (FakeNode(), tracer),
    )

    assert binance_data.public_usdm_first_trade_smoke(1) is tick
    assert calls == ["build", "run", "stop", "dispose"]


def test_trade_smoke_times_out_and_disposes_node(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    tracer = SimpleNamespace(first_tick=None)

    class FakeNode:
        def build(self) -> None:
            calls.append("build")

        async def run_async(self) -> None:
            calls.append("run")
            await asyncio.Event().wait()

        async def stop_async(self) -> None:
            calls.append("stop")

        def dispose(self) -> None:
            calls.append("dispose")

    monkeypatch.setattr(
        binance_data,
        "public_usdm_trade_node",
        lambda: (FakeNode(), tracer),
    )

    assert binance_data.public_usdm_first_trade_smoke(0.001) is None
    assert calls == ["build", "run", "stop", "dispose"]


def test_trade_smoke_prefers_startup_failure_over_available_tick(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracer = SimpleNamespace(first_tick=object())

    class FakeNode:
        def build(self) -> None:
            pass

        async def run_async(self) -> None:
            raise RuntimeError("startup failed")

        async def stop_async(self) -> None:
            pass

        def dispose(self) -> None:
            pass

    monkeypatch.setattr(
        binance_data,
        "public_usdm_trade_node",
        lambda: (FakeNode(), tracer),
    )

    with pytest.raises(RuntimeError, match="startup failed"):
        binance_data.public_usdm_first_trade_smoke(1)


def test_connectivity_smoke_times_out_and_disposes_node(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    class FakeNode:
        kernel = SimpleNamespace(
            data_engine=SimpleNamespace(check_connected=lambda: False),
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

    monkeypatch.setattr(binance_data, "public_usdm_node", FakeNode)

    assert binance_data.public_usdm_connectivity_smoke(0.001) is False
    assert calls == ["build", "run", "stop", "dispose"]


def test_connectivity_smoke_propagates_startup_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    class FakeNode:
        kernel = SimpleNamespace(
            data_engine=SimpleNamespace(check_connected=lambda: False),
        )

        def build(self) -> None:
            calls.append("build")

        async def run_async(self) -> None:
            calls.append("run")
            raise RuntimeError("startup failed")

        async def stop_async(self) -> None:
            calls.append("stop")

        def dispose(self) -> None:
            calls.append("dispose")

    monkeypatch.setattr(binance_data, "public_usdm_node", FakeNode)

    with pytest.raises(RuntimeError, match="startup failed"):
        binance_data.public_usdm_connectivity_smoke(1)
    assert calls == ["build", "run", "stop", "dispose"]


def test_connectivity_smoke_propagates_startup_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeNode:
        kernel = SimpleNamespace(
            data_engine=SimpleNamespace(check_connected=lambda: False),
        )

        def build(self) -> None:
            pass

        async def run_async(self) -> None:
            raise TimeoutError("startup timed out")

        async def stop_async(self) -> None:
            pass

        def dispose(self) -> None:
            pass

    monkeypatch.setattr(binance_data, "public_usdm_node", FakeNode)

    with pytest.raises(TimeoutError, match="startup timed out"):
        binance_data.public_usdm_connectivity_smoke(1)


def test_connectivity_smoke_prefers_startup_failure_over_connected_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeNode:
        kernel = SimpleNamespace(
            data_engine=SimpleNamespace(check_connected=lambda: True),
        )

        def build(self) -> None:
            pass

        async def run_async(self) -> None:
            raise RuntimeError("failed after connect")

        async def stop_async(self) -> None:
            pass

        def dispose(self) -> None:
            pass

    monkeypatch.setattr(binance_data, "public_usdm_node", FakeNode)

    with pytest.raises(RuntimeError, match="failed after connect"):
        binance_data.public_usdm_connectivity_smoke(1)


def test_public_market_state_subscribes_to_native_public_events() -> None:
    strategy = PublicMarketState(MarketState("BINANCE", "BTCUSDT-PERP"))
    calls: list[tuple[str, InstrumentId]] = []
    strategy.subscribe_quote_ticks = lambda instrument_id: calls.append(
        ("quote", instrument_id)
    )
    strategy.subscribe_trade_ticks = lambda instrument_id: calls.append(
        ("trade", instrument_id)
    )
    strategy.subscribe_mark_prices = lambda instrument_id: calls.append(
        ("mark", instrument_id)
    )

    strategy.on_start()

    assert calls == [
        (kind, InstrumentId.from_str("BTCUSDT-PERP.BINANCE"))
        for kind in ("quote", "trade", "mark")
    ]


def test_public_market_state_maps_native_events_to_shared_state() -> None:
    state = MarketState("BINANCE", "BTCUSDT-PERP")
    strategy = PublicMarketState(state)

    def value(number: Decimal) -> SimpleNamespace:
        return SimpleNamespace(as_decimal=lambda: number)

    strategy.on_quote_tick(
        SimpleNamespace(
            ts_event=0,
            bid_price=value(Decimal(100)),
            ask_price=value(Decimal(101)),
            bid_size=value(Decimal(2)),
            ask_size=value(Decimal(1)),
        )
    )
    strategy.on_trade_tick(
        SimpleNamespace(
            ts_event=1,
            price=value(Decimal("100.5")),
            size=value(Decimal("0.2")),
            aggressor_side=SimpleNamespace(name="BUYER"),
        )
    )
    strategy.on_mark_price(SimpleNamespace(ts_event=1, value=value(Decimal("100.4"))))
    strategy.on_funding_rate(SimpleNamespace(ts_event=1, rate=Decimal("0.0001")))

    snapshot = state.snapshot(1)
    assert (snapshot.bid, snapshot.ask, snapshot.mark_price, snapshot.funding_rate) == (
        Decimal(100),
        Decimal(101),
        Decimal("100.4"),
        Decimal("0.0001"),
    )
    assert snapshot.flow_imbalance == 1


def test_public_market_state_subscribes_to_configured_instrument() -> None:
    instrument = InstrumentId.from_str("ETHUSDT-PERP.BINANCE")
    strategy = PublicMarketState(
        MarketState("BINANCE", "ETHUSDT-PERP"), instrument_id=instrument
    )
    calls: list[InstrumentId] = []
    strategy.subscribe_quote_ticks = calls.append
    strategy.subscribe_trade_ticks = calls.append
    strategy.subscribe_mark_prices = calls.append

    strategy.on_start()

    assert calls == [instrument, instrument, instrument]


def test_funding_tracker_emits_one_settlement_when_boundary_advances() -> None:
    tracker = FundingSettlementTracker("BINANCE", "BTCUSDT-PERP")

    first = tracker.update(
        event_ns=90,
        next_funding_ns=100,
        mark=Decimal(10),
        rate=Decimal("0.001"),
    )
    boundary = tracker.update(
        event_ns=101,
        next_funding_ns=200,
        mark=Decimal(11),
        rate=Decimal("0.002"),
    )
    repeated = tracker.update(
        event_ns=102,
        next_funding_ns=200,
        mark=Decimal(12),
        rate=Decimal("0.002"),
    )

    assert tuple(event.kind for event in first) == ("funding",)
    assert tuple(event.kind for event in boundary) == (
        "funding_settlement",
        "funding",
    )
    assert (boundary[0].ts_event_ns, boundary[0].price, boundary[0].rate) == (
        100,
        Decimal(11),
        Decimal("0.001"),
    )
    assert tuple(event.kind for event in repeated) == ("funding",)


def test_raw_book_guard_gap_requests_one_native_resnapshot() -> None:
    state = MarketState("BINANCE", "BTCUSDT-PERP")
    guard = RawBookGuard(state)
    calls: list[str] = []
    guard.bind_resnapshot(lambda: calls.append("resnapshot"))
    guard.on_snapshot(0, 100)

    assert guard.on_raw(1, first=99, final=105, previous_final=98) is True
    assert guard.on_raw(2, first=106, final=110, previous_final=104) is False
    assert guard.on_raw(3, first=111, final=115, previous_final=110) is True
    assert calls == ["resnapshot"]
    assert state.snapshot(3).book_valid is False
    assert state.metrics.reconnect_count == 0


def test_raw_book_guard_uses_sequence_when_snapshot_timestamps_reorder() -> None:
    state = MarketState("BINANCE", "BTCUSDT-PERP")
    guard = RawBookGuard(state)

    guard.on_snapshot(10, 100)
    guard.on_snapshot(9, 101)
    guard.on_snapshot(11, 99)

    assert guard.on_raw(12, first=101, final=102, previous_final=100) is True
    snapshot = state.snapshot(12)
    assert snapshot.book_valid is True
    assert snapshot.observed_at_ns == 12


def test_raw_book_guard_missing_pu_fails_closed() -> None:
    state = MarketState("BINANCE", "BTCUSDT-PERP")
    guard = RawBookGuard(state)
    calls: list[str] = []
    guard.bind_resnapshot(lambda: calls.append("resnapshot"))

    assert guard.on_raw(0, first=1, final=2, previous_final=None) is False
    assert calls == ["resnapshot"]
    assert state.snapshot(0).book_valid is False
    assert state.metrics.reconnect_count == 0


def test_raw_book_guard_bridges_delta_buffered_before_snapshot_receipt() -> None:
    state = MarketState("BINANCE", "BTCUSDT-PERP")
    guard = RawBookGuard(state)

    assert guard.on_raw(10, first=99, final=105, previous_final=98) is True
    guard.on_snapshot(20, 100)

    assert state.snapshot(20).book_valid is True


def test_state_node_registers_strategy_and_raw_guard_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    added: list[object] = []
    factories: list[tuple[str, object]] = []
    fake_node = SimpleNamespace(
        trader=SimpleNamespace(add_strategy=added.append),
        add_data_client_factory=lambda name, factory: factories.append((name, factory)),
    )
    monkeypatch.setattr(binance_data, "TradingNode", lambda config: fake_node)

    node, strategy, guard = public_usdm_state_node()

    assert node is fake_node
    assert added == [strategy]
    assert strategy._book_guard is guard
    assert factories[0][0] == BINANCE
    assert issubclass(factories[0][1], LiveDataClientFactory)


def test_guarded_factory_resnapshots_configured_instrument(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instrument = InstrumentId.from_str("ETHUSDT-PERP.BINANCE")
    requested: list[InstrumentId] = []
    scheduled: list[object] = []
    websocket = SimpleNamespace(_handler_reconnect=None)
    client = SimpleNamespace(
        _ws_handlers={"@depth@": lambda raw: None, "@markPrice": lambda raw: None},
        _ws_client=websocket,
        _ws_public_client=SimpleNamespace(_handler_reconnect=None),
        _loop=SimpleNamespace(create_task=scheduled.append),
        _order_book_snapshot_then_deltas=lambda value: requested.append(value),
        _clock=SimpleNamespace(timestamp_ns=lambda: 0),
    )
    monkeypatch.setattr(
        binance_data.BinanceLiveDataClientFactory,
        "create",
        lambda *args, **kwargs: client,
    )
    state = MarketState("BINANCE", "ETHUSDT-PERP")
    guard = RawBookGuard(state)

    factory = binance_data._guarded_binance_factory(guard, state, None, instrument)
    factory.create()
    guard._resnapshot()

    assert requested == [instrument]
    assert scheduled == [None]
