import asyncio
import time
from collections.abc import Callable
from dataclasses import replace
from decimal import Decimal

from nautilus_trader.adapters.binance import (
    BINANCE,
    BinanceAccountType,
    BinanceDataClientConfig,
    BinanceLiveDataClientFactory,
)
from nautilus_trader.adapters.binance.common.enums import BinanceEnvironment
from nautilus_trader.config import (
    InstrumentProviderConfig,
    StrategyConfig,
    TradingNodeConfig,
)
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.data import (
    FundingRateUpdate,
    MarkPriceUpdate,
    OrderBookDeltas,
    QuoteTick,
    TradeTick,
)
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy

from alma.book_sequence import BookSequenceResult
from alma.market_recording import MarketEvent, ParquetRecorder
from alma.market_state import MarketState

BTCUSDT_USDM = InstrumentId.from_str("BTCUSDT-PERP.BINANCE")


class FundingSettlementTracker:
    def __init__(self, venue: str, symbol: str) -> None:
        self.venue = venue
        self.symbol = symbol
        self._next_ns: int | None = None
        self._rate: Decimal | None = None

    def update(
        self,
        *,
        event_ns: int,
        next_funding_ns: int,
        mark: Decimal,
        rate: Decimal,
    ) -> tuple[MarketEvent, ...]:
        events: list[MarketEvent] = []
        if (
            self._next_ns is not None
            and self._rate is not None
            and event_ns >= self._next_ns
            and next_funding_ns > self._next_ns
        ):
            events.append(
                MarketEvent.funding_settlement(
                    self._next_ns,
                    self.venue,
                    self.symbol,
                    self._rate,
                    mark,
                )
            )
        events.append(MarketEvent.funding(event_ns, self.venue, self.symbol, rate))
        self._next_ns = next_funding_ns
        self._rate = rate
        return tuple(events)


class FirstTradeTick(Strategy):
    def __init__(self, instrument_id: InstrumentId = BTCUSDT_USDM) -> None:
        super().__init__(config=StrategyConfig(strategy_id="FIRST-TRADE-001"))
        self.instrument_id = instrument_id
        self.first_tick: TradeTick | None = None

    def on_start(self) -> None:
        self.subscribe_trade_ticks(self.instrument_id)

    def on_trade_tick(self, tick: TradeTick) -> None:
        if self.first_tick is None:
            self.first_tick = tick


class FirstBookStream(Strategy):
    def __init__(self, instrument_id: InstrumentId = BTCUSDT_USDM) -> None:
        super().__init__(config=StrategyConfig(strategy_id="FIRST-BOOK-001"))
        self.instrument_id = instrument_id
        self.snapshot: OrderBookDeltas | None = None
        self.delta: OrderBookDeltas | None = None

    def on_start(self) -> None:
        self.subscribe_order_book_deltas(self.instrument_id, managed=True)

    def on_order_book_deltas(self, deltas: OrderBookDeltas) -> None:
        if deltas.is_snapshot:
            if self.snapshot is None:
                self.snapshot = deltas
        elif self.delta is None:
            self.delta = deltas


class RawBookGuard:
    def __init__(
        self,
        state: MarketState,
        recorder: ParquetRecorder | None = None,
    ) -> None:
        self.state = state
        self.recorder = recorder
        self.awaiting_snapshot = True
        self._buffer: list[MarketEvent] = []
        self._resnapshot: Callable[[], None] | None = None
        self._resnapshot_pending = False
        self._last_timestamp_ns = 0

    def _timestamp(self, ts_event_ns: int) -> int:
        if ts_event_ns < 0:
            raise ValueError("timestamp must be non-negative")
        self._last_timestamp_ns = max(self._last_timestamp_ns, ts_event_ns)
        return self._last_timestamp_ns

    def bind_resnapshot(self, callback: Callable[[], None]) -> None:
        self._resnapshot = callback

    def on_raw(
        self,
        ts_event_ns: int,
        *,
        first: int,
        final: int,
        previous_final: int | None,
    ) -> bool:
        if previous_final is None:
            self._request_resnapshot(ts_event_ns)
            return False
        event = MarketEvent.book_delta(
            ts_event_ns,
            self.state.venue,
            self.state.symbol,
            first=first,
            final=final,
            previous_final=previous_final,
        )
        if self.awaiting_snapshot:
            self._buffer.append(event)
            return True
        return self._apply(event)

    def on_snapshot(self, ts_event_ns: int, last_update_id: int) -> None:
        snapshot_ns = min(
            (event.ts_event_ns for event in self._buffer),
            default=ts_event_ns,
        )
        event = MarketEvent.book_snapshot(
            snapshot_ns,
            self.state.venue,
            self.state.symbol,
            last_update_id,
        )
        self._record_apply(event)
        self.awaiting_snapshot = False
        self._resnapshot_pending = False
        buffered, self._buffer = self._buffer, []
        for delta in buffered:
            if not self._apply(delta):
                break

    def _apply(self, event: MarketEvent) -> bool:
        result = self._record_apply(event)
        if result is BookSequenceResult.GAP:
            self._request_resnapshot(event.ts_event_ns)
            return False
        return True

    def _record_apply(self, event: MarketEvent) -> BookSequenceResult | None:
        event = replace(event, ts_event_ns=self._timestamp(event.ts_event_ns))
        if self.recorder is not None:
            self.recorder.record(event)
        return event.apply(self.state)

    def _request_resnapshot(self, ts_event_ns: int) -> None:
        self.awaiting_snapshot = True
        self._buffer.clear()
        self._record_apply(
            MarketEvent.book_invalidate(
                ts_event_ns,
                self.state.venue,
                self.state.symbol,
            )
        )
        if not self._resnapshot_pending and self._resnapshot is not None:
            self._resnapshot_pending = True
            self._resnapshot()


class PublicMarketState(Strategy):
    def __init__(
        self,
        state: MarketState,
        recorder: ParquetRecorder | None = None,
        book_guard: RawBookGuard | None = None,
        instrument_id: InstrumentId = BTCUSDT_USDM,
    ) -> None:
        super().__init__(
            config=StrategyConfig(strategy_id="PUBLIC-STATE-001", order_id_tag="002")
        )
        self._market_state = state
        self._recorder = recorder
        self._book_guard = book_guard
        self.instrument_id = instrument_id

    def on_start(self) -> None:
        self.subscribe_quote_ticks(self.instrument_id)
        self.subscribe_trade_ticks(self.instrument_id)
        self.subscribe_mark_prices(self.instrument_id)
        if self._book_guard is not None:
            self.subscribe_order_book_deltas(self.instrument_id, managed=True)

    def _apply(self, event: MarketEvent) -> None:
        started = time.perf_counter_ns()
        if self._recorder is not None:
            self._recorder.record(event)
        event.apply(self._market_state)
        self._market_state.metrics.observe_latency(time.perf_counter_ns() - started)

    def on_quote_tick(self, tick: QuoteTick) -> None:
        self._apply(
            MarketEvent.quote(
                tick.ts_event,
                self._market_state.venue,
                self._market_state.symbol,
                tick.bid_price.as_decimal(),
                tick.ask_price.as_decimal(),
                tick.bid_size.as_decimal(),
                tick.ask_size.as_decimal(),
            )
        )

    def on_trade_tick(self, tick: TradeTick) -> None:
        aggressor = {
            "BUYER": 1,
            "SELLER": -1,
        }.get(tick.aggressor_side.name, 0)
        self._apply(
            MarketEvent.trade(
                tick.ts_event,
                self._market_state.venue,
                self._market_state.symbol,
                tick.price.as_decimal(),
                tick.size.as_decimal(),
                aggressor,
            )
        )

    def on_mark_price(self, update: MarkPriceUpdate) -> None:
        self._apply(
            MarketEvent.mark(
                update.ts_event,
                self._market_state.venue,
                self._market_state.symbol,
                update.value.as_decimal(),
            )
        )

    def on_funding_rate(self, update: FundingRateUpdate) -> None:
        self._apply(
            MarketEvent.funding(
                update.ts_event,
                self._market_state.venue,
                self._market_state.symbol,
                update.rate,
            )
        )

    def on_order_book_deltas(self, deltas: OrderBookDeltas) -> None:
        if self._book_guard is not None and deltas.is_snapshot:
            self._book_guard.on_snapshot(deltas.ts_event, deltas.sequence)


def public_usdm_node_config() -> TradingNodeConfig:
    return TradingNodeConfig(
        data_clients={
            BINANCE: BinanceDataClientConfig(
                account_type=BinanceAccountType.USDT_FUTURES,
                environment=BinanceEnvironment.LIVE,
                instrument_provider=InstrumentProviderConfig(
                    load_ids=frozenset({BTCUSDT_USDM}),
                ),
            ),
        },
    )


def public_usdm_node() -> TradingNode:
    node = TradingNode(config=public_usdm_node_config())
    node.add_data_client_factory(BINANCE, BinanceLiveDataClientFactory)
    return node


def _guarded_binance_factory(
    guard: RawBookGuard,
    state: MarketState,
    recorder: ParquetRecorder | None,
    instrument_id: InstrumentId = BTCUSDT_USDM,
) -> type:
    class GuardedBinanceDataClientFactory(LiveDataClientFactory):
        @staticmethod
        def create(*args, **kwargs):
            client = BinanceLiveDataClientFactory.create(*args, **kwargs)
            original_depth = client._ws_handlers["@depth@"]
            original_mark = client._ws_handlers["@markPrice"]
            funding = FundingSettlementTracker(state.venue, state.symbol)

            def handle(raw: bytes) -> None:
                message = client._decoder_order_book_msg.decode(raw)
                data = message.data
                ts_event_ns = (data.T if data.T is not None else data.E) * 1_000_000
                if guard.on_raw(
                    ts_event_ns,
                    first=data.U,
                    final=data.u,
                    previous_final=data.pu,
                ):
                    original_depth(raw)

            def handle_mark(raw: bytes) -> None:
                message = client._decoder_futures_mark_price_msg.decode(raw)
                data = message.data
                for event in funding.update(
                    event_ns=data.E * 1_000_000,
                    next_funding_ns=data.T * 1_000_000,
                    mark=Decimal(data.p),
                    rate=Decimal(data.r),
                ):
                    if recorder is not None:
                        recorder.record(event)
                    event.apply(state)
                original_mark(raw)

            guard.bind_resnapshot(
                lambda: client._loop.create_task(
                    client._order_book_snapshot_then_deltas(instrument_id)
                )
            )
            client._ws_handlers["@depth@"] = handle
            client._ws_handlers["@markPrice"] = handle_mark
            for websocket in (client._ws_client, client._ws_public_client):
                original_reconnect = websocket._handler_reconnect

                async def reconnect(original=original_reconnect) -> None:
                    event = MarketEvent.reconnect(
                        client._clock.timestamp_ns(),
                        state.venue,
                        state.symbol,
                    )
                    if recorder is not None:
                        recorder.record(event)
                    event.apply(state)
                    if original is not None:
                        await original()

                websocket._handler_reconnect = reconnect
            return client

    return GuardedBinanceDataClientFactory


def public_usdm_state_node(
    state: MarketState | None = None,
    recorder: ParquetRecorder | None = None,
) -> tuple[TradingNode, PublicMarketState, RawBookGuard]:
    state = state or MarketState("BINANCE", "BTCUSDT-PERP")
    guard = RawBookGuard(state, recorder)
    strategy = PublicMarketState(state, recorder, guard)
    node = TradingNode(config=public_usdm_node_config())
    node.add_data_client_factory(
        BINANCE,
        _guarded_binance_factory(guard, state, recorder),
    )
    node.trader.add_strategy(strategy)
    return node, strategy, guard


def public_usdm_trade_node(
    tracer: FirstTradeTick | None = None,
) -> tuple[TradingNode, FirstTradeTick]:
    node = public_usdm_node()
    tracer = tracer or FirstTradeTick()
    node.trader.add_strategy(tracer)
    return node, tracer


def public_usdm_book_node(
    tracer: FirstBookStream | None = None,
) -> tuple[TradingNode, FirstBookStream]:
    node = public_usdm_node()
    tracer = tracer or FirstBookStream()
    node.trader.add_strategy(tracer)
    return node, tracer


def public_usdm_first_book_smoke(
    timeout: float,
) -> tuple[OrderBookDeltas, OrderBookDeltas] | None:
    node = None

    async def receive() -> tuple[OrderBookDeltas, OrderBookDeltas] | None:
        nonlocal node
        node, tracer = public_usdm_book_node()
        node.build()
        run_task = asyncio.create_task(node.run_async())
        try:
            async with asyncio.timeout(timeout):
                await asyncio.sleep(0)
                while True:
                    if run_task.done():
                        await run_task
                    if tracer.snapshot is not None and tracer.delta is not None:
                        return tracer.snapshot, tracer.delta
                    await asyncio.sleep(0.01)
        except TimeoutError:
            return None
        finally:
            run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)
            await node.stop_async()

    try:
        with asyncio.Runner() as runner:
            return runner.run(receive())
    finally:
        if node is not None:
            node.dispose()


def public_usdm_first_trade_smoke(timeout: float) -> TradeTick | None:
    node = None

    async def receive() -> TradeTick | None:
        nonlocal node
        node, tracer = public_usdm_trade_node()
        node.build()
        run_task = asyncio.create_task(node.run_async())
        try:
            async with asyncio.timeout(timeout):
                await asyncio.sleep(0)
                while True:
                    if run_task.done():
                        await run_task
                    if tracer.first_tick is not None:
                        return tracer.first_tick
                    await asyncio.sleep(0.01)
        except TimeoutError:
            return None
        finally:
            run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)
            await node.stop_async()

    try:
        with asyncio.Runner() as runner:
            return runner.run(receive())
    finally:
        if node is not None:
            node.dispose()


def public_usdm_connectivity_smoke(timeout: float) -> bool:
    node = None

    async def connect() -> bool:
        nonlocal node
        node = public_usdm_node()
        node.build()
        run_task = asyncio.create_task(node.run_async())
        deadline = asyncio.timeout(timeout)
        try:
            async with deadline:
                await asyncio.sleep(0)
                while True:
                    if run_task.done():
                        await run_task
                    if node.kernel.data_engine.check_connected():
                        return True
                    await asyncio.sleep(0.01)
        except TimeoutError:
            if deadline.expired():
                return False
            raise
        finally:
            run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)
            await node.stop_async()

    try:
        with asyncio.Runner() as runner:
            return runner.run(connect())
    finally:
        if node is not None:
            node.dispose()
