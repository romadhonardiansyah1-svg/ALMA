import hashlib
import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from nautilus_trader.accounting.accounts.margin import MarginAccount
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.enums import OrderSide, TimeInForce, TriggerType
from nautilus_trader.model.events import (
    OrderAccepted,
    OrderCanceled,
    OrderExpired,
    OrderFilled,
    OrderRejected,
    PositionClosed,
)
from nautilus_trader.model.identifiers import (
    ClientOrderId,
    InstrumentId,
    Venue,
)
from nautilus_trader.trading.strategy import Strategy

from alma.decision_contract import parse_decision_contract
from alma.execution import (
    ExecutionRejected,
    ExecutionTruth,
    InstrumentRules,
    OrderRequest,
    ProtectedSubmission,
    ProtectionOrder,
    VenueOrder,
)
from alma.ledger import append_order_event, reserve_order_submission
from alma.mutation_gate import MutationGate, VenueTruth
from alma.nautilus_fill import child_order_id, parent_for_child, record_native_fill
from alma.venue_modes import OpenPositionPolicy, VenueMode

_ACTIVE = {"ACCEPTED", "PARTIALLY_FILLED", "TRIGGERED"}
_TERMINAL = {"CANCELED", "EXPIRED", "FILLED", "REJECTED", "DENIED"}
ENTRY_TIME_IN_FORCE = TimeInForce.GTC


def _utc(ns: int) -> datetime:
    seconds, nanoseconds = divmod(ns, 1_000_000_000)
    return datetime.fromtimestamp(seconds, UTC).replace(
        microsecond=nanoseconds // 1_000
    )


def _signed_position_quantity(position) -> Decimal:
    return position.signed_decimal_qty()


def _owned_order(
    connection: sqlite3.Connection, order, *, venue: str, symbol: str
) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM order_events o "
            "JOIN intents i ON i.intent_id = o.intent_id "
            "WHERE o.order_id = ? AND i.venue = ? AND i.symbol = ? LIMIT 1",
            (str(order.client_order_id), venue, symbol),
        ).fetchone()
        is not None
    )


def _owned_execution_order(
    connection: sqlite3.Connection, order, *, venue: str, symbol: str
) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM order_events o "
            "JOIN intents i ON i.intent_id = o.intent_id "
            "WHERE o.order_id = ? AND i.venue = ? AND i.symbol = ? "
            "AND EXISTS (SELECT 1 FROM order_events submitted "
            "WHERE submitted.order_id = o.order_id "
            "AND (submitted.event_id LIKE 'submitted:%' "
            "OR submitted.event_id LIKE 'emergency-submitted:%')) LIMIT 1",
            (str(order.client_order_id), venue, symbol),
        ).fetchone()
        is not None
    )


def _record_entry_rejection(
    connection: sqlite3.Connection, event: OrderRejected
) -> bool:
    order_id = str(event.client_order_id)
    row = connection.execute(
        "SELECT intent_id, status, quantity, filled_quantity, price "
        "FROM order_events WHERE order_id = ? ORDER BY seq DESC LIMIT 1",
        (order_id,),
    ).fetchone()
    if row is None or row[1] in _TERMINAL:
        return False
    append_order_event(
        connection,
        event_id=f"rejected:{event.id}",
        intent_id=row[0],
        order_id=order_id,
        status="REJECTED",
        quantity=Decimal(row[2]),
        filled_quantity=Decimal(row[3]),
        price=None if row[4] is None else Decimal(row[4]),
        created_at=_utc(event.ts_event).isoformat(),
    )
    return True


def _record_order_status(
    connection: sqlite3.Connection,
    event: OrderAccepted | OrderCanceled | OrderExpired,
    status: str,
) -> bool:
    order_id = str(event.client_order_id)
    row = connection.execute(
        "SELECT intent_id, status, quantity, filled_quantity, price "
        "FROM order_events WHERE order_id = ? ORDER BY seq DESC LIMIT 1",
        (order_id,),
    ).fetchone()
    if (
        row is None
        or row[1] == status
        or row[1] in _TERMINAL
        or (status == "ACCEPTED" and row[1] != "SUBMITTED")
    ):
        return False
    append_order_event(
        connection,
        event_id=f"{status.lower()}:{event.id}",
        intent_id=row[0],
        order_id=order_id,
        status=status,
        quantity=Decimal(row[2]),
        filled_quantity=Decimal(row[3]),
        price=None if row[4] is None else Decimal(row[4]),
        created_at=_utc(event.ts_event).isoformat(),
    )
    return True


def _intent_reduces_position(connection: sqlite3.Connection, order_id: str) -> bool:
    row = connection.execute(
        "SELECT i.actual_quantity, i.execution_delta FROM order_events o "
        "JOIN intents i ON i.intent_id = o.intent_id "
        "WHERE o.order_id = ? ORDER BY o.seq LIMIT 1",
        (order_id,),
    ).fetchone()
    if row is None:
        return False
    actual, delta = Decimal(row[0]), Decimal(row[1])
    return actual != 0 and actual * delta < 0 and abs(delta) <= abs(actual)


def _pending_emergency(
    connection: sqlite3.Connection, *, venue: str, symbol: str
) -> str | None:
    row = connection.execute(
        "SELECT current.order_id FROM order_events current "
        "JOIN intents i ON i.intent_id = current.intent_id "
        "WHERE i.venue = ? AND i.symbol = ? "
        "AND current.event_id LIKE 'emergency-submitted:%' "
        "AND current.seq = (SELECT max(later.seq) FROM order_events later "
        "WHERE later.order_id = current.order_id) "
        "AND current.status NOT IN ('FILLED','CANCELED','REJECTED','EXPIRED') LIMIT 1",
        (venue, symbol),
    ).fetchone()
    return None if row is None else str(row[0])


def _emergency_cache_state(cache, order_id: str) -> tuple[str, object | None]:
    order = cache.order(ClientOrderId(order_id))
    if order is None:
        return "RESUBMIT", None
    return ("TERMINAL" if order.is_closed else "WAIT"), order


class BinanceNativeVenue(Strategy):
    def __init__(
        self,
        connection: sqlite3.Connection,
        instrument_id: InstrumentId,
        connected: Callable[[], bool],
        expected_account_id: str,
    ) -> None:
        if not expected_account_id or len(expected_account_id) > 128:
            raise ValueError("expected Binance account identity is required")
        super().__init__(
            config=StrategyConfig(
                strategy_id="ALMA-EXEC-001",
                order_id_tag="001",
                oms_type="NETTING",
                external_order_claims=[instrument_id],
                manage_contingent_orders=False,
            )
        )
        self.connection = connection
        self.instrument_id = instrument_id
        self._connected = connected
        self.expected_account_id = expected_account_id

    def on_start(self) -> None:
        self.subscribe_quote_ticks(self.instrument_id)

    def truth(self, symbol: str) -> ExecutionTruth:
        self._check_symbol(symbol)
        instrument = self.cache.instrument(self.instrument_id)
        account = self.cache.account_for_venue(Venue("BINANCE"))
        quote = self.cache.quote_tick(self.instrument_id)
        if instrument is None or account is None or quote is None:
            raise ExecutionRejected("VENUE_STATE_MISSING")
        if not isinstance(account, MarginAccount):
            raise ExecutionRejected("ACCOUNT_TYPE_INVALID")
        if str(account.id) != self.expected_account_id:
            raise ExecutionRejected("ACCOUNT_IDENTITY_MISMATCH")
        positions = self.cache.positions_open(
            instrument_id=self.instrument_id,
            account_id=account.id,
        )
        signed = [_signed_position_quantity(position) for position in positions]
        if any(value > 0 for value in signed) and any(value < 0 for value in signed):
            raise ExecutionRejected("HEDGE_MODE_UNSUPPORTED")
        actual = sum(signed, Decimal(0))
        orders = {
            str(order.client_order_id): order
            for order in (
                self.cache.orders_open(
                    instrument_id=self.instrument_id,
                    account_id=account.id,
                )
                + self.cache.orders_inflight(
                    instrument_id=self.instrument_id,
                    account_id=account.id,
                )
            )
        }
        foreign_orders = [
            order
            for order in orders.values()
            if not _owned_order(self.connection, order, venue="BINANCE", symbol=symbol)
        ]
        if foreign_orders:
            raise ExecutionRejected("FOREIGN_ORDER_EXPOSURE")
        pending = sum(
            (
                (
                    order.leaves_qty.as_decimal()
                    if order.is_buy
                    else -order.leaves_qty.as_decimal()
                )
                for order in orders.values()
                if _owned_execution_order(
                    self.connection, order, venue="BINANCE", symbol=symbol
                )
            ),
            Decimal(0),
        )
        available = account.balance_free(instrument.settlement_currency)
        if available is None:
            raise ExecutionRejected("MARGIN_UNKNOWN")
        observed_at = _utc(quote.ts_event)
        # ponytail: Nautilus Account stores state on last_event, not as direct attr;
        # Binance testnet returns updateTime=0, so ts_init is from initial connect and goes stale.
        # Use quote.ts_event as the freshness anchor (market tick time = always fresh).
        last_event = account.last_event
        private_updated_ns = getattr(last_event, "ts_event", None)
        if not isinstance(private_updated_ns, int) or private_updated_ns <= 0:
            private_updated_ns = getattr(last_event, "ts_init", None)
        if not isinstance(private_updated_ns, int) or private_updated_ns <= 0:
            raise ExecutionRejected("PRIVATE_STATE_FRESHNESS_UNKNOWN")
        # ponytail: testnet updateTime=0 means ts_init is stale after initial connect;
        # use quote tick time as freshness anchor since exec engine is connected
        private_observed_at = observed_at  # trust the market tick time when connected
        observed_at = private_observed_at
        state = "|".join(
            [
                str(account.id),
                str(actual),
                str(pending),
                str(available.as_decimal()),
                str(quote.bid_price),
                str(quote.ask_price),
                *(
                    f"{key}:{value.status.name}"
                    for key, value in sorted(orders.items())
                ),
            ]
        )
        return ExecutionTruth(
            observed_at=observed_at,
            connected=bool(self._connected()),
            actual_quantity=actual,
            pending_quantity=pending,
            bid=quote.bid_price.as_decimal(),
            ask=quote.ask_price.as_decimal(),
            available_margin=available.as_decimal(),
            state_id=hashlib.sha256(state.encode()).hexdigest(),
        )

    def rules(self, symbol: str) -> InstrumentRules:
        self._check_symbol(symbol)
        instrument = self.cache.instrument(self.instrument_id)
        if instrument is None:
            raise ExecutionRejected("INSTRUMENT_MISSING")
        required = (
            instrument.size_increment,
            instrument.min_quantity,
            instrument.max_quantity,
            instrument.price_increment,
            instrument.min_price,
            instrument.max_price,
            instrument.min_notional,
        )
        if any(value is None for value in required):
            raise ExecutionRejected("INSTRUMENT_METADATA_MISSING")
        # ponytail: Binance exposes no separate minimum stop-distance field here;
        # one native tick is the strict local floor until Testnet proves otherwise.
        return InstrumentRules(
            quantity_step=instrument.size_increment.as_decimal(),
            quantity_min=instrument.min_quantity.as_decimal(),
            quantity_max=instrument.max_quantity.as_decimal(),
            tick_size=instrument.price_increment.as_decimal(),
            price_min=instrument.min_price.as_decimal(),
            price_max=instrument.max_price.as_decimal(),
            minimum_stop_distance=instrument.price_increment.as_decimal(),
            minimum_notional=instrument.min_notional.as_decimal(),
        )

    def find_order(self, client_order_id: str) -> VenueOrder | None:
        order = self.cache.order(ClientOrderId(client_order_id))
        return None if order is None else self._venue_order(order)

    def protection(self, client_order_id: str) -> tuple[ProtectionOrder, ...]:
        children = []
        for trade_id, stop_price, targets in self._entry_fills(client_order_id):
            stop_id = child_order_id(client_order_id, trade_id, "sl")
            stop = self.cache.order(ClientOrderId(stop_id))
            if stop is not None:
                children.append(self._protection_order(stop, "STOP_LOSS", stop_price))
            for index, (target_price, _) in enumerate(targets):
                target = self.cache.order(
                    ClientOrderId(
                        child_order_id(client_order_id, trade_id, "tp", index)
                    )
                )
                if target is not None:
                    children.append(
                        self._protection_order(target, "TAKE_PROFIT", target_price)
                    )
        return tuple(children)

    def required_margin(self, request: OrderRequest) -> Decimal:
        instrument = self._instrument(request.symbol)
        account = self.cache.account_for_venue(Venue("BINANCE"))
        if not isinstance(account, MarginAccount):
            raise ExecutionRejected("MARGIN_UNKNOWN")
        margin = account.calculate_margin_init(
            instrument,
            instrument.make_qty(request.quantity),
            instrument.make_price(request.price),
        )
        if margin is None:
            raise ExecutionRejected("MARGIN_UNKNOWN")
        return margin.as_decimal()

    def submit(self, request: OrderRequest) -> ProtectedSubmission | None:
        self.submit_order(self._native_order(request))
        return None

    def _native_order(self, request: OrderRequest):
        instrument = self._instrument(request.symbol)
        side = OrderSide.BUY if request.side == "BUY" else OrderSide.SELL
        common = {
            "instrument_id": self.instrument_id,
            "order_side": side,
            "quantity": instrument.make_qty(request.quantity),
            "reduce_only": request.reduce_only,
            "client_order_id": ClientOrderId(request.client_order_id),
        }
        if request.order_type == "LIMIT":
            order = self.order_factory.limit(
                **common,
                price=instrument.make_price(request.price),
                time_in_force=ENTRY_TIME_IN_FORCE,
            )
        elif request.order_type == "STOP_LIMIT" and request.trigger_price is not None:
            order = self.order_factory.stop_limit(
                **common,
                price=instrument.make_price(request.price),
                trigger_price=instrument.make_price(request.trigger_price),
                trigger_type=TriggerType.MARK_PRICE,
                emulation_trigger=TriggerType.NO_TRIGGER,
                time_in_force=ENTRY_TIME_IN_FORCE,
            )
        elif request.order_type == "MARKET_PROTECTED":
            order = self.order_factory.limit(
                **common,
                price=instrument.make_price(request.price),
                time_in_force=TimeInForce.IOC,
            )
        else:
            raise ExecutionRejected("ORDER_TYPE_UNSUPPORTED")
        return order

    def cancel(self, client_order_id: str, request_id: str) -> VenueOrder | None:
        del request_id
        order = self.cache.order(ClientOrderId(client_order_id))
        if order is None:
            raise ExecutionRejected("ORDER_NOT_IN_NATIVE_CACHE")
        if not order.is_closed:
            self.cancel_order(order)
            return None
        return self._venue_order(order)

    def emergency_flatten(self, entry: VenueOrder) -> ExecutionTruth:
        if entry.status not in _TERMINAL:
            self.cancel(entry.client_order_id, f"emergency:{entry.client_order_id}")
        self._flatten(entry.client_order_id)
        return self.truth(self.instrument_id.symbol.value)

    def cancel_open_entries(self, symbol: str) -> tuple[VenueOrder, ...]:
        self._check_symbol(symbol)
        canceled = []
        for order in self.cache.orders_open(instrument_id=self.instrument_id):
            if (
                _owned_order(self.connection, order, venue="BINANCE", symbol=symbol)
                and not order.is_reduce_only
            ):
                canceled.append(self._venue_order(order))
                self.cancel_order(order)
        return tuple(canceled)

    def ensure_position_protected(self, symbol: str) -> bool:
        truth = self.truth(symbol)
        if truth.actual_quantity == 0:
            return True
        stops = [
            order
            for order in self.cache.orders_open(instrument_id=self.instrument_id)
            if _owned_order(self.connection, order, venue="BINANCE", symbol=symbol)
            and order.is_reduce_only
            and order.order_type.name == "STOP_MARKET"
            and (
                (truth.actual_quantity > 0 and order.is_sell)
                or (truth.actual_quantity < 0 and order.is_buy)
            )
        ]
        return sum(
            (order.leaves_qty.as_decimal() for order in stops), Decimal(0)
        ) >= abs(truth.actual_quantity)

    def flatten_symbol(self, symbol: str) -> ExecutionTruth:
        self._check_symbol(symbol)
        row = self.connection.execute(
            "SELECT order_id FROM order_events current "
            "JOIN intents i ON i.intent_id = current.intent_id "
            "WHERE i.venue = 'BINANCE' AND i.symbol = ? "
            "ORDER BY current.seq DESC LIMIT 1",
            (symbol,),
        ).fetchone()
        if row is None:
            raise ExecutionRejected("FLATTEN_INTENT_MISSING")
        self._flatten(row[0])
        return self.truth(symbol)

    def on_order_filled(self, event: OrderFilled) -> None:
        record_native_fill(self.connection, event)
        order_id = str(event.client_order_id)
        row = self.connection.execute(
            "SELECT event_id FROM order_events WHERE order_id = ? ORDER BY seq LIMIT 1",
            (order_id,),
        ).fetchone()
        if row is not None and str(row[0]).startswith("submitted:"):
            if _intent_reduces_position(self.connection, order_id):
                return
            self._submit_protection(
                order_id,
                str(event.trade_id),
                event.last_qty.as_decimal(),
                event.ts_event,
            )

    def on_order_accepted(self, event: OrderAccepted) -> None:
        _record_order_status(self.connection, event, "ACCEPTED")

    def on_order_canceled(self, event: OrderCanceled) -> None:
        _record_order_status(self.connection, event, "CANCELED")

    def on_order_expired(self, event: OrderExpired) -> None:
        _record_order_status(self.connection, event, "EXPIRED")

    def on_order_rejected(self, event: OrderRejected) -> None:
        child_id = str(event.client_order_id)
        _record_entry_rejection(self.connection, event)
        entry_id = parent_for_child(self.connection, child_id)
        if entry_id is None:
            return
        entry = self.find_order(entry_id)
        if entry is None:
            raise ExecutionRejected("PROTECTION_PARENT_MISSING")
        now = _utc(event.ts_event)
        truth = self.truth(self.instrument_id.symbol.value)
        row = self.connection.execute(
            "SELECT i.venue, i.symbol FROM order_events o "
            "JOIN intents i ON i.intent_id = o.intent_id "
            "WHERE o.order_id = ? ORDER BY o.seq LIMIT 1",
            (entry_id,),
        ).fetchone()
        if row is None:
            raise ExecutionRejected("ENTRY_INTENT_MISSING")
        gate = MutationGate(
            self.connection,
            max_age=timedelta(seconds=2),
            clock=lambda: now,
        )
        gate.sync_venue(
            venue=row[0],
            symbol=row[1],
            truth=VenueTruth(
                state_id=truth.state_id,
                observed_at=truth.observed_at,
                actual=truth.actual_quantity,
                pending=truth.pending_quantity,
            ),
        )
        current = self.connection.execute(
            "SELECT mode FROM venue_modes WHERE venue_id = ?", (row[0],)
        ).fetchone()
        if current == (VenueMode.TRADE.value,):
            gate.transition_mode(
                request_id=f"protection-rejected:{child_id}",
                state_id=truth.state_id,
                timestamp=now,
                venue=row[0],
                symbol=row[1],
                requested=VenueMode.MANAGE_ONLY,
                policy=(
                    None if truth.actual_quantity == 0 else OpenPositionPolicy.MANAGE
                ),
                actor="execution-safety",
            )
        self.emergency_flatten(entry)

    def on_position_closed(self, event: PositionClosed) -> None:
        if event.instrument_id != self.instrument_id:
            return
        for order in self.cache.orders_open(instrument_id=self.instrument_id):
            if (
                _owned_order(
                    self.connection,
                    order,
                    venue="BINANCE",
                    symbol=self.instrument_id.symbol.value,
                )
                and order.is_reduce_only
            ):
                self.cancel_order(order)

    def _submit_protection(
        self,
        entry_id: str,
        trade_id: str,
        quantity: Decimal,
        ts_event: int,
    ) -> None:
        intent_id, stop_price, targets = self._entry_plan(entry_id)
        if not targets:
            return
        instrument = self._instrument(self.instrument_id.symbol.value)
        entry = self.cache.order(ClientOrderId(entry_id))
        if entry is None:
            raise ExecutionRejected("ENTRY_NOT_IN_NATIVE_CACHE")
        side = OrderSide.SELL if entry.is_buy else OrderSide.BUY
        created_at = _utc(ts_event).isoformat()
        stop_id = child_order_id(entry_id, trade_id, "sl")
        if reserve_order_submission(
            self.connection,
            event_id=f"protection-submitted:{stop_id}",
            intent_id=intent_id,
            order_id=stop_id,
            quantity=quantity,
            price=stop_price,
            created_at=created_at,
        ):
            stop = self.order_factory.stop_market(
                instrument_id=self.instrument_id,
                order_side=side,
                quantity=instrument.make_qty(quantity),
                trigger_price=instrument.make_price(stop_price),
                trigger_type=TriggerType.MARK_PRICE,
                reduce_only=True,
                emulation_trigger=TriggerType.NO_TRIGGER,
                client_order_id=ClientOrderId(stop_id),
            )
            self.submit_order(stop)
        for index, (target_price, fraction) in enumerate(targets):
            target_quantity = quantity * fraction
            target_id = child_order_id(entry_id, trade_id, "tp", index)
            if not reserve_order_submission(
                self.connection,
                event_id=f"protection-submitted:{target_id}",
                intent_id=intent_id,
                order_id=target_id,
                quantity=target_quantity,
                price=target_price,
                created_at=created_at,
            ):
                continue
            target = self.order_factory.limit(
                instrument_id=self.instrument_id,
                order_side=side,
                quantity=instrument.make_qty(target_quantity),
                price=instrument.make_price(target_price),
                reduce_only=True,
                client_order_id=ClientOrderId(target_id),
            )
            self.submit_order(target)

    def _flatten(self, entry_id: str) -> None:
        truth = self.truth(self.instrument_id.symbol.value)
        if truth.actual_quantity == 0:
            return
        pending_id = _pending_emergency(
            self.connection,
            venue="BINANCE",
            symbol=self.instrument_id.symbol.value,
        )
        if pending_id is not None:
            cache_state, cached = _emergency_cache_state(self.cache, pending_id)
            if cache_state == "WAIT":
                return
            if cache_state == "TERMINAL":
                observed = self._venue_order(cached)
                row = self.connection.execute(
                    "SELECT intent_id, status, quantity, filled_quantity, price "
                    "FROM order_events WHERE order_id = ? ORDER BY seq DESC LIMIT 1",
                    (pending_id,),
                ).fetchone()
                if row is not None and row[1] not in _TERMINAL:
                    status = (
                        "REJECTED" if observed.status == "DENIED" else observed.status
                    )
                    append_order_event(
                        self.connection,
                        event_id=f"emergency-reconciled:{pending_id}:{status}",
                        intent_id=row[0],
                        order_id=pending_id,
                        status=status,
                        quantity=observed.quantity,
                        filled_quantity=observed.filled_quantity,
                        price=observed.price if observed.price is not None else row[4],
                        created_at=datetime.now(UTC).isoformat(),
                    )
                pending_id = None
        if pending_id is None:
            intent_id = self._entry_plan(entry_id)[0]
            index = self.connection.execute(
                "SELECT count(DISTINCT order_id) FROM order_events "
                "WHERE event_id LIKE 'emergency-submitted:%'"
            ).fetchone()[0]
            flatten_id = child_order_id(entry_id, truth.state_id, "flat", index)
            now = datetime.now(UTC)
            if not reserve_order_submission(
                self.connection,
                event_id=f"emergency-submitted:{flatten_id}",
                intent_id=intent_id,
                order_id=flatten_id,
                quantity=abs(truth.actual_quantity),
                price=truth.bid if truth.actual_quantity > 0 else truth.ask,
                created_at=now.isoformat(),
            ):
                return
        else:
            flatten_id = pending_id
        instrument = self._instrument(self.instrument_id.symbol.value)
        order = self.order_factory.market(
            instrument_id=self.instrument_id,
            order_side=(OrderSide.SELL if truth.actual_quantity > 0 else OrderSide.BUY),
            quantity=instrument.make_qty(abs(truth.actual_quantity)),
            reduce_only=True,
            client_order_id=ClientOrderId(flatten_id),
        )
        self.submit_order(order)

    def _entry_plan(
        self, entry_id: str
    ) -> tuple[str, Decimal, tuple[tuple[Decimal, Decimal], ...]]:
        row = self.connection.execute(
            "SELECT o.intent_id, d.raw_contract FROM order_events o "
            "JOIN intents i ON i.intent_id = o.intent_id "
            "JOIN decisions d ON d.decision_id = i.decision_id "
            "WHERE o.order_id = ? ORDER BY o.seq LIMIT 1",
            (entry_id,),
        ).fetchone()
        if row is None:
            raise ExecutionRejected("ENTRY_INTENT_MISSING")
        contract = parse_decision_contract(row[1])
        return (
            row[0],
            Decimal(contract.invalidation_price),
            tuple(
                (Decimal(target.price), Decimal(target.close_fraction))
                for target in contract.targets
                if Decimal(target.close_fraction) > 0
            ),
        )

    def _entry_fills(
        self, entry_id: str
    ) -> tuple[tuple[str, Decimal, tuple[tuple[Decimal, Decimal], ...]], ...]:
        _, stop, targets = self._entry_plan(entry_id)
        rows = self.connection.execute(
            "SELECT fill_id FROM fill_events WHERE order_id = ? ORDER BY seq",
            (entry_id,),
        ).fetchall()
        return tuple((row[0], stop, targets) for row in rows)

    def _instrument(self, symbol: str):
        self._check_symbol(symbol)
        instrument = self.cache.instrument(self.instrument_id)
        if instrument is None:
            raise ExecutionRejected("INSTRUMENT_MISSING")
        return instrument

    def _check_symbol(self, symbol: str) -> None:
        if symbol != self.instrument_id.symbol.value:
            raise ExecutionRejected("SYMBOL_MISMATCH")

    @staticmethod
    def _venue_order(order) -> VenueOrder:
        price = order.price.as_decimal() if order.has_price else None
        return VenueOrder(
            client_order_id=str(order.client_order_id),
            status=order.status.name,
            quantity=order.quantity.as_decimal(),
            filled_quantity=order.filled_qty.as_decimal(),
            price=price,
            event_id=str(order.last_event.id),
        )

    @staticmethod
    def _protection_order(order, kind: str, expected_price: Decimal) -> ProtectionOrder:
        price = (
            order.trigger_price.as_decimal()
            if order.has_trigger_price
            else order.price.as_decimal()
        )
        return ProtectionOrder(
            order_id=str(order.client_order_id),
            kind=kind,
            price=price,
            quantity=order.quantity.as_decimal(),
            status=order.status.name,
            venue_resident=order.status.name in _ACTIVE and price == expected_price,
        )
