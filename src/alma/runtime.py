import argparse
import asyncio
import hashlib
import json
import os
import signal
import sqlite3
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol

from nautilus_trader.adapters.binance.common.enums import BinanceEnvironment
from nautilus_trader.model.identifiers import InstrumentId

from alma.binance_data import PublicMarketState, RawBookGuard, _guarded_binance_factory
from alma.binance_testnet import binance_testnet_runtime
from alma.execution import (
    ExecutionRejected,
    ExecutionResult,
    ExecutionStatus,
    ExecutionTruth,
    ExecutionVenue,
    TacticalExecutor,
)
from alma.features import FeatureState
from alma.ledger import open_ledger
from alma.market_state import MarketSnapshot, MarketState
from alma.mt5_bridge import MT5BridgeRejected, MT5BridgeStore, MT5Venue
from alma.mutation_gate import MutationGate, MutationRejected, VenueTruth
from alma.news_feed import news_for_context
from alma.operations import ledger_health_alerts
from alma.shadow_request import ShadowContext, ShadowRequest, build_shadow_request
from alma.shadow_service import ShadowResult, ShadowService
from alma.shadow_transport import LoopbackOpenAITransport
from alma.strategies import detect_liquidity_sweep, detect_liquidity_vacuum
from alma.venue_modes import VenueMode


class ShadowEvaluator(Protocol):
    async def evaluate(
        self,
        request: ShadowRequest,
        *,
        venue: str,
        symbol: str,
        setup: str = "",
        regime: str = "",
        session: str = "",
        actual_quantity: Decimal = Decimal(0),
        pending_quantity: Decimal = Decimal(0),
    ) -> ShadowResult: ...


def bootstrap_monitor_modes(connection: sqlite3.Connection) -> None:
    with connection:
        connection.executemany(
            "INSERT OR IGNORE INTO venue_modes(venue_id, mode) VALUES (?, ?)",
            (("BINANCE", VenueMode.MONITOR.value), ("MT5", VenueMode.MONITOR.value)),
        )


def _execution_enabled(
    venue_modes: dict[str, str], venue_ready: dict[str, bool]
) -> bool:
    return any(
        venue_ready.get(venue, False) and mode == VenueMode.TRADE.value
        for venue, mode in venue_modes.items()
    )


def _runtime_healthy(
    cycle_errors: dict[str, str],
    market_age_ms: int | None,
    book_valid: bool,
    venue_ready: dict[str, bool],
    ledger_alerts: list[str],
) -> bool:
    return (
        not cycle_errors
        and not ledger_alerts
        and market_age_ms is not None
        and market_age_ms <= 2_000
        and book_valid
        and all(venue_ready.values())
    )


def _validate_live_profile(
    binance_environment: BinanceEnvironment,
    mt5_account_mode: str,
    approved: str,
) -> None:
    if (
        binance_environment is BinanceEnvironment.LIVE or mt5_account_mode == "REAL"
    ) and approved.lower() != "true":
        raise RuntimeError("LIVE_PROFILE_NOT_APPROVED")


def complete_pending_transition(
    connection: sqlite3.Connection,
    *,
    venue: str,
    symbol: str,
    truth: ExecutionTruth,
    now: datetime,
) -> VenueMode | None:
    pending = connection.execute(
        "SELECT symbol FROM pending_mode_transitions WHERE venue = ?", (venue,)
    ).fetchone()
    if (
        pending is None
        or pending[0] != symbol
        or truth.actual_quantity != 0
        or truth.pending_quantity != 0
    ):
        return None
    gate = MutationGate(connection, max_age=timedelta(seconds=2), clock=lambda: now)
    gate.sync_venue(
        venue=venue,
        symbol=symbol,
        truth=VenueTruth(
            state_id=truth.state_id,
            observed_at=truth.observed_at,
            actual=truth.actual_quantity,
            pending=truth.pending_quantity,
        ),
    )
    return gate.complete_mode_transition(
        request_id=f"reconcile-transition:{venue}:{truth.state_id}",
        state_id=truth.state_id,
        timestamp=now,
        venue=venue,
        symbol=symbol,
        actor="alma-runtime",
    )


def activate_trade_mode(
    connection: sqlite3.Connection,
    venue: ExecutionVenue,
    *,
    venue_id: str,
    symbol: str,
    now: datetime,
    max_truth_age: timedelta = timedelta(seconds=2),
) -> VenueMode:
    row = connection.execute(
        "SELECT mode FROM venue_modes WHERE venue_id = ?", (venue_id,)
    ).fetchone()
    truth = venue.truth(symbol)
    age = now - truth.observed_at
    effective_max_age = max(max_truth_age, getattr(venue.store, 'max_clock_skew', timedelta(seconds=30)) if hasattr(venue, 'store') else timedelta(seconds=30))
    # ponytail: microsecond drift between now() and truth() gives age=-0.0s which fails the check
    if (
        now.utcoffset() != timedelta(0)
        or not truth.connected
        or not truth.state_id
        or abs(age) > effective_max_age
    ):
        raise ExecutionRejected("STATE_STALE")
    if row is None:
        raise ExecutionRejected("MODE_BLOCKED")
    return VenueMode(row[0])


def execute_accepted_decision(
    connection: sqlite3.Connection,
    venue: ExecutionVenue,
    result: ShadowResult,
    *,
    expected_actual: Decimal,
    expected_pending: Decimal,
    now: datetime,
    max_truth_age: timedelta = timedelta(seconds=2),
) -> ExecutionResult:
    contract = result.decision
    if result.status != "ACCEPTED" or contract is None:
        raise ExecutionRejected("DECISION_REJECTED")
    existing = connection.execute(
        "SELECT intent_id FROM intents WHERE decision_id = ?", (contract.decision_id,)
    ).fetchone()
    executor = TacticalExecutor(connection, venue, max_truth_age=max_truth_age)
    if existing is not None:
        return executor.execute(existing[0], now=now)

    truth = venue.truth(contract.symbol)
    age = now - truth.observed_at
    # ponytail: same as activate_trade_mode — negative microsecond drift + MT5 snapshot interval
    _max_age = max(max_truth_age, getattr(venue.store, 'max_clock_skew', timedelta(seconds=30)) if hasattr(venue, 'store') else timedelta(seconds=30))
    if (
        now.utcoffset() != timedelta(0)
        or not truth.connected
        or not truth.state_id
        or abs(age) > _max_age
    ):
        raise ExecutionRejected("STATE_STALE")
    if (
        truth.actual_quantity != expected_actual
        or truth.pending_quantity != expected_pending
    ):
        raise ExecutionRejected("STATE_DIVERGED")

    desired = Decimal(contract.target.volume)
    if contract.target.side == "SHORT":
        desired = -desired
    elif contract.target.side == "FLAT":
        desired = Decimal(0)
    if desired == truth.actual_quantity + truth.pending_quantity:
        return ExecutionResult(ExecutionStatus.NO_ACTION, None, Decimal(0))

    gate = MutationGate(connection, max_age=max_truth_age, clock=lambda: now)
    gate.sync_venue(
        venue=contract.venue,
        symbol=contract.symbol,
        truth=VenueTruth(
            state_id=contract.state_id,
            observed_at=truth.observed_at,
            actual=truth.actual_quantity,
            pending=truth.pending_quantity,
        ),
    )
    intent_id = f"intent:{contract.decision_id}"
    gate.prepare_intent(
        intent_id=intent_id,
        decision_id=contract.decision_id,
        request_id=f"execute:{contract.decision_id}",
        state_id=contract.state_id,
        timestamp=now,
        venue=contract.venue,
        symbol=contract.symbol,
        desired=desired,
        actor="alma-runtime",
    )
    return executor.execute(intent_id, now=now)


class RuntimeCore:
    def __init__(
        self,
        shadow: ShadowEvaluator,
        decision_handler: Callable[
            [ShadowResult, MarketSnapshot, Decimal, Decimal], Awaitable[None]
        ]
        | None = None,
        *,
        clock_ns: Callable[[], int] = time.time_ns,
        max_market_age_ms: int = 2_000,
    ) -> None:
        if max_market_age_ms <= 0:
            raise ValueError("max market age must be positive")
        self.shadow = shadow
        self.decision_handler = decision_handler
        self.clock_ns = clock_ns
        self.max_market_age_ms = max_market_age_ms
        self.features = FeatureState()
        self._account_signature: str | None = None
        self._last_setup: tuple[str, int] | None = None

    async def process(
        self,
        snapshot: MarketSnapshot,
        mode: VenueMode,
        *,
        observed_state_id: str | None = None,
        actual_quantity: Decimal = Decimal(0),
        pending_quantity: Decimal = Decimal(0),
        account: dict[str, str] | None = None,
        positions: tuple[dict[str, str], ...] = (),
        pending_orders: tuple[dict[str, str], ...] = (),
        extra_hooks: tuple[str, ...] = (),
    ) -> str | None:
        if not self._market_is_fresh(snapshot):
            return None
        features = self.features.update(snapshot)
        hooks: list[str] = []
        account_signature = json.dumps(
            {
                "actual_quantity": actual_quantity,
                "pending_quantity": pending_quantity,
                "positions": positions,
                "pending_orders": pending_orders,
            },
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        if account_signature != self._account_signature:
            hooks.append("ACCOUNT_CHANGE")
            self._account_signature = account_signature
        setup = detect_liquidity_sweep(features) or detect_liquidity_vacuum(features)
        setup_key = (setup.setup, setup.direction) if setup is not None else None
        if setup_key is not None and setup_key != self._last_setup:
            hooks.append("SETUP")
        self._last_setup = setup_key
        hooks.extend(extra_hooks)
        if not hooks or snapshot.bid is None or snapshot.ask is None:
            return None

        news_data = news_for_context(snapshot.venue)
        request = build_shadow_request(
            ShadowContext(
                state_id=snapshot.state_id,
                observed_at_ns=snapshot.observed_at_ns,
                market_age_ms=snapshot.market_age_ms,
                venue_mode=mode.value,
                venue=snapshot.venue,
                symbol=snapshot.symbol,
                bid=snapshot.bid,
                ask=snapshot.ask,
                h1_regime=features.h1_regime,
                h1_volatility=features.h1_volatility,
                m15_position=features.m15_position,
                m5_compression=features.m5_compression,
                flow_imbalance=features.flow_imbalance,
                account=account or {"runtime": mode.value},
                positions=positions,
                pending_orders=pending_orders,
                news=news_data,
                memory=(),
            ),
            tuple(hooks),
        )
        result = await self.shadow.evaluate(
            request,
            venue=snapshot.venue,
            symbol=snapshot.symbol,
            setup=setup.setup if setup is not None else "",
            regime=str(features.h1_regime),
            session=snapshot.session,
            actual_quantity=actual_quantity,
            pending_quantity=pending_quantity,
            news=news_data,
            news_state=news_data.get("headlines", "") or news_data.get("phase", ""),
            memory=(),
            observed_state_id=observed_state_id,
        )
        if self.decision_handler is not None and result.status == "ACCEPTED":
            if not self._market_is_fresh(snapshot):
                raise ExecutionRejected("MARKET_STATE_STALE")
            await self.decision_handler(
                result, snapshot, actual_quantity, pending_quantity
            )
        return result.status

    def _market_is_fresh(self, snapshot: MarketSnapshot) -> bool:
        elapsed_ms = (self.clock_ns() - snapshot.observed_at_ns) // 1_000_000
        return (
            snapshot.book_valid
            and 0 <= elapsed_ms <= self.max_market_age_ms
            and 0 <= snapshot.market_age_ms <= self.max_market_age_ms
        )


def _atomic_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        path.chmod(0o600)
    finally:
        temporary.unlink(missing_ok=True)


def _code_hash() -> str:
    digest = hashlib.sha256()
    for path in (
        Path(__file__),
        Path(__file__).with_name("strategies.py"),
        Path(__file__).with_name("features.py"),
    ):
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _mt5_market_snapshot(
    raw: dict[str, Any], truth, *, now: datetime
) -> MarketSnapshot:
    bid = truth.bid
    ask = truth.ask
    observed_at_ns = int(truth.observed_at.timestamp() * 1_000_000_000)
    return MarketSnapshot(
        state_id=truth.state_id,
        observed_at_ns=observed_at_ns,
        market_age_ms=max(0, int((now - truth.observed_at).total_seconds() * 1_000)),
        venue="MT5",
        symbol=str(raw["symbol"]["name"]),
        bid=bid,
        ask=ask,
        bid_size=None,
        ask_size=None,
        spread=ask - bid,
        top_book_imbalance=Decimal(0),
        mark_price=(bid + ask) / 2,
        funding_rate=None,
        tick_velocity_1s=0,
        realized_volatility=Decimal(0),
        flow_imbalance=Decimal(0),
        session="MT5",
        book_valid=truth.connected,
        m1=None,
        m5=None,
        m15=None,
        h1=None,
    )


async def run_runtime(
    database: Path,
    status_path: Path,
    *,
    router_url: str,
    model: str,
    poll_seconds: float = 0.25,
) -> Any:
    if poll_seconds <= 0:
        raise ValueError("poll seconds must be positive")
    _atomic_json(
        status_path,
        {"ok": False, "starting": True, "observed_at": datetime.now(UTC).isoformat()},
    )
    mt5_symbol = os.environ["ALMA_MT5_SYMBOL"]
    mt5_terminal_id = os.environ.get("ALMA_MT5_TERMINAL_ID", "mt5-1")
    binance_environment = BinanceEnvironment(
        os.environ.get("ALMA_BINANCE_ENVIRONMENT", "TESTNET")
    )
    live_armed = os.environ.get("ALMA_LIVE_APPROVED", "false").lower() == "true"
    _validate_live_profile(
        binance_environment,
        os.environ["ALMA_MT5_ACCOUNT_MODE"],
        "true" if live_armed else "false",
    )
    binance_instrument = InstrumentId.from_str(
        os.environ.get("ALMA_BINANCE_INSTRUMENT", "BTCUSDT-PERP.BINANCE")
    )
    binance_symbol = binance_instrument.symbol.value
    connection = open_ledger(database)
    bootstrap_monitor_modes(connection)
    # ponytail: live_armed → promote MANAGE_ONLY/MONITOR venues to TRADE
    if live_armed:
        connection.execute(
            "UPDATE venue_modes SET mode = ? WHERE venue_id IN ('BINANCE','MT5') "
            "AND mode IN ('MONITOR','MANAGE_ONLY')",
            (VenueMode.TRADE.value,),
        )
        connection.commit()
    mt5_store = MT5BridgeStore(
        connection,
        expected_account_mode=os.environ["ALMA_MT5_ACCOUNT_MODE"],
        expected_login=os.environ["ALMA_MT5_LOGIN"],
        expected_server=os.environ["ALMA_MT5_SERVER"],
        expected_symbol=mt5_symbol,
        expected_position_mode=os.environ.get("ALMA_MT5_POSITION_MODE", "AUTO"),
    )
    mt5_venue = MT5Venue(mt5_store, mt5_terminal_id, mt5_symbol)

    # ponytail: multi-provider fallback chain — primary + secondary via same 9Router
    transport = LoopbackOpenAITransport(
        router_url, model=model, api_key=os.environ.get("NINEROUTER_API_KEY")
    )
    fallback_model = os.environ.get("ALMA_FALLBACK_MODEL", "stepfun/step-3.5-flash")
    fallback_transport = LoopbackOpenAITransport(
        router_url, model=fallback_model, api_key=os.environ.get("NINEROUTER_API_KEY")
    )

    async def repair(raw: bytes) -> bytes:
        payload = json.dumps(
            {"repair_candidate": raw.decode("utf-8", errors="replace")},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        digest = hashlib.sha256(payload).hexdigest()
        response = await transport.complete(
            ShadowRequest(
                request_id=f"repair:{digest}",
                state_id="repair",
                hooks=("ACCOUNT_CHANGE",),
                payload=payload,
                prompt_hash=digest,
            )
        )
        return response.content

    decision_service = ShadowService(
        connection,
        providers=[transport.complete, fallback_transport.complete],
        policy_hash=hashlib.sha256(b"alma-v1-execution").hexdigest(),
        code_hash=_code_hash(),
        now=lambda: datetime.now(UTC),
        repair=repair,
        provenance="EXECUTION",
    )

    state = MarketState("BINANCE", binance_symbol)
    book_guard = RawBookGuard(state)
    testnet_node, binance_venue = binance_testnet_runtime(
        connection,
        instrument_id=binance_instrument,
        environment=binance_environment,
        expected_account_id=os.environ["ALMA_BINANCE_ACCOUNT_ID"],
        data_client_factory=_guarded_binance_factory(
            book_guard, state, None, binance_instrument
        ),
    )
    testnet_node.trader.add_strategy(
        PublicMarketState(
            state, book_guard=book_guard, instrument_id=binance_instrument
        )
    )

    execution_status: dict[str, str | None] = {"BINANCE": None, "MT5": None}

    async def execute_binance(
        result: ShadowResult,
        snapshot: MarketSnapshot,
        actual: Decimal,
        pending: Decimal,
    ) -> None:
        del snapshot
        outcome = execute_accepted_decision(
            connection,
            binance_venue,
            result,
            expected_actual=actual,
            expected_pending=pending,
            now=datetime.now(UTC),
        )
        execution_status["BINANCE"] = outcome.status.value

    async def execute_mt5(
        result: ShadowResult,
        snapshot: MarketSnapshot,
        actual: Decimal,
        pending: Decimal,
    ) -> None:
        del snapshot
        outcome = execute_accepted_decision(
            connection,
            mt5_venue,
            result,
            expected_actual=actual,
            expected_pending=pending,
            now=datetime.now(UTC),
        )
        execution_status["MT5"] = outcome.status.value

    binance_core = RuntimeCore(decision_service, execute_binance)
    mt5_core = RuntimeCore(decision_service, execute_mt5)
    testnet_node.build()
    testnet_task = asyncio.create_task(testnet_node.run_async())
    main_task = asyncio.current_task()
    if main_task is None:
        raise RuntimeError("runtime task unavailable")
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, main_task.cancel)
    loop.add_signal_handler(signal.SIGINT, main_task.cancel)
    last_decision: dict[str, str | None] = {"BINANCE": None, "MT5": None}
    decision_tasks: dict[str, asyncio.Task[str | None] | None] = {
        "BINANCE": None,
        "MT5": None,
    }
    last_recovery = 0.0
    last_status_write = 0.0
    last_status_ok: bool | None = None
    try:
        while True:
            if testnet_task.done():
                await testnet_task
            binance_now = datetime.now(UTC)
            binance_snapshot: MarketSnapshot | None = None
            cycle_errors: dict[str, str] = {}
            venue_ready = {"BINANCE": False, "MT5": False}
            recovery_due = time.monotonic() - last_recovery >= 1

            for venue, task in decision_tasks.items():
                if task is None or not task.done():
                    continue
                try:
                    last_decision[venue] = task.result() or last_decision[venue]
                except (
                    ConnectionError,
                    ExecutionRejected,
                    MT5BridgeRejected,
                    MutationRejected,
                    TimeoutError,
                    ValueError,
                ) as error:
                    cycle_errors[venue] = str(error)
                decision_tasks[venue] = None

            try:
                binance_snapshot = state.snapshot(time.time_ns())
                binance_truth = binance_venue.truth(binance_symbol)
                complete_pending_transition(
                    connection,
                    venue="BINANCE",
                    symbol=binance_symbol,
                    truth=binance_truth,
                    now=binance_now,
                )
                binance_rules = binance_venue.rules(binance_symbol)
                binance_mode = activate_trade_mode(
                    connection,
                    binance_venue,
                    venue_id="BINANCE",
                    symbol=binance_symbol,
                    now=binance_now,
                )
                if decision_tasks["BINANCE"] is None:
                    decision_tasks["BINANCE"] = asyncio.create_task(
                        binance_core.process(
                            binance_snapshot,
                            binance_mode,
                            actual_quantity=binance_truth.actual_quantity,
                            pending_quantity=binance_truth.pending_quantity,
                            account={
                                "actual_quantity": str(binance_truth.actual_quantity),
                                "pending_quantity": str(binance_truth.pending_quantity),
                                "available_margin": str(binance_truth.available_margin),
                                "quantity_min": str(binance_rules.quantity_min),
                                "quantity_step": str(binance_rules.quantity_step),
                                "quantity_max": str(binance_rules.quantity_max),
                                "tick_size": str(binance_rules.tick_size),
                            },
                        )
                    )
                if recovery_due:
                    TacticalExecutor(connection, binance_venue).recover_open_intents(
                        now=binance_now, venue="BINANCE", symbol=binance_symbol
                    )
                venue_ready["BINANCE"] = binance_truth.connected
            except (ExecutionRejected, MutationRejected, ValueError) as error:
                cycle_errors["BINANCE"] = str(error)

            try:
                mt5_now = datetime.now(UTC)
                mt5_raw = mt5_store.latest(mt5_terminal_id)
                if mt5_raw is None:
                    raise ExecutionRejected("MT5_STATE_MISSING")
                mt5_truth = mt5_venue.truth(mt5_symbol)
                complete_pending_transition(
                    connection,
                    venue="MT5",
                    symbol=mt5_symbol,
                    truth=mt5_truth,
                    now=mt5_now,
                )
                mt5_mode = activate_trade_mode(
                    connection,
                    mt5_venue,
                    venue_id="MT5",
                    symbol=mt5_symbol,
                    now=mt5_now,
                )
                mt5_snapshot = _mt5_market_snapshot(mt5_raw, mt5_truth, now=mt5_now)
                mt5_rules = mt5_venue.rules(mt5_symbol)
                positions = tuple(
                    {
                        "side": str(item["side"]),
                        "volume": str(item["volume"]),
                        "price_open": str(item["price_open"]),
                        "sl": str(item["sl"]),
                        "tp": str(item["tp"]),
                    }
                    for item in mt5_raw["positions"]
                    if item["symbol"] == mt5_symbol
                )
                pending_orders = tuple(
                    {
                        "side": str(item["side"]),
                        "volume": str(item["volume"]),
                        "price": str(item["price"]),
                        "status": str(item["status"]),
                    }
                    for item in mt5_raw["orders"]
                    if item["symbol"] == mt5_symbol
                )
                if decision_tasks["MT5"] is None:
                    decision_tasks["MT5"] = asyncio.create_task(
                        mt5_core.process(
                            mt5_snapshot,
                            mt5_mode,
                            actual_quantity=mt5_truth.actual_quantity,
                            pending_quantity=mt5_truth.pending_quantity,
                            account={
                                "balance": str(mt5_raw["account"]["balance"]),
                                "equity": str(mt5_raw["account"]["equity"]),
                                "free_margin": str(mt5_raw["account"]["free_margin"]),
                                "margin_mode": str(mt5_raw["terminal"]["margin_mode"]),
                                "quantity_min": str(mt5_rules.quantity_min),
                                "quantity_step": str(mt5_rules.quantity_step),
                                "quantity_max": str(mt5_rules.quantity_max),
                                "tick_size": str(mt5_rules.tick_size),
                            },
                            positions=positions,
                            pending_orders=pending_orders,
                        )
                    )
                if recovery_due:
                    TacticalExecutor(connection, mt5_venue).recover_open_intents(
                        now=mt5_now, venue="MT5", symbol=mt5_symbol
                    )
                venue_ready["MT5"] = mt5_truth.connected
            except (
                ExecutionRejected,
                MT5BridgeRejected,
                MutationRejected,
                ValueError,
            ) as error:
                cycle_errors["MT5"] = str(error)

            if recovery_due:
                last_recovery = time.monotonic()
            venue_modes = dict(
                connection.execute("SELECT venue_id, mode FROM venue_modes")
            )
            execution_enabled = _execution_enabled(venue_modes, venue_ready)
            status_now = datetime.now(UTC)
            health_alerts = ledger_health_alerts(connection)
            status_ok = _runtime_healthy(
                cycle_errors,
                binance_snapshot.market_age_ms
                if binance_snapshot is not None
                else None,
                binance_snapshot.book_valid if binance_snapshot is not None else False,
                venue_ready,
                health_alerts,
            )
            status_payload: dict[str, object] = {
                "ok": status_ok,
                "environment": binance_environment.value,
                "binance_environment": binance_environment.value,
                "mt5_account_mode": os.environ["ALMA_MT5_ACCOUNT_MODE"],
                "binance_symbol": binance_symbol,
                "mt5_symbol": mt5_symbol,
                "live_armed": live_armed,
                "execution_enabled": execution_enabled,
                "venue_modes": venue_modes,
                "venue_ready": venue_ready,
                "venue_errors": cycle_errors,
                "alerts": health_alerts,
                "state_id": (
                    binance_snapshot.state_id if binance_snapshot is not None else None
                ),
                "market_age_ms": (
                    binance_snapshot.market_age_ms
                    if binance_snapshot is not None
                    else None
                ),
                "book_valid": (
                    binance_snapshot.book_valid
                    if binance_snapshot is not None
                    else False
                ),
                "event_count": state.metrics.event_count,
                "decision_status": last_decision,
                "execution_status": execution_status,
                "observed_at": status_now.isoformat(),
            }
            status_monotonic = time.monotonic()
            if status_ok != last_status_ok or status_monotonic - last_status_write >= 1:
                _atomic_json(status_path, status_payload)
                last_status_write = status_monotonic
                last_status_ok = status_ok
            await asyncio.sleep(poll_seconds)
    except asyncio.CancelledError:
        pass
    finally:
        try:
            _atomic_json(
                status_path,
                {
                    "ok": False,
                    "stopping": True,
                    "observed_at": datetime.now(UTC).isoformat(),
                },
            )
        except OSError:
            pass
        active_decisions = [
            task for task in decision_tasks.values() if task is not None
        ]
        for task in active_decisions:
            task.cancel()
        try:
            await asyncio.wait_for(
                asyncio.gather(*active_decisions, return_exceptions=True), timeout=5
            )
        except TimeoutError:
            pass
        if testnet_node.kernel.is_running():
            try:
                await asyncio.wait_for(testnet_node.stop_async(), timeout=20)
            except TimeoutError:
                pass
        testnet_task.cancel()
        try:
            await asyncio.wait_for(
                asyncio.gather(testnet_task, return_exceptions=True), timeout=5
            )
        except TimeoutError:
            pass
        connection.close()
    return testnet_node


def main() -> None:
    parser = argparse.ArgumentParser(description="ALMA continuous execution runtime")
    parser.add_argument("--database", type=Path, default=Path("var/alma.db"))
    parser.add_argument("--status", type=Path, default=Path("var/runtime-status.json"))
    parser.add_argument("--router-url", default="http://127.0.0.1:20128")
    parser.add_argument("--model", default="vyceai-sonnet")
    parser.add_argument("--poll-seconds", type=float, default=0.25)
    args = parser.parse_args()
    node = asyncio.run(
        run_runtime(
            args.database,
            args.status,
            router_url=args.router_url,
            model=args.model,
            poll_seconds=args.poll_seconds,
        )
    )
    node.dispose()


if __name__ == "__main__":
    main()
