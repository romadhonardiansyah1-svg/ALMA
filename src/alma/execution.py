import hashlib
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Protocol

from alma.decision_contract import (
    DecisionAction,
    DecisionContract,
    EntryMode,
    OnPartialFill,
    parse_decision_contract,
    validate_decision_expiry,
)
from alma.ledger import append_order_event, reserve_order_submission
from alma.mutation_gate import MutationGate, VenueTruth
from alma.reconciler import execution_delta
from alma.technical_guards import (
    conforms_to_increment,
    has_minimum_distance,
    has_sufficient_margin,
    is_within_range,
)
from alma.venue_modes import OpenPositionPolicy, VenueMode, allows_quantity_change


class ExecutionRejected(RuntimeError):
    pass


class ExecutionStatus(StrEnum):
    ACTIVE = "ACTIVE"
    CANCELED = "CANCELED"
    NO_ACTION = "NO_ACTION"
    REPLACED = "REPLACED"
    WAITING = "WAITING"
    SUBMITTED = "SUBMITTED"
    RECOVERED = "RECOVERED"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class InstrumentRules:
    quantity_step: Decimal
    quantity_min: Decimal
    quantity_max: Decimal
    tick_size: Decimal
    price_min: Decimal
    price_max: Decimal
    minimum_stop_distance: Decimal
    minimum_notional: Decimal = Decimal(0)


@dataclass(frozen=True)
class ExecutionTruth:
    observed_at: datetime
    connected: bool
    actual_quantity: Decimal
    pending_quantity: Decimal
    bid: Decimal
    ask: Decimal
    available_margin: Decimal
    state_id: str = ""


@dataclass(frozen=True)
class OrderRequest:
    client_order_id: str
    intent_id: str
    symbol: str
    side: str
    quantity: Decimal
    order_type: str
    price: Decimal
    trigger_price: Decimal | None
    reduce_only: bool
    expires_at: datetime
    stop_loss: Decimal
    take_profits: tuple[tuple[Decimal, Decimal], ...]


@dataclass(frozen=True)
class VenueOrder:
    client_order_id: str
    status: str
    quantity: Decimal
    filled_quantity: Decimal
    price: Decimal | None
    event_id: str


@dataclass(frozen=True)
class ProtectionOrder:
    order_id: str
    kind: str
    price: Decimal
    quantity: Decimal
    status: str
    venue_resident: bool


@dataclass(frozen=True)
class ProtectedSubmission:
    entry: VenueOrder
    protection: tuple[ProtectionOrder, ...]


@dataclass(frozen=True)
class ExecutionResult:
    status: ExecutionStatus
    client_order_id: str | None
    delta: Decimal


class ExecutionVenue(Protocol):
    def truth(self, symbol: str) -> ExecutionTruth: ...

    def rules(self, symbol: str) -> InstrumentRules: ...

    def find_order(self, client_order_id: str) -> VenueOrder | None: ...

    def protection(self, client_order_id: str) -> tuple[ProtectionOrder, ...]: ...

    def required_margin(self, request: OrderRequest) -> Decimal: ...

    def submit(self, request: OrderRequest) -> ProtectedSubmission | None: ...

    def cancel(self, client_order_id: str, request_id: str) -> VenueOrder | None: ...

    def emergency_flatten(self, entry: VenueOrder) -> ExecutionTruth: ...

    def cancel_open_entries(self, symbol: str) -> tuple[VenueOrder, ...]: ...

    def ensure_position_protected(self, symbol: str) -> bool: ...

    def flatten_symbol(self, symbol: str) -> ExecutionTruth: ...


def client_order_id(intent_id: str, revision: int = 0) -> str:
    if revision < 0:
        raise ValueError("order revision must be non-negative")
    digest = hashlib.sha256(f"{intent_id}:{revision}".encode()).hexdigest()[:24]
    return f"alma-{digest}"


class TacticalExecutor:
    def __init__(
        self,
        connection: sqlite3.Connection,
        venue: ExecutionVenue,
        *,
        max_truth_age: timedelta = timedelta(seconds=2),
    ) -> None:
        if max_truth_age <= timedelta(0):
            raise ValueError("max truth age must be positive")
        self.connection = connection
        self.venue = venue
        self.max_truth_age = max_truth_age

    def execute(
        self, intent_id: str, *, now: datetime | None = None
    ) -> ExecutionResult:
        now = now or datetime.now(UTC)
        if now.utcoffset() != timedelta(0):
            raise ExecutionRejected("INVALID_CLOCK")
        intent, contract = self._intent(intent_id)
        if self._execution_order_ids(intent_id):
            return self.maintain(intent_id, now=now)
        validate_decision_expiry(contract, now=now)
        truth = self.venue.truth(str(intent["symbol"]))
        self._validate_truth(truth, now)

        desired = Decimal(str(intent["desired_quantity"]))
        submission_desired = self._submission_desired(contract, truth, desired)
        delta = execution_delta(
            desired=submission_desired,
            actual=truth.actual_quantity,
            pending=truth.pending_quantity,
        )
        order_id = client_order_id(intent_id)
        if delta == 0:
            return ExecutionResult(ExecutionStatus.NO_ACTION, None, delta)
        mode = self._mode(str(intent["venue"]))
        if not allows_quantity_change(
            mode,
            actual=truth.actual_quantity,
            desired=submission_desired,
        ):
            raise ExecutionRejected("MODE_BLOCKED")

        request = self._request(
            order_id,
            intent_id,
            str(intent["symbol"]),
            contract,
            delta,
            submission_desired,
            truth,
            now,
        )
        if request is None:
            return ExecutionResult(ExecutionStatus.WAITING, order_id, delta)

        locally_submitted = self.connection.execute(
            "SELECT 1 FROM order_events WHERE order_id = ?", (order_id,)
        ).fetchone()
        if locally_submitted is not None:
            return ExecutionResult(ExecutionStatus.UNKNOWN, order_id, delta)

        # Venue truth and mode are read again at the last possible point before
        # the irreversible network mutation.
        truth = self.venue.truth(str(intent["symbol"]))
        self._validate_truth(truth, now)
        submission_desired = self._submission_desired(contract, truth, desired)
        delta = execution_delta(
            desired=submission_desired,
            actual=truth.actual_quantity,
            pending=truth.pending_quantity,
        )
        if delta == 0:
            return ExecutionResult(ExecutionStatus.NO_ACTION, None, delta)
        mode = self._mode(str(intent["venue"]))
        if not allows_quantity_change(
            mode,
            actual=truth.actual_quantity,
            desired=submission_desired,
        ):
            raise ExecutionRejected("MODE_BLOCKED")
        request = self._request(
            order_id,
            intent_id,
            str(intent["symbol"]),
            contract,
            delta,
            submission_desired,
            truth,
            now,
        )
        if request is None:
            return ExecutionResult(ExecutionStatus.WAITING, order_id, delta)

        if not self._ensure_submitted(intent_id, request, now):
            observed = self.venue.find_order(order_id)
            if observed is not None:
                self._verify_or_cancel(intent_id, contract, observed, now)
                self._record_observed(intent_id, observed, now, recovered=True)
                return ExecutionResult(ExecutionStatus.RECOVERED, order_id, delta)
            return ExecutionResult(ExecutionStatus.UNKNOWN, order_id, delta)
        try:
            submission = self.venue.submit(request)
        except (TimeoutError, ConnectionError):
            return ExecutionResult(ExecutionStatus.UNKNOWN, order_id, delta)
        except (ExecutionRejected, ValueError):
            self._record_rejected(intent_id, request, now)
            raise
        if submission is None:
            return ExecutionResult(ExecutionStatus.UNKNOWN, order_id, delta)
        observed = self._verify_or_cancel(
            intent_id,
            contract,
            submission.entry,
            now,
            submission.protection,
            expected_client_order_id=order_id,
        )
        self._record_observed(intent_id, observed, now, recovered=False)
        return ExecutionResult(ExecutionStatus.SUBMITTED, order_id, delta)

    def recover_open_intents(
        self,
        *,
        now: datetime | None = None,
        venue: str | None = None,
        symbol: str | None = None,
    ) -> tuple[ExecutionResult, ...]:
        filters = []
        parameters: list[str] = []
        if venue is not None:
            filters.append("i.venue = ?")
            parameters.append(venue)
        if symbol is not None:
            filters.append("i.symbol = ?")
            parameters.append(symbol)
        where = " AND " + " AND ".join(filters) if filters else ""
        rows = self.connection.execute(
            "SELECT DISTINCT i.intent_id FROM intents i "
            "LEFT JOIN order_events any_order ON any_order.intent_id = i.intent_id "
            "WHERE any_order.intent_id IS NULL "
            "AND i.seq = (SELECT MAX(i2.seq) FROM intents i2 "
            "WHERE i2.venue = i.venue AND i2.symbol = i.symbol) "
            + where
            + " UNION SELECT DISTINCT i.intent_id FROM order_events current "
            "JOIN intents i ON i.intent_id = current.intent_id "
            "JOIN (SELECT order_id, MAX(seq) AS seq FROM order_events GROUP BY order_id) latest "
            "ON latest.seq = current.seq "
            "WHERE current.status NOT IN ('REJECTED', 'CANCELED', 'EXPIRED') "
            "AND current.order_id IN (SELECT order_id FROM order_events "
            "WHERE event_id LIKE 'submitted:%') " + where + " ORDER BY 1",
            parameters * 2,
        ).fetchall()
        recovered: list[ExecutionResult] = []
        for (intent_id,) in rows:
            try:
                recovered.append(self.execute(intent_id, now=now))
            except ValueError:
                # Historical contracts may predate current semantic validation or be expired.
                continue
        return tuple(recovered)

    def maintain(
        self, intent_id: str, *, now: datetime | None = None
    ) -> ExecutionResult:
        now = now or datetime.now(UTC)
        if now.utcoffset() != timedelta(0):
            raise ExecutionRejected("INVALID_CLOCK")
        intent, contract = self._intent(intent_id)
        desired = Decimal(str(intent["desired_quantity"]))
        order_ids = self._execution_order_ids(intent_id)
        active_id = order_ids[-1] if order_ids else client_order_id(intent_id)
        replacement_id = client_order_id(intent_id, len(order_ids))
        order = self.venue.find_order(active_id)
        if order is None:
            order = self._durable_terminal_order(active_id)
        if order is None:
            return ExecutionResult(
                ExecutionStatus.UNKNOWN if order_ids else ExecutionStatus.NO_ACTION,
                active_id if order_ids else None,
                Decimal(0),
            )
        active_reduces_position = self._order_reduces_position(
            intent_id, contract, active_id
        )
        if order.status not in {"REJECTED", "CANCELED", "EXPIRED"}:
            self._verify_or_cancel(intent_id, contract, order, now)
        self._record_observed(intent_id, order, now, recovered=True)
        reverse_continuation = False
        if (
            contract.action is DecisionAction.REVERSE
            and active_reduces_position
            and order.status == "FILLED"
        ):
            truth = self.venue.truth(str(intent["symbol"]))
            self._validate_truth(truth, now)
            reverse_continuation = (
                truth.actual_quantity == 0 and truth.pending_quantity == 0
            )
            if not reverse_continuation:
                return ExecutionResult(ExecutionStatus.WAITING, active_id, Decimal(0))
        reprice_after_cancel = (
            order.status == "CANCELED"
            and order.filled_quantity > 0
            and contract.entry.on_partial_fill is OnPartialFill.REPRICE_REMAINDER
        )
        continuing = reverse_continuation or reprice_after_cancel
        if (
            order.status in {"REJECTED", "FILLED", "CANCELED", "EXPIRED"}
            and not continuing
        ):
            return ExecutionResult(ExecutionStatus.RECOVERED, active_id, Decimal(0))

        expires_at = contract.created_at + timedelta(seconds=contract.entry.ttl_seconds)
        if now >= expires_at:
            if continuing:
                return ExecutionResult(ExecutionStatus.CANCELED, active_id, Decimal(0))
            if self._cancel(intent_id, order, now, "expiry") is None:
                return ExecutionResult(ExecutionStatus.UNKNOWN, active_id, Decimal(0))
            return ExecutionResult(ExecutionStatus.CANCELED, active_id, Decimal(0))
        if not continuing and order.status != "PARTIALLY_FILLED":
            return ExecutionResult(ExecutionStatus.ACTIVE, active_id, Decimal(0))
        policy = contract.entry.on_partial_fill
        if not continuing and policy is OnPartialFill.KEEP_REMAINDER:
            return ExecutionResult(ExecutionStatus.ACTIVE, active_id, Decimal(0))
        if not continuing:
            if self._cancel(intent_id, order, now, "partial") is None:
                return ExecutionResult(ExecutionStatus.UNKNOWN, active_id, Decimal(0))
            if policy is OnPartialFill.CANCEL_REMAINDER:
                return ExecutionResult(ExecutionStatus.CANCELED, active_id, Decimal(0))

        opening_reversal = contract.action is DecisionAction.REVERSE and (
            reverse_continuation or not active_reduces_position
        )
        truth = self.venue.truth(str(intent["symbol"]))
        self._validate_truth(truth, now)
        submission_desired = self._submission_desired(
            contract, truth, desired, opening_reversal=opening_reversal
        )
        delta = execution_delta(
            desired=submission_desired,
            actual=truth.actual_quantity,
            pending=truth.pending_quantity,
        )
        if delta == 0:
            return ExecutionResult(ExecutionStatus.NO_ACTION, None, delta)
        mode = self._mode(str(intent["venue"]))
        if not allows_quantity_change(
            mode, actual=truth.actual_quantity, desired=submission_desired
        ):
            raise ExecutionRejected("MODE_BLOCKED")
        request = self._request(
            replacement_id,
            intent_id,
            str(intent["symbol"]),
            contract,
            delta,
            submission_desired,
            truth,
            now,
        )
        if request is None:
            return ExecutionResult(ExecutionStatus.WAITING, replacement_id, delta)
        observed = self.venue.find_order(replacement_id)
        if observed is not None:
            self._verify_or_cancel(intent_id, contract, observed, now)
            self._record_observed(intent_id, observed, now, recovered=True)
            return ExecutionResult(ExecutionStatus.RECOVERED, replacement_id, delta)
        local = self.connection.execute(
            "SELECT 1 FROM order_events WHERE order_id = ?", (replacement_id,)
        ).fetchone()
        if local is not None:
            return ExecutionResult(ExecutionStatus.UNKNOWN, replacement_id, delta)

        # Re-read broker truth at the last mutation boundary; the position may
        # have changed after the original order was canceled.
        truth = self.venue.truth(str(intent["symbol"]))
        self._validate_truth(truth, now)
        submission_desired = self._submission_desired(
            contract, truth, desired, opening_reversal=opening_reversal
        )
        delta = execution_delta(
            desired=submission_desired,
            actual=truth.actual_quantity,
            pending=truth.pending_quantity,
        )
        if delta == 0:
            return ExecutionResult(ExecutionStatus.NO_ACTION, None, delta)
        mode = self._mode(str(intent["venue"]))
        if not allows_quantity_change(
            mode, actual=truth.actual_quantity, desired=submission_desired
        ):
            raise ExecutionRejected("MODE_BLOCKED")
        request = self._request(
            replacement_id,
            intent_id,
            str(intent["symbol"]),
            contract,
            delta,
            submission_desired,
            truth,
            now,
        )
        if request is None:
            return ExecutionResult(ExecutionStatus.WAITING, replacement_id, delta)
        if not self._ensure_submitted(intent_id, request, now):
            observed = self.venue.find_order(replacement_id)
            if observed is not None:
                self._verify_or_cancel(intent_id, contract, observed, now)
                self._record_observed(intent_id, observed, now, recovered=True)
                return ExecutionResult(ExecutionStatus.RECOVERED, replacement_id, delta)
            return ExecutionResult(ExecutionStatus.UNKNOWN, replacement_id, delta)
        try:
            submission = self.venue.submit(request)
        except (TimeoutError, ConnectionError):
            return ExecutionResult(ExecutionStatus.UNKNOWN, replacement_id, delta)
        except (ExecutionRejected, ValueError):
            self._record_rejected(intent_id, request, now)
            raise
        if submission is None:
            return ExecutionResult(ExecutionStatus.UNKNOWN, replacement_id, delta)
        observed = self._verify_or_cancel(
            intent_id,
            contract,
            submission.entry,
            now,
            submission.protection,
            expected_client_order_id=replacement_id,
        )
        self._record_observed(intent_id, observed, now, recovered=False)
        return ExecutionResult(ExecutionStatus.REPLACED, replacement_id, delta)

    def _execution_order_ids(self, intent_id: str) -> tuple[str, ...]:
        return tuple(
            row[0]
            for row in self.connection.execute(
                "SELECT order_id FROM order_events "
                "WHERE intent_id = ? AND event_id LIKE 'submitted:%' "
                "GROUP BY order_id ORDER BY MIN(seq)",
                (intent_id,),
            )
        )

    def _order_reduces_position(
        self, intent_id: str, contract: DecisionContract, order_id: str
    ) -> bool:
        row = self.connection.execute(
            "SELECT event_id FROM order_events "
            "WHERE intent_id = ? AND order_id = ? AND event_id LIKE 'submitted:%' "
            "ORDER BY seq LIMIT 1",
            (intent_id, order_id),
        ).fetchone()
        if row is not None and row[0].startswith("submitted:reduce:"):
            return True
        if row is not None and row[0].startswith("submitted:open:"):
            return False
        return contract.action in {DecisionAction.REDUCE, DecisionAction.CLOSE} or (
            contract.action is DecisionAction.REVERSE
            and order_id == client_order_id(intent_id)
        )

    def _cancel(
        self,
        intent_id: str,
        order: VenueOrder,
        now: datetime,
        reason: str,
    ) -> VenueOrder | None:
        try:
            canceled = self.venue.cancel(
                order.client_order_id,
                f"cancel:{reason}:{order.client_order_id}",
            )
        except (TimeoutError, ConnectionError) as error:
            raise ExecutionRejected("CANCEL_UNCONFIRMED") from error
        if canceled is None:
            return None
        if canceled.client_order_id != order.client_order_id or canceled.status not in {
            "CANCELED",
            "EXPIRED",
        }:
            raise ExecutionRejected("CANCEL_UNCONFIRMED")
        self._record_observed(intent_id, canceled, now, recovered=False)
        return canceled

    def _intent(self, intent_id: str) -> tuple[dict[str, object], DecisionContract]:
        cursor = self.connection.execute(
            "SELECT i.intent_id, i.venue, i.symbol, i.desired_quantity, "
            "d.raw_contract, d.validation_result, d.provenance "
            "FROM intents i JOIN decisions d ON d.decision_id = i.decision_id "
            "WHERE i.intent_id = ?",
            (intent_id,),
        )
        row = cursor.fetchone()
        if row is None:
            raise ExecutionRejected("INTENT_MISSING")
        if row[5] != "ACCEPTED" or row[6] != "EXECUTION":
            raise ExecutionRejected("DECISION_REJECTED")
        names = [column[0] for column in cursor.description]
        intent = dict(zip(names, row, strict=True))
        contract = parse_decision_contract(row[4])
        if (contract.venue, contract.symbol) != (row[1], row[2]):
            raise ExecutionRejected("CONTRACT_DIVERGED")
        return intent, contract

    def _validate_truth(self, truth: ExecutionTruth, now: datetime) -> None:
        values = (
            truth.actual_quantity,
            truth.pending_quantity,
            truth.bid,
            truth.ask,
            truth.available_margin,
        )
        if truth.observed_at.utcoffset() != timedelta(0):
            raise ExecutionRejected("INVALID_VENUE_TIME")
        age = now - truth.observed_at
        if not truth.connected:
            raise ExecutionRejected("VENUE_UNAVAILABLE")
        if age < timedelta(0) or age > self.max_truth_age:
            raise ExecutionRejected("STATE_STALE")
        if not all(value.is_finite() for value in values):
            raise ExecutionRejected("VENUE_STATE_INVALID")
        if truth.bid <= 0 or truth.ask < truth.bid or truth.available_margin < 0:
            raise ExecutionRejected("VENUE_STATE_INVALID")

    @staticmethod
    def _submission_desired(
        contract: DecisionContract,
        truth: ExecutionTruth,
        desired: Decimal,
        *,
        opening_reversal: bool = False,
    ) -> Decimal:
        actual = truth.actual_quantity
        pending = truth.pending_quantity
        effective = actual + pending
        action = contract.action
        if desired == effective:
            return desired
        if (
            action
            in {
                DecisionAction.REDUCE,
                DecisionAction.CLOSE,
                DecisionAction.REVERSE,
            }
            and pending != 0
        ):
            raise ExecutionRejected("PENDING_CONFLICT")
        if action is DecisionAction.NO_CHANGE:
            valid = desired == effective
        elif action in {DecisionAction.OPEN_LONG, DecisionAction.INCREASE_LONG}:
            valid = desired > 0 and effective >= 0 and desired > effective
        elif action in {DecisionAction.OPEN_SHORT, DecisionAction.INCREASE_SHORT}:
            valid = desired < 0 and effective <= 0 and abs(desired) > abs(effective)
        elif action is DecisionAction.REDUCE:
            valid = actual * desired > 0 and abs(desired) < abs(actual)
        elif action is DecisionAction.CLOSE:
            valid = desired == 0
        elif action is DecisionAction.REVERSE:
            if opening_reversal:
                if effective != 0 or desired == 0:
                    raise ExecutionRejected("ACTION_STATE_MISMATCH")
                return desired
            valid = actual * desired < 0
            if valid:
                return Decimal(0)
        else:
            valid = False
        if not valid:
            raise ExecutionRejected("ACTION_STATE_MISMATCH")
        return desired

    def _mode(self, venue: str) -> VenueMode:
        row = self.connection.execute(
            "SELECT mode FROM venue_modes WHERE venue_id = ?", (venue,)
        ).fetchone()
        if row is None:
            raise ExecutionRejected("MODE_BLOCKED")
        return VenueMode(row[0])

    def _request(
        self,
        order_id: str,
        intent_id: str,
        symbol: str,
        contract: DecisionContract,
        delta: Decimal,
        desired: Decimal,
        truth: ExecutionTruth,
        now: datetime,
    ) -> OrderRequest | None:
        side = "BUY" if delta > 0 else "SELL"
        quantity = abs(delta)
        rules = self.venue.rules(symbol)
        if not conforms_to_increment(
            value=quantity, increment=rules.quantity_step
        ) or not is_within_range(
            value=quantity,
            minimum=rules.quantity_min,
            maximum=rules.quantity_max,
        ):
            raise ExecutionRejected("INSTRUMENT_INVALID")

        low = Decimal(contract.entry.preferred_low)
        high = Decimal(contract.entry.preferred_high)
        executable = truth.ask if side == "BUY" else truth.bid
        mode = contract.entry.mode
        if (
            mode in {EntryMode.WAIT_RETEST, EntryMode.ADAPTIVE}
            and not low <= executable <= high
        ):
            return None
        if mode is EntryMode.PASSIVE:
            price = low if side == "BUY" else high
            order_type = "LIMIT"
            trigger = None
        elif mode in {
            EntryMode.WAIT_RETEST,
            EntryMode.ADAPTIVE,
            EntryMode.AGGRESSIVE_LIMIT,
        }:
            if not low <= executable <= high:
                raise ExecutionRejected("PRICE_OUTSIDE_ENVELOPE")
            price = executable
            order_type = "LIMIT"
            trigger = None
        elif mode is EntryMode.STOP_ENTRY:
            trigger = high if side == "BUY" else low
            price = (
                Decimal(contract.entry.max_acceptable_price) if side == "BUY" else low
            )
            order_type = "STOP_LIMIT"
        elif mode is EntryMode.MARKET_PROTECTED:
            trigger = None
            price = (
                Decimal(contract.entry.max_acceptable_price) if side == "BUY" else low
            )
            order_type = "MARKET_PROTECTED"
        else:
            raise ExecutionRejected("ENTRY_MODE_UNSUPPORTED")

        if side == "BUY" and price > Decimal(contract.entry.max_acceptable_price):
            raise ExecutionRejected("PRICE_OUTSIDE_ENVELOPE")
        if not conforms_to_increment(
            value=price, increment=rules.tick_size
        ) or not is_within_range(
            value=price,
            minimum=rules.price_min,
            maximum=rules.price_max,
        ):
            raise ExecutionRejected("INSTRUMENT_INVALID")
        reducing = abs(desired) < abs(truth.actual_quantity) and (
            desired == 0 or desired * truth.actual_quantity > 0
        )
        reduce_only = reducing and delta * truth.actual_quantity < 0
        if reducing and not reduce_only:
            raise ExecutionRejected("PENDING_CONFLICT")
        if not reduce_only and quantity * price < rules.minimum_notional:
            raise ExecutionRejected("INSTRUMENT_INVALID")
        if reduce_only:
            return OrderRequest(
                client_order_id=order_id,
                intent_id=intent_id,
                symbol=symbol,
                side=side,
                quantity=quantity,
                order_type=order_type,
                price=price,
                trigger_price=trigger,
                reduce_only=True,
                expires_at=contract.created_at
                + timedelta(seconds=contract.entry.ttl_seconds),
                stop_loss=Decimal(0),
                take_profits=(),
            )
        stop_loss = Decimal(contract.invalidation_price)
        if not conforms_to_increment(
            value=stop_loss, increment=rules.tick_size
        ) or not has_minimum_distance(
            first=price,
            second=stop_loss,
            minimum_distance=rules.minimum_stop_distance,
        ):
            raise ExecutionRejected("INSTRUMENT_INVALID")
        targets = tuple(
            (Decimal(target.price), Decimal(target.close_fraction))
            for target in contract.targets
            if Decimal(target.close_fraction) > 0
        )
        if not targets:
            raise ExecutionRejected("PROTECTION_INVALID")
        if any(
            not conforms_to_increment(
                value=quantity * fraction, increment=rules.quantity_step
            )
            or not is_within_range(
                value=quantity * fraction,
                minimum=rules.quantity_min,
                maximum=rules.quantity_max,
            )
            for _, fraction in targets
        ):
            raise ExecutionRejected("PROTECTION_INVALID")
        if any(
            not conforms_to_increment(value=target, increment=rules.tick_size)
            or not is_within_range(
                value=target,
                minimum=rules.price_min,
                maximum=rules.price_max,
            )
            for target, _ in targets
        ):
            raise ExecutionRejected("INSTRUMENT_INVALID")
        if side == "BUY":
            protection_valid = stop_loss < price and all(
                target > price for target, _ in targets
            )
        else:
            protection_valid = stop_loss > price and all(
                target < price for target, _ in targets
            )
        if not protection_valid:
            raise ExecutionRejected("PROTECTION_INVALID")
        request = OrderRequest(
            client_order_id=order_id,
            intent_id=intent_id,
            symbol=symbol,
            side=side,
            quantity=quantity,
            order_type=order_type,
            price=price,
            trigger_price=trigger,
            reduce_only=reduce_only,
            expires_at=contract.created_at
            + timedelta(seconds=contract.entry.ttl_seconds),
            stop_loss=stop_loss,
            take_profits=targets,
        )
        if not request.reduce_only and not has_sufficient_margin(
            available=truth.available_margin,
            required=self.venue.required_margin(request),
        ):
            raise ExecutionRejected("MARGIN_UNKNOWN")
        return request

    def _verify_or_cancel(
        self,
        intent_id: str,
        contract: DecisionContract,
        entry: VenueOrder,
        now: datetime,
        protection: tuple[ProtectionOrder, ...] | None = None,
        *,
        expected_client_order_id: str | None = None,
    ) -> VenueOrder:
        if (
            expected_client_order_id is not None
            and entry.client_order_id != expected_client_order_id
        ):
            raise ExecutionRejected("CORRELATION_MISMATCH")
        protection = (
            self.venue.protection(entry.client_order_id)
            if protection is None
            else protection
        )
        protected_quantity = entry.filled_quantity
        if protected_quantity == 0:
            return entry
        if self._order_reduces_position(intent_id, contract, entry.client_order_id):
            intent = self._intent(intent_id)[0]
            if not self.venue.ensure_position_protected(str(intent["symbol"])):
                raise ExecutionRejected("PROTECTION_UNCONFIRMED")
            return entry
        active = {"ACCEPTED", "SUBMITTED", "PARTIALLY_FILLED", "TRIGGERED"}
        stop_loss = Decimal(contract.invalidation_price)
        stops = [
            order
            for order in protection
            if order.kind == "STOP_LOSS"
            and order.venue_resident
            and order.status in active
            and order.price == stop_loss
        ]
        expected_by_price: dict[Decimal, Decimal] = {}
        for target in contract.targets:
            fraction = Decimal(target.close_fraction)
            if fraction > 0:
                price = Decimal(target.price)
                expected_by_price[price] = (
                    expected_by_price.get(price, Decimal(0))
                    + protected_quantity * fraction
                )
        actual_by_price: dict[Decimal, Decimal] = {}
        for order in protection:
            if (
                order.kind == "TAKE_PROFIT"
                and order.venue_resident
                and order.status in active
            ):
                actual_by_price[order.price] = (
                    actual_by_price.get(order.price, Decimal(0)) + order.quantity
                )
        if (
            sum((order.quantity for order in stops), Decimal(0)) != protected_quantity
            or actual_by_price != expected_by_price
        ):
            if entry.status not in {"FILLED", "CANCELED", "EXPIRED", "REJECTED"}:
                try:
                    self._cancel(intent_id, entry, now, "unprotected")
                except ExecutionRejected:
                    pass
            truth = self.venue.emergency_flatten(entry)
            self._validate_truth(truth, now)
            if not truth.state_id:
                raise ExecutionRejected("EMERGENCY_DERISK_UNCONFIRMED")
            gate = MutationGate(
                self.connection,
                max_age=self.max_truth_age,
                clock=lambda: now,
            )
            intent = self._intent(intent_id)[0]
            venue = str(intent["venue"])
            symbol = str(intent["symbol"])
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
            if self._mode(venue) is VenueMode.TRADE:
                gate.transition_mode(
                    request_id=f"protection-failure:{entry.client_order_id}",
                    state_id=truth.state_id,
                    timestamp=now,
                    venue=venue,
                    symbol=symbol,
                    requested=VenueMode.MANAGE_ONLY,
                    policy=(
                        None
                        if truth.actual_quantity == 0
                        else OpenPositionPolicy.MANAGE
                    ),
                    actor="execution-safety",
                )
            if truth.actual_quantity != 0 or truth.pending_quantity != 0:
                raise ExecutionRejected("EMERGENCY_DERISK_UNCONFIRMED")
            raise ExecutionRejected("PROTECTION_UNCONFIRMED")
        return entry

    def _ensure_submitted(
        self, intent_id: str, request: OrderRequest, now: datetime
    ) -> bool:
        return reserve_order_submission(
            self.connection,
            event_id=(
                f"submitted:{'reduce' if request.reduce_only else 'open'}:"
                f"{request.client_order_id}"
            ),
            intent_id=intent_id,
            order_id=request.client_order_id,
            quantity=request.quantity,
            price=request.price,
            created_at=now.isoformat(),
        )

    def _record_rejected(
        self, intent_id: str, request: OrderRequest, now: datetime
    ) -> None:
        append_order_event(
            self.connection,
            event_id=f"rejected-local:{request.client_order_id}",
            intent_id=intent_id,
            order_id=request.client_order_id,
            status="REJECTED",
            quantity=request.quantity,
            filled_quantity=Decimal(0),
            price=request.price,
            created_at=now.isoformat(),
        )

    def _durable_terminal_order(self, order_id: str) -> VenueOrder | None:
        row = self.connection.execute(
            "SELECT status, quantity, filled_quantity, price, event_id "
            "FROM order_events WHERE order_id = ? ORDER BY seq DESC LIMIT 1",
            (order_id,),
        ).fetchone()
        if row is None or row[0] not in {"REJECTED", "FILLED", "CANCELED", "EXPIRED"}:
            return None
        return VenueOrder(
            client_order_id=order_id,
            status=str(row[0]),
            quantity=Decimal(str(row[1])),
            filled_quantity=Decimal(str(row[2])),
            price=None if row[3] is None else Decimal(str(row[3])),
            event_id=str(row[4]),
        )

    def _record_observed(
        self,
        intent_id: str,
        order: VenueOrder,
        now: datetime,
        *,
        recovered: bool,
    ) -> None:
        latest = self.connection.execute(
            "SELECT status, filled_quantity FROM order_events "
            "WHERE order_id = ? ORDER BY seq DESC LIMIT 1",
            (order.client_order_id,),
        ).fetchone()
        if latest == (order.status, str(order.filled_quantity)):
            return
        append_order_event(
            self.connection,
            event_id=order.event_id,
            intent_id=intent_id,
            order_id=order.client_order_id,
            status=order.status,
            quantity=order.quantity,
            filled_quantity=order.filled_quantity,
            price=order.price,
            created_at=now.isoformat(),
            recovered=recovered and latest is None,
        )
