import json
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from alma.database import immediate_transaction
from alma.ledger import record_intent_mutation
from alma.reconciler import execution_delta
from alma.venue_modes import (
    ModeTransitionPlan,
    OpenPositionPolicy,
    VenueMode,
    allows_quantity_change,
    plan_mode_transition,
)


class MutationRejected(RuntimeError):
    pass


@dataclass(frozen=True)
class VenueTruth:
    state_id: str
    observed_at: datetime
    actual: Decimal
    pending: Decimal


class MutationGate:
    def __init__(
        self,
        connection: sqlite3.Connection,
        *,
        max_age: timedelta,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if max_age <= timedelta(0):
            raise ValueError("max_age must be positive")
        self.connection = connection
        self.max_age = max_age
        self.clock = clock or (lambda: datetime.now(UTC))
        self._truth: dict[tuple[str, str], VenueTruth] = {}

    def sync_venue(self, *, venue: str, symbol: str, truth: VenueTruth) -> None:
        if truth.observed_at.utcoffset() != timedelta(0):
            raise ValueError("venue timestamp must be UTC")
        if not truth.actual.is_finite() or not truth.pending.is_finite():
            raise ValueError("venue quantities must be finite")
        self._truth[venue, symbol] = truth

    def invalidate(self, venue: str, symbol: str) -> None:
        self._truth.pop((venue, symbol), None)

    def _verified_truth(
        self,
        *,
        venue: str,
        symbol: str,
        state_id: str,
        timestamp: datetime,
    ) -> VenueTruth:
        if timestamp.utcoffset() != timedelta(0):
            raise MutationRejected("INVALID_TIMESTAMP")
        truth = self._truth.get((venue, symbol))
        if truth is None:
            raise MutationRejected("VENUE_STATE_MISSING")
        now = self.clock()
        if now.utcoffset() != timedelta(0):
            raise MutationRejected("INVALID_CLOCK")
        age = now - truth.observed_at
        if age < timedelta(0) or age > self.max_age:
            raise MutationRejected("STATE_STALE")
        if truth.state_id != state_id:
            raise MutationRejected("STATE_DIVERGED")
        return truth

    def _mode(self, venue: str) -> VenueMode:
        row = self.connection.execute(
            "SELECT mode FROM venue_modes WHERE venue_id = ?", (venue,)
        ).fetchone()
        if row is None:
            raise MutationRejected("MODE_BLOCKED")
        return VenueMode(row[0])

    def prepare_intent(
        self,
        *,
        intent_id: str,
        decision_id: str,
        request_id: str,
        state_id: str,
        timestamp: datetime,
        venue: str,
        symbol: str,
        desired: Decimal,
        actor: str,
    ) -> Decimal:
        with immediate_transaction(self.connection):
            truth = self._verified_truth(
                venue=venue,
                symbol=symbol,
                state_id=state_id,
                timestamp=timestamp,
            )
            mode = self._mode(venue)
            if not allows_quantity_change(mode, actual=truth.actual, desired=desired):
                raise MutationRejected("MODE_BLOCKED")
            decision = self.connection.execute(
                "SELECT state_id, validation_result, provenance "
                "FROM decisions WHERE decision_id = ?",
                (decision_id,),
            ).fetchone()
            if (
                decision is None
                or decision[1] != "ACCEPTED"
                or decision[2] != "EXECUTION"
            ):
                raise MutationRejected("DECISION_REJECTED")
            if decision[0] != state_id:
                raise MutationRejected("STATE_DIVERGED")
            delta = execution_delta(
                desired=desired, actual=truth.actual, pending=truth.pending
            )
            stored = record_intent_mutation(
                self.connection,
                audit_event_id=f"audit:{request_id}",
                actor=actor,
                before_summary=json.dumps(
                    {"actual": str(truth.actual), "pending": str(truth.pending)},
                    sort_keys=True,
                ),
                after_summary=json.dumps(
                    {"desired": str(desired), "execution_delta": str(delta)},
                    sort_keys=True,
                ),
                intent_id=intent_id,
                decision_id=decision_id,
                request_id=request_id,
                venue=venue,
                symbol=symbol,
                state_id=state_id,
                created_at=timestamp.isoformat(),
                mode=mode.value,
                desired_quantity=desired,
                actual_quantity=truth.actual,
                pending_quantity=truth.pending,
                execution_delta=delta,
            )
            if not stored:
                raise MutationRejected("DUPLICATE_REQUEST")
        return delta

    def transition_mode(
        self,
        *,
        request_id: str,
        state_id: str,
        timestamp: datetime,
        venue: str,
        symbol: str,
        requested: VenueMode,
        policy: OpenPositionPolicy | None,
        actor: str,
    ) -> ModeTransitionPlan:
        with immediate_transaction(self.connection):
            truth = self._verified_truth(
                venue=venue,
                symbol=symbol,
                state_id=state_id,
                timestamp=timestamp,
            )
            current = self._mode(venue)
            try:
                plan = plan_mode_transition(
                    current=current,
                    requested=requested,
                    actual=truth.actual,
                    policy=policy,
                )
            except ValueError as error:
                raise MutationRejected("POSITION_POLICY_REQUIRED") from error
            reserved = self.connection.execute(
                "INSERT OR IGNORE INTO request_ids VALUES (?)", (request_id,)
            )
            if reserved.rowcount != 1:
                raise MutationRejected("DUPLICATE_REQUEST")
            self.connection.execute(
                "INSERT INTO venue_modes VALUES (?, ?) "
                "ON CONFLICT(venue_id) DO UPDATE SET mode=excluded.mode",
                (venue, plan.active_mode.value),
            )
            self.connection.execute(
                "DELETE FROM pending_mode_transitions WHERE venue = ?",
                (venue,),
            )
            if plan.final_mode is not plan.active_mode:
                self.connection.execute(
                    "INSERT INTO pending_mode_transitions "
                    "(venue, symbol, state_id, final_mode, created_at, request_id) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        venue,
                        symbol,
                        state_id,
                        plan.final_mode.value,
                        timestamp.isoformat(),
                        request_id,
                    ),
                )
            self.connection.execute(
                "INSERT INTO audit_events "
                "(event_id, actor, action, request_id, created_at, before_summary, after_summary) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    f"audit:{request_id}",
                    actor,
                    "VENUE_MODE_TRANSITION",
                    request_id,
                    timestamp.isoformat(),
                    json.dumps({"mode": current.value}, sort_keys=True),
                    json.dumps(
                        {
                            "active_mode": plan.active_mode.value,
                            "final_mode": plan.final_mode.value,
                            "policy": None if policy is None else policy.value,
                        },
                        sort_keys=True,
                    ),
                ),
            )
        return plan

    def complete_mode_transition(
        self,
        *,
        request_id: str,
        state_id: str,
        timestamp: datetime,
        venue: str,
        symbol: str,
        actor: str,
    ) -> VenueMode:
        with immediate_transaction(self.connection):
            truth = self._verified_truth(
                venue=venue,
                symbol=symbol,
                state_id=state_id,
                timestamp=timestamp,
            )
            pending = self.connection.execute(
                "SELECT symbol, final_mode FROM pending_mode_transitions WHERE venue = ?",
                (venue,),
            ).fetchone()
            if pending is None or pending[0] != symbol:
                raise MutationRejected("NO_PENDING_TRANSITION")
            if truth.actual != 0 or truth.pending != 0:
                raise MutationRejected("POSITION_NOT_FLAT")
            final_mode = VenueMode(pending[1])
            current = self._mode(venue)
            reserved = self.connection.execute(
                "INSERT OR IGNORE INTO request_ids VALUES (?)",
                (request_id,),
            )
            if reserved.rowcount != 1:
                raise MutationRejected("DUPLICATE_REQUEST")
            self.connection.execute(
                "UPDATE venue_modes SET mode = ? WHERE venue_id = ?",
                (final_mode.value, venue),
            )
            self.connection.execute(
                "DELETE FROM pending_mode_transitions WHERE venue = ?",
                (venue,),
            )
            self.connection.execute(
                "INSERT INTO audit_events "
                "(event_id, actor, action, request_id, created_at, before_summary, after_summary) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    f"audit:{request_id}",
                    actor,
                    "VENUE_MODE_TRANSITION_COMPLETED",
                    request_id,
                    timestamp.isoformat(),
                    json.dumps({"mode": current.value}, sort_keys=True),
                    json.dumps({"mode": final_mode.value}, sort_keys=True),
                ),
            )
        return final_mode
