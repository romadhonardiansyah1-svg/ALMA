from dataclasses import dataclass
from datetime import datetime, timedelta

from alma.execution import ExecutionRejected, ExecutionTruth, ExecutionVenue
from alma.mutation_gate import MutationGate, VenueTruth
from alma.venue_modes import OpenPositionPolicy, VenueMode


@dataclass(frozen=True)
class EmergencyStopResult:
    mode: VenueMode
    truth: ExecutionTruth
    canceled_entries: int


def execute_emergency_stop(
    gate: MutationGate,
    venue: ExecutionVenue,
    *,
    request_id: str,
    completion_request_id: str,
    venue_id: str,
    symbol: str,
    policy: OpenPositionPolicy,
    actor: str,
    now: datetime,
    max_truth_age: timedelta = timedelta(seconds=2),
) -> EmergencyStopResult:
    def sync(truth: ExecutionTruth) -> None:
        age = now - truth.observed_at
        if (
            now.utcoffset() != timedelta(0)
            or truth.observed_at.utcoffset() != timedelta(0)
            or age < timedelta(0)
            or age > max_truth_age
            or not truth.connected
            or not truth.state_id
        ):
            raise ExecutionRejected("STATE_STALE")
        gate.sync_venue(
            venue=venue_id,
            symbol=symbol,
            truth=VenueTruth(
                state_id=truth.state_id,
                observed_at=truth.observed_at,
                actual=truth.actual_quantity,
                pending=truth.pending_quantity,
            ),
        )

    truth = venue.truth(symbol)
    sync(truth)
    plan = gate.transition_mode(
        request_id=request_id,
        state_id=truth.state_id,
        timestamp=now,
        venue=venue_id,
        symbol=symbol,
        requested=VenueMode.OFF,
        policy=policy,
        actor=actor,
    )
    canceled = venue.cancel_open_entries(symbol)

    if plan.desired_quantity == 0 and truth.actual_quantity != 0:
        truth = venue.flatten_symbol(symbol)
    else:
        truth = venue.truth(symbol)
    sync(truth)

    if truth.actual_quantity != 0 and not venue.ensure_position_protected(symbol):
        raise ExecutionRejected("PROTECTION_UNCONFIRMED")
    if (
        plan.final_mode is not plan.active_mode
        and truth.actual_quantity == 0
        and truth.pending_quantity == 0
    ):
        mode = gate.complete_mode_transition(
            request_id=completion_request_id,
            state_id=truth.state_id,
            timestamp=now,
            venue=venue_id,
            symbol=symbol,
            actor=actor,
        )
    else:
        mode = plan.active_mode
    return EmergencyStopResult(mode, truth, len(canceled))
