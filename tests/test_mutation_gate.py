import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from alma.ledger import append_decision, open_ledger
from alma.mutation_gate import MutationGate, MutationRejected, VenueTruth
from alma.venue_mode_store import initialize_venue_mode
from alma.venue_modes import OpenPositionPolicy, VenueMode

NOW = datetime(2026, 7, 31, 8, tzinfo=UTC)


def setup_connection(path, mode: VenueMode = VenueMode.TRADE):
    connection = open_ledger(path)
    initialize_venue_mode(connection, "BINANCE", mode)
    append_decision(
        connection,
        decision_id="decision-1",
        state_id="state-1",
        created_at=NOW.isoformat(),
        raw_contract=b"{}",
        validation_result="ACCEPTED",
        model_id="model",
        prompt_hash="prompt",
        policy_hash="policy",
        code_hash="code",
    )
    return connection


def mutation_gate(connection, *, now: datetime = NOW) -> MutationGate:
    return MutationGate(
        connection,
        max_age=timedelta(seconds=2),
        clock=lambda: now,
    )


def sync(
    gate: MutationGate, *, state_id: str = "state-1", age_seconds: int = 0
) -> None:
    gate.sync_venue(
        venue="BINANCE",
        symbol="BTCUSDT-PERP",
        truth=VenueTruth(
            state_id=state_id,
            observed_at=NOW - timedelta(seconds=age_seconds),
            actual=Decimal("0.25"),
            pending=Decimal("0.50"),
        ),
    )


def prepare(gate: MutationGate, request_id: str = "request-1") -> Decimal:
    return gate.prepare_intent(
        intent_id=f"intent-{request_id}",
        decision_id="decision-1",
        request_id=request_id,
        state_id="state-1",
        timestamp=NOW,
        venue="BINANCE",
        symbol="BTCUSDT-PERP",
        desired=Decimal(1),
        actor="decision-brain",
    )


def test_restart_requires_fresh_venue_resync_before_trade(tmp_path) -> None:
    path = tmp_path / "alma.db"
    connection = setup_connection(path)
    gate = mutation_gate(connection)
    with pytest.raises(MutationRejected, match="VENUE_STATE_MISSING"):
        prepare(gate)
    connection.close()

    reopened = open_ledger(path)
    try:
        restarted = mutation_gate(reopened)
        with pytest.raises(MutationRejected, match="VENUE_STATE_MISSING"):
            prepare(restarted)
        sync(restarted)
        assert prepare(restarted) == Decimal("0.25")
    finally:
        reopened.close()


def test_stale_and_divergent_venue_truth_fail_closed(tmp_path) -> None:
    connection = setup_connection(tmp_path / "alma.db")
    try:
        gate = mutation_gate(connection)
        sync(gate, age_seconds=3)
        with pytest.raises(MutationRejected, match="STATE_STALE"):
            prepare(gate)

        sync(gate, state_id="state-new")
        with pytest.raises(MutationRejected, match="STATE_DIVERGED"):
            prepare(gate)
    finally:
        connection.close()


def test_prepare_intent_atomically_reserves_request_and_audits(tmp_path) -> None:
    connection = setup_connection(tmp_path / "alma.db")
    try:
        gate = mutation_gate(connection)
        sync(gate)
        assert prepare(gate) == Decimal("0.25")

        assert connection.execute(
            "SELECT desired_quantity, actual_quantity, pending_quantity, execution_delta "
            "FROM intents"
        ).fetchone() == ("1", "0.25", "0.50", "0.25")
        assert connection.execute(
            "SELECT actor, action, request_id FROM audit_events"
        ).fetchone() == ("decision-brain", "INTENT_PREPARED", "request-1")
        with pytest.raises(MutationRejected, match="DUPLICATE_REQUEST"):
            prepare(gate)
        assert connection.execute("SELECT count(*) FROM intents").fetchone() == (1,)
        assert connection.execute("SELECT count(*) FROM audit_events").fetchone() == (
            1,
        )
    finally:
        connection.close()


def test_invalidation_and_non_trade_mode_block_mutation(tmp_path) -> None:
    connection = setup_connection(tmp_path / "alma.db", VenueMode.MONITOR)
    try:
        gate = mutation_gate(connection)
        sync(gate)
        with pytest.raises(MutationRejected, match="MODE_BLOCKED"):
            prepare(gate)
        gate.invalidate("BINANCE", "BTCUSDT-PERP")
        with pytest.raises(MutationRejected, match="VENUE_STATE_MISSING"):
            prepare(gate, "request-2")
    finally:
        connection.close()


def test_mode_transition_with_open_position_requires_policy_and_is_audited(
    tmp_path,
) -> None:
    connection = setup_connection(tmp_path / "alma.db")
    try:
        gate = mutation_gate(connection)
        sync(gate)
        with pytest.raises(MutationRejected, match="POSITION_POLICY_REQUIRED"):
            gate.transition_mode(
                request_id="mode-1",
                state_id="state-1",
                timestamp=NOW,
                venue="BINANCE",
                symbol="BTCUSDT-PERP",
                requested=VenueMode.OFF,
                policy=None,
                actor="operator",
            )

        plan = gate.transition_mode(
            request_id="mode-2",
            state_id="state-1",
            timestamp=NOW,
            venue="BINANCE",
            symbol="BTCUSDT-PERP",
            requested=VenueMode.OFF,
            policy=OpenPositionPolicy.CLOSE_AND_DISABLE,
            actor="operator",
        )
        assert plan.active_mode is VenueMode.MANAGE_ONLY
        assert plan.desired_quantity == Decimal(0)
        assert plan.final_mode is VenueMode.OFF
        assert connection.execute(
            "SELECT mode FROM venue_modes WHERE venue_id = 'BINANCE'"
        ).fetchone() == ("MANAGE_ONLY",)
        assert connection.execute(
            "SELECT action, request_id FROM audit_events"
        ).fetchone() == ("VENUE_MODE_TRANSITION", "mode-2")
    finally:
        connection.close()


def test_mode_transition_plan_covers_manage_freeze_and_flat_position(tmp_path) -> None:
    connection = setup_connection(tmp_path / "alma.db")
    try:
        gate = mutation_gate(connection)
        sync(gate)
        manage = gate.transition_mode(
            request_id="manage",
            state_id="state-1",
            timestamp=NOW,
            venue="BINANCE",
            symbol="BTCUSDT-PERP",
            requested=VenueMode.MANAGE_ONLY,
            policy=OpenPositionPolicy.MANAGE,
            actor="operator",
        )
        assert (
            manage.active_mode,
            manage.desired_quantity,
            manage.ensure_protection,
        ) == (
            VenueMode.MANAGE_ONLY,
            Decimal("0.25"),
            True,
        )

        gate.transition_mode(
            request_id="retrade",
            state_id="state-1",
            timestamp=NOW,
            venue="BINANCE",
            symbol="BTCUSDT-PERP",
            requested=VenueMode.TRADE,
            policy=None,
            actor="operator",
        )
        freeze = gate.transition_mode(
            request_id="freeze",
            state_id="state-1",
            timestamp=NOW,
            venue="BINANCE",
            symbol="BTCUSDT-PERP",
            requested=VenueMode.MONITOR,
            policy=OpenPositionPolicy.FREEZE,
            actor="operator",
        )
        assert (
            freeze.active_mode,
            freeze.desired_quantity,
            freeze.ensure_protection,
        ) == (
            VenueMode.MONITOR,
            Decimal("0.25"),
            True,
        )

        gate.sync_venue(
            venue="BINANCE",
            symbol="BTCUSDT-PERP",
            truth=VenueTruth("state-1", NOW, Decimal(0), Decimal(0)),
        )
        flat = gate.transition_mode(
            request_id="flat",
            state_id="state-1",
            timestamp=NOW,
            venue="BINANCE",
            symbol="BTCUSDT-PERP",
            requested=VenueMode.OFF,
            policy=None,
            actor="operator",
        )
        assert (flat.active_mode, flat.final_mode) == (VenueMode.OFF, VenueMode.OFF)
    finally:
        connection.close()


def test_failed_mode_audit_rolls_back_mode_and_request(tmp_path) -> None:
    connection = setup_connection(tmp_path / "alma.db")
    try:
        gate = mutation_gate(connection)
        sync(gate)
        connection.execute(
            "INSERT INTO audit_events "
            "(event_id, actor, action, request_id, created_at, before_summary, after_summary) "
            "VALUES ('audit:mode-crash', 'test', 'EXISTING', NULL, ?, '{}', '{}')",
            (NOW.isoformat(),),
        )
        connection.commit()

        with pytest.raises(sqlite3.IntegrityError):
            gate.transition_mode(
                request_id="mode-crash",
                state_id="state-1",
                timestamp=NOW,
                venue="BINANCE",
                symbol="BTCUSDT-PERP",
                requested=VenueMode.OFF,
                policy=OpenPositionPolicy.FREEZE,
                actor="operator",
            )

        assert connection.execute(
            "SELECT mode FROM venue_modes WHERE venue_id = 'BINANCE'"
        ).fetchone() == ("TRADE",)
        assert connection.execute(
            "SELECT count(*) FROM request_ids WHERE request_id = 'mode-crash'"
        ).fetchone() == (0,)
    finally:
        connection.close()


def test_request_timestamp_cannot_make_stale_venue_truth_fresh(tmp_path) -> None:
    connection = setup_connection(tmp_path / "alma.db")
    try:
        gate = mutation_gate(connection, now=NOW + timedelta(seconds=3))
        sync(gate)
        with pytest.raises(MutationRejected, match="STATE_STALE"):
            prepare(gate)
    finally:
        connection.close()


def test_rejected_decision_cannot_create_intent(tmp_path) -> None:
    connection = setup_connection(tmp_path / "alma.db")
    try:
        append_decision(
            connection,
            decision_id="decision-rejected",
            state_id="state-1",
            created_at=NOW.isoformat(),
            raw_contract=b"{}",
            validation_result="REJECTED",
            model_id="model",
            prompt_hash="prompt",
            policy_hash="policy",
            code_hash="code",
        )
        gate = mutation_gate(connection)
        sync(gate)
        with pytest.raises(MutationRejected, match="DECISION_REJECTED"):
            gate.prepare_intent(
                intent_id="intent-rejected",
                decision_id="decision-rejected",
                request_id="request-rejected",
                state_id="state-1",
                timestamp=NOW,
                venue="BINANCE",
                symbol="BTCUSDT-PERP",
                desired=Decimal(1),
                actor="decision-brain",
            )
        assert connection.execute(
            "SELECT count(*) FROM intents WHERE intent_id = 'intent-rejected'"
        ).fetchone() == (0,)
    finally:
        connection.close()


def test_close_and_disable_survives_restart_and_completes_only_when_flat(
    tmp_path,
) -> None:
    path = tmp_path / "alma.db"
    connection = setup_connection(path)
    gate = mutation_gate(connection)
    sync(gate)
    gate.transition_mode(
        request_id="close-start",
        state_id="state-1",
        timestamp=NOW,
        venue="BINANCE",
        symbol="BTCUSDT-PERP",
        requested=VenueMode.OFF,
        policy=OpenPositionPolicy.CLOSE_AND_DISABLE,
        actor="operator",
    )
    connection.close()

    reopened = open_ledger(path)
    try:
        restarted = mutation_gate(reopened)
        sync(restarted)
        with pytest.raises(MutationRejected, match="POSITION_NOT_FLAT"):
            restarted.complete_mode_transition(
                request_id="close-finish-early",
                state_id="state-1",
                timestamp=NOW,
                venue="BINANCE",
                symbol="BTCUSDT-PERP",
                actor="executor",
            )

        restarted.sync_venue(
            venue="BINANCE",
            symbol="BTCUSDT-PERP",
            truth=VenueTruth("state-1", NOW, Decimal(0), Decimal(0)),
        )
        assert (
            restarted.complete_mode_transition(
                request_id="close-finish",
                state_id="state-1",
                timestamp=NOW,
                venue="BINANCE",
                symbol="BTCUSDT-PERP",
                actor="executor",
            )
            is VenueMode.OFF
        )
        assert reopened.execute(
            "SELECT mode FROM venue_modes WHERE venue_id = 'BINANCE'"
        ).fetchone() == ("OFF",)
        assert reopened.execute(
            "SELECT count(*) FROM pending_mode_transitions WHERE venue = 'BINANCE'"
        ).fetchone() == (0,)
    finally:
        reopened.close()


def test_prepare_intent_locks_before_reading_mode(tmp_path) -> None:
    connection = setup_connection(tmp_path / "alma.db")
    statements: list[str] = []
    try:
        gate = mutation_gate(connection)
        sync(gate)
        connection.set_trace_callback(statements.append)
        prepare(gate)
        assert statements.index("BEGIN IMMEDIATE") < next(
            index
            for index, statement in enumerate(statements)
            if statement.startswith("SELECT mode FROM venue_modes")
        )
    finally:
        connection.close()


def test_shadow_decision_cannot_create_intent(tmp_path) -> None:
    connection = setup_connection(tmp_path / "alma.db")
    try:
        append_decision(
            connection,
            decision_id="decision-shadow",
            state_id="state-1",
            created_at=NOW.isoformat(),
            raw_contract=b"{}",
            validation_result="ACCEPTED",
            model_id="model",
            prompt_hash="prompt",
            policy_hash="policy",
            code_hash="code",
            provenance="SHADOW",
        )
        gate = mutation_gate(connection)
        sync(gate)
        with pytest.raises(MutationRejected, match="DECISION_REJECTED"):
            gate.prepare_intent(
                intent_id="intent-shadow",
                decision_id="decision-shadow",
                request_id="request-shadow",
                state_id="state-1",
                timestamp=NOW,
                venue="BINANCE",
                symbol="BTCUSDT-PERP",
                desired=Decimal(1),
                actor="decision-brain",
            )
        assert connection.execute("SELECT count(*) FROM intents").fetchone() == (0,)
    finally:
        connection.close()
