import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from alma.ledger import open_ledger
from alma.shadow_request import ShadowRequest
from alma.shadow_service import ShadowService
from alma.shadow_transport import ShadowResponse

NOW = datetime(2026, 7, 31, 5, 0, 1, tzinfo=UTC)


def request() -> ShadowRequest:
    payload = b'{"state_id":"state-1"}'
    return ShadowRequest(
        request_id="shadow:req-1",
        state_id="state-1",
        hooks=("M1_CLOSE", "SETUP"),
        payload=payload,
        prompt_hash="a" * 64,
    )


def decision(**changes) -> bytes:
    body = {
        "policy_version": "alma-v1",
        "state_id": "state-1",
        "decision_id": "decision-1",
        "created_at": "2026-07-31T05:00:00Z",
        "venue": "BINANCE",
        "symbol": "BTCUSDT-PERP",
        "action": "NO_CHANGE",
        "target": {"side": "FLAT", "volume": "0"},
        "entry": {
            "mode": "WAIT_RETEST",
            "preferred_low": "63000",
            "preferred_high": "64000",
            "max_acceptable_price": "64000",
            "ttl_seconds": 45,
            "on_missed": "ABORT",
            "on_partial_fill": "CANCEL_REMAINDER",
        },
        "invalidation_price": "62000",
        "targets": [],
        "review_triggers": ["FLOW_SHIFT"],
        "evidence": ["shadow"],
        "uncertainty": "0.4",
    }
    body.update(changes)
    return json.dumps(body).encode()


def response(content: bytes, model: str = "model-a") -> ShadowResponse:
    return ShadowResponse(
        content=content,
        requested_model=model,
        actual_model=f"actual/{model}",
        prompt_tokens=100,
        completion_tokens=50,
        latency_ms=12.5,
    )


def test_shadow_accepts_and_persists_decision_without_intent(tmp_path) -> None:
    async def run() -> None:
        connection = open_ledger(tmp_path / "alma.db")

        async def provider(received: ShadowRequest) -> ShadowResponse:
            assert received is request_value
            return response(decision())

        request_value = request()
        service = ShadowService(
            connection,
            providers=[provider],
            policy_hash="policy-1",
            code_hash="code-1",
            now=lambda: NOW,
        )
        result = await service.evaluate(
            request_value,
            venue="BINANCE",
            symbol="BTCUSDT-PERP",
            setup="LIQUIDITY_SWEEP_REVERSAL",
            regime="BULL_LOW_VOL",
            session="LONDON",
            actual_quantity=Decimal("0.25"),
            pending_quantity=Decimal("0.10"),
        )

        assert result.status == "ACCEPTED"
        assert result.decision is not None
        assert result.decision.decision_id == "decision-1"
        assert result.hypothetical_delta == Decimal("-0.35")
        repeated = await service.evaluate(
            request_value,
            venue="BINANCE",
            symbol="BTCUSDT-PERP",
            setup="LIQUIDITY_SWEEP_REVERSAL",
            regime="BULL_LOW_VOL",
            session="LONDON",
        )
        assert repeated == result
        assert connection.execute("SELECT count(*) FROM decisions").fetchone() == (1,)
        assert connection.execute("SELECT count(*) FROM shadow_runs").fetchone() == (1,)
        assert connection.execute("SELECT count(*) FROM intents").fetchone() == (0,)
        assert connection.execute("SELECT count(*) FROM order_events").fetchone() == (
            0,
        )
        run_row = connection.execute(
            "SELECT status, actual_model, prompt_tokens, completion_tokens, setup, regime, "
            "session, hypothetical_delta "
            "FROM shadow_runs"
        ).fetchone()
        assert run_row == (
            "ACCEPTED",
            "actual/model-a",
            100,
            50,
            "LIQUIDITY_SWEEP_REVERSAL",
            "BULL_LOW_VOL",
            "LONDON",
            "-0.35",
        )
        connection.close()

    asyncio.run(run())


def test_execution_decision_persists_execution_provenance(tmp_path) -> None:
    async def run() -> None:
        connection = open_ledger(tmp_path / "alma.db")

        async def provider(_: ShadowRequest) -> ShadowResponse:
            return response(decision())

        result = await ShadowService(
            connection,
            providers=[provider],
            policy_hash="policy-1",
            code_hash="code-1",
            provenance="EXECUTION",
            now=lambda: NOW,
        ).evaluate(request(), venue="BINANCE", symbol="BTCUSDT-PERP")

        assert result.status == "ACCEPTED"
        assert connection.execute(
            "SELECT provenance FROM decisions WHERE decision_id='decision-1'"
        ).fetchone() == ("EXECUTION",)
        connection.close()

    asyncio.run(run())


def test_duplicate_model_decision_id_is_rewritten_without_crashing(tmp_path) -> None:
    async def run() -> None:
        connection = open_ledger(tmp_path / "alma.db")

        async def provider(received: ShadowRequest) -> ShadowResponse:
            return response(decision(state_id=received.state_id))

        service = ShadowService(
            connection,
            providers=[provider],
            policy_hash="policy-1",
            code_hash="code-1",
            now=lambda: NOW,
        )
        first = await service.evaluate(
            request(), venue="BINANCE", symbol="BTCUSDT-PERP"
        )
        second_request = ShadowRequest(
            request_id="shadow:req-2",
            state_id="state-2",
            hooks=("ACCOUNT_CHANGE",),
            payload=b'{"state_id":"state-2"}',
            prompt_hash="b" * 64,
        )
        second = await service.evaluate(
            second_request, venue="BINANCE", symbol="BTCUSDT-PERP"
        )

        # ponytail: model decision_id collision → runtime mints a fresh uuid, both ACCEPTED
        assert first.status == "ACCEPTED"
        assert second.status == "ACCEPTED"
        assert connection.execute("SELECT count(*) FROM decisions").fetchone() == (2,)
        assert connection.execute(
            "SELECT status FROM shadow_runs ORDER BY seq"
        ).fetchall() == [("ACCEPTED",), ("ACCEPTED",)]
        connection.close()

    asyncio.run(run())


def test_shadow_repairs_schema_once_but_not_semantic_failure(tmp_path) -> None:
    async def run() -> None:
        connection = open_ledger(tmp_path / "alma.db")
        repair_calls = []

        async def malformed(_: ShadowRequest) -> ShadowResponse:
            return response(b"{")

        async def repair(raw: bytes) -> bytes:
            repair_calls.append(raw)
            return decision(decision_id="repaired")

        service = ShadowService(
            connection,
            providers=[malformed],
            policy_hash="policy-1",
            code_hash="code-1",
            repair=repair,
            now=lambda: NOW,
        )
        accepted = await service.evaluate(
            request(), venue="BINANCE", symbol="BTCUSDT-PERP"
        )
        assert accepted.status == "ACCEPTED"
        assert repair_calls == [b"{"]
        assert connection.execute(
            "SELECT raw_contract FROM decisions WHERE decision_id = 'repaired'"
        ).fetchone() == (decision(decision_id="repaired"),)

        semantic_calls = []

        async def wrong_state(_: ShadowRequest) -> ShadowResponse:
            return response(decision(state_id="wrong", decision_id="wrong-state"))

        async def forbidden_repair(raw: bytes) -> bytes:
            semantic_calls.append(raw)
            return decision()

        rejected = await ShadowService(
            connection,
            providers=[wrong_state],
            policy_hash="policy-1",
            code_hash="code-1",
            repair=forbidden_repair,
            now=lambda: NOW,
        ).evaluate(
            ShadowRequest(
                request_id="shadow:semantic-failure",
                state_id="state-1",
                hooks=("M1_CLOSE",),
                payload=b"{}",
                prompt_hash="c" * 64,
            ),
            venue="BINANCE",
            symbol="BTCUSDT-PERP",
        )
        assert rejected.status == "REJECTED"
        assert semantic_calls == []
        assert connection.execute(
            "SELECT validation_result FROM decisions WHERE decision_id = 'wrong-state'"
        ).fetchone() == ("REJECTED",)
        assert connection.execute("SELECT count(*) FROM intents").fetchone() == (0,)
        connection.close()

    asyncio.run(run())


def test_shadow_exhaustion_records_no_decision_and_no_mutation(tmp_path) -> None:
    async def run() -> None:
        connection = open_ledger(tmp_path / "alma.db")

        async def unavailable(_: ShadowRequest) -> ShadowResponse:
            raise TimeoutError

        result = await ShadowService(
            connection,
            providers=[unavailable],
            policy_hash="policy-1",
            code_hash="code-1",
            now=lambda: NOW,
        ).evaluate(request(), venue="BINANCE", symbol="BTCUSDT-PERP")

        assert result.status == "NO_DECISION"
        assert result.decision is None
        assert connection.execute("SELECT count(*) FROM decisions").fetchone() == (0,)
        assert connection.execute("SELECT count(*) FROM intents").fetchone() == (0,)
        assert connection.execute("SELECT status FROM shadow_runs").fetchone() == (
            "NO_DECISION",
        )
        connection.close()

    asyncio.run(run())


def test_shadow_logs_non_retryable_provider_and_repair_failures(tmp_path) -> None:
    async def run() -> None:
        connection = open_ledger(tmp_path / "failures.db")

        async def invalid_response(_: ShadowRequest) -> ShadowResponse:
            raise ValueError("invalid provider envelope")

        first = await ShadowService(
            connection,
            providers=[invalid_response],
            policy_hash="policy-1",
            code_hash="code-1",
            now=lambda: NOW,
        ).evaluate(request(), venue="BINANCE", symbol="BTCUSDT-PERP")
        assert first.status == "NO_DECISION"

        async def malformed(_: ShadowRequest) -> ShadowResponse:
            return response(b"{")

        async def failed_repair(_: bytes) -> bytes:
            raise TimeoutError

        second_request = ShadowRequest(
            request_id="shadow:req-2",
            state_id="state-1",
            hooks=("M1_CLOSE",),
            payload=b"{}",
            prompt_hash="b" * 64,
        )
        second = await ShadowService(
            connection,
            providers=[malformed],
            policy_hash="policy-1",
            code_hash="code-1",
            repair=failed_repair,
            now=lambda: NOW,
        ).evaluate(second_request, venue="BINANCE", symbol="BTCUSDT-PERP")

        assert second.status == "REJECTED"
        assert connection.execute(
            "SELECT status, validation_error FROM shadow_runs ORDER BY seq"
        ).fetchall() == [
            ("NO_DECISION", "ValueError: invalid provider envelope"),
            ("REJECTED", "ValueError: decision repair failed"),
        ]
        assert connection.execute("SELECT count(*) FROM intents").fetchone() == (0,)
        connection.close()

    asyncio.run(run())


def test_shadow_rejects_expired_or_wrong_instrument(tmp_path) -> None:
    async def run(payload: bytes, message: str) -> None:
        connection = open_ledger(tmp_path / f"{message}.db")

        async def provider(_: ShadowRequest) -> ShadowResponse:
            return response(payload)

        result = await ShadowService(
            connection,
            providers=[provider],
            policy_hash="policy-1",
            code_hash="code-1",
            now=lambda: datetime(2026, 7, 31, 5, 1, 0, tzinfo=UTC),
        ).evaluate(request(), venue="BINANCE", symbol="BTCUSDT-PERP")
        assert result.status == "REJECTED"
        assert connection.execute("SELECT count(*) FROM intents").fetchone() == (0,)
        connection.close()

    asyncio.run(run(decision(), "expired"))
    asyncio.run(run(decision(symbol="ETHUSDT-PERP"), "instrument"))


def test_shadow_rejects_non_finite_account_quantity_before_provider(tmp_path) -> None:
    async def run() -> None:
        connection = open_ledger(tmp_path / "alma.db")
        called = False

        async def provider(_: ShadowRequest) -> ShadowResponse:
            nonlocal called
            called = True
            return response(decision())

        service = ShadowService(
            connection,
            providers=[provider],
            policy_hash="policy-1",
            code_hash="code-1",
            now=lambda: NOW,
        )
        with pytest.raises(ValueError, match="finite"):
            await service.evaluate(
                request(),
                venue="BINANCE",
                symbol="BTCUSDT-PERP",
                actual_quantity=Decimal("NaN"),
            )
        assert not called
        connection.close()

    asyncio.run(run())
