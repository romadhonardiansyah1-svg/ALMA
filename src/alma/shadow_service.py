import sqlite3
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

import msgspec

from alma.decision_contract import (
    DecisionContract,
    parse_decision_contract,
    validate_decision_expiry,
    validate_decision_state,
)
from alma.decision_fallback import FallbackResult, request_with_fallback_report
from alma.ledger import record_shadow_run
from alma.reconciler import execution_delta
from alma.shadow_request import ShadowRequest
from alma.shadow_transport import ShadowResponse


@dataclass(frozen=True)
class ShadowResult:
    status: str
    decision: DecisionContract | None
    error: str | None
    hypothetical_delta: Decimal | None


class ShadowService:
    def __init__(
        self,
        connection: sqlite3.Connection,
        *,
        providers: Iterable[Callable[[ShadowRequest], Awaitable[ShadowResponse]]],
        policy_hash: str,
        code_hash: str,
        now: Callable[[], datetime],
        repair: Callable[[bytes], Awaitable[bytes]] | None = None,
        provenance: str = "SHADOW",
    ) -> None:
        if not policy_hash or not code_hash:
            raise ValueError("policy and code hashes are required")
        if provenance not in {"EXECUTION", "SHADOW"}:
            raise ValueError("unknown decision provenance")
        self.connection = connection
        self.providers = tuple(providers)
        self.policy_hash = policy_hash
        self.code_hash = code_hash
        self.now = now
        self.repair = repair
        self.provenance = provenance

    async def evaluate(
        self,
        request: ShadowRequest,
        *,
        venue: str,
        symbol: str,
        setup: str = "",
        regime: str = "",
        session: str = "",
        news_state: str = "",
        actual_quantity: Decimal = Decimal(0),
        pending_quantity: Decimal = Decimal(0),
        news: dict[str, str | int | None] | None = None,
        memory: tuple[dict[str, str | int], ...] = (),
        observed_state_id: str | None = None,
    ) -> ShadowResult:
        if not actual_quantity.is_finite() or not pending_quantity.is_finite():
            raise ValueError("shadow quantities must be finite")
        if news is not None and any(
            not isinstance(value, (str, int)) and value is not None for value in news.values()
        ):
            raise ValueError("news values must be str/int/None")
        if any(
            not isinstance(value, (str, int)) for item in memory for value in item.values()
        ):
            raise ValueError("memory values must be str/int")
        existing = self.connection.execute(
            "SELECT r.status, r.validation_error, r.hypothetical_delta, d.raw_contract "
            "FROM shadow_runs r LEFT JOIN decisions d ON d.decision_id = r.decision_id "
            "WHERE r.request_id = ?",
            (request.request_id,),
        ).fetchone()
        if existing is not None:
            contract = (
                parse_decision_contract(existing[3])
                if existing[3] is not None
                else None
            )
            try:
                delta = Decimal(existing[2]) if existing[2] is not None else None
            except (ArithmeticError, ValueError):
                delta = None  # ponytail: corrupt delta in DB shouldn't crash evaluate
            return ShadowResult(existing[0], contract, existing[1], delta)
        created_at = self.now().isoformat()
        fallback = await request_with_fallback_report(request, self.providers)
        response = fallback.response
        if fallback.terminal_error is not None:
            message = (
                f"{type(fallback.terminal_error).__name__}: {fallback.terminal_error}"
            )
            self._record(
                request,
                response=None,
                decision=None,
                status="NO_DECISION",
                error=message,
                setup=setup,
                regime=regime,
                session=session,
                venue=venue,
                symbol=symbol,
                news_state=news_state,
                hypothetical_delta=None,
                created_at=created_at,
                fallback=fallback,
            )
            return ShadowResult("NO_DECISION", None, message, None)
        if response is None:
            self._record(
                request,
                response=None,
                decision=None,
                status="NO_DECISION",
                error="PROVIDERS_EXHAUSTED",
                setup=setup,
                regime=regime,
                session=session,
                venue=venue,
                symbol=symbol,
                news_state=news_state,
                hypothetical_delta=None,
                created_at=created_at,
                fallback=fallback,
            )
            return ShadowResult("NO_DECISION", None, "PROVIDERS_EXHAUSTED", None)

        contract: DecisionContract | None = None
        validated_raw = response.content
        try:
            try:
                contract = parse_decision_contract(validated_raw)
            except msgspec.DecodeError:
                if self.repair is None:
                    raise
                try:
                    validated_raw = await self.repair(response.content)
                except (TimeoutError, ConnectionError, ValueError) as error:
                    raise ValueError("decision repair failed") from error
                contract = parse_decision_contract(validated_raw)
            validate_decision_state(contract, expected_state_id=request.state_id)
            validate_decision_expiry(contract, now=self.now())
            if (contract.venue, contract.symbol) != (venue, symbol):
                raise ValueError("decision instrument does not match shadow context")
        except (msgspec.DecodeError, ValueError) as error:
            message = f"{type(error).__name__}: {error}"
            self._record(
                request,
                response=response,
                decision=contract,
                raw_contract=validated_raw,
                status="REJECTED",
                error=message,
                setup=setup,
                regime=regime,
                session=session,
                venue=venue,
                symbol=symbol,
                news_state=news_state,
                hypothetical_delta=None,
                created_at=created_at,
                fallback=fallback,
            )
            return ShadowResult("REJECTED", contract, message, None)

        # ponytail: model may produce non-unique decision_id — mint our own on conflict
        if contract.decision_id and self.connection.execute(
            "SELECT 1 FROM decisions WHERE decision_id = ?",
            (contract.decision_id,),
        ).fetchone():
            import json as _json
            import uuid as _uuid
            _obj = _json.loads(validated_raw)
            _obj["decision_id"] = str(_uuid.uuid4())
            validated_raw = _json.dumps(_obj, separators=(",", ":")).encode()
            contract = parse_decision_contract(validated_raw)
            validate_decision_state(contract, expected_state_id=request.state_id)
            validate_decision_expiry(contract, now=self.now())
            if (contract.venue, contract.symbol) != (venue, symbol):
                raise ValueError("decision instrument does not match shadow context")

        desired = Decimal(contract.target.volume)
        if contract.target.side == "SHORT":
            desired = -desired
        elif contract.target.side == "FLAT":
            desired = Decimal(0)
        hypothetical_delta = execution_delta(
            desired=desired,
            actual=actual_quantity,
            pending=pending_quantity,
        )

        self._record(
            request,
            response=response,
            decision=contract,
            raw_contract=validated_raw,
            status="ACCEPTED",
            error=None,
            setup=setup,
            regime=regime,
            session=session,
            venue=venue,
            symbol=symbol,
            news_state=news_state,
            hypothetical_delta=hypothetical_delta,
            created_at=created_at,
            fallback=fallback,
        )
        return ShadowResult("ACCEPTED", contract, None, hypothetical_delta)

    def _record(
        self,
        request: ShadowRequest,
        *,
        response: ShadowResponse | None,
        decision: DecisionContract | None,
        raw_contract: bytes | None = None,
        status: str,
        error: str | None,
        setup: str,
        regime: str,
        session: str,
        venue: str,
        symbol: str,
        news_state: str,
        hypothetical_delta: Decimal | None,
        created_at: str,
        fallback: FallbackResult[ShadowResponse],
    ) -> None:
        decision_row = None
        if decision is not None and response is not None:
            decision_row = {
                "decision_id": decision.decision_id,
                "state_id": decision.state_id,
                "created_at": decision.created_at.isoformat(),
                "raw_contract": raw_contract
                if raw_contract is not None
                else response.content,
                "validation_result": status,
                "model_id": response.actual_model,
                "prompt_hash": request.prompt_hash,
                "policy_hash": self.policy_hash,
                "code_hash": self.code_hash,
            }
        record_shadow_run(
            self.connection,
            request_id=request.request_id,
            state_id=request.state_id,
            decision=decision_row,
            status=status,
            validation_error=error,
            requested_model=response.requested_model if response else "",
            actual_model=response.actual_model if response else "",
            prompt_tokens=response.prompt_tokens if response else 0,
            completion_tokens=response.completion_tokens if response else 0,
            latency_ms=fallback.elapsed_ms,
            attempt_count=fallback.attempts,
            failure_classes=",".join(fallback.failures),
            fallback_used=fallback.fallback_used,
            hooks=",".join(request.hooks),
            setup=setup,
            regime=regime,
            session=session,
            venue=venue,
            symbol=symbol,
            news_state=news_state,
            hypothetical_delta=hypothetical_delta,
            created_at=created_at,
            provenance=self.provenance,
        )
