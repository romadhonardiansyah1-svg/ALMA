import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

_ALLOWED_HOOKS = frozenset(
    {
        "M1_CLOSE",
        "M5_CLOSE",
        "SETUP",
        "SWEEP_RECLAIM",
        "ACCEPTANCE",
        "REGIME_SHIFT",
        "FLOW_SHIFT",
        "FUNDING_SHIFT",
        "LIQUIDATION_SHIFT",
        "ENVELOPE_CHANGE",
        "FILL",
        "REJECT",
        "PARTIAL",
        "CANCEL",
        "THESIS_INVALIDATION",
        "ACCOUNT_CHANGE",
        "MARGIN_CHANGE",
        "MANUAL_POSITION",
        "NEWS",
    }
)
_SECRET_KEY_PARTS = (
    "secret",
    "token",
    "password",
    "credential",
    "api_key",
    "private_key",
)
_DEFAULT_MAX_PAYLOAD_BYTES = 64_000


@dataclass(frozen=True)
class ShadowContext:
    state_id: str
    observed_at_ns: int
    market_age_ms: int
    venue_mode: str
    venue: str
    symbol: str
    bid: Decimal
    ask: Decimal
    h1_regime: int
    h1_volatility: Decimal
    m15_position: Decimal | None
    m5_compression: Decimal | None
    flow_imbalance: Decimal
    account: dict[str, str]
    positions: tuple[dict[str, str], ...]
    pending_orders: tuple[dict[str, str], ...]
    news: dict[str, str | int | None]
    memory: tuple[dict[str, str | int], ...]


@dataclass(frozen=True)
class ShadowRequest:
    request_id: str
    state_id: str
    hooks: tuple[str, ...]
    payload: bytes
    prompt_hash: str


def safe_json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("context decimals must be finite")
        return str(value)
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("snapshot keys must be strings")
        converted = {}
        for key, item in sorted(value.items()):
            normalized = key.lower().replace("-", "_")
            if any(part in normalized for part in _SECRET_KEY_PARTS):
                raise ValueError("secret-like snapshot key is forbidden")
            converted[key] = safe_json_value(item)
        return converted
    if isinstance(value, (list, tuple)):
        return [safe_json_value(item) for item in value]
    return value


def build_shadow_request(
    context: ShadowContext,
    hooks: tuple[str, ...],
    *,
    max_payload_bytes: int = _DEFAULT_MAX_PAYLOAD_BYTES,
) -> ShadowRequest:
    if not context.state_id:
        raise ValueError("state ID is required")
    if context.observed_at_ns < 0:
        raise ValueError("observed time must be non-negative")
    if context.market_age_ms < 0:
        raise ValueError("market age must be non-negative")
    if not context.venue or not context.symbol:
        raise ValueError("venue and symbol are required")
    if max_payload_bytes <= 0:
        raise ValueError("max payload bytes must be positive")
    if not context.bid.is_finite() or not context.ask.is_finite() or context.bid <= 0:
        raise ValueError("bid and ask must be finite and positive")
    if context.ask < context.bid:
        raise ValueError("ask must not be below bid")
    normalized_hooks = tuple(sorted(set(hooks)))
    if not normalized_hooks or any(
        hook not in _ALLOWED_HOOKS for hook in normalized_hooks
    ):
        raise ValueError("unknown or empty hook set")

    body = {
        "state_id": context.state_id,
        "now": datetime.fromtimestamp(context.observed_at_ns / 1e9, tz=UTC).isoformat(),
        "observed_at_ns": context.observed_at_ns,
        "market_age_ms": context.market_age_ms,
        "venue_mode": context.venue_mode,
        "instrument": {
            "venue": context.venue,
            "symbol": context.symbol,
            "bid": context.bid,
            "ask": context.ask,
        },
        "regime": {
            "h1": context.h1_regime,
            "h1_volatility": context.h1_volatility,
        },
        "structure": {
            "m15_position": context.m15_position,
            "m5_compression": context.m5_compression,
        },
        "trigger": {"flow_imbalance": context.flow_imbalance},
        "account": context.account,
        "positions": context.positions,
        "pending_orders": context.pending_orders,
        "news": context.news,
        "memory": context.memory,
        "hooks": normalized_hooks,
    }
    payload = json.dumps(
        safe_json_value(body),
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    if len(payload) > max_payload_bytes:
        raise ValueError("shadow payload exceeds byte limit")
    digest = hashlib.sha256(payload).hexdigest()
    return ShadowRequest(
        request_id=f"shadow:{digest}",
        state_id=context.state_id,
        hooks=normalized_hooks,
        payload=payload,
        prompt_hash=digest,
    )


class HookCoalescer:
    def __init__(self) -> None:
        self._state_id: str | None = None
        self._hooks: set[str] = set()
        self._last_flushed_state_id: str | None = None
        self._cooldown_state_id: str | None = None
        self._cooldown_hooks: set[str] = set()

    def accept(self, *, hook: str, state_id: str) -> bool:
        if hook not in _ALLOWED_HOOKS:
            raise ValueError("unknown hook")
        if not state_id:
            raise ValueError("state ID is required")
        if state_id == self._last_flushed_state_id:
            return False
        if self._state_id != state_id:
            self._state_id = state_id
            self._hooks.clear()
        before = len(self._hooks)
        self._hooks.add(hook)
        return len(self._hooks) != before

    def accept_cooldown(self, hook: str, state_id: str) -> bool:
        if hook not in _ALLOWED_HOOKS:
            raise ValueError("unknown hook")
        if not state_id:
            raise ValueError("state ID is required")
        self._cooldown_state_id = state_id
        self._cooldown_hooks.add(hook)
        return True

    def add(self, hook: str, state_id: str) -> bool:
        if hook == "TICK":
            return False
        return self.accept(hook=hook, state_id=state_id)

    def flush(self, context: ShadowContext) -> ShadowRequest | None:
        if not self._hooks:
            return None
        if context.state_id != self._state_id:
            raise ValueError("context state does not match queued hooks")
        request = build_shadow_request(context, tuple(self._hooks))
        self._hooks.clear()
        self._last_flushed_state_id = context.state_id
        return request
