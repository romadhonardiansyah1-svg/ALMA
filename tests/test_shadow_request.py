from decimal import Decimal

import pytest

from alma.shadow_request import HookCoalescer, ShadowContext, build_shadow_request


def context(state_id: str = "state-1") -> ShadowContext:
    return ShadowContext(
        state_id=state_id,
        observed_at_ns=1_785_492_000_000_000_000,
        market_age_ms=25,
        venue_mode="MONITOR",
        venue="BINANCE",
        symbol="BTCUSDT-PERP",
        bid=Decimal("63600.1"),
        ask=Decimal("63600.2"),
        h1_regime=1,
        h1_volatility=Decimal("0.0012"),
        m15_position=Decimal("0.8"),
        m5_compression=Decimal("0.4"),
        flow_imbalance=Decimal("0.31"),
        account={"equity": "1000.00", "free_margin": "700.00"},
        positions=(),
        pending_orders=(),
        news={"state": "NONE"},
        memory=(),
    )


def test_shadow_request_is_compact_stable_and_immutable() -> None:
    first = build_shadow_request(context(), ("M1_CLOSE", "FLOW_SHIFT"))
    second = build_shadow_request(context(), ("FLOW_SHIFT", "M1_CLOSE"))

    assert first.request_id == second.request_id
    assert first.payload == second.payload
    assert first.hooks == ("FLOW_SHIFT", "M1_CLOSE")
    assert b'"state_id":"state-1"' in first.payload
    assert b'"equity":"1000.00"' in first.payload
    assert b"credential" not in first.payload.lower()
    assert len(first.prompt_hash) == 64


def test_hook_coalescer_skips_ticks_duplicates_and_unchanged_state() -> None:
    hooks = HookCoalescer()

    assert hooks.add("TICK", "state-1") is False
    assert hooks.add("M1_CLOSE", "state-1") is True
    assert hooks.add("M1_CLOSE", "state-1") is False
    assert hooks.flush(context()) is not None
    assert hooks.flush(context()) is None

    assert hooks.add("FLOW_SHIFT", "state-1") is False
    assert hooks.add("FLOW_SHIFT", "state-2") is True
    request = hooks.flush(context("state-2"))
    assert request is not None and request.hooks == ("FLOW_SHIFT",)


def test_shadow_request_rejects_untrusted_or_invalid_context() -> None:
    with pytest.raises(ValueError, match="hook"):
        build_shadow_request(context(), ("NOT_A_HOOK",))
    with pytest.raises(ValueError, match="age"):
        build_shadow_request(
            context().__class__(**{**context().__dict__, "market_age_ms": -1}),
            ("M1_CLOSE",),
        )
    with pytest.raises(ValueError, match="state"):
        build_shadow_request(context(""), ("M1_CLOSE",))


def test_shadow_request_rejects_secrets_and_oversized_context() -> None:
    base = context()
    secret = base.__class__(
        **{**base.__dict__, "account": {"api_secret": "must-not-leave"}}
    )
    with pytest.raises(ValueError, match="secret-like"):
        build_shadow_request(secret, ("M1_CLOSE",))

    large = base.__class__(**{**base.__dict__, "memory": ({"summary": "x" * 1_000},)})
    with pytest.raises(ValueError, match="byte limit"):
        build_shadow_request(large, ("M1_CLOSE",), max_payload_bytes=500)

    mixed_keys = base.__class__(
        **{**base.__dict__, "account": {"equity": "1", 2: "invalid"}}
    )
    with pytest.raises(TypeError, match="keys must be strings"):
        build_shadow_request(mixed_keys, ("M1_CLOSE",))
