import asyncio
import json

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from alma.decision_fallback import ProviderUnavailable
from alma.shadow_request import ShadowRequest
from alma.shadow_transport import LoopbackOpenAITransport, _sse_envelope


def request() -> ShadowRequest:
    payload = b'{"state_id":"state-1"}'
    return ShadowRequest(
        request_id="shadow:1",
        state_id="state-1",
        hooks=("M1_CLOSE",),
        payload=payload,
        prompt_hash="a" * 64,
    )


def test_sse_accepts_data_field_without_optional_space() -> None:
    envelope = _sse_envelope(
        b'data:{"choices":[{"delta":{"content":"ok"}}]}\n\ndata:[DONE]\n\n',
        "model-a",
    )

    assert envelope["choices"][0]["message"]["content"] == "ok"


def test_prompt_preserves_ai_discretion_without_tactical_defaults() -> None:
    prompt = LoopbackOpenAITransport.SYSTEM_PROMPT
    assert "prefer AGGRESSIVE_LIMIT" not in prompt
    assert "WAIT_RETEST only" not in prompt
    assert "do not default to NO_CHANGE" not in prompt
    assert '"ttl_seconds":45' not in prompt
    assert 'review_triggers"' in prompt
    assert "not placeholders" in prompt
    assert "Choose the lowest-risk valid answer" in prompt
    assert "why any requested action is technically unavailable" in prompt

def test_loopback_transport_sends_bounded_openai_request_and_returns_telemetry() -> (
    None
):
    async def run() -> None:
        seen = {}

        async def completion(received: web.Request) -> web.Response:
            seen.update(await received.json())
            return web.json_response(
                {
                    "model": "router/model-a",
                    "choices": [{"message": '{"ok":true}'}],
                    "usage": {"prompt_tokens": 120, "completion_tokens": 30},
                }
            )

        app = web.Application()
        app.router.add_post("/v1/chat/completions", completion)
        client = TestClient(TestServer(app))
        await client.start_server()
        try:
            transport = LoopbackOpenAITransport(
                str(client.make_url("")),
                model="model-a",
                timeout_seconds=1,
                max_tokens=256,
            )
            response = await transport.complete(request())
        finally:
            await client.close()

        assert seen["model"] == "model-a"
        assert seen["max_tokens"] == 256
        assert seen["temperature"] == 0
        assert seen["stream"] is False
        assert '"policy_version":"alma-v1"' in seen["messages"][0]["content"]
        assert "copy venue and symbol from input" in seen["messages"][0]["content"]
        assert "sovereign within the contract" in seen["messages"][0]["content"]
        assert (
            "AGGRESSIVE_LIMIT, STOP_ENTRY, MARKET_PROTECTED"
            in seen["messages"][0]["content"]
        )
        assert '"mode":"WAIT_RETEST"' not in seen["messages"][0]["content"]
        assert "target with price and close_fraction" in seen["messages"][0]["content"]
        assert ("review_triggers" in seen["messages"][0]["content"])
        assert '"close_fraction"' in seen["messages"][0]["content"]
        assert "prefer AGGRESSIVE_LIMIT" not in seen["messages"][0]["content"]
        assert '"venue":"BINANCE"' not in seen["messages"][0]["content"]
        assert "No other fields" in seen["messages"][0]["content"]
        assert json.loads(seen["messages"][1]["content"])["state_id"] == "state-1"
        assert response.content == b'{"ok":true}'
        assert response.actual_model == "router/model-a"
        assert response.prompt_tokens == 120
        assert response.completion_tokens == 30
        assert response.latency_ms >= 0

    asyncio.run(run())


def test_transport_accepts_router_sse_even_when_stream_was_not_requested() -> None:
    async def run() -> None:
        async def completion(received: web.Request) -> web.Response:
            assert (await received.json())["stream"] is False
            chunks = (
                'data: {"model":"router/model-a","choices":[{"delta":{"content":"{\\"ok\\":"}}]}\n\n'
                'data: {"choices":[{"delta":{"content":"true}"}}],'
                '"usage":{"prompt_tokens":12,"completion_tokens":3}}\n\n'
                "data: [DONE]\n\n"
            )
            return web.Response(text=chunks, content_type="text/event-stream")

        app = web.Application()
        app.router.add_post("/v1/chat/completions", completion)
        client = TestClient(TestServer(app))
        await client.start_server()
        try:
            response = await LoopbackOpenAITransport(
                str(client.make_url("")), model="model-a", timeout_seconds=1
            ).complete(request())
        finally:
            await client.close()

        assert response.content == b'{"ok":true}'
        assert response.actual_model == "router/model-a"
        assert response.prompt_tokens == 12
        assert response.completion_tokens == 3

    asyncio.run(run())


def test_transport_classifies_only_infrastructure_failures_for_fallback() -> None:
    async def run(status: int) -> None:
        async def completion(_: web.Request) -> web.Response:
            return web.Response(status=status, text="failed")

        app = web.Application()
        app.router.add_post("/v1/chat/completions", completion)
        client = TestClient(TestServer(app))
        await client.start_server()
        try:
            transport = LoopbackOpenAITransport(
                str(client.make_url("")), model="model-a", timeout_seconds=1
            )
            if status in {402, 429, 500}:
                with pytest.raises(ProviderUnavailable):
                    await transport.complete(request())
            else:
                with pytest.raises(ValueError, match="non-retryable"):
                    await transport.complete(request())
        finally:
            await client.close()

    asyncio.run(run(402))
    asyncio.run(run(429))
    asyncio.run(run(500))
    asyncio.run(run(400))


def test_transport_timeout_and_response_cap_fail_closed() -> None:
    async def run() -> None:
        async def slow(_: web.Request) -> web.Response:
            await asyncio.sleep(0.05)
            return web.json_response({"choices": []})

        async def large(_: web.Request) -> web.Response:
            return web.Response(body=b"x" * 101)

        app = web.Application()
        app.router.add_post("/slow/v1/chat/completions", slow)
        app.router.add_post("/large/v1/chat/completions", large)
        client = TestClient(TestServer(app))
        await client.start_server()
        try:
            slow_transport = LoopbackOpenAITransport(
                str(client.make_url("/slow")),
                model="model-a",
                timeout_seconds=0.01,
            )
            with pytest.raises(TimeoutError):
                await slow_transport.complete(request())

            large_transport = LoopbackOpenAITransport(
                str(client.make_url("/large")),
                model="model-a",
                timeout_seconds=1,
                max_response_bytes=100,
            )
            with pytest.raises(ValueError, match="response exceeds"):
                await large_transport.complete(request())
        finally:
            await client.close()

    asyncio.run(run())


def test_transport_rejects_non_loopback_and_invalid_limits() -> None:
    assert (
        LoopbackOpenAITransport(
            "http://127.0.0.1:20128", model="model-a"
        ).timeout_seconds
        == 60
    )
    with pytest.raises(ValueError, match="loopback"):
        LoopbackOpenAITransport("https://example.com", model="model-a")
    with pytest.raises(ValueError, match="max_tokens"):
        LoopbackOpenAITransport("http://127.0.0.1:20128", model="model-a", max_tokens=0)
    with pytest.raises(ValueError, match="timeout"):
        LoopbackOpenAITransport(
            "http://127.0.0.1:20128", model="model-a", timeout_seconds=0
        )


def test_transport_sends_configured_bearer_key() -> None:
    async def run() -> None:
        async def completion(request: web.Request) -> web.Response:
            assert request.headers["Authorization"] == "Bearer router-key"
            return web.json_response(
                {
                    "model": "model-a",
                    "choices": [{"message": {"content": "{}"}}],
                    "usage": {},
                }
            )

        app = web.Application()
        app.router.add_post("/v1/chat/completions", completion)
        client = TestClient(TestServer(app))
        await client.start_server()
        try:
            await LoopbackOpenAITransport(
                str(client.make_url("")), model="model-a", api_key="router-key"
            ).complete(request())
        finally:
            await client.close()

    asyncio.run(run())
