import asyncio
import json
from decimal import Decimal

from aiohttp.test_utils import TestClient, TestServer

from alma.market_api import create_readonly_app, snapshot_payload
from alma.market_state import MarketState

SECOND = 1_000_000_000


def populated_state() -> MarketState:
    state = MarketState("BINANCE", "BTCUSDT-PERP")
    state.on_quote(SECOND, Decimal(100), Decimal(101), Decimal(2), Decimal(1))
    state.on_trade(SECOND, Decimal("100.5"), Decimal("0.2"), aggressor=1)
    state.metrics.observe_latency(2_000_000)
    return state


def test_snapshot_payload_is_json_safe_and_stable() -> None:
    state = populated_state()

    payload = snapshot_payload(state.snapshot(SECOND))

    assert payload["bid"] == "100"
    assert payload["spread"] == "1"
    assert payload["state_id"] == state.snapshot(SECOND).state_id
    json.dumps(payload)


def test_readonly_health_state_and_sse_endpoints(tmp_path) -> None:
    async def run() -> None:
        state = populated_state()
        (tmp_path / "events.parquet").write_bytes(b"1234")
        app = create_readonly_app(
            state,
            data_root=tmp_path,
            clock_ns=lambda: SECOND,
            stale_after_ms=1_000,
            sse_interval=0.01,
        )
        client = TestClient(TestServer(app))
        await client.start_server()
        try:
            health = await client.get("/api/health")
            assert health.status == 200
            assert await health.json() == {
                "ok": True,
                "book_valid": False,
                "event_count": 2,
                "gap_count": 0,
                "reconnect_count": 0,
                "market_age_ms": 0,
                "p95_processing_latency_ms": 2.0,
                "disk_bytes": 4,
            }

            response = await client.get("/api/state")
            state_payload = await response.json()
            assert state_payload["state_id"] == state.snapshot(SECOND).state_id
            assert state_payload["ask"] == "101"

            events = await client.get("/api/events")
            assert events.headers["Content-Type"].startswith("text/event-stream")
            assert (await events.content.readline()) == b"event: state\n"
            data = await events.content.readline()
            assert (
                json.loads(data.removeprefix(b"data: "))["state_id"]
                == state_payload["state_id"]
            )
            events.close()
        finally:
            await client.close()

    asyncio.run(run())


def test_health_fails_closed_for_missing_or_stale_state(tmp_path) -> None:
    async def run() -> None:
        state = MarketState("BINANCE", "BTCUSDT-PERP")
        app = create_readonly_app(
            state,
            data_root=tmp_path,
            clock_ns=lambda: 2 * SECOND,
            stale_after_ms=10,
        )
        client = TestClient(TestServer(app))
        await client.start_server()
        try:
            response = await client.get("/api/health")
            assert response.status == 503
            assert (await response.json())["reason"] == "NO_MARKET_STATE"

            state.on_trade(SECOND, Decimal(100), Decimal(1))
            response = await client.get("/api/health")
            assert response.status == 503
            assert (await response.json())["reason"] == "MARKET_STATE_STALE"
        finally:
            await client.close()

    asyncio.run(run())
