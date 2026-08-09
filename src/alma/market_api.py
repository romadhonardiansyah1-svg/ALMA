import asyncio
import json
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path
from typing import Any

from aiohttp import web

from alma.market_state import MarketSnapshot, MarketState


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def snapshot_payload(snapshot: MarketSnapshot) -> dict[str, object]:
    return _json_value(asdict(snapshot))


def _disk_bytes(root: Path) -> int:
    if not root.exists():
        return 0
    return sum(
        path.stat().st_size for path in root.rglob("*.parquet") if path.is_file()
    )


def create_readonly_app(
    state: MarketState,
    *,
    data_root: str | Path,
    clock_ns,
    stale_after_ms: int,
    sse_interval: float = 1.0,
) -> web.Application:
    if stale_after_ms <= 0:
        raise ValueError("stale_after_ms must be positive")
    if sse_interval <= 0:
        raise ValueError("sse_interval must be positive")
    root = Path(data_root)

    def current() -> MarketSnapshot:
        return state.snapshot(clock_ns())

    async def health(_: web.Request) -> web.Response:
        try:
            snapshot = current()
        except ValueError:
            return web.json_response(
                {"ok": False, "reason": "NO_MARKET_STATE"},
                status=503,
            )
        if snapshot.market_age_ms > stale_after_ms:
            return web.json_response(
                {
                    "ok": False,
                    "reason": "MARKET_STATE_STALE",
                    "market_age_ms": snapshot.market_age_ms,
                },
                status=503,
            )
        metrics = state.metrics
        return web.json_response(
            {
                "ok": True,
                "book_valid": snapshot.book_valid,
                "event_count": metrics.event_count,
                "gap_count": metrics.gap_count,
                "reconnect_count": metrics.reconnect_count,
                "market_age_ms": snapshot.market_age_ms,
                "p95_processing_latency_ms": metrics.p95_latency_ms,
                "disk_bytes": _disk_bytes(root),
            }
        )

    async def market_state(_: web.Request) -> web.Response:
        try:
            return web.json_response(snapshot_payload(current()))
        except ValueError as error:
            return web.json_response(
                {"error": str(error)},
                status=503,
            )

    async def events(_: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
            }
        )
        await response.prepare(_)
        try:
            while True:
                payload = json.dumps(snapshot_payload(current()), separators=(",", ":"))
                await response.write(f"event: state\ndata: {payload}\n\n".encode())
                await asyncio.sleep(sse_interval)
        except (ConnectionResetError, asyncio.CancelledError):
            pass
        return response

    app = web.Application()
    app.add_routes(
        [
            web.get("/api/health", health),
            web.get("/api/state", market_state),
            web.get("/api/events", events),
        ]
    )
    return app
