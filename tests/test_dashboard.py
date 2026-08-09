import asyncio
import json
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from aiohttp.test_utils import TestClient, TestServer

from alma.dashboard import create_dashboard_app, dashboard_snapshot
from alma.ledger import append_calendar_event, open_ledger, record_shadow_run
from alma.mutation_gate import MutationGate, VenueTruth

NOW = datetime(2026, 7, 31, 15, tzinfo=UTC)
SECRET = "d" * 48
AUTH = {"Authorization": f"Bearer {SECRET}"}


def calendar(revision: int = 0, actual: str | None = None) -> dict[str, object]:
    return {
        "event_id": "us-cpi-20260731",
        "revision": revision,
        "release_at": "2026-07-31T15:01:00+00:00",
        "currency": "USD",
        "impact": "HIGH",
        "title": "US CPI",
        "actual": actual,
        "forecast": "2.6",
        "prior": "2.7",
        "source": "fixture",
        "received_at": f"2026-07-31T15:0{revision + 1}:00+00:00",
    }


def test_calendar_revision_is_idempotent_append_only_and_monotonic(tmp_path) -> None:
    connection = open_ledger(tmp_path / "alma.db")
    try:
        assert append_calendar_event(connection, **calendar())
        assert not append_calendar_event(connection, **calendar())
        with pytest.raises(ValueError, match="payload conflict"):
            append_calendar_event(connection, **calendar(actual="2.8"))
        assert append_calendar_event(connection, **calendar(1, "2.8"))
        assert not append_calendar_event(connection, **calendar(0))
        with pytest.raises(Exception, match="append-only"):
            connection.execute("DELETE FROM calendar_events")
    finally:
        connection.close()


def test_dashboard_auth_sse_portfolio_calendar_and_gated_control(tmp_path) -> None:
    async def run() -> None:
        connection = open_ledger(tmp_path / "alma.db")
        connection.executemany(
            "INSERT INTO venue_modes VALUES (?, ?)",
            [("BINANCE", "TRADE"), ("MT5", "MONITOR")],
        )
        connection.commit()
        gate = MutationGate(connection, max_age=timedelta(seconds=5), clock=lambda: NOW)
        gate.sync_venue(
            venue="BINANCE",
            symbol="BTCUSDT-PERP",
            truth=VenueTruth("state-1", NOW, Decimal(0), Decimal(0)),
        )
        app = create_dashboard_app(
            connection,
            secret=SECRET,
            gate=gate,
            truth_provider=lambda: {
                "BINANCE": {
                    "state_id": "state-1",
                    "observed_at": NOW.isoformat(),
                    "position": "0",
                },
                "MT5": {
                    "state_id": "state-mt5",
                    "observed_at": NOW.isoformat(),
                    "position": "1",
                },
            },
            clock=lambda: NOW,
            sse_interval=0.01,
        )
        client = TestClient(TestServer(app))
        await client.start_server()
        try:
            assert (await client.get("/api/state")).status == 401
            index = await client.get("/")
            assert index.status == 200
            assert SECRET not in await index.text()

            first = await client.post("/api/calendar", headers=AUTH, json=calendar())
            assert first.status == 201 and await first.json() == {"stored": True}
            duplicate = await client.post(
                "/api/calendar", headers=AUTH, json=calendar()
            )
            assert duplicate.status == 200 and await duplicate.json() == {
                "stored": False
            }
            revision = await client.post(
                "/api/calendar", headers=AUTH, json=calendar(1, "2.8")
            )
            assert revision.status == 201

            state = await client.get("/api/state", headers=AUTH)
            payload = await state.json()
            assert payload["portfolio"]["cross_venue_atomic"] is False
            assert payload["portfolio"]["aggregate_money"] is None
            assert set(payload["portfolio"]["venues"]) == {"BINANCE", "MT5"}
            assert payload["calendar"] == [
                {
                    **calendar(1, "2.8"),
                    "phase": "PRE_RELEASE",
                }
            ]

            started = time.monotonic()
            events = await client.get("/api/events", headers=AUTH)
            assert events.headers["Content-Type"].startswith("text/event-stream")
            assert await events.content.readline() == b"event: state\n"
            line = await events.content.readline()
            assert time.monotonic() - started < 2
            assert (
                json.loads(line.removeprefix(b"data: "))["portfolio"][
                    "cross_venue_atomic"
                ]
                is False
            )
            events.close()

            command = {
                "request_id": "mode-1",
                "state_id": "state-1",
                "venue": "BINANCE",
                "symbol": "BTCUSDT-PERP",
                "mode": "MONITOR",
                "policy": None,
                "confirmation": "wrong",
            }
            rejected = await client.post(
                "/api/controls/mode", headers=AUTH, json=command
            )
            assert rejected.status == 409
            assert connection.execute(
                "SELECT mode FROM venue_modes WHERE venue_id='BINANCE'"
            ).fetchone() == ("TRADE",)

            command["confirmation"] = "TRANSITION BINANCE TO MONITOR"
            accepted = await client.post(
                "/api/controls/mode", headers=AUTH, json=command
            )
            assert accepted.status == 200
            assert await accepted.json() == {
                "active_mode": "MONITOR",
                "final_mode": "MONITOR",
                "ensure_protection": False,
            }
            assert connection.execute(
                "SELECT actor, action, request_id FROM audit_events"
            ).fetchone() == ("dashboard", "VENUE_MODE_TRANSITION", "mode-1")
        finally:
            await client.close()
            connection.close()

    asyncio.run(run())


def test_dashboard_control_fails_closed_without_live_mutation_gate(tmp_path) -> None:
    async def run() -> None:
        connection = open_ledger(tmp_path / "alma.db")
        app = create_dashboard_app(connection, secret=SECRET, clock=lambda: NOW)
        client = TestClient(TestServer(app))
        await client.start_server()
        try:
            response = await client.post("/api/controls/mode", headers=AUTH, json={})
            assert response.status == 503
            assert await response.json() == {"error": "CONTROL_UNAVAILABLE"}
        finally:
            await client.close()
            connection.close()

    asyncio.run(run())


def test_dashboard_health_includes_ledger_alerts(tmp_path, monkeypatch) -> None:
    async def run() -> None:
        connection = open_ledger(tmp_path / "alma.db")
        monkeypatch.setattr(
            "alma.dashboard.ledger_health_alerts",
            lambda _: ["FALLBACK_EXHAUSTED_REPEATED"],
        )
        app = create_dashboard_app(connection, secret=SECRET, clock=lambda: NOW)
        client = TestClient(TestServer(app))
        await client.start_server()
        try:
            response = await client.get("/healthz")
            assert response.status == 503
            assert await response.json() == {"ok": False}
        finally:
            await client.close()
            connection.close()

    asyncio.run(run())


def test_dashboard_rejects_secret_like_truth_keys(tmp_path) -> None:
    connection = open_ledger(tmp_path / "alma.db")
    try:
        with pytest.raises(ValueError, match="secret-like"):
            dashboard_snapshot(
                connection,
                now=NOW,
                truth_provider=lambda: {
                    "BINANCE": {"nested": {"api_key": "must-not-render"}}
                },
            )
    finally:
        connection.close()


def test_dashboard_renders_ai_usage_counts_without_weakening_secret_filter(
    tmp_path,
) -> None:
    connection = open_ledger(tmp_path / "alma.db")
    try:
        record_shadow_run(
            connection,
            request_id="shadow-usage",
            state_id="state-1",
            decision=None,
            status="NO_DECISION",
            validation_error=None,
            requested_model="model-a",
            actual_model="model-a",
            prompt_tokens=12,
            completion_tokens=3,
            latency_ms=1,
            attempt_count=1,
            failure_classes="",
            fallback_used=False,
            hooks="M1_CLOSE",
            venue="BINANCE",
            symbol="BTCUSDT-PERP",
            setup="",
            regime="0",
            session="ASIA",
            news_state="",
            hypothetical_delta=None,
            created_at=NOW.isoformat(),
        )
        payload = dashboard_snapshot(
            connection,
            now=NOW,
            runtime_provider=lambda: {
                "execution_enabled": True,
                "venue_modes": {"BINANCE": "TRADE", "MT5": "TRADE"},
            },
        )
        assert payload["execution"]["execution_enabled"] is True
        assert payload["execution"]["healthy"] is False
        assert payload["execution"]["venue_modes"]["MT5"] == "TRADE"
        assert payload["shadow"][0]["prompt_usage"] == 12
        assert payload["shadow"][0]["completion_usage"] == 3
        assert "prompt_tokens" not in payload["shadow"][0]
    finally:
        connection.close()
