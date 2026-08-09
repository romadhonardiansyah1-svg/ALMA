import asyncio
import hmac
import ipaddress
import json
import sqlite3
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from aiohttp import web

from alma.ledger import append_calendar_event
from alma.mutation_gate import MutationGate, MutationRejected
from alma.operations import ledger_health_alerts, runtime_status_healthy
from alma.shadow_request import safe_json_value
from alma.venue_modes import OpenPositionPolicy, VenueMode

JsonObject = dict[str, Any]
TruthProvider = Callable[[], Mapping[str, object]]
RuntimeProvider = Callable[[], Mapping[str, object]]


def _rows(
    connection: sqlite3.Connection, sql: str, parameters: tuple[object, ...] = ()
) -> list[JsonObject]:
    cursor = connection.execute(sql, parameters)
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def _latest_mt5(
    connection: sqlite3.Connection, now: datetime, terminal_id: str | None = None
) -> JsonObject | None:
    try:
        row = (
            connection.execute(
                "SELECT state_id, observed_at, payload FROM mt5_terminal_state "
                "WHERE terminal_id = ?",
                (terminal_id,),
            ).fetchone()
            if terminal_id is not None
            else connection.execute(
                "SELECT state_id, observed_at, payload FROM mt5_terminal_state "
                "ORDER BY observed_at DESC LIMIT 1"
            ).fetchone()
        )
    except sqlite3.OperationalError:
        return None
    if row is None:
        return None
    payload = json.loads(row[2])
    terminal = payload["terminal"]
    observed_at = datetime.fromisoformat(str(row[1]))
    age = now - observed_at
    return {
        "state_id": row[0],
        "observed_at": row[1],
        "connected": bool(terminal["connected"]),
        "account_mode": terminal["account_mode"],
        "position_mode": terminal["margin_mode"],
        "symbol": payload["symbol"]["name"],
        "positions": len(payload["positions"]),
        "orders": len(payload["orders"]),
        "fresh": timedelta(0) <= age <= timedelta(seconds=10),
    }


def dashboard_snapshot(
    connection: sqlite3.Connection,
    *,
    now: datetime,
    truth_provider: TruthProvider | None = None,
    runtime_provider: RuntimeProvider | None = None,
    mt5_terminal_id: str | None = None,
) -> JsonObject:
    if now.utcoffset() != timedelta(0):
        raise ValueError("dashboard clock must be UTC")
    modes = _rows(
        connection,
        "SELECT venue_id AS venue, mode FROM venue_modes ORDER BY venue_id",
    )
    pending = _rows(
        connection,
        "SELECT venue, symbol, state_id, final_mode, created_at "
        "FROM pending_mode_transitions ORDER BY venue",
    )
    latest_orders = _rows(
        connection,
        "SELECT i.venue, i.symbol, o.order_id, o.status, o.quantity, "
        "o.filled_quantity, o.price, o.created_at, o.recovered "
        "FROM order_events o JOIN intents i ON i.intent_id = o.intent_id "
        "WHERE o.seq = (SELECT max(o2.seq) FROM order_events o2 "
        "WHERE o2.order_id = o.order_id) ORDER BY o.seq DESC LIMIT 100",
    )
    fill_rows = _rows(
        connection,
        "SELECT i.venue, f.fee, f.funding "
        "FROM fill_events f JOIN order_events o ON o.event_id = f.order_event_id "
        "JOIN intents i ON i.intent_id = o.intent_id ORDER BY f.seq",
    )
    fills: dict[str, JsonObject] = {}
    for fill in fill_rows:
        venue = str(fill["venue"])
        summary = fills.setdefault(
            venue,
            {"venue": venue, "count": 0, "fee": Decimal(0), "funding": Decimal(0)},
        )
        summary["count"] += 1
        summary["fee"] += Decimal(str(fill["fee"]))
        summary["funding"] += Decimal(str(fill["funding"]))
    shadow = _rows(
        connection,
        "SELECT status, requested_model, actual_model, prompt_tokens AS prompt_usage, "
        "completion_tokens AS completion_usage, latency_ms, fallback_used, "
        "failure_classes, created_at FROM shadow_runs ORDER BY seq DESC LIMIT 20",
    )
    audits = _rows(
        connection,
        "SELECT actor, action, request_id, created_at, before_summary, after_summary "
        "FROM audit_events ORDER BY seq DESC LIMIT 50",
    )
    calendar = _rows(
        connection,
        "SELECT c.event_id, c.revision, c.release_at, c.currency, c.impact, c.title, "
        "c.actual, c.forecast, c.prior, c.source, c.received_at "
        "FROM calendar_events c WHERE c.revision = "
        "(SELECT max(c2.revision) FROM calendar_events c2 WHERE c2.event_id = c.event_id) "
        "ORDER BY c.release_at, c.event_id LIMIT 100",
    )
    for item in calendar:
        distance = now - datetime.fromisoformat(str(item["release_at"]))
        item["phase"] = (
            "PRE_RELEASE"
            if distance < timedelta(0)
            else "RELEASE"
            if distance <= timedelta(minutes=5)
            else "REACTION"
        )
    truth = dict(truth_provider() if truth_provider else {})
    mt5 = _latest_mt5(connection, now, mt5_terminal_id)
    if mt5 is not None and "MT5" not in truth:
        truth["MT5"] = mt5
    venues = {
        row["venue"]: {
            "mode": row["mode"],
            "truth": truth.get(row["venue"]),
            "orders": [
                order for order in latest_orders if order["venue"] == row["venue"]
            ],
            "fills": fills.get(
                str(row["venue"]),
                {
                    "venue": row["venue"],
                    "count": 0,
                    "fee": Decimal(0),
                    "funding": Decimal(0),
                },
            ),
        }
        for row in modes
    }
    for venue, venue_truth in truth.items():
        venues.setdefault(
            venue,
            {
                "mode": None,
                "truth": venue_truth,
                "orders": [],
                "fills": {
                    "venue": venue,
                    "count": 0,
                    "fee": Decimal(0),
                    "funding": Decimal(0),
                },
            },
        )
    execution = dict(runtime_provider() if runtime_provider else {})
    if runtime_provider is not None:
        execution["healthy"] = runtime_status_healthy(execution, now=now)
    return safe_json_value(
        {
            "generated_at": now.isoformat(),
            "portfolio": {
                "cross_venue_atomic": False,
                "aggregate_money": None,
                "venues": venues,
            },
            "pending_mode_transitions": pending,
            "execution": execution,
            "shadow": shadow,
            "calendar": calendar,
            "audit": audits,
        }
    )


_INDEX = """<!doctype html>
<html lang=en><meta charset=utf-8><meta name=viewport content="width=device-width">
<title>ALMA Operations</title>
<style>body{font:14px system-ui;max-width:1200px;margin:auto;padding:1rem;background:#111;color:#ddd}input,button{font:inherit;padding:.5rem}pre{white-space:pre-wrap;background:#1b1b1b;padding:1rem;border-radius:.4rem}.bad{color:#f77}</style>
<h1>ALMA Operations</h1><p>Private read-only view. Token stays in this tab.</p>
<label>Bearer token <input id=t type=password autocomplete=off></label> <button id=c>Connect</button>
<p id=s></p><pre id=o>No state loaded.</pre>
<script>
let stop;
c.onclick=async()=>{if(stop)stop.abort();stop=new AbortController();s.textContent='connecting';
const r=await fetch('/api/events',{headers:{Authorization:'Bearer '+t.value},signal:stop.signal});
if(!r.ok){s.textContent='HTTP '+r.status;s.className='bad';return} s.textContent='connected';s.className='';
const rd=r.body.getReader(),d=new TextDecoder();let b='';for(;;){const x=await rd.read();if(x.done)break;b+=d.decode(x.value,{stream:true});let p;
while((p=b.indexOf('\n\n'))>=0){const e=b.slice(0,p);b=b.slice(p+2);const line=e.split('\n').find(x=>x.startsWith('data: '));if(line)o.textContent=JSON.stringify(JSON.parse(line.slice(6)),null,2)}}};
</script>"""


def create_dashboard_app(
    connection: sqlite3.Connection,
    *,
    secret: str,
    gate: MutationGate | None = None,
    truth_provider: TruthProvider | None = None,
    runtime_provider: RuntimeProvider | None = None,
    mt5_terminal_id: str | None = None,
    clock: Callable[[], datetime] | None = None,
    sse_interval: float = 1.0,
) -> web.Application:
    if len(secret) < 32:
        raise ValueError("dashboard secret is too short")
    if sse_interval <= 0 or sse_interval > 2:
        raise ValueError("SSE interval must be in (0, 2]")
    now = clock or (lambda: datetime.now(UTC))

    @web.middleware
    async def private(request: web.Request, handler):
        try:
            remote = ipaddress.ip_address(request.remote or "")
        except ValueError as error:
            raise web.HTTPForbidden() from error
        if not remote.is_loopback:
            raise web.HTTPForbidden()
        if request.path in {"/", "/healthz"}:
            return await handler(request)
        supplied = request.headers.get("Authorization", "")
        if not hmac.compare_digest(supplied, f"Bearer {secret}"):
            raise web.HTTPUnauthorized()
        return await handler(request)

    async def index(_: web.Request) -> web.Response:
        return web.Response(text=_INDEX, content_type="text/html")

    async def health(_: web.Request) -> web.Response:
        try:
            result = connection.execute("PRAGMA quick_check").fetchone()
            ok = result == ("ok",)
        except sqlite3.Error:
            ok = False
        if ok:
            ok = not ledger_health_alerts(connection)
        if ok and runtime_provider is not None:
            ok = runtime_status_healthy(runtime_provider(), now=now())
        return web.json_response({"ok": ok}, status=200 if ok else 503)

    async def state(_: web.Request) -> web.Response:
        return web.json_response(
            dashboard_snapshot(
                connection,
                now=now(),
                truth_provider=truth_provider,
                runtime_provider=runtime_provider,
                mt5_terminal_id=mt5_terminal_id,
            )
        )

    async def events(request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-store",
                "X-Accel-Buffering": "no",
            }
        )
        await response.prepare(request)
        try:
            while True:
                payload = dashboard_snapshot(
                    connection,
                    now=now(),
                    truth_provider=truth_provider,
                    runtime_provider=runtime_provider,
                    mt5_terminal_id=mt5_terminal_id,
                )
                body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
                await response.write(f"event: state\ndata: {body}\n\n".encode())
                await asyncio.sleep(sse_interval)
        except (ConnectionResetError, asyncio.CancelledError):
            pass
        return response

    async def transition(request: web.Request) -> web.Response:
        if gate is None:
            return web.json_response({"error": "CONTROL_UNAVAILABLE"}, status=503)
        try:
            body = await request.json()
        except (json.JSONDecodeError, UnicodeDecodeError):
            return web.json_response({"error": "INVALID_JSON"}, status=400)
        expected = {
            "request_id",
            "state_id",
            "venue",
            "symbol",
            "mode",
            "policy",
            "confirmation",
        }
        if not isinstance(body, dict) or set(body) != expected:
            return web.json_response({"error": "INVALID_SCHEMA"}, status=400)
        text_fields = (
            "request_id",
            "state_id",
            "venue",
            "symbol",
            "mode",
            "confirmation",
        )
        if any(
            not isinstance(body[field], str)
            or not body[field]
            or len(body[field]) > 128
            for field in text_fields
        ) or (body["policy"] is not None and not isinstance(body["policy"], str)):
            return web.json_response({"error": "INVALID_SCHEMA"}, status=400)
        confirmation = f"TRANSITION {body['venue']} TO {body['mode']}"
        if body["confirmation"] != confirmation:
            return web.json_response({"error": "CONFIRMATION_REQUIRED"}, status=409)
        try:
            mode = VenueMode(body["mode"])
            policy = (
                None if body["policy"] is None else OpenPositionPolicy(body["policy"])
            )
            plan = gate.transition_mode(
                request_id=body["request_id"],
                state_id=body["state_id"],
                timestamp=now(),
                venue=body["venue"],
                symbol=body["symbol"],
                requested=mode,
                policy=policy,
                actor="dashboard",
            )
        except (KeyError, TypeError, ValueError, MutationRejected) as error:
            reason = (
                str(error) if isinstance(error, MutationRejected) else "INVALID_REQUEST"
            )
            return web.json_response({"error": reason}, status=409)
        return web.json_response(
            {
                "active_mode": plan.active_mode.value,
                "final_mode": plan.final_mode.value,
                "ensure_protection": plan.ensure_protection,
            }
        )

    async def calendar(request: web.Request) -> web.Response:
        try:
            body = await request.json()
            if not isinstance(body, dict):
                raise TypeError("invalid body")
            stored = append_calendar_event(connection, **body)
        except (json.JSONDecodeError, TypeError, ValueError) as error:
            return web.json_response({"error": str(error)}, status=400)
        return web.json_response({"stored": stored}, status=201 if stored else 200)

    app = web.Application(middlewares=[private], client_max_size=64 * 1024)
    app.add_routes(
        [
            web.get("/", index),
            web.get("/healthz", health),
            web.get("/api/state", state),
            web.get("/api/events", events),
            web.post("/api/controls/mode", transition),
            web.post("/api/calendar", calendar),
        ]
    )
    return app
