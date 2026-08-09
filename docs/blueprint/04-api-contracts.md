# 04 — API and Contract Specification

## Conventions

- JSON UTF-8, snake_case fields, UTC RFC3339 timestamps.
- Every mutation includes `request_id`, `state_id`, and `timestamp`.
- Decimal market values travel as strings at external boundaries to avoid float drift.
- Unknown fields are rejected in money-moving contracts.
- Secrets never appear in request bodies, logs, or model prompts.

## AI Decision Contract v1

```json
{
  "policy_version": "alma-v1",
  "state_id": "01J...",
  "decision_id": "01J...",
  "created_at": "2026-07-31T05:00:00Z",
  "venue": "MT5",
  "symbol": "XAUUSDC",
  "action": "INCREASE_LONG",
  "target": {
    "side": "LONG",
    "volume": "0.25"
  },
  "entry": {
    "mode": "ADAPTIVE",
    "preferred_low": "3284.80",
    "preferred_high": "3286.20",
    "max_acceptable_price": "3287.40",
    "ttl_seconds": 45,
    "on_missed": "WAIT_RETEST",
    "on_partial_fill": "REPRICE_REMAINDER"
  },
  "invalidation_price": "3279.60",
  "targets": [
    {"price": "3292.00", "close_fraction": "0.40"},
    {"price": "3298.50", "close_fraction": "0.60"}
  ],
  "review_triggers": ["FLOW_REVERSAL", "THESIS_INVALID", "NEWS_UPDATE"],
  "evidence": ["sweep_reclaim", "flow_reversal"],
  "uncertainty": "0.31"
}
```

### Enums

- `action`: `NO_CHANGE`, `OPEN_LONG`, `OPEN_SHORT`, `INCREASE_LONG`, `INCREASE_SHORT`, `REDUCE`, `CLOSE`, `REVERSE`.
- `entry.mode`: `PASSIVE`, `AGGRESSIVE_LIMIT`, `STOP_ENTRY`, `MARKET_PROTECTED`, `ADAPTIVE`, `WAIT_RETEST`.
- `on_missed`: `ABORT`, `WAIT_RETEST`, `REQUEST_REVIEW`.
- `on_partial_fill`: `KEEP_REMAINDER`, `REPRICE_REMAINDER`, `CANCEL_REMAINDER`.

### Validation rules

- `state_id` must equal the snapshot used by the call.
- Contract must not be expired.
- Venue must be `TRADE` to increase exposure; `MANAGE_ONLY` permits reductions/management only.
- Target fractions sum ≤ 1.
- Price/volume conform to current instrument metadata.
- Fresh venue truth is read before reconciliation/submission.
- Decision does not directly authorize an order; it creates an execution intent.

## Compact snapshot sent to AI

```json
{
  "state_id": "01J...",
  "observed_at": "2026-07-31T04:59:58.120Z",
  "market_age_ms": 72,
  "venue_mode": "TRADE",
  "instrument": {
    "venue": "BINANCE",
    "symbol": "BTCUSDT",
    "bid": "118310.0",
    "ask": "118311.0",
    "tick_size": "0.1",
    "volume_step": "0.001"
  },
  "regime": {"h1": "BULL_HIGH_VOL", "confidence": "0.68"},
  "structure": {"m15": "ABOVE_RANGE", "m5_setup": "VACUUM_CONTINUATION"},
  "trigger": {"m1": "RETEST", "flow_imbalance": "0.31"},
  "account": {"equity": "1000.00", "free_margin": "710.00"},
  "positions": [],
  "pending_orders": [],
  "news": {"state": "NONE", "next_high_impact_seconds": 2800},
  "memory": [{"setup": "VACUUM_CONTINUATION", "sample": 42, "net_expectancy": "0.0018"}]
}
```

## MT5 bridge protocol

Prefer localhost TCP/WebSocket on single-host Linux. If remote, require private VPN + HMAC.

### EA → Core messages

- `hello`: terminal/account/symbol capability, bridge version.
- `tick`: bid, ask, timestamp, flags.
- `account`: balance, equity, margin, free margin, leverage, currency.
- `symbol_spec`: digits, point, tick size/value, contract size, volume min/max/step, stops level.
- `position_snapshot`: complete positions.
- `order_snapshot`: complete pending orders.
- `trade_event`: accepted, partial, filled, canceled, rejected.
- `heartbeat`: terminal/algo-trading/connection status.

Example:

```json
{
  "type": "trade_event",
  "seq": 8127,
  "timestamp": "2026-07-31T05:00:01.120Z",
  "request_id": "01J...",
  "status": "PARTIAL",
  "ticket": "938121",
  "filled_volume": "0.10",
  "remaining_volume": "0.15"
}
```

### Core → EA commands

- `place_order`
- `modify_order`
- `cancel_order`
- `close_position`
- `sync_request`
- `set_protection`

EA rejects duplicate `request_id`. Core does not mark success until matching venue event arrives.

## Dashboard API

### Read endpoints

- `GET /api/health`
- `GET /api/state`
- `GET /api/portfolio`
- `GET /api/positions`
- `GET /api/orders`
- `GET /api/decisions?limit=...`
- `GET /api/events` — SSE stream.

### Mutation endpoints

- `POST /api/venues/{venue}/mode`
- `POST /api/orders/{id}/cancel`
- `POST /api/positions/{id}/close`
- `POST /api/emergency-stop`

Mutation example:

```json
{
  "request_id": "01J...",
  "mode": "MANAGE_ONLY",
  "open_position_policy": "MANAGE",
  "confirmation": "venue:binance:manage_only"
}
```

## Error model

```json
{
  "error": {
    "code": "STATE_STALE",
    "message": "Account state exceeds allowed age",
    "retryable": true,
    "request_id": "01J..."
  }
}
```

Core codes: `INVALID_SCHEMA`, `STATE_STALE`, `MODE_BLOCKED`, `DUPLICATE_REQUEST`, `INSTRUMENT_INVALID`, `MARGIN_UNKNOWN`, `PRICE_OUTSIDE_ENVELOPE`, `VENUE_UNAVAILABLE`, `AI_UNAVAILABLE`.

## Versioning

- Contract version lives in `policy_version`/bridge version.
- Breaking change increments major (`alma-v2`).
- Ledger stores raw contract, validation result, model ID, prompt hash, and code version.
