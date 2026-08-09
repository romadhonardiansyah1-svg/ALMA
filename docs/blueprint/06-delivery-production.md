# 06 — Delivery, Verification, and Production Plan

## Delivery principle

Build one vertical slice at a time. Live trading is the last deployment state, not the first test.

## Milestones

### M0 — Blueprint and repository

Deliverables:

- these six approved documents;
- Git repository;
- Python 3.12 `uv` project;
- pinned dependencies and basic CI command.

Exit: document links/checks pass; unresolved decisions are explicit.

### M1 — Deterministic core

Build:

- contracts and validation;
- venue modes;
- desired-state reconciler;
- technical guards;
- SQLite WAL ledger;
- idempotency and audit events.

Verification:

- duplicate request self-check;
- mode transition tests;
- partial/pending reconciliation tests;
- crash/restart state recovery smoke test.

### M2 — Binance public read-only

Build:

- Nautilus native Binance adapter configuration;
- public streams and sequence validation;
- incremental bars/features;
- Parquet recording;
- read-only dashboard.

Exit: 24-hour stream with automatic reconnect and no unresolved sequence gap.

### M3 — Strategy + AI shadow

Build:

- two ALMA setup detectors;
- compact snapshot;
- AI Decision Contract;
- 9Router client, fallback, telemetry;
- calibration buckets;
- no order submission.

Exit: replay determinism, schema conformance, bounded token/latency, and shadow decisions visible.

### M4 — Binance Testnet

Build/test:

- testnet credential integration;
- submit/cancel/replace;
- partial fill and reconnect;
- venue mode controls;
- emergency stop.

Exit: failure injection suite passes and no duplicate exposure.

### M5 — MT5 under Wine on same Linux VPS

Build:

- dedicated Wine prefix;
- MT5 installed and demo account logged in locally;
- `AlmaBridge.mq5` compile/run;
- localhost authenticated bridge;
- symbol/account/order synchronization;
- systemd/watchdog startup.

Required soak tests:

- 7 days receiving ticks and heartbeat;
- terminal/Wine crash auto-recovery;
- reboot recovery;
- order lifecycle on demo;
- broker symbol metadata correctness;
- no unexplained disconnect/state divergence.

**Fallback decision:** if reliability gates fail repeatedly, move MT5+EA to Windows VPS. Do not rewrite core.

### M6 — News and combined dashboard

Build:

- calendar ingestion and revisions;
- price-reaction hooks;
- portfolio aggregation;
- authenticated controls and audit.

Exit: simulated news/revision/deduplication tests pass.

### M7 — Production hardening

- systemd units and `LimitNOFILE=65536`;
- private dashboard access;
- secret file permissions;
- backups and restore drill;
- disk/log retention;
- clock drift alert;
- venue/AI outage drills;
- runbook and operator checklist.

### M8 — Canary live

Requires explicit operator approval. Start one venue/symbol with minimum practical exposure. Scale only from measured evidence.

## Test strategy

### Unit/self-check

- contract parser and unknown-field rejection;
- Decimal/precision normalization;
- mode gate;
- reconciliation arithmetic;
- idempotency;
- snapshot freshness;
- fallback classifier;
- calibration update.

### Integration

- mock venue → order lifecycle;
- 9Router response/failure variants;
- SQLite restart and recovery;
- SSE dashboard state;
- MT5 bridge sequence/duplicate handling.

### Replay

- deterministic event ordering;
- realistic bid/ask and fee;
- configurable latency/slippage;
- partial fills and missed entries;
- funding and disconnect events.

### Failure injection

- network drop;
- stale state;
- duplicate event;
- out-of-order event;
- AI timeout/429/5xx/invalid JSON;
- process kill during order transition;
- database busy/disk warning;
- MT5 terminal restart.

## Production acceptance checklist

- [ ] NTP synchronized and drift monitored.
- [ ] Binance/MT5 positions reconcile after restart.
- [ ] No duplicate order in retry/crash tests.
- [ ] Venue modes enforced server-side.
- [ ] AI fallback bounded and auditable.
- [ ] All-model failure creates no new target.
- [ ] Active venue protection survives core outage.
- [ ] Dashboard private, authenticated, and audited.
- [ ] Backup restore tested.
- [ ] Replay includes costs and execution realism.
- [ ] Shadow/demo evidence reviewed.
- [ ] Emergency stop drill passed.
- [ ] Operator explicitly approves canary live.

## Operations

### Services

```text
alma.service       main core, dashboard, Binance
9router.service    local model router
mt5-wine.service   MT5 terminal under Wine (profile A only)
```

Use systemd restart/backoff and health checks. Do not add an orchestrator.

### Backups

- SQLite online backup daily, retain rolling copies.
- Parquet partition checksum and retention policy.
- Configuration and prompt versions in Git.
- Secrets excluded from Git and backup exports unless encrypted separately.

### Alerts

Minimum alerts:

- process/heartbeat down;
- feed/account state stale;
- reconciliation mismatch;
- repeated order rejection;
- fallback chain exhausted;
- disk low or database error;
- MT5 algo trading disabled;
- drawdown/portfolio conditions chosen by operator.

## Coding workflow

No GUI editor is required. Implementation uses:

- file patch/write tools;
- Git for version control;
- `uv` for environment/lock;
- `pytest` for runnable checks;
- `ruff` for lint/format;
- Python/MQL5 compilers and smoke tests;
- terminal logs/debugging.

A GUI editor is optional for human convenience, not a build dependency.

## Immediate next work after blueprint approval

1. Initialize `/root/alma` as Git repo only when requested.
2. Add `pyproject.toml` and lock Python 3.12 dependencies.
3. Implement M1 deterministic core first.
4. Run unit checks and lint.
5. Connect Binance public read-only stream.
6. Postpone Wine/MT5 installation until M1–M3 are stable; it is not needed to prove core correctness.

## Known decisions still requiring operator input

- First Binance symbols (`BTCUSDT`, `ETHUSDT`, or others).
- Broker and exact MT5 gold symbol.
- Calendar/news provider.
- Dashboard access method (localhost tunnel or private VPN).
- Canary live approval and exposure policy after test evidence.
