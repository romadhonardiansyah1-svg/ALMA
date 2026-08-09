# ALMA Sovereign Trader — Implementation Plan

Status reference: `AUTONOMY.md`. Product contracts: `docs/blueprint/`. This file is the executable backlog; blueprints remain the source of truth for behavior.

## Delivery rules

- Build one verified vertical capability at a time; reuse NautilusTrader and stdlib before adding code or dependencies.
- Keep broker/exchange as source of truth. AI proposes target state; it never bypasses reconciliation, venue mode, metadata, margin, freshness, or idempotency checks.
- Every money/order/state branch has a runnable check. Every phase ends with full `pytest`, Ruff, format, and diff checks.
- No cron jobs. Work continues interactively from `AUTONOMY.md`.
- No live money before replay, shadow, testnet/demo, recovery drills, and explicit operator approval.

## Phase map

| Phase | Outcome | Current state |
|---|---|---|
| F0 | Repository, blueprint, reproducible toolchain | Complete |
| F1 | Deterministic contracts, guards, state, ledger | Complete |
| F2 | Binance public market-data pipeline | Implementation complete; 24h soak running |
| F3 | Features, ALMA strategies, replay | Implementation complete; promotion sample-blocked |
| F4 | AI shadow decision service | Complete for shadow-only scope |
| F5 | Binance execution | Testnet venue-native drills proven; runtime continuous; acceptance monitor optional |
| F6 | MT5/Wine bridge | Configurable DEMO/REAL startup proven; field drill pending; monitor continuous |
| F7 | Dashboard, news, portfolio operations | Local implementation complete; external providers/access pending |
| F8 | Reliability and production hardening | Local hardening complete; production acceptance pending |
| F9 | Canary live and evidence-gated scale-up | Local gates complete; activation blocked on evidence/approval |

## F0 — Repository and blueprint

### Deliverables

- Six approved blueprint documents.
- Python 3.12 `uv` project and locked dependencies.
- Git repository, `src/` package, test and lint commands.
- Durable progress record in `AUTONOMY.md`.

### Exit gate

- Blueprint links and required sections validate.
- Fresh environment installs from `uv.lock`.
- Package smoke test, full tests, Ruff, and diff checks pass.

### Status

Complete. Initial Git commit remains operator-controlled and is not required for code execution.

## F1 — Deterministic core

### F1.1 Contracts and policy

- Strict Decision Contract v1, UTC timestamp, state identity, TTL, enums, decimal boundaries, target allocation.
- Venue modes `OFF`, `MONITOR`, `MANAGE_ONLY`, `TRADE`.
- Explicit open-position transition policy: manage, freeze with venue protection, or close-and-disable.

### F1.2 Reconciliation and technical integrity

- Desired minus actual minus signed pending quantity.
- Venue-truth freshness and state-ID checks.
- Tick/volume increments, range, stops distance, margin, and finite-value guards.
- Idempotency/correlation reservation before mutations.

### F1.3 Durable ledger

Minimum append-only records:

- `decisions`: raw contract, validation result, model, prompt/policy/code hash.
- `intents`: desired state, reconciled delta, venue truth version, mode.
- `order_events`: submitted/accepted/rejected/canceled/expired.
- `fill_events`: partial/final fill, fee, slippage, funding attribution.
- `audit_events`: actor, action, request ID, timestamp, before/after summary.

Use one SQLite WAL database, foreign keys, explicit transactions, consistent backup, and a measured busy timeout. Do not create a generic event framework.

### F1.4 Recovery

- Restore venue modes and idempotency after restart.
- Re-read venue positions/orders before allowing `TRADE`.
- Freeze new exposure on stale, missing, or divergent state.

### Exit gate

- Duplicate, restart, stale-state, pending/partial reconciliation, and mode-transition tests pass.
- Ledger can reconstruct decision → intent → order/fill outcome.
- No mutation can bypass the shared gate.

### Status

Complete. The append-only SQLite ledger reconstructs decision → intent → order/fill/audit outcomes; the shared mutation gate enforces accepted decision state, trusted-clock freshness, venue mode, reconciliation, and atomic idempotency/audit. SQLite `BEGIN IMMEDIATE` covers each read → validate → write lifecycle transition. Restart requires venue resync, and close-and-disable remains durable until fresh venue truth confirms position and pending exposure are both zero.

## F2 — Binance public read-only pipeline

### F2.1 Native client lifecycle

- Credential-free Binance USDⓈ-M `LIVE` data client.
- Explicit `BTCUSDT-PERP.BINANCE` instrument load.
- Bounded connect/start/stop/dispose and reconnect behavior.

### F2.2 Market events

- Trade ticks, quotes/mark price, funding, and liquidation where available.
- Order-book snapshot plus delta sequence validation.
- Gap detection invalidates the book and requests a fresh snapshot.
- Monotonic event timestamp/age tracking.

### F2.3 Incremental state

- Build M1 from events; derive M5/M15/H1 without redownloading history.
- Maintain spread, tick velocity, realized volatility, imbalance, session state, and data age.
- Emit compact immutable snapshots with state ID.

### F2.4 Persistence and observability

- Partitioned Parquet recording with deterministic replay order.
- Read-only health/state/SSE endpoints.
- Metrics: event count, gap/reconnect count, age, processing latency, disk growth.

### Exit gate

- 24-hour BTCUSDT soak with automatic reconnect.
- No unresolved order-book sequence gap.
- p95 local event-to-state ≤ 50 ms under v1 load.
- Replay reproduces bar/state hashes from the recorded stream.

### Current state

Implementation complete; the 24-hour exit gate is running and F2 is not yet closed. The native credential-free node now routes trade, quote, mark/funding, and raw depth events into one deterministic state used by live operation and replay. Raw `U/u/pu` is validated before Nautilus' lossy conversion; gaps invalidate depth and trigger the native snapshot rebuild. Compact immutable state, M1/M5/M15/H1 bars, partitioned Parquet recording, deterministic replay, read-only health/state/SSE, and bounded metrics are implemented. A short live soak processed 707 events with valid depth, replay hash equality, and p95 event-to-state latency of 0.043 ms. The first long run was terminated cleanly by a Hermes gateway restart after about 5.3 hours and is not combined with later evidence. A fresh 86,400-second run started as transient systemd unit `alma-f2-soak-20260731T1532Z.service` at 2026-07-31 15:32:26 UTC under `var/f2-soak-20260731T1532Z/`; completion evidence must pass every exit criterion before this phase becomes `Complete`.

## F3 — Features, ALMA strategies, and replay

### F3.1 Feature pipeline

- H1 volatility/regime context.
- M15 structure, liquidity pools, displacement, and acceptance/rejection.
- M5 setup state.
- M1/tick trigger, spread, velocity, and flow confirmation.
- Feature values are numeric and replay-deterministic.

### F3.2 Two strategies only

1. Liquidity Sweep Reversal.
2. Liquidity Vacuum Continuation.

Each detector returns evidence and invalidation context, not an order. No additional strategy is added before both baselines have measured out-of-sample results.

### F3.3 Replay realism

- Bid/ask, fees, funding, latency, slippage, missed entries, partial fills, disconnects.
- Purged walk-forward and out-of-sample partitions.
- Stable event ordering and reproducible result hash.

### Exit gate

- Deterministic replay from recorded data.
- Strategy benchmark includes all modeled costs.
- No look-ahead leakage.
- Out-of-sample results reported honestly; profitability is not assumed.

### Current state

`Complete` for implementation and research reproducibility; baseline promotion remains blocked by sample size. Closed-bar numeric features now cover H1 return/volatility/regime, M15 liquidity/structure/displacement/acceptance, M5 compression, and M1/tick spread/velocity/flow/top-book confirmation. Exactly two stale-state-rejecting evidence-only detectors exist: Liquidity Sweep Reversal and Liquidity Vacuum Continuation; the continuation target is an explicit measured-range projection so reported edge is prospective rather than an already-observed move. Replay preserves recorded cross-stream order and uses a monotonic event-time watermark. It models executable bid/ask, fees, discrete funding settlements at settlement mark/notional, latency, adverse slippage, entry envelopes, partial fills, missed entries, position overlap, disconnect mark-to-market, and end-of-data mark-to-market. OOS candidates are simulated in one chronological pass across train/purge/test boundaries so open positions cannot reset and count the same market interval twice; only test-fold trades enter reported metrics. An interim read-only replay of 230,000 live-recorded events produced zero candidates and status `INSUFFICIENT_SAMPLE`; this is not evidence of profitability or failure and cannot promote either baseline.

## F4 — AI shadow decision service

### F4.1 Snapshot and hooks

- Compact snapshot with market, account, positions, pending orders, news, and relevant calibrated memory.
- Hooks only on candle/setup/regime/flow/news/fill/account/thesis changes; never every tick.

### F4.2 Transport

- Local OpenAI-compatible 9Router client with hard timeout and token cap.
- Same request/schema/policy across retries and fallbacks.
- One repair for malformed/schema-invalid output.
- Infrastructure failure may retry/fallback; disliked trading direction may not.
- Exhaustion yields no new target.

### F4.3 Calibration and memory

- Decision ledger and outcome attribution by venue/symbol/setup/regime/session/news state.
- Calibration uses adequate samples and decay; raw model confidence never sizes positions directly.
- Memory promotion requires sample threshold, replay evidence, and no regression.

### F4.4 Shadow operation

- AI decisions are validated, reconciled, logged, and visualized but never submitted.
- Track latency, tokens, schema failure, fallback frequency, hypothetical fills/outcomes.

### Exit gate

- Bounded latency/token behavior.
- Schema and semantic validation remain fail-closed.
- Replay/shadow decisions are reproducible for fixed inputs/settings.
- No order path is reachable in shadow mode.

### Current state

`Complete` for shadow-only implementation and deterministic validation; no real-provider quality or strategy profitability claim is made. Compact canonical snapshots reject secret-like keys recursively and enforce a 64 KB payload ceiling. Hook coalescing excludes tick-level invocation. The loopback-only OpenAI-compatible transport enforces timeout and token caps; fallback uses identical request identity, bounded attempts and overall deadline, one schema/decode repair only, fail-closed semantic validation, and no-target exhaustion. Append-only shadow runs record latency, tokens, attempts, failure classes, fallback use, hypothetical reconciliation, and accepted-decision outcomes derived from immutable ledger context for decayed calibration. Decisions carry immutable `SHADOW` provenance, while the shared mutation gate accepts only `EXECUTION`, with a regression proving a shadow decision cannot create an intent. Fixed-input idempotency and request/prompt hashes are covered. Full validation passed 215 tests with Ruff and compile checks clean.

## F5 — Binance Testnet execution

### Deliverables

- Secret loading from permission-restricted local environment; no withdrawal capability.
- Native execution client, account/order/position resync.
- Entry envelope: passive, aggressive-limit, stop, market-protected, wait-retest, abort.
- Submit/cancel/replace, TTL, missed entry, partial fill, protective stop/target.
- Desired-state reconciliation immediately before every mutation.
- Emergency stop and server-side venue-mode enforcement.

### Failure injection

- Timeout after submission, duplicate request, disconnect/reconnect, manual position, partial fill, rejection, stale state, core crash during transition.

### Exit gate

- No duplicate exposure across retry/restart tests.
- Broker truth reconciles after restart.
- Protection remains venue-resident where supported.
- Testnet soak and failure suite pass.

### External input

Binance Futures Testnet credentials stored locally; never entered into prompts or source.

### Current state

`Venue-native drills complete; 24-hour read-only soak running.` Owner-only Testnet credentials authenticated against USDⓈ-M Futures. Evidence proves nonzero balances, BTCUSDT metadata, native LIMIT accept/cancel, a minimal fill, venue-resident reduce-only TP plus STOP algo protection, emergency flatten, and fresh-process reconnect/resync. Every drill ended with zero position, zero regular orders, and zero algo orders. Binance conditional orders are included through signed `openAlgoOrders` truth. Full validation passes 280 tests. Transient unit `alma-f5-soak-20260731T1706Z.service` started at 2026-08-01 00:06:31 WIB; it samples balance/position/regular-order/algo-order truth every 60 seconds without mutations and can pass no earlier than 2026-08-02 00:06:31 WIB.

## F6 — MT5 under Wine, demo validation with configurable account mode

### F6.1 Host

- Dedicated Wine prefix on the existing Linux VPS.
- MT5 and MetaEditor installed; demo account logged in locally.
- Exact broker symbol specification discovered, including `XAUUSD`/`XAUUSDC` variants.

### F6.2 Thin MQL5 bridge

- EA emits hello, tick, heartbeat, account, symbol spec, full order/position snapshots, and trade events.
- Core sends place/modify/cancel/close/sync/protection commands.
- Sequence, timestamp, request ID, duplicate rejection, and loopback authentication.
- Credential remains inside MT5.

### F6.3 Recovery

- Terminal/Wine/core crash and reboot recovery.
- Full resync before enabling new exposure.
- Validate volume, point/tick value, contract size, stops level, and cent-account semantics from live broker metadata.

### Exit gate

- Seven-day demo soak.
- Stable tick/heartbeat and order lifecycle.
- No unexplained state divergence.
- If Wine repeatedly fails the gate, move only MT5+EA to Windows VPS; core remains unchanged.

### External input

Installed MT5 terminal, locally logged-in demo account, broker, and exact symbol.

### Current state

`Startup/restart proven; open-market field drill pending; continuous monitoring active.` The current deployment profile is DEMO on `Exness-MT5Trial6`, symbol `XAUUSD`, with configured `AUTO` resolving to broker `HEDGING`; these are profile values, not source invariants. Runtime has no duration limit. Any finite soak is only an operator-selected acceptance window. Atomic owner-only `FILE_COMMON` IPC removes MT5 WebRequest allowlist dependence while retaining strict account/symbol/mode validation, sequence/replay protection, and the durable SQLite command outbox. A controlled service restart produced a new fresh session automatically with zero positions, orders, and active commands. MetaEditor compiled the active EA with `0 errors, 0 warnings`. Seven-day evidence cannot pass before 2026-08-08 04:48:24 UTC. Minimum fill, venue-resident SL/TP, HEDGING partial close/reversal, foreign-exposure rejection, and owned emergency flatten require broker-open execution; every drill must finish at zero exposure.

## F7 — Dashboard, news, and portfolio operations

### F7.1 Dashboard

- Stdlib/aiohttp HTML + SSE; no frontend framework until measured need.
- Health, venue modes, account, positions, orders, PnL/drawdown, feed age, decision/fallback, latency/token data.
- Authenticated/confirmed/audited mutation controls.
- Localhost/private VPN only; secrets never rendered.

### F7.2 News

- Calendar actual/forecast/prior/revision and deduplication.
- Pre-release/release/reaction hooks.
- XAU: DXY/yields/session/spread/price response.
- Crypto: funding/OI/liquidations/basis/market response.
- No narrative headline trading without reaction evidence.

### F7.3 Portfolio view

- Separate Binance and MT5 modes and truth.
- Aggregate risk/PnL view without pretending cross-venue orders are atomic.

### Exit gate

- p95 dashboard visibility ≤ 2 seconds.
- Auth/audit tests and simulated news revision tests pass.
- Dashboard remains private and mutations remain server-gated.

### External input

Calendar/enrichment provider remains to be selected. Dashboard access uses an SSH local-forward tunnel; the server remains bound to `127.0.0.1` and no public/VPN listener is added.

### Current state

`Local implementation complete; external ingestion pending.` The owner-only loopback dashboard exposes authenticated state and sub-two-second SSE, venue-separated truth/orders/fills, exact-Decimal fee/funding summaries, shadow/fallback telemetry, append-only calendar revisions, and deterministic pre-release/release/reaction phases. Mode changes require bearer authentication, exact confirmation, fresh state identity, the shared `MutationGate`, durable idempotency, and append-only audit. Recursive secret-like keys are rejected before rendering; standalone runtime controls fail closed without an injected live gate. Cross-venue money remains deliberately unaggregated until currency/risk conversion policy exists. Access is fixed to an SSH local-forward tunnel. Live calendar/DXY/yield/OI/liquidation providers and reaction-hook wiring remain external inputs.

## F8 — Production hardening

### Runtime

- `systemd` services for ALMA, 9Router, and MT5/Wine; no cron.
- Restart/backoff, health/readiness, `LimitNOFILE=65536`, graceful shutdown.
- Secrets with least privilege and restrictive permissions.

### Data and recovery

- SQLite online backup/restore drill.
- Parquet checksum, retention, disk alerts, and replay verification.
- Clock drift, stale feed, mismatch, rejection, fallback exhaustion, and process alerts.
- Runbooks for venue outage, AI outage, DB issue, disk pressure, reboot, emergency close.

### Security

- Dashboard private and authenticated.
- Binance key without withdrawal.
- MT5 credential never leaves terminal.
- Threat review for bridge/dashboard/mutation boundaries.

### Acceptance drills

- Kill core during active protected position.
- Kill network, venue feed, AI provider, MT5, and DB write path.
- Restore from backup on a clean directory.
- Reboot full host and reconcile without duplicate order.

### Exit gate

Every production acceptance item in `docs/blueprint/06-delivery-production.md` is evidenced and signed off. Shadow/testnet/demo evidence is reviewed; unresolved severe failures block release.

### Current state

`Integrated fail-closed runtime active; production exit gate remains blocked.` Enabled `alma.service` consumes only the credential-free public Binance stream, evaluates the two baseline detectors, and sends strict shadow requests to loopback 9Router while both venues remain persisted as `MONITOR`; the module imports no execution client or Testnet credential path. A real `alma-v1` decision was accepted and persisted while intent/order/fill counts remained zero. The private dashboard is enabled only on `127.0.0.1:8080`; minute health and verified daily backup timers are active. Health, authentication, dashboard state, owner-only SQLite backup `quick_check`, Parquet manifest verification, and a controlled core-only restart were verified against the live system. Production acceptance is still blocked on completed F2/F5/F6 evidence, the MT5 open-market field drill, external alert delivery, selected risk/drawdown thresholds, and host/network/venue/full-reboot drills. Concurrent 9Router instances remain prohibited.

## F9 — Canary live and scale-up

### Preconditions

- Explicit operator approval.
- Replay out-of-sample, shadow, Binance Testnet, MT5 demo, and recovery gates pass.
- Catastrophic venue protection and emergency stop verified.
- Exposure policy selected by operator from evidence.

### Canary

- One venue, one symbol, minimum practical exposure.
- Daily reconciliation and outcome review.
- Automatic freeze on stale state, divergence, protection failure, or unexplained loss path.
- No expansion by elapsed time alone.

### Scale gate

Scale only after sufficient live sample confirms execution, cost, recovery, and risk assumptions. Profitability is never claimed from architecture or a small sample.

### Current state

`Local gate implementation complete; canary activation prohibited.` A pure fail-closed evaluator requires replay OOS, shadow, Binance Testnet, MT5 demo, recovery, protection, emergency-stop, explicit operator approval, a selected exposure policy, and exactly one non-empty venue/symbol. Automatic freeze is required for stale state, divergence, protection failure, unexplained loss, or malformed flags. Scale requires a configured positive sample minimum plus measured execution, cost, recovery, and risk evidence; elapsed time alone cannot pass. The module has no venue, submit, credential, or live-activation path. Current evidence evaluates blocked on every prerequisite, so no canary order or production mutation was attempted.

## Critical path

```text
F0 complete
  → finish F1 ledger/recovery
  → finish F2 public stream/record/replay
  → F3 features + two strategies
  → F4 AI shadow
  → F5 Binance Testnet
  → F6 MT5 demo
  → F7 dashboard/news
  → F8 hardening/drills
  → explicit approval
  → F9 canary
```

F6 host setup may proceed beside F3/F4 after deterministic core is stable, but its trading path cannot bypass the same contracts and reconciler. Dashboard read-only work may proceed during F2; mutation controls wait for F1 ledger/audit completion.

## Current execution queue

1. Add deterministic BTCUSDT public event subscription/tracer and lifecycle tests.
2. Add book snapshot/delta sequencing and gap invalidation.
3. Add incremental M1 → M5/M15/H1 bars and data-age/state IDs.
4. Record/replay public events to Parquet.
5. Add read-only health/state/SSE view.
6. Run 24-hour public soak.
7. Return to the minimal F1 decision/intent/order/fill/audit ledger before any execution work.

## Operator-controlled gates

Implementation proceeds without routine approval for local code, tests, public read-only data, replay, shadow, Testnet, and MT5 demo. The following never proceed by assumption:

- credentials/account login not already local;
- paid data/service purchase;
- public exposure or security weakening;
- destructive data/Git action;
- production deployment or real-money/canary trading;
- money behavior not specified by blueprint or measured evidence.
