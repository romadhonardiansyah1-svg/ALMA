# 05 — Data, Strategy, and AI Blueprint

## Trading horizon

ALMA v1 is a non-HFT scalper:

| Layer | Purpose |
|---|---|
| H1 | regime and dominant direction |
| M15 | structure, session range, liquidity map |
| M5 | candidate setup |
| M1 | trigger/acceptance/reclaim |
| tick/order flow | execution timing and evidence changes |

Typical holding time: seconds to 45 minutes; longer only when the current thesis explicitly remains valid.

## Data ingestion

### Binance Futures

- best bid/ask and sequenced depth deltas;
- aggregate trades/taker flow;
- mark price, funding, basis;
- open interest and liquidation events;
- account, positions, orders, fills;
- optional spot-perpetual lead/lag.

Order book starts from one snapshot then applies deltas. Gap → invalidate → resync.

### MT5 gold

- broker bid/ask ticks, spread, tick velocity/volume;
- account, positions, orders, fills;
- actual symbol specification;
- Asia/London/New York session ranges;
- DXY/yield proxies and macro events.

MT5 depth is broker-local and must not be treated as global gold order flow.

### Storage

- RAM ring buffers: recent tick/depth/features.
- Parquet: replayable market events partitioned by date/venue/symbol.
- SQLite: modes, decisions, order lifecycle, fills, calibration, memory, audit.

## Initial strategies

### A. Liquidity Sweep Reversal

Candidate conditions:

1. Known liquidity level crossed by a minimum volatility-normalized distance.
2. Price fails to sustain acceptance beyond the level.
3. Reclaim/re-entry occurs.
4. Flow/tick velocity weakens or reverses.
5. Net expectancy after cost remains positive.

Entry: reclaim or first valid retest.  
Invalidation: renewed sustained acceptance beyond sweep.  
Targets: VWAP/range center/opposite liquidity or evidence-decay exit.

### B. Liquidity Vacuum Continuation

Candidate conditions:

1. Realized volatility compression.
2. Liquidity thins toward one direction.
3. Flow strengthens and breakout receives acceptance.
4. Crowding/funding/OI context does not invalidate continuation.
5. Net expectancy after cost remains positive.

Entry: acceptance or first retest through adaptive envelope.  
Invalidation: sustained return into prior range.  
Targets: next liquidity clusters; partial exits allowed.

## Quantifying ICT/SMC vocabulary

No visual label is trusted until numerical:

- **Sweep:** cross confirmed swing by threshold relative to ATR/realized volatility, then close/re-enter.
- **BOS/CHOCH:** close through confirmed swing with displacement threshold.
- **FVG:** three-bar gap with minimum normalized width.
- **Displacement:** return/range/velocity percentile condition.
- **Premium/discount:** normalized position inside active dealing range.
- **Acceptance:** persistence/time/volume beyond a level—not one wick.

Exact thresholds begin as configuration for replay, not universal truths.

## Opportunity score

```text
score = calibrated_expected_log_growth
      - execution_cost
      - uncertainty_penalty
      - correlation/crowding_penalty
```

AI may propose any exposure. The system does not enforce a fixed 1% risk rule. It does enforce technical solvency and venue validity: known margin, legal volume, no immediate invalid liquidation state, synchronized account state.

## News and fundamentals

### XAU priority

- Fed/FOMC, CPI/PCE, NFP/labor/wages;
- DXY and US nominal/real yields;
- geopolitical/safe-haven shocks;
- actual vs forecast vs prior revision;
- measured price/spread response.

### Crypto priority

- funding, basis, OI, liquidation;
- spot/ETF flow where reliable;
- regulation, exploit, exchange outage;
- macro liquidity and BTC market leadership.

### News processing

```text
calendar loaded daily
→ pre-release hook
→ actual/forecast/prior/revision update
→ measure price/spread/DXY/yield response
→ AI review only if material
```

No trade is opened from headline sentiment alone. Deduplicate by source/event/time.

## Hook policy

Hooks include:

- M1/M5 close;
- sweep/reclaim/acceptance;
- volatility or regime transition;
- material flow/OI/funding/liquidation change;
- price enters/exits active envelope;
- fill/reject/partial/cancel;
- position thesis invalidation;
- account/margin/manual-position change;
- scheduled or breaking news event.

Coalesce bursts into one decision request. Unchanged state reuses active policy without new token spend.

## AI fallback policy

```text
primary pinned model
→ one retry for transient failure
→ validated fallback model
→ validated provider/model fallback
→ no new decision
```

Triggers: `429`, quota exhausted, connection failure, timeout, provider `5xx`. Invalid JSON gets one repair attempt. Semantic disagreement is not a fallback trigger.

9Router auto-fallback may operate underneath, but ALMA records the requested/actual model when available and owns application-level timeout, retries, cooldown, and audit behavior.

## Memory model

Three stages:

1. **Episode:** one observed sequence; immutable.
2. **Pattern:** repeated behavior with sample and uncertainty.
3. **Skill/policy:** replay + shadow validated behavior.

Partition performance by venue, symbol, strategy, regime, session, and news state. Apply recency decay. AI receives only a few relevant summaries, never the entire trade history.

## Calibration

Initial implementation uses SQLite buckets:

```text
(model, venue, symbol, setup, regime, confidence_bucket)
→ count, wins, net expectancy, calibration error, recency weight
```

This corrects self-reported confidence without introducing another ML stack. Upgrade only when measured sample size justifies it.

## Research gates

A challenger must:

- use purged walk-forward splits;
- include fee, spread, slippage, latency, funding, partial fill;
- beat baseline after costs;
- remain stable across neighboring parameters;
- survive shadow mode;
- not worsen drawdown beyond approved tolerance.

No strategy is described as profitable before these tests pass on actual venue/broker data.
