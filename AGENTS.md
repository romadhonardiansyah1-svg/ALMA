# ALMA Autonomous Development Rules

## Mission
Build ALMA Sovereign Trader from the six documents in `docs/blueprint/` to a production-ready, testable system. Work autonomously in small verified increments.

## Source of truth
Read these before changing behavior:
1. `docs/blueprint/01-prd.md`
2. `docs/blueprint/02-system-requirements.md`
3. `docs/blueprint/03-architecture.md`
4. `docs/blueprint/04-api-contracts.md`
5. `docs/blueprint/05-data-strategy.md`
6. `docs/blueprint/06-delivery-production.md`
7. `IMPLEMENTATION_PLAN.md` for executable phase/work-package order.
8. `AUTONOMY.md` for current durable progress.

## Working mode
- Ponytail full: understand the flow, reuse existing code/platform features, smallest correct diff.
- Continue through adjacent unblocked slices when requested, validating after each slice so later work starts from green state.
- Inspect existing files before writing. Never duplicate helpers or abstractions.
- Use Python 3.12, `uv`, NautilusTrader 1.230.0, stdlib SQLite, aiohttp, pytest, and ruff as specified.
- Do not add dependencies unless a current blueprint requirement cannot be met with stdlib, Nautilus, aiohttp, or already-pinned packages.
- Every non-trivial money/order/state branch leaves one runnable test.
- Run targeted tests and ruff after each change. Fix failures before recording completion.
- Update `AUTONOMY.md` atomically after every run with completed work, validation, blockers, and the next smallest task.

## Autonomous boundaries
Proceed without asking for routine reads, edits, tests, package sync, Git initialization, local services, public market-data access, replay, shadow mode, Binance testnet work, MT5 demo work, and dashboard development.

Stop and record a blocker instead of guessing when work requires:
- credentials or account login not already present locally;
- a paid service or purchase;
- destructive Git/filesystem/database action;
- public internet exposure or security weakening;
- production deployment, real-money trading, withdrawal permission, or canary-live approval;
- an unresolved product decision that changes money behavior and has no safe blueprint default.

Never put secrets in source, logs, prompts, tests, documentation, or `AUTONOMY.md`. Never send project code or user data to third parties except APIs explicitly required by the blueprints.

## Provider behavior
Hermes and the autonomous worker start with `custom:9router` model `kr/gpt-5.6-sol-thinking-agentic`. ALMA's separate Decision Contract runtime uses `kr/gpt-5.6-luna-thinking-agentic`. On rate limit, timeout, connection failure, malformed response, or provider `5xx`, Hermes falls back to OpenAI Codex `gpt-5.6-sol`. A session whose primary is Codex can fall back to 9Router. Do not modify provider/model/fallback configuration without explicit operator direction.

## Interactive-run behavior
- Do not create or schedule cron jobs. Development continues only in interactive sessions.
- Do not wait for user input; if blocked, record it and work on the next independent unblocked task.
- Use up to three subagents only for independent read-only research, test-case design, or code review. The main worker alone edits the shared repository and verifies all claims.
- Aim for a coherent verified batch per request and leave `AUTONOMY.md` accurate.
- Do not claim success without reading back artifacts and running validation.
- Do not commit or push unless the user explicitly asks.
