# ALMA operations runbook

## Safety boundary

- Live/canary trading is disabled until explicit operator approval.
- Dashboard and MT5 bridge bind only to `127.0.0.1`; use an SSH tunnel or an approved private VPN.
- Never expose ports 8080/8765 publicly. Never copy secret values into commands, logs, tickets, or Git.
- A dashboard mode change is valid only when bearer auth, exact confirmation text, fresh venue truth, `MutationGate`, and append-only audit all succeed.
- Standalone dashboard operation is intentionally read-only; controls return `CONTROL_UNAVAILABLE` without an injected live `MutationGate`.

Dashboard access uses SSH local forwarding; keep the dashboard bound to loopback:

```bash
ssh -N -L 8080:127.0.0.1:8080 <vps-user>@<vps-host>
```

Then open `http://127.0.0.1:8080` locally and enter the dashboard bearer token. Do not forward 9Router or the MT5 bridge.

## Service checks

```bash
systemctl status alma-9router alma-dashboard alma-mt5-bridge alma-mt5-wine
systemctl status alma-health.timer alma-backup.timer
journalctl -u alma-health.service -n 100 --no-pager
uv run python -m alma.operations health --database var/alma.db --data-root var --router-url http://127.0.0.1:20128/api/health --runtime-status var/runtime-status.json --mt5-terminal-id "$ALMA_MT5_TERMINAL_ID"
```

Do not repeatedly restart a failing trading process. Set the affected venue to `OFF` or `MONITOR`, preserve logs, and reconcile venue truth first.

## Venue/feed outage or stale truth

1. Freeze new exposure: set the venue to `MONITOR`, or `MANAGE_ONLY` only when existing exposure must remain protected.
2. Confirm broker/venue-resident protection directly at the venue.
3. Restore connectivity and obtain a full fresh snapshot; a command acknowledgement is not venue truth.
4. Reconcile positions, pending orders, fills, protection, account identity, and sequence continuity.
5. Return to `TRADE` only after divergence is zero and the audit entry is present.

## MT5 account login and switch

Account mode, position mode, login, server, and exact broker symbol come from `/root/.config/alma/mt5-account.conf` (mode `0600`). `ALMA_MT5_POSITION_MODE=AUTO` accepts broker truth `HEDGING` or `NETTING`; an explicit value pins one mode and rejects the other. Enter the selected account's trading password only through the hidden SSH prompt:

```bash
cd /root/alma
uv run python -m alma.mt5_demo_login
```

The helper name is retained for compatibility but accepts either `DEMO` or `REAL` from the config. It uses an owner-only tmpfs bootstrap file, keeps the password out of argv/output/repository, and removes the file after startup.

To switch accounts: `CLOSE_AND_DISABLE`, verify zero ALMA exposure, stop MT5 and the bridge, replace all five values together, log in through the hidden prompt, then restart and reconcile:

```ini
ALMA_MT5_ACCOUNT_MODE=REAL
ALMA_MT5_POSITION_MODE=AUTO
ALMA_MT5_LOGIN=<live-login>
ALMA_MT5_SERVER=<live-server>
ALMA_MT5_SYMBOL=<exact-live-symbol>
```

The bridge shares this authenticated config with the EA, hides stale truth, rejects old pending commands, and requires the new account or position-mode snapshot to restart at sequence 1. Netting is deliberately limited to one active ALMA lifecycle per symbol; a second root, pending entry, foreign magic, or manual position/order fails closed before mutation. Switching the account does not itself change the venue from `OFF`/`MONITOR` to `TRADE`.

## Binance account/environment switch

Binance environment, exact instrument, expected native account ID, and live arming come from `/root/.config/alma/binance-account.conf` (mode `0600`). Testnet and live credentials use separate owner-only variables/files; do not search or fall back across environments.

```ini
ALMA_BINANCE_ENVIRONMENT=LIVE
ALMA_BINANCE_INSTRUMENT=<SYMBOL>-PERP.BINANCE
ALMA_BINANCE_ACCOUNT_ID=<exact-native-account-id>
ALMA_LIVE_APPROVED=true
```

Before switching, use `CLOSE_AND_DISABLE`, verify ALMA exposure and owned orders are zero, install the matching profile and credentials together, then restart. Require fresh cache truth to match the configured account ID and instrument. Finally perform a separate audited `MutationGate` transition if `TRADE` is intended. Environment selection and `ALMA_LIVE_APPROVED` only arm capability; neither changes a persisted venue mode. Rollback replaces the profile and credential pair with Testnet values and repeats identity reconciliation before any mode transition.

## AI/router outage

1. Verify fallback telemetry and `NO_DECISION` events.
2. Keep existing venue protection; do not manufacture a target from prose or stale output.
3. Three recent exhausted outcomes trigger `FALLBACK_EXHAUSTED_REPEATED` in the health gate.
4. Do not launch a second 9Router instance for a drill; version 0.5.45 has process coupling beyond `DATA_DIR`, and stopping the second instance can stop the primary listener. Test the unit on a maintenance window after stopping the primary cleanly.
5. Keep `/root/.9router` and its SQLite DB/WAL/SHM owner-only; the service `UMask=0077` preserves this for new files.

## Database error

1. Stop mutation-capable services; do not delete WAL/SHM files.
2. Copy the database directory for forensics.
3. Run `PRAGMA quick_check` via `alma.operations health`.
4. Restore only to a new path, verify it, then reconcile venue truth before swapping paths.

## Backup and restore drill

```bash
uv run python -m alma.operations backup --database var/alma.db --output var/backups --retain 14
uv run python -m alma.operations restore var/backups/<backup>.db var/restore-drill/alma.db
uv run python -m alma.operations health --database var/restore-drill/alma.db --data-root var
```

Backups and restored databases are owner-only. Secrets are outside the database and are not included.

## Parquet integrity and retention

```bash
uv run python -m alma.operations manifest --root var/data --output var/parquet-manifest.json
uv run python -m alma.operations verify-manifest --root var/data var/parquet-manifest.json
uv run python -m alma.operations prune --root var/data --before 2026-01-01T00:00:00+00:00
```

Generate and verify a manifest before pruning. Pruning ignores symlinks and is never run by the backup timer.

## Disk, NTP, rejection, and MT5 alerts

- `DISK_LOW`: free space below 5 GiB; stop nonessential recording, preserve money ledger, expand/clean only known artifacts.
- `NTP_UNSYNCED`: restore host time synchronization; new mutations must remain frozen when timestamps cannot be trusted.
- `DATABASE_ERROR`: stop mutation-capable services and follow the database procedure.
- `RUNTIME_UNHEALTHY`: require a fresh `runtime-status.json` with all configured readiness checks passing.
- `MT5_STATE_MISSING`: verify the configured terminal ID and bridge session; another terminal's row is not a substitute.
- `ORDER_REJECTIONS_REPEATED`: reconcile symbol/account/rules and stop resubmission loops.
- `MT5_STATE_INVALID`: require a complete sequential broker snapshot before any MT5 mutation.
- `ROUTER_UNAVAILABLE`: verify the loopback process and `/api/health`; all-model failure must remain `NO_DECISION`.

Drawdown thresholds remain unset until the operator selects a measured risk policy. Architecture does not invent one.

## Reboot recovery

1. Verify NTP and disk first.
2. Start storage/core dependencies, then bridge/dashboard, then MT5.
3. Keep venues non-trading until complete venue snapshots arrive.
4. Reconcile account identity, balances, positions, orders, fills, and protection.
5. Verify no duplicate request/order IDs and review audit events before enabling `TRADE`.

## Emergency stop

Use the existing durable emergency-stop path. Confirm cancel/protect/flatten at each venue independently; cross-venue actions are not atomic. `CLOSE_AND_DISABLE` remains pending until fresh venue truth proves exposure and pending quantity are both zero.

## Threat review

| Threat | Control |
|---|---|
| Public dashboard/bridge exposure | Loopback-only bind plus loopback middleware; no wildcard service argument |
| Stolen/replayed dashboard request | Owner-only bearer secret, exact request ID, shared idempotency table, fresh state ID, audit |
| CSRF/browser ambient authority | API requires explicit Authorization header; no cookie auth |
| UI bypass of money rules | Dashboard calls `MutationGate`; no SQL fallback; standalone controls disabled |
| Secret leakage | Secret files outside repo, `0600`, no secret rendering or backup export |
| Oversized/malformed input | Strict schemas and bounded aiohttp request bodies |
| Stale/divergent venue state | Shared freshness/state-ID checks fail closed |
| Backup corruption/tampering | Atomic SQLite online backup, `quick_check`, SHA-256 Parquet manifest, restore to new path |
| Symlink/path escape | Secret loader uses `O_NOFOLLOW`; manifest/retention reject or ignore unsafe paths |
| Cross-venue atomicity illusion | Dashboard reports venues separately and sets `cross_venue_atomic=false` |

External TLS/VPN termination, alert delivery destination, calendar provider, drawdown thresholds, and live approval remain operator decisions.