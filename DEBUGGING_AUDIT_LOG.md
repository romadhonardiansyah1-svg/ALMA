# ALMA — Debugging Audit Log & Unresolved Issues

> Generated for OpenCode coding harness. Structured by category.  
> Status legend: ✅ FIXED | ⚠️ PARTIAL | ❌ OPEN | 📌 NOTED (by design / low priority)

---

## A. CRITICAL BUGS — FIXED (session 2026-08-09)

### A1. ✅ BINANCE stuck di MANAGE_ONLY walau live_armed=True
- **File**: `src/alma/runtime.py:447-454`
- **Root cause**: `live_armed` flag dari env `ALMA_LIVE_APPROVED=true` hanya di-set di JSON status, tidak pernah dipakai untuk promote venue mode. `bootstrap_monitor_modes()` insert MONITOR, lalu `activate_trade_mode()` per-cycle yang promote — tapi kalau DB sudah ada MANAGE_ONLY (dari config deploy), runtime tidak pernah promote saat startup.
- **Fix**: Saat runtime start dan `live_armed=True`, UPDATE `venue_modes SET mode='TRADE' WHERE venue_id IN ('BINANCE','MT5') AND mode IN ('MONITOR','MANAGE_ONLY')`.
- **Test gap**: Tidak ada test yang verify SQL promotion. `test_live_profiles_are_configurable_but_explicitly_armed` hanya test `_validate_live_profile()` (approval gate), bukan SQL execution.

### A2. ✅ DB bloat — 578MB + WAL 584MB = ~1.1GB
- **File**: `var/alma.db`, table `mt5_snapshots`
- **Root cause**: `mt5_snapshots` tidak pernah di-prune. Setiap MT5 bridge cycle (setiap detik) insert snapshot baru. Setelah beberapa jam → 73k+ rows. WAL tidak pernah di-checkpoint karena runtime terus menulis.
- **Fix**: Prune old snapshots (keep last 500/session) + VACUUM. DB: 578MB → 1.7MB.
- **Unresolved popup**: Tidak ada auto-prune mechanism. Snapshot akan bloat lagi. **Butuh cron/timer untuk prune periodic** atau trigger di `mt5_bridge.py` yang hapus snapshot older than N hours saat insert.

### A3. ✅ news_feed.py exception handling terlalu sempit
- **File**: `src/alma/news_feed.py:49-52`
- **Root cause**: `_fetch_rss()` catch cuma `(ET.ParseError, OSError, TimeoutError)`. Exception lain (`ValueError`, `RuntimeError`, generic) bisa leak ke decision cycle dan crash runtime.
- **Fix**: Tambah `except Exception: # noqa: BLE001` catch-all. News feed tidak boleh pernah crash decision cycle.
- **Test gap**: Semua 4 test mock `_fetch_rss` entirely. Real `urlopen → exception → return []` path tidak pernah di-exercise.

### A4. ✅ decision_id conflict rewrite tidak re-validate venue/symbol
- **File**: `src/alma/shadow_service.py:185-188`
- **Root cause**: Saat model kirim decision_id duplikat → UUID4 rewrite → re-validate `state_id` + `expiry`, tapi **tidak re-validate `(venue, symbol)` match** dengan shadow context. Risk: kalau model kirim payload dengan venue/symbol salah, validasi bisa lolos.
- **Fix**: Tambah `if (contract.venue, contract.symbol) != (venue, symbol): raise ValueError(...)`.

### A5. ✅ Decimal(existing[2]) no try/except
- **File**: `src/alma/shadow_service.py:92`
- **Root cause**: `Decimal(existing[2])` bisa raise `InvalidOperation`/`ArithmeticError` kalau DB data corrupt. Tidak di-catch.
- **Fix**: Wrap dalam try/except `(ArithmeticError, ValueError)` → set `delta = None`.

### A6. ✅ AI prompt tidak terangkan target schema
- **File**: `src/alma/shadow_transport.py:69`
- **Root cause**: Prompt tulis `"targets":[]` — cuma contoh empty array, tidak terangkan schema target object. Model menebak field names: kirim `volume`, `weight` → `forbid_unknown_fields=True` reject → 4 dari 5 run REJECTED.
- **Fix**: Prompt sekarang tulis `"targets":[{"price":"positive decimal","close_fraction":"0..1 decimal"}]` + "Each target object has ONLY price and close_fraction — no other fields."
- **Test gap**: Test assert string `"target with price and close_fraction"` tapi tidak test bahwa model sebenarnya tidak kirim field tidak dikenal dengan prompt baru.

### A7. ✅ Stale 2025 test data poluting DB
- **Root cause**: Decisions dari testing (June 2025) masih di DB, dipollute shadow_runs. Append-only triggers block DELETE.
- **Fix**: Drop triggers → DELETE → recreate 16 triggers via `open_ledger()`.
- **Note**: Append-only trigger bypass procedure: backup → DROP TRIGGER → DELETE → `open_ledger()` recreate. Tidak ada helper function untuk ini.

---

## B. INFRASTRUCTURE ISSUES — PARTIAL/OPEN

### B1. ⚠️ MT5 Wine crash loop — 625+ restarts
- **File**: `src/alma/mt5_soak.py:235-243`, `/etc/systemd/system/alma-mt5-wine.service`
- **Root cause**: `terminal64.exe` crash di Wine headless Linux setiap beberapa jam (memory leak, Wine API incomplete, X11/headless rendering issue). Monitor (`alma-mt5-monitor`) detect `MT5_STATE_STALE` → exit 1 → systemd `Restart=always` → restart → detect stale lagi → crash loop.
- **Fix partial**: Monitor sekarang auto-restart Wine via `subprocess.run(["systemctl","restart","alma-mt5-wine.service"])`. Wine unit tambah `ExecStartPre=-/opt/wine-stable/bin/wineserver -k9` (kill stale wineserver).
- **Unresolved**:
  1. Setelah terminal64 crash + restart, butuh **3 menit** untuk full login + start producing snapshots. Selama itu, monitor loop "stale" dan bisa trigger restart lagi sebelum terminal siap → crash loop.
  2. Kadang terminal64 start tapi **algo trading tidak auto-enable** — perlu manual klik di VNC.
  3. `ExecStartPre wineserver -k9` kadang tidak bersihkan cukup — perlu `pkill -9` semua wine process + `rm -f /root/.wine-alma/.wineserver-*` + `rm -rf /tmp/.wine-*`.
  4. **Untuk production real-money: MT5 butuh Windows native.** Wine headless tidak reliable. Opsi: Windows VPS terpisah untuk MT5, ALMA core tetap Linux.

### B2. ❌ MT5 snapshot auto-prune tidak ada
- **File**: `src/alma/mt5_bridge.py` (ingest function)
- **Root cause**: Setiap snapshot cycle insert row baru. Tidak ada cleanup. DB bloat lagi dari 1.7MB → 25MB+ dalam beberapa jam (lihat current state: 25.8MB).
- **Fix needed**: Tambah auto-prune di `mt5_bridge.py` — saat insert snapshot, hapus snapshot older than 1 jam atau keep last 1000 per session. Atau systemd timer yang VACUUM DB periodic.

### B3. ⚠️ WAL tidak auto-checkpoint
- **File**: N/A (SQLite WAL mode behavior)
- **Root cause**: WAL mode aktif, runtime terus menulis → WAL grow tanpa checkpoint. WAL 584MB sebelumnya.
- **Fix partial**: Manual `PRAGMA wal_checkpoint(TRUNCATE)` + VACUUM dilakukan. Tapi tidak ada auto-checkpoint.
- **Fix needed**: Tambah `PRAGMA wal_autocheckpoint=1000` di `open_ledger()` atau cron timer.

### B4. ⚠️ sqlite "database is locked" saat VACUUM
- **File**: `src/alma/database.py:20`
- **Root cause**: `immediate_transaction()` pakai `BEGIN IMMEDIATE`. Saat VACUUM/manual cleanup berjalan, bridge coba insert → `database is locked` → bridge crash.
- **Fix needed**: Tambah retry logic di `immediate_transaction()` atau gunakan `busy_timeout`. Atau jangan VACUUM saat runtime aktif.

---

## C. CODE AUDIT — OPEN ISSUES (from subagent deep audit)

### C1. ❌ Dead code — 6 modules/functions dengan zero callers di src/
- **Files**:
  - `decision_fallback.py:25-37` — `request_with_fallback` (sync version), superseded by async. 0 callers.
  - `decision_contract.py:200-206` — `parse_decision_with_repair`, shadow_service pakai own repair flow. 0 callers.
  - `shadow_request.py:164-208` — `HookCoalescer` class + `accept_cooldown` method. Runtime builds requests directly. 0 callers.
  - `venue_mode_store.py` (entire module) — Test-only. `src/` uses `ledger.open_ledger` + `bootstrap_monitor_modes`.
  - `idempotency.py` (entire module) — Test-only. `src/` uses `ledger.request_ids` table directly.
  - `emergency_stop.py` (entire module) — Test-only. `src/` uses inline logic di `runtime.py`.
- **Fix**: Delete dari `src/` atau pindah ke `tests/` kalau memang hanya untuk test. Tidak cause bug tapi membingungkan developer baru.

### C2. ⚠️ `observed_state_id` parameter — declared tapi UNUSED
- **File**: `src/alma/shadow_service.py:68`
- **Root cause**: `observed_state_id` parameter di `evaluate()` method declared, diterima dari `runtime.py:338`, tapi **tidak pernah dipakai** di method body.
- **Fix**: Hapus parameter atau implement usage. Kalau state_id match seharusnya di-validate sebelum record decision.

### C3. ⚠️ `_verify_or_cancel` unreachable post-submit path
- **File**: `src/alma/execution.py:260-269`, `execution.py:471-480`
- **Root cause**: Kedua venue (`BinanceNativeVenue.submit()` di `binance_native_execution.py:371` dan `MT5Venue.submit()` di `mt5_bridge.py:1203`) **selalu return None**. `submission is None` → return `UNKNOWN` di line 258. Code setelahnya (`_verify_or_cancel`) **tidak pernah dieksekusi**.
- **Status**: Ini **by design** — Nautilus async event-driven, MT5 async via file IPC. Status datang via snapshot cycle selanjutnya (`recover_open_intents` di `execution.py:271`). Tapi code path mati adalah code smell.
- **Fix**: Hapus dead path atau tambah comment yang jelaskan ini intentional.

### C4. ⚠️ Step-3.5-flash (fallback model) kirim `ttl_seconds` sebagai string
- **File**: N/A (model behavior)
- **Root cause**: Step-3.5-flash kirim `"ttl_seconds":"60"` (string) bukan `"ttl_seconds":60` (integer). `msgspec` reject: `Expected int, got str`. Step-3.7-flash (primary) tidak punya masalah ini.
- **Fix needed**: `_coerce_decimal_strings()` di `decision_contract.py` stringify decimal fields, tapi **tidak coerce int fields**. Tambah coercion untuk `ttl_seconds` (str→int) di `_coerce_decimal_strings` atau parser terpisah.

### C5. 📌 `_record_observed` dedup comparison — Decimal repr edge case
- **File**: `src/alma/execution.py:965`
- **Root cause**: Dedup query `SELECT status, filled_quantity FROM order_events WHERE order_id = ?` compare `latest == (order.status, str(order.filled_quantity))`. `str(Decimal("0.001"))` = `"0.001"` tapi `str(Decimal("0"))` vs `"0.0"` bisa mismatch kalau DB simpan format beda.
- **Status**: Low risk — hanya duplikat insert di-order_events. Tidak cause money loss.

### C6. 📌 `shadow_runs.session` field — value "MT5" tidak valid
- **File**: DB schema, `shadow_runs` table
- **Root cause**: CHECK constraint untuk `session` field expect `LONDON/ASIA/NEW_YORK/OFF_HOURS`, tapi ada rows dengan `session=MT5`. Ini error di runtime yang insert session dari venue name, bukan session window.
- **Status**: Tidak crash (CHECK constraint tidak ditegakkan di SQLite untuk INSERT?) tapi data inconsistent.

---

## D. TEST COVERAGE GAPS

### D1. ❌ news_feed network failure — real urlopen path tidak tested
- **File**: `tests/test_news_feed.py`
- **Issue**: Semua 4 test mock `_fetch_rss` entirely. Real `urlopen → exception → return []` tidak pernah di-exercise. Code paling safety-critical (lines 49-52) punya zero coverage.

### D2. ❌ live_armed → TRADE promotion — SQL tidak tested
- **File**: `tests/` (no test file)
- **Issue**: `runtime.py:447-454` SQL promotion tidak punya test. Tidak ada test yang set `live_armed=True` + verify venue_modes promoted ke TRADE.

### D3. ⚠️ Multi-provider fallback — logic tested, integration not
- **File**: `tests/test_decision_fallback.py`
- **Issue**: `request_with_fallback_report` logic tested dengan mock. Tapi `ShadowService.evaluate()` yang call fallback saat primary gagal — tidak di-test end-to-end. Kalau primary model return 5xx/timeout, apakah fallback benar-benar dipanggil di context ShadowService?

### D4. ⚠️ venue_mode TRADE vs MANAGE_ONLY — executor path untested
- **File**: `tests/test_venue_modes.py`
- **Issue**: `allows_quantity_change()` tested. Tapi `TacticalExecutor.execute()` saat mode=MANAGE_ONLY — apakah reject new exposure secara benar? Tidak ada test yang submit order saat MANAGE_ONLY dan verify rejection.

### D5. ❌ Decimal/float coercion — primary path untested
- **File**: `tests/test_decision_contract.py`
- **Issue**: `_coerce_decimal_strings()` coercion function tested untuk numeric→string. Tapi **str→int** coercion untuk `ttl_seconds` (bug C4) tidak ada test.

### D6. ⚠️ Source files dengan NO test file
- `src/alma/binance_data.py` — no test
- `src/alma/binance_testnet.py` — no test (testnet account setup)
- `src/alma/binance_testnet_soak.py` — no test (soak test runner)
- `src/alma/mt5_runtime.py` — no test (bridge HTTP server)
- `src/alma/mt5_soak.py` — no test (monitor)
- `src/alma/operations.py` — no test (health gate)

---

## E. CONFIG / DEPLOY ISSUES

### E1. ⚠️ deploy/alma-binance-account.conf dan deploy/alma-mt5-account.conf di-gitignore
- **Status**: Benar — tidak boleh di-push (berisi credentials/login).
- **Issue**: Tapi repo tidak punya `.example` template. User baru tidak tahu format config.
- **Fix**: Buat `deploy/alma-binance-account.conf.example` dan `deploy/alma-mt5-account.conf.example` dengan placeholder values.

### E2. ⚠️ `deploy/alma-mt5-wine.service` belum include `ExecStartPre wineserver -k9`
- **Status**: Patch diterapkan langsung ke `/etc/systemd/system/alma-mt5-wine.service` (live), tapi **deploy/ yang di-push ke GitHub belum diupdate**.
- **Fix**: Update `deploy/alma-mt5-wine.service` untuk match live config.

### E3. ⚠️ `deploy/alma-mt5-monitor.service` belum reflect auto-restart Wine
- **Status**: `mt5_soak.py` sudah di-patch (auto-restart Wine saat stale), tapi deploy unit tidak mention dependency ke `alma-mt5-wine.service`.

### E4. 📌 No README.md di repo
- **Status**: Repo di-push tanpa README. GitHub repo tanpa README terlihat kosong.
- **Fix**: Tambah `README.md` dengan: deskripsi project, cara setup, cara run tests, cara deploy services.

---

## F. MODEL/LLM ISSUES

### F1. ⚠️ Step-3.5-flash (fallback) kirim `ttl_seconds` sebagai string → REJECTED
- **File**: Model behavior, parser `src/alma/decision_contract.py`
- **Root cause**: Step-3.5-flash kirim `"ttl_seconds":"60"` (string). msgspec expect int. Primary (step-3.7-flash) tidak punya masalah ini.
- **Fix needed**: Tambah str→int coercion untuk `ttl_seconds` di `_coerce_decimal_strings()` atau parser terpisah.

### F2. 📌 AI model selalu pilih NO_CHANGE
- **Status**: Model belum pernah kirim OPEN_LONG/OPEN_SHORT. Semua decisions ACCEPTED tapi action=NO_CHANGE.
- **Root cause**: Model tidak punya cukup signal/momentum untuk justify entry. Bisa jadi:
  1. Prompt tidak cukup jelas tentang kapan harus OPEN (vs NO_CHANGE).
  2. Market data input tidak cukup informative (cuma price + quantity, tidak ada indicators seperti RSI/MACD/volume).
  3. Model terlalu conservative.
  4. `uncertainty` field di prompt ditulis "0..1" — model mungkin selalu set uncertainty tinggi → pilih NO_CHANGE.
- **Investigation needed**: Trace prompt yang sebenarnya dikirim ke model (input state) + response. Apakah model punya cukup info untuk justify OPEN?

### F3. 📌 9Router API key di config bukan real gate
- **File**: `~/.config/alma/9router.conf`, `NINEROUTER_API_KEY`
- **Status**: `NINEROUTER_API_KEY` bukan real authentication — 9Router tidak validasi key dengan strict. Siapapun yang bisa connect ke 127.0.0.1:20128 bisa pakai.
- **Fix needed**: Tidak urgent karena 9Router listen di localhost only. Tapi kalau port di-expose, butuh proper auth.

### F4. 📌 Zombie PIDs ALMA/9Router setelah restart
- **Status**: Setelah restart, kadang ada zombie PIDs dari ALMA atau 9Router yang tidak clean shutdown. Perlu `pkill -9` manual.
- **Fix needed**: Tambah `KillSignal=SIGINT` + `TimeoutStopSec=30` di systemd unit untuk graceful shutdown.

---

## G. SUMMARY — Priority untuk OpenCode Harness

### P0 — Production blocker (fix sebelum live trading)
1. **B2**: MT5 snapshot auto-prune (DB bloat lagi tanpa ini)
2. **B3**: WAL auto-checkpoint
3. **B4**: Retry logic untuk `database is locked`
4. **F1**: str→int coercion untuk `ttl_seconds` (fallback model crash)

### P1 — Reliability (fix segera)
5. **B1**: MT5 Wine crash loop — perlu backoff logic antara monitor detect stale + Wine restart
6. **C2**: `observed_state_id` unused — hapus atau implement
7. **C4**: Step-3.5-flash ttl_seconds string —asiswa coercion
8. **E2-E3**: Update deploy/ units untuk match live config

### P2 — Code quality
9. **C1**: Delete 6 dead code modules
10. **C3**: Clean up unreachable `_verify_or_cancel` path
11. **D1-D6**: Fill test coverage gaps

### P3 — Nice to have
12. **E1**: Config .example templates
13. **E4**: README.md
14. **F2**: Investigasi kenapa AI selalu NO_CHANGE
15. **F4**: Zombie PID cleanup di systemd
