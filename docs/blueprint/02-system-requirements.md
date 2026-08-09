# 02 — System Requirements

## Prioritas

- **MUST:** wajib agar sistem aman dan operasional.
- **SHOULD:** bernilai tinggi, boleh setelah jalur inti stabil.
- **MAY:** hanya ditambah jika pengukuran membuktikan kebutuhan.

## Functional requirements

### Market dan account state

- **FR-001 MUST:** menerima Binance market/account event secara streaming.
- **FR-002 MUST:** menerima MT5 tick, account, symbol specification, order, position, dan fill event.
- **FR-003 MUST:** membentuk bar M1/M5/M15/H1 secara incremental, bukan mengunduh ulang tick.
- **FR-004 MUST:** broker/exchange menjadi source of truth; state lokal dapat di-resync.
- **FR-005 MUST:** sequence gap order book memicu snapshot ulang sebelum data dianggap valid.
- **FR-006 MUST:** setiap snapshot memiliki timestamp, age, venue, symbol, dan state ID.

### AI decision

- **FR-010 MUST:** AI menerima compact snapshot, relevant memory, active positions, pending orders, account/margin, dan news state.
- **FR-011 MUST:** AI menghasilkan Decision Contract pada schema di [04-api-contracts.md](04-api-contracts.md).
- **FR-012 MUST:** invalid JSON mendapat maksimal satu repair attempt.
- **FR-013 MUST:** rate limit, quota, timeout, connection error, atau 5xx memicu bounded fallback.
- **FR-014 MUST:** arah yang tidak disukai bukan alasan fallback.
- **FR-015 MUST:** semua model fallback memakai snapshot, policy, schema, dan deterministic settings yang sama.
- **FR-016 MUST:** seluruh model gagal berarti tidak ada target baru.

### Execution

- **FR-020 MUST:** AI menentukan desired portfolio state, bukan perintah order buta.
- **FR-021 MUST:** reconciler menghitung delta dari posisi aktual + pending orders.
- **FR-022 MUST:** tactical executor mendukung limit, aggressive-limit, stop entry, market-protected, wait-retest, dan abort.
- **FR-023 MUST:** executor menangani partial fill, cancel/replace, expiry, dan missed-entry policy tanpa memanggil AI setiap tick.
- **FR-024 MUST:** order submit membaca ulang venue truth dan mode venue.
- **FR-025 MUST:** setiap intent menggunakan idempotency/correlation key.
- **FR-026 MUST:** volume, tick size, price precision, stop level, dan margin divalidasi dari metadata venue aktual.

### Venue modes

- **FR-030 MUST:** setiap venue mendukung `OFF`, `MONITOR`, `MANAGE_ONLY`, `TRADE`.
- **FR-031 MUST:** mode disimpan persisten dan diperiksa sebelum setiap order.
- **FR-032 MUST:** transisi dari `TRADE` dengan posisi terbuka meminta policy: manage, freeze with venue protection, atau close-and-disable.
- **FR-033 MUST:** emergency stop membatalkan entry baru dan menjalankan policy posisi yang dipilih operator.

### Strategy dan learning

- **FR-040 MUST:** implementasi awal hanya dua setup ALMA pada [05-data-strategy.md](05-data-strategy.md).
- **FR-041 MUST:** episode menyimpan snapshot → decision → reconciliation → orders/fills/costs → outcome.
- **FR-042 MUST:** episode tunggal tidak dapat mengubah live policy.
- **FR-043 MUST:** kandidat strategy/pattern melewati replay dan shadow gate sebelum promotion.
- **FR-044 SHOULD:** confidence AI dikalibrasi per venue/symbol/setup/regime/session/news state.

### Dashboard

- **FR-050 MUST:** menampilkan health, venue modes, account, positions, orders, PnL, drawdown, feed age, model/fallback, latency, dan token usage.
- **FR-051 MUST:** dashboard menerima update melalui SSE.
- **FR-052 MUST:** kontrol mutation diautentikasi, dikonfirmasi, dan dicatat.
- **FR-053 MUST:** dashboard tidak pernah menampilkan credential.

## Non-functional requirements

### Performance

- **NFR-001:** market events diproses tanpa menunggu AI.
- **NFR-002:** p95 local event-to-state update target ≤ 50 ms pada beban v1.
- **NFR-003:** p95 dashboard state visibility ≤ 2 detik.
- **NFR-004:** AI call memiliki hard timeout dan token cap.
- **NFR-005:** scalping target horizon detik sampai puluhan menit, bukan HFT.

### Reliability

- **NFR-010:** restart tidak boleh membuat duplicate order.
- **NFR-011:** reconnect harus resync positions/orders sebelum `TRADE` dilanjutkan.
- **NFR-012:** stale/unknown state membekukan exposure baru.
- **NFR-013:** server-side/venue-resident protection bertahan ketika core mati.
- **NFR-014:** SQLite memakai WAL, foreign keys, busy timeout, dan backup konsisten.

### Security

- **NFR-020:** API key Binance tidak memiliki withdrawal permission.
- **NFR-021:** broker credential tetap di terminal MT5 dan tidak masuk prompt/ledger.
- **NFR-022:** secret disimpan sebagai environment file berizin ketat.
- **NFR-023:** dashboard bind localhost/private VPN; bukan internet publik.
- **NFR-024:** bridge MT5 memakai shared secret + timestamp + nonce/HMAC atau private loopback bila satu host.
- **NFR-025:** seluruh mutation dan mode transition masuk audit log.

### Maintainability

- **NFR-030:** Python 3.12; dependency dikunci `uv.lock`.
- **NFR-031:** NautilusTrader menggunakan wheel rilis, bukan fork.
- **NFR-032:** satu aplikasi utama; modul dipisah berdasarkan tanggung jawab nyata.
- **NFR-033:** branch/loop/parser/money path memiliki minimal satu runnable check.

## Platform requirement

### Linux core

- Ubuntu x86_64, 4 vCPU, 8 GB RAM, 100+ GB storage.
- Python 3.12, `uv`, systemd, NTP aktif.
- 9Router hanya di localhost/private network.

### MT5 pada VPS Linux yang sama

**Didukung sebagai deployment target awal melalui Wine**, dengan syarat:

1. terminal MT5 broker dapat login dan menerima ticks stabil;
2. AutoTrading/EA berjalan setelah reboot;
3. MetaEditor dapat compile `AlmaBridge.mq5` atau file `.ex5` dibangun di host kompatibel;
4. soak test minimal 7 hari tanpa disconnect/crash yang tidak pulih;
5. restart Wine/terminal dan resync state lulus;
6. broker tidak memblokir environment Wine.

Jika salah satu syarat gagal berulang, pindahkan hanya MT5 + EA ke Windows VPS; core Linux tidak berubah.

## External inputs yang masih diperlukan

- Binance Futures Testnet key ketika fase execution dimulai.
- Akun MT5 demo yang login lokal.
- Nama simbol dan contract specification aktual broker.
- Pilihan calendar/news source sebelum fase news.
- Persetujuan eksplisit sebelum canary live.
