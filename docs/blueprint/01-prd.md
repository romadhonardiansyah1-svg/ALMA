# 01 — Product Requirements Document (PRD)

## Produk

**Nama:** ALMA Sovereign Trader  
**Tujuan:** bot trading otonom, real-time, ringkas, dan dapat diaudit untuk Binance USDⓈ-M Futures serta MT5 gold (`XAUUSD` atau simbol broker seperti `XAUUSDC`).

ALMA memproses market/account state secara lokal, memanggil AI hanya pada peristiwa material, lalu mengeksekusi target portofolio melalui tactical executor. AI boleh menentukan arah, exposure, entry envelope, pengelolaan posisi, TP, cut loss, partial close, dan reversal. Kode deterministik hanya menjaga integritas teknis: data segar, order valid, tidak duplikat, margin tersedia, dan state broker sinkron.

## Pengguna utama

- Pemilik bot yang mengoperasikan akun Binance Futures dan/atau MT5.
- Operator yang memantau kondisi sistem, portfolio, keputusan AI, dan mode venue.
- Pengembang yang melakukan replay, shadow test, dan peningkatan strategi.

## Masalah yang diselesaikan

1. Analisis AI sering terlambat jika menangani setiap tick.
2. Entry angka tunggal mudah terlewat atau pending terlalu lama.
3. Retry/reconnect dapat menggandakan posisi.
4. Model/provider dapat terkena quota, timeout, atau menghasilkan JSON tidak valid.
5. State akun, posisi, news, dan market sering tersebar dan tidak konsisten.
6. Strategi mudah overfit bila pengalaman tunggal langsung dianggap pengetahuan.

## Nilai inti

- **Real-time:** tick diproses lokal; AI tidak berada pada jalur setiap tick.
- **Sovereign decision:** AI menentukan target portofolio dan manajemen posisi.
- **Adaptive execution:** entry berupa envelope, expiry, urgency, dan missed-entry policy.
- **Persistent learning:** episode disimpan; pattern/skill hanya dipromosikan setelah bukti.
- **Operational clarity:** dashboard real-time dan audit trail lengkap.
- **Compact deployment:** satu aplikasi, satu ledger, satu dashboard, dua adapter.

## Ruang lingkup v1

### Termasuk

- Binance USDⓈ-M Futures: public market stream, private account stream, testnet execution, lalu canary live.
- MT5 gold: ticks, account, symbol specification, positions/orders, dan execution melalui EA tipis.
- Timeframe H1/M15/M5/M1 + tick/order flow.
- Dua hipotesis awal: Liquidity Sweep Reversal dan Liquidity Vacuum Continuation.
- Calendar/news event terstruktur dan reaction confirmation.
- AI melalui 9Router dengan schema ketat dan fallback eksplisit.
- SQLite WAL, Parquet, dashboard HTML + SSE.
- Venue mode: `OFF`, `MONITOR`, `MANAGE_ONLY`, `TRADE`.
- Replay, shadow, demo/testnet, crash recovery, dan production runbook.

### Tidak termasuk

- HFT sub-milidetik.
- Puluhan agen/persona atau model lokal besar.
- Copy-trading, social signal, atau withdrawal.
- Jaminan profit atau win rate tertentu.
- Optimasi lintas ratusan simbol pada v1.
- Kubernetes, Kafka, Redis, vector database, atau dashboard framework besar.

## Persona AI

> Anda adalah pengelola modal otonom berbasis bukti. Anda membedakan fakta, asumsi, dan ketidakpastian. Anda bebas menentukan arah, exposure, entry, pengelolaan, dan exit. Jangan mengklaim edge tanpa evidence. Jangan mengejar harga di luar envelope. Jika bukti berubah, ubah target. Jika tidak ada expectancy bersih positif, pertahankan state atau tidak mengambil exposure baru.

Persona menjaga konsistensi, bukan menggantikan pembuktian statistik.

## Alur pengguna utama

1. Operator membuka dashboard privat.
2. Operator memilih mode per venue.
3. Bot menerima data real-time dan memperbarui state lokal.
4. Hook material membuat compact snapshot.
5. AI mengembalikan Decision Contract.
6. Contract divalidasi dan direkonsiliasi dengan state venue terbaru.
7. Executor mengelola order/fill secara real-time.
8. Hasil dan biaya dicatat sebagai episode.
9. Dashboard memperbarui portfolio, health, keputusan, dan telemetry.

## Ukuran keberhasilan produk

### Sistem

- Tidak ada duplicate exposure akibat retry/reconnect.
- State venue pulih setelah restart dan sesuai broker/exchange.
- P95 dashboard update ≤ 2 detik.
- Semua order memiliki correlation ID dan audit event.
- AI/provider failure tidak menghasilkan order baru yang tidak terverifikasi.
- Venue mode selalu ditegakkan di backend.

### Trading research

- Hasil replay memasukkan fee, spread, slippage, latency, funding, dan partial fill.
- Keputusan dapat direproduksi dari snapshot + policy/model version.
- Kandidat strategi tidak masuk live sebelum melewati replay dan shadow gate.
- Profitabilitas dinilai lewat expectancy bersih, drawdown, profit factor, dan stability—bukan win rate saja.

## Definition of Done produk

Produk disebut production-ready setelah seluruh gate pada [06-delivery-production.md](06-delivery-production.md) lulus. Live trading tetap memerlukan persetujuan eksplisit operator setelah testnet/demo dan canary plan disetujui.

## Dokumen terkait

- [02-system-requirements.md](02-system-requirements.md)
- [03-architecture.md](03-architecture.md)
- [04-api-contracts.md](04-api-contracts.md)
- [05-data-strategy.md](05-data-strategy.md)
- [06-delivery-production.md](06-delivery-production.md)
