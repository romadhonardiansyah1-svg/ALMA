import json
import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from alma.ledger import (
    append_decision,
    append_order_event,
    open_ledger,
    record_intent_mutation,
    record_shadow_run,
    reserve_order_submission,
)
from alma.mt5_bridge import ensure_mt5_schema
from alma.operations import (
    create_backup,
    health_report,
    prune_parquet,
    restore_backup,
    router_healthy,
    runtime_status_healthy,
    verify_parquet_manifest,
    write_parquet_manifest,
)

NOW = datetime(2026, 7, 31, 15, tzinfo=UTC)


def test_runtime_status_requires_fresh_healthy_payload() -> None:
    healthy = {
        "ok": True,
        "observed_at": NOW.isoformat(),
        "venue_ready": {"BINANCE": True, "MT5": True},
    }
    assert runtime_status_healthy(healthy, now=NOW)
    assert not runtime_status_healthy(
        {**healthy, "observed_at": (NOW - timedelta(seconds=11)).isoformat()},
        now=NOW,
    )
    assert not runtime_status_healthy({**healthy, "ok": False}, now=NOW)


def test_online_backup_restore_and_retention(tmp_path: Path) -> None:
    database = tmp_path / "alma.db"
    connection = open_ledger(database)
    try:
        connection.execute("INSERT INTO venue_modes VALUES ('BINANCE', 'MONITOR')")
        connection.commit()
        backup_dir = tmp_path / "backups"
        first = create_backup(database, backup_dir, now=NOW, retain=1)
        assert first.stat().st_mode & 0o777 == 0o600
        connection.execute("UPDATE venue_modes SET mode='OFF' WHERE venue_id='BINANCE'")
        connection.commit()
        second = create_backup(
            database, backup_dir, now=NOW + timedelta(seconds=1), retain=1
        )
        assert not first.exists() and second.exists()
    finally:
        connection.close()

    restored = restore_backup(second, tmp_path / "restore" / "alma.db")
    reopened = open_ledger(restored)
    try:
        assert reopened.execute(
            "SELECT mode FROM venue_modes WHERE venue_id='BINANCE'"
        ).fetchone() == ("OFF",)
    finally:
        reopened.close()
    with pytest.raises(FileExistsError):
        restore_backup(second, restored)


def test_parquet_manifest_detects_tamper_and_retention_ignores_symlink(
    tmp_path: Path,
) -> None:
    root = tmp_path / "data"
    old = root / "venue=BINANCE" / "old.parquet"
    fresh = root / "venue=BINANCE" / "fresh.parquet"
    old.parent.mkdir(parents=True)
    old.write_bytes(b"old")
    fresh.write_bytes(b"fresh")
    os.utime(old, (NOW.timestamp() - 100, NOW.timestamp() - 100))
    os.utime(fresh, (NOW.timestamp() + 100, NOW.timestamp() + 100))
    outside = tmp_path / "outside.parquet"
    outside.write_bytes(b"secret")
    link = root / "linked.parquet"
    link.symlink_to(outside)

    manifest_path = tmp_path / "manifest.json"
    manifest = write_parquet_manifest(root, manifest_path, now=NOW)
    assert [item["path"] for item in manifest["files"]] == [
        "venue=BINANCE/fresh.parquet",
        "venue=BINANCE/old.parquet",
    ]
    verify_parquet_manifest(root, manifest_path)
    old.write_bytes(b"tampered")
    os.utime(old, (NOW.timestamp() - 100, NOW.timestamp() - 100))
    with pytest.raises(RuntimeError, match="mismatch"):
        verify_parquet_manifest(root, manifest_path)

    removed = prune_parquet(root, before=NOW)
    assert removed == [old]
    assert fresh.exists() and link.is_symlink() and outside.read_bytes() == b"secret"


def test_health_report_fails_closed_for_database_disk_and_ntp(tmp_path: Path) -> None:
    missing = health_report(
        tmp_path / "missing.db",
        tmp_path,
        minimum_free_bytes=10**30,
        ntp_check=lambda: False,
    )
    assert missing["ok"] is False
    assert missing["alerts"] == ["DATABASE_ERROR", "DISK_LOW", "NTP_UNSYNCED"]

    database = tmp_path / "alma.db"
    connection = open_ledger(database)
    connection.close()
    healthy = health_report(
        database,
        tmp_path,
        minimum_free_bytes=0,
        ntp_check=lambda: True,
    )
    assert healthy["ok"] is True and healthy["alerts"] == []

    router_down = health_report(
        database,
        tmp_path,
        minimum_free_bytes=0,
        ntp_check=lambda: True,
        router_url="http://127.0.0.1:20128/api/health",
        router_check=lambda _: False,
    )
    assert router_down["alerts"] == ["ROUTER_UNAVAILABLE"]


def test_health_fallback_alert_recovers_after_latest_validated_run(
    tmp_path: Path,
) -> None:
    database = tmp_path / "alma.db"
    connection = open_ledger(database)

    def record(request_id: str, status: str) -> None:
        record_shadow_run(
            connection,
            request_id=request_id,
            state_id=request_id,
            decision=None,
            status=status,
            validation_error="provider failed" if status == "NO_DECISION" else None,
            requested_model="model-a",
            actual_model="model-a" if status != "NO_DECISION" else "",
            prompt_tokens=0,
            completion_tokens=0,
            latency_ms=1,
            attempt_count=1,
            failure_classes="NON_RETRYABLE" if status == "NO_DECISION" else "",
            fallback_used=False,
            hooks="M1_CLOSE",
            venue="BINANCE",
            symbol="BTCUSDT-PERP",
            setup="",
            regime="0",
            session="ASIA",
            news_state="",
            hypothetical_delta=Decimal(0) if status == "ACCEPTED" else None,
            created_at=NOW.isoformat(),
        )

    for index in range(3):
        record(f"failed-{index}", "NO_DECISION")
    assert health_report(
        database, tmp_path, minimum_free_bytes=0, ntp_check=lambda: True
    )["alerts"] == ["FALLBACK_EXHAUSTED_REPEATED"]

    record("recovered", "ACCEPTED")
    assert (
        health_report(database, tmp_path, minimum_free_bytes=0, ntp_check=lambda: True)[
            "alerts"
        ]
        == []
    )
    connection.close()


def test_health_order_rejection_streak_recovers_after_fill(tmp_path: Path) -> None:
    database = tmp_path / "alma.db"
    connection = open_ledger(database)
    append_decision(
        connection,
        decision_id="decision-1",
        state_id="state-1",
        created_at=NOW.isoformat(),
        raw_contract=b"{}",
        validation_result="ACCEPTED",
        model_id="model",
        prompt_hash="prompt",
        policy_hash="policy",
        code_hash="code",
    )

    def terminal(index: int, status: str) -> None:
        intent_id = f"intent-{index}"
        order_id = f"order-{index}"
        assert record_intent_mutation(
            connection,
            audit_event_id=f"audit-{index}",
            actor="test",
            before_summary="{}",
            after_summary="{}",
            intent_id=intent_id,
            decision_id="decision-1",
            request_id=f"request-{index}",
            venue="BINANCE",
            symbol="BTCUSDT-PERP",
            state_id="state-1",
            created_at=NOW.isoformat(),
            mode="TRADE",
            desired_quantity=Decimal(1),
            actual_quantity=Decimal(0),
            pending_quantity=Decimal(0),
            execution_delta=Decimal(1),
        )
        assert reserve_order_submission(
            connection,
            event_id=f"submitted-{index}",
            intent_id=intent_id,
            order_id=order_id,
            quantity=Decimal(1),
            price=Decimal(100),
            created_at=NOW.isoformat(),
        )
        append_order_event(
            connection,
            event_id=f"terminal-{index}",
            intent_id=intent_id,
            order_id=order_id,
            status=status,
            quantity=Decimal(1),
            filled_quantity=Decimal(1) if status == "FILLED" else Decimal(0),
            price=Decimal(100),
            created_at=NOW.isoformat(),
        )

    for index in range(3):
        terminal(index, "REJECTED")
    assert health_report(
        database, tmp_path, minimum_free_bytes=0, ntp_check=lambda: True
    )["alerts"] == ["ORDER_REJECTIONS_REPEATED"]

    terminal(3, "FILLED")
    assert (
        health_report(database, tmp_path, minimum_free_bytes=0, ntp_check=lambda: True)[
            "alerts"
        ]
        == []
    )
    connection.close()


def test_router_health_accepts_only_exact_loopback_health(monkeypatch) -> None:
    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return None

        def read(self, _: int) -> bytes:
            return b'{"ok":true}'

    monkeypatch.setattr("alma.operations.urlopen", lambda *_args, **_kwargs: Response())
    assert router_healthy("http://127.0.0.1:20128/api/health") is True
    assert router_healthy("http://example.com/api/health") is False


def test_health_report_detects_stale_disconnected_mt5_and_disabled_algo(
    tmp_path: Path,
) -> None:
    database = tmp_path / "alma.db"
    connection = open_ledger(database)
    ensure_mt5_schema(connection)
    payload = {
        "terminal": {
            "connected": False,
            "trade_allowed": False,
            "account_trade_allowed": False,
        }
    }
    connection.execute(
        "INSERT INTO mt5_terminal_state VALUES (?, ?, ?, ?, ?, ?)",
        (
            "terminal-1",
            "session-1",
            1,
            "state-1",
            (NOW - timedelta(minutes=1)).isoformat(),
            json.dumps(payload),
        ),
    )
    connection.commit()
    connection.close()

    report = health_report(
        database,
        tmp_path,
        minimum_free_bytes=0,
        ntp_check=lambda: True,
        clock=lambda: NOW,
    )
    assert report["alerts"] == [
        "MT5_STATE_STALE",
        "MT5_DISCONNECTED",
        "MT5_ALGO_TRADING_DISABLED",
    ]


def test_production_systemd_assets_are_native_private_and_hardened() -> None:
    root = Path(__file__).parents[1]
    services = [
        root / "deploy/alma-dashboard.service",
        root / "deploy/alma-backup.service",
        root / "deploy/alma-health.service",
        root / "deploy/alma-mt5-bridge.service",
        root / "deploy/alma-mt5-wine.service",
        root / "deploy/alma-9router.service",
    ]
    for path in services:
        text = path.read_text()
        assert "NoNewPrivileges=true" in text
        assert "ProtectSystem=strict" in text
        assert "ProtectHome=read-only" in text
        assert "cron" not in text.lower()
    for path in (services[0], services[3], services[4], services[5]):
        assert "LimitNOFILE=65536" in path.read_text()
    dashboard = services[0].read_text()
    assert "alma.dashboard_runtime" in dashboard
    assert "--port" not in dashboard
    assert "0.0.0.0" not in dashboard
    router = services[5].read_text()
    assert "--host 127.0.0.1" in router
    assert "ReadWritePaths=/root/.9router" in router
    backup_timer = (root / "deploy/alma-backup.timer").read_text()
    health_timer = (root / "deploy/alma-health.timer").read_text()
    assert "Persistent=true" in backup_timer
    assert "OnUnitActiveSec=1m" in health_timer
