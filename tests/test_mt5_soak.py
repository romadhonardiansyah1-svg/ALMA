import json
import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from alma.mt5_soak import observe, run, validate_observation

NOW = datetime(2026, 8, 1, 4, 45, tzinfo=UTC)


def database(tmp_path):
    path = tmp_path / "alma.db"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE mt5_terminal_state (
            terminal_id TEXT PRIMARY KEY, session_id TEXT, last_seq INTEGER,
            state_id TEXT, observed_at TEXT, payload TEXT
        );
        CREATE TABLE mt5_terminal_invalidations (
            terminal_id TEXT PRIMARY KEY, reason TEXT, invalidated_at TEXT
        );
        CREATE TABLE mt5_commands (
            request_id TEXT PRIMARY KEY, terminal_id TEXT, kind TEXT,
            payload_hash TEXT, payload TEXT, created_at TEXT, status TEXT,
            ack_payload TEXT
        );
        """
    )
    payload = {
        "terminal": {
            "connected": True,
            "trade_allowed": True,
            "account_trade_allowed": True,
            "account_mode": "DEMO",
            "margin_mode": "HEDGING",
            "server": "Exness-MT5Trial6",
            "build": 6074,
        },
        "account": {"login": "123456"},
        "symbol": {"name": "XAUUSD"},
        "positions": [],
        "orders": [],
    }
    connection.execute(
        "INSERT INTO mt5_terminal_state VALUES (?, ?, ?, ?, ?, ?)",
        ("mt5-1", "session-1", 42, "state-1", NOW.isoformat(), json.dumps(payload)),
    )
    connection.commit()
    connection.close()
    return path


def test_monitor_failure_overwrites_stale_healthy_evidence(
    tmp_path, monkeypatch
) -> None:
    evidence = tmp_path / "monitor.json"
    evidence.write_text('{"healthy": true}', encoding="utf-8")

    def fail(*args, **kwargs):
        raise RuntimeError("MT5_STATE_STALE")

    monkeypatch.setattr("alma.mt5_soak.observe", fail)
    with pytest.raises(RuntimeError, match="MT5_STATE_STALE"):
        run(
            tmp_path / "alma.db",
            evidence,
            duration_seconds=1,
            interval_seconds=0.01,
            max_age_seconds=5,
            expected_login="123",
            expected_account_mode="DEMO",
            expected_position_mode="HEDGING",
            expected_server="Broker-Demo",
            expected_symbol="XAUUSD",
            terminal_id="terminal-1",
        )
    payload = json.loads(evidence.read_text(encoding="utf-8"))
    assert payload["healthy"] is False
    assert payload["in_progress"] is False
    assert payload["failure"] == "MT5_STATE_STALE"


def test_mt5_soak_observation_accepts_only_fresh_clean_expected_demo(tmp_path) -> None:
    path = database(tmp_path)
    result = observe(path, expected_login="123456", terminal_id="mt5-1", now=NOW)
    validate_observation(
        result,
        max_age_seconds=5,
        expected_account_mode="DEMO",
        expected_position_mode="HEDGING",
        expected_server="Exness-MT5Trial6",
        expected_symbol="XAUUSD",
    )
    assert len(result["session_hash"]) == 64
    assert result["login_matches"] is True
    assert result["seq"] == 42
    assert result["positions"] == result["orders"] == result["active_commands"] == 0

    connection = sqlite3.connect(path)
    connection.execute(
        "UPDATE mt5_terminal_state SET observed_at=?",
        ((NOW - timedelta(seconds=6)).isoformat(),),
    )
    connection.commit()
    connection.close()
    with pytest.raises(RuntimeError, match="MT5_STATE_STALE"):
        validate_observation(
            observe(path, expected_login="123456", terminal_id="mt5-1", now=NOW),
            max_age_seconds=5,
            expected_account_mode="DEMO",
            expected_position_mode="HEDGING",
            expected_server="Exness-MT5Trial6",
            expected_symbol="XAUUSD",
        )


def test_mt5_soak_accepts_protected_owned_exposure_and_rejects_unprotected(
    tmp_path,
) -> None:
    path = database(tmp_path)
    connection = sqlite3.connect(path)
    payload = json.loads(
        connection.execute("SELECT payload FROM mt5_terminal_state").fetchone()[0]
    )
    payload["positions"] = [{"root_id": "alma-root", "sl": "2300", "tp": "2400"}]
    connection.execute(
        "UPDATE mt5_terminal_state SET payload=?", (json.dumps(payload),)
    )
    connection.commit()
    protected = observe(path, expected_login="123456", terminal_id="mt5-1", now=NOW)
    validate_observation(
        protected,
        max_age_seconds=5,
        expected_account_mode="DEMO",
        expected_position_mode="AUTO",
        expected_server="Exness-MT5Trial6",
        expected_symbol="XAUUSD",
    )

    payload["positions"][0]["sl"] = "NaN"
    connection.execute(
        "UPDATE mt5_terminal_state SET payload=?", (json.dumps(payload),)
    )
    connection.commit()
    connection.close()
    with pytest.raises(RuntimeError, match="MT5_UNPROTECTED_POSITION"):
        validate_observation(
            observe(path, expected_login="123456", terminal_id="mt5-1", now=NOW),
            max_age_seconds=5,
            expected_account_mode="DEMO",
            expected_position_mode="AUTO",
            expected_server="Exness-MT5Trial6",
            expected_symbol="XAUUSD",
        )


def test_mt5_soak_rejects_exposure_commands_and_identity_mismatch(tmp_path) -> None:
    path = database(tmp_path)
    connection = sqlite3.connect(path)
    payload = json.loads(
        connection.execute("SELECT payload FROM mt5_terminal_state").fetchone()[0]
    )
    payload["positions"] = [{"ticket": "1"}]
    payload["terminal"]["server"] = "wrong"
    connection.execute(
        "UPDATE mt5_terminal_state SET payload=?", (json.dumps(payload),)
    )
    connection.execute(
        "INSERT INTO mt5_commands VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "cmd",
            "mt5-1",
            "sync_request",
            "hash",
            "{}",
            NOW.isoformat(),
            "PENDING",
            None,
        ),
    )
    connection.commit()
    connection.close()
    with pytest.raises(RuntimeError, match="MT5_IDENTITY_MISMATCH"):
        validate_observation(
            observe(path, expected_login="123456", terminal_id="mt5-1", now=NOW),
            max_age_seconds=5,
            expected_account_mode="DEMO",
            expected_position_mode="HEDGING",
            expected_server="Exness-MT5Trial6",
            expected_symbol="XAUUSD",
        )
