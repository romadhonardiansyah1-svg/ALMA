import argparse
import hashlib
import json
import os
import sqlite3
import time
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, TypedDict


class Observation(TypedDict):
    observed_at: str
    age_seconds: float
    session_hash: str
    seq: int
    connected: bool
    trade_allowed: bool
    account_trade_allowed: bool
    account_mode: str
    position_mode: str
    server: str
    login_matches: bool
    symbol: str
    positions: int
    orders: int
    unprotected_positions: int
    foreign_positions: int
    foreign_orders: int
    active_commands: int
    invalidations: int


def _positive(value: object) -> bool:
    try:
        number = Decimal(str(value))
    except InvalidOperation:
        return False
    return number.is_finite() and number > 0


def observe(
    database: Path,
    *,
    expected_login: str,
    terminal_id: str,
    now: datetime | None = None,
) -> Observation:
    if not expected_login or not terminal_id:
        raise ValueError("expected login and terminal ID are required")
    now = now or datetime.now(UTC)
    connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
    try:
        row = connection.execute(
            "SELECT session_id, last_seq, observed_at, payload "
            "FROM mt5_terminal_state WHERE terminal_id=?",
            (terminal_id,),
        ).fetchone()
        if row is None:
            raise RuntimeError("MT5_STATE_MISSING")
        payload: dict[str, Any] = json.loads(row[3])
        terminal = payload["terminal"]
        observed = datetime.fromisoformat(row[2])
        age = (now - observed).total_seconds()
        active_commands = connection.execute(
            "SELECT count(*) FROM mt5_commands "
            "WHERE terminal_id=? AND status IN ('PENDING','DELIVERED')",
            (terminal_id,),
        ).fetchone()[0]
        invalidations = connection.execute(
            "SELECT count(*) FROM mt5_terminal_invalidations WHERE terminal_id=?",
            (terminal_id,),
        ).fetchone()[0]
        return {
            "observed_at": observed.isoformat(),
            "age_seconds": round(age, 6),
            "session_hash": hashlib.sha256(str(row[0]).encode()).hexdigest(),
            "seq": int(row[1]),
            "connected": bool(terminal["connected"]),
            "trade_allowed": bool(terminal["trade_allowed"]),
            "account_trade_allowed": bool(terminal["account_trade_allowed"]),
            "account_mode": str(terminal["account_mode"]),
            "position_mode": str(terminal["margin_mode"]),
            "server": str(terminal["server"]),
            "login_matches": str(payload["account"]["login"]) == expected_login,
            "symbol": str(payload["symbol"]["name"]),
            "positions": len(payload["positions"]),
            "orders": len(payload["orders"]),
            "unprotected_positions": sum(
                not str(item.get("root_id", "foreign:")).startswith("foreign:")
                and not (_positive(item.get("sl")) and _positive(item.get("tp")))
                for item in payload["positions"]
            ),
            "foreign_positions": sum(
                str(item.get("root_id", "foreign:")).startswith("foreign:")
                for item in payload["positions"]
            ),
            "foreign_orders": sum(
                str(item.get("root_id", "foreign:")).startswith("foreign:")
                for item in payload["orders"]
            ),
            "active_commands": int(active_commands),
            "invalidations": int(invalidations),
        }
    finally:
        connection.close()


def validate_observation(
    observation: Observation,
    *,
    max_age_seconds: float,
    expected_account_mode: str,
    expected_position_mode: str,
    expected_server: str,
    expected_symbol: str,
) -> None:
    if max_age_seconds <= 0:
        raise ValueError("max age must be positive")
    if (
        observation["account_mode"] != expected_account_mode
        or observation["position_mode"] not in {"HEDGING", "NETTING"}
        or (
            expected_position_mode != "AUTO"
            and observation["position_mode"] != expected_position_mode
        )
        or not observation["login_matches"]
        or observation["server"] != expected_server
        or observation["symbol"] != expected_symbol
    ):
        raise RuntimeError("MT5_IDENTITY_MISMATCH")
    if observation["age_seconds"] < 0 or observation["age_seconds"] > max_age_seconds:
        raise RuntimeError("MT5_STATE_STALE")
    if not (
        observation["connected"]
        and observation["trade_allowed"]
        and observation["account_trade_allowed"]
    ):
        raise RuntimeError("MT5_NOT_READY")
    if observation["invalidations"]:
        raise RuntimeError("MT5_STATE_INVALID")
    if observation["unprotected_positions"]:
        raise RuntimeError("MT5_UNPROTECTED_POSITION")
    if observation["active_commands"]:
        raise RuntimeError("MT5_COMMAND_ACTIVE")


def _write(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        path.chmod(0o600)
    finally:
        temporary.unlink(missing_ok=True)


def run(
    database: Path,
    evidence: Path,
    *,
    duration_seconds: float,
    interval_seconds: float,
    max_age_seconds: float,
    expected_login: str,
    expected_account_mode: str,
    expected_position_mode: str,
    expected_server: str,
    expected_symbol: str,
    terminal_id: str,
) -> None:
    if duration_seconds < 0 or interval_seconds <= 0:
        raise ValueError("duration must be non-negative and interval must be positive")
    started_at = datetime.now(UTC)
    deadline = None if duration_seconds == 0 else time.monotonic() + duration_seconds
    samples = 0
    session_changes = 0
    previous_session: str | None = None
    previous_seq: int | None = None
    progress: dict[str, object] = {
        "passed": False,
        "healthy": False,
        "in_progress": True,
        "read_only": True,
        "mutation_attempted": False,
        "started_at": started_at.isoformat(),
        "requested_duration_seconds": duration_seconds or None,
        "samples": samples,
        "session_changes": session_changes,
    }
    try:
        while True:
            current = observe(
                database, expected_login=expected_login, terminal_id=terminal_id
            )
            validate_observation(
                current,
                max_age_seconds=max_age_seconds,
                expected_account_mode=expected_account_mode,
                expected_position_mode=expected_position_mode,
                expected_server=expected_server,
                expected_symbol=expected_symbol,
            )
            if previous_session == current["session_hash"] and previous_seq is not None:
                if current["seq"] <= previous_seq:
                    raise RuntimeError("MT5_SEQUENCE_NOT_ADVANCING")
            elif previous_session is not None:
                if current["seq"] < 1:
                    raise RuntimeError("MT5_SESSION_RESTART_INVALID")
                session_changes += 1
            previous_session = current["session_hash"]
            previous_seq = current["seq"]
            samples += 1
            progress.update(
                healthy=True,
                samples=samples,
                session_changes=session_changes,
                last_observation=current,
            )
            _write(evidence, progress)
            if deadline is None:
                time.sleep(interval_seconds)
                continue
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(interval_seconds, remaining))
    except Exception as error:
        progress.update(
            healthy=False,
            in_progress=False,
            failed_at=datetime.now(UTC).isoformat(),
            failure=str(error),
        )
        _write(evidence, progress)
        if str(error) == "MT5_STATE_STALE":
            import subprocess

            subprocess.run(
                ["systemctl", "restart", "alma-mt5-wine.service"],
                timeout=30,
                check=False,
            )
        raise
    completed_at = datetime.now(UTC)
    _write(
        evidence,
        {
            **progress,
            "passed": True,
            "healthy": True,
            "in_progress": False,
            "completed_at": completed_at.isoformat(),
            "duration_seconds": (completed_at - started_at).total_seconds(),
        },
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run continuous read-only MT5 health monitor"
    )
    parser.add_argument("--database", type=Path, default=Path("var/alma.db"))
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--duration", type=float, default=0)
    parser.add_argument("--interval", type=float, default=60)
    parser.add_argument("--max-age", type=float, default=5)
    parser.add_argument(
        "--expected-login", default=os.environ.get("ALMA_MT5_LOGIN", "")
    )
    parser.add_argument(
        "--expected-account-mode",
        default=os.environ.get("ALMA_MT5_ACCOUNT_MODE", ""),
    )
    parser.add_argument(
        "--expected-position-mode",
        default=os.environ.get("ALMA_MT5_POSITION_MODE", ""),
    )
    parser.add_argument(
        "--expected-server", default=os.environ.get("ALMA_MT5_SERVER", "")
    )
    parser.add_argument(
        "--expected-symbol", default=os.environ.get("ALMA_MT5_SYMBOL", "")
    )
    parser.add_argument(
        "--terminal-id", default=os.environ.get("ALMA_MT5_TERMINAL_ID", "mt5-1")
    )
    args = parser.parse_args()
    try:
        run(
            args.database,
            args.evidence,
            duration_seconds=args.duration,
            interval_seconds=args.interval,
            max_age_seconds=args.max_age,
            expected_login=args.expected_login,
            expected_account_mode=args.expected_account_mode,
            expected_position_mode=args.expected_position_mode,
            expected_server=args.expected_server,
            expected_symbol=args.expected_symbol,
            terminal_id=args.terminal_id,
        )
    except RuntimeError as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
