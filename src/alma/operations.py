import argparse
import hashlib
import ipaddress
import json
import os
import shutil
import sqlite3
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from urllib.request import urlopen

from alma.ledger import backup_ledger, open_ledger


def _quick_check(path: Path) -> None:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        if connection.execute("PRAGMA quick_check").fetchone() != ("ok",):
            raise RuntimeError("SQLite quick_check failed")
    finally:
        connection.close()


def create_backup(
    database: Path, backup_dir: Path, *, now: datetime, retain: int = 14
) -> Path:
    if now.utcoffset() != timedelta(0) or retain < 1:
        raise ValueError("backup clock must be UTC and retain must be positive")
    if not database.is_file() or database.is_symlink():
        raise FileNotFoundError("database must be an existing regular file")
    backup_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    name = f"alma-{now.strftime('%Y%m%dT%H%M%SZ')}.db"
    destination = backup_dir / name
    temporary = backup_dir / f".{name}.tmp"
    if destination.exists() or temporary.exists():
        raise FileExistsError(destination)
    connection = open_ledger(database)
    try:
        backup_ledger(connection, temporary)
    finally:
        connection.close()
    try:
        os.chmod(temporary, 0o600)
        _quick_check(temporary)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    backups = sorted(backup_dir.glob("alma-*.db"), key=lambda item: item.name)
    for expired in backups[:-retain]:
        expired.unlink()
    return destination


def restore_backup(backup: Path, destination: Path) -> Path:
    if destination.exists():
        raise FileExistsError(destination)
    if not backup.is_file() or backup.is_symlink():
        raise FileNotFoundError("backup must be an existing regular file")
    _quick_check(backup)
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.tmp")
    if temporary.exists():
        raise FileExistsError(temporary)
    source = sqlite3.connect(f"file:{backup}?mode=ro", uri=True)
    target = sqlite3.connect(temporary)
    try:
        source.backup(target)
    finally:
        target.close()
        source.close()
    try:
        os.chmod(temporary, 0o600)
        _quick_check(temporary)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    return destination


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_parquet_manifest(
    root: Path, output: Path, *, now: datetime
) -> dict[str, Any]:
    if now.utcoffset() != timedelta(0):
        raise ValueError("manifest clock must be UTC")
    files = [
        {
            "path": path.relative_to(root).as_posix(),
            "bytes": path.stat().st_size,
            "sha256": _sha256(path),
        }
        for path in sorted(root.rglob("*.parquet"))
        if path.is_file() and not path.is_symlink()
    ]
    manifest = {"created_at": now.isoformat(), "files": files}
    output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.tmp")
    temporary.write_text(json.dumps(manifest, sort_keys=True) + "\n", encoding="utf-8")
    os.chmod(temporary, 0o600)
    os.replace(temporary, output)
    return manifest


def verify_parquet_manifest(root: Path, manifest_path: Path) -> None:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or set(manifest) != {"created_at", "files"}:
        raise ValueError("invalid manifest")
    expected: dict[str, tuple[int, str]] = {}
    for item in manifest["files"]:
        if not isinstance(item, dict) or set(item) != {"path", "bytes", "sha256"}:
            raise ValueError("invalid manifest entry")
        if (
            not isinstance(item["path"], str)
            or isinstance(item["bytes"], bool)
            or not isinstance(item["bytes"], int)
            or item["bytes"] < 0
            or not isinstance(item["sha256"], str)
            or len(item["sha256"]) != 64
        ):
            raise ValueError("invalid manifest entry")
        path = Path(item["path"])
        if path.is_absolute() or ".." in path.parts or path.suffix != ".parquet":
            raise ValueError("invalid manifest path")
        expected[path.as_posix()] = (item["bytes"], item["sha256"])
    actual = {
        path.relative_to(root).as_posix(): (path.stat().st_size, _sha256(path))
        for path in root.rglob("*.parquet")
        if path.is_file() and not path.is_symlink()
    }
    if actual != expected:
        raise RuntimeError("Parquet manifest mismatch")


def prune_parquet(root: Path, *, before: datetime) -> list[Path]:
    if before.utcoffset() != timedelta(0):
        raise ValueError("retention cutoff must be UTC")
    removed: list[Path] = []
    cutoff = before.timestamp()
    for path in sorted(root.rglob("*.parquet")):
        if path.is_file() and not path.is_symlink() and path.stat().st_mtime < cutoff:
            path.unlink()
            removed.append(path)
    return removed


def ntp_synchronized() -> bool:
    try:
        result = subprocess.run(
            ["timedatectl", "show", "--property=NTPSynchronized", "--value"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return result.stdout.strip() == "yes"


def router_healthy(url: str) -> bool:
    parsed = urlsplit(url)
    try:
        address = ipaddress.ip_address(parsed.hostname or "")
    except ValueError:
        return False
    if (
        parsed.scheme != "http"
        or not address.is_loopback
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        return False
    try:
        with urlopen(url, timeout=3) as response:
            if response.status != 200:
                return False
            body = json.loads(response.read(1024))
    except (OSError, ValueError, json.JSONDecodeError):
        return False
    return body == {"ok": True}


def runtime_status_healthy(
    value: object,
    *,
    now: datetime,
    max_age: timedelta = timedelta(seconds=10),
) -> bool:
    if not isinstance(value, dict) or max_age <= timedelta(0):
        return False
    try:
        observed = datetime.fromisoformat(str(value["observed_at"]))
    except (KeyError, TypeError, ValueError):
        return False
    age = now - observed
    return (
        now.utcoffset() == timedelta(0)
        and observed.utcoffset() == timedelta(0)
        and timedelta(0) <= age <= max_age
        and value.get("ok") is True
        and isinstance(value.get("venue_ready"), dict)
    )


def ledger_health_alerts(connection: sqlite3.Connection) -> list[str]:
    alerts: list[str] = []
    tables = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    if "order_events" in tables:
        recent = connection.execute(
            "WITH latest AS ("
            "SELECT order_id, max(seq) AS seq FROM order_events GROUP BY order_id"
            ") SELECT o.status FROM order_events o "
            "JOIN latest l ON l.seq = o.seq "
            "WHERE o.status IN ('REJECTED','FILLED','CANCELED','EXPIRED') "
            "ORDER BY o.seq DESC LIMIT 3"
        ).fetchall()
        if len(recent) == 3 and all(row[0] == "REJECTED" for row in recent):
            alerts.append("ORDER_REJECTIONS_REPEATED")
    if "shadow_runs" in tables:
        recent = connection.execute(
            "SELECT status FROM shadow_runs ORDER BY seq DESC LIMIT 3"
        ).fetchall()
        if len(recent) == 3 and all(row[0] == "NO_DECISION" for row in recent):
            alerts.append("FALLBACK_EXHAUSTED_REPEATED")
    return alerts


def health_report(
    database: Path,
    data_root: Path,
    *,
    minimum_free_bytes: int = 5 * 1024**3,
    ntp_check=ntp_synchronized,
    clock=lambda: datetime.now(UTC),
    max_mt5_age: timedelta = timedelta(seconds=30),
    runtime_status: Path | None = None,
    max_runtime_age: timedelta = timedelta(seconds=10),
    mt5_terminal_id: str | None = None,
    router_url: str | None = None,
    router_check=router_healthy,
) -> dict[str, Any]:
    if max_mt5_age <= timedelta(0) or max_runtime_age <= timedelta(0):
        raise ValueError("health ages must be positive")
    alerts: list[str] = []
    if runtime_status is not None:
        try:
            status = json.loads(runtime_status.read_text())
        except (OSError, json.JSONDecodeError):
            status = None
        if not runtime_status_healthy(status, now=clock(), max_age=max_runtime_age):
            alerts.append("RUNTIME_UNHEALTHY")
    try:
        _quick_check(database)
        connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
        try:
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            alerts.extend(ledger_health_alerts(connection))
            if (
                "mt5_terminal_invalidations" in tables
                and mt5_terminal_id is not None
                and connection.execute(
                    "SELECT 1 FROM mt5_terminal_invalidations WHERE terminal_id = ?",
                    (mt5_terminal_id,),
                ).fetchone()
            ):
                alerts.append("MT5_STATE_INVALID")
            if "mt5_terminal_state" in tables:
                row = (
                    connection.execute(
                        "SELECT observed_at, payload FROM mt5_terminal_state "
                        "WHERE terminal_id = ?",
                        (mt5_terminal_id,),
                    ).fetchone()
                    if mt5_terminal_id is not None
                    else connection.execute(
                        "SELECT observed_at, payload FROM mt5_terminal_state "
                        "ORDER BY observed_at DESC LIMIT 1"
                    ).fetchone()
                )
                if row is None:
                    alerts.append("MT5_STATE_MISSING")
                else:
                    now = clock()
                    observed = datetime.fromisoformat(row[0])
                    age = now - observed
                    if (
                        now.utcoffset() != timedelta(0)
                        or observed.utcoffset() != timedelta(0)
                        or age < timedelta(0)
                        or age > max_mt5_age
                    ):
                        alerts.append("MT5_STATE_STALE")
                    payload = json.loads(row[1])
                    terminal = payload.get("terminal")
                    if not isinstance(terminal, dict):
                        raise ValueError("invalid MT5 terminal payload")
                    if not terminal.get("connected", False):
                        alerts.append("MT5_DISCONNECTED")
                    if not terminal.get("trade_allowed", False) or not terminal.get(
                        "account_trade_allowed", False
                    ):
                        alerts.append("MT5_ALGO_TRADING_DISABLED")
        finally:
            connection.close()
    except (
        OSError,
        sqlite3.Error,
        RuntimeError,
        ValueError,
        TypeError,
        json.JSONDecodeError,
    ):
        alerts.append("DATABASE_ERROR")
    try:
        usage = shutil.disk_usage(data_root if data_root.exists() else data_root.parent)
    except OSError:
        alerts.append("DISK_ERROR")
        free_bytes = 0
    else:
        free_bytes = usage.free
        if free_bytes < minimum_free_bytes:
            alerts.append("DISK_LOW")
    if not ntp_check():
        alerts.append("NTP_UNSYNCED")
    if router_url is not None and not router_check(router_url):
        alerts.append("ROUTER_UNAVAILABLE")
    return {
        "ok": not alerts,
        "alerts": alerts,
        "disk_free_bytes": free_bytes,
        "minimum_free_bytes": minimum_free_bytes,
    }


def _utc(value: str) -> datetime:
    timestamp = datetime.fromisoformat(value)
    if timestamp.utcoffset() != timedelta(0):
        raise argparse.ArgumentTypeError("timestamp must be UTC RFC3339")
    return timestamp


def main() -> None:
    parser = argparse.ArgumentParser(description="ALMA production operations")
    sub = parser.add_subparsers(dest="action", required=True)
    backup = sub.add_parser("backup")
    backup.add_argument("--database", type=Path, default=Path("var/alma.db"))
    backup.add_argument("--output", type=Path, default=Path("var/backups"))
    backup.add_argument("--retain", type=int, default=14)
    restore = sub.add_parser("restore")
    restore.add_argument("backup", type=Path)
    restore.add_argument("destination", type=Path)
    manifest = sub.add_parser("manifest")
    manifest.add_argument("--root", type=Path, default=Path("var/data"))
    manifest.add_argument(
        "--output", type=Path, default=Path("var/parquet-manifest.json")
    )
    verify = sub.add_parser("verify-manifest")
    verify.add_argument("--root", type=Path, default=Path("var/data"))
    verify.add_argument("manifest", type=Path)
    prune = sub.add_parser("prune")
    prune.add_argument("--root", type=Path, default=Path("var/data"))
    prune.add_argument("--before", type=_utc, required=True)
    health = sub.add_parser("health")
    health.add_argument("--database", type=Path, default=Path("var/alma.db"))
    health.add_argument("--data-root", type=Path, default=Path("var"))
    health.add_argument("--router-url")
    health.add_argument("--runtime-status", type=Path)
    health.add_argument("--mt5-terminal-id")
    args = parser.parse_args()
    now = datetime.now(UTC)
    if args.action == "backup":
        print(create_backup(args.database, args.output, now=now, retain=args.retain))
    elif args.action == "restore":
        print(restore_backup(args.backup, args.destination))
    elif args.action == "manifest":
        print(json.dumps(write_parquet_manifest(args.root, args.output, now=now)))
    elif args.action == "verify-manifest":
        verify_parquet_manifest(args.root, args.manifest)
        print("ok")
    elif args.action == "prune":
        print(
            json.dumps(
                [str(path) for path in prune_parquet(args.root, before=args.before)]
            )
        )
    else:
        report = health_report(
            args.database,
            args.data_root,
            router_url=args.router_url,
            runtime_status=args.runtime_status,
            mt5_terminal_id=args.mt5_terminal_id,
        )
        print(json.dumps(report, sort_keys=True))
        raise SystemExit(0 if report["ok"] else 1)


if __name__ == "__main__":
    main()
