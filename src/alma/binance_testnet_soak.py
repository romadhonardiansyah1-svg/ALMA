import argparse
import hashlib
import hmac
import json
import os
import time
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, TypedDict

from alma.binance_testnet_secrets import read_testnet_credentials

BASE_URL = "https://demo-fapi.binance.com"
SYMBOL = "BTCUSDT"


class Observation(TypedDict):
    nonzero_balance_assets: int
    position_amount: str
    orders_open: int
    algo_orders_open: int


def _signed_get(
    path: str,
    credentials: dict[str, str],
    *,
    clock_ms=lambda: int(time.time() * 1000),
) -> list[dict[str, Any]]:
    params = urllib.parse.urlencode(
        {"symbol": SYMBOL, "timestamp": clock_ms(), "recvWindow": 5000}
    )
    secret = credentials["BINANCE_FUTURES_TESTNET_API_SECRET"].encode()
    signature = hmac.new(secret, params.encode(), hashlib.sha256).hexdigest()
    request = urllib.request.Request(
        f"{BASE_URL}{path}?{params}&signature={signature}",
        headers={"X-MBX-APIKEY": credentials["BINANCE_FUTURES_TESTNET_API_KEY"]},
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        payload = json.load(response)
    if not isinstance(payload, list) or not all(
        isinstance(item, dict) for item in payload
    ):
        raise RuntimeError("invalid Binance Testnet response")
    return payload


def observe(credentials: dict[str, str]) -> Observation:
    balances = _signed_get("/fapi/v2/balance", credentials)
    positions = _signed_get("/fapi/v2/positionRisk", credentials)
    orders = _signed_get("/fapi/v1/openOrders", credentials)
    algo_orders = _signed_get("/fapi/v1/openAlgoOrders", credentials)
    return {
        "nonzero_balance_assets": sum(
            Decimal(item["balance"]) != 0
            for item in balances  # type: ignore[index]
        ),
        "position_amount": str(
            sum((Decimal(item["positionAmt"]) for item in positions), Decimal(0))  # type: ignore[index]
        ),
        "orders_open": len(orders),
        "algo_orders_open": len(algo_orders),
    }


def validate_observation(observation: Observation) -> None:
    if observation["nonzero_balance_assets"] <= 0:
        raise RuntimeError("TESTNET_BALANCE_MISSING")


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


def run(duration_seconds: float, interval_seconds: float, evidence_path: Path) -> None:
    if duration_seconds < 0 or interval_seconds <= 0:
        raise ValueError("duration must be non-negative and interval must be positive")
    credentials = read_testnet_credentials()
    started_at = datetime.now(UTC)
    deadline = None if duration_seconds == 0 else time.monotonic() + duration_seconds
    samples = 0
    last: Observation | None = None
    while True:
        last = observe(credentials)
        validate_observation(last)
        samples += 1
        now = time.monotonic()
        progress = {
            "passed": False,
            "in_progress": True,
            "read_only": True,
            "mutation_attempted": False,
            "started_at": started_at.isoformat(),
            "requested_duration_seconds": duration_seconds or None,
            "samples": samples,
            "last_observation": last,
        }
        _write(evidence_path, progress)
        if deadline is None:
            time.sleep(interval_seconds)
            continue
        if now >= deadline:
            break
        time.sleep(min(interval_seconds, deadline - now))
    completed_at = datetime.now(UTC)
    _write(
        evidence_path,
        {
            **progress,
            "passed": True,
            "in_progress": False,
            "completed_at": completed_at.isoformat(),
            "duration_seconds": (completed_at - started_at).total_seconds(),
        },
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run continuous read-only Binance Testnet account monitor"
    )
    parser.add_argument("--duration", type=float, default=0)
    parser.add_argument("--interval", type=float, default=60)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    run(args.duration, args.interval, args.evidence)


if __name__ == "__main__":
    main()
