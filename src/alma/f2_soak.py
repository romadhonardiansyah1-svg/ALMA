import argparse
import asyncio
import json
import time
from collections.abc import Callable
from pathlib import Path

from alma.binance_data import public_usdm_state_node
from alma.market_recording import ParquetRecorder, replay
from alma.market_state import MarketState


async def run_soak(
    *,
    duration_seconds: float,
    data_root: str | Path,
    evidence_path: str | Path,
    node_factory=public_usdm_state_node,
    poll_seconds: float = 1.0,
    clock_ns: Callable[[], int] = time.time_ns,
) -> dict[str, object]:
    if duration_seconds <= 0:
        raise ValueError("duration_seconds must be positive")
    if poll_seconds <= 0:
        raise ValueError("poll_seconds must be positive")

    root = Path(data_root)
    evidence_file = Path(evidence_path)
    state = MarketState("BINANCE", "BTCUSDT-PERP")
    recorder = ParquetRecorder(root, state.venue, state.symbol)
    node, _, _ = node_factory(state, recorder)
    node.build()
    task = asyncio.create_task(node.run_async())
    started = time.monotonic()
    max_age_ms = 0
    error: str | None = None

    try:
        deadline = started + duration_seconds
        while time.monotonic() < deadline:
            if task.done():
                await task
            try:
                snapshot = state.snapshot(clock_ns())
                max_age_ms = max(max_age_ms, snapshot.market_age_ms)
            except ValueError:
                pass
            await asyncio.sleep(min(poll_seconds, max(0, deadline - time.monotonic())))
    except (ConnectionError, OSError, RuntimeError, TimeoutError, ValueError) as caught:
        error = f"{type(caught).__name__}: {caught}"
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await node.stop_async()
        recorder.flush()

    duration = time.monotonic() - started
    replay_match = False
    book_valid = False
    state_id: str | None = None
    replay_state_id: str | None = None
    try:
        live = state.snapshot(max(clock_ns(), 0))
        book_valid = live.book_valid
        state_id = live.state_id
        replayed, last_ts = replay(root, state.venue, state.symbol)
        replay_snapshot = replayed.snapshot(last_ts)
        live_at_replay_end = state.snapshot(last_ts)
        replay_state_id = replay_snapshot.state_id
        replay_match = replay_state_id == live_at_replay_end.state_id
    except (ValueError, FileNotFoundError) as caught:
        if error is None:
            error = f"{type(caught).__name__}: {caught}"

    disk_bytes = sum(
        path.stat().st_size for path in root.rglob("*.parquet") if path.is_file()
    )
    passed = (
        error is None
        and state.metrics.event_count > 0
        and book_valid
        and replay_match
        and state.metrics.p95_latency_ms <= 50
    )
    evidence: dict[str, object] = {
        "passed": passed,
        "requested_duration_seconds": duration_seconds,
        "actual_duration_seconds": round(duration, 6),
        "full_24h_gate": duration_seconds >= 86_400,
        "event_count": state.metrics.event_count,
        "gap_count": state.metrics.gap_count,
        "reconnect_count": state.metrics.reconnect_count,
        "book_valid": book_valid,
        "max_market_age_ms": max_age_ms,
        "p95_processing_latency_ms": state.metrics.p95_latency_ms,
        "disk_bytes": disk_bytes,
        "state_id": state_id,
        "replay_state_id": replay_state_id,
        "replay_hash_match": replay_match,
        "error": error,
    }
    evidence_file.parent.mkdir(parents=True, exist_ok=True)
    evidence_file.write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return evidence


def main() -> int:
    parser = argparse.ArgumentParser(description="Run bounded ALMA F2 public soak")
    parser.add_argument("--duration-seconds", type=float, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    evidence = asyncio.run(
        run_soak(
            duration_seconds=args.duration_seconds,
            data_root=args.data_root,
            evidence_path=args.evidence,
        )
    )
    print(json.dumps(evidence, sort_keys=True))
    return 0 if evidence["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
