import asyncio
import json
from decimal import Decimal
from types import SimpleNamespace

from alma.f2_soak import run_soak
from alma.market_recording import MarketEvent


def test_soak_writes_evidence_and_verifies_replay(tmp_path) -> None:
    async def run() -> dict[str, object]:
        class FakeNode:
            def build(self) -> None:
                pass

            async def run_async(self) -> None:
                await asyncio.Event().wait()

            async def stop_async(self) -> None:
                pass

            def dispose(self) -> None:
                pass

        def factory(state, recorder):
            async def feed() -> None:
                await asyncio.sleep(0)
                for event in (
                    MarketEvent.quote(
                        1,
                        "BINANCE",
                        "BTCUSDT-PERP",
                        Decimal(100),
                        Decimal(101),
                        Decimal(1),
                        Decimal(1),
                    ),
                    MarketEvent.trade(
                        2,
                        "BINANCE",
                        "BTCUSDT-PERP",
                        Decimal("100.5"),
                        Decimal(1),
                        1,
                    ),
                    MarketEvent.mark(
                        2,
                        "BINANCE",
                        "BTCUSDT-PERP",
                        Decimal("100.4"),
                    ),
                    MarketEvent.funding(
                        2,
                        "BINANCE",
                        "BTCUSDT-PERP",
                        Decimal("0.0001"),
                    ),
                    MarketEvent.book_snapshot(
                        2,
                        "BINANCE",
                        "BTCUSDT-PERP",
                        100,
                    ),
                    MarketEvent.book_delta(
                        2,
                        "BINANCE",
                        "BTCUSDT-PERP",
                        first=99,
                        final=105,
                        previous_final=98,
                    ),
                ):
                    recorder.record(event)
                    event.apply(state)
                    state.metrics.observe_latency(1_000_000)

            asyncio.create_task(feed())
            return FakeNode(), SimpleNamespace(), SimpleNamespace()

        return await run_soak(
            duration_seconds=0.03,
            data_root=tmp_path / "data",
            evidence_path=tmp_path / "evidence.json",
            node_factory=factory,
            poll_seconds=0.005,
            clock_ns=lambda: 2,
        )

    evidence = asyncio.run(run())

    assert evidence["passed"] is True
    assert evidence["book_valid"] is True
    assert evidence["replay_hash_match"] is True
    assert evidence["p95_processing_latency_ms"] == 1.0
    assert json.loads((tmp_path / "evidence.json").read_text()) == evidence
    assert evidence["full_24h_gate"] is False


def test_soak_fails_when_depth_is_unresolved(tmp_path) -> None:
    async def run() -> dict[str, object]:
        class FakeNode:
            def build(self) -> None:
                pass

            async def run_async(self) -> None:
                await asyncio.Event().wait()

            async def stop_async(self) -> None:
                pass

            def dispose(self) -> None:
                pass

        def factory(state, recorder):
            event = MarketEvent.trade(
                1,
                "BINANCE",
                "BTCUSDT-PERP",
                Decimal(100),
                Decimal(1),
                0,
            )
            recorder.record(event)
            event.apply(state)
            return FakeNode(), None, None

        return await run_soak(
            duration_seconds=0.01,
            data_root=tmp_path / "data",
            evidence_path=tmp_path / "evidence.json",
            node_factory=factory,
            poll_seconds=0.001,
            clock_ns=lambda: 1,
        )

    evidence = asyncio.run(run())
    assert evidence["passed"] is False
    assert evidence["book_valid"] is False
