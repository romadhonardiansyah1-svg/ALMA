import argparse
import asyncio
import hashlib
import hmac
import json
import os
import sqlite3
import tempfile
import time
import urllib.parse
import urllib.request
from collections.abc import Mapping
from pathlib import Path

from nautilus_trader.adapters.binance import (
    BINANCE,
    BinanceAccountType,
    BinanceDataClientConfig,
    BinanceExecClientConfig,
    BinanceLiveDataClientFactory,
    BinanceLiveExecClientFactory,
)
from nautilus_trader.adapters.binance.common.enums import BinanceEnvironment
from nautilus_trader.config import (
    InstrumentProviderConfig,
    LoggingConfig,
    TradingNodeConfig,
)
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.identifiers import InstrumentId, Venue

from alma.binance_data import BTCUSDT_USDM
from alma.binance_native_execution import BinanceNativeVenue
from alma.binance_testnet_secrets import load_testnet_credentials
from alma.ledger import open_ledger

_KEY = "BINANCE_FUTURES_TESTNET_API_KEY"
_SECRET = "BINANCE_FUTURES_TESTNET_API_SECRET"
_HTTP_BASE = "https://demo-fapi.binance.com"
_CREDENTIALS = {
    BinanceEnvironment.LIVE: ("BINANCE_API_KEY", "BINANCE_API_SECRET"),
    BinanceEnvironment.TESTNET: (_KEY, _SECRET),
    BinanceEnvironment.DEMO: ("BINANCE_DEMO_API_KEY", "BINANCE_DEMO_API_SECRET"),
}


def binance_testnet_open_algo_orders(
    symbol: str,
    environ: Mapping[str, str] | None = None,
) -> tuple[dict[str, object], ...]:
    if not symbol or not symbol.isalnum():
        raise ValueError("invalid Binance symbol")
    if environ is None and not testnet_credentials_available():
        load_testnet_credentials()
    credentials = os.environ if environ is None else environ
    if not testnet_credentials_available(credentials):
        raise RuntimeError(
            "Binance Futures Testnet credentials are not installed locally"
        )
    params = urllib.parse.urlencode(
        {
            "symbol": symbol,
            "timestamp": int(time.time() * 1000),
            "recvWindow": 5000,
        }
    )
    signature = hmac.new(
        credentials[_SECRET].encode(), params.encode(), hashlib.sha256
    ).hexdigest()
    request = urllib.request.Request(
        f"{_HTTP_BASE}/fapi/v1/openAlgoOrders?{params}&signature={signature}",
        headers={"X-MBX-APIKEY": credentials[_KEY]},
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        payload = json.load(response)
    if not isinstance(payload, list) or not all(
        isinstance(item, dict) for item in payload
    ):
        raise RuntimeError("invalid Binance algo-order response")
    return tuple(payload)


def testnet_credentials_available(
    environ: Mapping[str, str] | None = None,
) -> bool:
    return credentials_available(BinanceEnvironment.TESTNET, environ)


def credentials_available(
    environment: BinanceEnvironment,
    environ: Mapping[str, str] | None = None,
) -> bool:
    environ = os.environ if environ is None else environ
    key, secret = _CREDENTIALS[environment]
    return bool(environ.get(key, "").strip() and environ.get(secret, "").strip())


def binance_testnet_node_config(
    environ: Mapping[str, str] | None = None,
    *,
    environment: BinanceEnvironment = BinanceEnvironment.TESTNET,
    instrument_id: InstrumentId = BTCUSDT_USDM,
) -> TradingNodeConfig:
    if (
        environment is BinanceEnvironment.TESTNET
        and environ is None
        and not credentials_available(environment)
    ):
        try:
            load_testnet_credentials()
        except (OSError, ValueError) as error:
            raise RuntimeError(
                "Binance Futures credentials are not installed locally"
            ) from error
        environ = os.environ
    if not credentials_available(environment, environ):
        raise RuntimeError("Binance Futures credentials are not installed locally")
    instruments = InstrumentProviderConfig(load_ids=frozenset({instrument_id}))
    common = {
        "account_type": BinanceAccountType.USDT_FUTURES,
        "environment": environment,
        "instrument_provider": instruments,
    }
    return TradingNodeConfig(
        logging=LoggingConfig(log_level="WARNING", log_colors=False),
        data_clients={BINANCE: BinanceDataClientConfig(**common)},
        exec_clients={
            BINANCE: BinanceExecClientConfig(
                **common,
                use_gtd=True,
                use_reduce_only=True,
            )
        },
    )


def binance_testnet_node(
    environ: Mapping[str, str] | None = None,
    *,
    environment: BinanceEnvironment = BinanceEnvironment.TESTNET,
    instrument_id: InstrumentId = BTCUSDT_USDM,
    data_client_factory: object = BinanceLiveDataClientFactory,
) -> TradingNode:
    node = TradingNode(
        config=binance_testnet_node_config(
            environ, environment=environment, instrument_id=instrument_id
        )
    )
    node.add_data_client_factory(BINANCE, data_client_factory)
    node.add_exec_client_factory(BINANCE, BinanceLiveExecClientFactory)
    return node


def binance_testnet_runtime(
    connection: sqlite3.Connection,
    environ: Mapping[str, str] | None = None,
    instrument_id: InstrumentId = BTCUSDT_USDM,
    environment: BinanceEnvironment = BinanceEnvironment.TESTNET,
    expected_account_id: str | None = None,
    data_client_factory: object = BinanceLiveDataClientFactory,
) -> tuple[TradingNode, BinanceNativeVenue]:
    source = os.environ if environ is None else environ
    expected_account_id = expected_account_id or source.get(
        "ALMA_BINANCE_ACCOUNT_ID", ""
    )
    if not expected_account_id:
        raise RuntimeError("Binance account identity is not configured")
    # ponytail: systemd doesn't load binance-testnet.ini as EnvironmentFile,
    # so ensure credentials are always loaded before building the node
    if environment is BinanceEnvironment.TESTNET and not credentials_available(environment, source):
        import sys
        print("[ALMA] Loading testnet credentials from .ini...", file=sys.stderr, flush=True)
        load_testnet_credentials()
        print(f"[ALMA] Credentials loaded: KEY={os.environ.get('BINANCE_FUTURES_TESTNET_API_KEY','')[:8]}...", file=sys.stderr, flush=True)
    node = binance_testnet_node(
        environ,
        environment=environment,
        instrument_id=instrument_id,
        data_client_factory=data_client_factory,
    )
    try:
        venue = BinanceNativeVenue(
            connection,
            instrument_id,
            connected=node.kernel.exec_engine.check_connected,
            expected_account_id=expected_account_id,
        )
        node.trader.add_strategy(venue)
        return node, venue
    except BaseException:
        node.dispose()
        raise


def binance_testnet_read_only_smoke(
    connection: sqlite3.Connection,
    timeout: float,
    environ: Mapping[str, str] | None = None,
) -> dict[str, object] | None:
    if timeout <= 0:
        raise ValueError("timeout must be positive")
    node = None

    async def inspect_cache() -> dict[str, object] | None:
        nonlocal node
        node, venue = binance_testnet_runtime(connection, environ)
        node.build()
        run_task = asyncio.create_task(node.run_async())
        deadline = asyncio.timeout(timeout)
        try:
            async with deadline:
                await asyncio.sleep(0)
                while True:
                    if run_task.done():
                        await run_task
                    account = node.cache.account_for_venue(Venue("BINANCE"))
                    instrument = node.cache.instrument(venue.instrument_id)
                    if (
                        node.kernel.data_engine.check_connected()
                        and node.kernel.exec_engine.check_connected()
                        and account is not None
                        and instrument is not None
                    ):
                        balances = tuple(
                            sorted(
                                (
                                    currency.code,
                                    str(balance.total),
                                    str(balance.free),
                                    str(balance.locked),
                                )
                                for currency, balance in account.balances().items()
                            )
                        )
                        algo_orders = await asyncio.to_thread(
                            binance_testnet_open_algo_orders,
                            venue.instrument_id.symbol.value.replace("-PERP", ""),
                            environ,
                        )
                        return {
                            "environment": BinanceEnvironment.TESTNET.name,
                            "account_type": BinanceAccountType.USDT_FUTURES.name,
                            "account_id": str(account.id),
                            "instrument_id": str(instrument.id),
                            "balances": balances,
                            "positions": len(
                                node.cache.positions(
                                    venue=Venue("BINANCE"),
                                    instrument_id=venue.instrument_id,
                                )
                            ),
                            "positions_open": len(
                                node.cache.positions_open(
                                    venue=Venue("BINANCE"),
                                    instrument_id=venue.instrument_id,
                                )
                            ),
                            "orders": len(
                                node.cache.orders(
                                    venue=Venue("BINANCE"),
                                    instrument_id=venue.instrument_id,
                                )
                            ),
                            "orders_open": len(
                                node.cache.orders_open(
                                    venue=Venue("BINANCE"),
                                    instrument_id=venue.instrument_id,
                                )
                            ),
                            "algo_orders_open": len(algo_orders),
                        }
                    await asyncio.sleep(0.01)
        except TimeoutError:
            if deadline.expired():
                return None
            raise
        finally:
            run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)
            await node.stop_async()

    try:
        with asyncio.Runner() as runner:
            return runner.run(inspect_cache())
    finally:
        if node is not None:
            node.dispose()


def write_read_only_evidence(
    evidence_path: str | Path,
    timeout: float,
) -> dict[str, object]:
    path = Path(evidence_path)
    error: str | None = None
    observed: dict[str, object] | None = None
    try:
        with tempfile.TemporaryDirectory(prefix="alma-testnet-smoke-") as directory:
            connection = open_ledger(Path(directory) / "ledger.db")
            try:
                observed = binance_testnet_read_only_smoke(connection, timeout)
            finally:
                connection.close()
    except (OSError, RuntimeError, TimeoutError, ValueError) as caught:
        error = type(caught).__name__

    evidence: dict[str, object] = {
        "passed": observed is not None and error is None,
        "read_only": True,
        "mutation_attempted": False,
        "observed": observed,
        "error": error or ("Timeout" if observed is None else None),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        path.chmod(0o600)
    finally:
        temporary.unlink(missing_ok=True)
    return evidence


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run non-mutating Binance Futures Testnet account smoke"
    )
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    evidence = write_read_only_evidence(args.evidence, args.timeout)
    print(json.dumps(evidence, sort_keys=True))
    return 0 if evidence["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
