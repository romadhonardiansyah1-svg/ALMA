import argparse
import asyncio
import contextlib
import json
import logging
import os
import secrets
from pathlib import Path

from aiohttp import web

from alma.ledger import open_ledger
from alma.mt5_bridge import (
    MT5BridgeRejected,
    MT5BridgeStore,
    MT5FileBridge,
    create_mt5_bridge_app,
    read_bridge_secret,
)

DEFAULT_SECRET = Path.home() / ".config/alma/mt5-bridge.secret"
DEFAULT_IPC_DIRECTORY = (
    Path.home()
    / ".wine-alma/drive_c/users/root/AppData/Roaming/MetaQuotes/Terminal/Common/Files/ALMA/mt5-1"
)
LOGGER = logging.getLogger(__name__)


def ensure_bridge_secret(path: Path = DEFAULT_SECRET) -> str:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        return read_bridge_secret(path)
    except FileNotFoundError:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(secrets.token_urlsafe(48))
        return read_bridge_secret(path)


def main() -> None:
    parser = argparse.ArgumentParser(description="ALMA MT5 localhost bridge")
    parser.add_argument("--database", type=Path, default=Path("var/alma.db"))
    parser.add_argument("--secret", type=Path, default=DEFAULT_SECRET)
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--ipc-directory", type=Path, default=DEFAULT_IPC_DIRECTORY)
    parser.add_argument("--terminal-id", default="mt5-1")
    parser.add_argument(
        "--expected-account-mode",
        choices=("DEMO", "REAL"),
        default=os.environ.get("ALMA_MT5_ACCOUNT_MODE"),
        required=not os.environ.get("ALMA_MT5_ACCOUNT_MODE"),
    )
    parser.add_argument(
        "--expected-position-mode",
        choices=("AUTO", "HEDGING", "NETTING"),
        default=os.environ.get("ALMA_MT5_POSITION_MODE"),
        required=not os.environ.get("ALMA_MT5_POSITION_MODE"),
    )
    parser.add_argument(
        "--expected-login",
        default=os.environ.get("ALMA_MT5_LOGIN"),
        required=not os.environ.get("ALMA_MT5_LOGIN"),
    )
    parser.add_argument(
        "--expected-server",
        default=os.environ.get("ALMA_MT5_SERVER"),
        required=not os.environ.get("ALMA_MT5_SERVER"),
    )
    parser.add_argument(
        "--expected-symbol",
        default=os.environ.get("ALMA_MT5_SYMBOL"),
        required=not os.environ.get("ALMA_MT5_SYMBOL"),
    )
    args = parser.parse_args()
    args.database.parent.mkdir(parents=True, exist_ok=True)
    connection = open_ledger(args.database)
    store = MT5BridgeStore(
        connection,
        expected_account_mode=args.expected_account_mode,
        expected_position_mode=args.expected_position_mode,
        expected_login=args.expected_login,
        expected_server=args.expected_server,
        expected_symbol=args.expected_symbol,
    )
    app = create_mt5_bridge_app(store, ensure_bridge_secret(args.secret))
    ipc = MT5FileBridge(store, args.ipc_directory, args.terminal_id)

    async def file_bridge(_: web.Application):
        ipc.prepare()

        async def run() -> None:
            while True:
                try:
                    ipc.tick()
                except (json.JSONDecodeError, MT5BridgeRejected, OSError, ValueError):
                    LOGGER.exception("MT5 file bridge tick rejected")
                    await asyncio.sleep(1)
                else:
                    await asyncio.sleep(0.1)

        task = asyncio.create_task(run())
        yield
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    app.cleanup_ctx.append(file_bridge)

    async def close_database(_: web.Application) -> None:
        connection.close()

    app.on_cleanup.append(close_database)
    web.run_app(app, host="127.0.0.1", port=args.port, print=None)


if __name__ == "__main__":
    main()
