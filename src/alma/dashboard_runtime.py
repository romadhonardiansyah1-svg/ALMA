import argparse
import json
from pathlib import Path

from aiohttp import web

from alma.dashboard import create_dashboard_app
from alma.ledger import open_ledger
from alma.mt5_runtime import ensure_bridge_secret

DEFAULT_SECRET = Path.home() / ".config/alma/dashboard.secret"


def main() -> None:
    parser = argparse.ArgumentParser(description="ALMA private operations dashboard")
    parser.add_argument("--database", type=Path, default=Path("var/alma.db"))
    parser.add_argument("--secret", type=Path, default=DEFAULT_SECRET)
    parser.add_argument(
        "--runtime-status", type=Path, default=Path("var/runtime-status.json")
    )
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--mt5-terminal-id")
    args = parser.parse_args()
    args.database.parent.mkdir(parents=True, exist_ok=True)
    connection = open_ledger(args.database)

    def runtime_status() -> dict[str, object]:
        try:
            value = json.loads(args.runtime_status.read_text())
        except (OSError, json.JSONDecodeError):
            return {}
        return value if isinstance(value, dict) else {}

    app = create_dashboard_app(
        connection,
        secret=ensure_bridge_secret(args.secret),
        runtime_provider=runtime_status,
        mt5_terminal_id=args.mt5_terminal_id,
    )

    async def close_database(_: web.Application) -> None:
        connection.close()

    app.on_cleanup.append(close_database)
    web.run_app(app, host="127.0.0.1", port=args.port, print=None)


if __name__ == "__main__":
    main()
