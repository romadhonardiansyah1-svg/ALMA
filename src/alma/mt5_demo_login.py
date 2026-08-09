import argparse
import getpass
import os
import subprocess
import tempfile
import time
from pathlib import Path

WINE = Path("/opt/wine-stable/bin/wine")
TERMINAL = Path("/root/.wine-alma/drive_c/Program Files/MetaTrader 5/terminal64.exe")
WINEPREFIX = Path("/root/.wine-alma")
ACCOUNT_CONFIG = Path.home() / ".config/alma/mt5-account.conf"
ACCOUNT_KEYS = {
    "ALMA_MT5_ACCOUNT_MODE",
    "ALMA_MT5_POSITION_MODE",
    "ALMA_MT5_LOGIN",
    "ALMA_MT5_SERVER",
    "ALMA_MT5_SYMBOL",
    "ALMA_MT5_TERMINAL_ID",
}


def load_account_config(path: Path = ACCOUNT_CONFIG) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator or key not in ACCOUNT_KEYS or key in values or not value:
            raise ValueError("invalid MT5 account config")
        values[key] = value
    if values.keys() != ACCOUNT_KEYS:
        raise ValueError("incomplete MT5 account config")
    _validate_account(values)
    return values


def _validate_account(values: dict[str, str]) -> None:
    if values.keys() != ACCOUNT_KEYS:
        raise ValueError("incomplete MT5 account config")
    if values["ALMA_MT5_ACCOUNT_MODE"] not in {"DEMO", "REAL"}:
        raise ValueError("MT5 account mode must be DEMO or REAL")
    if values["ALMA_MT5_POSITION_MODE"] not in {"AUTO", "HEDGING", "NETTING"}:
        raise ValueError("MT5 position mode must be AUTO, HEDGING, or NETTING")
    _config(
        values["ALMA_MT5_LOGIN"],
        values["ALMA_MT5_SERVER"],
        "validation-only",
    )
    symbol = values["ALMA_MT5_SYMBOL"]
    if len(symbol) > 64 or any(
        not (character.isalnum() or character in "._-") for character in symbol
    ):
        raise ValueError("invalid MT5 symbol")


def write_startup_config(values: dict[str, str], path: Path) -> None:
    _validate_account(values)
    body = (
        "[Common]\r\n"
        f"Login={values['ALMA_MT5_LOGIN']}\r\n"
        f"Server={values['ALMA_MT5_SERVER']}\r\n"
        "KeepPrivate=1\r\nNewsEnable=0\r\nCertInstall=0\r\n\r\n"
        "[Experts]\r\nAllowLiveTrading=1\r\nAllowDllImport=0\r\n"
        "Enabled=1\r\nAccount=0\r\nProfile=0\r\n\r\n"
        "[StartUp]\r\n"
        f"Symbol={values['ALMA_MT5_SYMBOL']}\r\n"
        "Period=H1\r\nExpert=AlmaBridge\r\n"
        "ExpertParameters=alma-bridge.set\r\n"
    ).encode()
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        path.chmod(0o600)
    finally:
        temporary.unlink(missing_ok=True)


def _config(login: str, server: str, password: str) -> bytes:
    if not login.isdigit() or len(login) > 32:
        raise ValueError("MT5 login must contain digits only")
    if (
        not server
        or len(server) > 128
        or any(
            character
            not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
            for character in server
        )
    ):
        raise ValueError("invalid MT5 server")
    if (
        not password
        or len(password) > 256
        or any(character in password for character in "\r\n\0")
    ):
        raise ValueError("invalid MT5 password")
    return (
        "[Common]\r\n"
        f"Login={login}\r\n"
        f"Password={password}\r\n"
        f"Server={server}\r\n"
        "KeepPrivate=1\r\n"
        "NewsEnable=0\r\n"
    ).encode()


def start_terminal(
    login: str,
    server: str,
    password: str,
    *,
    cleanup_delay: float = 15,
) -> int:
    if cleanup_delay <= 0:
        raise ValueError("cleanup delay must be positive")
    if not WINE.is_file() or not TERMINAL.is_file() or not Path("/dev/shm").is_dir():
        raise RuntimeError("MT5/Wine or tmpfs is unavailable")

    descriptor, name = tempfile.mkstemp(
        prefix="alma-mt5-", suffix=".ini", dir="/dev/shm"
    )
    config = Path(name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(_config(login, server, password))
            handle.flush()
            os.fsync(handle.fileno())
        password = ""  # ponytail: drop the Python reference before starting Wine.
        translated = subprocess.run(
            [str(WINE), "winepath", "-w", str(config)],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
            env={
                **os.environ,
                "WINEPREFIX": str(WINEPREFIX),
                "WINEARCH": "win64",
                "WINEDLLOVERRIDES": "mscoree,mshtml=",
            },
        ).stdout.strip()
        process = subprocess.Popen(
            [
                "/usr/bin/xvfb-run",
                "-a",
                str(WINE),
                str(TERMINAL),
                "/portable",
                f"/config:{translated}",
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            env={
                **os.environ,
                "WINEPREFIX": str(WINEPREFIX),
                "WINEARCH": "win64",
                "WINEDLLOVERRIDES": "mscoree,mshtml=",
            },
        )
        time.sleep(cleanup_delay)
        if process.poll() is not None:
            raise RuntimeError("MT5 exited during login bootstrap")
        return process.pid
    finally:
        config.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Start configured MT5 login without password argv"
    )
    parser.add_argument("--account-config", type=Path, default=ACCOUNT_CONFIG)
    parser.add_argument("--cleanup-delay", type=float, default=15)
    parser.add_argument("--write-startup", type=Path)
    args = parser.parse_args()
    account = load_account_config(args.account_config)
    if args.write_startup is not None:
        write_startup_config(account, args.write_startup)
        return
    password = getpass.getpass(
        f"MT5 {account['ALMA_MT5_ACCOUNT_MODE']} trading password: "
    )
    pid = start_terminal(
        account["ALMA_MT5_LOGIN"],
        account["ALMA_MT5_SERVER"],
        password,
        cleanup_delay=args.cleanup_delay,
    )
    print(
        f"MT5 started for configured {account['ALMA_MT5_ACCOUNT_MODE']} identity "
        f"(pid={pid}); password tmpfs removed"
    )


if __name__ == "__main__":
    main()
