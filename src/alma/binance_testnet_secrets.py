import configparser
import getpass
import os
import stat
from pathlib import Path

SECRET_FILE = Path.home() / ".config/alma/binance-testnet.ini"
_SECTION = "binance_futures_testnet"
_KEY = "BINANCE_FUTURES_TESTNET_API_KEY"
_SECRET = "BINANCE_FUTURES_TESTNET_API_SECRET"


def read_testnet_credentials(path: Path = SECRET_FILE) -> dict[str, str]:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as error:
        raise PermissionError(
            "Testnet credential file must be a regular file owned by this user"
        ) from error
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
            raise PermissionError(
                "Testnet credential file must be a regular file owned by this user"
            )
        if stat.S_IMODE(info.st_mode) != 0o600:
            raise PermissionError("Testnet credential file permissions must be 0600")

        parser = configparser.ConfigParser(interpolation=None)
        with os.fdopen(descriptor, encoding="utf-8") as handle:
            descriptor = -1
            try:
                parser.read_file(handle)
            except configparser.Error as error:
                raise ValueError("Testnet credential file is malformed") from error
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    try:
        key = parser[_SECTION]["api_key"].strip()
        secret = parser[_SECTION]["api_secret"].strip()
    except KeyError as error:
        raise ValueError("Testnet credential file is incomplete") from error
    if not key or not secret:
        raise ValueError("Testnet credential file is incomplete")
    return {_KEY: key, _SECRET: secret}


def load_testnet_credentials(path: Path = SECRET_FILE) -> None:
    credentials = read_testnet_credentials(path)
    os.environ.update(credentials)


def write_testnet_credentials(
    api_key: str, api_secret: str, path: Path = SECRET_FILE
) -> None:
    api_key = api_key.strip()
    api_secret = api_secret.strip()
    if not api_key or not api_secret:
        raise ValueError("API key and secret are required")

    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.parent.chmod(0o700)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(
                f"[{_SECTION}]\napi_key = {api_key}\napi_secret = {api_secret}\n"
            )
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        path.chmod(0o600)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> None:
    api_key = getpass.getpass("Binance Futures Testnet API key: ")
    api_secret = getpass.getpass("Binance Futures Testnet API secret: ")
    write_testnet_credentials(api_key, api_secret)
    print(f"Installed Binance Futures Testnet credentials at {SECRET_FILE} (0600)")


if __name__ == "__main__":
    main()
