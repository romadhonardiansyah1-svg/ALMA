import os
from pathlib import Path

import pytest

from alma.binance_testnet_secrets import (
    load_testnet_credentials,
    read_testnet_credentials,
    write_testnet_credentials,
)

_KEY = "BINANCE_FUTURES_TESTNET_API_KEY"
_SECRET = "BINANCE_FUTURES_TESTNET_API_SECRET"


def test_secret_file_round_trip_is_private_and_loads_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "binance-testnet.ini"
    write_testnet_credentials("test-key", "test-secret", path)

    assert path.stat().st_mode & 0o777 == 0o600
    assert read_testnet_credentials(path) == {
        _KEY: "test-key",
        _SECRET: "test-secret",
    }
    monkeypatch.delenv(_KEY, raising=False)
    monkeypatch.delenv(_SECRET, raising=False)
    load_testnet_credentials(path)
    assert os.environ[_KEY] == "test-key"
    assert os.environ[_SECRET] == "test-secret"


def test_secret_file_rejects_unsafe_permissions_and_symlink(tmp_path: Path) -> None:
    path = tmp_path / "binance-testnet.ini"
    write_testnet_credentials("test-key", "test-secret", path)
    path.chmod(0o644)
    with pytest.raises(PermissionError, match="0600"):
        read_testnet_credentials(path)

    path.chmod(0o600)
    link = tmp_path / "credentials-link.ini"
    link.symlink_to(path)
    with pytest.raises(PermissionError, match="regular file"):
        read_testnet_credentials(link)


def test_secret_file_rejects_empty_content_without_leaking_it(tmp_path: Path) -> None:
    path = tmp_path / "binance-testnet.ini"
    path.touch(mode=0o600)
    with pytest.raises(ValueError, match="incomplete"):
        read_testnet_credentials(path)
