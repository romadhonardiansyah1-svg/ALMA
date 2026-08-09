from pathlib import Path
from types import SimpleNamespace

import pytest

from alma import mt5_demo_login


def test_demo_login_uses_owner_only_tmpfs_and_never_password_argv(
    tmp_path: Path, monkeypatch
) -> None:
    wine = tmp_path / "wine"
    terminal = tmp_path / "terminal64.exe"
    wine.touch()
    terminal.touch()
    observed: dict[str, object] = {}

    monkeypatch.setattr(mt5_demo_login, "WINE", wine)
    monkeypatch.setattr(mt5_demo_login, "TERMINAL", terminal)
    monkeypatch.setattr(mt5_demo_login.time, "sleep", lambda _: None)

    def run(command, **_):
        config = Path(command[-1])
        observed["config"] = config
        observed["mode"] = config.stat().st_mode & 0o777
        observed["body"] = config.read_text()
        return SimpleNamespace(stdout="Z:\\dev\\shm\\alma-mt5.ini\n")

    class Process:
        pid = 123

        def poll(self):
            return None

    def popen(command, **_):
        observed["argv"] = command
        return Process()

    monkeypatch.setattr(mt5_demo_login.subprocess, "run", run)
    monkeypatch.setattr(mt5_demo_login.subprocess, "Popen", popen)

    password = "fixture-only-password"
    assert (
        mt5_demo_login.start_terminal(
            "123456", "Broker-Demo", password, cleanup_delay=1
        )
        == 123
    )
    assert observed["mode"] == 0o600
    assert f"Password={password}" in observed["body"]
    assert all(password not in argument for argument in observed["argv"])
    assert not Path(observed["config"]).exists()


@pytest.mark.parametrize(
    ("login", "server", "password"),
    [("bad", "server", "x"), ("1", "bad server", "x"), ("1", "server", "")],
)
def test_demo_login_rejects_invalid_identity_or_password(
    login: str, server: str, password: str
) -> None:
    with pytest.raises(ValueError):
        mt5_demo_login._config(login, server, password)


def test_account_config_selects_demo_or_real_without_source_change(
    tmp_path: Path,
) -> None:
    path = tmp_path / "mt5-account.conf"
    path.write_text(
        "ALMA_MT5_ACCOUNT_MODE=REAL\n"
        "ALMA_MT5_POSITION_MODE=AUTO\n"
        "ALMA_MT5_LOGIN=987654\n"
        "ALMA_MT5_SERVER=Broker-Live\n"
        "ALMA_MT5_SYMBOL=XAUUSD.r\n"
        "ALMA_MT5_TERMINAL_ID=mt5-live\n"
    )
    assert mt5_demo_login.load_account_config(path) == {
        "ALMA_MT5_ACCOUNT_MODE": "REAL",
        "ALMA_MT5_POSITION_MODE": "AUTO",
        "ALMA_MT5_LOGIN": "987654",
        "ALMA_MT5_SERVER": "Broker-Live",
        "ALMA_MT5_SYMBOL": "XAUUSD.r",
        "ALMA_MT5_TERMINAL_ID": "mt5-live",
    }


def test_startup_config_is_generated_from_profile_without_password(
    tmp_path: Path,
) -> None:
    output = tmp_path / "mt5-startup.ini"
    mt5_demo_login.write_startup_config(
        {
            "ALMA_MT5_ACCOUNT_MODE": "REAL",
            "ALMA_MT5_POSITION_MODE": "AUTO",
            "ALMA_MT5_LOGIN": "987654",
            "ALMA_MT5_SERVER": "Broker-Live",
            "ALMA_MT5_SYMBOL": "XAUUSD.r",
            "ALMA_MT5_TERMINAL_ID": "mt5-live",
        },
        output,
    )

    body = output.read_text()
    assert output.stat().st_mode & 0o777 == 0o600
    assert "Login=987654" in body
    assert "Server=Broker-Live" in body
    assert "Symbol=XAUUSD.r" in body
    assert "password" not in body.lower()
