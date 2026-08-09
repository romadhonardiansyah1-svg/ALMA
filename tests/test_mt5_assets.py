from pathlib import Path

from alma.execution import client_order_id

ROOT = Path(__file__).parents[1]


def test_mt5_ea_money_safety_contract_is_present() -> None:
    source = (ROOT / "mt5/AlmaBridge.mq5").read_text()
    required = (
        "ACCOUNT_MARGIN_MODE_RETAIL_HEDGING",
        "ACCOUNT_MARGIN_MODE_RETAIL_NETTING",
        'Http("GET", "/v1/config"',
        'JsonString(config, "account_mode")',
        'JsonString(config, "position_mode")',
        'JsonString(config, "symbol")',
        "FILE_COMMON | FILE_REWRITE",
        'WriteIpc("snapshot.json", body)',
        'ReadIpc("snapshot_ack.json")',
        'WriteIpc("ack.json", ack)',
        "GlobalVariableSet(marker, 0.0)",
        "GlobalVariablesFlush()",
        "trade.OrderOpen(symbol",
        'root = "foreign:"',
        "string deals = DealsJson()",
        "HistoryDealsTotal()",
        'JsonNumber(payload, "expires_at_unix")',
        "expires_at <= (long)TimeGMT()",
        "MathAbs(fraction - 1.0)",
        "if(status == 200)",
        "ArrayResize(pending_events, 0)",
        "ArrayResize(pending_deals, 0)",
        "trade.BuyLimit(volume, price, symbol, sl, tp",
        "trade.SellLimit(volume, price, symbol, sl, tp",
        "trade.PositionClose(ticket",
        "trade.OrderDelete(ticket)",
        "ALMA bridge config rejected",
        "ALMA bridge identity rejected",
        "ALMA account readiness rejected",
    )
    assert all(item in source for item in required)
    assert source.count("PositionGetInteger(POSITION_MAGIC)") >= 3
    assert source.count("OrderGetInteger(ORDER_MAGIC)") >= 4
    assert source.count("HistoryDealGetInteger(ticket, DEAL_MAGIC)") >= 2
    assert source.count("ActualAccountMode()") == 3
    assert "mode == ACCOUNT_TRADE_MODE_DEMO" in source
    assert "mode != ACCOUNT_TRADE_MODE_REAL" in source
    assert "server == expected_server" in source
    assert 'StringFind(marker, "trial")' in source
    assert 'StringFind(server, "demo")' not in source
    assert "Num(min_volume)" in source
    assert "result.retcode == TRADE_RETCODE_DONE ||" in source
    assert "result.retcode == TRADE_RETCODE_PLACED" in source
    assert len(client_order_id("intent-1")) <= 31
    assert "password" not in source.lower()


def test_mt5_systemd_units_are_restartable_and_dedicated() -> None:
    bridge = (ROOT / "deploy/alma-mt5-bridge.service").read_text()
    account = (ROOT / "deploy/alma-mt5-account.conf").read_text()
    wine = (ROOT / "deploy/alma-mt5-wine.service").read_text()

    assert "Restart=on-failure" in bridge and "Restart=on-failure" in wine
    assert "EnvironmentFile=/root/.config/alma/mt5-account.conf" in bridge
    assert "--expected-account-mode" not in bridge
    assert "--expected-position-mode" not in bridge
    assert "--expected-login" not in bridge
    assert "--expected-server" not in bridge
    assert "--expected-symbol" not in bridge
    assert "--port 8765" in bridge
    assert "--secret /root/.config/alma/mt5-bridge.secret" in bridge
    assert "--ipc-directory" in bridge
    assert "--terminal-id ${ALMA_MT5_TERMINAL_ID}" in bridge
    assert "Terminal/Common/Files/ALMA" in bridge
    assert "ALMA_MT5_ACCOUNT_MODE=DEMO" in account
    assert "ALMA_MT5_POSITION_MODE=AUTO" in account
    assert "ALMA_MT5_LOGIN=[REDACTED]" in account
    assert "ALMA_MT5_SERVER=Exness-MT5Trial6" in account
    assert "ALMA_MT5_TERMINAL_ID=mt5-1" in account
    assert "password" not in bridge.lower()
    assert "WINEPREFIX=/root/.wine-alma" in wine
    assert "Requires=alma-mt5-bridge.service" in wine
    assert "WINEARCH=win64" in wine
    assert "WINEDLLOVERRIDES=mscoree,mshtml=" in wine
    assert "WINEDEBUG=-all" in wine
    assert "/opt/wine-stable/bin/wine" in wine
    assert "--write-startup /root/.config/alma/mt5-startup.ini" in wine
    assert '"/config:Z:\\\\root\\\\.config\\\\alma\\\\mt5-startup.ini"' in wine
    assert "ExecStop=/opt/wine-stable/bin/wineserver -k" in wine
    assert "ConditionPathExists=" in wine
    assert "cron" not in (bridge + wine).lower()
