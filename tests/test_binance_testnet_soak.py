import pytest

from alma.binance_testnet_soak import validate_observation


def test_account_monitor_accepts_active_trading_state() -> None:
    validate_observation(
        {
            "nonzero_balance_assets": 1,
            "position_amount": "0.001",
            "orders_open": 1,
            "algo_orders_open": 1,
        }
    )
    with pytest.raises(RuntimeError, match="TESTNET_BALANCE_MISSING"):
        validate_observation(
            {
                "nonzero_balance_assets": 0,
                "position_amount": "0",
                "orders_open": 0,
                "algo_orders_open": 0,
            }
        )
