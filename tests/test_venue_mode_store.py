import sqlite3

import pytest

from alma.venue_mode_store import (
    initialize_venue_mode,
    load_venue_modes,
    open_venue_mode_store,
)
from alma.venue_modes import VenueMode


def test_store_opens_in_wal_mode(tmp_path) -> None:
    connection = open_venue_mode_store(tmp_path / "alma.db")
    try:
        assert connection.execute("PRAGMA journal_mode").fetchone() == ("wal",)
    finally:
        connection.close()


def test_stored_venue_mode_survives_reopen(tmp_path) -> None:
    path = tmp_path / "alma.db"
    connection = open_venue_mode_store(path)
    initialize_venue_mode(connection, "binance", VenueMode.MONITOR)
    connection.close()

    reopened = open_venue_mode_store(path)
    try:
        assert load_venue_modes(reopened) == {"binance": VenueMode.MONITOR}
    finally:
        reopened.close()


def test_bootstrap_cannot_overwrite_existing_mode(tmp_path) -> None:
    connection = open_venue_mode_store(tmp_path / "alma.db")
    try:
        initialize_venue_mode(connection, "binance", VenueMode.MONITOR)
        with pytest.raises(sqlite3.IntegrityError):
            initialize_venue_mode(connection, "binance", VenueMode.TRADE)
        assert load_venue_modes(connection) == {"binance": VenueMode.MONITOR}
    finally:
        connection.close()
