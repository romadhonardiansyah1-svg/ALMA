import sqlite3
from os import PathLike

from alma.database import open_database
from alma.venue_modes import VenueMode


def open_venue_mode_store(path: str | PathLike[str]) -> sqlite3.Connection:
    connection = open_database(path)
    connection.execute(
        "CREATE TABLE IF NOT EXISTS venue_modes "
        "(venue_id TEXT PRIMARY KEY, mode TEXT NOT NULL)"
    )
    connection.commit()
    return connection


def initialize_venue_mode(
    connection: sqlite3.Connection,
    venue_id: str,
    mode: VenueMode,
) -> None:
    with connection:
        connection.execute(
            "INSERT INTO venue_modes VALUES (?, ?)",
            (venue_id, mode.value),
        )


def load_venue_modes(connection: sqlite3.Connection) -> dict[str, VenueMode]:
    return {
        venue_id: VenueMode(mode)
        for venue_id, mode in connection.execute(
            "SELECT venue_id, mode FROM venue_modes"
        )
    }
