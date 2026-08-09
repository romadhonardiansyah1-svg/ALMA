import sqlite3
from os import PathLike

from alma.database import open_database


def open_idempotency_store(path: str | PathLike[str]) -> sqlite3.Connection:
    connection = open_database(path)
    connection.execute(
        "CREATE TABLE IF NOT EXISTS request_ids (request_id TEXT PRIMARY KEY)"
    )
    connection.commit()
    return connection


def reserve_request_id(connection: sqlite3.Connection, request_id: str) -> bool:
    with connection:
        cursor = connection.execute(
            "INSERT OR IGNORE INTO request_ids VALUES (?)",
            (request_id,),
        )
    return cursor.rowcount == 1
