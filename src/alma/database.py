import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from os import PathLike


def open_database(path: str | PathLike[str]) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA busy_timeout=5000")
    return connection


@contextmanager
def immediate_transaction(connection: sqlite3.Connection) -> Iterator[None]:
    if connection.in_transaction:
        yield
        return
    connection.execute("BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()
