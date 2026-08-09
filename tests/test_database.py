from alma.database import open_database


def test_database_enforces_foreign_keys(tmp_path) -> None:
    connection = open_database(tmp_path / "alma.db")
    try:
        assert connection.execute("PRAGMA foreign_keys").fetchone() == (1,)
    finally:
        connection.close()


def test_database_has_measured_busy_timeout(tmp_path) -> None:
    connection = open_database(tmp_path / "alma.db")
    try:
        assert connection.execute("PRAGMA busy_timeout").fetchone() == (5000,)
    finally:
        connection.close()
