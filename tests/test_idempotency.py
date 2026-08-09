from alma.idempotency import open_idempotency_store, reserve_request_id


def test_duplicate_request_is_rejected_after_reopen(tmp_path) -> None:
    path = tmp_path / "alma.db"
    connection = open_idempotency_store(path)
    assert reserve_request_id(connection, "request-1") is True
    connection.close()

    reopened = open_idempotency_store(path)
    try:
        assert reserve_request_id(reopened, "request-1") is False
    finally:
        reopened.close()
