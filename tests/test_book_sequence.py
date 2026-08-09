from alma.book_sequence import BookSequenceResult, BookSequenceState


def test_snapshot_then_bridge_and_contiguous_delta_are_accepted() -> None:
    state = BookSequenceState()

    state.on_snapshot(100)

    assert (
        state.on_delta(first=99, final=101, previous_final=98)
        is BookSequenceResult.APPLIED
    )
    assert (
        state.on_delta(first=102, final=104, previous_final=101)
        is BookSequenceResult.APPLIED
    )
    assert state.valid is True
    assert state.last_update_id == 104


def test_events_older_than_snapshot_are_ignored() -> None:
    state = BookSequenceState()
    state.on_snapshot(100)

    assert (
        state.on_delta(first=90, final=99, previous_final=89)
        is BookSequenceResult.STALE
    )
    assert state.valid is True
    assert state.last_update_id == 100


def test_previous_final_gap_invalidates_until_new_snapshot() -> None:
    state = BookSequenceState()
    state.on_snapshot(100)
    assert (
        state.on_delta(first=100, final=101, previous_final=99)
        is BookSequenceResult.APPLIED
    )

    assert (
        state.on_delta(first=105, final=106, previous_final=104)
        is BookSequenceResult.GAP
    )
    assert state.valid is False
    assert (
        state.on_delta(first=107, final=108, previous_final=106)
        is BookSequenceResult.INVALID
    )

    state.on_snapshot(108)
    assert (
        state.on_delta(first=108, final=109, previous_final=107)
        is BookSequenceResult.APPLIED
    )
    assert state.valid is True


def test_bridge_must_cover_snapshot_sequence() -> None:
    state = BookSequenceState()
    state.on_snapshot(100)

    assert (
        state.on_delta(first=101, final=102, previous_final=100)
        is BookSequenceResult.GAP
    )
    assert state.valid is False


def test_malformed_sequences_fail_closed() -> None:
    state = BookSequenceState()
    state.on_snapshot(100)

    assert (
        state.on_delta(first=102, final=101, previous_final=100)
        is BookSequenceResult.GAP
    )
    assert state.valid is False
