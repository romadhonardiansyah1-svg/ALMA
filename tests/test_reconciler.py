from decimal import Decimal

from alma.reconciler import execution_delta


def test_execution_delta_is_positive_when_desired_exceeds_committed_quantity() -> None:
    assert execution_delta(
        desired=Decimal("2.00"),
        actual=Decimal("0.50"),
        pending=Decimal("0.25"),
    ) == Decimal("1.25")


def test_execution_delta_is_zero_when_pending_fills_target() -> None:
    assert execution_delta(
        desired=Decimal("1.00"),
        actual=Decimal("0.75"),
        pending=Decimal("0.25"),
    ) == Decimal("0.00")


def test_execution_delta_is_negative_when_committed_quantity_exceeds_target() -> None:
    assert execution_delta(
        desired=Decimal("0.50"),
        actual=Decimal("1.00"),
        pending=Decimal("0.25"),
    ) == Decimal("-0.75")
