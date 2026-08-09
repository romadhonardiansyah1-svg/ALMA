from decimal import Decimal


def execution_delta(
    *,
    desired: Decimal,
    actual: Decimal,
    pending: Decimal,
) -> Decimal:
    return desired - actual - pending
