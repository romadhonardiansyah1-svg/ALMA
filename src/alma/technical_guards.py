from decimal import Decimal


def conforms_to_increment(*, value: Decimal, increment: Decimal) -> bool:
    return (
        value.is_finite()
        and increment.is_finite()
        and increment > 0
        and value % increment == 0
    )


def is_within_range(*, value: Decimal, minimum: Decimal, maximum: Decimal) -> bool:
    return (
        value.is_finite()
        and minimum.is_finite()
        and maximum.is_finite()
        and minimum <= value <= maximum
    )


def has_minimum_distance(
    *, first: Decimal, second: Decimal, minimum_distance: Decimal
) -> bool:
    return (
        first.is_finite()
        and second.is_finite()
        and minimum_distance.is_finite()
        and minimum_distance >= 0
        and abs(first - second) >= minimum_distance
    )


def has_sufficient_margin(*, available: Decimal, required: Decimal) -> bool:
    return (
        available.is_finite()
        and required.is_finite()
        and available >= 0
        and required >= 0
        and available >= required
    )
