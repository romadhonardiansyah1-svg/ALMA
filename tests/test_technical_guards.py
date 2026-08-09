from decimal import Decimal

from alma.technical_guards import (
    conforms_to_increment,
    has_minimum_distance,
    has_sufficient_margin,
    is_within_range,
)


def test_accepts_available_margin_above_requirement() -> None:
    assert has_sufficient_margin(
        available=Decimal("100.00"),
        required=Decimal("75.00"),
    )


def test_rejects_available_margin_below_requirement() -> None:
    assert not has_sufficient_margin(
        available=Decimal("74.99"),
        required=Decimal("75.00"),
    )


def test_accepts_available_margin_equal_to_requirement() -> None:
    assert has_sufficient_margin(
        available=Decimal("75.00"),
        required=Decimal("75.00"),
    )


def test_rejects_invalid_margin_operands() -> None:
    assert not has_sufficient_margin(
        available=Decimal("-0.01"),
        required=Decimal("0.00"),
    )
    assert not has_sufficient_margin(
        available=Decimal("100.00"),
        required=Decimal("-0.01"),
    )
    assert not has_sufficient_margin(
        available=Decimal("NaN"),
        required=Decimal("75.00"),
    )
    assert not has_sufficient_margin(
        available=Decimal("100.00"),
        required=Decimal("Infinity"),
    )


def test_accepts_prices_beyond_minimum_distance() -> None:
    assert has_minimum_distance(
        first=Decimal("100.00"),
        second=Decimal("99.40"),
        minimum_distance=Decimal("0.50"),
    )


def test_rejects_prices_below_minimum_distance() -> None:
    assert not has_minimum_distance(
        first=Decimal("100.00"),
        second=Decimal("99.60"),
        minimum_distance=Decimal("0.50"),
    )


def test_accepts_prices_at_minimum_distance() -> None:
    assert has_minimum_distance(
        first=Decimal("100.00"),
        second=Decimal("99.50"),
        minimum_distance=Decimal("0.50"),
    )


def test_rejects_invalid_distance_operands() -> None:
    assert not has_minimum_distance(
        first=Decimal("100.00"),
        second=Decimal("99.50"),
        minimum_distance=Decimal("-0.01"),
    )
    assert not has_minimum_distance(
        first=Decimal("100.00"),
        second=Decimal("99.50"),
        minimum_distance=Decimal("NaN"),
    )
    assert not has_minimum_distance(
        first=Decimal("NaN"),
        second=Decimal("99.50"),
        minimum_distance=Decimal("0.50"),
    )
    assert not has_minimum_distance(
        first=Decimal("100.00"),
        second=Decimal("Infinity"),
        minimum_distance=Decimal("0.50"),
    )


def test_accepts_value_inside_range() -> None:
    assert is_within_range(
        value=Decimal("0.25"),
        minimum=Decimal("0.01"),
        maximum=Decimal("1.00"),
    )


def test_rejects_value_outside_range() -> None:
    assert not is_within_range(
        value=Decimal("0.001"),
        minimum=Decimal("0.01"),
        maximum=Decimal("1.00"),
    )
    assert not is_within_range(
        value=Decimal("1.01"),
        minimum=Decimal("0.01"),
        maximum=Decimal("1.00"),
    )


def test_accepts_inclusive_range_endpoints() -> None:
    assert is_within_range(
        value=Decimal("0.01"),
        minimum=Decimal("0.01"),
        maximum=Decimal("1.00"),
    )
    assert is_within_range(
        value=Decimal("1.00"),
        minimum=Decimal("0.01"),
        maximum=Decimal("1.00"),
    )


def test_rejects_invalid_range_operands() -> None:
    assert not is_within_range(
        value=Decimal("0.25"),
        minimum=Decimal("1.00"),
        maximum=Decimal("0.01"),
    )
    assert not is_within_range(
        value=Decimal("NaN"),
        minimum=Decimal("0.01"),
        maximum=Decimal("1.00"),
    )
    assert not is_within_range(
        value=Decimal("0.25"),
        minimum=Decimal("NaN"),
        maximum=Decimal("1.00"),
    )
    assert not is_within_range(
        value=Decimal("0.25"),
        minimum=Decimal("0.01"),
        maximum=Decimal("Infinity"),
    )


def test_accepts_value_aligned_to_increment() -> None:
    assert conforms_to_increment(
        value=Decimal("0.25"),
        increment=Decimal("0.01"),
    )


def test_rejects_value_not_aligned_to_increment() -> None:
    assert not conforms_to_increment(
        value=Decimal("0.251"),
        increment=Decimal("0.01"),
    )


def test_rejects_non_positive_increment_metadata() -> None:
    assert not conforms_to_increment(value=Decimal("0.25"), increment=Decimal(0))
    assert not conforms_to_increment(value=Decimal("0.25"), increment=Decimal("-0.01"))


def test_rejects_non_finite_operands() -> None:
    assert not conforms_to_increment(value=Decimal("0.25"), increment=Decimal("NaN"))
    assert not conforms_to_increment(value=Decimal("NaN"), increment=Decimal("0.01"))
