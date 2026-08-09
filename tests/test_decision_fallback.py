import asyncio

from alma.decision_fallback import (
    ProviderUnavailable,
    request_with_fallback,
    request_with_fallback_async,
    request_with_fallback_report,
)


def test_transient_primary_failure_retries_once_before_fallback() -> None:
    request = object()
    primary_requests = []
    fallback_called = False

    def primary(received: object) -> bytes:
        primary_requests.append(received)
        if len(primary_requests) == 1:
            raise TimeoutError
        return b"primary-decision"

    def fallback(_: object) -> bytes:
        nonlocal fallback_called
        fallback_called = True
        return b"fallback-decision"

    result = request_with_fallback(request, [primary, fallback])

    assert result == b"primary-decision"
    assert primary_requests == [request, request]
    assert fallback_called is False


def test_exhausted_primary_retry_uses_fallback_with_same_request() -> None:
    request = object()
    seen_requests = []

    def primary(received: object) -> bytes:
        seen_requests.append(received)
        raise TimeoutError

    def fallback(received: object) -> bytes:
        seen_requests.append(received)
        return b"fallback-decision"

    result = request_with_fallback(request, [primary, fallback])

    assert result == b"fallback-decision"
    assert seen_requests == [request, request, request]


def test_connection_failure_uses_fallback() -> None:
    def disconnected(_: object) -> bytes:
        raise ConnectionError

    result = request_with_fallback(object(), [disconnected, lambda _: b"fallback"])

    assert result == b"fallback"


def test_provider_unavailable_skips_retry_and_uses_fallback() -> None:
    unavailable_calls = 0

    def unavailable(_: object) -> bytes:
        nonlocal unavailable_calls
        unavailable_calls += 1
        raise ProviderUnavailable

    result = request_with_fallback(object(), [unavailable, lambda _: b"fallback"])

    assert result == b"fallback"
    assert unavailable_calls == 1


def test_async_fallback_preserves_request_identity_and_retry_policy() -> None:
    async def run() -> None:
        request = object()
        seen = []

        async def primary(received: object) -> bytes:
            seen.append(received)
            raise TimeoutError

        async def fallback(received: object) -> bytes:
            seen.append(received)
            return b"fallback"

        assert (
            await request_with_fallback_async(request, [primary, fallback])
            == b"fallback"
        )
        assert seen == [request, request, request]

    asyncio.run(run())


def test_async_fallback_report_is_bounded_and_auditable() -> None:
    async def run() -> None:
        request = object()

        async def timeout(_: object) -> bytes:
            await asyncio.sleep(1)
            return b"late"

        report = await request_with_fallback_report(
            request, [timeout], timeout_seconds=0.01, max_attempts=1
        )
        assert report.response is None
        assert report.attempts == 1
        assert report.failures == ("OVERALL_TIMEOUT",)
        assert report.fallback_used is False
        assert report.elapsed_ms >= 0

        async def unavailable(_: object) -> bytes:
            raise ProviderUnavailable

        async def fallback(received: object) -> bytes:
            assert received is request
            return b"fallback"

        report = await request_with_fallback_report(request, [unavailable, fallback])
        assert report.response == b"fallback"
        assert report.attempts == 2
        assert report.failures == ("PROVIDER_UNAVAILABLE",)
        assert report.fallback_used is True

    asyncio.run(run())
