import asyncio
import time
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Generic, TypeVar

Request = TypeVar("Request")
Response = TypeVar("Response")


class ProviderUnavailable(Exception):
    """Provider reported rate limit, quota exhaustion, or server failure."""


@dataclass(frozen=True)
class FallbackResult(Generic[Response]):  # noqa: UP046 - portable file tooling
    response: Response | None
    attempts: int
    failures: tuple[str, ...]
    fallback_used: bool
    elapsed_ms: float
    terminal_error: ValueError | None = None


def request_with_fallback(  # noqa: UP047 - portable syntax for file tooling
    request: Request,
    providers: Iterable[Callable[[Request], bytes]],
) -> bytes | None:
    for index, provider in enumerate(providers):
        for _ in range(2 if index == 0 else 1):
            try:
                return provider(request)
            except ProviderUnavailable:
                break
            except (TimeoutError, ConnectionError):
                pass
    return None


async def request_with_fallback_async(  # noqa: UP047 - portable syntax for file tooling
    request: Request,
    providers: Iterable[Callable[[Request], Awaitable[Response]]],
) -> Response | None:
    return (await request_with_fallback_report(request, providers)).response


async def request_with_fallback_report(  # noqa: UP047 - portable syntax for file tooling
    request: Request,
    providers: Iterable[Callable[[Request], Awaitable[Response]]],
    *,
    timeout_seconds: float = 60,
    max_attempts: int = 3,
) -> FallbackResult[Response]:
    if timeout_seconds <= 0 or max_attempts <= 0:
        raise ValueError("fallback limits must be positive")
    attempts = 0
    failures: list[str] = []
    fallback_used = False
    terminal_error: ValueError | None = None
    started = time.perf_counter_ns()

    async def execute() -> Response | None:
        nonlocal attempts, fallback_used, terminal_error
        for index, provider in enumerate(providers):
            for _ in range(2 if index == 0 else 1):
                if attempts >= max_attempts:
                    return None
                attempts += 1
                try:
                    response = await provider(request)
                except ProviderUnavailable:
                    failures.append("PROVIDER_UNAVAILABLE")
                    break
                except TimeoutError:
                    failures.append("TIMEOUT")
                except ConnectionError:
                    failures.append("CONNECTION")
                except ValueError as error:
                    failures.append("NON_RETRYABLE")
                    terminal_error = error
                    return None
                else:
                    fallback_used = index > 0
                    return response
        return None

    try:
        async with asyncio.timeout(timeout_seconds):
            response = await execute()
    except TimeoutError:
        failures.append("OVERALL_TIMEOUT")
        response = None
    return FallbackResult(
        response=response,
        attempts=attempts,
        failures=tuple(failures),
        fallback_used=fallback_used,
        elapsed_ms=(time.perf_counter_ns() - started) / 1_000_000,
        terminal_error=terminal_error,
    )
