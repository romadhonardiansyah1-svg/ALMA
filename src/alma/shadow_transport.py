import ipaddress
import json
import time
from dataclasses import dataclass
from urllib.parse import urlparse

import aiohttp

from alma.decision_fallback import ProviderUnavailable
from alma.shadow_request import ShadowRequest


@dataclass(frozen=True)
class ShadowResponse:
    content: bytes
    requested_model: str
    actual_model: str
    prompt_tokens: int
    completion_tokens: int
    latency_ms: float


def _sse_envelope(raw: bytes, requested_model: str) -> dict:
    content: list[str] = []
    model = requested_model
    usage: dict = {}
    found = False
    for line in raw.splitlines():
        if not line.startswith(b"data:"):
            continue
        data = line.removeprefix(b"data:")
        if data.startswith(b" "):
            data = data[1:]
        data = data.strip()
        if data == b"[DONE]":
            continue
        chunk = json.loads(data)
        found = True
        model = chunk.get("model", model)
        usage.update(chunk.get("usage") or {})
        choices = chunk.get("choices") or []
        if choices:
            value = (choices[0].get("delta") or {}).get("content")
            if value is not None:
                if not isinstance(value, str):
                    raise TypeError
                content.append(value)
    if not found:
        raise ValueError
    return {
        "model": model,
        "choices": [{"message": {"content": "".join(content)}}],
        "usage": usage,
    }


_SYSTEM_PROMPT = (
    "Return only JSON with exactly this alma-v1 shape and no markdown: "
    '{"policy_version":"alma-v1","state_id":"copy from input",'
    '"decision_id":"unique","created_at":"copy the now field from input",'
    '"venue":"copy from input","symbol":"copy from input",'
    '"action":"choose an allowed action","target":{'
    '"side":"choose LONG, SHORT, or FLAT","volume":"non-negative decimal"},'
    '"entry":{"mode":"choose an allowed entry mode",'
    '"preferred_low":"positive decimal",'
    '"preferred_high":"positive decimal","max_acceptable_price":"positive decimal",'
    '"ttl_seconds":"positive integer","on_missed":"choose an allowed missed-entry action",'
    '"on_partial_fill":"choose an allowed partial-fill action"},'
    '"invalidation_price":"positive decimal",'
    '"targets":[{"price":"positive decimal","close_fraction":"0..1 decimal"}],'
    '"review_triggers":[],"evidence":[],"uncertainty":"0..1"}. '
    "Always copy venue and symbol from input. No other fields. "
    "You are sovereign within the contract: independently choose among "
    "NO_CHANGE, OPEN_LONG, OPEN_SHORT, INCREASE_LONG, INCREASE_SHORT, REDUCE, "
    "CLOSE, and REVERSE; choose direction, exposure, entry mode and TTL, "
    "missed-entry / partial-fill policies, invalidation, targets, review "
    "triggers, and evidence based on the input state. Entry mode must be one "
    "of PASSIVE, AGGRESSIVE_LIMIT, STOP_ENTRY, MARKET_PROTECTED, ADAPTIVE, or "
    "WAIT_RETEST; on_missed one of ABORT, WAIT_RETEST, or REQUEST_REVIEW; "
    "on_partial_fill one of KEEP_REMAINDER, REPRICE_REMAINDER, or "
    "CANCEL_REMAINDER; uncertainty 0..1. "
    "Mechanical rules: every OPEN, INCREASE, or REVERSE must include at least "
    "one target with price and close_fraction (e.g. take-profit); NO_CHANGE or "
    "CLOSE may use an empty targets list. Each target object has ONLY price "
    "and close_fraction — no other fields. Respect the venue quantity minimum, step, and margin from "
    "the input. Choose the lowest-risk valid answer; only require the evidence "
    "truly needed, and state why any requested action is technically "
    "unavailable when rejecting it. Set review_triggers and evidence to "
    "non-empty concise material reasons, not placeholders; populated "
    "review_triggers are acted on by the runtime. "
    "If the input contains repair_candidate, repair it to this exact shape."
)


class LoopbackOpenAITransport:
    SYSTEM_PROMPT = _SYSTEM_PROMPT

    def __init__(
        self,
        base_url: str,
        *,
        model: str,
        timeout_seconds: float = 60,
        max_tokens: int = 16_000,
        max_response_bytes: int = 256_000,
        api_key: str | None = None,
    ) -> None:
        parsed = urlparse(base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("invalid OpenAI-compatible endpoint")
        try:
            loopback = ipaddress.ip_address(parsed.hostname).is_loopback
        except ValueError:
            loopback = parsed.hostname == "localhost"
        if not loopback:
            raise ValueError("AI endpoint must be loopback")
        if not model:
            raise ValueError("model is required")
        if timeout_seconds <= 0:
            raise ValueError("timeout must be positive")
        if max_tokens <= 0:
            raise ValueError("max_tokens must be positive")
        if max_response_bytes <= 0:
            raise ValueError("max_response_bytes must be positive")
        if api_key is not None and not api_key:
            raise ValueError("api_key must not be empty")
        self.url = base_url.rstrip("/") + "/v1/chat/completions"
        self.model = model
        self.timeout_seconds = timeout_seconds
        self.max_tokens = max_tokens
        self.max_response_bytes = max_response_bytes
        self.headers = (
            {"Authorization": f"Bearer {api_key}"} if api_key is not None else None
        )

    async def complete(self, request: ShadowRequest) -> ShadowResponse:
        body = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": self.SYSTEM_PROMPT,
                },
                {"role": "user", "content": request.payload.decode("utf-8")},
            ],
            "temperature": 0,
            "max_tokens": self.max_tokens,
            "stream": False,
            # ponytail: reasoning models like step-3.7-flash produce huge thinking
            # content that eats max_tokens before the final JSON — disable it
            "reasoning_effort": "none",
        }
        started = time.perf_counter_ns()
        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        try:
            async with (
                aiohttp.ClientSession(timeout=timeout) as session,
                session.post(self.url, json=body, headers=self.headers) as response,
            ):
                if response.status in {402, 429} or 500 <= response.status <= 599:
                    raise ProviderUnavailable(f"provider HTTP {response.status}")
                if response.status >= 400:
                    raise ValueError(f"non-retryable provider HTTP {response.status}")
                raw = await response.content.read(self.max_response_bytes + 1)
        except TimeoutError as error:
            raise TimeoutError("provider request timed out") from error
        except aiohttp.ClientConnectionError as error:
            raise ConnectionError("provider connection failed") from error
        if len(raw) > self.max_response_bytes:
            raise ValueError("provider response exceeds byte limit")
        try:
            envelope = (
                _sse_envelope(raw, self.model)
                if raw.lstrip().startswith(b"data:")
                else json.loads(raw)
            )
            choice = envelope["choices"][0]["message"]
            if isinstance(choice, dict):
                choice = choice["content"]
            usage = envelope.get("usage", {})
            actual_model = envelope.get("model", self.model)
            if not isinstance(choice, str) or not isinstance(actual_model, str):
                raise TypeError
            prompt_tokens = int(usage.get("prompt_tokens", 0))
            completion_tokens = int(usage.get("completion_tokens", 0))
            if prompt_tokens < 0 or completion_tokens < 0:
                raise ValueError
        except (
            KeyError,
            IndexError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ) as error:
            raise ValueError("invalid OpenAI-compatible response") from error
        return ShadowResponse(
            content=choice.encode(),
            requested_model=self.model,
            actual_model=actual_model,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            latency_ms=(time.perf_counter_ns() - started) / 1_000_000,
        )
