import hashlib
import json
from dataclasses import asdict, dataclass
from decimal import Decimal

from alma.book_sequence import BookSequenceResult, BookSequenceState
from alma.market_recording import MarketEvent
from alma.strategies import SetupEvidence

BPS = Decimal(10_000)


@dataclass(frozen=True)
class ReplayConfig:
    latency_ns: int = 0
    slippage_bps: Decimal = Decimal(0)
    fee_bps: Decimal = Decimal(0)
    partial_fill_fraction: Decimal = Decimal(1)
    max_entry_deviation_bps: Decimal = Decimal(100)
    max_entry_delay_ns: int = 5_000_000_000
    max_holding_ns: int = 45 * 60 * 1_000_000_000

    def __post_init__(self) -> None:
        for name, value in (
            ("latency", self.latency_ns),
            ("max entry delay", self.max_entry_delay_ns),
            ("max holding", self.max_holding_ns),
        ):
            if value < 0:
                raise ValueError(f"{name} must be non-negative")
        for name, value in (
            ("slippage bps", self.slippage_bps),
            ("fee bps", self.fee_bps),
            ("max entry deviation bps", self.max_entry_deviation_bps),
        ):
            if not value.is_finite() or value < 0:
                raise ValueError(f"{name} must be finite and non-negative")
        if (
            not self.partial_fill_fraction.is_finite()
            or not 0 < self.partial_fill_fraction <= 1
        ):
            raise ValueError("partial fill fraction must be within (0, 1]")


@dataclass(frozen=True)
class ReplayTrade:
    setup: str
    direction: int
    feature_id: str
    status: str
    exit_reason: str
    entry_ns: int | None
    exit_ns: int | None
    quantity: Decimal
    entry_price: Decimal | None
    exit_price: Decimal | None
    gross_pnl: Decimal
    fees: Decimal
    funding: Decimal
    net_pnl: Decimal


@dataclass(frozen=True)
class BenchmarkResult:
    trades: tuple[ReplayTrade, ...]
    total_net_pnl: Decimal
    filled: int
    missed: int
    result_hash: str


@dataclass
class _Feed:
    bid: Decimal | None = None
    ask: Decimal | None = None
    funding_rate: Decimal = Decimal(0)
    book: BookSequenceState = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.book is None:
            self.book = BookSequenceState()

    def apply(self, event: MarketEvent) -> None:
        if event.kind == "quote":
            self.bid = event.bid
            self.ask = event.ask
        elif event.kind == "funding":
            assert event.rate is not None
            self.funding_rate = event.rate
        elif event.kind == "book_snapshot":
            assert event.final_update_id is not None
            self.book.on_snapshot(event.final_update_id)
        elif event.kind == "book_delta":
            assert event.first_update_id is not None
            assert event.final_update_id is not None
            assert event.previous_final_update_id is not None
            result = self.book.on_delta(
                first=event.first_update_id,
                final=event.final_update_id,
                previous_final=event.previous_final_update_id,
            )
            if result is BookSequenceResult.GAP:
                self.book.valid = False
        elif event.kind in {"book_invalidate", "reconnect"}:
            self.book.valid = False


def _slipped(price: Decimal, direction: int, entering: bool, bps: Decimal) -> Decimal:
    adverse = direction if entering else -direction
    return price * (Decimal(1) + Decimal(adverse) * bps / BPS)


def _missed(candidate: SetupEvidence, reason: str) -> ReplayTrade:
    return ReplayTrade(
        setup=candidate.setup,
        direction=candidate.direction,
        feature_id=candidate.feature_id,
        status="MISSED",
        exit_reason=reason,
        entry_ns=None,
        exit_ns=None,
        quantity=Decimal(0),
        entry_price=None,
        exit_price=None,
        gross_pnl=Decimal(0),
        fees=Decimal(0),
        funding=Decimal(0),
        net_pnl=Decimal(0),
    )


def _simulate(
    events: list[MarketEvent],
    candidate: SetupEvidence,
    config: ReplayConfig,
) -> ReplayTrade:
    feed = _Feed()
    eligible_ns = candidate.observed_at_ns + config.latency_ns
    deadline_ns = candidate.observed_at_ns + config.max_entry_delay_ns
    entry_ns: int | None = None
    entry_price: Decimal | None = None
    quantity = config.partial_fill_fraction
    funding = Decimal(0)
    envelope_rejected = False
    watermark_ns = 0

    for event_index, event in enumerate(events):
        watermark_ns = max(watermark_ns, event.ts_event_ns)
        feed.apply(event)
        if candidate.event_index is not None:
            if event_index <= candidate.event_index:
                continue
        elif event.ts_event_ns < candidate.observed_at_ns:
            continue
        if entry_ns is None:
            if event.ts_event_ns < eligible_ns or event.kind != "quote":
                continue
            if event.ts_event_ns > deadline_ns:
                break
            if not feed.book.valid or feed.bid is None or feed.ask is None:
                continue
            raw_entry = feed.ask if candidate.direction > 0 else feed.bid
            deviation = (
                abs(raw_entry - candidate.entry_reference)
                / candidate.entry_reference
                * BPS
            )
            if deviation > config.max_entry_deviation_bps:
                envelope_rejected = True
                continue
            entry_ns = event.ts_event_ns
            entry_price = _slipped(
                raw_entry,
                candidate.direction,
                True,
                config.slippage_bps,
            )
            continue

        assert entry_price is not None
        if event.kind == "funding_settlement" and event.ts_event_ns >= entry_ns:
            assert event.price is not None and event.rate is not None
            funding += (
                event.price * quantity * event.rate * Decimal(candidate.direction)
            )
            continue
        if event.kind == "funding":
            continue
        if event.kind in {"book_invalidate", "reconnect"}:
            executable = feed.bid if candidate.direction > 0 else feed.ask
            exit_price = (
                _slipped(executable, candidate.direction, False, config.slippage_bps)
                if executable is not None
                else entry_price
            )
            return _close(
                candidate,
                entry_ns,
                max(entry_ns, watermark_ns),
                entry_price,
                exit_price,
                quantity,
                funding,
                "DISCONNECT",
                config,
            )
        if event.kind != "quote" or feed.bid is None or feed.ask is None:
            continue
        executable = feed.bid if candidate.direction > 0 else feed.ask
        target_hit = candidate.target_reference is not None and (
            executable >= candidate.target_reference
            if candidate.direction > 0
            else executable <= candidate.target_reference
        )
        invalidated = (
            executable <= candidate.invalidation
            if candidate.direction > 0
            else executable >= candidate.invalidation
        )
        timed_out = watermark_ns - entry_ns >= config.max_holding_ns
        if target_hit or invalidated or timed_out:
            reason = (
                "TARGET" if target_hit else "INVALIDATION" if invalidated else "TIMEOUT"
            )
            exit_price = _slipped(
                executable,
                candidate.direction,
                False,
                config.slippage_bps,
            )
            return _close(
                candidate,
                entry_ns,
                max(entry_ns, watermark_ns),
                entry_price,
                exit_price,
                quantity,
                funding,
                reason,
                config,
            )

    if entry_ns is None or entry_price is None:
        return _missed(
            candidate, "PRICE_ENVELOPE" if envelope_rejected else "NO_VALID_QUOTE"
        )
    executable = feed.bid if candidate.direction > 0 else feed.ask
    exit_price = (
        _slipped(executable, candidate.direction, False, config.slippage_bps)
        if executable is not None
        else entry_price
    )
    return _close(
        candidate,
        entry_ns,
        max(entry_ns, watermark_ns),
        entry_price,
        exit_price,
        quantity,
        funding,
        "END_OF_DATA",
        config,
    )


def _close(
    candidate: SetupEvidence,
    entry_ns: int,
    exit_ns: int,
    entry_price: Decimal,
    exit_price: Decimal,
    quantity: Decimal,
    funding: Decimal,
    reason: str,
    config: ReplayConfig,
) -> ReplayTrade:
    gross = (exit_price - entry_price) * quantity * Decimal(candidate.direction)
    fees = (entry_price + exit_price) * quantity * config.fee_bps / BPS
    net = gross - fees - funding
    return ReplayTrade(
        setup=candidate.setup,
        direction=candidate.direction,
        feature_id=candidate.feature_id,
        status="FILLED",
        exit_reason=reason,
        entry_ns=entry_ns,
        exit_ns=exit_ns,
        quantity=quantity,
        entry_price=entry_price,
        exit_price=exit_price,
        gross_pnl=gross,
        fees=fees,
        funding=funding,
        net_pnl=net,
    )


def benchmark(
    events: list[MarketEvent],
    candidates: list[SetupEvidence],
    config: ReplayConfig | None = None,
) -> BenchmarkResult:
    config = config or ReplayConfig()
    if any(
        (
            candidates[index].event_index is not None
            and candidates[index - 1].event_index is not None
            and candidates[index].event_index < candidates[index - 1].event_index
        )
        or candidates[index].observed_at_ns < candidates[index - 1].observed_at_ns
        for index in range(1, len(candidates))
    ):
        raise ValueError("strategy candidates are out of order")
    built: list[ReplayTrade] = []
    busy_until = -1
    for candidate in candidates:
        if candidate.observed_at_ns < busy_until:
            built.append(_missed(candidate, "POSITION_OPEN"))
            continue
        trade = _simulate(events, candidate, config)
        built.append(trade)
        if trade.status == "FILLED" and trade.exit_ns is not None:
            busy_until = trade.exit_ns
    trades = tuple(built)
    total = sum((trade.net_pnl for trade in trades), Decimal(0))
    payload = {
        "config": asdict(config),
        "trades": [asdict(trade) for trade in trades],
        "total_net_pnl": total,
    }
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), default=str
    ).encode()
    return BenchmarkResult(
        trades=trades,
        total_net_pnl=total,
        filled=sum(trade.status == "FILLED" for trade in trades),
        missed=sum(trade.status == "MISSED" for trade in trades),
        result_hash=hashlib.sha256(encoded).hexdigest(),
    )


def purged_walk_forward(
    *, total: int, train_size: int, test_size: int, purge: int
) -> tuple[tuple[range, range], ...]:
    if min(total, train_size, test_size) <= 0:
        raise ValueError("total, train size, and test size must be positive")
    if purge < 0:
        raise ValueError("purge must be non-negative")
    folds: list[tuple[range, range]] = []
    train_start = 0
    while True:
        train = range(train_start, train_start + train_size)
        test_start = train.stop + purge
        test = range(test_start, test_start + test_size)
        if test.stop > total:
            break
        folds.append((train, test))
        train_start += test_size
    return tuple(folds)
