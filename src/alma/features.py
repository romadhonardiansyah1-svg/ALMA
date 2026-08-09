import hashlib
import json
from collections import deque
from dataclasses import asdict, dataclass
from decimal import Decimal

from alma.bars import Bar
from alma.market_state import MarketSnapshot


@dataclass(frozen=True)
class FeatureConfig:
    history_size: int = 64
    max_market_age_ms: int = 2_000
    acceptance_bars: int = 2
    flow_threshold: Decimal = Decimal("0.2")
    book_imbalance_threshold: Decimal = Decimal("0.2")
    sweep_distance_ratio: Decimal = Decimal("0.05")
    displacement_threshold: Decimal = Decimal("0.25")
    compression_threshold: Decimal = Decimal("0.5")
    modeled_cost_bps: Decimal = Decimal(5)
    funding_limit: Decimal = Decimal("0.001")

    def __post_init__(self) -> None:
        if self.history_size < 2:
            raise ValueError("history size must be at least two")
        if self.acceptance_bars < 1:
            raise ValueError("acceptance bars must be positive")
        if self.max_market_age_ms < 0:
            raise ValueError("max market age must be non-negative")
        if not self.flow_threshold.is_finite() or not 0 <= self.flow_threshold <= 1:
            raise ValueError("flow threshold must be finite and within [0, 1]")
        for name, value in (
            ("book imbalance threshold", self.book_imbalance_threshold),
            ("sweep distance ratio", self.sweep_distance_ratio),
            ("displacement threshold", self.displacement_threshold),
            ("compression threshold", self.compression_threshold),
            ("modeled cost bps", self.modeled_cost_bps),
            ("funding limit", self.funding_limit),
        ):
            if not value.is_finite() or value < 0:
                raise ValueError(f"{name} must be finite and non-negative")
        if self.book_imbalance_threshold > 1:
            raise ValueError("book imbalance threshold must not exceed one")


@dataclass(frozen=True)
class FeatureSnapshot:
    feature_id: str
    source_state_id: str
    observed_at_ns: int
    market_age_ms: int
    price: Decimal | None
    spread_bps: Decimal | None
    top_book_imbalance: Decimal
    flow_imbalance: Decimal
    tick_velocity_1s: int
    realized_volatility: Decimal
    funding_rate: Decimal | None
    book_valid: bool
    h1_return: Decimal | None
    h1_volatility: Decimal
    h1_regime: int
    liquidity_high: Decimal | None
    liquidity_low: Decimal | None
    m15_open: Decimal | None
    m15_high: Decimal | None
    m15_low: Decimal | None
    m15_close: Decimal | None
    m15_position: Decimal | None
    m15_displacement: Decimal | None
    m15_body_bps: Decimal | None
    acceptance_above: int
    acceptance_below: int
    m5_compression: Decimal | None
    m1_return: Decimal | None


class FeatureState:
    def __init__(self, config: FeatureConfig | None = None) -> None:
        self.config = config or FeatureConfig()
        self._bars: dict[int, deque[Bar]] = {
            minutes: deque(maxlen=self.config.history_size)
            for minutes in (1, 5, 15, 60)
        }

    def update(self, snapshot: MarketSnapshot) -> FeatureSnapshot:
        incoming = {
            1: snapshot.m1,
            5: snapshot.m5,
            15: snapshot.m15,
            60: snapshot.h1,
        }
        for minutes, bar in incoming.items():
            if bar is not None:
                self._accept_bar(minutes, bar, snapshot.observed_at_ns)

        m1 = self._latest(1)
        m5 = self._latest(5)
        m15 = self._latest(15)
        h1 = self._latest(60)
        m15_history = list(self._bars[15])
        acceptance_window = min(
            self.config.acceptance_bars,
            max(1, len(m15_history) - 1),
        )
        prior_m15 = m15_history[:-acceptance_window] if m15 is not None else []
        liquidity_high = max((bar.high for bar in prior_m15), default=None)
        liquidity_low = min((bar.low for bar in prior_m15), default=None)
        liquidity_range = (
            liquidity_high - liquidity_low
            if liquidity_high is not None and liquidity_low is not None
            else None
        )
        m15_range = m15.high - m15.low if m15 is not None else None
        price = snapshot.mark_price
        if price is None and snapshot.bid is not None and snapshot.ask is not None:
            price = (snapshot.bid + snapshot.ask) / 2
        elif price is None:
            price = snapshot.bid or snapshot.ask

        values: dict[str, object] = {
            "source_state_id": snapshot.state_id,
            "observed_at_ns": snapshot.observed_at_ns,
            "market_age_ms": snapshot.market_age_ms,
            "price": price,
            "spread_bps": (
                snapshot.spread / snapshot.bid * 10_000
                if snapshot.spread is not None and snapshot.bid is not None
                else None
            ),
            "top_book_imbalance": snapshot.top_book_imbalance,
            "flow_imbalance": snapshot.flow_imbalance,
            "tick_velocity_1s": snapshot.tick_velocity_1s,
            "realized_volatility": snapshot.realized_volatility,
            "funding_rate": snapshot.funding_rate,
            "book_valid": snapshot.book_valid,
            "h1_return": self._bar_return(h1),
            "h1_volatility": self._rms_returns(60),
            "h1_regime": self._sign(self._bar_return(h1)),
            "liquidity_high": liquidity_high,
            "liquidity_low": liquidity_low,
            "m15_open": m15.open if m15 else None,
            "m15_high": m15.high if m15 else None,
            "m15_low": m15.low if m15 else None,
            "m15_close": m15.close if m15 else None,
            "m15_position": (
                (m15.close - liquidity_low) / liquidity_range
                if m15 is not None and liquidity_range not in (None, 0)
                else None
            ),
            "m15_displacement": (
                abs(m15.close - m15.open) / liquidity_range
                if m15 is not None and liquidity_range not in (None, 0)
                else None
            ),
            "m15_body_bps": (
                abs(m15.close - m15.open) / m15.open * 10_000
                if m15 is not None
                else None
            ),
            "acceptance_above": self._acceptance(liquidity_high, above=True),
            "acceptance_below": self._acceptance(liquidity_low, above=False),
            "m5_compression": (
                (m5.high - m5.low) / m15_range
                if m5 is not None and m15_range not in (None, 0)
                else None
            ),
            "m1_return": self._bar_return(m1),
        }
        encoded = json.dumps(
            asdict(FeatureSnapshot(feature_id="", **values)),
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode()
        return FeatureSnapshot(
            feature_id=hashlib.sha256(encoded).hexdigest(),
            **values,
        )

    def _accept_bar(self, minutes: int, bar: Bar, observed_at_ns: int) -> None:
        if bar.minutes != minutes:
            raise ValueError("bar timeframe mismatch")
        if bar.end_ns > observed_at_ns:
            raise ValueError("bar is not closed at observation time")
        history = self._bars[minutes]
        if history and bar.start_ns < history[-1].start_ns:
            raise ValueError("closed bar is out of order")
        if history and bar.start_ns == history[-1].start_ns:
            if bar != history[-1]:
                raise ValueError("closed bar was revised")
            return
        history.append(bar)

    def _latest(self, minutes: int) -> Bar | None:
        history = self._bars[minutes]
        return history[-1] if history else None

    @staticmethod
    def _bar_return(bar: Bar | None) -> Decimal | None:
        return None if bar is None else (bar.close - bar.open) / bar.open

    @staticmethod
    def _sign(value: Decimal | None) -> int:
        if value is None or value == 0:
            return 0
        return 1 if value > 0 else -1

    def _rms_returns(self, minutes: int) -> Decimal:
        returns = [self._bar_return(bar) for bar in self._bars[minutes]]
        return (
            (
                sum(
                    (value * value for value in returns if value is not None),
                    Decimal(0),
                )
                / len(returns)
            ).sqrt()
            if returns
            else Decimal(0)
        )

    def _acceptance(self, level: Decimal | None, *, above: bool) -> int:
        if level is None:
            return 0
        count = 0
        for bar in reversed(self._bars[15]):
            accepted = bar.close > level if above else bar.close < level
            if not accepted:
                break
            count += 1
        return min(count, self.config.acceptance_bars)
