from dataclasses import dataclass
from enum import StrEnum


class BookSequenceResult(StrEnum):
    APPLIED = "APPLIED"
    STALE = "STALE"
    GAP = "GAP"
    INVALID = "INVALID"


@dataclass
class BookSequenceState:
    last_update_id: int | None = None
    valid: bool = False
    _bridged: bool = False

    def on_snapshot(self, last_update_id: int) -> None:
        if last_update_id < 0:
            raise ValueError("snapshot update ID must be non-negative")
        self.last_update_id = last_update_id
        self.valid = True
        self._bridged = False

    def on_delta(
        self,
        *,
        first: int,
        final: int,
        previous_final: int,
    ) -> BookSequenceResult:
        if not self.valid or self.last_update_id is None:
            return BookSequenceResult.INVALID
        if min(first, final, previous_final) < 0 or final < first:
            return self._invalidate()
        if final <= self.last_update_id:
            return BookSequenceResult.STALE
        if not self._bridged:
            if not first <= self.last_update_id <= final:
                return self._invalidate()
            self._bridged = True
        elif previous_final != self.last_update_id:
            return self._invalidate()
        self.last_update_id = final
        return BookSequenceResult.APPLIED

    def _invalidate(self) -> BookSequenceResult:
        self.valid = False
        return BookSequenceResult.GAP
