from typing import Iterator, Optional
from datetime import datetime

from .types import OptValue, ABSENT
from .log import Log


class Snapshot:
    def __init__(self, tx: int, log: Log):
        self._tx = tx
        self._state = log.states()[tx]
        self._timestamp = log.tx_timestamp(tx - 1) if tx > 0 else None

    def tx(self) -> int:
        return self._tx

    def timestamp(self) -> Optional[datetime]:
        return self._timestamp

    def get(self, key: str) -> OptValue:
        return self._state.get(key, ABSENT)

    def keys(self) -> Iterator[str]:
        return iter(self._state)

    def items(self) -> Iterator[tuple[str, OptValue]]:
        return iter(self._state.items())

    def diff(
        self, other: "Snapshot"
    ) -> Iterator[tuple[str, OptValue, OptValue]]:
        a, b = self._state, other._state
        for k in set(a) | set(b):
            a_val = a.get(k, ABSENT)
            b_val = b.get(k, ABSENT)
            if a_val != b_val:
                yield (k, a_val, b_val)

    def __contains__(self, key: str) -> bool:
        return self.get(key) is not ABSENT

    def __repr__(self) -> str:
        return f"Snapshot(tx={self._tx})"
