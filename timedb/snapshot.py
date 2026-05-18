from typing import Optional, Iterator
from datetime import datetime

from .types import OptValue, ABSENT
from .log import Log


class Snapshot:
    def __init__(self, log: Log, seq: int):
        self._seq = seq
        self._state = log.state_at(seq)
        if seq == 0:
            self._timestamp = None
        else:
            self._timestamp = log.event_at(seq - 1).timestamp

    def seq(self) -> int:
        return self._seq

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
        return f"Snapshot(seq={self._seq})"
