from typing import Iterator

from .types import Event
from .log import Log


class Interval:
    def __init__(self, log: Log, lo: int, hi: int):
        self._log = log
        self._lo = lo
        self._hi = hi

    def __iter__(self) -> Iterator[Event]:
        for i in range(self._lo, self._hi):
            yield self._log.events()[i]

    def history(self, key: str) -> Iterator[Event]:
        return (e for e in self if e.key == key)

    def keys_touched(self) -> set[str]:
        return {e.key for e in self}

    def tx_ids(self) -> set[int]:
        return {e.tx_id for e in self if e.tx_id is not None}

    def count(self) -> int:
        return self._hi - self._lo

    def __repr__(self) -> str:
        return f"Interval(seq=[{self._lo}, {self._hi}))"
