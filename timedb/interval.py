from typing import Iterator

from .types import Event
from .log import Log


class Interval:
    def __init__(self, log: Log, lo: int, hi: int):
        self._log = log
        self._lo = lo
        self._hi = hi

    def __iter__(self) -> Iterator[Event]:
        return self._log.events_between(self._lo, self._hi)

    def history(self, key: str) -> Iterator[Event]:
        return (e for e in self if e.key == key)

    def keys_touched(self) -> set[str]:
        return {e.key for e in self}

    def __repr__(self) -> str:
        return f"Interval(tx=[{self._lo}, {self._hi}))"
