from typing import Optional, Any, Iterator
from datetime import datetime

from .types import ABSENT
from .log import Log


class Snapshot:
    def __init__(self, log: Log, through: int):
        self._log = log
        self._seq = through

    def seq(self) -> int:
        return self._seq

    def get(self, key: str) -> Any:
        return self._log.resolve(key, self._seq)

    def timestamp(self) -> Optional[datetime]:
        if self._seq < 0:
            return None
        else:
            return self._log.events()[self._seq].timestamp

    def keys(self) -> Iterator[str]:
        return (key for key, _ in self.items())

    def items(self) -> Iterator[tuple[str, Any]]:
        for key in self._log.keys():
            value = self._log.resolve(key, self._seq)
            if value is not ABSENT:
                yield key, value

    def diff(self, other: "Snapshot") -> dict[str, tuple[Any, Any]]:
        if self._log is not other._log:
            raise ValueError("cannot diff across logs")

        lo, hi = sorted((self._seq, other._seq))
        seen = {e.key for e in self._log.events_between(lo, hi)}

        res = {}

        for key in seen:
            a = self._log.resolve(key, self._seq)
            b = self._log.resolve(key, other._seq)
            if a != b:
                res[key] = (a, b)

        return res

    def __len__(self) -> int:
        return sum(1 for _ in self.items())

    def __contains__(self, key: str) -> bool:
        return self.get(key) is not ABSENT

    def __repr__(self) -> str:
        return f"Snapshot(seq={self._seq})"
