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
        if event := self._log.get(self._seq):
            return event.timestamp
        else:
            return None

    def keys(self) -> Iterator[str]:
        return (key for key, _ in self.items())

    def items(self) -> Iterator[tuple[str, Any]]:
        return self._log.items(self._seq)

    def diff(self, other: "Snapshot") -> Iterator[tuple[str, Any, Any]]:
        lo, hi = sorted((self._seq, other._seq))
        return self._log.diff(lo, hi)

    def __contains__(self, key: str) -> bool:
        return self.get(key) is not ABSENT

    def __repr__(self) -> str:
        return f"Snapshot(seq={self._seq})"
