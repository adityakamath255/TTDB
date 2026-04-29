from typing import Optional, Any, Iterator
from datetime import datetime

from .types import ABSENT
from .log import Log


class Snapshot:
    def __init__(self, log: Log, through: int):
        self._log = log
        self._seq = through
        self._root = log.root_at(through)

    def seq(self) -> int:
        return self._seq

    def get(self, key: str) -> Any:
        return self._root.get(key, ABSENT)

    def timestamp(self) -> Optional[datetime]:
        if self._seq == 0:
            return None
        else:
            return self._log.event_at(self._seq - 1).timestamp

    def keys(self) -> Iterator[str]:
        return iter(self._root)

    def items(self) -> Iterator[tuple[str, Any]]:
        return iter(self._root.items())

    def diff(self, other: "Snapshot") -> Iterator[tuple[str, Any, Any]]:
        lo, hi = self._root, other._root
        for k in set(lo) | set(hi):
            lo_val = lo.get(k, ABSENT)
            hi_val = hi.get(k, ABSENT)
            if lo_val != hi_val:
                yield (k, lo_val, hi_val)

    def __contains__(self, key: str) -> bool:
        return self.get(key) is not ABSENT

    def __repr__(self) -> str:
        return f"Snapshot(seq={self._seq})"
