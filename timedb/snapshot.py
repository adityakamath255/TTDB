from typing import Optional, Any, Iterator
from datetime import datetime

from .types import ABSENT
from .log import Log


class Snapshot:
    def __init__(self, log: Log, through: int):
        self._log = log
        self._through = through

    def get(self, key: str) -> Any:
        return self._log.resolve(key, self._through)

    def timestamp(self) -> Optional[datetime]:
        if self._through < 0:
            return None
        else:
            return self._log.events[self._through].timestamp

    def keys(self) -> Iterator[str]:
        return (key for key, _ in self.items())

    def items(self) -> Iterator[tuple[str, Any]]:
        for key in self._log.index:
            value = self._log.resolve(key, self._through)
            if value is not ABSENT:
                yield key, value

    def __len__(self) -> int:
        return sum(1 for _ in self.items())

    def __contains__(self, key: str) -> bool:
        return self.get(key) is not ABSENT

    def __repr__(self) -> str:
        return f"Snapshot(seq={self._through})"
