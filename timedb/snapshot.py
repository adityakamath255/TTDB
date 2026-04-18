from typing import Optional, Any, Iterator
from datetime import datetime

from .types import Event, ABSENT


class Snapshot:
    def __init__(self, log: list[Event], through: int):
        self._log = log
        self._through = through

    def get(self, key: str) -> Any:
        for i in range(self._through, -1, -1):
            if self._log[i].key == key:
                return self._log[i].value
        else:
            return ABSENT

    def timestamp(self) -> Optional[datetime]:
        if self._through < 0:
            return None
        else:
            return self._log[self._through].timestamp

    def exists(self, key: str) -> bool:
        return self.get(key) is not ABSENT

    def keys(self) -> Iterator[str]:
        return (key for key, _ in self.items())

    def values(self) -> Iterator[Any]:
        return (value for _, value in self.items())

    def items(self) -> Iterator[tuple[str, Any]]:
        return ItemsIterator(self._log, self._through)

    def len(self) -> int:
        return sum(1 for _ in self.keys())

    def __repr__(self) -> str:
        return f"Snapshot(seq={self._through})"


class ItemsIterator:
    def __init__(self, log: list[Event], through: int) -> None:
        self._log = log
        self._through = through
        self._pos = through
        self._seen: set[str] = set()

    def __iter__(self) -> "ItemsIterator":
        return self

    def __next__(self) -> tuple[str, Any]:
        while self._pos >= 0:
            event = self._log[self._pos]
            self._pos -= 1
            if event.key not in self._seen:
                self._seen.add(event.key)
                if event.value is not ABSENT:
                    return event.key, event.value
        raise StopIteration
