from typing import Any, Optional, Iterator, Sequence
from datetime import datetime
from bisect import bisect_right

from .types import Event, ABSENT


class Log:
    def __init__(self, events: list[Event]):
        self._events = events
        self._index: dict[str, list[int]] = {}
        for event in events:
            self._index.setdefault(event.key, []).append(event.seq)

    def events(self) -> Sequence[Event]:
        return self._events

    def keys(self) -> Iterator[str]:
        return iter(self._index)

    def events_between(self, lo: int, hi: int) -> Iterator[Event]:
        lo = lo if lo >= -1 else -1
        return (self._events[i] for i in range(lo + 1, hi + 1))

    def append(
        self, key: str, value: Any, at: datetime, tx_id: Optional[int]
    ) -> int:
        event = Event(len(self._events), key, value, at, tx_id)
        self._events.append(event)
        self._index.setdefault(key, []).append(event.seq)
        return event.seq

    def append_transaction(
        self, writes: dict[str, Any], at: datetime
    ) -> tuple[int, int]:
        tx_id = len(self._events)
        last = tx_id
        for key, value in writes.items():
            last = self.append(key, value, at, tx_id)
        return (tx_id, last)

    def resolve(self, key: str, through: int) -> Any:
        seqs = self._index.get(key, ())
        idx = bisect_right(seqs, through) - 1
        if idx < 0:
            return ABSENT
        else:
            return self._events[seqs[idx]].value

    def history(self, key: str) -> Iterator[Event]:
        return (
            self._events[s] 
            for s in self._index.get(key, ())
        )

    def transaction_events(self, tx_id: int) -> Iterator[Event]:
        if (
            tx_id < 0 
            or tx_id >= len(self._events)
            or self._events[tx_id].tx_id != tx_id
        ):
            raise ValueError(f"no transaction with id {tx_id}")

        def walk():
            i = tx_id
            while (
                i < len(self._events)
                and self._events[i].tx_id == tx_id
            ):
                yield self._events[i]
                i += 1

        return walk()
