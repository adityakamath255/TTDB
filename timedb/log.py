from typing import Any, Optional, Iterator, Sequence
from threading import Lock
from datetime import datetime
from bisect import bisect_right

from .types import Event, ABSENT


class Log:
    def __init__(self, events: list[Event]):
        self._events = events
        self._index: dict[str, list[int]] = {}
        self._lock = Lock()
        for event in events:
            self._index.setdefault(event.key, []).append(event.seq)

    def _append(self, event: Event):
        self._events.append(event)
        self._index.setdefault(event.key, []).append(event.seq)

    def write(self, key: str, value: Any, at: datetime) -> int:
        with self._lock:
            event = Event(len(self._events), key, value, at, None)
            self._append(event)
            return event.seq

    def write_tx(
        self, writes: dict[str, Any], at: datetime
    ) -> tuple[int, int]:
        with self._lock:
            tx_id = len(self._events)
            for key, value in writes.items():
                event = Event(len(self._events), key, value, at, tx_id)
                self._append(event)
            return (tx_id, self._events[-1].seq)

    def get(self, seq: int) -> Optional[Event]:
        if seq < 0:
            return None
        else:
            return self._events[seq]

    def events(self) -> Sequence[Event]:
        return self._events

    def events_between(self, lo: int, hi: int) -> Iterator[Event]:
        return (self._events[i] for i in range(max(0, lo), hi + 1))

    def tx_exists(self, tx_id: int) -> bool:
        return (
            0 <= tx_id < len(self._events)
            and self._events[tx_id].tx_id == tx_id
        )

    def tx_events(self, tx_id: int) -> Iterator[Event]:
        i = tx_id
        while (
            i < len(self._events)
            and self._events[i].tx_id == tx_id
        ):
            yield self._events[i]
            i += 1

    def resolve(self, key: str, through: int) -> Any:
        seqs = self._index.get(key, ())
        idx = bisect_right(seqs, through) - 1
        if idx < 0:
            return ABSENT
        else:
            return self._events[seqs[idx]].value

    def latest(self) -> int:
        with self._lock:
            return len(self._events) - 1

    def items(self, through: int) -> Iterator[tuple[str, Any]]:
        seen: set[str] = set()

        for ix in range(through, -1, -1):
            event = self._events[ix]
            
            if event.key not in seen:
                seen.add(event.key)

                if event.value is not ABSENT:
                    yield (event.key, event.value)

    def history(self, key: str, lo: int, hi: int) -> Iterator[Event]:
        return (
            self._events[ix] 
            for ix in self._index.get(key, ())
            if lo < ix <= hi
        )

    def diff(self, lo: int, hi: int) -> Iterator[tuple[str, Any, Any]]:
        lo = max(-1, lo)
        seen: set[str] = set()

        for ix in range(hi, lo, -1):
            event = self._events[ix]

            if event.key not in seen:
                seen.add(event.key)
                prev_val = self.resolve(event.key, lo)

                if event.value != prev_val:
                    yield (event.key, prev_val, event.value)
