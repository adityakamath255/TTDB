from typing import Any, Optional, Iterator, Sequence, Iterable
from threading import Lock
from datetime import datetime
from contextlib import nullcontext, AbstractContextManager

import immutables

from .types import Event, ABSENT


_EMPTY = immutables.Map()


class Log:
    def __init__(self, listener: AbstractContextManager):
        self._listener = listener
        self._events: list[Event] = []
        self._roots: list[immutables.Map] = []
        self._latest_per_key: dict[str, int] = {}
        self._lock = Lock()

    @classmethod
    def in_memory(cls) -> "Log":
        return cls(nullcontext())

    def attach(self, listener: AbstractContextManager):
        self._listener = listener

    def replay(self, events: Iterable[Event]):
        for e in events:
            self._index(e)

    def _index(self, event: Event):
        prev_root = self._roots[-1] if self._roots else _EMPTY
        if event.value is not ABSENT:
            new_root = prev_root.set(event.key, event.value)
        elif event.key in prev_root:
            new_root = prev_root.delete(event.key)
        else:
            new_root = prev_root
        self._events.append(event)
        self._roots.append(new_root)
        self._latest_per_key[event.key] = event.seq

    def _publish(
        self, key: str, value: Any, at: datetime, tx_id: Optional[int]
    ) -> int:
        event = Event(
            seq=len(self._events),
            key=key,
            value=value,
            timestamp=at,
            tx_id=tx_id,
            prev_seq=self._latest_per_key.get(key),
        )
        self._index(event)
        return event.seq

    def write(self, key: str, value: Any, at: datetime) -> int:
        with self._listener, self._lock:
            return self._publish(key, value, at, None)

    def write_tx(
        self, writes: dict[str, Any], at: datetime
    ) -> tuple[int, int]:
        with self._listener, self._lock:
            tx_id = len(self._events)
            last_seq = tx_id
            for key, value in writes.items():
                last_seq = self._publish(key, value, at, tx_id)
            return (tx_id, last_seq)

    def root_at(self, seq: int) -> immutables.Map:
        return _EMPTY if seq < 0 else self._roots[seq]

    def get(self, seq: int) -> Optional[Event]:
        return None if seq < 0 else self._events[seq]

    def events(self) -> Sequence[Event]:
        return self._events

    def events_between(self, lo: int, hi: int) -> Iterator[Event]:
        return (self._events[i] for i in range(max(0, lo + 1), hi + 1))

    def latest(self) -> int:
        with self._lock:
            return len(self._events) - 1

    def tx_exists(self, tx_id: int) -> bool:
        return (
            0 <= tx_id < len(self._events)
            and self._events[tx_id].tx_id == tx_id
        )

    def tx_events(self, tx_id: int) -> Iterator[Event]:
        i = tx_id
        while i < len(self._events) and self._events[i].tx_id == tx_id:
            yield self._events[i]
            i += 1
