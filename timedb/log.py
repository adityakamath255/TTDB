from typing import Any, Optional, Iterator, Sequence
from threading import Lock
from datetime import datetime

import immutables

from .types import Event, ABSENT


_EMPTY = immutables.Map()


class Log:
    def __init__(self, events: list[Event]):
        self._events: list[Event] = []
        self._roots: list[immutables.Map] = []
        self._latest_per_key: dict[str, int] = {}
        self._lock = Lock()
        for e in events:
            self._publish(e.key, e.value, e.timestamp, e.tx_id)

    def _publish(
        self, key: str, value: Any, at: datetime, tx_id: Optional[int]
    ) -> int:
        if self._roots:
            prev_root = self._roots[-1] 
        else:
            prev_root = _EMPTY

        prev_seq = self._latest_per_key.get(key)
        seq = len(self._events)

        if value is ABSENT:
            if key in prev_root:
                new_root = prev_root.delete(key) 
            else:
                new_root = prev_root
        else:
            new_root = prev_root.set(key, value)

        self._events.append(Event(seq, key, value, at, tx_id, prev_seq))
        self._roots.append(new_root)
        self._latest_per_key[key] = seq

        return seq

    def write(self, key: str, value: Any, at: datetime) -> int:
        with self._lock:
            return self._publish(key, value, at, None)

    def write_tx(
        self, writes: dict[str, Any], at: datetime
    ) -> tuple[int, int]:
        with self._lock:
            tx_id = len(self._events)
            last_seq = tx_id
            for key, value in writes.items():
                last_seq = self._publish(key, value, at, tx_id)
            return (tx_id, last_seq)

    def root_at(self, seq: int) -> immutables.Map:
        if seq < 0:
            return _EMPTY
        else:
            return self._roots[seq]

    def get(self, seq: int) -> Optional[Event]:
        if seq < 0:
            return None
        else:
            return self._events[seq]

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
        while (
            i < len(self._events)
            and self._events[i].tx_id == tx_id
        ):
            yield self._events[i]
            i += 1

    def history(self, key: str, lo: int, hi: int) -> Iterator[Event]:
        chain: list[Event] = []
        seq = self._latest_per_key.get(key)
        while seq is not None and seq > hi:
            seq = self._events[seq].prev_seq
        while seq is not None and seq > lo:
            chain.append(self._events[seq])
            seq = self._events[seq].prev_seq
        return reversed(chain)
