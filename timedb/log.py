from typing import Any, Optional, Iterator, Iterable
from threading import Lock
from datetime import datetime

import immutables

from .types import Event, apply


_EMPTY = immutables.Map()


class Log:
    def __init__(self):
        self._saver: Any = None
        self._events: list[Event] = []
        self._active_first_seq: int = 0
        self._active_base_root: immutables.Map = _EMPTY
        self._latest_root: immutables.Map = _EMPTY
        self._lock = Lock()

    @classmethod
    def in_memory(cls) -> "Log":
        return cls()

    def attach(self, saver):
        self._saver = saver

    def bootstrap(
        self,
        active_events: list[Event],
        latest_root: immutables.Map,
        active_base_root: immutables.Map,
        active_first_seq: int,
    ):
        self._events = list(active_events)
        self._latest_root = latest_root
        self._active_base_root = active_base_root
        self._active_first_seq = active_first_seq

    def truncate_active(
        self, new_first_seq: int, new_base_root: immutables.Map
    ):
        with self._lock:
            drop = new_first_seq - self._active_first_seq
            self._events = self._events[drop:]
            self._active_first_seq = new_first_seq
            self._active_base_root = new_base_root

    def _index(self, event: Event):
        self._latest_root = apply(self._latest_root, event)
        self._events.append(event)

    def _publish(
        self, key: str, value: Any, at: datetime, tx_id: Optional[int]
    ) -> int:
        seq = self._active_first_seq + len(self._events)
        event = Event(
            seq=seq,
            key=key,
            value=value,
            timestamp=at,
            tx_id=tx_id,
        )
        self._index(event)
        return seq

    def write(self, key: str, value: Any, at: datetime) -> int:
        if self._saver is not None:
            self._saver.before_write()
        with self._lock:
            seq = self._publish(key, value, at, None)
        if self._saver is not None:
            self._saver.after_write()
        return seq

    def write_tx(
        self, writes: dict[str, Any], at: datetime
    ) -> tuple[int, int]:
        if self._saver is not None:
            self._saver.before_write()
        with self._lock:
            tx_id = self._active_first_seq + len(self._events)
            for key, value in writes.items():
                self._publish(key, value, at, tx_id)
            end = self._active_first_seq + len(self._events)
        if self._saver is not None:
            self._saver.after_write()
        return (tx_id, end)

    @staticmethod
    def _replay(base: immutables.Map, events: Iterable[Event]) -> immutables.Map:
        root = base
        for e in events:
            root = apply(root, e)
        return root

    def root_at(self, p: int) -> immutables.Map:
        with self._lock:
            count = self._active_first_seq + len(self._events)
            if p == count:
                return self._latest_root
            if p >= self._active_first_seq:
                base = self._active_base_root
                events_slice = self._events[: p - self._active_first_seq]
                return self._replay(base, events_slice)
        return self._saver.state_at(p)

    def event_at(self, seq: int) -> Event:
        with self._lock:
            if seq >= self._active_first_seq:
                return self._events[seq - self._active_first_seq]
        return self._saver.event_at(seq)

    def events(self) -> Iterator[Event]:
        with self._lock:
            active_first = self._active_first_seq
            active_snapshot = list(self._events)
        if self._saver is not None and active_first > 0:
            yield from self._saver.events_between(0, active_first)
        yield from active_snapshot

    def events_between(self, lo: int, hi: int) -> Iterator[Event]:
        with self._lock:
            active_first = self._active_first_seq
            active_snapshot = list(self._events)
        if lo < active_first and self._saver is not None:
            sealed_hi = min(hi, active_first)
            yield from self._saver.events_between(lo, sealed_hi)
        active_lo = max(lo, active_first)
        active_hi = min(hi, active_first + len(active_snapshot))
        for i in range(active_lo, active_hi):
            yield active_snapshot[i - active_first]

    def active_events(self) -> list[Event]:
        with self._lock:
            return list(self._events)

    def active_first_seq(self) -> int:
        with self._lock:
            return self._active_first_seq

    def count(self) -> int:
        with self._lock:
            return self._active_first_seq + len(self._events)

    def tx_exists(self, tx_id: int) -> bool:
        if tx_id < 0 or tx_id >= self.count():
            return False
        return self.event_at(tx_id).tx_id == tx_id

    def tx_events(self, tx_id: int) -> Iterator[Event]:
        i = tx_id
        n = self.count()
        while i < n:
            e = self.event_at(i)
            if e.tx_id != tx_id:
                return
            yield e
            i += 1
