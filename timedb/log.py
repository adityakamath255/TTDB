from bisect import bisect_right
from itertools import accumulate
from typing import Optional, Iterator
from pathlib import Path
from datetime import datetime

import immutables

from . import journal
from .types import Event, OptValue, EMPTY, fold_event


class Log:
    def __init__(
        self, events: list[Event], log_path: Optional[Path], strict_time: bool,
    ):
        self._events = list(events)
        self._roots = list(accumulate(self._events, fold_event, initial=EMPTY))
        self._log_path = log_path
        self._strict_time = strict_time
        self._persisted_count = len(self._events)

    @classmethod
    def in_memory(cls, strict_time: bool) -> "Log":
        return cls([], None, strict_time)

    @classmethod
    def open(cls, log_path: Path, strict_time: bool) -> "Log":
        return cls(journal.read_and_recover(log_path), log_path, strict_time)

    @classmethod
    def open_readonly(cls, log_path: Path) -> "Log":
        return cls(journal.read(log_path), None, False)

    def append(self, key: str, value: OptValue, at: datetime) -> int:
        return self._publish(key, value, at, None)

    def append_tx(
        self, writes: dict[str, OptValue], at: datetime
    ) -> tuple[int, int]:
        tx_id = len(self._events)
        for key, value in writes.items():
            self._publish(key, value, at, tx_id)
        return (tx_id, len(self._events))

    def _publish(
        self, 
        key: str, 
        value: OptValue, 
        at: datetime, 
        tx_id: Optional[int]
    ) -> int:
        if (
            self._strict_time 
            and self._events 
            and at < self._events[-1].timestamp
        ):
            raise ValueError(
                f"strict_time: timestamp {at.isoformat()} is before last "
                f"event timestamp {self._events[-1].timestamp.isoformat()}"
            )

        event = Event(
            len(self._events), key, value, at, tx_id
        )
        self._events.append(event)
        self._roots.append(fold_event(self._roots[-1], event))
        return len(self._events) - 1

    def count(self) -> int:
        return len(self._events)

    def state_at(self, seq: int) -> immutables.Map:
        return self._roots[seq]

    def event_at(self, seq: int) -> Event:
        return self._events[seq]

    def events_between(self, lo: int, hi: int) -> Iterator[Event]:
        for i in range(lo, hi):
            yield self._events[i]

    def seq_as_of(self, t: datetime) -> int:
        if self._events and t >= self._events[0].timestamp:
            return bisect_right(self._events, t, key=lambda e: e.timestamp)
        else:
            return 0

    def tx_exists(self, tx_id: int) -> bool:
        return (
            0 <= tx_id < self.count()
            and self.event_at(tx_id).tx_id == tx_id
        )

    def tx_events(self, tx_id: int) -> Iterator[Event]:
        for e in self.events_between(tx_id, self.count()):
            if e.tx_id != tx_id:
                return
            yield e

    def save(self):
        if (
            self._log_path is not None
            and len(self._events) > self._persisted_count
        ):
            journal.append_events(
                self._log_path,
                self._events[self._persisted_count:]
            )
            self._persisted_count = len(self._events)
