from bisect import bisect_right
from itertools import accumulate
from typing import Optional, Iterable, Iterator, Sequence
from pathlib import Path
from datetime import datetime

import immutables

from . import journal
from .types import Event, OptValue, EMPTY, fold_event


class Log:
    def __init__(self, events: Iterable[Event], strict_time: bool):
        self._events = list(events)
        self._roots = list(accumulate(self._events, fold_event, initial=EMPTY))
        self._strict_time = strict_time

    @classmethod
    def in_memory(cls, strict_time: bool) -> "Log":
        return cls([], strict_time)

    @classmethod
    def open(cls, log_path: Path, strict_time: bool) -> "DurableLog":
        return DurableLog(
            journal.read_and_recover(log_path), strict_time, log_path
        )

    def append(
        self, writes: dict[str, OptValue], at: datetime, tx: bool
    ) -> tuple[int, int]:
        first_seq = len(self._events)
        tx_id = len(self._events) if tx else None
        for key, value in writes.items():
            self._publish(key, value, at, tx_id)
        return (first_seq, len(self._events))

    def _publish(
        self, 
        key: str, 
        value: OptValue, 
        at: datetime, 
        tx_id: Optional[int]
    ):
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

    def events(self) -> Sequence[Event]:
        return self._events

    def states(self) -> Sequence[immutables.Map]:
        return self._roots

    def seq_as_of(self, t: datetime) -> int:
        if self._events and t >= self._events[0].timestamp:
            return bisect_right(self._events, t, key=lambda e: e.timestamp)
        else:
            return 0

    def tx_exists(self, tx_id: int) -> bool:
        return (
            0 <= tx_id < len(self._events)
            and self._events[tx_id].tx_id == tx_id
        )

    def tx_events(self, tx_id: int) -> Iterator[Event]:
        for i in range(tx_id, len(self._events)):
            e = self._events[i]
            if e.tx_id != tx_id:
                return
            yield e


class DurableLog(Log):
    def __init__(
        self, events: Iterable[Event], strict_time: bool, log_path: Path
    ):
        super().__init__(events, strict_time)
        self._log_path = log_path
        self._persisted_count = len(self._events)

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
