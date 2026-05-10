from bisect import bisect_right
from threading import Lock
from typing import Any, Iterable, Optional
from datetime import datetime

import immutables

from .types import Event, apply


class Buffer:
    def __init__(
        self,
        lo: int,
        base_state: immutables.Map,
        events: Iterable[Event] = (),
        strict_time: bool = False,
    ):
        self._lo = lo
        self._events: list[Event] = []
        self._roots: list[immutables.Map] = [base_state]
        self._lock = Lock()
        self._strict_time = strict_time
        self._last_ts: Optional[datetime] = None
        for e in events:
            self._events.append(e)
            self._roots.append(apply(self._roots[-1], e))
            self._last_ts = e.timestamp

    def next_seq(self) -> int:
        with self._lock:
            return self._lo + len(self._events)

    def snapshot(self) -> tuple[int, list[Event]]:
        with self._lock:
            return (self._lo, list(self._events))

    def append(self, key: str, value: Any, at: datetime) -> int:
        with self._lock:
            return self._publish(key, value, at, None)

    def append_tx(
        self, writes: dict[str, Any], at: datetime
    ) -> tuple[int, int]:
        with self._lock:
            tx_id = self._lo + len(self._events)
            for key, value in writes.items():
                self._publish(key, value, at, tx_id)
            end = self._lo + len(self._events)
            return (tx_id, end)

    def _publish(
        self, key: str, value: Any, at: datetime, tx_id: Optional[int]
    ) -> int:
        if self._strict_time and self._last_ts is not None and at < self._last_ts:
            raise ValueError(
                f"strict_time: timestamp {at.isoformat()} is before "
                f"last event timestamp {self._last_ts.isoformat()}"
            )
        seq = self._lo + len(self._events)
        event = Event(
            seq=seq, key=key, value=value, timestamp=at, tx_id=tx_id,
        )
        self._events.append(event)
        self._roots.append(apply(self._roots[-1], event))
        self._last_ts = at
        return seq

    def event_at(self, seq: int) -> Optional[Event]:
        with self._lock:
            idx = seq - self._lo
            if 0 <= idx < len(self._events):
                return self._events[idx]
            return None

    def state_at(self, p: int) -> Optional[immutables.Map]:
        with self._lock:
            idx = p - self._lo
            if 0 <= idx < len(self._roots):
                return self._roots[idx]
            return None

    def truncate_to(self, new_lo: int):
        with self._lock:
            drop = new_lo - self._lo
            self._events = self._events[drop:]
            self._roots = self._roots[drop:]
            self._lo = new_lo

    def position_at_time(self, t: datetime) -> Optional[int]:
        # Assumes timestamps are non-decreasing in seq order. Out-of-order
        # writes make at_time results undefined (per README); a backdated
        # buffer event will fall through to the archive and silently mislead.
        with self._lock:
            if not self._events or t < self._events[0].timestamp:
                return None
            idx = bisect_right(self._events, t, key=lambda e: e.timestamp)
            return self._lo + idx
