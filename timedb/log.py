from bisect import bisect_left, bisect_right
from itertools import accumulate, groupby, islice, takewhile
from typing import Iterable, Iterator, Sequence, Optional
from pathlib import Path
from datetime import datetime

import immutables

from . import journal
from .types import Event, OptValue, EMPTY, fold_batch


class Log:
    def __init__(self, events: Iterable[Event]):
        self._events = list(events)
        batches = [
            list(g) 
            for _, g in groupby(
                self._events,
                key=lambda e: e.tx
            )
        ]
        self._roots = list(accumulate(batches, fold_batch, initial=EMPTY))
        self._tx_timestamps = [b[0].timestamp for b in batches]

    @staticmethod
    def open(log_path: Path) -> "Log":
        return Log(journal.read(log_path))

    @staticmethod
    def in_memory() -> "Log":
        return Log([])

    def _on_batch(self, batch: list[Event]):
        pass

    def events(self) -> Sequence[Event]:
        return self._events

    def states(self) -> Sequence[immutables.Map]:
        return self._roots

    def num_transactions(self) -> int:
        return len(self._tx_timestamps)

    def tx_timestamp(self, tx: int) -> datetime:
        return self._tx_timestamps[tx]

    def append(self, writes: dict[str, OptValue], at: datetime) -> int:
        if not writes:
            raise ValueError("apply called with no writes")
        if self._events and at < self._events[-1].timestamp:
            raise ValueError(
                f"timestamp {at.isoformat()} is before last "
                f"event timestamp {self._events[-1].timestamp.isoformat()}"
            )
        tx = self.num_transactions()
        batch = [Event(tx, k, v, at) for k, v in writes.items()]
        self._events.extend(batch)
        self._roots.append(fold_batch(self._roots[-1], batch))
        self._tx_timestamps.append(at)
        self._on_batch(batch)
        return tx

    def events_at(self, tx: int) -> Iterator[Event]:
        lo = bisect_left(self._events, tx, key=lambda e: e.tx)
        return takewhile(
            lambda e: e.tx == tx,
            islice(self._events, lo, None)
        )

    def events_between(self, tx_lo: int, tx_hi: int) -> Iterator[Event]:
        lo = bisect_left(self._events, tx_lo, key=lambda e: e.tx)
        return takewhile(
            lambda e: e.tx < tx_hi,
            islice(self._events, lo, None)
        )

    def last_tx(self, t: datetime) -> Optional[int]:
        n = bisect_right(self._tx_timestamps, t)
        return n - 1 if n > 0 else None


class DurableLog(Log):
    def __init__(
        self, events: Iterable[Event], log_path: Path
    ):
        super().__init__(events)
        self._log_path = log_path
        self._pending: list[list[Event]] = []

    @staticmethod
    def open(log_path: Path) -> "DurableLog":
        return DurableLog(
            journal.read_and_recover(log_path), log_path
        )

    def _on_batch(self, batch: list[Event]):
        self._pending.append(batch)

    def save(self):
        if self._pending:
            journal.append_batches(self._log_path, self._pending)
            self._pending.clear()
