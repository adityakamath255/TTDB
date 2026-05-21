import fcntl
import os
from bisect import bisect_left, bisect_right
from datetime import datetime
from pathlib import Path
from typing import Iterator, Iterable, Optional, Sequence, Callable
from itertools import accumulate, groupby, islice, takewhile

import immutables

from .types import Event, OptValue, EMPTY, fold_batch
from . import journal


def _acquire_lock(path: Path) -> int:
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        raise RuntimeError(f"database at {path} is locked by another process")
    return fd


def _release_lock(fd: int):
    fcntl.flock(fd, fcntl.LOCK_UN)
    os.close(fd)


class TimeDB:
    def __init__(self, events: Iterable[Event] = ()):
        self._events = list(events)
        batches = [
            list(g) 
            for _, g in groupby(self._events, key=lambda e: e.tx)
        ]
        self._roots = list(accumulate(batches, fold_batch, initial=EMPTY))
        self._tx_timestamps = [b[0].timestamp for b in batches]

    @staticmethod
    def open_readonly(path: str | Path) -> "TimeDB":
        return TimeDB(journal.read(Path(path)))

    @staticmethod
    def in_memory() -> "WritableTimeDB":
        return WritableTimeDB()

    @staticmethod
    def open(log_path: str | Path) -> "DurableTimeDB":
        return DurableTimeDB(Path(log_path))

    def _validate_tx(self, tx: int):
        n = self.num_transactions()
        if not (0 <= tx < n):
            raise ValueError(
                f"invalid transaction {tx}: log has {n} transactions"
            )

    def _scan_from(
        self, tx_lo: int, predicate: Callable[[Event], bool]
    ) -> Iterator[Event]:
        lo = bisect_left(self._events, tx_lo, key=lambda e: e.tx)
        return takewhile(predicate, islice(self._events, lo, None))

    def num_transactions(self) -> int:
        return len(self._tx_timestamps)

    def at(self, tx: int) -> immutables.Map:
        self._validate_tx(tx)
        return self._roots[tx + 1]

    def latest(self) -> immutables.Map:
        return self._roots[self.num_transactions()]

    def as_of(self, t: datetime) -> immutables.Map:
        return self._roots[bisect_right(self._tx_timestamps, t)]

    def events(self) -> Sequence[Event]:
        return self._events

    def events_at(self, tx: int) -> Iterator[Event]:
        self._validate_tx(tx)
        return self._scan_from(tx, lambda e: e.tx == tx)

    def between(self, tx_lo: int, tx_hi: int) -> Iterator[Event]:
        n = self.num_transactions()
        if not (0 <= tx_lo <= tx_hi <= n):
            raise ValueError(
                f"invalid range: tx_lo={tx_lo}, tx_hi={tx_hi}, "
                f"log has {n} transactions"
            )
        return self._scan_from(tx_lo, lambda e: e.tx < tx_hi)

    def tx_timestamp(self, tx: int) -> datetime:
        self._validate_tx(tx)
        return self._tx_timestamps[tx]

    def __repr__(self) -> str:
        n = self.num_transactions()
        return f"TimeDB({n} {'transactions' if n != 1 else 'transaction'})"


class WritableTimeDB(TimeDB):
    def _on_batch(self, batch: list[Event]):
        pass

    def apply(
        self, writes: dict[str, OptValue], at: Optional[datetime] = None
    ) -> int:
        if not writes:
            raise ValueError("apply called with no writes")
        if at is None:
            at = datetime.now()
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


class DurableTimeDB(WritableTimeDB):
    def __init__(self, log_path: Path):
        self._log_path = log_path
        self._pending: list[list[Event]] = []
        self._lock_fd = _acquire_lock(self._log_path)
        try:
            super().__init__(journal.read_and_recover(self._log_path))
        except BaseException:
            _release_lock(self._lock_fd)
            raise

    def _on_batch(self, batch: list[Event]):
        self._pending.append(batch)

    def save(self):
        if self._pending:
            journal.append_batches(self._log_path, self._pending)
            self._pending.clear()

    def __enter__(self) -> "DurableTimeDB":
        return self

    def __exit__(self, *exc):
        try:
            self.save()
        finally:
            _release_lock(self._lock_fd)
