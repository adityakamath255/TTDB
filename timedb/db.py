import fcntl
import os
from bisect import bisect_right
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional, Sequence
from itertools import accumulate

import immutables

from .types import Event, OptValue, fold_event
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


class DB:
    def __init__(self, events: Iterable[Event] = ()):
        self._events = list(events)
        self._roots = list(
            accumulate(self._events, fold_event, initial=immutables.Map())
        )

    def at(self, seq: int) -> immutables.Map:
        n = len(self._events)
        if not (0 <= seq < n):
            raise ValueError(
                f"invalid seq {seq}: log has {n} events"
            )
        return self._roots[seq]

    def latest(self) -> immutables.Map:
        return self._roots[-1]

    def as_of(self, t: datetime) -> immutables.Map:
        return self._roots[
            bisect_right(self._events, t, key=lambda e: e.timestamp)
        ]

    def events(self) -> Sequence[Event]:
        return self._events


class WritableDB(DB):
    def _on_event(self, event: Event):
        pass

    def apply(
        self, changes: dict[str, OptValue], at: Optional[datetime] = None
    ) -> int:
        if not changes:
            raise ValueError("apply called with no writes")
        if at is None:
            at = datetime.now()
        if self._events and at < self._events[-1].timestamp:
            raise ValueError(
                f"timestamp {at.isoformat()} is before last "
                f"event timestamp {self._events[-1].timestamp.isoformat()}"
            )
        seq = len(self._events)
        event = Event(seq, changes, at)
        self._events.append(event)
        self._roots.append(fold_event(self._roots[-1], event))
        self._on_event(event)
        return seq


class DurableDB(WritableDB):
    def __init__(self, log_path: Path):
        self._log_path = log_path
        self._pending: list[Event] = []
        self._lock_fd = _acquire_lock(self._log_path)
        try:
            super().__init__(journal.read_and_recover(self._log_path))
        except BaseException:
            _release_lock(self._lock_fd)
            raise

    def _on_event(self, event: Event):
        self._pending.append(event)

    def save(self):
        if self._pending:
            journal.append_events(self._log_path, self._pending)
            self._pending.clear()

    def __enter__(self) -> "DurableDB":
        return self

    def __exit__(self, *exc):
        try:
            self.save()
        finally:
            _release_lock(self._lock_fd)


def open(log_path: str | Path) -> DurableDB:
    return DurableDB(Path(log_path))


def open_readonly(path: str | Path) -> DB:
    return DB(journal.read(Path(path)))


def in_memory() -> WritableDB:
    return WritableDB()
