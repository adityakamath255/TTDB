import fcntl
import os
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional

from .types import Event, OptValue, ABSENT
from .log import Log
from .snapshot import Snapshot
from .interval import Interval


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
    def __init__(self, log: Log, readonly: bool, lock_fd: Optional[int]):
        self._log = log
        self._readonly = readonly
        self._lock_fd = lock_fd

    @classmethod
    def in_memory(cls, strict_time: bool = False) -> "TimeDB":
        return cls(Log.in_memory(strict_time), False, None)

    @classmethod
    def open_readonly(cls, path: str | Path) -> "TimeDB":
        return cls(Log.open_readonly(Path(path)), True, None)

    @classmethod
    def open(cls, path: str | Path, strict_time: bool = False) -> "TimeDB":
        path = Path(path)
        lock_fd = _acquire_lock(path)
        try:
            log = Log.open(path, strict_time)
        except BaseException:
            _release_lock(lock_fd)
            raise
        return cls(log, False, lock_fd)

    def save(self):
        self._log.save()

    def __enter__(self) -> "TimeDB":
        return self

    def __exit__(self, *exc):
        try:
            self._log.save()
        finally:
            if self._lock_fd is not None:
                _release_lock(self._lock_fd)

    def put(
        self, key: str, value: OptValue, at: Optional[datetime] = None
    ) -> int:
        if self._readonly:
            raise PermissionError("database is open read-only")
        if at is None:
            at = datetime.now()
        return self._log.append(key, value, at)

    def delete(self, key: str) -> int:
        return self.put(key, ABSENT)

    def at(self, seq: int) -> Snapshot:
        n = self._log.count()
        if not (0 <= seq <= n):
            raise ValueError(
                f"invalid seq {seq}: log has {n} events"
            )
        return Snapshot(self._log, seq)

    def latest(self) -> Snapshot:
        return Snapshot(self._log, self._log.count())

    def at_time(self, t: datetime) -> Snapshot:
        return Snapshot(self._log, self._log.seq_as_of(t))

    def events(self) -> Iterator[Event]:
        return self._log.events_between(0, self._log.count())

    def between(self, seq0: int, seq1: int) -> Interval:
        n = self._log.count()
        if not (0 <= seq0 <= seq1 <= n):
            raise ValueError(
                f"invalid range: seq0={seq0}, seq1={seq1}, "
                f"log has {n} events"
            )
        return Interval(self._log, seq0, seq1)

    def history(self, key: str) -> Iterator[Event]:
        return self.between(0, self._log.count()).history(key)

    def apply(self, writes: dict[str, OptValue]) -> tuple[int, int]:
        return self.apply_at(writes, datetime.now())

    def apply_at(
        self, writes: dict[str, OptValue], at: datetime
    ) -> tuple[int, int]:
        if self._readonly:
            raise PermissionError("database is open read-only")
        if not writes:
            raise ValueError("apply requires a non-empty writes dict")
        result = self._log.append_tx(writes, at)
        return result

    def tx_events(self, tx_id: int) -> Iterator[Event]:
        if not self._log.tx_exists(tx_id):
            raise ValueError(f"Transaction {tx_id} does not exist")
        return self._log.tx_events(tx_id)

    def __repr__(self) -> str:
        n = self._log.count()
        return f"TimeDB({n} {'events' if n != 1 else 'event'})"
