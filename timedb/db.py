import fcntl
import os
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional, Sequence

from .types import Event, OptValue, ABSENT
from .log import Log, DurableLog
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
    def __init__(self, log: Log):
        self._log = log

    @classmethod
    def open_readonly(cls, path: str | Path) -> "TimeDB":
        return TimeDB(Log.open(Path(path), False))

    @classmethod
    def in_memory(cls, strict_time: bool = False) -> "WritableTimeDB":
        return WritableTimeDB(Log.in_memory(strict_time))

    @classmethod
    def open(
        cls, path: str | Path, strict_time: bool = False
    ) -> "DurableTimeDB":
        path = Path(path)
        lock_fd = _acquire_lock(path)
        try:
            log = Log.open(path, strict_time)
        except BaseException:
            _release_lock(lock_fd)
            raise
        return DurableTimeDB(log, lock_fd)

    def at(self, seq: int) -> Snapshot:
        n = len(self.events())
        if not (0 <= seq <= n):
            raise ValueError(
                f"invalid seq {seq}: log has {n} events"
            )
        return Snapshot(
            seq,
            self._log.states()[seq],
            self.events()[seq - 1].timestamp if seq > 0 else None
        )

    def latest(self) -> Snapshot:
        return self.at(len(self.events()))

    def as_of(self, t: datetime) -> Snapshot:
        return self.at(self._log.seq_as_of(t))

    def events(self) -> Sequence[Event]:
        return self._log.events()

    def between(self, seq0: int, seq1: int) -> Interval:
        n = len(self.events())
        if not (0 <= seq0 <= seq1 <= n):
            raise ValueError(
                f"invalid range: seq0={seq0}, seq1={seq1}, "
                f"log has {n} events"
            )
        return Interval(self._log, seq0, seq1)

    def history(self, key: str) -> Iterator[Event]:
        return self.between(0, len(self.events())).history(key)

    def tx_events(self, tx_id: int) -> Iterator[Event]:
        if not self._log.tx_exists(tx_id):
            raise ValueError(f"Transaction {tx_id} does not exist")
        return self._log.tx_events(tx_id)

    def __repr__(self) -> str:
        n = len(self.events())
        return f"TimeDB({n} {'events' if n != 1 else 'event'})"


class WritableTimeDB(TimeDB):
    def __init__(self, log: Log):
        super().__init__(log)

    def put(
        self, key: str, value: OptValue, at: Optional[datetime] = None
    ) -> int:
        if at is None:
            at = datetime.now()
        return self._log.append({key: value}, at, tx=False)[0]

    def delete(self, key: str, at: Optional[datetime] = None) -> int:
        return self.put(key, ABSENT, at)

    def apply(
        self, writes: dict[str, OptValue], at: Optional[datetime] = None
    ) -> tuple[int, int]:
        if at is None:
            at = datetime.now()
        result = self._log.append(writes, at, tx=True)
        return result


class DurableTimeDB(WritableTimeDB):
    _log: DurableLog

    def __init__(self, log: DurableLog, lock_fd: int):
        super().__init__(log)
        self._lock_fd = lock_fd

    def save(self):
        self._log.save()

    def __enter__(self) -> "DurableTimeDB":
        return self

    def __exit__(self, *exc):
        try:
            self.save()
        finally:
            _release_lock(self._lock_fd)
