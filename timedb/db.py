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

    @staticmethod
    def open_readonly(path: str | Path) -> "TimeDB":
        return TimeDB(Log.open(Path(path), False))

    @staticmethod
    def in_memory(strict_time: bool = False) -> "WritableTimeDB":
        return WritableTimeDB(Log([], strict_time))

    @staticmethod
    def open(path: str | Path, strict_time: bool = False) -> "DurableTimeDB":
        path = Path(path)
        lock_fd = _acquire_lock(path)
        try:
            log = DurableLog.open(path, strict_time)
        except BaseException:
            _release_lock(lock_fd)
            raise
        return DurableTimeDB(log, lock_fd)

    def at(self, tx: int) -> Snapshot:
        n = self._log.num_transactions()
        if not (0 <= tx < n):
            raise ValueError(
                f"invalid transaction {tx}: log has {n} transactions"
            )
        return Snapshot(tx, self._log)

    def latest(self) -> Snapshot:
        return self.at(self._log.num_transactions())

    def as_of(self, t: datetime) -> Snapshot:
        return self.at(self._log.tx_as_of(t))

    def events(self) -> Sequence[Event]:
        return self._log.events()

    def events_in_tx(self, tx: int) -> Iterator[Event]:
        if not 0 <= tx < self._log.num_transactions():
            raise ValueError(f"Transaction {tx} does not exist")
        return self._log.events_at(tx)

    def between(self, tx0: int, tx1: int) -> Interval:
        n = self._log.num_transactions()
        if not (0 <= tx0 <= tx1 <= n):
            raise ValueError(
                f"invalid range: tx0={tx0}, tx1={tx1}, "
                f"log has {n} transactions"
            )
        return Interval(self._log, tx0, tx1)

    def __repr__(self) -> str:
        n = self._log.num_transactions()
        return f"TimeDB({n} {'transactions' if n != 1 else 'transaction'})"


class WritableTimeDB(TimeDB):
    def apply(
        self, writes: dict[str, OptValue], at: Optional[datetime] = None
    ) -> int:
        if at is None:
            at = datetime.now()
        return self._log.append(writes, at)


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
