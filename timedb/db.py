import fcntl
import os
from datetime import datetime
from pathlib import Path
from typing import Iterator, Iterable, Optional, Sequence

import immutables

from .types import Event, OptValue, ABSENT, EMPTY
from .log import Log, DurableLog


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
        return TimeDB(Log.open(Path(path)))

    @staticmethod
    def in_memory() -> "WritableTimeDB":
        return WritableTimeDB(Log.in_memory())

    @staticmethod
    def open(path: str | Path) -> "DurableTimeDB":
        path = Path(path)
        lock_fd = _acquire_lock(path)
        try:
            log = DurableLog.open(path)
        except BaseException:
            _release_lock(lock_fd)
            raise
        return DurableTimeDB(log, lock_fd)

    def at(self, tx: int) -> immutables.Map:
        n = self._log.num_transactions()
        if not (0 <= tx < n):
            raise ValueError(
                f"invalid transaction {tx}: log has {n} transactions"
            )
        return self._log.states()[tx + 1]

    def latest(self) -> immutables.Map:
        return self.at(self._log.num_transactions() - 1)

    def as_of(self, t: datetime) -> immutables.Map:
        tx = self._log.last_tx(t)
        return EMPTY if tx is None else self.at(tx)

    def events(self) -> Sequence[Event]:
        return self._log.events()

    def events_at(self, tx: int) -> Iterator[Event]:
        if not 0 <= tx < self._log.num_transactions():
            raise ValueError(f"Transaction {tx} does not exist")
        return self._log.events_at(tx)

    def between(self, tx0: int, tx1: int) -> Iterator[Event]:
        n = self._log.num_transactions()
        if not (0 <= tx0 <= tx1 <= n):
            raise ValueError(
                f"invalid range: tx0={tx0}, tx1={tx1}, "
                f"log has {n} transactions"
            )
        return self._log.events_between(tx0, tx1)

    def tx_timestamp(self, tx: int) -> datetime:
        if not 0 <= tx < self._log.num_transactions():
            raise ValueError(f"Transaction {tx} does not exist")
        return self._log.tx_timestamp(tx)

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


def diff(
    a: immutables.Map, b: immutables.Map
) -> Iterator[tuple[str, OptValue, OptValue]]:
    for k in set(a) | set(b):
        a_val = a.get(k, ABSENT)
        b_val = b.get(k, ABSENT)
        if a_val != b_val:
            yield (k, a_val, b_val)


def history(events: Iterable[Event], key: str) -> Iterator[Event]:
    return (e for e in events if e.key == key)
