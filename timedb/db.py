from datetime import datetime
from pathlib import Path
from typing import Any, Iterator, Optional

from .types import Event, ABSENT, validate_value
from .log import Log
from .saver import FsyncMode, OnError
from .snapshot import Snapshot
from .interval import Interval


class TimeDB:
    def __init__(self, log: Log, readonly: bool):
        self._log = log
        self._readonly = readonly

    @classmethod
    def in_memory(cls) -> "TimeDB":
        return cls(Log.in_memory(), False)

    @classmethod
    def open_readonly(cls, path: str | Path) -> "TimeDB":
        return cls(Log.open_readonly(Path(path)), True)

    @classmethod
    def open(
        cls,
        path: str | Path,
        save_events: int,
        save_seconds: float,
        segment_bytes: int,
        fsync_mode: FsyncMode,
        on_error: Optional[OnError] = None,
    ) -> "TimeDB":
        return cls(
            Log.open(
                Path(path), save_events, save_seconds, segment_bytes,
                fsync_mode, on_error,
            ),
            False
        )

    def health_check(self):
        self._log.health_check()

    def flush(self):
        self._log.flush()

    def __enter__(self) -> "TimeDB":
        return self

    def __exit__(self, *exc):
        self._log.close()

    def put(self, key: str, value: Any) -> int:
        return self.put_at(key, value, datetime.now())

    def put_at(self, key: str, value: Any, at: datetime) -> int:
        if self._readonly:
            raise PermissionError("database is open read-only")
        validate_value(value)
        seq = self._log.append(key, value, at)
        return seq

    def delete(self, key: str) -> int:
        return self.put(key, ABSENT)

    def delete_at(self, key: str, at: datetime) -> int:
        return self.put_at(key, ABSENT, at)

    def at(self, seq: int) -> Snapshot:
        n = self._log.count()
        if not (0 <= seq <= n):
            raise ValueError(
                f"invalid seq {seq}: log has {n} events"
            )
        return Snapshot(self._log, seq)

    def latest(self) -> Snapshot:
        return Snapshot(self._log, self._log.count())

    def events(self) -> Iterator[Event]:
        return self._log.events()

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

    def apply(self, writes: dict[str, Any]) -> tuple[int, int]:
        return self.apply_at(writes, datetime.now())

    def apply_at(
        self, writes: dict[str, Any], at: datetime
    ) -> tuple[int, int]:
        if self._readonly:
            raise PermissionError("database is open read-only")
        if not writes:
            raise ValueError("apply requires a non-empty writes dict")
        for value in writes.values():
            validate_value(value)
        result = self._log.append_tx(writes, at)
        return result

    def tx_events(self, tx_id: int) -> Iterator[Event]:
        if not self._log.tx_exists(tx_id):
            raise ValueError(f"Transaction {tx_id} does not exist")
        return self._log.tx_events(tx_id)

    def __repr__(self) -> str:
        n = self._log.count()
        return f"TimeDB({n} {'events' if n != 1 else 'event'})"
