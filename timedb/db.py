from datetime import datetime
from pathlib import Path
from typing import Any, Iterator, Optional, Sequence

from .types import Event, ABSENT, validate_value
from .log import Log
from .saver import Saver
from .snapshot import Snapshot
from .interval import Interval


class TimeDB:
    def __init__(self, log: Log, saver: Optional[Saver]):
        self._log = log
        self._saver = saver

    @classmethod
    def in_memory(cls) -> "TimeDB":
        return cls(Log.in_memory(), None)

    @classmethod
    def open(
        cls,
        path: str | Path,
        save_events: int,
        save_seconds: float,
        segment_bytes: int,
    ) -> "TimeDB":
        log = Log.in_memory()
        saver = Saver.open(
            log, Path(path), save_events, save_seconds, segment_bytes,
        )
        saver.start()
        return cls(log, saver)

    def flush(self):
        if self._saver is not None:
            self._saver.flush()

    def __enter__(self) -> "TimeDB":
        return self

    def __exit__(self, *exc):
        if self._saver is not None:
            self._saver.close()

    def put(self, key: str, value: Any) -> int:
        return self.put_at(key, value, datetime.now())

    def put_at(self, key: str, value: Any, at: datetime) -> int:
        validate_value(value)
        return self._log.write(key, value, at)

    def delete(self, key: str) -> int:
        return self.put(key, ABSENT)

    def delete_at(self, key: str, at: datetime) -> int:
        return self.put_at(key, ABSENT, at)

    def at(self, seq: int) -> Snapshot:
        if seq > self._log.latest():
            raise ValueError(f"no event at seq {seq}")
        return Snapshot(self._log, seq)

    def latest(self) -> Snapshot:
        return Snapshot(self._log, self._log.latest())

    def events(self) -> Sequence[Event]:
        return self._log.events()

    def between(self, seq0: int, seq1: int) -> Interval:
        n = self._log.latest()
        if not (seq0 <= seq1 <= n):
            raise ValueError(
                f"invalid range: seq0={seq0}, seq1={seq1}, "
                f"log has {n + 1} events"
            )
        return Interval(self._log, seq0, seq1)

    def history(self, key: str) -> Iterator[Event]:
        return self.between(-1, self._log.latest()).history(key)

    def apply(self, writes: dict[str, Any]) -> tuple[int, int]:
        return self.apply_at(writes, datetime.now())

    def apply_at(
        self, writes: dict[str, Any], at: datetime
    ) -> tuple[int, int]:
        if not writes:
            raise ValueError("apply requires a non-empty writes dict")
        for value in writes.values():
            validate_value(value)
        return self._log.write_tx(writes, at)

    def tx_events(self, tx_id: int) -> Iterator[Event]:
        if not self._log.tx_exists(tx_id):
            raise ValueError(f"Transaction {tx_id} does not exist")
        return self._log.tx_events(tx_id)

    def __repr__(self) -> str:
        n = len(self._log.events())
        return f"TimeDB({n} {'events' if n != 1 else 'event'})"
