from datetime import datetime
from pathlib import Path
from typing import Any, Iterator, Optional, Sequence

from .types import Event, ABSENT, validate_value
from .log import LogProtocol, Log
from .durable_log import DurableLog
from .snapshot import Snapshot


class TimeDB:
    def __init__(self, log: LogProtocol):
        self._log = log

    @classmethod
    def in_memory(cls) -> "TimeDB":
        return cls(Log([]))

    @classmethod
    def open(
        cls, 
        path: str | Path, 
        save_bytes: int, 
        save_seconds: float, 
        segment_bytes: int,
    ) -> "TimeDB":
        return cls(DurableLog.open(
            Path(path), save_bytes, save_seconds, segment_bytes
        ))

    def flush(self):
        self._log.flush()
        
    def __enter__(self) -> "TimeDB":
        return self

    def __exit__(self, *exc) -> bool:
        return self._log.__exit__(*exc)

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

    def between(self, seq0: int, seq1: int) -> Iterator[Event]:
        n = self._log.latest()

        if not (seq0 <= seq1 <= n):
            raise ValueError(
                f"invalid range: seq0={seq0}, seq1={seq1}, "
                f"log has {n + 1} events"
            )

        return self._log.events_between(seq0, seq1)

    def apply(
        self, writes: dict[str, Any]
    ) -> Optional[tuple[int, int]]:
        return self.apply_at(writes, datetime.now())

    def apply_at(
        self, writes: dict[str, Any], at: datetime
    ) -> Optional[tuple[int, int]]:
        if not writes:
            return None
        else:
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
