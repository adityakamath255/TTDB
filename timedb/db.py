import json
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator
from bisect import bisect_right

from .types import Event, ABSENT, validate_value
from .snapshot import Snapshot


class TimeDB:
    def __init__(self):
        self._log: list[Event] = []
        self._seq: int = 0

    # -- writes --

    def _validate_timestamp(self, ts: datetime):
        if self._log and ts < self._log[-1].timestamp:
            raise ValueError(
                f"timestamp {ts} precedes last event at "
                f"{self._log[-1].timestamp}"
            )

    def _append(self, key: str, value: Any, at: datetime) -> int:
        self._validate_timestamp(at)
        event = Event(self._seq, key, value, at)
        self._log.append(event)
        self._seq += 1
        return event.seq

    def put(self, key: str, value: Any) -> int:
        validate_value(value)
        return self._append(key, value, datetime.now())

    def put_at(self, key: str, value: Any, at: datetime) -> int:
        validate_value(value)
        return self._append(key, value, at)

    def delete(self, key: str) -> int:
        return self._append(key, ABSENT, datetime.now())

    def delete_at(self, key: str, at: datetime) -> int:
        return self._append(key, ABSENT, at)

    # -- snapshots --
    
    def at(self, seq: int) -> Snapshot:
        if seq >= self._seq:
            raise ValueError(f"no event at seq {seq}")
        return Snapshot(self._log, seq)

    def latest(self) -> Snapshot:
        return Snapshot(self._log, self._seq - 1)

    # -- time mapping --
    
    def seq_at(self, at: datetime) -> int:
        if not self._log:
            raise ValueError("database is empty")
        if self._log[0].timestamp > at:
            return -1
        else:
            return bisect_right(self._log, at, key=lambda e: e.timestamp) - 1

    def timestamp_at(self, seq: int) -> datetime:
        if seq < 0 or seq >= self._seq:
            raise ValueError(f"no event at seq {seq}")
        return self._log[seq].timestamp

    # -- log access --
    
    def events(self) -> Iterator[Event]:
        return iter(self._log)

    def history(self, key: str) -> Iterator[Event]:
        return (e for e in self._log if e.key == key)

    # -- persistence --
    
    def save(self, path: str | Path):
        with open(path, "w") as f:
            for event in self._log:
                record = {
                    "seq": event.seq,
                    "key": event.key,
                    "value": _serialize_value(event.value),
                    "timestamp": event.timestamp.isoformat(),
                }
                f.write(json.dumps(record) + "\n")

    @classmethod
    def _load_impl(cls, path: str | Path, check: bool) -> "TimeDB":
        db = cls()

        with open(path) as f:
            for line_num, line in enumerate(f, 1):

                record = json.loads(line)

                event = Event(
                    seq=record["seq"],
                    key=record["key"],
                    value=_deserialize_value(record["value"]),
                    timestamp=datetime.fromisoformat(
                        record["timestamp"]
                    ),
                )

                if check:
                    if event.seq != db._seq:
                        raise ValueError(
                            f"line {line_num}: "
                            f"expected seq {db._seq}, got {event.seq}"
                        )
                    if db._log and event.timestamp < db._log[-1].timestamp:
                        raise ValueError(
                            f"line {line_num}: "
                            f"timestamp {event.timestamp} precedes previous "
                            f"{db._log[-1].timestamp}"
                        )

                db._log.append(event)
                db._seq = event.seq + 1

        return db

    @classmethod
    def load(cls, path: str | Path) -> "TimeDB":
        return cls._load_impl(path, True)

    @classmethod
    def load_unchecked(cls, path: str | Path) -> "TimeDB":
        return cls._load_impl(path, False)

    def __repr__(self) -> str:
        n = len(self._log)
        return f"TimeDB({n} {'events' if n != 1 else 'event'})"


def _serialize_value(value: Any) -> dict:
    return (
        {"type": "absent"}
        if value is ABSENT
        else {"type": "data", "data": value}
    )


def _deserialize_value(raw: dict) -> Any:
    return (
        ABSENT
        if raw["type"] == "absent"
        else raw["data"]
    )
