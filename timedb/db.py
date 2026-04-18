import json
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator
from bisect import bisect_right

from .types import Event, ABSENT, validate_value
from .log import Log
from .snapshot import Snapshot


class TimeDB:
    def __init__(self):
        self._log = Log([])

    def put(self, key: str, value: Any) -> int:
        validate_value(value)
        return self._log.append(key, value, datetime.now())

    def put_at(self, key: str, value: Any, at: datetime) -> int:
        validate_value(value)
        return self._log.append(key, value, at)

    def delete(self, key: str) -> int:
        return self._log.append(key, ABSENT, datetime.now())

    def delete_at(self, key: str, at: datetime) -> int:
        return self._log.append(key, ABSENT, at)

    def at(self, seq: int) -> Snapshot:
        if seq >= len(self._log.events):
            raise ValueError(f"no event at seq {seq}")
        return Snapshot(self._log, seq)

    def latest(self) -> Snapshot:
        return Snapshot(self._log, len(self._log.events) - 1)

    def events(self) -> Iterator[Event]:
        return iter(self._log.events)

    def history(self, key: str) -> Iterator[Event]:
        return self._log.history(key)

    def save(self, path: str | Path):
        with open(path, "w") as f:
            for event in self._log.events:
                record = {
                    "seq": event.seq,
                    "key": event.key,
                    "value": _serialize_value(event.value),
                    "timestamp": event.timestamp.isoformat(),
                }
                f.write(json.dumps(record) + "\n")

    @classmethod
    def _load_impl(cls, path: str | Path, check: bool) -> "TimeDB":
        events = []

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
                    if event.seq != len(events):
                        raise ValueError(
                            f"line {line_num}: "
                            f"expected seq {len(events)}, got {event.seq}"
                        )

                events.append(event)

        db = TimeDB()
        db._log = Log(events)
        return db

    @classmethod
    def load(cls, path: str | Path) -> "TimeDB":
        return cls._load_impl(path, True)

    @classmethod
    def load_unchecked(cls, path: str | Path) -> "TimeDB":
        return cls._load_impl(path, False)

    def __repr__(self) -> str:
        n = len(self._log.events)
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
