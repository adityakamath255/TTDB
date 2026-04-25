import json
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator, Optional, Sequence

from .types import Event, ABSENT, validate_value
from .log import Log
from .snapshot import Snapshot


class TimeDB:
    def __init__(self):
        self._log = Log([])

    @classmethod
    def load(cls, path: str | Path) -> "TimeDB":
        db = cls.__new__(cls)
        db._log = Log(_read_events(path))
        return db

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
        if seq >= self._log.latest():
            raise ValueError(f"no event at seq {seq}")
        return Snapshot(self._log, seq)

    def latest(self) -> Snapshot:
        return Snapshot(self._log, self._log.latest())

    def events(self) -> Sequence[Event]:
        return self._log.events()

    def between(self, seq0: int, seq1: int) -> Iterator[Event]:
        n = len(self._log.events())

        if not (seq0 <= seq1 < n):
            raise ValueError(
                f"invalid range: seq0={seq0}, seq1={seq1}, log has {n} events"
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

    def transaction_events(self, tx_id: int) -> Iterator[Event]:
        if self._log.tx_exists(tx_id):
            return self._log.tx_events(tx_id)
        else:
            raise ValueError(
                f"Transaction {tx_id} does not exist"
            )

    def save(self, path: str | Path):
        with open(path, "w") as f:
            for event in self._log.events():
                record = {
                    "seq": event.seq,
                    "key": event.key,
                    "value": _serialize_value(event.value),
                    "timestamp": event.timestamp.isoformat(),
                    "tx_id": event.tx_id
                }
                f.write(json.dumps(record) + "\n")

    def __repr__(self) -> str:
        n = len(self._log.events())
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


def _read_events(path: str | Path) -> list[Event]:
    with open(path) as f:
        return [
            Event(
                seq=r["seq"],
                key=r["key"],
                value=_deserialize_value(r["value"]),
                timestamp=datetime.fromisoformat(r["timestamp"]),
                tx_id=r["tx_id"],
            )
            for r in map(json.loads, f)
        ]
