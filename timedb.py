from dataclasses import dataclass
from datetime import datetime
import json
import os
from pathlib import Path
from typing import Iterable, Iterator, Optional, Sequence
import fcntl
from bisect import bisect_right
from itertools import accumulate

import immutables


class Absent:
    pass


ABSENT = Absent()

Value = str | int | float | bool | None | list["Value"] | dict[str, "Value"]
OptValue = Value | Absent


@dataclass(frozen=True, slots=True)
class Event:
    seq: int
    changes: dict[str, OptValue]
    timestamp: datetime


def _fold_event(root: immutables.Map, event: Event) -> immutables.Map:
    new = root
    for k, v in event.changes.items():
        if v is ABSENT:
            if k in new:
                new = new.delete(k)
        else:
            new = new.set(k, v)
    return new


def _serialize_value(value: OptValue) -> dict:
    return (
        {"type": "data", "data": value}
        if value is not ABSENT
        else {"type": "absent"}
    )


def _deserialize_value(raw: dict) -> OptValue:
    return (
        raw["data"]
        if raw["type"] != "absent"
        else ABSENT
    )


def _serialize_event(event: Event) -> dict:
    return {
        "seq": event.seq,
        "changes": {k: _serialize_value(v) for k, v in event.changes.items()},
        "timestamp": event.timestamp.isoformat(),
    }


def _deserialize_event(raw: dict) -> Event:
    return Event(
        seq=raw["seq"],
        changes={k: _deserialize_value(v) for k, v in raw["changes"].items()},
        timestamp=datetime.fromisoformat(raw["timestamp"]),
    )


def _read_intact(path: Path) -> tuple[list[Event], int]:
    events: list[Event] = []
    committed_bytes = 0

    if path.exists():
        with path.open("rb") as f:
            for line in f:
                if not line.endswith(b"\n"):
                    break
                try:
                    event = _deserialize_event(json.loads(line))
                except (json.JSONDecodeError, KeyError):
                    break
                events.append(event)
                committed_bytes += len(line)
    return events, committed_bytes


def _read_and_recover(path: Path) -> list[Event]:
    events, consumed = _read_intact(path)
    if path.exists():
        with path.open("r+b") as f:
            f.truncate(consumed)
    return events


def _read(path: Path) -> list[Event]:
    return _read_intact(path)[0]


def _append_events(path: Path, events: Iterable[Event]):
    with path.open("a") as f:
        for event in events:
            f.write(json.dumps(_serialize_event(event)))
            f.write("\n")
        f.flush()
        os.fsync(f.fileno())


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


def diff(
    a: immutables.Map, b: immutables.Map
) -> Iterator[tuple[str, OptValue, OptValue]]:
    for k in set(a) | set(b):
        a_val = a.get(k, ABSENT)
        b_val = b.get(k, ABSENT)
        if a_val != b_val:
            yield (k, a_val, b_val)


def history(
    events: Iterable[Event], key: str
) -> Iterator[tuple[int, OptValue, datetime]]:
    return (
        (e.seq, e.changes[key], e.timestamp)
        for e in events
        if key in e.changes
    )


class DB:
    def __init__(self, events: Iterable[Event] = ()):
        self._events = list(events)
        self._roots = list(
            accumulate(self._events, _fold_event, initial=immutables.Map())
        )

    def at(self, seq: int) -> immutables.Map:
        n = len(self._events)
        if not (0 <= seq < n):
            raise ValueError(
                f"invalid seq {seq}: log has {n} events"
            )
        return self._roots[seq]

    def latest(self) -> immutables.Map:
        return self._roots[-1]

    def as_of(self, t: datetime) -> immutables.Map:
        return self._roots[
            bisect_right(self._events, t, key=lambda e: e.timestamp)
        ]

    def events(self) -> Sequence[Event]:
        return self._events


class WritableDB(DB):
    def _on_event(self, event: Event):
        pass

    def apply(
        self, changes: dict[str, OptValue], at: Optional[datetime] = None
    ) -> int:
        if at is None:
            at = datetime.now()
        if self._events and at < self._events[-1].timestamp:
            raise ValueError(
                f"timestamp {at.isoformat()} is before last "
                f"event timestamp {self._events[-1].timestamp.isoformat()}"
            )
        seq = len(self._events)
        event = Event(seq, changes, at)
        self._events.append(event)
        self._roots.append(_fold_event(self._roots[-1], event))
        self._on_event(event)
        return seq


class DurableDB(WritableDB):
    def __init__(self, log_path: Path):
        self._log_path = log_path
        self._pending: list[Event] = []
        self._lock_fd = _acquire_lock(self._log_path)
        try:
            super().__init__(_read_and_recover(self._log_path))
        except BaseException:
            _release_lock(self._lock_fd)
            raise

    def _on_event(self, event: Event):
        self._pending.append(event)

    def save(self):
        if self._pending:
            _append_events(self._log_path, self._pending)
            self._pending.clear()

    def __enter__(self) -> "DurableDB":
        return self

    def __exit__(self, *exc):
        try:
            self.save()
        finally:
            _release_lock(self._lock_fd)


def open(log_path: str | Path) -> DurableDB:
    return DurableDB(Path(log_path))


def view(path: str | Path) -> DB:
    return DB(_read(Path(path)))


def empty() -> WritableDB:
    return WritableDB()
