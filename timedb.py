import fcntl
import json
import os
from bisect import bisect_right
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from itertools import accumulate
from pathlib import Path
from typing import Any

import immutables


type Value = str | int | float | bool

_EMPTY: immutables.Map[str, Value] = immutables.Map()


@dataclass(frozen=True, slots=True)
class Event:
    changes: Mapping[str, Value | None]
    ts: datetime


def _fold_event(
    state: immutables.Map[str, Value], event: Event
) -> immutables.Map[str, Value]:
    new = state.mutate()
    for k, change in event.changes.items():
        if change is None:
            new.pop(k, None)
        else:
            new[k] = change
    return new.finish()


def _serialize_event(event: Event) -> Mapping[str, Any]:
    return {
        "changes": event.changes,
        "ts": event.ts.isoformat()
    }


def _deserialize_event(raw: Mapping[str, Any]) -> Event:
    return Event(
        changes=raw["changes"],
        ts=datetime.fromisoformat(raw["ts"]),
    )


def _encode(event: Event) -> bytes:
    return json.dumps(_serialize_event(event)).encode() + b"\n"


def _read(path: Path, recover: bool) -> Sequence[Event]:
    events: list[Event] = []
    consumed = 0
    mode = "r+b" if recover else "rb"
    with path.open(mode) as f:
        for line in f:
            if not line.endswith(b"\n"):
                break
            try:
                event = _deserialize_event(json.loads(line))
            except (json.JSONDecodeError, KeyError, ValueError):
                break
            events.append(event)
            consumed += len(line)
        if recover:
            f.truncate(consumed)
    return events


def _open_locked(path: Path) -> int:
    fd = os.open(str(path), os.O_RDONLY | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as e:
        os.close(fd)
        raise RuntimeError(f"DB at {path} is locked by another process") from e
    return fd


def _resolve_ts(at: datetime | None, last: datetime | None) -> datetime:
    if at is None:
        now = datetime.now()
        return max(now, last) if last else now
    if last and at < last:
        raise ValueError(
            f"timestamp {at.isoformat()} is before last "
            f"event timestamp {last.isoformat()}"
        )
    return at


def diff(
    a: Mapping, b: Mapping
) -> Iterator[tuple[str, Value | None, Value | None]]:
    for k in set(a) | set(b):
        a_val = a.get(k)
        b_val = b.get(k)
        if a_val != b_val:
            yield (k, a_val, b_val)


def history(
    events: Iterable[Event], key: str
) -> Iterator[tuple[int, Value | None, datetime]]:
    for seq, e in enumerate(events):
        if key in e.changes:
            yield (seq, e.changes[key], e.ts)


class DB:
    def __init__(self, events: Sequence[Event]):
        self._events = list(events)
        self._states = list(accumulate(events, _fold_event, initial=_EMPTY))

    def at(self, seq: int) -> Mapping:
        n = len(self._events)
        if not (0 <= seq < n):
            raise ValueError(f"invalid seq {seq}: log has {n} events")
        return self._states[seq + 1]

    def latest(self) -> Mapping:
        return self._states[-1]

    def seq_as_of(self, t: datetime) -> int | None:
        ix = bisect_right(self._events, t, key=lambda e: e.ts) - 1
        return ix if ix >= 0 else None

    def as_of(self, t: datetime) -> Mapping:
        seq = self.seq_as_of(t)
        return self.at(seq) if seq is not None else _EMPTY

    def events(self) -> Sequence[Event]:
        return self._events


class WritableDB(DB):
    def __init__(
        self, events: Sequence[Event], on_apply: Callable[[Event], None]
    ):
        super().__init__(events)
        self._on_apply = on_apply

    # assigning None deletes the key
    def apply(
        self, 
        changes: Mapping[str, Value | None], 
        *, 
        at: datetime | None = None
    ) -> int:
        last = self._events[-1].ts if self._events else None
        ts = _resolve_ts(at, last)
        event = Event(dict(changes), ts)
        self._on_apply(event)
        self._events.append(event)
        self._states.append(_fold_event(self._states[-1], event))
        return len(self._events)


def inspect(path: str | Path) -> DB:
    return DB(_read(Path(path), recover=False))


def in_memory() -> WritableDB:
    return WritableDB([], lambda _: None)


@contextmanager
def connect(path: str | Path) -> Iterator[WritableDB]:
    path = Path(path)
    def persist(event: Event) -> None:
        with path.open("ab") as f:
            f.write(_encode(event))
            f.flush()
            os.fsync(f.fileno())
    lock_fd = _open_locked(path)
    try:
        yield WritableDB(_read(path, recover=True), persist)
    finally:
        os.close(lock_fd)
