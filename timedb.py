from dataclasses import dataclass
from datetime import datetime
import json
import os
from pathlib import Path
from collections.abc import Iterable, Iterator, Sequence, Mapping
from typing import Any
import fcntl
from bisect import bisect_right
from itertools import accumulate

import immutables


type Value = str | int | float | bool
type Change = Value | None
type State = immutables.Map[str, Value]

EMPTY: State = immutables.Map()


@dataclass(frozen=True, slots=True)
class Event:
    changes: Mapping[str, Change]
    ts: datetime


def _fold_event(state: State, event: Event) -> State:
    new = state.mutate()
    for k, change in event.changes.items():
        if change is None:
            new.pop(k, None)
        else:
            new[k] = change
    return new.finish()


def _serialize_event(event: Event) -> dict[str, Any]:
    out: dict[str, Any] = {"ts": event.ts.isoformat()}
    if event.changes:
        out["changes"] = dict(event.changes)
    return out


def _deserialize_event(raw: dict[str, Any]) -> Event:
    return Event(
        changes=raw.get("changes", {}),
        ts=datetime.fromisoformat(raw["ts"]),
    )


def _encode(event: Event) -> bytes:
    return json.dumps(_serialize_event(event)).encode() + b"\n"


def _read(path: Path, recover: bool) -> list[Event]:
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
    a: State, b: State
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
    def __init__(self, events: list[Event]):
        self._events = events
        self._states = list(accumulate(events, _fold_event, initial=EMPTY))

    def at(self, seq: int) -> State:
        n = len(self._events)
        if not (0 <= seq < n):
            raise ValueError(f"invalid seq {seq}: log has {n} events")
        return self._states[seq + 1]

    def latest(self) -> State:
        return self._states[-1]

    def as_of(self, t: datetime) -> State:
        ix = bisect_right(self._events, t, key=lambda e: e.ts)
        return self._states[ix]

    def events(self) -> Sequence[Event]:
        return self._events


class WritableDB(DB):
    def _persist(self, event: Event) -> None:
        pass

    def apply(
        self,
        sets: Mapping[str, Change] = {},
        *,
        at: datetime | None = None,
    ) -> int:
        """Assign each key its value; assigning None deletes the key."""
        last = self._events[-1].ts if self._events else None
        ts = _resolve_ts(at, last)
        event = Event(dict(sets), ts)

        self._persist(event)

        seq = len(self._events)
        self._events.append(event)
        self._states.append(_fold_event(self._states[-1], event))
        return seq


class DurableDB(WritableDB):
    def __init__(self, path: Path):
        self._path = path
        self._lock_fd = _open_locked(self._path)
        try:
            super().__init__(_read(path, recover=True))
        except BaseException:
            self.close()
            raise

    def _persist(self, event: Event) -> None:
        with self._path.open("ab") as f:
            f.write(_encode(event))
            f.flush()
            os.fsync(f.fileno())

    def close(self) -> None:
        os.close(self._lock_fd)

    def __enter__(self) -> "DurableDB":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def connect(path: str | Path) -> DurableDB:
    return DurableDB(Path(path))


def inspect(path: str | Path) -> DB:
    return DB(_read(Path(path), recover=False))


def in_memory() -> WritableDB:
    return WritableDB([])
