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


Value = str | int | float | bool
State = immutables.Map[str, Value]

EMPTY: State = immutables.Map()


@dataclass(frozen=True, slots=True)
class Event:
    seq: int
    sets: Mapping[str, Value]
    deletes: tuple[str, ...]
    ts: datetime


def _fold_event(state: State, event: Event) -> State:
    new = state.mutate()
    for k, v in event.sets.items():
        new[k] = v
    for k in event.deletes:
        new.pop(k, None)
    return new.finish()


def _serialize_event(event: Event) -> dict[str, Any]:
    out: dict[str, Any] = {"seq": event.seq, "ts": event.ts.isoformat()}
    if event.sets:
        out["sets"] = event.sets
    if event.deletes:
        out["deletes"] = event.deletes
    return out


def _deserialize_event(raw: dict[str, Any]) -> Event:
    return Event(
        seq=raw["seq"],
        sets=raw.get("sets", {}),
        deletes=tuple(raw.get("deletes", ())),
        ts=datetime.fromisoformat(raw["ts"]),
    )


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
    for e in events:
        if key in e.sets:
            yield (e.seq, e.sets[key], e.ts)
        elif key in e.deletes:
            yield (e.seq, None, e.ts)


class DB:
    def __init__(self, events: Iterable[Event] = ()):
        self._events = list(events)
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
    def _on_event(self, event: Event):
        pass

    def apply(
        self,
        sets: Mapping[str, Value] = {},  # noqa
        *,
        deletes: Iterable[str] = (),
        at: datetime | None = None,
    ) -> Event:
        if isinstance(deletes, str):
            raise TypeError(
                f"delete must be a collections of strings, not a single str; "
                f"got {deletes!r}"
            )
        deletes = tuple(deletes)
        if overlap := sets.keys() & set(deletes):
            raise ValueError(f"keys cannot be both set and deleted: {overlap}")

        last = self._events[-1].ts if self._events else None
        if at is None:
            now = datetime.now()
            ts = max(now, last) if last else now
        elif last and at < last:
            raise ValueError(
                f"timestamp {at.isoformat()} is before last "
                f"event timestamp {last.isoformat()}"
            )
        else:
            ts = at

        event = Event(len(self._events), dict(sets), deletes, ts)
        state = _fold_event(self._states[-1], event)
        self._events.append(event)
        self._states.append(state)
        self._on_event(event)
        return event


class DurableDB(WritableDB):
    def __init__(self, path: Path, flush_threshold_bytes: int):
        self._path = path
        self._pending = bytearray()
        self._flush_threshold = flush_threshold_bytes
        self._lock_fd = _open_locked(self._path)
        try:
            super().__init__(_read(path, recover=True))
        except BaseException:
            self.close()
            raise
 
    def _on_event(self, event: Event):
        encoded = json.dumps(_serialize_event(event)).encode() + b"\n"
        self._pending.extend(encoded)
        if len(self._pending) >= self._flush_threshold:
            self.flush()

    def flush(self):
        with self._path.open("ab") as f:
            f.write(self._pending)
            f.flush()
            os.fsync(f.fileno())
        self._pending.clear()

    def close(self):
        os.close(self._lock_fd)

    def __enter__(self) -> "DurableDB":
        return self

    def __exit__(self, *exc):
        try:
            self.flush()
        finally:
            self.close()


def connect(path: str | Path, flush_threshold_bytes: int = 1) -> DurableDB:
    return DurableDB(Path(path), flush_threshold_bytes)


def inspect(path: str | Path) -> DB:
    return DB(_read(Path(path), recover=False))


def in_memory() -> WritableDB:
    return WritableDB()
