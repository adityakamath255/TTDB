from dataclasses import dataclass
from datetime import datetime
import json
import os
from pathlib import Path
from collections.abc import Iterable, Iterator, Mapping
from types import MappingProxyType
import fcntl
from bisect import bisect_right
from itertools import accumulate

import immutables


Value = str | int | float | bool
State = immutables.Map[str, Value]


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


def _serialize_event(event: Event) -> dict:
    out = {"seq": event.seq, "ts": event.ts.isoformat()}
    if event.sets:
        out["sets"] = event.sets
    if event.deletes:
        out["deletes"] = event.deletes
    return out


def _deserialize_event(raw: dict) -> Event:
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
        self._states = list(
            accumulate(self._events, _fold_event, initial=immutables.Map())
        )

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

    def events(self) -> tuple[Event, ...]:
        return tuple(self._events)

    def num_events(self) -> int:
        return len(self._events)


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

        event = Event(
            seq=self.num_events(), 
            sets=MappingProxyType(dict(sets)), 
            deletes=deletes, ts=ts,
        )
        state = _fold_event(self._states[-1], event)
        self._events.append(event)
        self._states.append(state)
        self._on_event(event)
        return event


class DurableDB(WritableDB):
    def __init__(self, log_path: Path):
        self._log_path = log_path
        self._pending = bytearray()
        self._lock_fd = _open_locked(self._log_path)
        try:
            super().__init__(_read(log_path, recover=True))
        except BaseException:
            os.close(self._lock_fd)
            raise
 
    def _on_event(self, event: Event):
        encoded = json.dumps(_serialize_event(event)).encode() + b"\n"
        self._pending.extend(encoded)

    def save(self):
        if self._pending:
            with self._log_path.open("ab") as f:
                f.write(self._pending)
                f.flush()
                os.fsync(f.fileno())
            self._pending.clear()

    def __enter__(self) -> "DurableDB":
        return self

    def __exit__(self, *exc):
        try:
            self.save()
        finally:
            os.close(self._lock_fd)


def open(log_path: str | Path) -> DurableDB:
    return DurableDB(Path(log_path))


def view(path: str | Path) -> DB:
    path = Path(path)
    return DB(_read(path, recover=False))


def in_memory() -> WritableDB:
    return WritableDB()
