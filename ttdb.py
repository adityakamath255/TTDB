import fcntl
import json
import os
from bisect import bisect_right
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from math import isfinite
from pathlib import Path

import immutables


type Value = str | int | float | bool
type Change = Value | None

_EMPTY: immutables.Map[str, Value] = immutables.Map()


class CorruptJournal(ValueError):
    pass


def _parse_changes(raw: object) -> immutables.Map[str, Change]:
    if not isinstance(raw, Mapping):
        raise ValueError("changes must be a mapping")

    changes: dict[str, Change] = {}
    for key, value in raw.items():
        if type(key) is not str:
            raise ValueError("change keys must be strings")
        if value is not None and type(value) not in (str, int, float, bool):
            kind = type(value).__name__
            raise ValueError(f"unsupported value for {key!r}: {kind}")
        if type(value) is float and not isfinite(value):
            raise ValueError(f"non-finite float for {key!r}")
        changes[key] = value
    return immutables.Map(changes)


def _normalize_ts(ts: object) -> datetime:
    if not isinstance(ts, datetime):
        raise ValueError("timestamp must be a datetime")
    if ts.tzinfo is None or ts.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return ts.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class Event:
    changes: Mapping[str, Change]
    ts: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "changes", _parse_changes(self.changes))
        object.__setattr__(self, "ts", _normalize_ts(self.ts))


@dataclass(frozen=True, slots=True)
class _Revision:
    event: Event
    state: immutables.Map[str, Value]


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


def _serialize_event(event: Event) -> dict[str, object]:
    return {
        "changes": dict(event.changes),
        "ts": event.ts.isoformat(),
    }


def _deserialize_event(raw: object) -> Event:
    if not isinstance(raw, Mapping):
        raise ValueError("event must be an object")
    if set(raw) != {"changes", "ts"}:
        raise ValueError("event must contain exactly 'changes' and 'ts'")
    ts = raw["ts"]
    if not isinstance(ts, str):
        raise ValueError("event timestamp must be a string")
    return Event(
        changes=raw["changes"],
        ts=datetime.fromisoformat(ts),
    )


def _encode(event: Event) -> bytes:
    return (
        json.dumps(
            _serialize_event(event), allow_nan=False, separators=(",", ":")
        )
        .encode()
        + b"\n"
    )


def _read(path: Path, recover: bool) -> Sequence[Event]:
    events: list[Event] = []
    consumed = 0
    mode = "r+b" if recover else "rb"
    with path.open(mode) as f:
        for line_number, line in enumerate(f, start=1):
            if not line.endswith(b"\n"):
                break
            try:
                event = _deserialize_event(json.loads(line))
            except (
                json.JSONDecodeError,
                UnicodeDecodeError,
                TypeError,
                ValueError,
            ) as e:
                raise CorruptJournal(
                    f"{path}: invalid event at line {line_number}: {e}"
                ) from e
            if events and event.ts < events[-1].ts:
                raise CorruptJournal(
                    f"{path}: timestamp at line {line_number} "
                    "precedes the prior event"
                )
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
        now = datetime.now(UTC)
        return max(now, last) if last else now
    at = _normalize_ts(at)
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
        self._revisions: list[_Revision] = []
        state = _EMPTY
        for event in events:
            if self._revisions and event.ts < self._revisions[-1].event.ts:
                raise ValueError("event timestamps must be non-decreasing")
            state = _fold_event(state, event)
            self._revisions.append(_Revision(event, state))

    def at(self, seq: int) -> immutables.Map[str, Value]:
        n = len(self._revisions)
        if not (0 <= seq < n):
            raise ValueError(f"invalid seq {seq}: log has {n} events")
        return self._revisions[seq].state

    def latest(self) -> immutables.Map[str, Value]:
        return self._revisions[-1].state if self._revisions else _EMPTY

    def seq_as_of(self, t: datetime) -> int | None:
        t = _normalize_ts(t)
        ix = bisect_right(
            self._revisions, t, key=lambda revision: revision.event.ts
        ) - 1
        return ix if ix >= 0 else None

    def as_of(self, t: datetime) -> immutables.Map[str, Value]:
        seq = self.seq_as_of(t)
        return self.at(seq) if seq is not None else _EMPTY

    def events(self) -> tuple[Event, ...]:
        return tuple(revision.event for revision in self._revisions)


class WritableDB(DB):
    def __init__(
        self, events: Sequence[Event], on_apply: Callable[[Event], None]
    ):
        super().__init__(events)
        self._on_apply = on_apply

    # assigning None deletes the key
    def apply(
        self,
        changes: Mapping[str, Change],
        *,
        at: datetime | None = None,
    ) -> int:
        last = self._revisions[-1].event.ts if self._revisions else None
        ts = _resolve_ts(at, last)
        event = Event(changes, ts)
        revision = _Revision(event, _fold_event(self.latest(), event))
        self._on_apply(event)
        self._revisions.append(revision)
        return len(self._revisions)


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
