import json
import os
from datetime import datetime
from pathlib import Path
from typing import Iterable

from .types import Event, OptValue, ABSENT


def _serialize_value(value: OptValue) -> dict:
    return (
        {"type": "absent"}
        if value is ABSENT
        else {"type": "data", "data": value}
    )


def _deserialize_value(raw: dict) -> OptValue:
    return (
        ABSENT
        if raw["type"] == "absent"
        else raw["data"]
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


def append_events(path: Path, events: Iterable[Event]):
    with open(path, "a") as f:
        for event in events:
            f.write(json.dumps(_serialize_event(event)))
            f.write("\n")
        f.flush()
        os.fsync(f.fileno())


def read_and_recover(path: Path) -> list[Event]:
    events, consumed = _read_intact(path)
    if path.exists():
        with path.open("r+b") as f:
            f.truncate(consumed)
    return events


def read(path: Path) -> list[Event]:
    return _read_intact(path)[0]
