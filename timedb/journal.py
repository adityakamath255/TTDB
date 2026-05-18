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
        "key": event.key,
        "value": _serialize_value(event.value),
        "timestamp": event.timestamp.isoformat(),
        "tx_id": event.tx_id,
    }


def _deserialize_event(raw: dict) -> Event:
    return Event(
        seq=raw["seq"],
        key=raw["key"],
        value=_deserialize_value(raw["value"]),
        timestamp=datetime.fromisoformat(raw["timestamp"]),
        tx_id=raw["tx_id"],
    )


def append_events(path: Path, events: Iterable[Event]):
    with open(path, "a") as f:
        for event in events:
            f.write(json.dumps(_serialize_event(event)))
            f.write("\n")
        f.flush()
        os.fsync(f.fileno())
        

def _read_intact(path: Path) -> tuple[list[Event], int]:
    events: list[Event] = []
    consumed = 0

    if path.exists():
        with path.open("rb") as f:
            for line in f:
                if not line.endswith(b"\n"):
                    break
                try:
                    events.append(
                        _deserialize_event(json.loads(line))
                    )
                except (json.JSONDecodeError, KeyError):
                    break
                consumed += len(line)

    return events, consumed


def read_and_recover(path: Path) -> list[Event]:
    events, consumed = _read_intact(path)
    if path.exists():
        with path.open("r+b") as f:
            f.truncate(consumed)
    return events


def read(path: Path) -> list[Event]:
    return _read_intact(path)[0]
