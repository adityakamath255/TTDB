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


def _serialize_event(event: Event, final: bool) -> dict:
    return {
        "tx": event.tx,
        "key": event.key,
        "value": _serialize_value(event.value),
        "timestamp": event.timestamp.isoformat(),
        "final": final
    }


def _deserialize_event(raw: dict) -> Event:
    return Event(
        tx=raw["tx"],
        key=raw["key"],
        value=_deserialize_value(raw["value"]),
        timestamp=datetime.fromisoformat(raw["timestamp"]),
    )


def _read_intact(path: Path) -> tuple[list[Event], int]:
    events: list[Event] = []
    pending: list[Event] = []
    bytes_read = 0
    committed_bytes = 0

    if path.exists():
        with path.open("rb") as f:
            for line in f:
                if not line.endswith(b"\n"):
                    break
                try:
                    raw = json.loads(line)
                    event = _deserialize_event(raw)
                    final = raw["final"]
                except (json.JSONDecodeError, KeyError):
                    break

                pending.append(event)
                bytes_read += len(line)

                if final:
                    events.extend(pending)
                    pending.clear()
                    committed_bytes = bytes_read

    return events, committed_bytes


def append_batches(path: Path, batches: Iterable[list[Event]]):
    with open(path, "a") as f:
        for batch in batches:
            for i, event in enumerate(batch):
                final = i == len(batch) - 1
                f.write(json.dumps(_serialize_event(event, final)))
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
