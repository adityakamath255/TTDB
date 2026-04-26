import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from .types import Event, ABSENT


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


def _serialize_event(event: Event) -> dict:
    return {
        "seq": event.seq,
        "key": event.key,
        "value": _serialize_value(event.value),
        "timestamp": event.timestamp.isoformat(),
        "tx_id": event.tx_id,
        "prev_seq": event.prev_seq,
    }


def _deserialize_event(raw: dict) -> Event:
    return Event(
        seq=raw["seq"],
        key=raw["key"],
        value=_deserialize_value(raw["value"]),
        timestamp=datetime.fromisoformat(raw["timestamp"]),
        tx_id=raw["tx_id"],
        prev_seq=raw["prev_seq"],
    )


def read_events(path: str | Path) -> list[Event]:
    with open(path) as f:
        return [_deserialize_event(json.loads(line)) for line in f]


def atomic_write_events(path: str | Path, events: Iterable[Event]):
    path = Path(path)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w") as f:
        for event in events:
            f.write(json.dumps(_serialize_event(event)) + "\n")
    os.replace(tmp, path)


def bin_events_by_size(
    events: Iterable[Event], max_bytes: int
) -> list[list[Event]]:
    chunks: list[list[Event]] = []
    current: list[Event] = []
    current_size = 0
    for event in events:
        line_size = len(json.dumps(_serialize_event(event))) + 1
        if current and current_size + line_size > max_bytes:
            chunks.append(current)
            current = []
            current_size = 0
        current.append(event)
        current_size += line_size
    if current:
        chunks.append(current)
    return chunks


def read_manifest(path: str | Path) -> dict:
    with open(path) as f:
        return json.load(f)


def write_manifest(path: str | Path, manifest: dict):
    path = Path(path)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(manifest, f)
        f.write("\n")
    os.replace(tmp, path)
