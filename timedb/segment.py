import json
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


def write_events(path: str | Path, events: Iterable[Event]):
    _write(path, events, mode="w")


def append_events(path: str | Path, events: Iterable[Event]):
    _write(path, events, mode="a")


def _write(path: str | Path, events: Iterable[Event], mode: str):
    with open(path, mode) as f:
        for event in events:
            f.write(json.dumps(_serialize_event(event)) + "\n")
