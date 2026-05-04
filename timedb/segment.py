import json
import os
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

import immutables

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
    }


def _deserialize_event(raw: dict) -> Event:
    return Event(
        seq=raw["seq"],
        key=raw["key"],
        value=_deserialize_value(raw["value"]),
        timestamp=datetime.fromisoformat(raw["timestamp"]),
        tx_id=raw["tx_id"],
    )


@lru_cache(maxsize=4)
def read_events(path: str | Path) -> list[Event]:
    with open(path) as f:
        return [_deserialize_event(json.loads(line)) for line in f]


def _fsync_dir(path: Path):
    fd = os.open(str(path), os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_events(
    path: str | Path, events: Iterable[Event], fsync: bool
):
    path = Path(path)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w") as f:
        for event in events:
            f.write(json.dumps(_serialize_event(event)) + "\n")
        if fsync:
            f.flush()
            os.fsync(f.fileno())
    os.replace(tmp, path)
    if fsync:
        _fsync_dir(path.parent)


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


def write_manifest(path: str | Path, manifest: dict, fsync: bool):
    path = Path(path)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(manifest, f)
        f.write("\n")
        if fsync:
            f.flush()
            os.fsync(f.fileno())
    os.replace(tmp, path)
    if fsync:
        _fsync_dir(path.parent)


def segment_filename(num: int) -> str:
    return f"{num:06d}.log"


def state_filename(num: int) -> str:
    return f"{num:06d}.state"


def segment_num(filename: str) -> int:
    return int(filename.split(".")[0])


@lru_cache(maxsize=4)
def read_state(path: str | Path) -> immutables.Map:
    items: dict = {}
    with open(path) as f:
        for line in f:
            entry = json.loads(line)
            items[entry["k"]] = entry["v"]
    return immutables.Map(items)


def atomic_write_state(path: str | Path, root: immutables.Map, fsync: bool):
    path = Path(path)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w") as f:
        for k, v in root.items():
            f.write(json.dumps({"k": k, "v": v}) + "\n")
        if fsync:
            f.flush()
            os.fsync(f.fileno())
    os.replace(tmp, path)
    if fsync:
        _fsync_dir(path.parent)
