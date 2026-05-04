import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable, Optional

import immutables

from .types import Event, ABSENT


@dataclass(frozen=True, slots=True)
class SegmentMeta:
    num: int
    last_seq: int

    def filename(self) -> str:
        return f"{self.num:06d}.log"

    def state_filename(self) -> str:
        return f"{self.num:06d}.state"


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


SerializedEvent = tuple[Event, str]


def serialize_event(event: Event) -> str:
    return json.dumps(_serialize_event(event))


@lru_cache(maxsize=4)
def read_events(path: Path) -> list[Event]:
    with open(path) as f:
        return [_deserialize_event(json.loads(line)) for line in f]


def _fsync_dir(path: Path):
    fd = os.open(str(path), os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_lines(
    path: Path, lines: Iterable[str], fsync: bool
):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w") as f:
        for line in lines:
            f.write(line)
            f.write("\n")
        if fsync:
            f.flush()
            os.fsync(f.fileno())
    os.replace(tmp, path)
    if fsync:
        _fsync_dir(path.parent)


def bin_events_by_size(
    events: Iterable[Event], max_bytes: int
) -> list[list[SerializedEvent]]:
    chunks: list[list[SerializedEvent]] = []
    current: list[SerializedEvent] = []
    current_size = 0
    for event in events:
        line = serialize_event(event)
        line_size = len(line) + 1
        if current and current_size + line_size > max_bytes:
            chunks.append(current)
            current = []
            current_size = 0
        current.append((event, line))
        current_size += line_size
    if current:
        chunks.append(current)
    return chunks


def read_manifest(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)


def write_manifest(path: Path, manifest: dict, fsync: bool):
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


_SEGMENT_PATTERN = re.compile(r"^(\d{6})\.(log|state)$")


def parse_segment_filename(name: str) -> Optional[int]:
    m = _SEGMENT_PATTERN.match(name)
    return int(m.group(1)) if m else None


@lru_cache(maxsize=4)
def read_state(path: Path) -> immutables.Map:
    items: dict = {}
    with open(path) as f:
        for line in f:
            entry = json.loads(line)
            items[entry["k"]] = entry["v"]
    return immutables.Map(items)


def atomic_write_state(path: Path, root: immutables.Map, fsync: bool):
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
