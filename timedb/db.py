from datetime import datetime
from pathlib import Path
from typing import Any, Iterator, Optional, Sequence
from threading import Lock

from .types import Event, ABSENT, validate_value
from .log import Log
from .snapshot import Snapshot
from . import segment


DEFAULT_SEGMENT_BYTES = 64 * 1024 * 1024
_MANIFEST_NAME = "manifest"


class TimeDB:
    def __init__(
        self, path: str | Path, *, segment_bytes: int = DEFAULT_SEGMENT_BYTES
    ):
        directory = Path(path)
        if not directory.is_dir():
            raise ValueError(f"not a directory: {directory}")
        manifest = segment.read_manifest(directory / _MANIFEST_NAME)

        events: list[Event] = []
        for seg in manifest["segments"]:
            seg_events = segment.read_events(directory / seg["filename"])
            if len(seg_events) != seg["event_count"]:
                raise ValueError(
                    f"segment {seg['filename']}: manifest says "
                    f"{seg['event_count']} events, file has {len(seg_events)}"
                )
            events.extend(seg_events)

        self._log = Log(events)
        self._save_lock = Lock()
        self._segments = manifest["segments"]
        self._segment_bytes = segment_bytes

    def save(self, path: str | Path):
        with self._save_lock:
            directory = Path(path)
            directory.mkdir(parents=True, exist_ok=True)

            sealed = [s for s in self._segments if s["sealed"]]

            if sealed:
                first_unsealed_seq = sealed[-1]["last_seq"]
            else:
                first_unsealed_seq = 0

            if not self._segments:
                next_num = -1
            elif self._segments[-1]["sealed"]:
                next_num = _segment_num(self._segments[-1]["filename"]) + 1
            else:
                next_num = _segment_num(self._segments[-1]["filename"])

            to_write = self.events()[first_unsealed_seq:]
            chunks = segment.bin_events_by_size(to_write, self._segment_bytes)

            new_segments = []
            for i, chunk in enumerate(chunks):
                filename = f"{next_num + i:06d}.log"
                segment.atomic_write_events(directory / filename, chunk)
                new_segments.append({
                    "filename": filename,
                    "first_seq": chunk[0].seq,
                    "last_seq": chunk[-1].seq,
                    "event_count": len(chunk),
                    "sealed": i < len(chunks) - 1,
                })

            all_segments = sealed + new_segments
            segment.write_manifest(
                directory / _MANIFEST_NAME, {"segments": all_segments}
            )
            self._segments = all_segments

    def put(self, key: str, value: Any) -> int:
        return self.put_at(key, value, datetime.now())

    def put_at(self, key: str, value: Any, at: datetime) -> int:
        validate_value(value)
        return self._log.write(key, value, at)

    def delete(self, key: str) -> int:
        return self.put(key, ABSENT)

    def delete_at(self, key: str, at: datetime) -> int:
        return self.put_at(key, ABSENT, at)

    def at(self, seq: int) -> Snapshot:
        if seq > self._log.latest():
            raise ValueError(f"no event at seq {seq}")
        return Snapshot(self._log, seq)

    def latest(self) -> Snapshot:
        return Snapshot(self._log, self._log.latest())

    def events(self) -> Sequence[Event]:
        return self._log.events()

    def between(self, seq0: int, seq1: int) -> Iterator[Event]:
        n = self._log.latest()

        if not (seq0 <= seq1 <= n):
            raise ValueError(
                f"invalid range: seq0={seq0}, seq1={seq1}, "
                f"log has {n + 1} events"
            )

        return self._log.events_between(seq0, seq1)

    def apply(
        self, writes: dict[str, Any]
    ) -> Optional[tuple[int, int]]:
        return self.apply_at(writes, datetime.now())

    def apply_at(
        self, writes: dict[str, Any], at: datetime
    ) -> Optional[tuple[int, int]]:
        if not writes:
            return None
        else:
            for value in writes.values():
                validate_value(value)
            return self._log.write_tx(writes, at)

    def tx_events(self, tx_id: int) -> Iterator[Event]:
        if not self._log.tx_exists(tx_id):
            raise ValueError(f"Transaction {tx_id} does not exist")
        return self._log.tx_events(tx_id)

    def __repr__(self) -> str:
        n = len(self._log.events())
        return f"TimeDB({n} {'events' if n != 1 else 'event'})"


def _segment_num(filename: str) -> int:
    return int(filename.split(".")[0])
