from typing import Any, Optional, Sequence, Iterator
from datetime import datetime
from dataclasses import dataclass, asdict
from pathlib import Path
from threading import Thread, Condition

import immutables

from .types import Event
from .log import LogProtocol, Log
from . import segment


_MANIFEST_NAME = "manifest"


@dataclass(frozen=True, slots=True)
class SegmentMeta:
    filename: str
    first_seq: int
    last_seq: int
    event_count: int
    sealed: bool


class DurableLog(LogProtocol):
    def __init__(
        self, 
        directory: Path,
        log: Log,
        segments: list[SegmentMeta],
        save_bytes: int,
        save_seconds: float,
        segment_bytes: int,
    ):
        self._directory = directory
        self._log = log
        self._segments = segments
        self._save_bytes = save_bytes
        self._save_seconds = save_seconds
        self._segment_bytes = segment_bytes
        if segments:
            self._persisted_through_seq = segments[-1].last_seq
        else:
            self._persisted_through_seq = -1
        self._wake = Condition()
        self._closing = False
        self._save_error: Optional[Exception] = None
        self._flush_requested = False
        self._save_thread = Thread(target=self._saver_loop, daemon=True)
        self._save_thread.start()

    @classmethod
    def open(
        cls,
        dir: Path,
        save_bytes: int,
        save_seconds: float,
        segment_bytes: int,
    ) -> "DurableLog":
        dir.mkdir(parents=True, exist_ok=True)
        manifest_path = dir / _MANIFEST_NAME

        if manifest_path.exists():
            raw = segment.read_manifest(manifest_path)
            segments = [SegmentMeta(**s) for s in raw["segments"]]
        else:
            segments = []

        events: list[Event] = []

        for seg in segments:
            seg_events = segment.read_events(dir / seg.filename)
            if len(seg_events) != seg.event_count:
                raise ValueError(
                    f"segment {seg.filename}: manifest says "
                    f"{seg.event_count} events, file has {len(seg_events)}"
                )
            events.extend(seg_events)

        log = Log(events)

        return cls(dir, log, segments, save_bytes, save_seconds, segment_bytes)

    def write(self, key: str, value: Any, at: datetime) -> int: 
        self._raise_if_unhealthy()
        seq = self._log.write(key, value, at)
        self._maybe_wake(seq)
        return seq

    def write_tx(
        self, writes: dict[str, Any], at: datetime
    ) -> tuple[int, int]:
        self._raise_if_unhealthy()
        tx_id, last_seq = self._log.write_tx(writes, at)
        self._maybe_wake(last_seq)
        return (tx_id, last_seq)

    def root_at(self, seq: int) -> immutables.Map:
        return self._log.root_at(seq)

    def get(self, seq: int) -> Optional[Event]:
        return self._log.get(seq)

    def events(self) -> Sequence[Event]:
        return self._log.events()

    def events_between(self, lo: int, hi: int) -> Iterator[Event]:
        return self._log.events_between(lo, hi)

    def latest(self) -> int:
        return self._log.latest()

    def tx_exists(self, tx_id: int) -> bool:
        return self._log.tx_exists(tx_id)

    def tx_events(self, tx_id: int) -> Iterator[Event]:
        return self._log.tx_events(tx_id)

    def history(self, key: str, lo: int, hi: int) -> Iterator[Event]:
        return self.history(key, lo, hi)

    def flush(self):
        self._raise_if_unhealthy()
        target = self._log.latest()

        with self._wake:
            self._flush_requested = True
            self._wake.notify()

            while self._persisted_through_seq < target:
                if self._save_error is not None:
                    raise self._save_error
                self._wake.wait()

    def _raise_if_unhealthy(self):
        if self._save_error is not None:
            raise self._save_error
        if self._closing:
            raise RuntimeError("DurableLog is closed")

    def _maybe_wake(self, seq: int):
        # save_bytes treated as event-count proxy
        if seq - self._persisted_through_seq >= self._save_bytes:
            with self._wake:
                self._wake.notify()

    def _saver_loop(self):
        while True:
            with self._wake:
                self._wake.wait_for(
                    lambda: self._should_save() or self._closing,
                    timeout=self._save_seconds,
                )
                if self._closing and not self._has_unsaved():
                    return

                if not self._has_unsaved():
                    continue

                target_seq = self._log.latest()
                self._flush_requested = False

            try:
                self._do_save(target_seq)

            except Exception as e:
                with self._wake:
                    self._save_error = e
                    self._wake.notify_all()

            with self._wake:
                self._persisted_through_seq = target_seq
                self._wake.notify_all()

            return

    def _should_save(self) -> bool:
        return self._flush_requested or self._has_unsaved()

    def _has_unsaved(self) -> bool:
        return self._log.latest() > self._persisted_through_seq

    def _do_save(self, target_seq: int):
        sealed = [s for s in self._segments if s.sealed]

        if sealed:
            first_unsealed_seq = sealed[-1].last_seq + 1
        else:
            first_unsealed_seq = 0

        if not self._segments:
            next_num = 1
        elif self._segments[-1].sealed:
            next_num = _segment_num(self._segments[-1].filename) + 1
        else:
            next_num = _segment_num(self._segments[-1].filename)

        to_write = self._log.events()[first_unsealed_seq: target_seq + 1]

        chunks = segment.bin_events_by_size(to_write, self._segment_bytes)

        new_segments: list[SegmentMeta] = []

        for i, chunk in enumerate(chunks):
            filename = f"{next_num + i:06d}.log"
            segment.atomic_write_events(self._directory / filename, chunk)

            new_segments.append(SegmentMeta(
                filename=filename,
                first_seq=chunk[0].seq,
                last_seq=chunk[-1].seq,
                event_count=len(chunk),
                sealed=i < len(chunks) - 1,
            ))

        all_segments = sealed + new_segments

        segment.write_manifest(
            self._directory / _MANIFEST_NAME,
            {"segments": [asdict(s) for s in all_segments]},
        )

        self._segments = all_segments

    def __enter__(self) -> "DurableLog":
        return self
    
    def __exit__(self, *exc) -> bool:
        with self._wake:
            self._closing = True
            self._wake.notify()

        self._save_thread.join()
        if self._save_error is not None:
            raise self._save_error

        return False


def _segment_num(filename: str) -> int:
    return int(filename.split(".")[0])
