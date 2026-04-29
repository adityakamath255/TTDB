from dataclasses import dataclass, asdict
from pathlib import Path
from threading import Thread, Condition
from typing import Optional

from .log import Log
from .types import Event
from . import segment


_MANIFEST_NAME = "manifest"


@dataclass(frozen=True, slots=True)
class SegmentMeta:
    filename: str
    last_seq: int


class Saver:
    def __init__(
        self,
        log: Log,
        directory: Path,
        sealed: list[SegmentMeta],
        active: Optional[SegmentMeta],
        save_events: int,
        save_seconds: float,
        segment_bytes: int,
    ):
        # immutable
        self._log = log
        self._directory = directory
        self._sealed = sealed
        self._active = active
        self._save_events = save_events
        self._save_seconds = save_seconds
        self._segment_bytes = segment_bytes

        # mutable
        self._persisted_through_seq = active.last_seq if active else -1
        self._wake = Condition()
        self._closing = False
        self._save_error: Optional[Exception] = None
        self._save_thread = Thread(target=self._saver_loop, daemon=True)

    @classmethod
    def open(
        cls,
        log: Log,
        directory: Path,
        save_events: int,
        save_seconds: float,
        segment_bytes: int,
    ) -> "Saver":
        directory.mkdir(parents=True, exist_ok=True)
        manifest_path = directory / _MANIFEST_NAME

        if manifest_path.exists():
            raw = segment.read_manifest(manifest_path)
            metas = [SegmentMeta(**s) for s in raw["segments"]]
        else:
            metas = []

        if metas:
            sealed, active = metas[:-1], metas[-1]
        else:
            sealed, active = [], None

        events: list[Event] = []
        for seg in metas:
            events.extend(segment.read_events(directory / seg.filename))

        log.replay(events)

        saver = cls(
            log, directory, sealed, active,
            save_events, save_seconds, segment_bytes,
        )
        log.attach(saver)
        return saver

    def start(self):
        self._save_thread.start()

    def __enter__(self):
        if self._save_error is not None:
            raise self._save_error
        if self._closing:
            raise RuntimeError("Saver is closed")

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            return
        seq = self._log.latest()
        if seq - self._persisted_through_seq >= self._save_events:
            with self._wake:
                self._wake.notify()

    def flush(self):
        if self._save_error is not None:
            raise self._save_error
        target = self._log.latest()
        with self._wake:
            self._wake.notify()
            while self._persisted_through_seq < target:
                if self._save_error is not None:
                    raise self._save_error
                self._wake.wait()

    def close(self):
        with self._wake:
            self._closing = True
            self._wake.notify()
        self._save_thread.join()
        if self._save_error is not None:
            raise self._save_error

    def _saver_loop(self):
        while True:
            with self._wake:
                self._wake.wait_for(
                    lambda: self._has_unsaved() or self._closing,
                    timeout=self._save_seconds,
                )
                if self._closing and not self._has_unsaved():
                    return
                if not self._has_unsaved():
                    continue
                target_seq = self._log.latest()

            try:
                self._do_save(target_seq)
            except Exception as e:
                with self._wake:
                    self._save_error = e
                    self._wake.notify_all()
                return

            with self._wake:
                self._persisted_through_seq = target_seq
                self._wake.notify_all()

    def _has_unsaved(self) -> bool:
        return self._log.latest() > self._persisted_through_seq

    def _do_save(self, target_seq: int):
        if self._sealed:
            active_first_seq = self._sealed[-1].last_seq + 1
            next_num = segment.segment_num(self._sealed[-1].filename) + 1
        else:
            active_first_seq = 0
            next_num = 1

        to_write = self._log.events()[active_first_seq: target_seq + 1]
        chunks = segment.bin_events_by_size(to_write, self._segment_bytes)

        new_segments: list[SegmentMeta] = []
        for i, chunk in enumerate(chunks):
            filename = segment.segment_filename(next_num + i)
            segment.atomic_write_events(self._directory / filename, chunk)
            new_segments.append(SegmentMeta(
                filename=filename,
                last_seq=chunk[-1].seq,
            ))

        self._sealed = self._sealed + new_segments[:-1]
        self._active = new_segments[-1]
        all_metas = self._sealed + [self._active]

        segment.write_manifest(
            self._directory / _MANIFEST_NAME,
            {"segments": [asdict(s) for s in all_metas]},
        )
