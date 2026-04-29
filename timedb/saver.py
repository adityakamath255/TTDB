from dataclasses import dataclass, asdict
from pathlib import Path
from threading import Thread, Condition, Lock
from typing import Optional, Iterator

import immutables

from .log import Log, _EMPTY
from .types import Event, ABSENT
from . import segment


_MANIFEST_NAME = "manifest"


@dataclass(frozen=True, slots=True)
class SegmentMeta:
    filename: str
    last_seq: int


def _state_filename(seg: SegmentMeta) -> str:
    return segment.state_filename(segment.segment_num(seg.filename))


def _first_seq(sealed: list[SegmentMeta], idx: int) -> int:
    return 0 if idx == 0 else sealed[idx - 1].last_seq + 1


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
        self._log = log
        self._directory = directory
        self._sealed = sealed
        self._active = active
        self._save_events = save_events
        self._save_seconds = save_seconds
        self._segment_bytes = segment_bytes

        self._meta_lock = Lock()
        self._persisted_count = active.last_seq + 1 if active else 0
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

        if active:
            active_events = segment.read_events(directory / active.filename)
            latest_root = segment.read_state(directory / _state_filename(active))
            if sealed:
                active_base_root = segment.read_state(
                    directory / _state_filename(sealed[-1])
                )
                active_first_seq = sealed[-1].last_seq + 1
            else:
                active_base_root = _EMPTY
                active_first_seq = 0
        else:
            active_events = []
            latest_root = _EMPTY
            active_base_root = _EMPTY
            active_first_seq = 0

        log.bootstrap(active_events, latest_root, active_base_root, active_first_seq)

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
        n = self._log.count()
        if n - self._persisted_count >= self._save_events:
            with self._wake:
                self._wake.notify()

    def flush(self):
        if self._save_error is not None:
            raise self._save_error
        target = self._log.count()
        with self._wake:
            self._wake.notify()
            while self._persisted_count < target:
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

            try:
                new_persisted = self._do_save()
            except Exception as e:
                with self._wake:
                    self._save_error = e
                    self._wake.notify_all()
                return

            with self._wake:
                self._persisted_count = new_persisted
                self._wake.notify_all()

    def _has_unsaved(self) -> bool:
        return self._log.count() > self._persisted_count

    def _do_save(self) -> int:
        active_events = self._log.active_events()
        if not active_events:
            return self._persisted_count

        if self._sealed:
            base_root = segment.read_state(
                self._directory / _state_filename(self._sealed[-1])
            )
        else:
            base_root = _EMPTY

        if self._active:
            active_num = segment.segment_num(self._active.filename)
        elif self._sealed:
            active_num = segment.segment_num(self._sealed[-1].filename) + 1
        else:
            active_num = 1

        chunks = segment.bin_events_by_size(active_events, self._segment_bytes)

        new_segments: list[SegmentMeta] = []
        chunk_end_states: list[immutables.Map] = []
        state = base_root
        for i, chunk in enumerate(chunks):
            for e in chunk:
                if e.value is not ABSENT:
                    state = state.set(e.key, e.value)
                elif e.key in state:
                    state = state.delete(e.key)
            chunk_end_states.append(state)
            chunk_num = active_num + i
            log_filename = segment.segment_filename(chunk_num)
            segment.atomic_write_events(self._directory / log_filename, chunk)
            segment.atomic_write_state(
                self._directory / segment.state_filename(chunk_num), state
            )
            new_segments.append(SegmentMeta(
                filename=log_filename,
                last_seq=chunk[-1].seq,
            ))

        new_sealed = self._sealed + new_segments[:-1]
        new_active = new_segments[-1]
        all_metas = new_sealed + [new_active]

        segment.write_manifest(
            self._directory / _MANIFEST_NAME,
            {"segments": [asdict(s) for s in all_metas]},
        )

        with self._meta_lock:
            self._sealed = new_sealed
            self._active = new_active

        if len(chunks) > 1:
            new_active_first_seq = chunks[-1][0].seq
            new_active_base = chunk_end_states[-2]
            self._log.truncate_active(new_active_first_seq, new_active_base)

        return new_active.last_seq + 1

    def event_at(self, seq: int) -> Event:
        with self._meta_lock:
            sealed = self._sealed
        for idx, seg in enumerate(sealed):
            first = _first_seq(sealed, idx)
            if first <= seq <= seg.last_seq:
                events = segment.read_events(self._directory / seg.filename)
                return events[seq - first]
        raise IndexError(f"seq {seq} not in sealed range")

    def events_between(self, lo: int, hi: int) -> Iterator[Event]:
        with self._meta_lock:
            sealed = self._sealed
        for idx, seg in enumerate(sealed):
            first = _first_seq(sealed, idx)
            last = seg.last_seq
            if hi <= first:
                return
            if lo > last:
                continue
            events = segment.read_events(self._directory / seg.filename)
            for e in events:
                if lo <= e.seq < hi:
                    yield e

    def state_at(self, p: int) -> immutables.Map:
        with self._meta_lock:
            sealed = self._sealed
        for idx, seg in enumerate(sealed):
            end_pos = seg.last_seq + 1
            if p == end_pos:
                return segment.read_state(self._directory / _state_filename(seg))
            if p < end_pos:
                base = (
                    segment.read_state(
                        self._directory / _state_filename(sealed[idx - 1])
                    )
                    if idx > 0
                    else _EMPTY
                )
                events = segment.read_events(self._directory / seg.filename)
                root = base
                for e in events:
                    if e.seq >= p:
                        break
                    if e.value is not ABSENT:
                        root = root.set(e.key, e.value)
                    elif e.key in root:
                        root = root.delete(e.key)
                return root
        raise IndexError(f"position {p} not in sealed range")
