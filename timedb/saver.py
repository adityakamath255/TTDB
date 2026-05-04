from dataclasses import asdict, dataclass
from pathlib import Path
from threading import Thread, Condition
from typing import Callable, Optional, Literal

from . import segment
from .segment import SegmentMeta, SerializedEvent
from .types import EMPTY, apply
from .buffer import Buffer
from .archive import Archive

FsyncMode = Literal["none", "seal", "save"]
OnError = Callable[[Exception], None]


@dataclass(frozen=True, slots=True)
class _FsyncPolicy:
    active: bool
    sealed: bool
    manifest: bool


class Saver:
    def __init__(
        self,
        buffer: Buffer,
        archive: Archive,
        directory: Path,
        manifest_path: Path,
        active: Optional[SegmentMeta],
        save_events: int,
        save_seconds: float,
        segment_bytes: int,
        fsync_mode: FsyncMode,
        on_error: Optional[OnError] = None,
    ):
        self._buffer = buffer
        self._archive = archive
        self._directory = directory
        self._manifest_path = manifest_path
        self._active: Optional[SegmentMeta] = active
        self._save_events = save_events
        self._save_seconds = save_seconds
        self._segment_bytes = segment_bytes
        self._fsync_mode = fsync_mode
        self._on_error = on_error

        self._wake = Condition()
        self._closing = False
        self._save_error: Optional[Exception] = None
        self._save_thread = Thread(target=self._saver_loop, daemon=True)

    def start(self):
        self._save_thread.start()

    def check_writable(self):
        if self._save_error is not None:
            raise self._save_error
        if self._closing:
            raise RuntimeError("Saver is closed")

    def health_check(self):
        if self._save_error is not None:
            raise self._save_error

    def signal_pending(self):
        n = self._buffer.next_seq()
        if n - self._persisted_count() >= self._save_events:
            with self._wake:
                self._wake.notify()

    def _persisted_count(self) -> int:
        return self._active.last_seq + 1 if self._active else 0

    def flush(self):
        if self._save_error is not None:
            raise self._save_error
        target = self._buffer.next_seq()
        with self._wake:
            self._wake.notify()
            while self._persisted_count() < target:
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
                self._persist_buffer()
            except Exception as e:
                with self._wake:
                    self._save_error = e
                    self._wake.notify_all()
                if self._on_error is not None:
                    try:
                        self._on_error(e)
                    except Exception:
                        pass
                return

            with self._wake:
                self._wake.notify_all()

    def _has_unsaved(self) -> bool:
        return self._buffer.next_seq() > self._persisted_count()

    def _persist_buffer(self):
        _, events = self._buffer.snapshot()
        if not events:
            return

        chunks = segment.bin_events_by_size(events, self._segment_bytes)
        base_state = self._archive_head_state()
        next_num = self._next_segment_num()
        fsync = self._fsync_policy(has_seal=len(chunks) > 1)

        new_metas = self._write_chunks(chunks, base_state, next_num, fsync)
        self._publish_manifest_update(new_metas, fsync.manifest)

        for meta in new_metas[:-1]:
            self._archive.add_sealed(meta)
        self._active = new_metas[-1]

        self._delete_orphans()

        if len(chunks) > 1:
            self._buffer.truncate_to(chunks[-1][0][0].seq)

    def _archive_head_state(self):
        sealed = self._archive.segments()
        if not sealed:
            return EMPTY
        return segment.read_state(
            self._directory / sealed[-1].state_filename()
        )

    def _next_segment_num(self) -> int:
        if self._active:
            return self._active.num + 1
        sealed = self._archive.segments()
        if sealed:
            return sealed[-1].num + 1
        return 1

    def _fsync_policy(self, has_seal: bool) -> _FsyncPolicy:
        mode = self._fsync_mode
        return _FsyncPolicy(
            active=mode == "save",
            sealed=mode in ("save", "seal"),
            manifest=mode == "save" or (mode == "seal" and has_seal),
        )

    def _write_chunks(
        self,
        chunks: list[list[SerializedEvent]],
        base_state,
        next_num: int,
        fsync: _FsyncPolicy,
    ) -> list[SegmentMeta]:
        metas: list[SegmentMeta] = []
        state = base_state
        last = len(chunks) - 1
        for i, chunk in enumerate(chunks):
            for event, _ in chunk:
                state = apply(state, event)
            do_fsync = fsync.active if i == last else fsync.sealed
            meta = SegmentMeta(num=next_num + i, last_seq=chunk[-1][0].seq)
            segment.atomic_write_lines(
                self._directory / meta.filename(),
                (line for _, line in chunk),
                do_fsync,
            )
            segment.atomic_write_state(
                self._directory / meta.state_filename(), state, do_fsync,
            )
            metas.append(meta)
        return metas

    def _publish_manifest_update(
        self, new_metas: list[SegmentMeta], fsync_manifest: bool
    ):
        all_metas = self._archive.segments() + new_metas[:-1] + [new_metas[-1]]
        segment.write_manifest(
            self._manifest_path,
            {"segments": [asdict(s) for s in all_metas]},
            fsync_manifest,
        )

    def _delete_orphans(self):
        kept = {s.num for s in self._archive.segments()}
        if self._active is not None:
            kept.add(self._active.num)
        for path in self._directory.iterdir():
            num = segment.parse_segment_filename(path.name)
            if num is not None and num not in kept:
                path.unlink(missing_ok=True)
