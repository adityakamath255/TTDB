import fcntl
import os
from dataclasses import asdict
from pathlib import Path
from threading import Thread, Condition, Lock
from typing import Literal, Optional

from . import segment
from .types import EMPTY, apply
from .buffer import Buffer
from .archive import Archive, SegmentMeta
from .log import Log


_MANIFEST_NAME = "manifest"
_LOCK_NAME = "LOCK"

FsyncMode = Literal["none", "seal", "save"]


class Saver:
    def __init__(
        self,
        buffer: Buffer,
        archive: Archive,
        directory: Path,
        active: Optional[SegmentMeta],
        save_events: int,
        save_seconds: float,
        segment_bytes: int,
        fsync_mode: FsyncMode,
        lock_fd: int,
    ):
        self._buffer = buffer
        self._archive = archive
        self._directory = directory
        self._active: Optional[SegmentMeta] = active
        self._save_events = save_events
        self._save_seconds = save_seconds
        self._segment_bytes = segment_bytes
        self._fsync_mode = fsync_mode
        self._lock_fd: Optional[int] = lock_fd

        self._meta_lock = Lock()
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

    def signal_pending(self):
        n = self._buffer.count()
        if n - self._persisted_count() >= self._save_events:
            with self._wake:
                self._wake.notify()

    def _persisted_count(self) -> int:
        with self._meta_lock:
            return self._active.last_seq + 1 if self._active else 0

    def flush(self):
        if self._save_error is not None:
            raise self._save_error
        target = self._buffer.count()
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
        if self._lock_fd is not None:
            fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
            os.close(self._lock_fd)
            self._lock_fd = None
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
                return

            with self._wake:
                self._wake.notify_all()

    def _has_unsaved(self) -> bool:
        return self._buffer.count() > self._persisted_count()

    def _persist_buffer(self):
        _, events = self._buffer.snapshot()
        if not events:
            return

        archive_segments = self._archive.segments()
        if archive_segments:
            base_state = segment.read_state(
                self._directory / archive_segments[-1].state_filename
            )
        else:
            base_state = EMPTY

        if self._active:
            existing_max = self._active.num
        elif archive_segments:
            existing_max = archive_segments[-1].num
        else:
            existing_max = 0
        next_num = existing_max + 1

        chunks = segment.bin_events_by_size(events, self._segment_bytes)
        has_seal = len(chunks) > 1
        fsync_active = self._fsync_mode == "save"
        fsync_sealed = self._fsync_mode in ("save", "seal")
        fsync_manifest = (
            self._fsync_mode == "save"
            or (self._fsync_mode == "seal" and has_seal)
        )

        new_metas: list[SegmentMeta] = []
        state = base_state
        for i, chunk in enumerate(chunks):
            for e in chunk:
                state = apply(state, e)
            chunk_num = next_num + i
            is_active = i == len(chunks) - 1
            do_fsync = fsync_active if is_active else fsync_sealed
            meta = SegmentMeta(num=chunk_num, last_seq=chunk[-1].seq)
            segment.atomic_write_events(
                self._directory / meta.filename, chunk, do_fsync,
            )
            segment.atomic_write_state(
                self._directory / meta.state_filename, state, do_fsync,
            )
            new_metas.append(meta)

        new_sealed = new_metas[:-1]
        new_active = new_metas[-1]
        all_metas = self._archive.segments() + new_sealed + [new_active]

        segment.write_manifest(
            self._directory / _MANIFEST_NAME,
            {"segments": [asdict(s) for s in all_metas]},
            fsync_manifest,
        )

        for meta in new_sealed:
            self._archive.add_sealed(meta)

        with self._meta_lock:
            self._active = new_active

        self._delete_orphans(all_metas)

        if has_seal:
            self._buffer.truncate_to(chunks[-1][0].seq)

    def _delete_orphans(self, kept: list[SegmentMeta]):
        kept_nums = {s.num for s in kept}
        for path in self._directory.iterdir():
            name = path.name
            if name == _MANIFEST_NAME or name == _LOCK_NAME:
                continue
            if not (name.endswith(".log") or name.endswith(".state")):
                path.unlink(missing_ok=True)
                continue
            try:
                num = segment.segment_num(name)
            except ValueError:
                continue
            if num not in kept_nums:
                path.unlink(missing_ok=True)


def open_database(
    directory: Path,
    save_events: int,
    save_seconds: float,
    segment_bytes: int,
    fsync_mode: FsyncMode,
) -> tuple[Log, Saver]:
    directory.mkdir(parents=True, exist_ok=True)
    lock_fd = _acquire_writer_lock(directory)

    manifest_path = directory / _MANIFEST_NAME
    if manifest_path.exists():
        raw = segment.read_manifest(manifest_path)
        metas = [
            SegmentMeta(num=s["num"], last_seq=s["last_seq"])
            for s in raw["segments"]
        ]
    else:
        metas = []

    if metas:
        sealed_metas, active = metas[:-1], metas[-1]
    else:
        sealed_metas, active = [], None

    archive = Archive(directory, sealed_metas)

    if active:
        base_state = archive.head_state()
        active_events = segment.read_events(directory / active.filename)
        buffer_lo = sealed_metas[-1].last_seq + 1 if sealed_metas else 0
        buffer = Buffer(buffer_lo, base_state, active_events)
    else:
        buffer = Buffer(0, EMPTY)

    log = Log(buffer, archive)
    saver = Saver(
        buffer, archive, directory, active,
        save_events, save_seconds, segment_bytes,
        fsync_mode, lock_fd,
    )
    return log, saver


def _acquire_writer_lock(directory: Path) -> int:
    lock_fd = os.open(
        str(directory / _LOCK_NAME),
        os.O_CREAT | os.O_RDWR,
        0o644,
    )
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(lock_fd)
        raise RuntimeError(
            f"Database at {directory} is already open by another process"
        )
    return lock_fd
