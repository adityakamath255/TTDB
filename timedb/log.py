import fcntl
import os
from typing import Optional, Any, Iterator
from pathlib import Path
from datetime import datetime

import immutables

from . import segment
from .segment import SegmentMeta
from .types import Event, EMPTY
from .buffer import Buffer
from .archive import Archive
from .saver import Saver, FsyncMode, OnError


MANIFEST_NAME = "manifest"
LOCK_NAME = "LOCK"


def _acquire_lock(path: Path) -> int:
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        raise RuntimeError(f"database at {path.parent} is locked by another process")
    return fd


def _sweep_tmp_files(directory: Path):
    for p in directory.iterdir():
        if p.suffix == ".tmp":
            p.unlink(missing_ok=True)


class Log:
    def __init__(
        self,
        buffer: Buffer,
        archive: Optional[Archive],
        saver: Optional[Saver],
        lock_fd: Optional[int] = None,
    ):
        self._buffer = buffer
        self._archive = archive
        self._saver = saver
        self._lock_fd = lock_fd

    @classmethod
    def in_memory(cls) -> "Log":
        return cls(Buffer(0, EMPTY), None, None)

    @classmethod
    def open(
        cls,
        directory: Path,
        save_events: int,
        save_seconds: float,
        segment_bytes: int,
        fsync_mode: FsyncMode,
        on_error: Optional[OnError] = None,
    ) -> "Log":

        directory.mkdir(parents=True, exist_ok=True)
        lock_fd = _acquire_lock(directory / LOCK_NAME)
        _sweep_tmp_files(directory)
        manifest_path = directory / MANIFEST_NAME

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
            if sealed_metas:
                buffer_lo = sealed_metas[-1].last_seq + 1
            else:
                buffer_lo = 0

            base_state = archive.head_state()
            active_events = segment.read_events(directory / active.filename())

            buffer = Buffer(buffer_lo, base_state, active_events)

        else:
            buffer = Buffer(0, EMPTY, [])

        saver = Saver(
            buffer, archive, directory, manifest_path, active, save_events,
            save_seconds, segment_bytes, fsync_mode, on_error,
        )

        saver.start()

        return cls(buffer, archive, saver, lock_fd)

    def append(self, key: str, value: Any, at: datetime) -> int:
        if self._saver is not None:
            self._saver.check_writable()
        res = self._buffer.append(key, value, at)
        if self._saver is not None:
            self._saver.signal_pending()
        return res

    def append_tx(
        self, writes: dict[str, Any], at: datetime
    ) -> tuple[int, int]:
        if self._saver is not None:
            self._saver.check_writable()
        res = self._buffer.append_tx(writes, at)
        if self._saver is not None:
            self._saver.signal_pending()
        return res

    def count(self) -> int:
        return self._buffer.next_seq()

    def state_at(self, seq: int) -> immutables.Map:
        s = self._buffer.state_at(seq)
        if s is not None:
            return s
        elif self._archive is not None:
            return self._archive.state_at(seq)
        else:
            raise ValueError(f"No state found at seq {seq}")

    def event_at(self, seq: int) -> Event:
        e = self._buffer.event_at(seq)
        if e is not None:
            return e
        elif self._archive is not None:
            return self._archive.event_at(seq)
        else:
            raise ValueError(f"No event found at seq {seq}")

    def events_between(self, lo: int, hi: int) -> Iterator[Event]:
        buf_lo, buf_events = self._buffer.snapshot()
        if lo < buf_lo and self._archive is not None:
            yield from self._archive.events_between(lo, min(hi, buf_lo))
        active_lo = max(lo, buf_lo)
        active_hi = min(hi, buf_lo + len(buf_events))
        for i in range(active_lo, active_hi):
            yield buf_events[i - buf_lo]

    def events(self) -> Iterator[Event]:
        buf_lo, buf_events = self._buffer.snapshot()
        if buf_lo > 0 and self._archive is not None:
            yield from self._archive.events_between(0, buf_lo)
        yield from buf_events

    def tx_exists(self, tx_id: int) -> bool:
        if tx_id < 0 or tx_id >= self.count():
            return False
        return self.event_at(tx_id).tx_id == tx_id

    def tx_events(self, tx_id: int) -> Iterator[Event]:
        for e in self.events_between(tx_id, self.count()):
            if e.tx_id != tx_id:
                return
            yield e

    def health_check(self):
        if self._saver is not None:
            self._saver.health_check()

    def flush(self):
        if self._saver is not None:
            self._saver.flush()

    def close(self):
        try:
            if self._saver is not None:
                self._saver.close()
        finally:
            if self._lock_fd is not None:
                fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
                os.close(self._lock_fd)
                self._lock_fd = None
