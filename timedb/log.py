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


def _load_segments(
    directory: Path, manifest_path: Path
) -> tuple[list[SegmentMeta], Optional[SegmentMeta], "Archive", int]:
    if manifest_path.exists():
        raw = segment.read_manifest(manifest_path)
        metas = [SegmentMeta.from_dict(s) for s in raw["segments"]]
    else:
        metas = []
    # Writer invariant: every committed manifest has the active segment as
    # its trailing entry. The format is positional, not tagged, so this is
    # what makes the split below well-defined. Saver._publish_manifest_update
    # is the single producer and must preserve this.
    sealed_metas = metas[:-1] if metas else []
    active = metas[-1] if metas else None
    archive = Archive(directory, sealed_metas)
    buffer_lo = sealed_metas[-1].last_seq + 1 if sealed_metas else 0
    return sealed_metas, active, archive, buffer_lo


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
    def in_memory(cls, strict_time: bool = False) -> "Log":
        return cls(Buffer(0, EMPTY, strict_time=strict_time), None, None)

    @classmethod
    def open(
        cls,
        directory: Path,
        save_events: int,
        save_seconds: float,
        segment_bytes: int,
        fsync_mode: FsyncMode,
        on_error: Optional[OnError] = None,
        strict_time: bool = False,
    ) -> "Log":

        directory.mkdir(parents=True, exist_ok=True)
        lock_fd = _acquire_lock(directory / LOCK_NAME)
        try:
            _sweep_tmp_files(directory)
            manifest_path = directory / MANIFEST_NAME
            sealed_metas, active, archive, buffer_lo = _load_segments(
                directory, manifest_path,
            )

            if active is not None:
                expected_count = active.last_seq - buffer_lo + 1
                active_events = segment.recover_active_log(
                    directory / active.filename(), expected_count,
                )
                if len(active_events) < expected_count:
                    if active_events:
                        active = SegmentMeta(
                            active.num,
                            active_events[-1].seq,
                            active_events[-1].timestamp,
                        )
                    else:
                        active = None
                    fixed = sealed_metas + ([active] if active else [])
                    segment.write_manifest(
                        manifest_path,
                        {"segments": [s.to_dict() for s in fixed]},
                        fsync_mode == "save",
                    )
                # active.num.state may sit on disk reflecting a last_seq past
                # the truncated active log. Recovery never reads it (we replay
                # the active log onto head_state), and the next save will
                # overwrite it. Don't change that without addressing this.
                base_state = archive.head_state()
                buffer = Buffer(buffer_lo, base_state, active_events, strict_time=strict_time)
            else:
                base_state = archive.head_state() if sealed_metas else EMPTY
                buffer = Buffer(buffer_lo, base_state, strict_time=strict_time)

            saver = Saver(
                buffer, archive, directory, manifest_path, active, save_events,
                save_seconds, segment_bytes, fsync_mode, on_error,
            )
            saver.start()
        except BaseException:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)
            raise

        return cls(buffer, archive, saver, lock_fd)

    @classmethod
    def open_readonly(cls, directory: Path) -> "Log":
        manifest_path = directory / MANIFEST_NAME
        sealed_metas, active, archive, buffer_lo = _load_segments(
            directory, manifest_path,
        )
        if active is not None:
            expected_count = active.last_seq - buffer_lo + 1
            active_events = segment.read_active_events(
                directory / active.filename(), expected_count,
            )
            base_state = archive.head_state() if sealed_metas else EMPTY
            buffer = Buffer(buffer_lo, base_state, active_events)
        else:
            base_state = archive.head_state() if sealed_metas else EMPTY
            buffer = Buffer(buffer_lo, base_state)
        return cls(buffer, archive, None, None)

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

    def position_at_time(self, t: datetime) -> int:
        p = self._buffer.position_at_time(t)
        if p is not None:
            return p
        if self._archive is not None:
            return self._archive.position_at_time(t)
        return 0

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
