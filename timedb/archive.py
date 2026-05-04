from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Iterator, Optional

import immutables

from . import segment
from .types import Event, EMPTY, apply


@dataclass(frozen=True, slots=True)
class SegmentMeta:
    num: int
    last_seq: int

    @property
    def filename(self) -> str:
        return segment.segment_filename(self.num)

    @property
    def state_filename(self) -> str:
        return segment.state_filename(self.num)


def _first_seq(segments: list[SegmentMeta], idx: int) -> int:
    return 0 if idx == 0 else segments[idx - 1].last_seq + 1


class Archive:
    def __init__(
        self,
        directory: Optional[Path],
        segments: list[SegmentMeta],
    ):
        if segments and directory is None:
            raise ValueError("Archive with segments requires a directory")
        self._directory = directory
        self._segments = list(segments)
        self._lock = Lock()

    @classmethod
    def empty(cls) -> "Archive":
        return cls(None, [])

    def segments(self) -> list[SegmentMeta]:
        with self._lock:
            return list(self._segments)

    def head_state(self) -> immutables.Map:
        with self._lock:
            if not self._segments:
                return EMPTY
            last = self._segments[-1]
            directory = self._directory
        assert directory is not None
        return segment.read_state(directory / last.state_filename)

    def add_sealed(self, meta: SegmentMeta):
        with self._lock:
            self._segments.append(meta)

    def event_at(self, seq: int) -> Event:
        sealed = self.segments()
        for idx, seg in enumerate(sealed):
            first = _first_seq(sealed, idx)
            if first <= seq <= seg.last_seq:
                assert self._directory is not None
                events = segment.read_events(self._directory / seg.filename)
                return events[seq - first]
        raise IndexError(f"seq {seq} not in archive")

    def events_between(self, lo: int, hi: int) -> Iterator[Event]:
        sealed = self.segments()
        for idx, seg in enumerate(sealed):
            first = _first_seq(sealed, idx)
            last = seg.last_seq
            if hi <= first:
                return
            if lo > last:
                continue
            assert self._directory is not None
            events = segment.read_events(self._directory / seg.filename)
            for e in events:
                if lo <= e.seq < hi:
                    yield e

    def state_at(self, p: int) -> immutables.Map:
        sealed = self.segments()
        for idx, seg in enumerate(sealed):
            end_pos = seg.last_seq + 1
            if p == end_pos:
                assert self._directory is not None
                return segment.read_state(
                    self._directory / seg.state_filename
                )
            if p < end_pos:
                assert self._directory is not None
                base = (
                    segment.read_state(
                        self._directory / sealed[idx - 1].state_filename
                    )
                    if idx > 0
                    else EMPTY
                )
                events = segment.read_events(self._directory / seg.filename)
                root = base
                for e in events:
                    if e.seq >= p:
                        break
                    root = apply(root, e)
                return root
        raise IndexError(f"position {p} not in archive")
