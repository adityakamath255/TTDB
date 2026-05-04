from typing import Any, Iterator
from datetime import datetime

import immutables

from .types import Event
from .buffer import Buffer
from .archive import Archive


class Log:
    def __init__(self, buffer: Buffer, archive: Archive):
        self._buffer = buffer
        self._archive = archive

    def append(self, key: str, value: Any, at: datetime) -> int:
        return self._buffer.append(key, value, at)

    def append_tx(
        self, writes: dict[str, Any], at: datetime
    ) -> tuple[int, int]:
        return self._buffer.append_tx(writes, at)

    def count(self) -> int:
        return self._buffer.count()

    def state_at(self, p: int) -> immutables.Map:
        s = self._buffer.state_at(p)
        if s is not None:
            return s
        return self._archive.state_at(p)

    def event_at(self, seq: int) -> Event:
        e = self._buffer.event_at(seq)
        if e is not None:
            return e
        return self._archive.event_at(seq)

    def events_between(self, lo: int, hi: int) -> Iterator[Event]:
        buf_lo, buf_events = self._buffer.snapshot()
        if lo < buf_lo:
            yield from self._archive.events_between(lo, min(hi, buf_lo))
        active_lo = max(lo, buf_lo)
        active_hi = min(hi, buf_lo + len(buf_events))
        for i in range(active_lo, active_hi):
            yield buf_events[i - buf_lo]

    def events(self) -> Iterator[Event]:
        buf_lo, buf_events = self._buffer.snapshot()
        if buf_lo > 0:
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
