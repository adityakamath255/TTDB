from typing import Any, Iterator
from datetime import datetime
from bisect import bisect_right

from .types import Event, ABSENT


class Log:
    def __init__(self, events: list[Event]):
        self.events = events
        self.index: dict[str, list[int]] = {}
        for event in events:
            self.index.setdefault(event.key, []).append(event.seq)

    def append(self, key: str, value: Any, at: datetime) -> int:
        event = Event(len(self.events), key, value, at)
        self.events.append(event)
        self.index.setdefault(key, []).append(event.seq)
        return event.seq

    def resolve(self, key: str, through: int) -> Any:
        seqs = self.index.get(key, ())
        idx = bisect_right(seqs, through) - 1
        if idx < 0:
            return ABSENT
        else:
            return self.events[seqs[idx]].value

    def history(self, key: str) -> Iterator[Event]:
        return (
            self.events[s] 
            for s in self.index.get(key, ())
        )
