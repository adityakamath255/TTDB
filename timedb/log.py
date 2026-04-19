from typing import Any, Optional, Iterator
from datetime import datetime
from bisect import bisect_right

from .types import Event, ABSENT


class Log:
    def __init__(self, events: list[Event]):
        self.events = events
        self.index: dict[str, list[int]] = {}
        for event in events:
            self.index.setdefault(event.key, []).append(event.seq)

    def append(
        self, key: str, value: Any, at: datetime, tx_id: Optional[int]
    ) -> int:
        event = Event(len(self.events), key, value, at, tx_id)
        self.events.append(event)
        self.index.setdefault(key, []).append(event.seq)
        return event.seq

    def append_transaction(
        self, writes: dict[str, Any], at: datetime
    ) -> tuple[int, int]:
        tx_id = len(self.events)
        last = tx_id
        for key, value in writes.items():
            last = self.append(key, value, at, tx_id)
        return (tx_id, last)

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

    def transaction_events(self, tx_id: int) -> Iterator[Event]:
        if (
            tx_id < 0 
            or tx_id >= len(self.events)
            or self.events[tx_id].tx_id != tx_id
        ):
            raise ValueError(f"no transaction with id {tx_id}")

        def walk():
            i = tx_id
            while (
                i < len(self.events)
                and self.events[i].tx_id == tx_id
            ):
                yield self.events[i]
                i += 1

        return walk()
