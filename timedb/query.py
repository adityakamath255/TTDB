from typing import Iterable, Iterator
from datetime import datetime

import immutables

from .types import Event, OptValue, ABSENT


def diff(
    a: immutables.Map, b: immutables.Map
) -> Iterator[tuple[str, OptValue, OptValue]]:
    for k in set(a) | set(b):
        a_val = a.get(k, ABSENT)
        b_val = b.get(k, ABSENT)
        if a_val != b_val:
            yield (k, a_val, b_val)


def history(
    events: Iterable[Event], key: str
) -> Iterator[tuple[int, OptValue, datetime]]:
    return (
        (e.seq, e.changes[key], e.timestamp)
        for e in events
        if key in e.changes
    )
