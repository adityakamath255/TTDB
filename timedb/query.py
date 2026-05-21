from typing import Iterable, Iterator

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


def history(events: Iterable[Event], key: str) -> Iterator[Event]:
    return (e for e in events if e.key == key)
