from dataclasses import dataclass
from datetime import datetime
from functools import reduce
from typing import Iterable

import immutables


class Absent:
    def __repr__(self) -> str:
        return "ABSENT"


ABSENT = Absent()
EMPTY: immutables.Map = immutables.Map()

Value = str | int | float | bool | None | list["Value"] | dict[str, "Value"]
OptValue = Value | Absent


@dataclass(frozen=True, slots=True)
class Event:
    tx: int
    key: str
    value: OptValue
    timestamp: datetime


def fold_event(root: immutables.Map, event: Event) -> immutables.Map:
    if event.value is not ABSENT:
        return root.set(event.key, event.value)
    elif event.key in root:
        return root.delete(event.key)
    else:
        return root


def fold_batch(root: immutables.Map, batch: Iterable[Event]) -> immutables.Map:
    return reduce(fold_event, batch, root)
