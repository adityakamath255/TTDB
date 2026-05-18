from dataclasses import dataclass
from datetime import datetime
from typing import Optional

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
    seq: int
    key: str
    value: OptValue
    timestamp: datetime
    tx_id: Optional[int]


def fold_event(root: immutables.Map, event: Event) -> immutables.Map:
    if event.value is not ABSENT:
        return root.set(event.key, event.value)
    elif event.key in root:
        return root.delete(event.key)
    else:
        return root
