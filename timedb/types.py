from dataclasses import dataclass
from datetime import datetime

import immutables


class Absent:
    pass


ABSENT = Absent()

Value = str | int | float | bool | None | list["Value"] | dict[str, "Value"]
OptValue = Value | Absent


@dataclass(frozen=True, slots=True)
class Event:
    seq: int
    changes: dict[str, OptValue]
    timestamp: datetime


def fold_event(root: immutables.Map, event: Event) -> immutables.Map:
    new = root
    for k, v in event.changes.items():
        if v is ABSENT:
            if k in new:
                new = new.delete(k)
        else:
            new = new.set(k, v)
    return new
