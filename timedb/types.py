from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterator


class _Absent:
    def __repr__(self) -> str:
        return "ABSENT"


ABSENT = _Absent()


@dataclass(frozen=True, slots=True)
class Event:
    seq: int
    key: str
    value: Any
    timestamp: datetime


def validate_value(value: Any):
    if isinstance(value, (str, int, float, bool)) or value is None:
        pass
    elif isinstance(value, list):
        for item in value:
            validate_value(item)
    elif isinstance(value, dict):
        for k, v in value.items():
            if not isinstance(k, str):
                raise TypeError(
                    f"dict keys must be strings, "
                    f"got {type(k).__name__}"
                )
            validate_value(v)
    else:
        raise TypeError(
            f"value of type {type(value).__name__} "
            f"is not JSON-serializable"
        )


class Log:
    def __init__(self, events: list[Event]):
        self.events = events

    def append(self, key: str, value: Any, at: datetime) -> int:
        if self.events and at < self.events[-1].timestamp:
            raise ValueError(
                f"timestamp {at} precedes last event at "
                f"{self.events[-1].timestamp}"
            )

        event = Event(len(self.events), key, value, at)
        self.events.append(event)
        return event.seq

    def resolve(self, key: str, through: int) -> Any:
        for i in range(through, -1, -1):
            if self.events[i].key == key:
                return self.events[i].value
        else:
            return ABSENT

    def history(self, key: str) -> Iterator[Event]:
        return (e for e in self.events if e.key == key)
