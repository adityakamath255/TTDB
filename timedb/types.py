from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional


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
    tx_id: Optional[int]
    prev_seq: Optional[int]


def _validate_json(value: Any):
    if isinstance(value, (str, int, float, bool)) or value is None:
        pass

    elif isinstance(value, list):
        for item in value:
            _validate_json(item)

    elif isinstance(value, dict):
        for k, v in value.items():
            if not isinstance(k, str):
                raise TypeError(
                    f"dict keys must be strings, "
                    f"got {type(k).__name__}"
                )
            _validate_json(v)

    else:
        raise TypeError(
            f"value of type {type(value).__name__} "
            f"is not JSON-serializable"
        )


def validate_value(value: Any):
    if value is not ABSENT:
        return _validate_json(value)
