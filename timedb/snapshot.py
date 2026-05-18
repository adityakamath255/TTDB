from typing import Optional, Iterator
from datetime import datetime
from dataclasses import dataclass

import immutables

from .types import OptValue, ABSENT


@dataclass(frozen=True)
class Snapshot:
    seq: int
    state: immutables.Map
    timestamp: Optional[datetime]

    def get(self, key: str) -> OptValue:
        return self.state.get(key, ABSENT)

    def keys(self) -> Iterator[str]:
        return iter(self.state)

    def items(self) -> Iterator[tuple[str, OptValue]]:
        return iter(self.state.items())

    def diff(
        self, other: "Snapshot"
    ) -> Iterator[tuple[str, OptValue, OptValue]]:
        a, b = self.state, other.state
        for k in set(a) | set(b):
            a_val = a.get(k, ABSENT)
            b_val = b.get(k, ABSENT)
            if a_val != b_val:
                yield (k, a_val, b_val)

    def __contains__(self, key: str) -> bool:
        return self.get(key) is not ABSENT

    def __repr__(self) -> str:
        return f"Snapshot(seq={self.seq})"
