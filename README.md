# TTDB

TTDB is an append-only key-value store for Python. Every call to `apply` records one event in a JSONL journal, and immutable snapshots expose the state after any earlier event or timestamp.

The store accepts strings, integers, finite floats, and booleans. Assigning `None` deletes a key. TTDB validates and copies each change set before storing it.

## Running locally

TTDB requires Python 3.14 or newer and [uv](https://docs.astral.sh/uv/):

```sh
uv sync
uv run pytest
```

The implementation is one module, `ttdb.py`. It can also be copied directly into another project that provides the `immutables` dependency.

## Usage

Create an in-memory store:

```python
import ttdb

db = ttdb.in_memory()
db.apply({"user.name": "alice"})
db.apply({"user.name": "bob", "enabled": True})

assert db.latest()["user.name"] == "bob"
assert db.at(0)["user.name"] == "alice"
```

Open a persistent store through its context manager:

```python
from pathlib import Path

import ttdb

with ttdb.connect(Path("events.jsonl")) as db:
    db.apply({"order.status": "pending"})
    db.apply({"order.status": "paid"})
```

`connect` holds an exclusive advisory lock while the context is open. `inspect` opens an existing journal without taking that lock and returns a read-only `DB`:

```python
db = ttdb.inspect("events.jsonl")
current = db.latest()
past = db.at(0)
```

`DB` has no write method. `WritableDB` adds `apply`, so write capability is represented by the handle type.

## Time travel

Event indexes start at zero. `at(index)` returns the state after that event, and `latest()` returns the newest state.

`as_of(timestamp)` returns the state containing every event whose timestamp is less than or equal to the supplied time. Timestamps must include a UTC offset. TTDB normalizes them to UTC and rejects an explicit timestamp earlier than the previous event.

```python
from datetime import UTC, datetime

state = db.as_of(datetime(2026, 1, 1, tzinfo=UTC))
```

The module also provides two operations over returned values:

- `diff(before, after)` yields keys whose values differ between two snapshots.
- `history(events, key)` yields every event that changed or deleted a key.

## Storage and recovery

Each transaction is one newline-terminated JSON object containing a changes mapping and an ISO 8601 timestamp. Persistent `apply` calls append one line, flush it, and call `fsync` before updating in-memory state.

When `connect` finds a final record without a newline, it treats that record as a torn write and truncates it. `inspect` ignores the same incomplete tail without modifying the file. A newline-terminated record that is invalid JSON or violates the event format raises `CorruptJournal` and remains untouched.

Snapshots and event change sets use `immutables.Map`. `events()` returns an immutable tuple. Internally, TTDB retains one event and one structurally shared map root per revision, so selecting a known event does not replay the journal.

## Limitations

- Persistence and locking depend on POSIX `fcntl`; Windows is not supported.
- One writer may hold a journal at a time. Readers see the contents present when they were opened and do not refresh automatically.
- The entire revision list and all snapshot roots remain in memory.
- Recovery removes only a torn final write. It does not authenticate or repair a complete corrupt record.
- The journal format has no version field or migration mechanism.
- Sequence indexes are positional and change if journal history is removed or rewritten outside TTDB.
