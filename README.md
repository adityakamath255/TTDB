# tiny-time-travel-db

An append-only key-value store for Python that retains full history. Every transaction is recorded as an immutable event in a log. Past states are queryable by sequence number or timestamp. Nothing is ever overwritten.

---

## Model

All writes go into a log of `Event` objects, each carrying a `changes` dict, and a timestamp `ts`. Append-only is the core invariant; everything else derives from it.

A *transaction* is one `apply` call producing one event. Multiple keys in the same `apply` share that event. Atomicity is per-event, by construction. There is no separate "single write" code path: a one-key write is just the N=1 case of a multi-key write.

State views are plain `immutables.Map` instances (HAMTs). Each event produces a new root by folding its changes into the prior root; structural sharing keeps every root cheap. Roots are cached, one per event plus an empty root at index 0. Lookups are O(log n); time-travel to a known sequence is O(1).

## The capability ladder

Two classes, ordered by capability:

* `DB`: read-only. Returns immutable Maps; no writes available.
* `WritableDB`: adds writes via `apply`.

Each is constructed via a module-level factory:

* `timedb.inspect(path)` → `DB`
* `timedb.in_memory()` → `WritableDB`
* `timedb.connect(path)` → `WritableDB` (inside a context manager)

The ladder is encoded in the type system. Calling `.apply()` on a `DB` handle is a type error, not a runtime error, since the method doesn't exist on the read-only class.

---

## Installation

Depends on `immutables` (HAMT). Copy `timedb.py` into your project.

---

## Usage

### In-memory

```python
import timedb

db = timedb.in_memory()
db.apply({"user.name": "alice"})
db.apply({"user.name": "bob"})

print(db.latest()["user.name"])  # "bob"

```

### Persistent

```python
with timedb.connect(path) as db:
    db.apply({"user.name": "alice"})

```

`path` is the journal file itself, not a directory. Writes are instantly appended and flushed to disk. Crash loses nothing that successfully completed an `apply` call; recovery on next connect truncates any partial trailing bytes.

Opening acquires an exclusive `flock` on the journal file. A second process trying to connect to the same path raises `RuntimeError`. The lock is released on context-manager exit, or by the kernel on process exit.

### Read-only

```python
ro = timedb.inspect(path)
print(ro.latest().get("user.name"))

```

No lock acquired, no writes available. Multiple read-only handles can coexist. A read-only handle does not refresh as a concurrent writer extends the journal, it sees the snapshot at open time. Calling `inspect` on a path that does not exist raises `FileNotFoundError`.

### Time travel

```python
snap = db.at(0)        # state after the first event
snap = db.at(1)        # state after the second event
snap = db.latest()     # state after all events

```

`at(seq)` is the state after event `seq`, symmetric with `events()[seq]`; both index the same event. Valid range is `[0, len(db.events()))`; `at(len(db.events()) - 1)` equals `latest()`.

By timestamp:

```python
snap = db.as_of(some_datetime)

```

`as_of(t)` returns the state reflecting every event with timestamp <= t. Lookup is O(log n) via binary search, which assumes timestamps are non-decreasing across events. `apply` enforces this at write time: a timestamp earlier than the last event raises `ValueError`.

### Atomic writes

```python
seq_total = db.apply({
    "order.id": 42,
    "order.status": "pending",
})

```

Every key in a single `apply` belongs to the same event and shares a timestamp. The return value is the total number of events currently in the store.

Deletion:

```python
db.apply({"user.name": None})
print(db.latest().get("user.name"))  # None (key absent)

```

### Iterating events

```python
for event in db.events():
    print(event.changes, event.ts)

```

`db.events()` returns the full event list. Windowed access, single-event access, and counts are all list operations:

```python
db.events()[lo:hi]
db.events()[seq]
len(db.events())
db.events()[seq].ts

```

### Diffing snapshots

```python
from timedb import diff

for key, before, after in diff(db.at(0), db.latest()):
    print(key, before, after)

```

`diff(a, b)` yields `(key, value_in_a, value_in_b)` for every key where the two snapshots disagree. `None` represents "not present in this snapshot."

### History of a key

```python
from timedb import history

for seq, value, ts in history(db.events(), "user.name"):
    print(seq, value, ts)

```

`history(events, key)` yields `(seq, value, timestamp)` tuples for every event whose `changes` touched `key`. Compose with slicing for windowed history:

```python
history(db.events()[lo:hi], "user.name")

```

Both `diff` and `history` are module-level rather than methods. They operate on values (Maps, event iterables) that already carry everything they need.

---

## Values

Accepted value types are: `str`, `int`, `float`, `bool`. The `Value` type alias encodes this exactly. Validation happens at type-check time, not runtime; pass anything other than a `Value` and pyright or mypy will reject the call.

Two constraints drove the choice. The journal is JSONL and must stay readable from any language. And in a versioned store, composite values fight the grain: mutating one element of a stored list or dict is read-modify-write, every change re-serializes the whole composite into the journal, and `diff`/`history` lose granularity (a changed element shows up as "the whole list changed"). Nested entity data flattens to namespaced keys (`user.address.street`, etc.); opaque blobs can JSON-stringify into a `str`.

---

## Durability and recovery

The journal is JSONL, one event per line. Each line carries the event's `changes` (as a dict) and timestamp `ts`.

Recovery: read lines until one fails to parse or is missing a trailing newline, then truncate the file to the last good byte. There are no commit records and no two-phase writes. Newline framing IS the transaction boundary, because every transaction is one event and every event is one line. Torn trailing writes are caught by the missing newline; corruption is caught by JSON parse failure. Same recovery guarantees as a flagged protocol, fewer moving parts.
