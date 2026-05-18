# TimeDB

An append-only key-value store for Python that retains full history. Every write is recorded as an immutable event. Past states are queryable by sequence number or timestamp. Nothing is ever overwritten.

---

## How it works

All writes go into a log as `Event` objects with a sequence number, key, value, timestamp, and optional transaction ID. Append-only is the core invariant; everything else derives from it. Deletion is a write of the sentinel value `ABSENT`, which also represents "never existed" — one sentinel covers both cases. If you need to distinguish them, walk the event history.

The log is queried through two view types:

- `Snapshot` — the value-view at a single seq. `get`, `keys`, `items`, `diff`. Lookups go through a persistent HAMT root, so creating a snapshot is cheap regardless of log size. Snapshots are frozen values: `seq`, `state`, and `timestamp` are attributes.
- `Interval` — the event-view across a seq window. `history`, `keys_touched`, `tx_ids`, `count`, and iteration over events. Holds bounds plus a log reference; creating one is cheap.

The split mirrors the question being asked: "what is the world at this moment?" goes through Snapshot; "what happened in this window?" goes through Interval.

The database itself comes in three flavors arranged as a capability ladder:

- `TimeDB` — read-only. Returned by `TimeDB.open_readonly(path)`. No writes, no persistence side effects.
- `WritableTimeDB` — adds in-memory writes. Returned by `TimeDB.in_memory()`.
- `DurableTimeDB` — adds journal persistence and a process-wide write lock. Returned by `TimeDB.open(path)`.

Type checkers enforce the ladder. Calling `.put()` on a `TimeDB.open_readonly` handle is a type error, not a runtime error, because the method doesn't exist on the read-only class.

---

## Installation

Depends on `immutables` (HAMT). Copy the `timedb` package directory into your project.

---

## Usage

### In-memory

```python
from timedb import TimeDB

db = TimeDB.in_memory()

db.put("user.name", "alice")
db.put("user.name", "bob")

snap = db.latest()
print(snap.get("user.name"))  # "bob"
```

### Persistent

```python
with TimeDB.open(path) as db:
    db.put("user.name", "alice")
```

`path` is the journal file itself, not a directory. Writes accumulate in memory; `db.save()` appends new events to the journal with fsync. Context-manager exit calls `save` automatically. Crash before save loses the unsaved tail; recovery on next open scans for the last complete JSON line and truncates any partial trailing bytes.

Opening also acquires an exclusive `flock` on the journal file itself. A second process trying to open the same path raises `RuntimeError`. The lock is released on context-manager exit, or by the kernel if the process crashes.

### Read-only

```python
ro = TimeDB.open_readonly(path)
print(ro.latest().get("user.name"))
```

Returns a `TimeDB` (the read-only base class). No lock acquired, no writes available. Multiple read-only handles can coexist.

### Time travel

```python
snap1 = db.at(1)
print(snap1.get("user.name"))  # "alice"
```

A Snapshot's seq is a *position* in the log: `at(0)` is the empty state before any events; `at(k)` is the state after the first `k` events have been applied; `at(n)` is the latest. `db.latest()` is shorthand for the snapshot at the current event count.

You can also travel by timestamp:

```python
snap = db.as_of(some_datetime)
print(snap.get("user.name"))
```

`as_of(t)` returns the snapshot reflecting every event with timestamp ≤ t. Lookup is O(log n) via binary search, which assumes timestamps are written in non-decreasing order. Out-of-order writes still succeed (use `strict_time=True` at open to reject them), but `as_of` results are then undefined. Use `at(seq)` if you need correctness without that constraint.

### Deletion

```python
db.delete("user.name")
print(db.latest().get("user.name"))  # ABSENT
```

`delete(key, at=None)` is shorthand for `put(key, ABSENT, at)`.

### Atomic writes

```python
start, end = db.apply({
    "order.id": 42,
    "order.status": "pending",
})
```

All keys in a single `apply` call share the same transaction ID and timestamp. The return value is the half-open seq range `[start, end)` covering the events written, so `db.between(start, end)` gives you back this transaction. An empty dict is a no-op and returns `(count, count)`.

### Diffing snapshots

```python
a = db.at(0)
b = db.latest()

for key, lo, hi in a.diff(b):
    print(key, lo, hi)
```

`diff` yields `(key, value_in_a, value_in_b)` for every key where the two snapshots disagree. `ABSENT` represents "not present in this snapshot."

### Iterating

```python
for key, value in db.latest().items():
    print(key, value)

for event in db.between(seq0, seq1):
    print(event.seq, event.key, event.value)
```

`between(seq0, seq1)` returns an `Interval`: a view over events in `[seq0, seq1)` (Python slice semantics). It pairs naturally with snapshots: `db.at(seq0)` is the state before the interval's events and `db.at(seq1)` is the state after. Iterating an Interval yields its events. It also exposes:

- `iv.history(key)` — events whose key matches, in seq order
- `iv.keys_touched()` — set of distinct keys with at least one event in the interval
- `iv.tx_ids()` — set of distinct tx_ids in the interval (singleton writes excluded)
- `iv.count()` — number of events in the interval

### History of a key

```python
for event in db.history("user.name"):
    print(event.seq, event.value, event.timestamp)
```

`db.history(key)` is shorthand for the full-log case: `db.between(0, n).history(key)`.

### Transactions

`apply` writes multiple keys atomically: all events share a `tx_id` equal to the seq of the first event in the group.

```python
for event in db.tx_events(tx_id):
    print(event.key, event.value)
```

Single-key writes via `put` and `delete` have `tx_id = None`.

---

## Values

Accepted value types are `str`, `int`, `float`, `bool`, `None`, and arbitrarily nested `list` and `dict` combinations with string keys. The recursive type alias `OptValue` in `timedb.types` encodes this exactly. Validation happens at type-check time, not runtime — pass anything other than an `OptValue` and pyright or mypy will reject the call.

The constraint comes from the on-disk log being JSONL: it should stay readable from any language.

---
