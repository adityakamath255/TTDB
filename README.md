# TimeDB

An append-only key-value store for Python that retains full history. Every write is recorded as an immutable event. Past states are queryable by transaction index or timestamp. Nothing is ever overwritten.

---

## How it works

All writes go into a log as `Event` objects with a transaction id, key, value, and timestamp. Append-only is the core invariant; everything else derives from it. Deletion is a write of the sentinel value `ABSENT`, which also represents "never existed" — one sentinel covers both cases. If you need to distinguish them, walk the event history.

A *transaction* is a group of one or more events with the same `tx` and timestamp, produced by a single `apply` call. Transactions are numbered `0, 1, 2, …` in commit order.

State views are plain `immutables.Map` instances (HAMTs). Lookups are O(log n) and creating a snapshot is just an indexed access into the cached root chain — there's a root per transaction, kept in memory, structurally shared.

The database itself comes in three flavors arranged as a capability ladder:

- `TimeDB` — read-only. Returned by `TimeDB.open_readonly(path)` or constructed directly from an iterable of events. No writes, no persistence side effects.
- `WritableTimeDB` — adds in-memory writes via `apply`. Returned by `TimeDB.in_memory()`.
- `DurableTimeDB` — adds journal persistence and a process-wide write lock. Returned by `TimeDB.open(path)`.

Type checkers enforce the ladder. Calling `.apply()` on a `TimeDB.open_readonly` handle is a type error, not a runtime error, because the method doesn't exist on the read-only class.

---

## Installation

Depends on `immutables` (HAMT). Copy the `timedb` package directory into your project.

---

## Usage

### In-memory

```python
from timedb import TimeDB

db = TimeDB.in_memory()

db.apply({"user.name": "alice"})
db.apply({"user.name": "bob"})

print(db.latest()["user.name"])  # "bob"
```

### Persistent

```python
with TimeDB.open(path) as db:
    db.apply({"user.name": "alice"})
```

`path` is the journal file itself, not a directory. Writes accumulate in memory; `db.save()` appends new events to the journal with fsync. Context-manager exit calls `save` automatically. Crash before save loses the unsaved tail; recovery on next open scans for the last complete transaction (marked by a `"final": true` event) and truncates any partial trailing bytes.

Opening also acquires an exclusive `flock` on the journal file. A second process trying to open the same path raises `RuntimeError`. The lock is released on context-manager exit, or by the kernel if the process crashes.

### Read-only

```python
ro = TimeDB.open_readonly(path)
print(ro.latest().get("user.name"))
```

Returns a `TimeDB` (the read-only base class). No lock acquired, no writes available. Multiple read-only handles can coexist.

### Time travel

```python
snap = db.at(0)
print(snap["user.name"])  # state after the first transaction
```

`at(tx)` is the state after applying transaction `tx`. `db.latest()` is shorthand for the state after the most recent transaction. Transaction indices start at zero.

You can also travel by timestamp:

```python
snap = db.as_of(some_datetime)
print(snap.get("user.name"))
```

`as_of(t)` returns the state reflecting every transaction with timestamp ≤ t. Lookup is O(log n) via binary search, which assumes timestamps are non-decreasing across transactions. `apply` enforces this at write time: a timestamp earlier than the last event raises `ValueError`.

### Atomic writes

```python
tx = db.apply({
    "order.id": 42,
    "order.status": "pending",
})
```

All keys in a single `apply` call share the same transaction index and timestamp. The return value is the index of the committed transaction. An empty dict raises `ValueError`.

Deletion is a write of `ABSENT`:

```python
from timedb import ABSENT

db.apply({"user.name": ABSENT})
print(db.latest().get("user.name"))  # None (key absent)
```

### Diffing snapshots

```python
from timedb import diff

a = db.at(0)
b = db.latest()

for key, lo, hi in diff(a, b):
    print(key, lo, hi)
```

`diff(a, b)` yields `(key, value_in_a, value_in_b)` for every key where the two snapshots disagree. `ABSENT` represents "not present in this snapshot."

### Iterating events

```python
for event in db.events():
    print(event.tx, event.key, event.value)

for event in db.between(0, 3):
    print(event.tx, event.key, event.value)

for event in db.events_at(tx):
    print(event.key, event.value)
```

- `db.events()` returns every event in commit order.
- `db.between(tx_lo, tx_hi)` returns events from transactions in the half-open range `[tx_lo, tx_hi)`.
- `db.events_at(tx)` returns just the events from one transaction.

### History of a key

```python
from timedb import history

for event in history(db.events(), "user.name"):
    print(event.tx, event.value, event.timestamp)
```

`history(events, key)` filters any event iterable down to the events for a single key. Compose with `db.between(...)` to scope to a window.

### Other accessors

- `db.num_transactions()` — count of committed transactions.
- `db.tx_timestamp(tx)` — timestamp of transaction `tx`.

---

## Values

Accepted value types are `str`, `int`, `float`, `bool`, `None`, and arbitrarily nested `list` and `dict` combinations with string keys. The recursive type alias `Value` in `timedb.types` encodes this exactly. Validation happens at type-check time, not runtime — pass anything other than a `Value` and pyright or mypy will reject the call.

The constraint comes from the on-disk log being JSONL: it should stay readable from any language.

---
