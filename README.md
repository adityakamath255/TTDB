# TimeDB

An append-only key-value store for Python that retains full history. Every write is recorded as an immutable event. Past states are queryable by sequence number. Nothing is ever overwritten.

---

## How it works

All writes go into a log as `Event` objects with a sequence number, key, value, timestamp, and optional transaction ID. The current value of any key is read from a HAMT root snapshot at that sequence number. Deletion is a write of the sentinel value `ABSENT`.

A `Snapshot` is a view of the log frozen at a specific sequence number. Lookups go straight through the persistent root, so creating a snapshot is cheap regardless of log size.

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
with TimeDB.open(path, save_events=1000, save_seconds=5.0,
                 segment_bytes=64 * 1024 * 1024) as db:
    db.put("user.name", "alice")
```

A background thread writes events to segment files in `path/`. Writes are durable up to `save_events` events / `save_seconds` seconds behind. Call `db.flush()` to block until persisted.

### Time travel

```python
snap0 = db.at(0)
print(snap0.get("user.name"))  # "alice"
```

`db.at(-1)` is a snapshot before any events; everything resolves to `ABSENT`. `db.latest()` is shorthand for the snapshot at the last written event (or seq -1 if empty).

### Deletion

```python
db.delete("user.name")
print(db.latest().get("user.name"))  # ABSENT
```

### Atomic writes

```python
first_seq, last_seq = db.apply({
    "order.id": 42,
    "order.status": "pending",
})
```

All keys in a single `apply` call share the same transaction ID and timestamp. `apply` raises `ValueError` on an empty dict.

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

`between(seq0, seq1)` yields events `(seq0, seq1]` (exclusive at the low end so it pairs naturally with `db.at(seq0)` / `db.at(seq1)`).

### Transactions

`apply` writes multiple keys atomically: all events share a `tx_id` equal to the seq of the first event in the group.

```python
for event in db.tx_events(tx_id):
    print(event.key, event.value)
```

Single-key writes via `put` and `delete` have `tx_id = None`.

---

## Values

Accepted value types are `str`, `int`, `float`, `bool`, `None`, and arbitrarily nested `list` and `dict` combinations with string keys. Any other type raises `TypeError` at write time.

---

## Limitations

- Memory grows forever (no compaction).
- Single-process. No lockfile.
- No fsync. Segment writes go to OS page cache only.
- Single writer at a time. Snapshot reads are concurrent-safe via HAMT.
