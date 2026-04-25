# TimeDB

An append-only, in-memory key-value store for Python that retains full history. Every write is recorded as an immutable event. Past states are queryable by sequence number. Nothing is ever overwritten.

---

## How it works

All writes go into a log as `Event` objects with a sequence number, key, value, timestamp, and optional transaction ID. The current value of any key is derived by finding the most recent event for that key at or before a given sequence number. Deletion is a write of the sentinel value `ABSENT`.

A `Snapshot` is a view of the log frozen at a specific sequence number. It resolves values on demand rather than copying them, so creating a snapshot is cheap regardless of log size.

---

## Installation

No dependencies beyond the Python standard library. Copy the package directory into your project.

---

## Usage

### Basic reads and writes

```python
from timedb import TimeDB

db = TimeDB()

db.put("user.name", "alice")
db.put("user.name", "bob")

snap = db.latest()
print(snap.get("user.name"))  # "bob"
```

### Time travel

```python
snap0 = db.at(0)
print(snap0.get("user.name"))  # "alice"
```

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

All keys in a single `apply` call share the same transaction ID and timestamp.

### Diffing snapshots

```python
a = db.at(0)
b = db.latest()

changes = a.diff(b)
# {"user.name": ("alice", ABSENT), "order.id": (ABSENT, 42), ...}
```

`diff` only examines keys that appear in events between the two sequence numbers, so it does not scan the full key set.

### Iterating

```python
for key, value in db.latest().items():
    print(key, value)

for event in db.history("user.name"):
    print(event.seq, event.value)
```

### Persistence

```python
db.save("db.jsonl")
db2 = TimeDB.load("db.jsonl")
```

The file format is newline-delimited JSON. Each line is one event. `load` verifies that sequence numbers are contiguous; `load_unchecked` skips that check.

---

## Values

Accepted value types are `str`, `int`, `float`, `bool`, `None`, and arbitrarily nested `list` and `dict` combinations with string keys. Any other type raises `TypeError` at write time.

---

## Sequence numbers

Sequence numbers start at 0 and increment by one per event. `db.at(seq)` raises `ValueError` if `seq` is out of range. `db.latest()` returns a snapshot at the last written event, or a snapshot at seq -1 if the log is empty (all keys resolve to `ABSENT`).

---

## Transactions

`apply` and `apply_at` write multiple keys atomically in the sense that all events share the same `tx_id`, which equals the sequence number of the first event in the group. To retrieve all events in a transaction:

```python
for event in db.transaction_events(tx_id):
    print(event.key, event.value)
```

Single-key writes via `put` and `delete` have `tx_id = None`.

---

## Limitations

- No persistence layer beyond manual `save`/`load`.
- No compaction. The log grows without bound.
- Not thread-safe.
- No indexing beyond per-key sequence lists.
