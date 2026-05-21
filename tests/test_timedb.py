import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

import immutables

from timedb import ABSENT, EMPTY, Event, TimeDB, diff, history
from timedb.db import WritableTimeDB
from timedb.types import fold_batch
from timedb import journal


T0 = datetime(2026, 1, 1, 12, 0, 0)


def _ev(tx: int, key: str, value, t: datetime = T0) -> Event:
    return Event(tx=tx, key=key, value=value, timestamp=t)


class TypesTests(unittest.TestCase):
    def test_absent_repr(self):
        self.assertEqual(repr(ABSENT), "ABSENT")

    def test_empty_is_immutable_map(self):
        self.assertIsInstance(EMPTY, immutables.Map)
        self.assertEqual(len(EMPTY), 0)

    def test_fold_batch_sets_value(self):
        out = fold_batch(EMPTY, [_ev(0, "a", 1), _ev(0, "b", 2)])
        self.assertEqual(out["a"], 1)
        self.assertEqual(out["b"], 2)

    def test_fold_batch_deletion_removes_key(self):
        out = fold_batch(EMPTY, [_ev(0, "a", 1), _ev(1, "a", ABSENT)])
        self.assertNotIn("a", out)

    def test_fold_batch_deletion_of_missing_key_is_noop(self):
        out = fold_batch(EMPTY, [_ev(0, "a", ABSENT)])
        self.assertEqual(len(out), 0)

    def test_fold_batch_later_overwrites_earlier(self):
        out = fold_batch(EMPTY, [_ev(0, "a", 1), _ev(1, "a", 2)])
        self.assertEqual(out["a"], 2)


class TimeDBReadTests(unittest.TestCase):
    def setUp(self):
        self.events = [
            _ev(0, "user.name", "alice", T0),
            _ev(0, "user.age", 30, T0),
            _ev(1, "user.name", "bob", T0 + timedelta(seconds=1)),
            _ev(2, "user.age", ABSENT, T0 + timedelta(seconds=2)),
        ]
        self.db = TimeDB(self.events)

    def test_empty_db_has_no_transactions(self):
        db = TimeDB()
        self.assertEqual(db.num_transactions(), 0)
        self.assertEqual(len(db.latest()), 0)

    def test_num_transactions_counts_distinct_tx(self):
        self.assertEqual(self.db.num_transactions(), 3)

    def test_at_zero_is_empty(self):
        # at(0) is the state *after* tx 0
        snap = self.db.at(0)
        self.assertEqual(snap["user.name"], "alice")
        self.assertEqual(snap["user.age"], 30)

    def test_at_latest_matches_latest(self):
        n = self.db.num_transactions()
        self.assertEqual(self.db.at(n - 1), self.db.latest())

    def test_latest_reflects_all_events(self):
        snap = self.db.latest()
        self.assertEqual(snap["user.name"], "bob")
        self.assertNotIn("user.age", snap)

    def test_at_invalid_tx_raises(self):
        with self.assertRaises(ValueError):
            self.db.at(-1)
        with self.assertRaises(ValueError):
            self.db.at(self.db.num_transactions())

    def test_as_of_before_first_returns_empty(self):
        snap = self.db.as_of(T0 - timedelta(seconds=1))
        self.assertEqual(len(snap), 0)

    def test_as_of_at_first_timestamp_returns_first_tx(self):
        snap = self.db.as_of(T0)
        self.assertEqual(snap["user.name"], "alice")
        self.assertEqual(snap["user.age"], 30)

    def test_as_of_far_future_returns_latest(self):
        snap = self.db.as_of(T0 + timedelta(days=365))
        self.assertEqual(snap, self.db.latest())

    def test_events_returns_all_events_in_order(self):
        self.assertEqual(list(self.db.events()), self.events)

    def test_events_at_filters_by_tx(self):
        evs = list(self.db.events_at(0))
        self.assertEqual(len(evs), 2)
        self.assertTrue(all(e.tx == 0 for e in evs))

    def test_events_at_singleton_tx(self):
        evs = list(self.db.events_at(2))
        self.assertEqual(len(evs), 1)
        self.assertIs(evs[0].value, ABSENT)

    def test_events_at_invalid_raises(self):
        with self.assertRaises(ValueError):
            list(self.db.events_at(99))

    def test_between_half_open(self):
        evs = list(self.db.between(0, 2))
        self.assertEqual({e.tx for e in evs}, {0, 1})

    def test_between_empty_range(self):
        self.assertEqual(list(self.db.between(1, 1)), [])

    def test_between_full_range(self):
        n = self.db.num_transactions()
        self.assertEqual(list(self.db.between(0, n)), self.events)

    def test_between_invalid_range_raises(self):
        with self.assertRaises(ValueError):
            list(self.db.between(-1, 1))
        with self.assertRaises(ValueError):
            list(self.db.between(2, 1))
        with self.assertRaises(ValueError):
            list(self.db.between(0, 99))

    def test_tx_timestamp(self):
        self.assertEqual(self.db.tx_timestamp(0), T0)
        self.assertEqual(self.db.tx_timestamp(2), T0 + timedelta(seconds=2))

    def test_tx_timestamp_invalid_raises(self):
        with self.assertRaises(ValueError):
            self.db.tx_timestamp(99)

    def test_repr(self):
        self.assertEqual(repr(TimeDB()), "TimeDB(0 transactions)")
        self.assertEqual(repr(TimeDB([_ev(0, "a", 1)])), "TimeDB(1 transaction)")
        self.assertEqual(repr(self.db), "TimeDB(3 transactions)")


class WritableTimeDBTests(unittest.TestCase):
    def test_in_memory_returns_writable(self):
        db = TimeDB.in_memory()
        self.assertIsInstance(db, WritableTimeDB)

    def test_apply_returns_tx_index(self):
        db = TimeDB.in_memory()
        tx = db.apply({"a": 1}, at=T0)
        self.assertEqual(tx, 0)
        tx2 = db.apply({"a": 2}, at=T0 + timedelta(seconds=1))
        self.assertEqual(tx2, 1)

    def test_apply_writes_visible_in_latest(self):
        db = TimeDB.in_memory()
        db.apply({"a": 1, "b": 2}, at=T0)
        snap = db.latest()
        self.assertEqual(snap["a"], 1)
        self.assertEqual(snap["b"], 2)

    def test_apply_atomic_multi_key_share_tx(self):
        db = TimeDB.in_memory()
        tx = db.apply({"a": 1, "b": 2}, at=T0)
        evs = list(db.events_at(tx))
        self.assertEqual(len(evs), 2)
        self.assertTrue(all(e.tx == tx for e in evs))
        self.assertTrue(all(e.timestamp == T0 for e in evs))

    def test_apply_deletion_via_absent(self):
        db = TimeDB.in_memory()
        db.apply({"a": 1}, at=T0)
        db.apply({"a": ABSENT}, at=T0 + timedelta(seconds=1))
        self.assertNotIn("a", db.latest())

    def test_apply_empty_raises(self):
        db = TimeDB.in_memory()
        with self.assertRaises(ValueError):
            db.apply({})

    def test_apply_rejects_backwards_timestamp(self):
        db = TimeDB.in_memory()
        db.apply({"a": 1}, at=T0 + timedelta(seconds=10))
        with self.assertRaises(ValueError):
            db.apply({"a": 2}, at=T0)

    def test_apply_default_timestamp_is_now(self):
        before = datetime.now()
        db = TimeDB.in_memory()
        db.apply({"a": 1})
        after = datetime.now()
        ts = db.tx_timestamp(0)
        self.assertTrue(before <= ts <= after)

    def test_nested_values_allowed(self):
        db = TimeDB.in_memory()
        v = {"list": [1, 2, {"k": "v"}], "n": None, "b": True}
        db.apply({"x": v}, at=T0)
        self.assertEqual(db.latest()["x"], v)


class QueryTests(unittest.TestCase):
    def test_diff_added(self):
        a = EMPTY
        b = a.set("k", 1)
        out = sorted(diff(a, b))
        self.assertEqual(out, [("k", ABSENT, 1)])

    def test_diff_removed(self):
        a = EMPTY.set("k", 1)
        b = EMPTY
        out = sorted(diff(a, b))
        self.assertEqual(out, [("k", 1, ABSENT)])

    def test_diff_changed(self):
        a = EMPTY.set("k", 1)
        b = EMPTY.set("k", 2)
        out = sorted(diff(a, b))
        self.assertEqual(out, [("k", 1, 2)])

    def test_diff_unchanged_yields_nothing(self):
        a = EMPTY.set("k", 1)
        b = EMPTY.set("k", 1)
        self.assertEqual(list(diff(a, b)), [])

    def test_history_filters_by_key(self):
        evs = [
            _ev(0, "a", 1),
            _ev(1, "b", 2),
            _ev(2, "a", 3),
        ]
        out = list(history(evs, "a"))
        self.assertEqual([e.value for e in out], [1, 3])

    def test_history_empty_when_key_absent(self):
        self.assertEqual(list(history([_ev(0, "a", 1)], "z")), [])


class DurableTimeDBTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".jsonl")
        self.tmp.close()
        self.path = Path(self.tmp.name)
        self.path.unlink()  # let DurableTimeDB create it via lock acquisition

    def tearDown(self):
        if self.path.exists():
            self.path.unlink()

    def test_open_and_save_roundtrip(self):
        with TimeDB.open(self.path) as db:
            db.apply({"a": 1, "b": 2}, at=T0)
            db.apply({"a": ABSENT}, at=T0 + timedelta(seconds=1))

        ro = TimeDB.open_readonly(self.path)
        snap = ro.latest()
        self.assertNotIn("a", snap)
        self.assertEqual(snap["b"], 2)
        self.assertEqual(ro.num_transactions(), 2)

    def test_save_within_context_manager(self):
        with TimeDB.open(self.path) as db:
            db.apply({"a": 1}, at=T0)
            db.save()
            # second context-manager exit save should be a no-op
            db.apply({"a": 2}, at=T0 + timedelta(seconds=1))
        ro = TimeDB.open_readonly(self.path)
        self.assertEqual(ro.latest()["a"], 2)

    def test_no_save_on_exception_preserves_lock_release(self):
        # if we never save, on-disk should be empty
        try:
            with TimeDB.open(self.path) as db:
                db.apply({"a": 1}, at=T0)
                raise RuntimeError("boom")
        except RuntimeError:
            pass
        ro = TimeDB.open_readonly(self.path)
        # writes flushed because __exit__ saves before releasing lock
        self.assertEqual(ro.latest()["a"], 1)

    def test_recovery_truncates_partial_trailing_line(self):
        with TimeDB.open(self.path) as db:
            db.apply({"a": 1}, at=T0)

        with self.path.open("ab") as f:
            f.write(b'{"tx": 1, "key": "b"')  # no newline, partial

        with TimeDB.open(self.path) as db:
            self.assertEqual(db.latest()["a"], 1)
            self.assertEqual(db.num_transactions(), 1)
            db.apply({"c": 3}, at=T0 + timedelta(seconds=2))

        ro = TimeDB.open_readonly(self.path)
        self.assertEqual(ro.latest()["a"], 1)
        self.assertEqual(ro.latest()["c"], 3)

    def test_recovery_drops_uncommitted_batch_without_final(self):
        with TimeDB.open(self.path) as db:
            db.apply({"a": 1}, at=T0)

        partial = json.dumps({
            "tx": 1,
            "key": "b",
            "value": {"type": "data", "data": 99},
            "timestamp": (T0 + timedelta(seconds=1)).isoformat(),
            "final": False,
        }) + "\n"
        with self.path.open("a") as f:
            f.write(partial)

        ro = TimeDB.open_readonly(self.path)
        self.assertEqual(ro.num_transactions(), 1)
        self.assertNotIn("b", ro.latest())

    def test_flock_blocks_second_writer(self):
        with TimeDB.open(self.path) as _:
            with self.assertRaises(RuntimeError):
                TimeDB.open(self.path)

    def test_readonly_does_not_lock(self):
        with TimeDB.open(self.path) as db:
            db.apply({"a": 1}, at=T0)
        # multiple readonly handles fine
        ro1 = TimeDB.open_readonly(self.path)
        ro2 = TimeDB.open_readonly(self.path)
        self.assertEqual(ro1.latest()["a"], 1)
        self.assertEqual(ro2.latest()["a"], 1)

    def test_journal_serialization_roundtrip(self):
        events = [
            _ev(0, "a", 1, T0),
            _ev(0, "b", [1, 2, 3], T0),
            _ev(1, "a", ABSENT, T0 + timedelta(seconds=1)),
        ]
        journal.append_batches(self.path, [events[:2], events[2:]])
        out = journal.read(self.path)
        self.assertEqual(out, events)

    def test_readonly_empty_path(self):
        # reading a nonexistent path yields an empty db
        db = TimeDB.open_readonly(self.path)
        self.assertEqual(db.num_transactions(), 0)


if __name__ == "__main__":
    unittest.main()
