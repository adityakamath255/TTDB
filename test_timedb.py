import json
from datetime import datetime, timedelta

import immutables
import pytest

from timedb import (
    ABSENT,
    DB,
    Event,
    WritableDB,
    _deserialize_event,
    _fold_event,
    _serialize_event,
    diff,
    empty,
    history,
    view,
)
from timedb import open as db_open


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_event(seq, changes, t=None):
    """Build an Event with a deterministic default timestamp."""
    return Event(
        seq=seq,
        changes=changes,
        timestamp=t or datetime(2024, 1, 1, 12, 0, 0) + timedelta(seconds=seq),
    )


# ---------------------------------------------------------------------------
# _fold_event
# ---------------------------------------------------------------------------

class TestFoldEvent:
    def test_sets_keys(self):
        root = immutables.Map()
        result = _fold_event(root, make_event(0, {"a": 1, "b": "two"}))
        assert result["a"] == 1
        assert result["b"] == "two"

    def test_overwrites_existing_keys(self):
        root = immutables.Map({"a": 1})
        assert _fold_event(root, make_event(0, {"a": 99}))["a"] == 99

    def test_deletes_with_absent(self):
        root = immutables.Map({"a": 1, "b": 2})
        result = _fold_event(root, make_event(0, {"a": ABSENT}))
        assert "a" not in result
        assert result["b"] == 2

    def test_absent_on_missing_key_is_noop(self):
        root = immutables.Map({"a": 1})
        result = _fold_event(root, make_event(0, {"b": ABSENT}))
        assert dict(result) == {"a": 1}

    def test_does_not_mutate_input(self):
        root = immutables.Map({"a": 1})
        _fold_event(root, make_event(0, {"a": 2, "b": 3}))
        # immutables.Map is persistent, so root must be untouched
        assert dict(root) == {"a": 1}

    def test_mixed_set_and_delete(self):
        root = immutables.Map({"a": 1, "b": 2})
        result = _fold_event(root, make_event(0, {"a": ABSENT, "c": 3}))
        assert dict(result) == {"b": 2, "c": 3}


# ---------------------------------------------------------------------------
# Event serialization round-trip
# ---------------------------------------------------------------------------

class TestSerialization:
    @pytest.mark.parametrize("value", [
        "hello",
        "",
        "unicode: \u4e16\u754c \U0001f30d",
        42,
        0,
        -17,
        3.14,
        True,
        False,
        None,
    ])
    def test_value_round_trip(self, value):
        e = make_event(0, {"k": value})
        restored = _deserialize_event(_serialize_event(e))
        assert restored.changes["k"] == value

    def test_absent_round_trip(self):
        e = make_event(0, {"deleted": ABSENT})
        restored = _deserialize_event(_serialize_event(e))
        assert restored.changes["deleted"] is ABSENT

    def test_event_round_trip_preserves_all_fields(self):
        t = datetime(2024, 6, 15, 10, 30, 45, 123456)
        e = Event(seq=7, changes={"a": 1, "b": ABSENT}, timestamp=t)
        restored = _deserialize_event(_serialize_event(e))
        assert restored.seq == 7
        assert restored.timestamp == t
        assert restored.changes["a"] == 1
        assert restored.changes["b"] is ABSENT

    def test_serialized_form_is_json(self):
        e = make_event(0, {"k": "v", "deleted": ABSENT})
        text = json.dumps(_serialize_event(e))
        restored = _deserialize_event(json.loads(text))
        assert restored.changes["k"] == "v"
        assert restored.changes["deleted"] is ABSENT

    def test_empty_changes_round_trip(self):
        e = make_event(0, {})
        restored = _deserialize_event(_serialize_event(e))
        assert restored.changes == {}


# ---------------------------------------------------------------------------
# diff
# ---------------------------------------------------------------------------

class TestDiff:
    def test_identical_maps(self):
        m = immutables.Map({"a": 1, "b": 2})
        assert list(diff(m, m)) == []

    def test_empty_maps(self):
        assert list(diff(immutables.Map(), immutables.Map())) == []

    def test_added_key(self):
        a = immutables.Map()
        b = immutables.Map({"x": 1})
        assert list(diff(a, b)) == [("x", ABSENT, 1)]

    def test_removed_key(self):
        a = immutables.Map({"x": 1})
        b = immutables.Map()
        assert list(diff(a, b)) == [("x", 1, ABSENT)]

    def test_changed_value(self):
        a = immutables.Map({"x": 1})
        b = immutables.Map({"x": 2})
        assert list(diff(a, b)) == [("x", 1, 2)]

    def test_multiple_changes(self):
        a = immutables.Map({"keep": "same", "change": 1, "remove": "bye"})
        b = immutables.Map({"keep": "same", "change": 99, "add": "hi"})
        result = set(diff(a, b))
        assert result == {
            ("change", 1, 99),
            ("remove", "bye", ABSENT),
            ("add", ABSENT, "hi"),
        }

    def test_none_values_are_distinct_from_absent(self):
        # A key present with value None is NOT the same as absent
        a = immutables.Map({"x": None})
        b = immutables.Map()
        assert list(diff(a, b)) == [("x", None, ABSENT)]


# ---------------------------------------------------------------------------
# history
# ---------------------------------------------------------------------------

class TestHistory:
    def test_returns_only_relevant_events(self):
        events = [
            make_event(0, {"a": 1}),
            make_event(1, {"b": 2}),
            make_event(2, {"a": 99}),
            make_event(3, {"c": 3}),
        ]
        result = list(history(events, "a"))
        assert [r[0] for r in result] == [0, 2]
        assert [r[1] for r in result] == [1, 99]

    def test_includes_deletions(self):
        events = [
            make_event(0, {"a": 1}),
            make_event(1, {"a": ABSENT}),
        ]
        result = list(history(events, "a"))
        assert result[0][1] == 1
        assert result[1][1] is ABSENT

    def test_includes_timestamps(self):
        t0 = datetime(2024, 1, 1)
        t1 = datetime(2024, 1, 2)
        events = [
            make_event(0, {"a": 1}, t=t0),
            make_event(1, {"a": 2}, t=t1),
        ]
        result = list(history(events, "a"))
        assert result[0][2] == t0
        assert result[1][2] == t1

    def test_missing_key_yields_empty(self):
        events = [make_event(0, {"x": 1})]
        assert list(history(events, "nope")) == []

    def test_empty_events_yields_empty(self):
        assert list(history([], "anything")) == []


# ---------------------------------------------------------------------------
# DB (read-only time travel)
# ---------------------------------------------------------------------------

class TestDB:
    def test_empty_latest_is_empty_map(self):
        assert dict(DB().latest()) == {}

    def test_latest_after_events(self):
        db = DB([
            make_event(0, {"a": 1}),
            make_event(1, {"b": 2}),
            make_event(2, {"a": 99}),
        ])
        assert db.latest()["a"] == 99
        assert db.latest()["b"] == 2

    def test_latest_reflects_deletions(self):
        db = DB([
            make_event(0, {"a": 1}),
            make_event(1, {"a": ABSENT}),
        ])
        assert "a" not in db.latest()

    def test_at_semantics(self):
        """
        at(seq) returns the state AFTER event[seq] was applied. Symmetric with
        events()[seq]: same index space, both refer to the seq-th event.
        at(n - 1) equals latest(). The pre-state is not addressable via at.
        """
        db = DB([
            make_event(0, {"a": "v0"}),
            make_event(1, {"a": "v1"}),
        ])
        assert dict(db.at(0)) == {"a": "v0"}     # after e0
        assert dict(db.at(1)) == {"a": "v1"}     # after e1 (== latest)
        assert dict(db.latest()) == {"a": "v1"}

    def test_at_rejects_negative(self):
        db = DB([make_event(0, {"a": 1})])
        with pytest.raises(ValueError):
            db.at(-1)

    def test_at_rejects_out_of_range(self):
        db = DB([make_event(0, {"a": 1})])
        with pytest.raises(ValueError):
            db.at(1)

    def test_at_on_empty_db_always_rejects(self):
        with pytest.raises(ValueError):
            DB().at(0)

    def test_events_returns_log(self):
        events = [make_event(0, {"a": 1}), make_event(1, {"b": 2})]
        assert list(DB(events).events()) == events

    def test_as_of_before_first_event(self):
        db = DB([make_event(0, {"a": 1}, t=datetime(2024, 1, 1, 12, 0, 0))])
        assert dict(db.as_of(datetime(2024, 1, 1, 11, 0, 0))) == {}

    def test_as_of_exactly_at_timestamp_includes_event(self):
        # bisect_right places equal values to the right, so as_of(t) is inclusive
        t = datetime(2024, 1, 1, 12, 0, 0)
        db = DB([make_event(0, {"a": 1}, t=t)])
        assert dict(db.as_of(t)) == {"a": 1}

    def test_as_of_after_all_events(self):
        db = DB([
            make_event(0, {"a": 1}, t=datetime(2024, 1, 1)),
            make_event(1, {"b": 2}, t=datetime(2024, 1, 2)),
        ])
        assert dict(db.as_of(datetime(2025, 1, 1))) == {"a": 1, "b": 2}

    def test_as_of_between_events(self):
        db = DB([
            make_event(0, {"a": 1}, t=datetime(2024, 1, 1)),
            make_event(1, {"b": 2}, t=datetime(2024, 1, 3)),
        ])
        assert dict(db.as_of(datetime(2024, 1, 2))) == {"a": 1}

    def test_as_of_on_empty_db(self):
        assert dict(DB().as_of(datetime.now())) == {}


# ---------------------------------------------------------------------------
# WritableDB / empty()
# ---------------------------------------------------------------------------

class TestWritableDB:
    def test_empty_factory(self):
        db = empty()
        assert isinstance(db, WritableDB)
        assert dict(db.latest()) == {}

    def test_apply_returns_event_with_seq(self):
        db = empty()
        e0 = db.apply({"a": 1})
        e1 = db.apply({"b": 2})
        e2 = db.apply({"c": 3})
        assert isinstance(e0, Event)
        assert (e0.seq, e1.seq, e2.seq) == (0, 1, 2)

    def test_apply_returns_event_with_assigned_timestamp(self):
        db = empty()
        before = datetime.now()
        event = db.apply({"a": 1})
        after = datetime.now()
        assert before <= event.timestamp <= after

    def test_apply_updates_latest(self):
        db = empty()
        db.apply({"a": 1})
        assert db.latest()["a"] == 1

    def test_apply_with_explicit_timestamp(self):
        db = empty()
        t = datetime(2024, 6, 1)
        db.apply({"a": 1}, at=t)
        assert db.events()[0].timestamp == t

    def test_apply_default_timestamp_is_now(self):
        db = empty()
        before = datetime.now()
        db.apply({"a": 1})
        after = datetime.now()
        assert before <= db.events()[0].timestamp <= after

    def test_apply_rejects_timestamp_in_past(self):
        db = empty()
        db.apply({"a": 1}, at=datetime(2024, 6, 1))
        with pytest.raises(ValueError):
            db.apply({"b": 2}, at=datetime(2024, 5, 1))

    def test_apply_allows_equal_timestamp(self):
        db = empty()
        t = datetime(2024, 6, 1)
        db.apply({"a": 1}, at=t)
        db.apply({"b": 2}, at=t)  # equal is allowed; only strictly-earlier is rejected
        assert db.latest()["b"] == 2

    def test_deletion_via_apply(self):
        db = empty()
        db.apply({"a": 1})
        db.apply({"a": ABSENT})
        assert "a" not in db.latest()

    def test_apply_empty_changes(self):
        db = empty()
        event = db.apply({})
        assert event.seq == 0
        assert dict(db.latest()) == {}
        assert len(db.events()) == 1

    def test_events_grows_with_apply(self):
        db = empty()
        assert len(db.events()) == 0
        db.apply({"a": 1})
        assert len(db.events()) == 1
        db.apply({"b": 2})
        assert len(db.events()) == 2


# ---------------------------------------------------------------------------
# DurableDB persistence
# ---------------------------------------------------------------------------

class TestDurableDB:
    def test_apply_does_not_persist_until_save(self, tmp_path):
        log = tmp_path / "db.log"
        with db_open(log) as db:
            db.apply({"a": 1})
            assert log.read_text() == ""  # nothing flushed yet
            db.save()
            assert log.read_text() != ""

    def test_context_manager_saves_on_exit(self, tmp_path):
        log = tmp_path / "db.log"
        with db_open(log) as db:
            db.apply({"a": 1})
        # After context exit, data should be persisted
        with db_open(log) as db2:
            assert db2.latest()["a"] == 1

    def test_reopen_loads_state(self, tmp_path):
        log = tmp_path / "db.log"
        with db_open(log) as db:
            db.apply({"x": 1})
            db.apply({"y": 2})
            db.apply({"x": ABSENT})
        with db_open(log) as db:
            assert "x" not in db.latest()
            assert db.latest()["y"] == 2

    def test_multiple_save_calls(self, tmp_path):
        log = tmp_path / "db.log"
        with db_open(log) as db:
            db.apply({"a": 1})
            db.save()
            db.apply({"b": 2})
            db.save()
            db.apply({"c": 3})
            # implicit save on context exit
        with db_open(log) as db:
            assert dict(db.latest()) == {"a": 1, "b": 2, "c": 3}

    def test_save_with_no_pending_is_noop(self, tmp_path):
        log = tmp_path / "db.log"
        with db_open(log) as db:
            db.apply({"a": 1})
        size_after_first = log.stat().st_size
        with db_open(log) as db:
            db.save()  # nothing pending
        assert log.stat().st_size == size_after_first

    def test_seq_continues_across_reopens(self, tmp_path):
        log = tmp_path / "db.log"
        with db_open(log) as db:
            assert db.apply({"a": 1}).seq == 0
            assert db.apply({"b": 2}).seq == 1
        with db_open(log) as db:
            assert db.apply({"c": 3}).seq == 2

    def test_log_format_is_one_json_per_line(self, tmp_path):
        log = tmp_path / "db.log"
        with db_open(log) as db:
            db.apply({"a": 1})
            db.apply({"b": 2})
        lines = log.read_text().strip().split("\n")
        assert len(lines) == 2
        for line in lines:
            json.loads(line)  # each line is valid JSON

    def test_timestamps_persisted_correctly(self, tmp_path):
        log = tmp_path / "db.log"
        t1 = datetime(2024, 6, 1, 10, 0, 0)
        t2 = datetime(2024, 6, 1, 11, 0, 0)
        with db_open(log) as db:
            db.apply({"a": 1}, at=t1)
            db.apply({"b": 2}, at=t2)
        with db_open(log) as db:
            events = list(db.events())
            assert events[0].timestamp == t1
            assert events[1].timestamp == t2


# ---------------------------------------------------------------------------
# Crash recovery
# ---------------------------------------------------------------------------

class TestCrashRecovery:
    def test_recovers_from_torn_write_no_newline(self, tmp_path):
        log = tmp_path / "db.log"
        with db_open(log) as db:
            db.apply({"a": 1})
        # Simulate a crash mid-write: append a JSON line without trailing newline
        torn = json.dumps({
            "seq": 1,
            "changes": {"b": {"type": "data", "data": 2}},
            "timestamp": "2024-06-01T00:00:00",
        })
        with log.open("a") as f:
            f.write(torn)  # no \n
        # Re-open should truncate the bad line and recover the good one
        with db_open(log) as db:
            assert dict(db.latest()) == {"a": 1}
        lines = log.read_text().strip().split("\n")
        assert len(lines) == 1

    def test_recovers_from_invalid_json(self, tmp_path):
        log = tmp_path / "db.log"
        with db_open(log) as db:
            db.apply({"a": 1})
        with log.open("a") as f:
            f.write("not valid json\n")
        with db_open(log) as db:
            assert dict(db.latest()) == {"a": 1}

    def test_recovers_from_malformed_event(self, tmp_path):
        log = tmp_path / "db.log"
        with db_open(log) as db:
            db.apply({"a": 1})
        with log.open("a") as f:
            f.write(json.dumps({"only": "garbage"}) + "\n")
        with db_open(log) as db:
            assert dict(db.latest()) == {"a": 1}

    def test_recovery_truncates_and_allows_new_appends(self, tmp_path):
        log = tmp_path / "db.log"
        with db_open(log) as db:
            db.apply({"a": 1})
        with log.open("a") as f:
            f.write("garbage without newline")
        with db_open(log) as db:
            db.apply({"b": 2})
        with db_open(log) as db:
            assert dict(db.latest()) == {"a": 1, "b": 2}

    def test_empty_log_file(self, tmp_path):
        log = tmp_path / "db.log"
        log.touch()
        with db_open(log) as db:
            assert dict(db.latest()) == {}

    def test_nonexistent_log_file_is_created(self, tmp_path):
        log = tmp_path / "does_not_exist_yet.log"
        with db_open(log) as db:
            assert dict(db.latest()) == {}
            db.apply({"a": 1})
        assert log.exists()

    def test_torn_first_line_yields_empty_db(self, tmp_path):
        log = tmp_path / "db.log"
        # Write a partial first line with no good lines preceding
        with log.open("w") as f:
            f.write('{"seq": 0, "changes": {}, "timestamp"')  # cut off
        with db_open(log) as db:
            assert dict(db.latest()) == {}
        # The torn data should have been truncated to nothing
        assert log.stat().st_size == 0


# ---------------------------------------------------------------------------
# File locking
# ---------------------------------------------------------------------------

class TestLocking:
    def test_cannot_open_twice(self, tmp_path):
        log = tmp_path / "db.log"
        with db_open(log):
            with pytest.raises(RuntimeError, match="locked"):
                db_open(log)

    def test_lock_released_after_context_exit(self, tmp_path):
        log = tmp_path / "db.log"
        with db_open(log):
            pass
        # Should be reopenable
        with db_open(log):
            pass

    def test_lock_released_even_with_no_writes(self, tmp_path):
        log = tmp_path / "db.log"
        with db_open(log):
            pass
        with db_open(log):
            pass


# ---------------------------------------------------------------------------
# view()  (read-only, no lock)
# ---------------------------------------------------------------------------

class TestView:
    def test_view_returns_db(self, tmp_path):
        log = tmp_path / "db.log"
        with db_open(log) as db:
            db.apply({"a": 1})
        v = view(log)
        assert isinstance(v, DB)
        assert v.latest()["a"] == 1

    def test_view_does_not_acquire_lock(self, tmp_path):
        log = tmp_path / "db.log"
        with db_open(log) as db:
            db.apply({"a": 1})
            db.save()
            # While the writer holds the lock, a reader can still view
            v = view(log)
            assert v.latest()["a"] == 1

    def test_view_on_nonexistent_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            view(tmp_path / "missing.log")

    def test_view_does_not_modify_torn_writes(self, tmp_path):
        # view uses _read (not _read_and_recover), so it must NOT truncate
        log = tmp_path / "db.log"
        with db_open(log) as db:
            db.apply({"a": 1})
        with log.open("a") as f:
            f.write("torn line")  # no newline
        size_before = log.stat().st_size
        v = view(log)
        assert dict(v.latest()) == {"a": 1}
        assert log.stat().st_size == size_before  # file untouched


# ---------------------------------------------------------------------------
# Integration scenarios
# ---------------------------------------------------------------------------

class TestIntegration:
    def test_full_workflow(self, tmp_path):
        log = tmp_path / "db.log"
        with db_open(log) as db:
            db.apply({"users:1.name": "Alice", "users:1.age": 30})
            db.apply({"users:2.name": "Bob", "users:2.age": 25})
            db.apply({"users:1.age": 31})
            db.apply({"users:2.name": ABSENT, "users:2.age": ABSENT})

        with db_open(log) as db:
            latest = db.latest()
            assert latest["users:1.age"] == 31
            assert "users:2.name" not in latest
            assert "users:2.age" not in latest

    def test_history_across_persistence(self, tmp_path):
        log = tmp_path / "db.log"
        with db_open(log) as db:
            db.apply({"k": "v1"})
            db.apply({"other": "x"})
            db.apply({"k": "v2"})
            db.apply({"k": ABSENT})

        v = view(log)
        hist = list(history(v.events(), "k"))
        assert len(hist) == 3
        assert hist[0][1] == "v1"
        assert hist[1][1] == "v2"
        assert hist[2][1] is ABSENT

    def test_diff_between_versions(self, tmp_path):
        log = tmp_path / "db.log"
        with db_open(log) as db:
            db.apply({"a": 1, "b": 2})
            db.apply({"a": 99, "c": 3})

        v = view(log)
        # at(0) is the state after the first event; latest() includes both
        old = v.at(0)
        new = v.latest()
        changes = {(k, av, bv) for (k, av, bv) in diff(old, new)}
        assert changes == {
            ("a", 1, 99),
            ("c", ABSENT, 3),
        }

    def test_as_of_with_persistent_log(self, tmp_path):
        log = tmp_path / "db.log"
        t1 = datetime(2024, 1, 1, 10, 0, 0)
        t2 = datetime(2024, 1, 1, 11, 0, 0)
        t3 = datetime(2024, 1, 1, 12, 0, 0)
        with db_open(log) as db:
            db.apply({"a": "morning"}, at=t1)
            db.apply({"a": "midday"}, at=t2)
            db.apply({"a": "noon"}, at=t3)

        v = view(log)
        assert v.as_of(datetime(2024, 1, 1, 10, 30, 0))["a"] == "morning"
        assert v.as_of(datetime(2024, 1, 1, 11, 30, 0))["a"] == "midday"
        assert v.as_of(datetime(2024, 1, 1, 12, 30, 0))["a"] == "noon"

    def test_large_log_round_trips(self, tmp_path):
        log = tmp_path / "db.log"
        n = 500
        with db_open(log) as db:
            for i in range(n):
                db.apply({f"k{i}": i})
        with db_open(log) as db:
            latest = db.latest()
            assert len(latest) == n
            for i in range(n):
                assert latest[f"k{i}"] == i
