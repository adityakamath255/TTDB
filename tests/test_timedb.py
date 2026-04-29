from datetime import datetime, timedelta

import pytest

from conftest import SEG, SEG_TINY, SAVE_EVENTS, SAVE_SECONDS, FSYNC
from timedb.types import Event, ABSENT
from timedb.db import TimeDB
from timedb.interval import Interval


T0 = datetime(2025, 1, 1)
T1 = T0 + timedelta(seconds=1)
T2 = T0 + timedelta(seconds=2)
T3 = T0 + timedelta(seconds=3)
T4 = T0 + timedelta(seconds=4)


def make_db(*entries):
    db = TimeDB.in_memory()
    for key, value, dt in entries:
        db.put_at(key, value, dt)
    return db


# -- writes --

def test_put_returns_seq():
    db = TimeDB.in_memory()
    assert db.put_at("x", 1, T0) == 0
    assert db.put_at("y", 2, T1) == 1

def test_put_and_get():
    db = make_db(("x", 1, T0))
    assert db.latest().get("x") == 1

def test_overwrite():
    db = make_db(("x", 1, T0), ("x", 2, T1))
    assert db.latest().get("x") == 2

def test_multiple_keys():
    db = make_db(("x", 1, T0), ("y", 2, T1))
    assert db.latest().get("x") == 1
    assert db.latest().get("y") == 2

def test_get_missing_key():
    db = make_db(("x", 1, T0))
    assert db.latest().get("z") is ABSENT

def test_delete():
    db = make_db(("x", 1, T0))
    db.delete_at("x", T1)
    assert db.latest().get("x") is ABSENT

def test_delete_then_put():
    db = make_db(("x", 1, T0))
    db.delete_at("x", T1)
    db.put_at("x", 2, T2)
    assert db.latest().get("x") == 2

def test_delete_nonexistent():
    db = TimeDB.in_memory()
    db.delete_at("x", T0)
    assert db.latest().get("x") is ABSENT

def test_put_absent_equivalent_to_delete():
    db1 = make_db(("x", 1, T0))
    db1.delete_at("x", T1)

    db2 = make_db(("x", 1, T0))
    db2.put_at("x", ABSENT, T1)

    core = lambda e: (e.key, e.value, e.timestamp)
    assert [core(e) for e in db1.events()] == [core(e) for e in db2.events()]


# -- soft timestamps --

@pytest.mark.parametrize("ts", [T0, datetime(3000, 1, 1)])
def test_accepts_out_of_order_timestamps(ts):
    db = make_db(("anchor", 0, T2))
    db.put_at("x", 1, ts)
    assert db.latest().get("x") == 1


# -- snapshots --

def test_snapshot_at_each_seq():
    db = make_db(("x", 1, T0), ("x", 2, T1), ("x", 3, T2))
    assert db.at(1).get("x") == 1
    assert db.at(2).get("x") == 2
    assert db.at(3).get("x") == 3

def test_snapshot_at_latest_seq_allowed():
    db = make_db(("x", 1, T0), ("y", 2, T1))
    assert db.at(2).get("y") == 2

def test_snapshot_isolation_from_later_writes():
    db = make_db(("x", 1, T0))
    snap = db.at(1)
    db.put_at("x", 2, T1)
    assert snap.get("x") == 1

def test_snapshot_out_of_range_raises():
    db = make_db(("x", 1, T0))
    with pytest.raises(ValueError):
        db.at(5)

def test_snapshot_negative_raises():
    db = make_db(("x", 1, T0))
    with pytest.raises(ValueError):
        db.at(-1)

def test_empty_db_latest_is_empty_snapshot():
    db = TimeDB.in_memory()
    snap = db.latest()
    assert snap.get("anything") is ABSENT
    assert list(snap.keys()) == []
    assert list(snap.items()) == []
    assert snap.timestamp() is None

def test_snapshot_at_zero_is_empty():
    db = make_db(("x", 1, T0))
    snap = db.at(0)
    assert snap.get("x") is ABSENT
    assert list(snap.keys()) == []
    assert snap.timestamp() is None

def test_snapshot_seq():
    db = make_db(("x", 1, T0), ("y", 2, T1))
    assert db.at(0).seq() == 0
    assert db.at(1).seq() == 1
    assert db.at(2).seq() == 2
    assert db.latest().seq() == 2


# -- snapshot views --

def test_contains():
    db = make_db(("x", 1, T0))
    assert "x" in db.latest()
    assert "y" not in db.latest()

def test_keys_excludes_deleted():
    db = make_db(("x", 1, T0), ("y", 2, T1))
    db.delete_at("x", T2)
    assert set(db.latest().keys()) == {"y"}

def test_items_at_seq():
    db = make_db(("x", 1, T0), ("y", 2, T1), ("x", 3, T2))
    assert dict(db.at(2).items()) == {"x": 1, "y": 2}
    assert dict(db.at(3).items()) == {"x": 3, "y": 2}

def test_keys_after_overwrite_unique():
    db = make_db(("x", 1, T0), ("x", 2, T1), ("x", 3, T2))
    assert list(db.latest().keys()) == ["x"]

def test_timestamp_at_seq():
    db = make_db(("x", 1, T0), ("y", 2, T1))
    assert db.at(1).timestamp() == T0
    assert db.at(2).timestamp() == T1


# -- events --

def test_events_returns_all():
    db = make_db(("x", 1, T0), ("y", 2, T1))
    assert list(db.events()) == [
        Event(0, "x", 1, T0, None),
        Event(1, "y", 2, T1, None),
    ]

def test_singleton_put_has_no_tx_id():
    db = make_db(("x", 1, T0))
    assert list(db.events())[0].tx_id is None


# -- between --

def test_between_half_open():
    db = make_db(
        ("a", 0, T0), ("b", 1, T1), ("c", 2, T2),
        ("d", 3, T3), ("e", 4, T4),
    )
    assert [e.seq for e in db.between(1, 3)] == [1, 2]

def test_between_empty_range():
    db = make_db(("a", 0, T0), ("b", 1, T1))
    assert list(db.between(0, 0)) == []

def test_between_from_zero():
    db = make_db(("a", 0, T0), ("b", 1, T1), ("c", 2, T2))
    assert [e.seq for e in db.between(0, 2)] == [0, 1]

def test_between_full_log():
    db = make_db(("a", 0, T0), ("b", 1, T1), ("c", 2, T2))
    assert [e.seq for e in db.between(0, 3)] == [0, 1, 2]

@pytest.mark.parametrize("seq0,seq1", [
    (1, 0),
    (0, 5),
    (-1, 1),
])
def test_between_invalid_range_raises(seq0, seq1):
    db = make_db(("a", 0, T0), ("b", 1, T1))
    with pytest.raises(ValueError):
        list(db.between(seq0, seq1))


# -- interval --

def test_between_returns_interval():
    db = make_db(("x", 1, T0))
    assert isinstance(db.between(0, 1), Interval)

def test_interval_iterates_events():
    db = make_db(("a", 0, T0), ("b", 1, T1), ("c", 2, T2))
    assert [e.seq for e in db.between(0, 2)] == [0, 1]

def test_interval_history_filters_by_key():
    db = make_db(
        ("x", 1, T0), ("y", 2, T1), ("x", 3, T2), ("z", 4, T3), ("x", 5, T4),
    )
    assert [e.value for e in db.between(0, 5).history("x")] == [1, 3, 5]

def test_interval_history_within_bounds():
    db = make_db(("x", 1, T0), ("x", 2, T1), ("x", 3, T2), ("x", 4, T3))
    assert [e.value for e in db.between(0, 2).history("x")] == [1, 2]

def test_interval_history_missing_key_empty():
    db = make_db(("x", 1, T0))
    assert list(db.between(0, 1).history("missing")) == []

def test_interval_keys_touched():
    db = make_db(("x", 1, T0), ("y", 2, T1), ("x", 3, T2), ("z", 4, T3))
    assert db.between(0, 4).keys_touched() == {"x", "y", "z"}

def test_interval_keys_touched_within_bounds():
    db = make_db(("x", 1, T0), ("y", 2, T1), ("z", 3, T2))
    assert db.between(0, 2).keys_touched() == {"x", "y"}

def test_interval_keys_touched_empty_range():
    db = make_db(("x", 1, T0))
    assert db.between(0, 0).keys_touched() == set()

def test_interval_tx_ids_excludes_singletons():
    db = TimeDB.in_memory()
    db.put_at("a", 1, T0)
    tx, _ = db.apply_at({"x": 1, "y": 2}, T1)
    db.put_at("b", 3, T2)
    assert db.between(0, 4).tx_ids() == {tx}

def test_interval_tx_ids_multiple():
    db = TimeDB.in_memory()
    tx1, _ = db.apply_at({"a": 1, "b": 2}, T0)
    tx2, _ = db.apply_at({"c": 3, "d": 4}, T1)
    assert db.between(0, 4).tx_ids() == {tx1, tx2}

def test_interval_tx_ids_empty_when_only_singletons():
    db = make_db(("x", 1, T0), ("y", 2, T1))
    assert db.between(0, 2).tx_ids() == set()

def test_interval_count():
    db = make_db(("a", 0, T0), ("b", 1, T1), ("c", 2, T2), ("d", 3, T3))
    assert db.between(0, 4).count() == 4
    assert db.between(1, 4).count() == 3
    assert db.between(2, 4).count() == 2
    assert db.between(2, 2).count() == 0

def test_interval_count_empty_log():
    db = TimeDB.in_memory()
    assert db.between(0, 0).count() == 0

def test_interval_count_matches_iter():
    db = make_db(("a", 0, T0), ("b", 1, T1), ("c", 2, T2))
    iv = db.between(0, 3)
    assert iv.count() == len(list(iv))


# -- db.history shorthand --

def test_db_history_returns_all_events_for_key():
    db = make_db(("x", 1, T0), ("y", 2, T1), ("x", 3, T2), ("x", 5, T3))
    assert [e.value for e in db.history("x")] == [1, 3, 5]

def test_db_history_includes_deletes():
    db = make_db(("x", 1, T0))
    db.delete_at("x", T1)
    assert [e.value for e in db.history("x")] == [1, ABSENT]

def test_db_history_missing_key_empty():
    db = make_db(("x", 1, T0))
    assert list(db.history("missing")) == []

def test_db_history_empty_log():
    db = TimeDB.in_memory()
    assert list(db.history("x")) == []


# -- diff --

def diff_dict(snap_a, snap_b):
    return {k: (a, b) for k, a, b in snap_a.diff(snap_b)}

def test_diff_same_snapshot_is_empty():
    db = make_db(("x", 1, T0), ("y", 2, T1))
    assert diff_dict(db.latest(), db.latest()) == {}

def test_diff_value_changed_and_restored():
    db = make_db(("x", 1, T0), ("x", 2, T1), ("x", 1, T2))
    assert diff_dict(db.at(1), db.at(3)) == {}

def test_diff_directional():
    db = make_db(("x", 1, T0), ("x", 2, T1))
    assert diff_dict(db.at(1), db.at(2)) == {"x": (1, 2)}
    assert diff_dict(db.at(2), db.at(1)) == {"x": (2, 1)}

@pytest.mark.parametrize("setup,a,b,expected", [
    (lambda db: (db.put_at("x", 1, T0), db.put_at("y", 2, T1)),
     1, 2, {"y": (ABSENT, 2)}),
    (lambda db: (db.put_at("x", 1, T0), db.delete_at("x", T1)),
     1, 2, {"x": (1, ABSENT)}),
    (lambda db: (db.put_at("x", 1, T0), db.put_at("x", 2, T1)),
     1, 2, {"x": (1, 2)}),
    (lambda db: (
        db.put_at("x", 1, T0), db.put_at("y", 2, T1), db.put_at("x", 3, T2)
     ),
     1, 3, {"x": (1, 3), "y": (ABSENT, 2)}),
    (lambda db: (
        db.put_at("x", 1, T0), db.delete_at("x", T1), db.put_at("x", 1, T2)
     ),
     1, 3, {}),
    (lambda db: (
        db.put_at("x", 1, T0), db.delete_at("x", T1), db.put_at("x", 2, T2)
     ),
     1, 3, {"x": (1, 2)}),
])
def test_diff_cases(setup, a, b, expected):
    db = TimeDB.in_memory()
    setup(db)
    assert diff_dict(db.at(a), db.at(b)) == expected

def test_diff_from_zero():
    db = make_db(("x", 1, T0), ("y", 2, T1))
    assert diff_dict(db.at(0), db.latest()) == {
        "x": (ABSENT, 1), "y": (ABSENT, 2),
    }


# -- apply (transactions) --

def test_apply_basic():
    db = TimeDB.in_memory()
    db.apply_at({"x": 1, "y": 2}, T0)
    snap = db.latest()
    assert snap.get("x") == 1
    assert snap.get("y") == 2

def test_apply_returns_start_and_end():
    db = make_db(("a", 0, T0), ("b", 1, T1))
    assert db.apply_at({"x": 1, "y": 2}, T2) == (2, 4)

def test_apply_empty_dict_raises():
    db = TimeDB.in_memory()
    with pytest.raises(ValueError):
        db.apply_at({}, T0)
    assert list(db.events()) == []

def test_apply_shares_timestamp_and_tx_id():
    db = TimeDB.in_memory()
    tx_id, _ = db.apply_at({"x": 1, "y": 2, "z": 3}, T0)
    events = list(db.events())
    assert all(e.timestamp == T0 for e in events)
    assert all(e.tx_id == tx_id for e in events)

def test_apply_with_absent_deletes():
    db = make_db(("x", 1, T0), ("y", 2, T1))
    db.apply_at({"x": ABSENT, "z": 3}, T2)
    snap = db.latest()
    assert snap.get("x") is ABSENT
    assert snap.get("y") == 2
    assert snap.get("z") == 3

def test_apply_validation_failure_appends_nothing():
    db = make_db(("a", 0, T0))
    with pytest.raises(TypeError):
        db.apply_at({"x": 1, "y": {1, 2}}, T1)
    assert len(list(db.events())) == 1

def test_apply_uses_now_for_timestamp():
    db = TimeDB.in_memory()
    before = datetime.now()
    db.apply({"x": 1, "y": 2})
    after = datetime.now()
    assert all(before <= e.timestamp <= after for e in db.events())


# -- transaction_events --

def test_transaction_events_contiguous():
    db = TimeDB.in_memory()
    tx1, end1 = db.apply_at({"a": 1, "b": 2}, T0)
    db.put_at("singleton", 99, T1)
    tx2, end2 = db.apply_at({"c": 3, "d": 4, "e": 5}, T2)

    assert (tx1, end1) == (0, 2)
    assert (tx2, end2) == (3, 6)
    assert [e.key for e in db.tx_events(tx1)] == ["a", "b"]
    assert [e.key for e in db.tx_events(tx2)] == ["c", "d", "e"]

@pytest.mark.parametrize("bad_id", [-1, 999])
def test_transaction_events_unknown_id_raises(bad_id):
    db = make_db(("x", 1, T0))
    with pytest.raises(ValueError):
        db.tx_events(bad_id)

def test_transaction_events_on_singleton_seq_raises():
    db = make_db(("x", 1, T0))
    with pytest.raises(ValueError):
        db.tx_events(0)

def test_transaction_events_on_non_first_tx_seq_raises():
    db = TimeDB.in_memory()
    db.apply_at({"x": 1, "y": 2, "z": 3}, T0)
    with pytest.raises(ValueError):
        db.tx_events(1)


# -- validation --

def test_reject_non_json_value():
    db = TimeDB.in_memory()
    with pytest.raises(TypeError):
        db.put_at("x", {1, 2, 3}, T0)

def test_reject_non_string_dict_key():
    db = TimeDB.in_memory()
    with pytest.raises(TypeError):
        db.put_at("x", {1: "bad"}, T0)

def test_reject_nested_absent():
    db = TimeDB.in_memory()
    with pytest.raises(TypeError):
        db.put_at("x", [1, ABSENT, 3], T0)
    with pytest.raises(TypeError):
        db.put_at("x", {"a": ABSENT}, T0)

def test_none_is_valid_value():
    db = make_db(("x", None, T0))
    assert db.latest().get("x") is None

def test_top_level_absent_accepted_as_delete():
    db = TimeDB.in_memory()
    db.put_at("x", 1, T0)
    db.put_at("x", ABSENT, T1)
    assert db.latest().get("x") is ABSENT


# -- repr --

@pytest.mark.parametrize("n,expected", [
    (0, "TimeDB(0 events)"),
    (1, "TimeDB(1 event)"),
    (2, "TimeDB(2 events)"),
])
def test_db_repr(n, expected):
    db = TimeDB.in_memory()
    for i in range(n):
        db.put_at(f"k{i}", i, T0 + timedelta(seconds=i))
    assert repr(db) == expected


# -- persistence --

def test_roundtrip(tmp_path):
    path = tmp_path / "db"
    with TimeDB.open(path, SAVE_EVENTS, SAVE_SECONDS, SEG, FSYNC) as db:
        db.put_at("x", 1, T0)
        db.put_at("y", "hello", T1)
        db.delete_at("x", T2)

    with TimeDB.open(path, SAVE_EVENTS, SAVE_SECONDS, SEG, FSYNC) as loaded:
        assert dict(loaded.latest().items()) == {"y": "hello"}
        assert loaded.at(1).get("x") == 1
        assert loaded.latest().get("x") is ABSENT
        assert loaded.latest().get("y") == "hello"


def test_load_preserves_tx_id(tmp_path):
    path = tmp_path / "db"
    with TimeDB.open(path, SAVE_EVENTS, SAVE_SECONDS, SEG, FSYNC) as db:
        db.put_at("a", 1, T0)
        db.apply_at({"x": 10, "y": 20}, T1)

    with TimeDB.open(path, SAVE_EVENTS, SAVE_SECONDS, SEG, FSYNC) as loaded:
        events = list(loaded.events())
        assert events[0].tx_id is None
        assert events[1].tx_id == 1
        assert events[2].tx_id == 1
        assert [e.key for e in loaded.tx_events(1)] == ["x", "y"]


def test_nested_values_roundtrip(tmp_path):
    path = tmp_path / "db"
    with TimeDB.open(path, SAVE_EVENTS, SAVE_SECONDS, SEG, FSYNC) as db:
        db.put_at("x", {"a": [1, 2, {"b": True}]}, T0)

    with TimeDB.open(path, SAVE_EVENTS, SAVE_SECONDS, SEG, FSYNC) as loaded:
        assert loaded.latest().get("x") == {"a": [1, 2, {"b": True}]}


def test_flush_persists_without_close(tmp_path):
    from timedb.segment import read_events
    path = tmp_path / "db"
    with TimeDB.open(path, SAVE_EVENTS, SAVE_SECONDS, SEG, FSYNC) as db:
        db.put_at("x", 1, T0)
        db.flush()

        events = read_events(path / "000001.log")
        assert any(e.key == "x" and e.value == 1 for e in events)


def test_seals_when_over_threshold(tmp_path):
    path = tmp_path / "db"
    with TimeDB.open(path, SAVE_EVENTS, SAVE_SECONDS, SEG_TINY, FSYNC) as db:
        for i in range(50):
            db.put_at(f"k{i}", i, T0 + timedelta(seconds=i))

    segs = sorted(path.glob("*.log"))
    assert len(segs) > 1


def test_sealed_segment_not_rewritten(tmp_path):
    path = tmp_path / "db"
    with TimeDB.open(path, SAVE_EVENTS, SAVE_SECONDS, SEG_TINY, FSYNC) as db:
        for i in range(50):
            db.put_at(f"k{i}", i, T0 + timedelta(seconds=i))

    sealed_path = path / "000001.log"
    mtime_before = sealed_path.stat().st_mtime_ns

    with TimeDB.open(path, SAVE_EVENTS, SAVE_SECONDS, SEG_TINY, FSYNC) as db:
        for i in range(50, 60):
            db.put_at(f"k{i}", i, T0 + timedelta(seconds=i))

    assert sealed_path.stat().st_mtime_ns == mtime_before


def test_multi_segment_roundtrip(tmp_path):
    path = tmp_path / "db"
    expected_events: list[Event] = []
    with TimeDB.open(path, SAVE_EVENTS, SAVE_SECONDS, SEG_TINY, FSYNC) as db:
        for i in range(100):
            db.put_at(f"k{i}", i, T0 + timedelta(seconds=i))
        expected_events = list(db.events())

    with TimeDB.open(path, SAVE_EVENTS, SAVE_SECONDS, SEG_TINY, FSYNC) as loaded:
        assert list(loaded.events()) == expected_events
        assert dict(loaded.latest().items()) == {f"k{i}": i for i in range(100)}


def test_reopen_appends(tmp_path):
    path = tmp_path / "db"
    with TimeDB.open(path, SAVE_EVENTS, SAVE_SECONDS, SEG, FSYNC) as db:
        db.put_at("x", 1, T0)

    with TimeDB.open(path, SAVE_EVENTS, SAVE_SECONDS, SEG, FSYNC) as db:
        db.put_at("y", 2, T1)

    with TimeDB.open(path, SAVE_EVENTS, SAVE_SECONDS, SEG, FSYNC) as loaded:
        assert dict(loaded.latest().items()) == {"x": 1, "y": 2}
        assert [e.seq for e in loaded.events()] == [0, 1]


# -- fsync mode --

@pytest.mark.parametrize("mode", ["bogus", "", "FSync", None, 0])
def test_fsync_mode_rejects_bad_value(tmp_path, mode):
    path = tmp_path / "db"
    with pytest.raises(ValueError):
        TimeDB.open(path, SAVE_EVENTS, SAVE_SECONDS, SEG, mode)

@pytest.mark.parametrize("mode", ["none", "seal", "save"])
def test_fsync_mode_roundtrip(tmp_path, mode):
    path = tmp_path / "db"
    with TimeDB.open(path, SAVE_EVENTS, SAVE_SECONDS, SEG_TINY, mode) as db:
        for i in range(20):
            db.put_at(f"k{i}", i, T0 + timedelta(seconds=i))

    with TimeDB.open(path, SAVE_EVENTS, SAVE_SECONDS, SEG_TINY, mode) as loaded:
        assert dict(loaded.latest().items()) == {f"k{i}": i for i in range(20)}


# -- lockfile --

def test_double_open_rejected(tmp_path):
    path = tmp_path / "db"
    with TimeDB.open(path, SAVE_EVENTS, SAVE_SECONDS, SEG, FSYNC):
        with pytest.raises(RuntimeError):
            TimeDB.open(path, SAVE_EVENTS, SAVE_SECONDS, SEG, FSYNC)

def test_lock_released_on_close(tmp_path):
    path = tmp_path / "db"
    with TimeDB.open(path, SAVE_EVENTS, SAVE_SECONDS, SEG, FSYNC) as db:
        db.put_at("x", 1, T0)
    with TimeDB.open(path, SAVE_EVENTS, SAVE_SECONDS, SEG, FSYNC) as db:
        assert db.latest().get("x") == 1


# -- atomic save invariant --

def _files_in_manifest(path):
    from timedb.segment import read_manifest, segment_num, state_filename, segment_filename
    raw = read_manifest(path / "manifest")
    nums = [segment_num(s["filename"]) for s in raw["segments"]]
    expected = set()
    for n in nums:
        expected.add(segment_filename(n))
        expected.add(state_filename(n))
    return expected

def _files_on_disk(path):
    return {
        p.name for p in path.iterdir()
        if p.suffix in (".log", ".state")
    }

def test_directory_matches_manifest_after_save(tmp_path):
    path = tmp_path / "db"
    with TimeDB.open(path, SAVE_EVENTS, SAVE_SECONDS, SEG_TINY, FSYNC) as db:
        for i in range(20):
            db.put_at(f"k{i}", i, T0 + timedelta(seconds=i))
        db.flush()
    assert _files_on_disk(path) == _files_in_manifest(path)

def test_seal_does_not_overwrite_existing_files(tmp_path):
    path = tmp_path / "db"
    with TimeDB.open(path, SAVE_EVENTS, SAVE_SECONDS, SEG_TINY, FSYNC) as db:
        for i in range(10):
            db.put_at(f"k{i}", i, T0 + timedelta(seconds=i))
            db.flush()
        # write more to potentially trigger a seal
        for i in range(10, 30):
            db.put_at(f"k{i}", i, T0 + timedelta(seconds=i))
            db.flush()
    # every file referenced by the (final) manifest exists and the
    # directory has no extras
    assert _files_on_disk(path) == _files_in_manifest(path)

def test_orphan_files_cleaned_up(tmp_path):
    path = tmp_path / "db"
    with TimeDB.open(path, SAVE_EVENTS, SAVE_SECONDS, SEG, FSYNC) as db:
        db.put_at("x", 1, T0)
        db.flush()
        # plant an orphan
        (path / "999999.log").write_text("garbage")
        (path / "999999.state").write_text("garbage")
        # next save should remove them
        db.put_at("y", 2, T1)
        db.flush()
    assert not (path / "999999.log").exists()
    assert not (path / "999999.state").exists()
