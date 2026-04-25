from datetime import datetime, timedelta

import pytest

from timedb.types import Event, ABSENT
from timedb.db import TimeDB


# -- helpers --

def make_db(*entries):
    db = TimeDB()
    for key, value, dt in entries:
        db.put_at(key, value, dt)
    return db


T0 = datetime(2025, 1, 1)
T1 = T0 + timedelta(seconds=1)
T2 = T0 + timedelta(seconds=2)
T3 = T0 + timedelta(seconds=3)
T4 = T0 + timedelta(seconds=4)
T5 = T0 + timedelta(seconds=5)


# -- put and get --

def test_put_and_get():
    db = make_db(("x", 1, T0))
    assert db.latest().get("x") == 1

def test_put_returns_seq():
    db = TimeDB()
    assert db.put_at("x", 1, T0) == 0
    assert db.put_at("y", 2, T1) == 1

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


# -- delete --

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
    db = TimeDB()
    db.delete_at("x", T0)
    assert db.latest().get("x") is ABSENT


# -- soft timestamps --

@pytest.mark.parametrize("ts", [T0, datetime(3000, 1, 1)])
def test_accepts_soft_timestamps(ts):
    db = make_db(("anchor", 0, T2))
    db.put_at("x", 1, ts)
    assert db.latest().get("x") == 1


# -- snapshots --

def test_snapshot_at():
    db = make_db(("x", 1, T0), ("x", 2, T1), ("x", 3, T2))
    assert db.at(0).get("x") == 1
    assert db.at(1).get("x") == 2
    assert db.at(2).get("x") == 3

def test_snapshot_isolation():
    db = make_db(("x", 1, T0))
    snap = db.at(0)
    db.put_at("x", 2, T1)
    assert snap.get("x") == 1

def test_snapshot_empty_db():
    db = TimeDB()
    snap = db.latest()
    assert snap.get("anything") is ABSENT
    assert list(snap.keys()) == []
    assert len(snap) == 0

def test_snapshot_negative_seq():
    db = make_db(("x", 1, T0))
    snap = db.at(-1)
    assert snap.get("x") is ABSENT
    assert list(snap.keys()) == []

def test_snapshot_out_of_range_raises():
    db = make_db(("x", 1, T0))
    with pytest.raises(ValueError):
        db.at(5)


# -- snapshot methods --

def test_snapshot_contains():
    db = make_db(("x", 1, T0))
    assert "x" in db.latest()
    assert "y" not in db.latest()

def test_snapshot_keys():
    db = make_db(("x", 1, T0), ("y", 2, T1), ("z", 3, T2))
    assert set(db.latest().keys()) == {"x", "y", "z"}

def test_snapshot_keys_excludes_deleted():
    db = make_db(("x", 1, T0), ("y", 2, T1))
    db.delete_at("x", T2)
    assert set(db.latest().keys()) == {"y"}

def test_snapshot_items():
    db = make_db(("x", 1, T0), ("y", 2, T1))
    assert set(db.latest().items()) == {("x", 1), ("y", 2)}

def test_snapshot_len():
    db = make_db(("x", 1, T0), ("y", 2, T1))
    assert len(db.latest()) == 2
    db.delete_at("x", T2)
    assert len(db.latest()) == 1

def test_snapshot_timestamp():
    db = make_db(("x", 1, T0), ("y", 2, T1))
    assert db.at(0).timestamp() == T0
    assert db.at(1).timestamp() == T1

def test_snapshot_timestamp_empty():
    db = TimeDB()
    assert db.latest().timestamp() is None

def test_snapshot_seq():
    db = make_db(("x", 1, T0), ("y", 2, T1))
    assert db.at(0).seq() == 0
    assert db.at(1).seq() == 1
    assert db.latest().seq() == 1
    assert db.at(-1).seq() == -1


# -- events and history --

def test_events():
    db = make_db(("x", 1, T0), ("y", 2, T1))
    events = list(db.events())
    assert events == [Event(0, "x", 1, T0, None), Event(1, "y", 2, T1, None)]

def test_singleton_puts_have_no_tx_id():
    db = make_db(("x", 1, T0))
    assert list(db.events())[0].tx_id is None

def test_history():
    db = make_db(("x", 1, T0), ("y", 2, T1), ("x", 3, T2))
    hist = list(db.history("x"))
    assert [e.value for e in hist] == [1, 3]

def test_history_includes_deletes():
    db = make_db(("x", 1, T0))
    db.delete_at("x", T1)
    hist = list(db.history("x"))
    assert len(hist) == 2
    assert hist[1].value is ABSENT


# -- between --

def test_between_basic():
    db = make_db(("a", 0, T0), ("b", 1, T1), ("c", 2, T2), ("d", 3, T3), ("e", 4, T4))
    assert [e.seq for e in db.between(1, 3)] == [2, 3]

def test_between_empty_range():
    db = make_db(("a", 0, T0), ("b", 1, T1))
    assert list(db.between(0, 0)) == []

def test_between_from_before_all():
    db = make_db(("a", 0, T0), ("b", 1, T1), ("c", 2, T2))
    assert [e.seq for e in db.between(-1, 1)] == [0, 1]

def test_between_full_log():
    db = make_db(("a", 0, T0), ("b", 1, T1), ("c", 2, T2))
    assert [e.seq for e in db.between(-1, 2)] == [0, 1, 2]

def test_between_empty_db():
    db = TimeDB()
    assert list(db.between(-1, -1)) == []

@pytest.mark.parametrize("seq0,seq1", [
    (1, 0),     # reversed
    (0, 5),     # seq1 out of range
])
def test_between_invalid_range_raises(seq0, seq1):
    db = make_db(("a", 0, T0), ("b", 1, T1))
    with pytest.raises(ValueError):
        list(db.between(seq0, seq1))


# -- diff --

def test_diff_same_snapshot():
    db = make_db(("x", 1, T0), ("y", 2, T1))
    assert db.latest().diff(db.latest()) == {}

def test_diff_value_changed_and_restored():
    db = make_db(("x", 1, T0), ("x", 2, T1), ("x", 1, T2))
    assert db.at(0).diff(db.at(2)) == {}

def test_diff_ignores_keys_outside_range():
    db = make_db(
        ("x", 1, T0), ("x", 2, T1),
        ("y", 3, T2), ("y", 4, T3),
    )
    assert db.at(0).diff(db.at(1)) == {"x": (1, 2)}
    assert db.at(2).diff(db.at(3)) == {"y": (3, 4)}

def test_diff_directional():
    db = make_db(("x", 1, T0), ("x", 2, T1))
    assert db.at(0).diff(db.at(1)) == {"x": (1, 2)}
    assert db.at(1).diff(db.at(0)) == {"x": (2, 1)}

def test_diff_cross_log_raises():
    db1 = make_db(("x", 1, T0))
    db2 = make_db(("x", 1, T0))
    with pytest.raises(ValueError):
        db1.latest().diff(db2.latest())

@pytest.mark.parametrize("setup,a,b,expected", [
    # add
    (lambda db: (db.put_at("x", 1, T0), db.put_at("y", 2, T1)),
     0, 1, {"y": (ABSENT, 2)}),
    # remove
    (lambda db: (db.put_at("x", 1, T0), db.delete_at("x", T1)),
     0, 1, {"x": (1, ABSENT)}),
    # change
    (lambda db: (db.put_at("x", 1, T0), db.put_at("x", 2, T1)),
     0, 1, {"x": (1, 2)}),
    # multiple changes
    (lambda db: (db.put_at("x", 1, T0), db.put_at("y", 2, T1), db.put_at("x", 3, T2)),
     0, 2, {"x": (1, 3), "y": (ABSENT, 2)}),
    # delete then re-add same value
    (lambda db: (db.put_at("x", 1, T0), db.delete_at("x", T1), db.put_at("x", 1, T2)),
     0, 2, {}),
    # delete then re-add different value
    (lambda db: (db.put_at("x", 1, T0), db.delete_at("x", T1), db.put_at("x", 2, T2)),
     0, 2, {"x": (1, 2)}),
])
def test_diff_cases(setup, a, b, expected):
    db = TimeDB()
    setup(db)
    assert db.at(a).diff(db.at(b)) == expected

def test_diff_from_before_all():
    db = make_db(("x", 1, T0), ("y", 2, T1))
    assert db.at(-1).diff(db.latest()) == {"x": (ABSENT, 1), "y": (ABSENT, 2)}

def test_diff_to_before_all():
    db = make_db(("x", 1, T0), ("y", 2, T1))
    assert db.latest().diff(db.at(-1)) == {"x": (1, ABSENT), "y": (2, ABSENT)}


# -- put accepts absent --

def test_put_accepts_absent():
    db = TimeDB()
    db.put_at("x", 1, T0)
    db.put_at("x", ABSENT, T1)
    assert db.latest().get("x") is ABSENT

def test_delete_equivalent_to_put_absent():
    db1 = make_db(("x", 1, T0))
    db1.delete_at("x", T1)

    db2 = make_db(("x", 1, T0))
    db2.put_at("x", ABSENT, T1)

    core = lambda e: (e.key, e.value, e.timestamp)
    assert [core(e) for e in db1.events()] == [core(e) for e in db2.events()]


# -- apply --

def test_apply_basic():
    db = TimeDB()
    db.apply_at({"x": 1, "y": 2}, T0)
    assert db.latest().get("x") == 1
    assert db.latest().get("y") == 2

def test_apply_tx_id_is_first_seq():
    db = make_db(("a", 0, T0), ("b", 1, T1))
    assert db.apply_at({"x": 1, "y": 2}, T2) == (2, 3)

def test_apply_empty_dict_is_no_op():
    db = TimeDB()
    assert db.apply_at({}, T0) is None
    assert list(db.events()) == []

def test_apply_shared_timestamp():
    db = TimeDB()
    db.apply_at({"x": 1, "y": 2, "z": 3}, T0)
    assert all(e.timestamp == T0 for e in db.events())

def test_apply_shared_tx_id():
    db = TimeDB()
    tx_id, _ = db.apply_at({"x": 1, "y": 2, "z": 3}, T0)
    assert all(e.tx_id == tx_id for e in db.events())

def test_apply_with_absent_deletes():
    db = make_db(("x", 1, T0), ("y", 2, T1))
    db.apply_at({"x": ABSENT, "z": 3}, T2)
    snap = db.latest()
    assert snap.get("x") is ABSENT
    assert snap.get("y") == 2
    assert snap.get("z") == 3

def test_apply_mixed_puts_and_deletes():
    db = make_db(("x", 1, T0))
    db.apply_at({"x": 2, "y": ABSENT, "z": 5}, T1)
    snap = db.latest()
    assert snap.get("x") == 2
    assert snap.get("y") is ABSENT
    assert snap.get("z") == 5

def test_apply_validation_failure_appends_nothing():
    db = make_db(("a", 0, T0))
    with pytest.raises(TypeError):
        db.apply_at({"x": 1, "y": {1, 2}}, T1)
    assert len(list(db.events())) == 1

def test_apply_uses_now_for_timestamp():
    db = TimeDB()
    before = datetime.now()
    db.apply({"x": 1, "y": 2})
    after = datetime.now()
    assert all(before <= e.timestamp <= after for e in db.events())


# -- transaction_events --

def test_transactions_are_contiguous_and_addressable():
    db = TimeDB()
    tx1, last1 = db.apply_at({"a": 1, "b": 2}, T0)
    db.put_at("singleton", 99, T1)
    tx2, last2 = db.apply_at({"c": 3, "d": 4, "e": 5}, T2)

    assert (tx1, last1) == (0, 1)
    assert (tx2, last2) == (3, 5)
    assert [e.key for e in db.transaction_events(tx1)] == ["a", "b"]
    assert [e.key for e in db.transaction_events(tx2)] == ["c", "d", "e"]

@pytest.mark.parametrize("bad_id", [-1, 999])
def test_transaction_events_out_of_range_raises(bad_id):
    db = make_db(("x", 1, T0))
    with pytest.raises(ValueError):
        db.transaction_events(bad_id)

def test_transaction_events_on_singleton_seq_raises():
    db = make_db(("x", 1, T0))
    with pytest.raises(ValueError):
        db.transaction_events(0)

def test_transaction_events_mid_txn_seq_raises():
    db = TimeDB()
    db.apply_at({"x": 1, "y": 2, "z": 3}, T0)
    with pytest.raises(ValueError):
        db.transaction_events(1)


# -- validation --

def test_reject_non_json_value():
    db = TimeDB()
    with pytest.raises(TypeError):
        db.put_at("x", {1, 2, 3}, T0)

def test_reject_non_string_dict_key():
    db = TimeDB()
    with pytest.raises(TypeError):
        db.put_at("x", {1: "bad"}, T0)

def test_none_is_valid_value():
    db = make_db(("x", None, T0))
    assert db.latest().get("x") is None


# -- persistence --

def test_save_and_load(tmp_path):
    db = make_db(("x", 1, T0), ("y", "hello", T1))
    db.delete_at("x", T2)
    path = tmp_path / "db.jsonl"

    db.save(path)
    loaded = TimeDB.load(path)

    assert list(loaded.latest().items()) == list(db.latest().items())
    assert loaded.at(0).get("x") == 1
    assert loaded.latest().get("x") is ABSENT
    assert loaded.latest().get("y") == "hello"

def test_load_validates_seq(tmp_path):
    path = tmp_path / "db.jsonl"
    path.write_text(
        '{"seq": 5, "key": "x", "value": {"type": "data", "data": 1}, '
        '"timestamp": "2025-01-01T00:00:00", "tx_id": null}\n'
    )
    with pytest.raises(ValueError):
        TimeDB.load(path)

def test_load_unchecked(tmp_path):
    path = tmp_path / "db.jsonl"
    path.write_text(
        '{"seq": 0, "key": "x", "value": {"type": "data", "data": 1}, '
        '"timestamp": "2025-01-01T00:00:00", "tx_id": null}\n'
    )
    db = TimeDB.load_unchecked(path)
    assert db.latest().get("x") == 1

def test_save_nested_values(tmp_path):
    db = make_db(("x", {"a": [1, 2, {"b": True}]}, T0))
    path = tmp_path / "db.jsonl"
    db.save(path)
    loaded = TimeDB.load(path)
    assert loaded.latest().get("x") == {"a": [1, 2, {"b": True}]}

def test_save_and_load_preserves_tx_id(tmp_path):
    db = TimeDB()
    db.put_at("a", 1, T0)
    db.apply_at({"x": 10, "y": 20}, T1)
    path = tmp_path / "db.jsonl"

    db.save(path)
    loaded = TimeDB.load(path)

    events = list(loaded.events())
    assert events[0].tx_id is None
    assert events[1].tx_id == 1
    assert events[2].tx_id == 1
    assert [e.key for e in loaded.transaction_events(1)] == ["x", "y"]


# -- repr --

@pytest.mark.parametrize("n,expected", [
    (0, "TimeDB(0 events)"),
    (1, "TimeDB(1 event)"),
    (2, "TimeDB(2 events)"),
])
def test_db_repr(n, expected):
    db = TimeDB()
    for i in range(n):
        db.put_at(f"k{i}", i, T0 + timedelta(seconds=i))
    assert repr(db) == expected
