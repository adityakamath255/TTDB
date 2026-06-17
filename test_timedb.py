import json
from datetime import datetime, timedelta

import pytest

from timedb import (
    EMPTY,
    DB,
    Event,
    _deserialize_event,
    _fold_event,
    _serialize_event,
    connect,
    diff,
    history,
    in_memory,
    inspect,
)

T0 = datetime(2024, 1, 1, 12, 0, 0)


def ev(changes=(), ts=T0):
    return Event(changes=dict(changes), ts=ts)


class TestFold:
    def test_set_overwrite_delete(self):
        s = _fold_event(EMPTY, ev({"a": 1, "b": 2}))
        s = _fold_event(s, ev({"a": 99, "b": None}))
        assert dict(s) == {"a": 99}

    def test_delete_missing_is_noop(self):
        assert dict(_fold_event(EMPTY, ev({"gone": None}))) == {}

    def test_does_not_mutate_input(self):
        root = _fold_event(EMPTY, ev({"a": 1}))
        _fold_event(root, ev({"a": 2}))
        assert dict(root) == {"a": 1}


class TestSerialization:
    @pytest.mark.parametrize(
        "value", ["hi", "", "世界", 0, -17, 3.14, True, False]
    )
    def test_value_round_trip(self, value):
        e = ev({"k": value})
        assert _deserialize_event(_serialize_event(e)).changes["k"] == value

    def test_delete_round_trips_as_null(self):
        e = ev({"d": None}, ts=datetime(2024, 6, 15, 10, 30, 45))
        raw = _serialize_event(e)
        assert raw["changes"]["d"] is None
        assert _deserialize_event(json.loads(json.dumps(raw))) == e

    def test_empty_changes_omitted(self):
        raw = _serialize_event(ev())
        assert "changes" not in raw
        assert _deserialize_event(raw) == ev()


class TestDiff:
    def test_add_remove_change(self):
        a = EMPTY.set("keep", 1).set("drop", 2).set("chg", 3)
        b = EMPTY.set("keep", 1).set("chg", 99).set("new", 4)
        assert set(diff(a, b)) == {
            ("drop", 2, None),
            ("chg", 3, 99),
            ("new", None, 4),
        }

    def test_identical_is_empty(self):
        m = EMPTY.set("a", 1)
        assert list(diff(m, m)) == []


class TestHistory:
    def test_tracks_one_key_including_delete(self):
        events = [ev({"a": 1}), ev({"b": 2}), ev({"a": 99}), ev({"a": None})]
        assert [(s, v) for s, v, _ in history(events, "a")] == [
            (0, 1),
            (2, 99),
            (3, None),
        ]

    def test_missing_key_is_empty(self):
        assert list(history([ev({"x": 1})], "nope")) == []


class TestReadOnly:
    def make(self):
        return DB([ev({"a": "v0"}), ev({"a": "v1", "b": 2})])

    def test_at_is_state_after_event(self):
        db = self.make()
        assert dict(db.at(0)) == {"a": "v0"}
        assert dict(db.at(1)) == {"a": "v1", "b": 2}
        assert db.at(1) is db.latest()

    @pytest.mark.parametrize("seq", [-1, 2])
    def test_at_out_of_range(self, seq):
        with pytest.raises(ValueError):
            self.make().at(seq)

    def test_empty_db(self):
        db = DB([])
        assert dict(db.latest()) == {}
        assert dict(db.as_of(datetime.now())) == {}
        with pytest.raises(ValueError):
            db.at(0)

    def test_as_of_is_inclusive_and_bisects(self):
        t1, t2 = datetime(2024, 1, 1), datetime(2024, 1, 3)
        db = DB([ev({"a": 1}, ts=t1), ev({"b": 2}, ts=t2)])
        assert dict(db.as_of(t1 - timedelta(days=1))) == {}
        assert dict(db.as_of(t1)) == {"a": 1}
        assert dict(db.as_of(datetime(2024, 1, 2))) == {"a": 1}
        assert dict(db.as_of(t2)) == {"a": 1, "b": 2}


class TestApply:
    def test_returns_sequential_seqs_and_updates_state(self):
        db = in_memory()
        assert (db.apply({"a": 1}), db.apply({"b": 2})) == (0, 1)
        assert dict(db.latest()) == {"a": 1, "b": 2}

    def test_delete_via_none(self):
        db = in_memory()
        db.apply({"a": 1})
        db.apply({"a": None})
        assert "a" not in db.latest()

    def test_default_timestamp_is_now(self):
        before = datetime.now()
        db = in_memory()
        db.apply({"a": 1})
        assert before <= db.events()[0].ts <= datetime.now()

    def test_explicit_timestamp_and_monotonic_guard(self):
        db = in_memory()
        db.apply({"a": 1}, at=datetime(2024, 6, 1))
        db.apply({"b": 2}, at=datetime(2024, 6, 1))  # equal allowed
        with pytest.raises(ValueError):
            db.apply({"c": 3}, at=datetime(2024, 5, 1))


class TestDurable:
    def test_roundtrip_across_reopen(self, tmp_path):
        log = tmp_path / "db.log"
        with connect(log) as db:
            db.apply({"x": 1})
            db.apply({"y": 2})
            db.apply({"x": None})
        with connect(log) as db:
            assert dict(db.latest()) == {"y": 2}

    def test_apply_persists_immediately(self, tmp_path):
        log = tmp_path / "db.log"
        with connect(log) as db:
            db.apply({"a": 1})
            assert log.read_text() != ""  # no buffer: on disk at once

    def test_seq_continues_across_reopen(self, tmp_path):
        log = tmp_path / "db.log"
        with connect(log) as db:
            db.apply({"a": 1})
        with connect(log) as db:
            assert db.apply({"b": 2}) == 1

    def test_one_json_object_per_line(self, tmp_path):
        log = tmp_path / "db.log"
        with connect(log) as db:
            db.apply({"a": 1})
            db.apply({"b": 2})
        lines = log.read_text().splitlines()
        assert len(lines) == 2
        assert all(json.loads(line) for line in lines)


class TestRecovery:
    def _seed(self, log):
        with connect(log) as db:
            db.apply({"a": 1})

    def test_torn_line_truncated_and_appendable(self, tmp_path):
        log = tmp_path / "db.log"
        self._seed(log)
        with log.open("a") as f:
            f.write('{"changes": {"b": 2}, "ts": "2024-06-01T00:00:00"')  # no \n
        with connect(log) as db:
            assert dict(db.latest()) == {"a": 1}
            db.apply({"b": 2})
        with connect(log) as db:
            assert dict(db.latest()) == {"a": 1, "b": 2}

    @pytest.mark.parametrize("garbage", ["not json\n", '{"only": "x"}\n'])
    def test_unparsable_trailing_line_ignored(self, tmp_path, garbage):
        log = tmp_path / "db.log"
        self._seed(log)
        with log.open("a") as f:
            f.write(garbage)
        with connect(log) as db:
            assert dict(db.latest()) == {"a": 1}

    def test_torn_first_line_yields_empty(self, tmp_path):
        log = tmp_path / "db.log"
        log.write_text('{"changes": {}, "ts"')
        with connect(log) as db:
            assert dict(db.latest()) == {}
        assert log.stat().st_size == 0


class TestLocking:
    def test_second_open_is_rejected(self, tmp_path):
        log = tmp_path / "db.log"
        with connect(log):
            with pytest.raises(RuntimeError, match="locked"):
                connect(log)

    def test_lock_released_on_exit(self, tmp_path):
        log = tmp_path / "db.log"
        with connect(log):
            pass
        with connect(log):
            pass


class TestInspect:
    def test_reads_without_lock_or_truncation(self, tmp_path):
        log = tmp_path / "db.log"
        with connect(log) as db:
            db.apply({"a": 1})
            v = inspect(log)  # readable while writer holds the lock
            assert v.latest()["a"] == 1
        with log.open("a") as f:
            f.write("torn line")  # no newline
        size = log.stat().st_size
        assert dict(inspect(log).latest()) == {"a": 1}
        assert log.stat().st_size == size  # inspect never repairs

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            inspect(tmp_path / "missing.log")
