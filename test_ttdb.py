import json
from datetime import UTC, datetime, timedelta, timezone

import pytest

from ttdb import (
    CorruptJournal,
    DB,
    Event,
    connect,
    diff,
    history,
    in_memory,
    inspect,
)

T0 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)


def ev(changes=(), ts=T0):
    return Event(changes=dict(changes), ts=ts)


class TestSnapshots:
    def test_set_overwrite_delete(self):
        db = DB([ev({"a": 1, "b": 2}), ev({"a": 99, "b": None})])
        assert dict(db.latest()) == {"a": 99}

    def test_delete_missing_is_noop(self):
        db = in_memory()
        db.apply({"gone": None})
        assert dict(db.latest()) == {}

    def test_does_not_mutate_input(self):
        db = in_memory()
        db.apply({"a": 1})
        root = db.latest()
        db.apply({"a": 2})
        assert dict(root) == {"a": 1}


class TestSerialization:
    @pytest.mark.parametrize(
        "value", ["hi", "", "世界", 0, -17, 3.14, True, False]
    )
    def test_value_round_trip(self, tmp_path, value):
        log = tmp_path / "db.log"
        with connect(log) as db:
            db.apply({"k": value}, at=T0)

        reopened = inspect(log)
        assert reopened.latest()["k"] == value
        assert reopened.events()[0].changes["k"] == value

    def test_delete_round_trips_as_null(self, tmp_path):
        log = tmp_path / "db.log"
        with connect(log) as db:
            db.apply({"d": 1}, at=T0)
            db.apply({"d": None}, at=T0)

        reopened = inspect(log)
        assert "d" not in reopened.latest()
        assert reopened.events()[1].changes["d"] is None

    def test_journal_record_has_documented_envelope(self, tmp_path):
        log = tmp_path / "db.log"
        with connect(log) as db:
            db.apply({}, at=T0)

        raw = json.loads(log.read_text())
        assert set(raw) == {"changes", "ts"}
        assert inspect(log).events() == (ev(),)


class TestDiff:
    def test_add_remove_change(self):
        a = {"keep": 1, "drop": 2, "chg": 3}
        b = {"keep": 1, "chg": 99, "new": 4}
        assert set(diff(a, b)) == {
            ("drop", 2, None),
            ("chg", 3, 99),
            ("new", None, 4),
        }

    def test_identical_is_empty(self):
        m = {"a": 1}
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
        assert dict(db.as_of(datetime.now(UTC))) == {}
        with pytest.raises(ValueError):
            db.at(0)

    def test_as_of_is_inclusive_and_bisects(self):
        t1 = datetime(2024, 1, 1, tzinfo=UTC)
        t2 = datetime(2024, 1, 3, tzinfo=UTC)
        db = DB([ev({"a": 1}, ts=t1), ev({"b": 2}, ts=t2)])
        assert dict(db.as_of(t1 - timedelta(days=1))) == {}
        assert dict(db.as_of(t1)) == {"a": 1}
        assert dict(db.as_of(datetime(2024, 1, 2, tzinfo=UTC))) == {"a": 1}
        assert dict(db.as_of(t2)) == {"a": 1, "b": 2}

    def test_event_does_not_retain_caller_mapping(self):
        changes = {"a": 1}
        db = DB([Event(changes=changes, ts=T0)])

        changes["a"] = 2

        assert db.events()[0].changes["a"] == 1
        assert db.latest()["a"] == 1

    def test_returned_events_cannot_change_db_history(self):
        db = self.make()
        returned = db.events()

        assert isinstance(returned, tuple)
        assert len(db.events()) == 2
        assert dict(db.latest()) == {"a": "v1", "b": 2}


class TestApply:
    def test_returns_sequential_lengths_and_updates_state(self):
        db = in_memory()
        assert (db.apply({"a": 1}), db.apply({"b": 2})) == (1, 2)
        assert dict(db.latest()) == {"a": 1, "b": 2}

    def test_delete_via_none(self):
        db = in_memory()
        db.apply({"a": 1})
        db.apply({"a": None})
        assert "a" not in db.latest()

    def test_default_timestamp_is_now(self):
        before = datetime.now(UTC)
        db = in_memory()
        db.apply({"a": 1})
        assert before <= db.events()[0].ts <= datetime.now(UTC)

    def test_explicit_timestamp_and_monotonic_guard(self):
        db = in_memory()
        db.apply({"a": 1}, at=datetime(2024, 6, 1, tzinfo=UTC))
        db.apply({"b": 2}, at=datetime(2024, 6, 1, tzinfo=UTC))
        with pytest.raises(ValueError):
            db.apply({"c": 3}, at=datetime(2024, 5, 1, tzinfo=UTC))

    @pytest.mark.parametrize("value", [[], float("nan"), float("inf")])
    def test_rejects_invalid_value_before_changing_state(self, value):
        db = in_memory()

        with pytest.raises(ValueError):
            db.apply({"bad": value})

        assert dict(db.latest()) == {}

    def test_rejects_naive_timestamp(self):
        with pytest.raises(ValueError, match="timezone-aware"):
            in_memory().apply({"a": 1}, at=datetime(2024, 6, 1))

    def test_normalizes_explicit_timestamp_to_utc(self):
        db = in_memory()
        ist = timezone(timedelta(hours=5, minutes=30))

        db.apply({"a": 1}, at=datetime(2024, 6, 1, 12, tzinfo=ist))

        assert db.events()[0].ts == datetime(2024, 6, 1, 6, 30, tzinfo=UTC)
        assert db.events()[0].ts.tzinfo is UTC


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
            assert log.read_text() != ""

    def test_seq_continues_across_reopen(self, tmp_path):
        log = tmp_path / "db.log"
        with connect(log) as db:
            db.apply({"a": 1})
        with connect(log) as db:
            assert db.apply({"b": 2}) == 2

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
            f.write('{"changes": {"b": 2}, "ts": "2024-06-01T00:00:00"')
        with connect(log) as db:
            assert dict(db.latest()) == {"a": 1}
            db.apply({"b": 2})
        with connect(log) as db:
            assert dict(db.latest()) == {"a": 1, "b": 2}

    @pytest.mark.parametrize(
        "garbage",
        ["not json\n", f'{{"changes": [], "ts": "{T0.isoformat()}"}}\n'],
    )
    def test_complete_corrupt_line_is_reported_and_preserved(
        self, tmp_path, garbage
    ):
        log = tmp_path / "db.log"
        self._seed(log)
        with log.open("a") as f:
            f.write(garbage)
        contents = log.read_text()

        with pytest.raises(CorruptJournal, match="line 2"):
            with connect(log):
                pass

        assert log.read_text() == contents

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
                with connect(log):
                    pass

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
            v = inspect(log)
            assert v.latest()["a"] == 1
        with log.open("a") as f:
            f.write("torn line")
        size = log.stat().st_size
        assert dict(inspect(log).latest()) == {"a": 1}
        assert log.stat().st_size == size

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            inspect(tmp_path / "missing.log")
