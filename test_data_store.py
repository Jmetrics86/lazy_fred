"""Unit tests for data_store.py — SQLite history DB and config save/load."""

import os
import tempfile

from data_store import DataStore, save_config, list_configs, load_config


class TestDataStore:
    def _tmp_db(self):
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        return path

    def test_create_and_close(self):
        path = self._tmp_db()
        db = DataStore(db_path=path)
        assert os.path.exists(path)
        db.close()
        os.unlink(path)

    def test_start_and_finish_run(self):
        path = self._tmp_db()
        db = DataStore(db_path=path)
        run_id = db.start_run(["GDP", "UNRATE"], "2020-01-01", "test_cfg")
        assert isinstance(run_id, int)
        assert run_id >= 1

        db.finish_run(run_id, new_rows=100, updated_rows=5)
        runs = db.get_recent_runs(1)
        assert len(runs) == 1
        assert runs[0]["status"] == "completed"
        assert runs[0]["new_rows"] == 100
        assert runs[0]["updated_rows"] == 5
        assert runs[0]["series_count"] == 2
        db.close()
        os.unlink(path)

    def test_upsert_new_observations(self):
        path = self._tmp_db()
        db = DataStore(db_path=path)
        run_id = db.start_run(["GDP"], None)

        rows = [
            ("GDP", "2024-01-01", 28000.0),
            ("GDP", "2024-04-01", 28500.0),
            ("GDP", "2024-07-01", 29000.0),
        ]
        new, updated = db.upsert_observations(rows, run_id, "Q")
        assert new == 3
        assert updated == 0

        stats = db.get_total_stats()
        assert stats["series_count"] == 1
        assert stats["observation_count"] == 3
        db.close()
        os.unlink(path)

    def test_upsert_updates_existing(self):
        path = self._tmp_db()
        db = DataStore(db_path=path)
        run_id = db.start_run(["GDP"], None)

        rows1 = [("GDP", "2024-01-01", 28000.0)]
        db.upsert_observations(rows1, run_id, "Q")

        run_id2 = db.start_run(["GDP"], None)
        rows2 = [
            ("GDP", "2024-01-01", 28100.0),
            ("GDP", "2024-04-01", 28500.0),
        ]
        new, updated = db.upsert_observations(rows2, run_id2, "Q")
        assert new == 1
        assert updated == 1

        stats = db.get_total_stats()
        assert stats["observation_count"] == 2
        db.close()
        os.unlink(path)

    def test_get_series_date_range(self):
        path = self._tmp_db()
        db = DataStore(db_path=path)
        run_id = db.start_run(["SP500"], None)

        rows = [
            ("SP500", "2024-01-02", 4700.0),
            ("SP500", "2024-06-15", 5400.0),
            ("SP500", "2024-12-31", 5900.0),
        ]
        db.upsert_observations(rows, run_id, "D")

        info = db.get_series_date_range("SP500")
        assert info is not None
        assert info["count"] == 3
        assert info["min_date"] == "2024-01-02"
        assert info["max_date"] == "2024-12-31"

        assert db.get_series_date_range("NONEXISTENT") is None
        db.close()
        os.unlink(path)

    def test_handles_none_values(self):
        path = self._tmp_db()
        db = DataStore(db_path=path)
        run_id = db.start_run(["X"], None)

        rows = [("X", "2024-01-01", None), ("X", "2024-01-02", "NaN")]
        new, _ = db.upsert_observations(rows, run_id, "D")
        assert new == 2
        db.close()
        os.unlink(path)

    def test_multiple_runs_tracked(self):
        path = self._tmp_db()
        db = DataStore(db_path=path)
        db.start_run(["A"], None, "first")
        db.start_run(["B", "C"], "2020-01-01", "second")
        db.start_run(["D"], None, "third")

        runs = db.get_recent_runs(10)
        assert len(runs) == 3
        assert runs[0]["config_name"] == "third"
        assert runs[2]["config_name"] == "first"
        db.close()
        os.unlink(path)


class TestConfigSaveLoad:
    def test_save_and_load(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        path = save_config(
            name="My Test Config",
            series_ids=["GDP", "UNRATE", "SP500"],
            lookback="2020-01-01",
            mode="popular",
            min_popularity=50,
        )
        assert os.path.exists(path)
        assert path.endswith(".json")

        cfg = load_config(path)
        assert cfg["name"] == "My Test Config"
        assert cfg["series_ids"] == ["GDP", "UNRATE", "SP500"]
        assert cfg["lookback"] == "2020-01-01"
        assert cfg["mode"] == "popular"

    def test_list_configs(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        save_config("Alpha", ["A"], None)
        save_config("Beta", ["B", "C"], "2020-01-01")

        configs = list_configs()
        assert len(configs) >= 2
        names = [c["name"] for c in configs]
        assert "Alpha" in names
        assert "Beta" in names

    def test_list_configs_empty(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        configs = list_configs()
        assert configs == []

    def test_safe_filename(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        path = save_config("Test / with <special> chars!", ["GDP"], None)
        assert os.path.exists(path)
        assert "<" not in os.path.basename(path)
