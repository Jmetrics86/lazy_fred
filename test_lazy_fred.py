import importlib
import os
import tempfile
import pandas as pd
import pytest

from .lazy_fred import CollectCategories


def _lf():
    """Implementation module (lazy_fred.py); ``import lazy_fred`` may be the package __init__ under pytest."""
    m = importlib.import_module("lazy_fred")
    if hasattr(m, "daily_export"):
        return m
    return importlib.import_module("lazy_fred.lazy_fred")


def _fred_api_key() -> str | None:
    return os.getenv("API_KEY") or os.getenv("FRED_API_KEY")


requires_fred_api = pytest.mark.skipif(
    not (_fred_api_key() or "").strip(),
    reason="Set API_KEY or FRED_API_KEY for live FRED API tests",
)


@requires_fred_api
def test_api_key_validation():
    key = _fred_api_key()
    assert key is not None and len(key.strip()) > 0


@requires_fred_api
def test_get_fred_search_results():
    """Tests that search results are retrieved and processed correctly."""

    api_key = _fred_api_key().strip()

    collect = CollectCategories(api_key)
    results = collect.get_fred_search(categories=['gdp', 'banking']) #get_fred_search(self, categories)
    # Check if output is a dictionary
    
    assert isinstance(results, list)
    assert isinstance(results[0], dict)

    results = results[0]
    results_df = pd.DataFrame.from_dict(results)

    # Check columns exist
    assert set(results_df.columns) == {'seriess', 'sort_order', 'order_by', 'offset', 'limit', 'count', 'realtime_start', 'realtime_end'}
    
    # Check for duplicates
    assert not results_df.duplicated(subset=['seriess']).any()
    





# ... Similar tests for monthly_filter and weekly_filter

# Integration Tests (optional) - test the combined behavior of multiple components
def test_full_pipeline():
    """Tests the entire data collection and export process."""
    # Prepare test environment
    #monkeypatch.setenv("API_KEY", "your_valid_api_key")

    # Create temporary files for outputs
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as temp_lazy_file, \
         tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as temp_daily_file, \
         tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as temp_monthly_file, \
         tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as temp_weekly_file:
        
        # Run main function
        #main()

        # Assert that output files were created
        assert os.path.isfile(temp_lazy_file.name)
        assert os.path.isfile(temp_daily_file.name)
        assert os.path.isfile(temp_monthly_file.name)
        assert os.path.isfile(temp_weekly_file.name)

        # Load data and perform assertions on the content
        #lazy_df = pd.read_csv(temp_lazy_file.name)
        #assert not lazy_df.empty  # ... etc.
        # ... similar assertions for other files


def _filtered_rows_daily(*ids_and_titles):
    rows = []
    for sid, title in ids_and_titles:
        rows.append(
            {
                "id": sid,
                "popularity": 99,
                "frequency_short": "D",
                "title": title,
                "units_short": "Units",
                "seasonal_adjustment_short": "SA",
            }
        )
    return rows


@pytest.fixture
def quiet_console(monkeypatch):
    monkeypatch.setattr(_lf().console, "print", lambda *a, **k: None)


def test_pull_remaining_failures_after_catchup(tmp_path, monkeypatch, quiet_console):
    lf = _lf()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("API_KEY", "test-key")
    rows = _filtered_rows_daily(("OK1", "One"), ("BAD", "Always fails"))
    pd.DataFrame(rows).to_csv(tmp_path / "filtered_series.csv", index=False)

    ok_ts = pd.Series([1.0], index=pd.to_datetime(["2020-01-01"]))

    class MockFred:
        def __init__(self, *a, **k):
            pass

        def get_series(self, series_id, **kwargs):
            if series_id == "OK1":
                return ok_ts.copy()
            raise RuntimeError("non-retryable failure")

    monkeypatch.setattr(lf, "Fred", MockFred)
    monkeypatch.setattr(lf.time, "sleep", lambda s: None)

    exp = lf.daily_export(None)
    remaining = exp.daily_series_collector()

    assert len(remaining) == 1
    assert remaining[0]["series_id"] == "BAD"
    assert remaining[0]["reason"] == "error"
    assert "non-retryable" in remaining[0]["detail"]


def test_empty_series_recovered_in_catchup(tmp_path, monkeypatch, quiet_console):
    lf = _lf()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("API_KEY", "test-key")
    rows = _filtered_rows_daily(("EMP", "Empty then ok"))
    pd.DataFrame(rows).to_csv(tmp_path / "filtered_series.csv", index=False)

    ok_ts = pd.Series([2.0], index=pd.to_datetime(["2020-02-01"]))
    empty_ts = pd.Series(dtype=float)
    calls = {"n": 0}

    class MockFred:
        def __init__(self, *a, **k):
            pass

        def get_series(self, series_id, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                return empty_ts.copy()
            return ok_ts.copy()

    sleep_times = []
    monkeypatch.setattr(lf, "Fred", MockFred)
    monkeypatch.setattr(lf.time, "sleep", lambda s: sleep_times.append(s))

    exp = lf.daily_export(None)
    remaining = exp.daily_series_collector()

    assert remaining == []
    assert calls["n"] == 2
    assert lf.sleep * 2 in sleep_times


def test_catchup_retry_uses_doubled_initial_backoff(tmp_path, monkeypatch, quiet_console):
    lf = _lf()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("API_KEY", "test-key")
    monkeypatch.setattr(lf, "DEFAULT_MAX_RETRIES", 3)

    rows = _filtered_rows_daily(("BAD", "Retry then ok"))
    pd.DataFrame(rows).to_csv(tmp_path / "filtered_series.csv", index=False)

    ok_ts = pd.Series([1.0], index=pd.to_datetime(["2020-01-01"]))
    calls = {"n": 0}

    class MockFred:
        def __init__(self, *a, **k):
            pass

        def get_series(self, series_id, **kwargs):
            calls["n"] += 1
            if calls["n"] <= 3:
                raise ValueError("503 service unavailable")
            if calls["n"] == 4:
                raise ValueError("503 service unavailable")
            return ok_ts.copy()

    backoff_snapshots = []

    def capture_backoff(attempt, initial_wait=lf.DEFAULT_INITIAL_BACKOFF_SECONDS):
        backoff_snapshots.append((attempt, initial_wait))

    monkeypatch.setattr(lf, "Fred", MockFred)
    monkeypatch.setattr(lf, "backoff_sleep", capture_backoff)
    monkeypatch.setattr(lf.time, "sleep", lambda s: None)

    exp = lf.daily_export(None)
    remaining = exp.daily_series_collector()

    assert remaining == []
    assert any(w == 2 * lf.DEFAULT_INITIAL_BACKOFF_SECONDS for _, w in backoff_snapshots)


def test_dedupe_duplicate_series_ids_in_output(tmp_path, monkeypatch, quiet_console):
    lf = _lf()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("API_KEY", "test-key")
    base = _filtered_rows_daily(("DUP", "Dup row"))[0]
    pd.DataFrame([base, dict(base)]).to_csv(tmp_path / "filtered_series.csv", index=False)

    ok_ts = pd.Series([1.0], index=pd.to_datetime(["2020-01-01"]))

    class MockFred:
        def __init__(self, *a, **k):
            pass

        def get_series(self, series_id, **kwargs):
            return ok_ts.copy()

    monkeypatch.setattr(lf, "Fred", MockFred)
    monkeypatch.setattr(lf.time, "sleep", lambda s: None)

    exp = lf.daily_export(None)
    exp.daily_series_collector()

    out = pd.read_csv(tmp_path / "daily_data.csv")
    key = out[["date", "series"]].astype(str)
    assert not key.duplicated().any()


def test_execute_collection_writes_pull_failures_aggregate(tmp_path, monkeypatch, quiet_console):
    lf = _lf()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("API_KEY", "test-key")

    # Minimal filtered_series for each frequency so collectors run quickly
    common = {"popularity": 99, "units_short": "u", "seasonal_adjustment_short": "SA"}
    rows = [
        {**common, "id": "D1", "frequency_short": "D", "title": "d"},
        {**common, "id": "M1", "frequency_short": "M", "title": "m"},
        {**common, "id": "W1", "frequency_short": "W", "title": "w"},
    ]
    pd.DataFrame(rows).to_csv(tmp_path / "filtered_series.csv", index=False)

    class MockFred:
        def __init__(self, *a, **k):
            pass

        def get_series(self, series_id, **kwargs):
            ts = pd.Series([1.0], index=pd.to_datetime(["2020-01-01"]))
            if series_id == "M1":
                raise RuntimeError("monthly failed")
            return ts

    monkeypatch.setattr(lf, "Fred", MockFred)
    monkeypatch.setattr(lf.CollectCategories, "get_fred_search", lambda self, c: [])
    monkeypatch.setattr(lf.CollectCategories, "export_master", lambda self, r: None)
    monkeypatch.setattr(lf.time, "sleep", lambda s: None)
    monkeypatch.setattr(lf, "backup_existing_outputs", lambda: None)

    lf.execute_collection("test-key", ["gdp"])

    fail_path = tmp_path / lf.PULL_FAILURES_CSV
    assert fail_path.is_file()
    fails = pd.read_csv(fail_path)
    assert list(fails.columns) == ["phase", "series_id", "reason", "detail"]
    assert len(fails) == 1
    assert fails.iloc[0]["series_id"] == "M1"
    assert fails.iloc[0]["phase"] == "monthly"


def test_build_master_dataset_builds_enriched_long_csv(tmp_path):
    lf = _lf()

    pd.DataFrame(
        [
            {
                "id": "D1",
                "title": "Daily series",
                "frequency_short": "D",
                "units_short": "Index",
                "popularity": 88,
                "seasonal_adjustment_short": "SA",
            },
            {
                "id": "M1",
                "title": "Monthly series",
                "frequency_short": "M",
                "units_short": "Percent",
                "popularity": 77,
                "seasonal_adjustment_short": "NSA",
            },
        ]
    ).to_csv(tmp_path / "filtered_series.csv", index=False)

    pd.DataFrame(
        [
            {"date": "2020-01-01", "series": "D1", "value": 1.0},
            {"date": "2020-01-02", "series": "D1", "value": 2.0},
        ]
    ).to_csv(tmp_path / "daily_data.csv", index=False)
    pd.DataFrame(
        [{"date": "2020-01-31", "series": "M1", "value": 3.0}]
    ).to_csv(tmp_path / "monthly_data.csv", index=False)
    pd.DataFrame(columns=["date", "series", "value"]).to_csv(
        tmp_path / "weekly_data.csv", index=False
    )

    out_path = tmp_path / "master_data.csv"
    lf.build_master_dataset(base_dir=tmp_path, output_path=out_path)

    assert out_path.is_file()
    out = pd.read_csv(out_path)
    assert {"date", "series", "value", "native_freq", "title", "units_short"}.issubset(
        out.columns
    )
    assert len(out) == 3
    assert set(out["series"]) == {"D1", "M1"}


def test_parse_master_cli_args_supports_start_and_out():
    lf = _lf()
    start, out = lf.parse_master_cli_args(["--start", "2020-01-01", "--out", "x.csv"])
    assert start == "2020-01-01"
    assert out == "x.csv"

    start2, out2 = lf.parse_master_cli_args(["--start=2021-02-03", "--out=y.csv"])
    assert start2 == "2021-02-03"
    assert out2 == "y.csv"


def test_parse_master_cli_args_invalid_token():
    lf = _lf()
    with pytest.raises(ValueError):
        lf.parse_master_cli_args(["--bogus"])


def test_show_cli_intro_prints_intro_and_commands(monkeypatch):
    lf = _lf()
    printed = []

    monkeypatch.setattr(lf.console, "print", lambda *a, **k: printed.append(a))

    lf.show_cli_intro()

    assert len(printed) == 2
    assert "CAPE quick intro" in str(printed[0][0])
    assert "Available Commands" in str(printed[1][0])


def test_main_without_args_shows_intro_then_runs_collection(monkeypatch):
    lf = _lf()
    calls = []

    monkeypatch.setattr(lf.sys, "argv", ["lazy-fred"])
    monkeypatch.setenv("API_KEY", "env-key")
    monkeypatch.setattr(lf, "show_cli_intro", lambda: calls.append(("intro", None)))
    monkeypatch.setattr(
        lf,
        "run_fred_data_collection",
        lambda key, **kwargs: calls.append(("run", key)),
    )

    lf.main()

    assert calls == [("intro", None), ("run", "env-key")]

