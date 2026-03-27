"""Unit tests for wizard.py helper functions and classes."""

import os
import tempfile
import time

import pandas as pd

from wizard import (
    _fmt_duration,
    _estimate_download,
    _is_valid_date,
    _is_date_after_1900,
    _RateLimiter,
    _fetch_one_series,
    ErrorLog,
    POPULAR_SERIES,
    KITCHEN_SINK_CATEGORIES,
    FREQUENCY_LABELS,
)


# ── _fmt_duration ─────────────────────────────────────────────────────────────

class TestFmtDuration:
    def test_seconds(self):
        assert _fmt_duration(5) == "~5 sec"

    def test_one_second_minimum(self):
        assert _fmt_duration(0.3) == "~1 sec"

    def test_one_minute(self):
        assert _fmt_duration(90) == "~1 min"

    def test_several_minutes(self):
        result = _fmt_duration(300)
        assert result == "~5 min"

    def test_rounds_up_minutes(self):
        result = _fmt_duration(130)
        assert result.startswith("~")
        assert "min" in result

    def test_hours(self):
        result = _fmt_duration(7200)
        assert "hr" in result

    def test_zero(self):
        assert _fmt_duration(0) == "~1 sec"


# ── _estimate_download ────────────────────────────────────────────────────────

class TestEstimateDownload:
    def test_returns_string(self):
        result = _estimate_download(10)
        assert isinstance(result, str)
        assert result.startswith("~")

    def test_scales_with_count(self):
        small = _estimate_download(5)
        large = _estimate_download(500)
        assert "sec" in small or "min" in small
        assert "min" in large or "hr" in large

    def test_zero_series(self):
        result = _estimate_download(0)
        assert result == "~1 sec"


# ── _is_valid_date ────────────────────────────────────────────────────────────

class TestIsValidDate:
    def test_valid_date(self):
        assert _is_valid_date("2024-01-15") is True

    def test_invalid_format(self):
        assert _is_valid_date("01-15-2024") is False

    def test_invalid_string(self):
        assert _is_valid_date("not-a-date") is False

    def test_empty_string(self):
        assert _is_valid_date("") is False

    def test_leap_day(self):
        assert _is_valid_date("2024-02-29") is True

    def test_impossible_date(self):
        assert _is_valid_date("2023-02-29") is False


# ── _is_date_after_1900 ──────────────────────────────────────────────────────

class TestIsDateAfter1900:
    def test_modern_date(self):
        assert _is_date_after_1900("2020-06-15") is True

    def test_exactly_1900(self):
        assert _is_date_after_1900("1900-01-01") is True

    def test_before_1900(self):
        assert _is_date_after_1900("1899-12-31") is False

    def test_very_old_date(self):
        assert _is_date_after_1900("1776-07-04") is False

    def test_empty_string(self):
        assert _is_date_after_1900("") is False

    def test_none(self):
        assert _is_date_after_1900(None) is False

    def test_malformed(self):
        assert _is_date_after_1900("garbage") is False


# ── ErrorLog ──────────────────────────────────────────────────────────────────

class TestErrorLog:
    def test_empty_log(self):
        log = ErrorLog()
        assert log.count == 0
        assert log.write_csv(os.path.join(tempfile.gettempdir(),
                                          "empty.csv")) is None

    def test_add_and_count(self):
        log = ErrorLog()
        log.add("download", "GDP", "ValueError", "test error")
        log.add("metadata", "UNRATE", "TimeoutError", "timed out")
        assert log.count == 2

    def test_write_csv(self):
        log = ErrorLog()
        log.add("download", "GDP", "ValueError", "some error msg")
        log.add("download", "SP500", "ValueError", "another error")
        log.add("metadata", "UNRATE", "TimeoutError", "timed out")

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv",
                                         delete=False) as f:
            path = f.name

        result = log.write_csv(path)
        assert result == path

        df = pd.read_csv(path)
        assert len(df) == 3
        assert set(df.columns) == {"timestamp", "phase", "series_id",
                                    "error_type", "message"}
        assert df["phase"].tolist() == ["download", "download", "metadata"]
        os.unlink(path)

    def test_summary_table(self):
        log = ErrorLog()
        log.add("download", "A", "ValueError", "err")
        log.add("download", "B", "ValueError", "err")
        log.add("download", "C", "TimeoutError", "err")

        summary = log.summary_table()
        assert summary == {"ValueError": 2, "TimeoutError": 1}

    def test_message_truncation(self):
        log = ErrorLog()
        long_msg = "x" * 1000
        log.add("test", "ID", "Error", long_msg)
        assert len(log._rows[0]["message"]) <= 300


# ── _RateLimiter ──────────────────────────────────────────────────────────────

class TestRateLimiter:
    def test_enforces_interval(self):
        limiter = _RateLimiter(max_per_second=10.0)
        t0 = time.monotonic()
        for _ in range(5):
            limiter.wait()
        elapsed = time.monotonic() - t0
        # Timing jitter on CI/Windows can be slightly under ideal math.
        assert elapsed >= 0.35

    def test_first_call_immediate(self):
        limiter = _RateLimiter(max_per_second=2.0)
        t0 = time.monotonic()
        limiter.wait()
        elapsed = time.monotonic() - t0
        assert elapsed < 0.6


# ── POPULAR_SERIES structure ──────────────────────────────────────────────────

class TestPopularSeries:
    def test_all_have_three_fields(self):
        for cat, series_list in POPULAR_SERIES.items():
            for entry in series_list:
                assert len(entry) == 3, f"Bad entry in {cat}: {entry}"
                sid, desc, freq = entry
                assert isinstance(sid, str) and len(sid) > 0
                assert isinstance(desc, str) and len(desc) > 0
                assert freq in FREQUENCY_LABELS, (
                    f"Unknown freq '{freq}' for {sid}")

    def test_no_duplicate_series_ids(self):
        all_ids = []
        for series_list in POPULAR_SERIES.values():
            for sid, _, _ in series_list:
                all_ids.append(sid)
        assert len(all_ids) == len(set(all_ids)), "Duplicate series IDs"


class TestKitchenSinkCategories:
    def test_has_30_categories(self):
        assert len(KITCHEN_SINK_CATEGORIES) == 30

    def test_all_lowercase(self):
        for cat in KITCHEN_SINK_CATEGORIES:
            assert cat == cat.lower(), f"Category not lowercase: {cat}"

    def test_no_duplicates(self):
        assert len(KITCHEN_SINK_CATEGORIES) == len(set(KITCHEN_SINK_CATEGORIES))


# ── _fetch_one_series (pandas timestamp handling) ─────────────────────────────

class TestFetchOneSeriesTimestamp:
    """Test that _fetch_one_series handles old dates gracefully."""

    def test_normal_series_wraps_to_dataframe(self):
        idx = pd.date_range("2020-01-01", periods=5, freq="D")
        raw_series = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0], index=idx)

        class FakeFred:
            def get_series(self, sid, **kw):
                return raw_series

        result = _fetch_one_series(FakeFred(), "TEST", None)
        assert result is not None
        assert "series" in result.columns
        assert len(result) == 5
        assert (result["series"] == "TEST").all()

    def test_returns_none_on_persistent_error(self):
        class FailFred:
            def get_series(self, sid, **kw):
                raise RuntimeError("Something broke")

        result = _fetch_one_series(FailFred(), "BAD", None)
        assert result is None
