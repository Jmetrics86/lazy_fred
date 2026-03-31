"""Unit tests for panel alignment (no dashboard/streamlit imports)."""

from pathlib import Path

import pandas as pd

from panel import (
    build_aligned_panel,
    correlation_matrix,
    load_master_long,
    transform_master_timeframe,
    wide_to_long,
    write_aligned_master_csv,
)


def test_monthly_to_daily_ffill():
    master = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-01-31", "2020-02-29"]),
            "series": ["M1", "M1"],
            "value": [10.0, 20.0],
            "native_freq": ["M", "M"],
        }
    )
    wide = build_aligned_panel(
        master,
        "D",
        start="2020-01-15",
        end="2020-03-05",
        reducer="last",
        upsample_method="ffill",
    )
    assert not wide.empty
    jan = wide.loc["2020-01-20":"2020-01-30", "M1"].dropna()
    assert jan.iloc[0] == 10.0
    feb = wide.loc["2020-02-15", "M1"]
    assert feb == 10.0
    assert wide.loc["2020-03-01", "M1"] == 20.0


def test_daily_to_monthly_last():
    master = pd.DataFrame(
        {
            "date": pd.to_datetime(
                ["2020-01-02", "2020-01-15", "2020-01-30", "2020-02-05", "2020-02-28"]
            ),
            "series": ["D1"] * 5,
            "value": [1.0, 2.0, 3.0, 4.0, 5.0],
            "native_freq": ["D"] * 5,
        }
    )
    wide = build_aligned_panel(
        master,
        "M",
        start="2020-01-01",
        end="2020-02-29",
        reducer="last",
        upsample_method="ffill",
    )
    assert not wide.empty
    jan_end = pd.Timestamp("2020-01-31")
    if jan_end in wide.index:
        assert wide.loc[jan_end, "D1"] == 3.0


def test_load_master_long_from_csvs(tmp_path: Path):
    d = tmp_path / "daily_data.csv"
    pd.DataFrame(
        {"date": ["2020-01-01"], "series": ["S"], "value": [1.0]}
    ).to_csv(d, index=False)
    m = tmp_path / "monthly_data.csv"
    pd.DataFrame(
        {"date": ["2020-02-01"], "series": ["T"], "value": [2.0]}
    ).to_csv(m, index=False)
    w = tmp_path / "weekly_data.csv"
    pd.DataFrame(columns=["date", "series", "value"]).to_csv(w, index=False)

    master = load_master_long(tmp_path)
    assert len(master) == 2
    assert set(master["native_freq"]) == {"D", "M"}


def test_correlation_matrix_min_periods():
    wide = pd.DataFrame(
        {"a": [1.0, 2.0, 3.0], "b": [2.0, 4.0, 6.0]},
        index=pd.date_range("2020-01-01", periods=3, freq="D"),
    )
    corr = correlation_matrix(wide)
    assert corr.shape == (2, 2)
    assert abs(corr.loc["a", "b"] - 1.0) < 1e-9


def test_wide_to_long_roundtrip(tmp_path: Path):
    wide = pd.DataFrame(
        {"x": [1.0, 2.0], "y": [3.0, 4.0]},
        index=pd.date_range("2020-01-01", periods=2, freq="D"),
    )
    wide.index.name = "date"
    long_df = wide_to_long(wide)
    assert set(long_df.columns) == {"date", "series", "value"}
    assert len(long_df) == 4
    p = tmp_path / "out.csv"
    write_aligned_master_csv(wide, p, long_format=True)
    assert p.is_file()


def test_transform_master_timeframe_daily_even_allocation():
    master = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-01-31"]),
            "series": ["M1"],
            "value": [310.0],
            "native_freq": ["M"],
        }
    )

    daily = transform_master_timeframe(
        master,
        target_freq="D",
        start="2020-01-01",
        end="2020-01-31",
        distribute_for_daily=True,
        optimize_for_modeling=False,
    )

    assert len(daily) == 31
    assert daily["value"].notna().sum() == 31
    assert abs(daily["value"].iloc[0] - 10.0) < 1e-9
    assert abs(daily["value"].sum() - 310.0) < 1e-9


def test_transform_master_timeframe_rollup_week_month_quarter():
    dates = pd.date_range("2020-01-01", "2020-03-31", freq="D")
    master = pd.DataFrame(
        {
            "date": dates,
            "series": ["D1"] * len(dates),
            "value": [1.0] * len(dates),
            "native_freq": ["D"] * len(dates),
        }
    )

    weekly = transform_master_timeframe(
        master,
        target_freq="W",
        reducer="sum",
        optimize_for_modeling=False,
    )
    monthly = transform_master_timeframe(
        master,
        target_freq="M",
        reducer="sum",
        optimize_for_modeling=False,
    )
    quarterly = transform_master_timeframe(
        master,
        target_freq="Q",
        reducer="sum",
        optimize_for_modeling=False,
    )

    assert abs(weekly["value"].sum() - 91.0) < 1e-9
    jan = monthly[monthly["date"] == pd.Timestamp("2020-01-31")]["value"].iloc[0]
    feb = monthly[monthly["date"] == pd.Timestamp("2020-02-29")]["value"].iloc[0]
    mar = monthly[monthly["date"] == pd.Timestamp("2020-03-31")]["value"].iloc[0]
    assert jan == 31.0 and feb == 29.0 and mar == 31.0
    q1 = quarterly[quarterly["date"] == pd.Timestamp("2020-03-31")]["value"].iloc[0]
    assert q1 == 91.0


def test_transform_master_timeframe_optimize_for_modeling():
    master = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-01-01", "2020-01-02"]),
            "series": ["S1", "S1"],
            "value": [1.0, 2.0],
            "native_freq": ["D", "D"],
        }
    )
    out = transform_master_timeframe(master, target_freq="D")

    assert list(out.columns) == ["date", "series", "value"]
    assert str(out["series"].dtype) == "category"
    assert str(out["value"].dtype) == "float32"
    assert pd.api.types.is_datetime64_any_dtype(out["date"])
