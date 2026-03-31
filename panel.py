"""
Align lazy_fred CSV outputs (daily / weekly / monthly) to a common calendar
for analysis and correlation.

Conventions (documented for reproducibility):
- Weekly alignment uses ``W-SUN`` (week ending Sunday).
- Monthly uses month-end periods (``ME`` in pandas 2.x).
- Quarterly uses calendar quarter-end (``QE-DEC``).
- Upsampling (e.g. monthly to daily) uses forward-fill unless ``linear``
  interpolation is requested: the observed value is held until the next
  observation.
- Downsampling (e.g. daily to monthly) uses ``last``, ``mean``, or ``sum``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd

Reducer = Literal["last", "mean", "sum"]
UpsampleMethod = Literal["ffill", "linear"]
TargetFreq = Literal["D", "W", "M", "Q"]
NativeFreq = Literal["D", "W", "M", "Q"]
FillMethod = Literal["none", "ffill", "bfill", "ffill_bfill", "interpolate"]

_FREQ_PANDAS: dict[str, str] = {
    "D": "D",
    "W": "W-SUN",
    "M": "ME",
    "Q": "QE-DEC",
}

# Finer frequencies have lower rank (for comparing native vs target).
_FREQ_RANK: dict[str, int] = {"D": 0, "W": 1, "M": 2, "Q": 3}
_PERIOD_FREQ: dict[str, str] = {
    "D": "D",
    "W": "W-SUN",
    "M": "M",
    "Q": "Q-DEC",
}

_META_COLS = [
    "id",
    "title",
    "frequency_short",
    "units_short",
    "popularity",
    "seasonal_adjustment_short",
]


def _normalize_native_freq(raw: object) -> str | None:
    """
    Normalize frequency labels to one of D/W/M/Q.

    Supports short codes (e.g. ``M``) and common long names
    (e.g. ``monthly``).
    """
    if raw is None:
        return None
    text = str(raw).strip().upper()
    if not text:
        return None
    direct = {"D", "W", "M", "Q"}
    if text in direct:
        return text
    aliases = {
        "DAILY": "D",
        "WEEKLY": "W",
        "WEEK": "W",
        "MONTHLY": "M",
        "MONTH": "M",
        "QUARTERLY": "Q",
        "QUARTER": "Q",
    }
    return aliases.get(text)


def read_filtered_metadata(path: str | Path) -> pd.DataFrame:
    """Load ``filtered_series.csv``; return empty frame if missing."""
    path = Path(path)
    if not path.is_file():
        return pd.DataFrame(columns=_META_COLS)
    df = pd.read_csv(path)
    keep = [c for c in _META_COLS if c in df.columns]
    return df[keep] if keep else pd.DataFrame(columns=_META_COLS)


def _read_long_csv(csv_path: Path, native: NativeFreq) -> pd.DataFrame:
    if not csv_path.is_file():
        return pd.DataFrame(columns=["date", "series", "value", "native_freq"])
    df = pd.read_csv(csv_path)
    if df.empty:
        return pd.DataFrame(columns=["date", "series", "value", "native_freq"])
    rename = {}
    if "date" not in df.columns and "index" in df.columns:
        rename["index"] = "date"
    if rename:
        df = df.rename(columns=rename)
    need = {"date", "series", "value"}
    if not need.issubset(df.columns):
        return pd.DataFrame(columns=["date", "series", "value", "native_freq"])
    out = df[["date", "series", "value"]].copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["value"] = pd.to_numeric(out["value"], errors="coerce")
    out = out.dropna(subset=["date", "series"])
    out["series"] = out["series"].astype(str)
    out["native_freq"] = native
    return out


def load_master_long(
    base_dir: str | Path = ".",
    *,
    daily_name: str = "daily_data.csv",
    weekly_name: str = "weekly_data.csv",
    monthly_name: str = "monthly_data.csv",
    filtered_name: str = "filtered_series.csv",
) -> pd.DataFrame:
    """
    Load daily / weekly / monthly long CSVs and join optional metadata
    from ``filtered_series.csv`` (key ``series`` = ``id``).
    """
    base = Path(base_dir)
    parts = [
        _read_long_csv(base / daily_name, "D"),
        _read_long_csv(base / weekly_name, "W"),
        _read_long_csv(base / monthly_name, "M"),
    ]
    master = pd.concat([p for p in parts if not p.empty], ignore_index=True)
    if master.empty:
        return master
    master = master.sort_values(["series", "date"]).drop_duplicates(
        subset=["series", "date"], keep="last"
    )

    meta = read_filtered_metadata(base / filtered_name)
    if not meta.empty and "id" in meta.columns:
        m = meta.rename(columns={"id": "series"})
        master = master.merge(m, on="series", how="left")
    return master.sort_values(["series", "date"]).reset_index(drop=True)


def _compare_freq(native: str, target: str) -> int:
    """Return negative if native is finer than target, 0 if same, positive if coarser."""
    return _FREQ_RANK[native] - _FREQ_RANK[target]


def _align_one_series(
    s: pd.Series,
    native: str,
    target: TargetFreq,
    full_index: pd.DatetimeIndex,
    *,
    reducer: Reducer,
    upsample_method: UpsampleMethod,
) -> pd.Series:
    """Align a single series (DatetimeIndex -> values) to ``full_index``."""
    s = s.sort_index()
    s = s[~s.index.duplicated(keep="last")]

    tgt_pd = _FREQ_PANDAS[target]
    cmp = _compare_freq(native, target)

    if cmp < 0:
        # Native is finer than target: downsample
        how = {"last": "last", "mean": "mean", "sum": "sum"}[reducer]
        out = s.resample(tgt_pd).agg(how)
        out = out.reindex(full_index)
    elif cmp > 0:
        # Native is coarser: upsample to daily grid then downsample if needed
        if target == "D":
            idx = full_index
            aligned = s.reindex(idx)
            if upsample_method == "linear":
                aligned = aligned.interpolate(method="time", limit_direction="both")
            else:
                aligned = aligned.ffill()
            aligned = aligned.bfill()
            out = aligned
        else:
            # Expand to daily between bounds, ffill/linear, then resample to target
            start, end = full_index.min(), full_index.max()
            daily = pd.date_range(start, end, freq="D")
            base = s.reindex(daily)
            if upsample_method == "linear":
                base = base.interpolate(method="time", limit_direction="both")
            else:
                base = base.ffill()
            base = base.bfill()
            how = {"last": "last", "mean": "mean", "sum": "sum"}[reducer]
            out = base.resample(tgt_pd).agg(how)
            out = out.reindex(full_index)
    else:
        # Same nominal frequency: normalize to canonical grid
        how = {"last": "last", "mean": "mean", "sum": "sum"}[reducer]
        out = s.resample(tgt_pd).agg(how)
        out = out.reindex(full_index)

    return out


def build_aligned_panel(
    master_long: pd.DataFrame,
    target_freq: TargetFreq,
    *,
    start: pd.Timestamp | str | None = None,
    end: pd.Timestamp | str | None = None,
    reducer: Reducer = "last",
    upsample_method: UpsampleMethod = "ffill",
    series_ids: list[str] | None = None,
) -> pd.DataFrame:
    """
    Build a wide DataFrame (rows = dates at ``target_freq``, columns = series).

    If ``start`` / ``end`` are omitted, they are taken from the data range.
    """
    if master_long.empty:
        return pd.DataFrame()

    df = master_long.copy()
    if series_ids is not None:
        want = set(series_ids)
        df = df[df["series"].isin(want)]
    if df.empty:
        return pd.DataFrame()

    start_ts = pd.Timestamp(start) if start is not None else df["date"].min()
    end_ts = pd.Timestamp(end) if end is not None else df["date"].max()
    freq = _FREQ_PANDAS[target_freq]
    full_index = pd.date_range(start_ts, end_ts, freq=freq)

    columns = {}
    for sid, g in df.groupby("series"):
        native = g["native_freq"].iloc[0]
        if native not in _FREQ_RANK:
            continue
        ser = pd.Series(
            g["value"].values,
            index=pd.DatetimeIndex(g["date"]),
            dtype="float64",
        )
        columns[sid] = _align_one_series(
            ser,
            native,
            target_freq,
            full_index,
            reducer=reducer,
            upsample_method=upsample_method,
        )

    if not columns:
        return pd.DataFrame(index=full_index)
    wide = pd.DataFrame(columns, index=full_index)
    wide.index.name = "date"
    return wide


def _series_to_daily_even(s: pd.Series, native: str) -> pd.Series:
    """
    Convert one time series to daily values.

    - Daily native values stay on their native dates.
    - Weekly / monthly / quarterly values are treated as period totals and
      distributed evenly across all days in each period.
    """
    s = s.sort_index()
    s = s[~s.index.duplicated(keep="last")]
    s = s.dropna()
    if s.empty:
        return pd.Series(dtype="float64")

    if native == "D":
        out = s.copy()
        out.index = pd.DatetimeIndex(out.index).normalize()
        return out.groupby(level=0).sum().sort_index()

    period_freq = _PERIOD_FREQ[native]
    parts: list[pd.Series] = []
    for ts, value in s.items():
        period = pd.Timestamp(ts).to_period(period_freq)
        start = period.start_time.normalize()
        end = period.end_time.normalize()
        days = pd.date_range(start, end, freq="D")
        if days.empty:
            continue
        per_day = float(value) / float(len(days))
        parts.append(pd.Series(per_day, index=days, dtype="float64"))

    if not parts:
        return pd.Series(dtype="float64")
    out = pd.concat(parts)
    return out.groupby(level=0).sum().sort_index()


def _optimize_for_modeling(
    long_df: pd.DataFrame,
    *,
    as_wide: bool,
    fill_method: FillMethod,
) -> pd.DataFrame:
    """Apply modeling-friendly dtype and memory optimizations."""
    out = long_df.copy()
    if out.empty:
        if as_wide:
            return pd.DataFrame()
        return pd.DataFrame(columns=["date", "series", "value"])

    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["series"] = out["series"].astype(str)
    out["value"] = pd.to_numeric(out["value"], errors="coerce")
    out = out.dropna(subset=["date", "series", "value"])
    out = out.sort_values(["date", "series"]).reset_index(drop=True)

    wide = out.pivot_table(index="date", columns="series", values="value", aggfunc="last")
    wide = wide.sort_index()
    wide.columns.name = "series"
    wide.index.name = "date"

    if fill_method == "ffill":
        wide = wide.ffill()
    elif fill_method == "bfill":
        wide = wide.bfill()
    elif fill_method == "ffill_bfill":
        wide = wide.ffill().bfill()
    elif fill_method == "interpolate":
        wide = wide.interpolate(method="time", limit_direction="both")

    if as_wide:
        cols = list(wide.columns)
        arr = np.ascontiguousarray(wide.to_numpy(dtype=np.float32))
        optimized = pd.DataFrame(arr, index=wide.index, columns=cols)
        optimized.index.name = "date"
        return optimized

    long_out = wide.stack().reset_index(name="value")
    long_out = long_out.dropna(subset=["value"])
    long_out["series"] = long_out["series"].astype("category")
    long_out["value"] = long_out["value"].astype("float32")
    return long_out


def transform_master_timeframe(
    master_long: pd.DataFrame,
    target_freq: TargetFreq,
    *,
    start: pd.Timestamp | str | None = None,
    end: pd.Timestamp | str | None = None,
    reducer: Reducer = "sum",
    series_ids: list[str] | None = None,
    distribute_for_daily: bool = True,
    optimize_for_modeling: bool = True,
    as_wide: bool = False,
    fill_method: FillMethod = "none",
) -> pd.DataFrame:
    """
    Transform mixed-frequency master data to one target timeframe.

    Key behavior:
    - For target ``D`` with ``distribute_for_daily=True``, weekly/monthly/quarterly
      observations are split evenly across all days in each source period.
    - For target ``W`` / ``M`` / ``Q``, all series are rolled up by ``reducer``.

    The returned frame is long format (``date``, ``series``, ``value``) by
    default. Set ``as_wide=True`` for modeling-ready matrix output
    (index=date, columns=series).
    """
    if target_freq not in _FREQ_PANDAS:
        raise ValueError("target_freq must be one of: D, W, M, Q")
    if reducer not in {"last", "mean", "sum"}:
        raise ValueError("reducer must be one of: last, mean, sum")
    if fill_method not in {"none", "ffill", "bfill", "ffill_bfill", "interpolate"}:
        raise ValueError(
            "fill_method must be one of: none, ffill, bfill, ffill_bfill, interpolate"
        )
    if master_long.empty:
        return pd.DataFrame() if as_wide else pd.DataFrame(columns=["date", "series", "value"])

    df = master_long.copy()
    required = {"date", "series", "value"}
    if not required.issubset(df.columns):
        missing = ", ".join(sorted(required - set(df.columns)))
        raise ValueError(f"master_long missing required columns: {missing}")

    if "native_freq" not in df.columns:
        if "frequency_short" in df.columns:
            df["native_freq"] = df["frequency_short"]
        else:
            df["native_freq"] = target_freq

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["series"] = df["series"].astype(str)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["native_freq"] = df["native_freq"].map(_normalize_native_freq)
    df = df.dropna(subset=["date", "series", "value", "native_freq"])

    if series_ids is not None:
        want = set(series_ids)
        df = df[df["series"].isin(want)]
    if df.empty:
        return pd.DataFrame() if as_wide else pd.DataFrame(columns=["date", "series", "value"])

    start_ts = pd.Timestamp(start) if start is not None else df["date"].min()
    end_ts = pd.Timestamp(end) if end is not None else df["date"].max()
    if start_ts > end_ts:
        raise ValueError("start must be <= end")

    if not distribute_for_daily:
        wide = build_aligned_panel(
            df,
            target_freq,
            start=start_ts,
            end=end_ts,
            reducer=reducer,
            upsample_method="ffill",
            series_ids=series_ids,
        )
        long_fallback = wide_to_long(wide)
        if optimize_for_modeling:
            return _optimize_for_modeling(
                long_fallback, as_wide=as_wide, fill_method=fill_method
            )
        if as_wide:
            return wide
        return long_fallback

    daily_idx = pd.date_range(start_ts.normalize(), end_ts.normalize(), freq="D")
    agg = {"last": "last", "mean": "mean", "sum": "sum"}[reducer]

    series_outputs: list[pd.DataFrame] = []
    for sid, g in df.groupby("series"):
        native = str(g["native_freq"].iloc[0])
        s = pd.Series(g["value"].values, index=pd.DatetimeIndex(g["date"]), dtype="float64")
        daily = _series_to_daily_even(s, native)
        if daily.empty:
            continue
        daily = daily.reindex(daily_idx)

        if target_freq == "D":
            values = daily
        else:
            values = daily.resample(_FREQ_PANDAS[target_freq]).agg(agg)

        part = pd.DataFrame({"date": values.index, "series": sid, "value": values.values})
        series_outputs.append(part)

    if not series_outputs:
        return pd.DataFrame() if as_wide else pd.DataFrame(columns=["date", "series", "value"])

    out = pd.concat(series_outputs, ignore_index=True)
    out = out.sort_values(["date", "series"]).reset_index(drop=True)

    if optimize_for_modeling:
        return _optimize_for_modeling(out, as_wide=as_wide, fill_method=fill_method)
    if as_wide:
        return out.pivot(index="date", columns="series", values="value").sort_index()
    return out


def wide_to_long(wide: pd.DataFrame) -> pd.DataFrame:
    """Stack wide panel to long format (date, series, value)."""
    if wide.empty:
        return pd.DataFrame(columns=["date", "series", "value"])
    out = wide.reset_index().melt(
        id_vars=["date"],
        var_name="series",
        value_name="value",
    )
    return out.dropna(subset=["value"])


def write_aligned_master_csv(
    wide: pd.DataFrame,
    path: str | Path,
    *,
    long_format: bool = False,
) -> None:
    """Write aligned panel to CSV (wide by default)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if long_format:
        wide_to_long(wide).to_csv(path, index=False)
    else:
        wide.to_csv(path)


def correlation_matrix(wide: pd.DataFrame, min_periods: int = 2) -> pd.DataFrame:
    """Pairwise Pearson correlation on the wide panel (drops all-NaN columns)."""
    if wide.empty:
        return pd.DataFrame()
    sub = wide.dropna(axis=1, how="all")
    if sub.shape[1] < 2:
        return pd.DataFrame()
    return sub.corr(min_periods=min_periods)
