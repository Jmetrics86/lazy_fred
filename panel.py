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

import pandas as pd

Reducer = Literal["last", "mean", "sum"]
UpsampleMethod = Literal["ffill", "linear"]
TargetFreq = Literal["D", "W", "M", "Q"]
NativeFreq = Literal["D", "W", "M"]

_FREQ_PANDAS: dict[str, str] = {
    "D": "D",
    "W": "W-SUN",
    "M": "ME",
    "Q": "QE-DEC",
}

# Finer frequencies have lower rank (for comparing native vs target).
_FREQ_RANK: dict[str, int] = {"D": 0, "W": 1, "M": 2, "Q": 3}

_META_COLS = [
    "id",
    "title",
    "frequency_short",
    "units_short",
    "popularity",
    "seasonal_adjustment_short",
]


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
