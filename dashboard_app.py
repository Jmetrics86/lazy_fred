"""
Streamlit dashboard for aligned lazy_fred panels.

Run via: lazy-fred-dashboard
Or: streamlit run path/to/dashboard_app.py
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import streamlit as st
except ImportError as e:
    raise ImportError(
        "Dashboard requires streamlit. Install with: pip install 'lazy_fred[dashboard]'"
    ) from e

try:
    import plotly.express as px
    import plotly.graph_objects as go
except ImportError as e:
    raise ImportError(
        "Dashboard requires plotly. Install with: pip install 'lazy_fred[dashboard]'"
    ) from e

from panel import (
    build_aligned_panel,
    correlation_matrix,
    load_master_long,
    write_aligned_master_csv,
)


def _sidebar_paths() -> Path:
    st.sidebar.header("Data paths")
    base = st.sidebar.text_input(
        "Working directory",
        value=os.getcwd(),
        help="Directory containing daily_data.csv, weekly_data.csv, monthly_data.csv",
    )
    return Path(base).expanduser().resolve()


def _sidebar_options():
    st.sidebar.header("Alignment")
    target = st.sidebar.selectbox(
        "Target frequency",
        options=["D", "W", "M", "Q"],
        format_func=lambda x: {"D": "Daily", "W": "Weekly", "M": "Monthly", "Q": "Quarterly"}[x],
    )
    reducer = st.sidebar.selectbox(
        "Downsample aggregation",
        options=["last", "mean", "sum"],
        help="When a series is finer than the target calendar (e.g. daily → monthly).",
    )
    upsample = st.sidebar.selectbox(
        "Upsample method",
        options=["ffill", "linear"],
        help="When a series is coarser than the target (e.g. monthly → daily).",
    )
    use_dates = st.sidebar.checkbox("Limit date range", value=False)
    start = end = None
    if use_dates:
        start = pd.Timestamp(
            st.sidebar.date_input("Start", value=date(2000, 1, 1))
        )
        end = pd.Timestamp(st.sidebar.date_input("End", value=date.today()))
    normalize = st.sidebar.selectbox(
        "Series display scale",
        ["levels", "index_100", "yoy_pct"],
        help="index_100: rebase to 100 at first valid point; yoy_pct: year-over-year % change.",
    )
    return target, reducer, upsample, start, end, normalize


def _apply_display_scale(wide: pd.DataFrame, mode: str, target_freq: str) -> pd.DataFrame:
    if wide.empty or mode == "levels":
        return wide
    out = wide.copy()
    if mode == "index_100":
        for c in out.columns:
            s = out[c].dropna()
            if s.empty:
                continue
            base = s.iloc[0]
            if base and base != 0:
                out[c] = out[c] / base * 100.0
    elif mode == "yoy_pct":
        periods = {"D": 252, "W": 52, "M": 12, "Q": 4}.get(target_freq, 12)
        out = out.pct_change(periods=periods) * 100.0
    return out


def run_streamlit_app():
    st.set_page_config(page_title="lazy_fred analysis", layout="wide")
    st.title("lazy_fred — aligned panel dashboard")

    base = _sidebar_paths()
    target, reducer, upsample, start, end, normalize = _sidebar_options()

    if not (base / "daily_data.csv").is_file() and not (base / "monthly_data.csv").is_file():
        st.warning(
            f"No daily_data.csv or monthly_data.csv found under `{base}`. "
            "Run a collection first (e.g. `lazy-fred quick`)."
        )

    master = load_master_long(base)
    if master.empty:
        st.error("No data loaded. Check CSV paths and file contents.")
        st.stop()

    series_list = sorted(master["series"].unique())
    selected = st.sidebar.multiselect(
        "Series to include",
        options=series_list,
        default=series_list[: min(8, len(series_list))],
    )
    if not selected:
        st.error("Select at least one series.")
        st.stop()

    wide = build_aligned_panel(
        master,
        target_freq=target,
        start=start,
        end=end,
        reducer=reducer,
        upsample_method=upsample,
        series_ids=selected,
    )
    if wide.empty:
        st.error("Aligned panel is empty for this selection.")
        st.stop()

    out_path = base / "aligned_master.csv"
    if st.sidebar.button("Save aligned_master.csv (wide)"):
        write_aligned_master_csv(wide, out_path, long_format=False)
        st.sidebar.success(f"Wrote {out_path}")

    display_wide = _apply_display_scale(wide, normalize, target)

    tab_ts, tab_corr, tab_scatter = st.tabs(["Time series", "Correlation", "Scatter"])

    with tab_ts:
        dfl = display_wide.reset_index().melt(
            id_vars=["date"], var_name="series", value_name="value"
        )
        dfl = dfl.dropna(subset=["value"])
        fig = px.line(dfl, x="date", y="value", color="series")
        fig.update_layout(height=560, legend=dict(orientation="h", yanchor="bottom", y=-0.35))
        st.plotly_chart(fig, use_container_width=True)

    with tab_corr:
        corr = correlation_matrix(wide)
        if corr.empty:
            st.info("Need at least two series with overlapping data for correlation.")
        else:
            fig = go.Figure(
                data=go.Heatmap(
                    z=corr.values,
                    x=corr.columns.tolist(),
                    y=corr.index.tolist(),
                    colorscale="RdBu",
                    zmid=0,
                    zmin=-1,
                    zmax=1,
                )
            )
            fig.update_layout(height=600, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

    with tab_scatter:
        if len(selected) < 2:
            st.info("Select at least two series for scatter.")
        else:
            a = st.selectbox("X series", options=selected, key="sx")
            y_choices = [s for s in selected if s != a]
            if not y_choices:
                st.info("Pick a different X series.")
            else:
                b = st.selectbox("Y series", options=y_choices, key="sy")
                df = wide[[a, b]].dropna()
                if df.empty:
                    st.warning("No overlapping points for this pair.")
                else:
                    fig = px.scatter(df, x=a, y=b)
                    if len(df) >= 2:
                        xs = df[a].to_numpy(dtype=float)
                        ys = df[b].to_numpy(dtype=float)
                        slope, intercept = np.polyfit(xs, ys, 1)
                        xline = np.array([xs.min(), xs.max()])
                        fig.add_trace(
                            go.Scatter(
                                x=xline,
                                y=slope * xline + intercept,
                                mode="lines",
                                name="OLS fit",
                                line=dict(color="red", dash="dash"),
                            )
                        )
                    fig.update_layout(height=500)
                    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Loaded rows (long, sample)"):
        st.dataframe(master[master["series"].isin(selected)].head(50))


def main() -> None:
    """CLI entry: launch Streamlit with this file."""
    app = Path(__file__).resolve()
    rc = subprocess.call([sys.executable, "-m", "streamlit", "run", str(app)])
    raise SystemExit(rc)


if __name__ == "__main__":
    run_streamlit_app()
