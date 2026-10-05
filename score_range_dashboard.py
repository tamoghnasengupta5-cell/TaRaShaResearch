"""Year-range comparison and company drilldown for the combined debt-adjusted score."""
from dataclasses import dataclass
import re
from typing import Mapping, Optional

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

from ui_theme import dashboard_section, table_height


TITLE = "Debt-Adjusted Score Across Year Ranges"
DEBT_TOTAL = "Total Debt-Adjusted Scaled Volatility-Adjusted Score"
COVERAGE = "Included Years for Calculation"


@dataclass(frozen=True)
class ScoreWindow:
    label: str
    first_year: int


def component_recent_years(series: Mapping[str, pd.DataFrame]) -> dict:
    """Resolve Recent using exactly the same source anchors as the existing dashboard."""
    groups = {
        "BS": ("accumulated_profit", "roe", "roce", "interest_load_pct"),
        "PL": ("revenue",),
        "FS": ("fcff", "spread_pct"),
    }
    result = {}
    for group, names in groups.items():
        years = []
        for name in names:
            frame = series.get(name)
            if frame is not None and not frame.empty and "year" in frame:
                years.extend(pd.to_numeric(frame["year"], errors="coerce").dropna().tolist())
        result[group] = int(max(years)) if years else None
    return result


def score_windows(year_range: str, latest_year: Optional[int]) -> list[ScoreWindow]:
    """One inclusive window per lower bound, through the latest scoring fiscal year."""
    value = (year_range or "").strip()
    recent = re.fullmatch(r"recent\s*[-–—]\s*(\d{4})", value, flags=re.IGNORECASE)
    explicit = re.fullmatch(r"(\d{4})\s*[-–—]\s*(\d{4})", value)
    if recent:
        if latest_year is None:
            return []
        first, last = int(recent.group(1)), latest_year
        prefix = "Recent"
    elif explicit:
        first, last = sorted(map(int, explicit.groups()))
        prefix = str(last)
    else:
        raise ValueError("Could not parse the year range. Use 'Recent - YYYY' or 'YYYY-YYYY'.")
    return [ScoreWindow(f"{prefix} - {year}", year) for year in range(first, last + 1)]


def build_score_range_frame(
    company_rows: list[dict],
    dashboard_series: Mapping[int, Mapping[str, pd.DataFrame]],
    year_range: str,
    base_results: Mapping[int, dict],
    calculate,
) -> tuple[pd.DataFrame, list[ScoreWindow], dict]:
    """Reuse the selected-window result and recalculate other ranges from the same batch."""
    bounds = {row["_company_id"]: component_recent_years(dashboard_series.get(row["_company_id"], {}))
              for row in company_rows}
    latest = max((year for group in bounds.values() for year in group.values() if year is not None), default=None)
    windows = score_windows(year_range, latest)
    rows, coverage = [], {}
    is_recent = bool(re.match(r"^recent", year_range.strip(), flags=re.IGNORECASE))
    for row in company_rows:
        cid = row["_company_id"]
        base = base_results[cid]
        item = {"_company_id": cid, "Company": row["Company Name"], "Ticker": row["Ticker"],
                "Included Years": base["breakup"][COVERAGE]}
        coverage[cid] = {}
        for index, window in enumerate(windows):
            # A company with older data must not acquire a reversed/future window.
            outside_history = is_recent and any(
                year is not None and window.first_year > year for year in bounds[cid].values()
            )
            if outside_history:
                item[window.label] = None
                coverage[cid][window.label] = None
                continue
            result = base if index == 0 else calculate(cid, window.label)
            item[window.label] = result["scores"][DEBT_TOTAL]
            coverage[cid][window.label] = result["breakup"][COVERAGE]
        rows.append(item)
    frame = pd.DataFrame(rows, columns=["_company_id", "Company", "Ticker", "Included Years", *[w.label for w in windows]])
    if windows and not frame.empty:
        frame = frame.sort_values(windows[0].label, ascending=False, na_position="last", kind="stable")
    return frame.reset_index(drop=True), windows, coverage


def company_chart_frame(row: pd.Series, windows: list[ScoreWindow], coverage: Mapping[str, str]) -> pd.DataFrame:
    """Keep unavailable observations missing and fit a line only to finite scores."""
    chart = pd.DataFrame({
        "Year Range": [w.label for w in windows],
        "Start Year": [w.first_year for w in windows],
        "Debt-Adj. Total Score": [row[w.label] for w in windows],
        "Included Years": [coverage.get(w.label) or "—" for w in windows],
    })
    chart["Debt-Adj. Total Score"] = pd.to_numeric(chart["Debt-Adj. Total Score"], errors="coerce")
    chart.loc[~np.isfinite(chart["Debt-Adj. Total Score"]), "Debt-Adj. Total Score"] = np.nan
    chart["Trend"] = np.nan
    valid = chart["Debt-Adj. Total Score"].notna()
    if valid.sum() >= 2:
        x = chart.loc[valid, "Start Year"].to_numpy(dtype=float)
        y = chart.loc[valid, "Debt-Adj. Total Score"].to_numpy(dtype=float)
        scale = float(np.max(np.abs(y))) or 1.0
        centered = x - x.mean()
        coefficients = np.linalg.lstsq(np.column_stack([centered, np.ones(len(x))]), y / scale, rcond=None)[0]
        with np.errstate(over="ignore", invalid="ignore"):
            fit = (coefficients[0] * centered + coefficients[1]) * scale
        if np.isfinite(fit).all():
            chart.loc[valid, "Trend"] = fit
    return chart


def company_score_chart(chart: pd.DataFrame, labels: list[str]):
    # Vega treats periods in field names as nested access; use a plain internal field.
    plot_data = chart.rename(columns={"Debt-Adj. Total Score": "Score"})
    base = alt.Chart(plot_data).encode(
        x=alt.X("Year Range:N", sort=labels, scale=alt.Scale(domain=labels), title="Year range", axis=alt.Axis(labelAngle=-25)),
    )
    bars = base.mark_bar(size=40).encode(
        y=alt.Y("Score:Q", title="Debt-Adj. Total Score", scale=alt.Scale(zero=True)),
        color=alt.condition(alt.datum.Score >= 0, alt.value("#2563EB"), alt.value("#DC2626")),
        tooltip=[alt.Tooltip("Year Range:N"), alt.Tooltip("Score:Q", format=".2f", title="Debt-Adj. Total Score"),
                 alt.Tooltip("Included Years:N")],
    )
    values = base.mark_text(dy=-8).encode(
        y="Score:Q", text=alt.Text("Score:Q", format=".2f"),
    )
    layers = [bars, values]
    if chart["Trend"].notna().sum() >= 2:
        layers.append(base.mark_line(color="#F59E0B", strokeDash=[6, 4], strokeWidth=2.5).encode(
            y="Trend:Q", tooltip=[alt.Tooltip("Year Range:N"), alt.Tooltip("Trend:Q", format=".2f", title="Linear trend")],
        ))
    return alt.layer(*layers).properties(height=340).configure_view(stroke=None)


def selected_company_row(frame: pd.DataFrame, positions: list[int]) -> Optional[pd.Series]:
    """Streamlit selections use positions in the supplied frame, including after UI sorting."""
    if not positions or not 0 <= positions[0] < len(frame):
        return None
    return frame.iloc[positions[0]]


@st.fragment
def render_score_range_dashboard(frame: pd.DataFrame, windows: list[ScoreWindow], coverage: dict, generation: int) -> None:
    dashboard_section(TITLE, "Debt-Adj. Total Score for progressively shorter year ranges.")
    if not windows:
        st.info("No year ranges are available for this selection.")
        return
    direction = st.radio(
        "Year-range direction", ["Oldest start year first", "Latest start year first"],
        horizontal=True, key="combined_score_range_direction",
    )
    ordered = windows if direction == "Oldest start year first" else list(reversed(windows))
    labels = [w.label for w in ordered]
    view = frame[["Company", "Ticker", "Included Years", *labels]]
    config = {label: st.column_config.NumberColumn(
        label, format="%.2f", help=f"Debt-Adj. Total Score recalculated for {label}.",
    ) for label in labels}
    config["Company"] = st.column_config.TextColumn("Company", width="large")
    config["Included Years"] = st.column_config.TextColumn(
        "Included Years", help="Valid paired operating-income and interest-expense years / inclusive years in the selected overall range.",
    )
    event = st.dataframe(view, width="stretch", hide_index=True, height=table_height(view),
                         row_height=30, column_config=config, on_select="rerun", selection_mode="single-row",
                         key=f"combined_score_range_table_{generation}")
    row = selected_company_row(frame, event.selection.rows)
    company_key = f"combined_score_range_company_{generation}"
    direction_key = f"combined_score_range_last_direction_{generation}"
    direction_changed = st.session_state.get(direction_key, direction) != direction
    if row is not None:
        st.session_state[company_key] = row["_company_id"]
    elif direction_changed and company_key in st.session_state:
        # Streamlit clears dataframe selection when the supplied columns change.
        # Retain the chosen identity for this direction change, without relying on row labels.
        match = frame[frame["_company_id"] == st.session_state[company_key]]
        row = match.iloc[0] if not match.empty else None
    else:
        st.session_state.pop(company_key, None)
    st.session_state[direction_key] = direction
    if row is None:
        st.info("Select a company row to view its scores and trend.")
        return
    st.subheader(f"{row['Company']} · {row['Ticker']}")
    chart = company_chart_frame(row, ordered, coverage[row["_company_id"]])
    for start in range(0, len(ordered), 6):
        for column, (_, value) in zip(st.columns(len(ordered[start:start + 6])), chart.iloc[start:start + 6].iterrows()):
            score = value["Debt-Adj. Total Score"]
            column.metric(value["Year Range"], "—" if pd.isna(score) else f"{score:.2f}")
    if chart["Debt-Adj. Total Score"].notna().any():
        st.altair_chart(company_score_chart(chart, labels), use_container_width=True)
        legend = "Blue: nonnegative · Red: negative"
        if chart["Trend"].notna().sum() >= 2:
            legend += " · Dashed amber: linear trend across year ranges"
        else:
            legend += " · A trendline requires at least two available scores."
        st.caption(legend)
    else:
        st.info("Debt-adjusted scores are unavailable for this company in the displayed ranges.")
