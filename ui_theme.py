from __future__ import annotations

from typing import Any, Dict, Mapping, Optional

import pandas as pd
import streamlit as st


DASHBOARD_COLUMN_LABELS: Dict[str, str] = {
    "Company Name": "Company",
    "Industry Bucket": "Industry",
    "Additive Volatility-Adjusted Balance Sheet Strength Score": "Additive BS Score",
    "Scaled Volatility-Adjusted Balance Sheet Strength Score": "Scaled BS Score",
    "Debt-Adjusted Balance Sheet Strength Score": "Debt-Adj. BS Score",
    "Additive Volatility-Adjusted P&L Growth Score": "Additive P&L Score",
    "Scaled Volatility-Adjusted P&L Growth Score": "Scaled P&L Score",
    "Additive FCFF and Spread Score": "Additive FCFF/Spread",
    "Scaled FCFF and Spread Score": "Scaled FCFF/Spread",
    "Total Additive Volatility-Adjusted Score": "Total Additive Score",
    "Total Scaled Volatility-Adjusted Score": "Total Scaled Score",
    "Total Debt-Adjusted Scaled Volatility-Adjusted Score": "Debt-Adj. Total Score",
    "Total Debt-Adjusted Scaled Volatility-Adjusted Score": "Debt-Adj. Total Score",
    "Overall Score (0-400)": "Overall Score",
    "Current Market Price": "Market Price",
    "Price Source Detail": "Price Detail",
    "Quote As Of": "Quote Date",
    "Intrinsic Value": "Intrinsic Value",
    "Difference %": "Upside / Downside",
    "Median Spread % (ROIC - WACC)": "Median ROIC-WACC",
    "Spread % Standard Deviation (ROIC - WACC)": "ROIC-WACC Std Dev",
    "Median Operating Margin": "Median Op. Margin",
    "Operating Margin Standard Deviation": "Op. Margin Std Dev",
    "Median YoY Operating Margin Growth": "Median Op. Margin Growth",
    "YoY Operating Margin Growth Standard Deviation": "Op. Margin Growth Std Dev",
    "Median Revenue Growth": "Median Revenue Growth",
    "Revenue Growth Standard Deviation": "Revenue Growth Std Dev",
    "ROE Standard Deviation": "ROE Std Dev",
    "Median YoY FCFF Change %": "Median FCFF Change",
    "2020-2025 CAGR": "2020-25 CAGR",
    "2015-2020 CAGR": "2015-20 CAGR",
    "2010-2015 CAGR": "2010-15 CAGR",
    "Above 15% Year%": "Years >15%",
}


def install_dataframe_defaults() -> None:
    """Apply consistent defaults to Streamlit dataframe calls without changing call sites."""
    if getattr(st, "_tarasha_dataframe_defaults_installed", False):
        return

    original_dataframe = st.dataframe

    def themed_dataframe(data: Any = None, *args: Any, **kwargs: Any) -> Any:
        kwargs.setdefault("use_container_width", True)
        kwargs.setdefault("hide_index", True)
        kwargs.setdefault("row_height", 30)
        frame = getattr(data, "data", data)
        if isinstance(frame, pd.DataFrame):
            kwargs.setdefault("height", table_height(frame, row_height=int(kwargs.get("row_height") or 30)))
            kwargs.setdefault(
                "column_config",
                infer_column_config(frame, existing=kwargs.get("column_config")),
            )
        return original_dataframe(data, *args, **kwargs)

    st.dataframe = themed_dataframe  # type: ignore[method-assign]
    st._tarasha_dataframe_defaults_installed = True  # type: ignore[attr-defined]


def inject_dashboard_table_css() -> None:
    st.markdown(
        """
        <style>
          :root {
            --dashboard-border: rgba(148, 163, 184, 0.24);
            --dashboard-border-strong: rgba(148, 163, 184, 0.34);
            --dashboard-panel-bg: rgba(15, 23, 42, 0.035);
            --dashboard-muted: rgba(100, 116, 139, 1);
          }

          @media (prefers-color-scheme: dark) {
            :root {
              --dashboard-border: rgba(148, 163, 184, 0.26);
              --dashboard-border-strong: rgba(148, 163, 184, 0.38);
              --dashboard-panel-bg: rgba(15, 23, 42, 0.44);
              --dashboard-muted: rgba(203, 213, 225, 0.72);
            }
          }

          .ta-dashboard-section {
            margin: 1.05rem 0 0.65rem 0;
          }

          .ta-dashboard-section-title {
            margin: 0;
            font-size: 1.05rem;
            font-weight: 650;
            line-height: 1.25;
            color: inherit;
          }

          .ta-dashboard-section-subtitle {
            margin: 0.18rem 0 0 0;
            font-size: 0.88rem;
            line-height: 1.42;
            color: var(--dashboard-muted);
          }

          [data-testid="stDataFrame"],
          [data-testid="stTable"],
          [data-testid="stDataEditor"] {
            border: 1px solid var(--dashboard-border) !important;
            border-radius: 8px !important;
            overflow: hidden !important;
            background: var(--dashboard-panel-bg) !important;
            box-shadow: none !important;
          }

          [data-testid="stDataFrame"] div,
          [data-testid="stTable"] div,
          [data-testid="stDataEditor"] div {
            font-family: var(--app-font-family) !important;
          }

          [data-testid="stDataFrame"] [role="columnheader"],
          [data-testid="stDataEditor"] [role="columnheader"] {
            font-weight: 625 !important;
          }

          [data-testid="stMetric"] {
            border: 1px solid var(--dashboard-border);
            border-radius: 8px;
            padding: 0.75rem 0.85rem;
            background: var(--dashboard-panel-bg);
          }

          [data-testid="stExpander"] {
            border-color: var(--dashboard-border-strong) !important;
            border-radius: 8px !important;
            overflow: hidden;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )


def dashboard_section(title: str, subtitle: Optional[str] = None) -> None:
    subtitle_html = f'<p class="ta-dashboard-section-subtitle">{subtitle}</p>' if subtitle else ""
    st.markdown(
        f"""
        <div class="ta-dashboard-section">
          <h3 class="ta-dashboard-section-title">{title}</h3>
          {subtitle_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def display_table_frame(
    df: pd.DataFrame,
    *,
    label_map: Optional[Mapping[str, str]] = None,
) -> pd.DataFrame:
    if label_map is None:
        label_map = DASHBOARD_COLUMN_LABELS
    rename_map = {column: label_map[column] for column in df.columns if column in label_map}
    return df.rename(columns=rename_map)


def _source_column(display_column: str, label_map: Mapping[str, str]) -> str:
    for source, label in label_map.items():
        if label == display_column:
            return source
    return display_column


def infer_column_config(
    df: pd.DataFrame,
    *,
    existing: Optional[Mapping[str, Any]] = None,
    help_map: Optional[Mapping[str, str]] = None,
    label_map: Optional[Mapping[str, str]] = None,
) -> Dict[str, Any]:
    config: Dict[str, Any] = dict(existing or {})
    help_map = dict(help_map or {})
    label_map = dict(label_map or DASHBOARD_COLUMN_LABELS)

    for column in df.columns:
        if column in config:
            continue

        source_column = _source_column(str(column), label_map)
        help_text = help_map.get(str(column)) or help_map.get(source_column)
        column_l = str(column).lower()
        source_l = source_column.lower()
        is_numeric = pd.api.types.is_numeric_dtype(df[column])

        if not is_numeric:
            if help_text:
                config[column] = st.column_config.TextColumn(help=help_text)
            continue

        if "%" in str(column) or "percent" in source_l or "margin" in column_l or "cagr" in column_l or "upside" in column_l:
            config[column] = st.column_config.NumberColumn(format="%.2f", help=help_text)
        elif "price" in column_l or "value" in column_l or "market" in column_l:
            config[column] = st.column_config.NumberColumn(format="%.2f", help=help_text)
        elif "score" in column_l or "std dev" in column_l or "standard deviation" in source_l or "median" in column_l:
            config[column] = st.column_config.NumberColumn(format="%.2f", help=help_text)
        elif "year" in column_l:
            config[column] = st.column_config.NumberColumn(format="%d", help=help_text)
        else:
            config[column] = st.column_config.NumberColumn(format="%.2f", help=help_text)

    return config


def table_height(df: pd.DataFrame, *, max_height: int = 420, min_height: int = 72, row_height: int = 30) -> int:
    rows = 0 if df is None else int(len(df))
    visible_rows = max(rows, 1)
    return max(min_height, min(max_height, 42 + (visible_rows * row_height)))


def render_dashboard_table(
    data: Any,
    *,
    column_config: Optional[Mapping[str, Any]] = None,
    help_map: Optional[Mapping[str, str]] = None,
    height: Optional[int] = None,
    key: Optional[str] = None,
    row_height: int = 30,
    hide_index: bool = True,
    column_order: Optional[list[str]] = None,
) -> Any:
    frame = getattr(data, "data", data)
    if isinstance(frame, pd.DataFrame):
        config = infer_column_config(frame, existing=column_config, help_map=help_map)
        if height is None:
            height = table_height(frame, row_height=row_height)
    else:
        config = dict(column_config or {})

    return st.dataframe(
        data,
        use_container_width=True,
        hide_index=hide_index,
        row_height=row_height,
        height=height,
        column_config=config,
        column_order=column_order,
        key=key,
    )
