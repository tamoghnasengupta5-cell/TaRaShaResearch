import base64
import html as html_lib
import json
import re
import unicodedata
from pathlib import Path
from typing import Dict, List

import streamlit as st
import streamlit.components.v1 as components

from core import get_db
from ui_lazy_tabs import lazy_tab_bar
from ui_theme import company_label_map, inject_dashboard_table_css, install_dataframe_defaults

SEARCH_TAB_LABEL = "Search Aggregate"
SEARCH_ACTIVATE_KEY = "search_aggregate_activate_tab"
SEARCH_SELECTED_IDS_KEY = "search_aggregate_selected_ids"
SEARCH_YEAR_RANGE_KEY = "search_aggregate_year_range"
DEFAULT_YEAR_RANGE = "Recent - 2020"

# --- Branding assets (kept local to the repo) ---
_ASSETS_DIR = Path(__file__).parent / "assets"
_LOGO_PATH = _ASSETS_DIR / "tarasha_logo_bright.png"
_HERO_BANNER_PATH = _ASSETS_DIR / "Hero_Banner.png"
_HERO_BANNER_2_PATH = _ASSETS_DIR / "Hero_Banner_2_Semiconductors.png"
_HERO_BANNER_3_PATH = _ASSETS_DIR / "Hero_Banner_3_YoY_Growth.png"

# --- Featured Articles assets ---
_ARTICLES_DIR = _ASSETS_DIR / "articles"

# Streamlit requires set_page_config to be the first Streamlit command in the app.
st.set_page_config(
    page_title="TaRaSha Equity Research Tool",
    layout="wide",
    initial_sidebar_state="collapsed",
)
install_dataframe_defaults()

@st.cache_data(show_spinner=False)
def _bytes_to_b64(data: bytes) -> str:
    return base64.b64encode(data).decode("utf-8")

@st.cache_data(show_spinner=False)
def _file_to_b64(path: Path) -> str:
    return _bytes_to_b64(path.read_bytes())

def _inject_shell_css() -> None:
    """
    Minimal CSS to keep the Home page clean.
    (We avoid over-customizing Streamlit so we don't break layout across upgrades.)
    """
    st.markdown(
        """
        <style>
          :root {
            --app-font-family: Aptos, "Segoe UI Variable", "Segoe UI", Inter, Roboto, Arial, sans-serif;
            --app-page-x: 24px;
            --app-background: #f5f7fb;
            --app-background-raised: #f8fafc;
            --app-surface: rgba(255, 255, 255, 0.88);
            --app-surface-strong: #ffffff;
            --app-text-primary: #172033;
            --app-text-secondary: #5f6f86;
            --app-border: rgba(148, 163, 184, 0.30);
            --app-border-strong: rgba(148, 163, 184, 0.46);
            --app-hover-bg: rgba(226, 232, 240, 0.48);
            --app-focus-ring: rgba(37, 99, 235, 0.24);
            --app-shadow-soft: 0 14px 36px rgba(15, 23, 42, 0.07);
            --app-shadow-inset: inset 0 1px 0 rgba(255, 255, 255, 0.74);
            --tab-border-color: var(--app-border);
            --tab-text-color: var(--app-text-secondary);
            --tab-hover-text-color: var(--app-text-primary);
            --tab-hover-bg: var(--app-hover-bg);
            --tab-selected-text-color: #0f4fd6;
            --tab-selected-border-color: rgba(37, 99, 235, 0.95);
            --app-accent: #2563eb;
            --app-accent-hover: #1d4ed8;
            --app-accent-soft: #e0f2fe;
            --app-accent-soft-border: #bae6fd;
            --app-accent-soft-text: #075985;
          }

          @media (prefers-color-scheme: dark) {
            :root {
              --app-background: #0e1420;
              --app-background-raised: #111827;
              --app-surface: rgba(17, 24, 39, 0.88);
              --app-surface-strong: #111827;
              --app-text-primary: #f8fafc;
              --app-text-secondary: rgba(203, 213, 225, 0.82);
              --app-border: rgba(100, 116, 139, 0.44);
              --app-border-strong: rgba(148, 163, 184, 0.42);
              --app-hover-bg: rgba(51, 65, 85, 0.50);
              --app-focus-ring: rgba(96, 165, 250, 0.30);
              --app-shadow-soft: 0 16px 40px rgba(0, 0, 0, 0.28);
              --app-shadow-inset: inset 0 1px 0 rgba(255, 255, 255, 0.06);
              --tab-border-color: var(--app-border);
              --tab-text-color: var(--app-text-secondary);
              --tab-hover-text-color: var(--app-text-primary);
              --tab-hover-bg: var(--app-hover-bg);
              --tab-selected-text-color: #93c5fd;
              --tab-selected-border-color: rgba(96, 165, 250, 1);
              --app-accent: #3b82f6;
              --app-accent-hover: #60a5fa;
              --app-accent-soft: rgba(14, 116, 144, 0.26);
              --app-accent-soft-border: rgba(125, 211, 252, 0.34);
              --app-accent-soft-text: #e0f2fe;
            }
          }

          :root[data-theme="light"],
          [data-theme="light"] {
            --app-background: #f5f7fb;
            --app-background-raised: #f8fafc;
            --app-surface: rgba(255, 255, 255, 0.88);
            --app-surface-strong: #ffffff;
            --app-text-primary: #172033;
            --app-text-secondary: #5f6f86;
            --app-border: rgba(148, 163, 184, 0.30);
            --app-border-strong: rgba(148, 163, 184, 0.46);
            --app-hover-bg: rgba(226, 232, 240, 0.48);
            --app-focus-ring: rgba(37, 99, 235, 0.24);
            --app-shadow-soft: 0 14px 36px rgba(15, 23, 42, 0.07);
            --app-shadow-inset: inset 0 1px 0 rgba(255, 255, 255, 0.74);
            --tab-border-color: var(--app-border);
            --tab-text-color: var(--app-text-secondary);
            --tab-hover-text-color: var(--app-text-primary);
            --tab-hover-bg: var(--app-hover-bg);
            --tab-selected-text-color: #0f4fd6;
            --tab-selected-border-color: rgba(37, 99, 235, 0.95);
            --app-accent: #2563eb;
            --app-accent-hover: #1d4ed8;
            --app-accent-soft: #e0f2fe;
            --app-accent-soft-border: #bae6fd;
            --app-accent-soft-text: #075985;
          }

          :root[data-theme="dark"],
          [data-theme="dark"] {
            --app-background: #0e1420;
            --app-background-raised: #111827;
            --app-surface: rgba(17, 24, 39, 0.88);
            --app-surface-strong: #111827;
            --app-text-primary: #f8fafc;
            --app-text-secondary: rgba(203, 213, 225, 0.82);
            --app-border: rgba(100, 116, 139, 0.44);
            --app-border-strong: rgba(148, 163, 184, 0.42);
            --app-hover-bg: rgba(51, 65, 85, 0.50);
            --app-focus-ring: rgba(96, 165, 250, 0.30);
            --app-shadow-soft: 0 16px 40px rgba(0, 0, 0, 0.28);
            --app-shadow-inset: inset 0 1px 0 rgba(255, 255, 255, 0.06);
            --tab-border-color: var(--app-border);
            --tab-text-color: var(--app-text-secondary);
            --tab-hover-text-color: var(--app-text-primary);
            --tab-hover-bg: var(--app-hover-bg);
            --tab-selected-text-color: #93c5fd;
            --tab-selected-border-color: rgba(96, 165, 250, 1);
            --app-accent: #3b82f6;
            --app-accent-hover: #60a5fa;
            --app-accent-soft: rgba(14, 116, 144, 0.26);
            --app-accent-soft-border: rgba(125, 211, 252, 0.34);
            --app-accent-soft-text: #e0f2fe;
          }

          @media (max-width: 1024px) {
            :root { --app-page-x: 20px; }
          }

          @media (max-width: 640px) {
            :root { --app-page-x: 16px; }
          }

          html,
          body,
          .stApp,
          [data-testid="stAppViewContainer"],
          [data-testid="stAppViewContainer"] *,
          [data-testid="stSidebar"] *,
          [data-testid="stHeader"] *,
          [data-testid="stToolbar"] * {
            font-family: var(--app-font-family) !important;
            -webkit-font-smoothing: antialiased;
            -moz-osx-font-smoothing: grayscale;
            text-rendering: optimizeLegibility;
          }

          html,
          body,
          .stApp,
          [data-testid="stAppViewContainer"],
          [data-testid="stMain"],
          [data-testid="stMainBlockContainer"] {
            background: var(--app-background) !important;
            color: var(--app-text-primary) !important;
          }

          [data-testid="stHeader"] {
            background: transparent !important;
          }

          .stMarkdown,
          .stMarkdown *,
          p,
          li,
          label,
          [data-testid="stText"],
          [data-testid="stCaptionContainer"],
          [data-testid="stWidgetLabel"],
          [data-testid="stMetric"],
          [data-testid="stDataFrame"],
          [data-testid="stDataFrame"] *,
          [data-testid="stTable"],
          [data-testid="stTable"] *,
          [data-testid="stDataEditor"],
          [data-testid="stDataEditor"] *,
          input,
          textarea,
          select,
          button {
            font-family: var(--app-font-family) !important;
          }

          .material-icons,
          .material-icons-round,
          .material-icons-rounded,
          .material-icons-outlined,
          .material-symbols-rounded,
          .material-symbols-outlined,
          .material-symbols-sharp,
          [data-testid="stIconMaterial"],
          [class*="material-icons"],
          [class*="material-symbols"] {
            font-family: "Material Symbols Rounded", "Material Symbols Outlined", "Material Icons" !important;
            font-weight: normal !important;
            font-style: normal !important;
            font-size: 1.25rem !important;
            line-height: 1 !important;
            letter-spacing: normal !important;
            text-transform: none !important;
            display: inline-block !important;
            white-space: nowrap !important;
            direction: ltr !important;
            -webkit-font-feature-settings: "liga";
            -webkit-font-smoothing: antialiased;
            font-feature-settings: "liga";
          }

          p,
          li,
          [data-testid="stCaptionContainer"],
          [data-testid="stMarkdownContainer"] p {
            font-weight: 400;
            line-height: 1.48;
            color: inherit;
          }

          [data-testid="stWidgetLabel"],
          [data-testid="stWidgetLabel"] *,
          [data-testid="stExpander"] summary,
          [data-testid="stExpander"] summary *,
          [data-baseweb="select"] *,
          [data-baseweb="input"] *,
          [data-baseweb="textarea"] *,
          [data-baseweb="checkbox"] *,
          [data-baseweb="radio"] * {
            line-height: normal !important;
          }

          h1,
          h2,
          h3,
          h4,
          h5,
          h6 {
            font-family: var(--app-font-family) !important;
            font-weight: 625 !important;
            letter-spacing: 0 !important;
            line-height: 1.18 !important;
            color: var(--app-text-primary) !important;
          }

          h1 { font-size: clamp(1.75rem, 2.4vw, 2.35rem) !important; }
          h2 { font-size: clamp(1.38rem, 2.0vw, 1.85rem) !important; }
          h3 { font-size: clamp(1.12rem, 1.6vw, 1.35rem) !important; }

          [data-testid="stButton"] > button,
          [data-testid="stDownloadButton"] > button,
          [data-testid="stFormSubmitButton"] > button,
          [data-baseweb="select"] input,
          [data-baseweb="select"] span,
          [data-baseweb="input"] input,
          [data-baseweb="textarea"] textarea,
          [data-baseweb="tag"] * {
            font-family: var(--app-font-family) !important;
            font-weight: 500 !important;
          }

          [data-testid="stButton"] > button[kind="primary"],
          [data-testid="stDownloadButton"] > button[kind="primary"],
          [data-testid="stFormSubmitButton"] > button[kind="primary"],
          button[data-testid="baseButton-primary"] {
            background: var(--app-accent) !important;
            border-color: var(--app-accent) !important;
            color: #ffffff !important;
            box-shadow: none !important;
          }

          [data-testid="stButton"] > button[kind="primary"]:hover,
          [data-testid="stDownloadButton"] > button[kind="primary"]:hover,
          [data-testid="stFormSubmitButton"] > button[kind="primary"]:hover,
          button[data-testid="baseButton-primary"]:hover {
            background: var(--app-accent-hover) !important;
            border-color: var(--app-accent-hover) !important;
            color: #ffffff !important;
          }

          [data-testid="stButton"] > button:focus,
          [data-testid="stButton"] > button:focus-visible,
          [data-testid="stDownloadButton"] > button:focus,
          [data-testid="stDownloadButton"] > button:focus-visible,
          [data-testid="stFormSubmitButton"] > button:focus,
          [data-testid="stFormSubmitButton"] > button:focus-visible {
            box-shadow: 0 0 0 0.12rem var(--app-focus-ring) !important;
            outline: none !important;
          }

          [data-testid="stMultiSelect"] [data-baseweb="select"] > div:focus-within {
            border-color: var(--app-accent) !important;
            box-shadow: 0 0 0 0.12rem var(--app-focus-ring) !important;
          }

          [data-testid="stMultiSelect"] [data-baseweb="tag"] {
            background: var(--app-accent-soft) !important;
            border: 1px solid var(--app-accent-soft-border) !important;
            color: var(--app-accent-soft-text) !important;
          }

          [data-testid="stMultiSelect"] [data-baseweb="tag"] span,
          [data-testid="stMultiSelect"] [data-baseweb="tag"] svg {
            color: var(--app-accent-soft-text) !important;
            fill: var(--app-accent-soft-text) !important;
          }

          [data-testid="stDataFrame"],
          [data-testid="stTable"],
          [data-testid="stDataEditor"] {
            font-size: 0.92rem !important;
            line-height: 1.42 !important;
          }

          /* Shared page grid: keep all app sections on the same left edge. */
          [data-testid="stMainBlockContainer"],
          .block-container {
            max-width: 100% !important;
            padding-top: 1.15rem !important;
            padding-bottom: 2rem !important;
            padding-left: var(--app-page-x) !important;
            padding-right: var(--app-page-x) !important;
          }

          [data-testid="stAppViewContainer"],
          [data-testid="stAppViewContainer"] > .main {
            padding-left: 0 !important;
            padding-right: 0 !important;
          }

          [data-testid="stMainBlockContainer"] .block-container {
            padding-left: 0 !important;
            padding-right: 0 !important;
          }

          [data-testid="stMainBlockContainer"] > div,
          .block-container > div {
            max-width: 100% !important;
          }

          [data-testid="stVerticalBlock"] {
            gap: 0.85rem;
          }

          [data-testid="stForm"],
          [data-testid="stExpander"],
          [data-testid="stVerticalBlockBorderWrapper"] {
            border: 1px solid var(--app-border) !important;
            border-radius: 10px !important;
            background: var(--app-surface) !important;
            box-shadow: var(--app-shadow-soft), var(--app-shadow-inset) !important;
          }

          [data-testid="stForm"] {
            padding: 1rem 1.05rem 1.05rem !important;
          }

          [data-testid="stExpander"] {
            overflow: hidden;
          }

          [data-baseweb="select"] > div,
          [data-baseweb="input"] > div,
          [data-baseweb="textarea"] textarea {
            background: var(--app-surface-strong) !important;
            border-color: var(--app-border-strong) !important;
          }

          [data-baseweb="select"] > div:focus-within,
          [data-baseweb="input"] > div:focus-within,
          [data-baseweb="textarea"] textarea:focus {
            border-color: var(--app-accent) !important;
            box-shadow: 0 0 0 0.12rem var(--app-focus-ring) !important;
          }

          /* Native Streamlit tabs used inside a few legacy screens. */
          [data-testid="stTabs"] [data-baseweb="tab-list"] {
            gap: 0.25rem;
            padding-left: 0;
            padding-right: 0;
            border-bottom: 1px solid var(--tab-border-color);
          }

          [data-testid="stTabs"] button[role="tab"] {
            background: transparent !important;
            color: var(--tab-text-color) !important;
            font-family: var(--app-font-family) !important;
            -webkit-font-smoothing: antialiased;
            -moz-osx-font-smoothing: grayscale;
            font-weight: 540 !important;
            font-size: 0.93rem !important;
            line-height: 1.22 !important;
            padding: 0.68rem 0.9rem !important;
            border: none !important;
            border-bottom: 2px solid transparent !important;
            border-radius: 8px 8px 0 0 !important;
            margin: 0 !important;
            min-height: 2.4rem !important;
            transition: background-color 120ms ease, color 120ms ease, border-color 120ms ease;
          }

          [data-testid="stTabs"] button[role="tab"]:hover {
            color: var(--tab-hover-text-color) !important;
            background: var(--tab-hover-bg) !important;
          }

          [data-testid="stTabs"] button[role="tab"][aria-selected="true"] {
            color: var(--tab-selected-text-color) !important;
            font-weight: 625 !important;
            border-bottom: 2px solid var(--tab-selected-border-color) !important;
            background: transparent !important;
          }

          [data-testid="stTabs"] button[role="tab"]:focus,
          [data-testid="stTabs"] button[role="tab"]:focus-visible {
            outline: 2px solid var(--app-focus-ring) !important;
            outline-offset: -2px !important;
            box-shadow: none !important;
          }

          /* Disable BaseWeb underline so our border-bottom is the single source of truth */
          [data-testid="stTabs"] div[data-baseweb="tab-highlight"] {
            display: none !important;
          }

          /* Header logo: reduce vertical real estate */
          div[data-testid="stImage"] {
            margin-top: 0.0rem !important;
            margin-bottom: 0.15rem !important;
          }

          /* Home header search styling, scoped so other tabs keep their existing widget look */
          .st-key-home_header_search [data-testid="stMultiSelect"] [data-baseweb="select"] > div,
          .st-key-home_header_search [data-testid="stSelectbox"] [data-baseweb="select"] > div {
            border: 1px solid var(--app-border-strong) !important;
            box-shadow: var(--app-shadow-inset) !important;
            min-height: 2.75rem !important;
          }

          .st-key-home_header_search [data-testid="stMultiSelect"] [data-baseweb="select"] > div {
            background: var(--app-surface-strong) !important;
            border-radius: 12px !important;
            padding-left: 0.35rem !important;
          }

          .st-key-home_header_search [data-testid="stSelectbox"] [data-baseweb="select"] > div {
            background: var(--app-surface-strong) !important;
            border-radius: 12px !important;
          }

          .st-key-home_header_search [data-testid="stMultiSelect"] input,
          .st-key-home_header_search [data-testid="stMultiSelect"] input::placeholder,
          .st-key-home_header_search [data-testid="stMultiSelect"] [data-baseweb="tag"] span,
          .st-key-home_header_search [data-testid="stSelectbox"] [data-baseweb="select"] span,
          .st-key-home_header_search [data-testid="stSelectbox"] [data-baseweb="select"] input,
          .st-key-home_header_search [data-testid="stSelectbox"] [data-baseweb="select"] svg,
          .st-key-home_header_search [data-testid="stMultiSelect"] [data-baseweb="select"] svg {
            color: var(--app-text-primary) !important;
            opacity: 1 !important;
          }

          .st-key-home_header_search [data-testid="stMultiSelect"] [data-baseweb="tag"] {
            background: var(--app-accent-soft) !important;
            border: 1px solid var(--app-accent-soft-border) !important;
            border-radius: 8px !important;
          }

          .st-key-home_header_search [data-testid="stButton"] > button {
            background: var(--app-surface-strong) !important;
            border: 1px solid var(--app-border-strong) !important;
            border-radius: 12px !important;
            box-shadow: var(--app-shadow-inset) !important;
            color: var(--app-text-primary) !important;
            font-size: 0.95rem !important;
            font-weight: 625 !important;
            min-height: 2.75rem !important;
            padding: 0 !important;
          }

          .st-key-home_header_search [data-testid="stButton"] > button:hover {
            background: var(--app-hover-bg) !important;
            border-color: var(--app-border-strong) !important;
            color: var(--app-text-primary) !important;
          }

          .st-key-home_header_search [data-testid="stButton"] > button:focus,
          .st-key-home_header_search [data-testid="stButton"] > button:focus-visible {
            border-color: var(--app-accent) !important;
            box-shadow: 0 0 0 0.12rem var(--app-focus-ring) !important;
            outline: none !important;
          }

          /* Mobile responsiveness (primarily affects the Home shell) */
          @media (max-width: 640px) {
            .block-container {
              padding-top: 0.6rem !important;
            }

            /* Make tabs horizontally scrollable instead of overflowing */
            [data-testid="stTabs"] [data-baseweb="tab-list"] {
              gap: 0.45rem;
              overflow-x: auto;
              flex-wrap: nowrap;
              -webkit-overflow-scrolling: touch;
              padding-left: 0;
              padding-right: 0;
              border-bottom: 1px solid var(--tab-border-color);
            }
            [data-testid="stTabs"] button[role="tab"] {
              flex: 0 0 auto;
              white-space: nowrap;
              min-height: 2.75rem !important;
            }

            [data-testid="stButton"] > button,
            [data-testid="stDownloadButton"] > button,
            [data-testid="stFormSubmitButton"] > button {
              min-height: 2.75rem !important;
            }

            /* Keep the logo from dominating the top on narrow screens */
            div[data-testid="stImage"] img {
              max-width: 150px;
              height: auto;
            }
          }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _activate_top_level_tab(label: str) -> None:
    components.html(
        f"""
        <script>
          const label = {json.dumps(label)};
          let tries = 0;
          const timer = window.setInterval(() => {{
            tries += 1;
            try {{
              const tabs = Array.from(window.parent.document.querySelectorAll('button[role=\"tab\"]'));
              const target = tabs.find((btn) => ((btn.innerText || '').trim() === label));
              if (target) {{
                target.click();
                window.clearInterval(timer);
              }}
            }} catch (err) {{
              console.warn('header tab activation failed', err);
            }}
            if (tries >= 12) {{
              window.clearInterval(timer);
            }}
          }}, 150);
        </script>
        """,
        height=0,
        scrolling=False,
    )


def _submit_header_search() -> None:
    selected_ids = [int(x) for x in st.session_state.get(SEARCH_SELECTED_IDS_KEY, []) if str(x).isdigit()]
    if selected_ids:
        st.session_state[SEARCH_ACTIVATE_KEY] = True
        st.session_state["app_top_nav_active"] = SEARCH_TAB_LABEL


def _get_header_search_context(conn) -> tuple:
    from core import list_companies, read_df

    companies_df = list_companies(conn)
    years_df = read_df("SELECT MIN(fiscal_year) AS min_year, MAX(fiscal_year) AS max_year FROM revenues_annual", conn)
    if years_df is None or years_df.empty:
        return companies_df, [DEFAULT_YEAR_RANGE]

    min_year = years_df.iloc[0]["min_year"]
    max_year = years_df.iloc[0]["max_year"]
    if min_year is None or max_year is None or pd_is_na(min_year) or pd_is_na(max_year):
        return companies_df, [DEFAULT_YEAR_RANGE]

    min_year = int(min_year)
    max_year = int(max_year)
    start_year = max(min_year, max_year - 10)
    year_options = [f"Recent - {year}" for year in range(max_year - 1, start_year - 1, -1)]
    if DEFAULT_YEAR_RANGE not in year_options:
        year_options.insert(0, DEFAULT_YEAR_RANGE)
    return companies_df, year_options


def pd_is_na(value: object) -> bool:
    try:
        import pandas as pd

        return bool(pd.isna(value))
    except Exception:
        return value is None


def _render_header() -> tuple:
    _inject_shell_css()
    inject_dashboard_table_css()

    conn = get_db()
    companies_df, year_options = _get_header_search_context(conn)
    current_year_range = str(st.session_state.get(SEARCH_YEAR_RANGE_KEY, DEFAULT_YEAR_RANGE) or DEFAULT_YEAR_RANGE)
    if current_year_range not in year_options:
        year_options = [current_year_range, *[opt for opt in year_options if opt != current_year_range]]
    if SEARCH_YEAR_RANGE_KEY not in st.session_state:
        st.session_state[SEARCH_YEAR_RANGE_KEY] = current_year_range

    with st.container():
        logo_col, controls_col = st.columns([1.1, 5.2], vertical_alignment="center")
        with logo_col:
            if _LOGO_PATH.exists():
                st.image(str(_LOGO_PATH), width=200)
        with controls_col:
            header_company_label_map = company_label_map(companies_df)
            if SEARCH_SELECTED_IDS_KEY not in st.session_state:
                st.session_state[SEARCH_SELECTED_IDS_KEY] = []
            with st.container(key="home_header_search"):
                search_col, year_col, button_col = st.columns([4.8, 2.1, 0.7], vertical_alignment="center")
                with search_col:
                    st.multiselect(
                        'Company Search',
                        options=list(header_company_label_map.keys()),
                        format_func=lambda company_id: header_company_label_map.get(company_id, str(company_id)),
                        key=SEARCH_SELECTED_IDS_KEY,
                        label_visibility='collapsed',
                        placeholder='Company or stock symbol...',
                        help='Type part of a company name or ticker to see suggestions. You can select multiple companies.',
                    )
                with year_col:
                    st.selectbox(
                        'Year range',
                        options=year_options,
                        key=SEARCH_YEAR_RANGE_KEY,
                        label_visibility='collapsed',
                    )
                with button_col:
                    if st.button('Go', key='header_search_submit', use_container_width=True):
                        _submit_header_search()

        if st.session_state.get(SEARCH_ACTIVATE_KEY):
            st.session_state[SEARCH_ACTIVATE_KEY] = False
            st.session_state["app_top_nav_active"] = SEARCH_TAB_LABEL

        return lazy_tab_bar(
            [
                "Home",
                SEARCH_TAB_LABEL,
                "Key Data",
                "Equity Research",
                "Valuations",
            ],
            key="app_top_nav",
            default="Home",
        )

def _render_hero_carousel(image_paths: List[Path], slide_meta: List[Dict] | None = None) -> None:
    """
    Render a responsive hero image carousel.
    - Designed as a carousel so you can add more hero images later.
    - Keeps aspect ratio; avoids cropping/distortion.
    - Leaves a clean overlay layer available for future CTA buttons.
    """
    # Standard hero banner aspect ratio (prevents cropping/distortion)
    # Target size: 1802x601
    banner_ratio_w = 1802
    banner_ratio_h = 601

    # Starting height only — the iframe auto-resizes via JS (postMessage)
    hero_iframe_height = 720
    # "slide_meta" is a future-proof hook for adding CTA behavior per slide.
    # We intentionally do NOT implement CTA click behavior yet.
    if slide_meta is None:
        slide_meta = [None] * len(image_paths)
    # Keep ordering stable, but skip missing files.
    pairs = [(Path(p), m) for p, m in zip(image_paths, slide_meta)]
    pairs = [(p, m) for p, m in pairs if p.exists()]
    if not pairs:
        st.warning("Hero banner image not found. Put 'Hero_Banner.png' under ./assets/.")
        return

    imgs_b64 = [_file_to_b64(p) for p, _ in pairs]
    metas = [m for _, m in pairs]

    # Optional: a transparent text banner overlay on specific slides (e.g., the main Hero_Banner).
    slides_parts: List[str] = []
    for b64, meta in zip(imgs_b64, metas):
        overlay_text_html = ""
        if isinstance(meta, dict) and meta.get("overlay_text"):
            ov = meta.get("overlay_text") or {}
            line1 = html_lib.escape(str(ov.get("line1", "")))
            line2 = html_lib.escape(str(ov.get("line2", "")))
            cta_label = html_lib.escape(str(ov.get("button_label", "Explore Combined Dashboard")))
            overlay_text_html = f"""
                <div class=\"ta-hero-textbanner\" aria-label=\"Hero banner description\">
                  <div class=\"ta-hero-textbanner-line1\">{line1}</div>
                  <div class=\"ta-hero-textbanner-line2\">{line2}</div>
                  <button class=\"ta-hero-cta\" type=\"button\" onclick=\"navigateToCombinedDashboard(event)\">{cta_label}</button>
                </div>
            """

        slides_parts.append(
            f"""
            <div class=\"ta-hero-slide\" data-slide-meta='{json.dumps(meta) if meta else ""}'>
              <div class=\"ta-hero-frame\">
                <img src=\"data:image/png;base64,{b64}\" alt=\"Hero banner\"/>
                {overlay_text_html}
                <div class=\"ta-hero-overlay\" aria-hidden=\"true\"></div>
              </div>
            </div>
            """
        )

    slides_html = "".join(slides_parts)

    show_controls = "block" if len(imgs_b64) > 1 else "none"
    html = f"""
    <html>
    <head>
      <style>
        .ta-hero {{
          width: 100%;
          max-width: 100%;
          margin: 0 0 0.25rem 0;
          position: relative;
          user-select: none;
          -webkit-user-select: none;
        }}
        .ta-hero-slide {{ display: none; }}
        .ta-hero-slide.active {{ display: block; }}
        .ta-hero-frame {{
          width: 100%;
          /*aspect-ratio: {banner_ratio_w} / {banner_ratio_h};*/
          border-radius: 18px;
          overflow: hidden;
          box-shadow: 0 10px 30px rgba(0,0,0,0.08);
          position: relative;
          background: #ffffff;
        }}
        .ta-hero-frame img {{
          width: 100%;
          height: 100%;
          object-fit: contain; /* critical: no crop/distort */
          display: block;
        }}
        .ta-hero-overlay {{
          position: absolute;
          inset: 0;
          background: linear-gradient(180deg, rgba(255,255,255,0.00) 35%, rgba(0,0,0,0.04) 100%);
          pointer-events: none;
          z-index: 1;
        }}

        /* Transparent text banner (main Hero_Banner only) */
        .ta-hero-textbanner {{
          position: absolute;
          top: 65%;
          right: clamp(28px, 5.5vw, 120px);
          transform: translateY(-50%);
          width: min(720px, 56%);
          padding: 40px 22px 27px 22px;
          border-radius: 16px;
          background: rgba(255, 255, 255, 0.42);
          border: 1px solid rgba(255, 255, 255, 0.50);
          box-shadow: 0 10px 26px rgba(0,0,0,0.12);
          backdrop-filter: blur(10px);
          -webkit-backdrop-filter: blur(10px);
          color: rgba(15, 17, 20, 0.92);
          line-height: 1.38;
          font-family: ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, "Noto Sans", "Liberation Sans", sans-serif;
          -webkit-font-smoothing: antialiased;
          text-rendering: optimizeLegibility;
          z-index: 2;
          pointer-events: none;
        }}
        .ta-hero-textbanner-line1 {{
          font-size: 16px;
          font-weight: 600;
          letter-spacing: -0.15px;
        }}
        .ta-hero-textbanner-line2 {{
          margin-top: 10px;
          font-size: 15px;
          font-weight: 450;
          opacity: 0.92;
        }}
        .ta-hero-cta {{
          margin-top: 16px;
          display: inline-flex;
          align-items: center;
          justify-content: center;
          padding: 10px 14px;
          border-radius: 999px;
          border: 1px solid rgba(15, 17, 20, 0.18);
          background: rgba(255, 255, 255, 0.72);
          color: rgba(15, 17, 20, 0.92);
          font-size: 14px;
          font-weight: 600;
          letter-spacing: -0.10px;
          cursor: pointer;
          pointer-events: auto;
          box-shadow: 0 8px 20px rgba(0,0,0,0.10);
          transition: transform 140ms ease, background 140ms ease, box-shadow 140ms ease;
        }}
        .ta-hero-cta:hover {{
          background: rgba(255, 255, 255, 0.88);
          transform: translateY(-1px);
          box-shadow: 0 10px 26px rgba(0,0,0,0.12);
        }}
        .ta-hero-cta:active {{
          transform: translateY(0px);
        }}
        .ta-hero-cta:focus {{
          outline: none;
        }}
        .ta-hero-cta:focus-visible {{
          outline: 2px solid rgba(15, 17, 20, 0.30);
          outline-offset: 2px;
        }}
        @media (max-width: 960px) {{
          .ta-hero-textbanner {{
            left: 14px;
            right: 14px;
            top: auto;
            bottom: 14px;
            transform: none;
            width: auto;
          }}
        }}

        /* Extra tightening for small/mobile screens */
        @media (max-width: 520px) {{
          .ta-hero-textbanner {{
            padding: 16px 14px 14px 14px;
            border-radius: 14px;
            background: rgba(255, 255, 255, 0.62);
          }}
          .ta-hero-textbanner-line1 {{
            font-size: 13px;
          }}
          .ta-hero-textbanner-line2 {{
            margin-top: 8px;
            font-size: 12.5px;
          }}
          .ta-hero-cta {{
            margin-top: 12px;
            padding: 9px 12px;
            font-size: 13px;
          }}
          .ta-hero .ta-hero-btn {{
            width: 36px;
            height: 36px;
            font-size: 18px;
          }}
          .ta-hero .ta-hero-btn.prev {{ left: 8px; }}
          .ta-hero .ta-hero-btn.next {{ right: 8px; }}
          .ta-hero .ta-hero-dots {{
            margin-top: 6px;
          }}
          .ta-hero-dots .ta-dot {{
            width: 6px;
            height: 6px;
            margin: 0 3px;
          }}
        }}

        .ta-hero-nav {{
          display: {show_controls};
          position: absolute;
          inset: 0;
          pointer-events: none;
        }}
        .ta-hero-btn {{
          pointer-events: auto;
          position: absolute;
          top: 50%;
          transform: translateY(-50%);
          width: 42px;
          height: 42px;
          border-radius: 999px;
          border: 1px solid rgba(0,0,0,0.08);
          background: rgba(255,255,255,0.85);
          color: rgba(0,0,0,0.75);
          display: flex;
          align-items: center;
          justify-content: center;
          cursor: pointer;
          font-size: 20px;
          box-shadow: 0 8px 20px rgba(0,0,0,0.10);
        }}
        .ta-hero-btn:hover {{
          background: rgba(255,255,255,0.95);
        }}
        .ta-hero-btn.prev {{ left: 10px; }}
        .ta-hero-btn.next {{ right: 10px; }}

        .ta-hero-dots {{
          display: {show_controls};
          text-align: center;
          margin-top: 10px;
        }}
        .ta-dot {{
          display: inline-block;
          width: 7px;
          height: 7px;
          margin: 0 4px;
          border-radius: 999px;
          background: rgba(0,0,0,0.20);
          cursor: pointer;
        }}
        .ta-dot.active {{
          background: rgba(0,0,0,0.55);
        }}
      </style>
    </head>
    <body>
      <div class="ta-hero">
        {slides_html}
        <div class="ta-hero-nav">
          <div class="ta-hero-btn prev" onclick="prevSlide()" aria-label="Previous slide">‹</div>
          <div class="ta-hero-btn next" onclick="nextSlide()" aria-label="Next slide">›</div>
        </div>
      </div>
      <div class="ta-hero-dots" id="dots"></div>

      <script>
        const slides = Array.from(document.querySelectorAll('.ta-hero-slide'));
        const dotsEl = document.getElementById('dots');
        let idx = 0;

        function _clickStreamlitTabByText(label) {{
          try {{
            const doc = window.parent.document;
            const tabs = Array.from(doc.querySelectorAll('button[role=\"tab\"]'));
            const target = tabs.find(b => ((b.innerText || '').trim() === label));
            if (target) {{
              target.click();
              return true;
            }}
          }} catch (e) {{
            console.warn('tab click failed', e);
          }}
          return false;
        }}

        function navigateToCombinedDashboard(evt) {{
          if (evt) {{
            evt.preventDefault();
            evt.stopPropagation();
          }}
          const clickedOuter = _clickStreamlitTabByText('Equity Research');
          const delay = clickedOuter ? 250 : 0;
          window.setTimeout(() => {{
            let tries = 0;
            const maxTries = 12;
            const timer = window.setInterval(() => {{
              tries += 1;
              const ok = _clickStreamlitTabByText('Combined Dashboard');
              if (ok || tries >= maxTries) {{
                window.clearInterval(timer);
              }}
            }}, 200);
          }}, delay);
        }}

        function renderDots() {{
          if (!dotsEl) return;
          dotsEl.innerHTML = '';
          slides.forEach((_, i) => {{
            const d = document.createElement('span');
            d.className = 'ta-dot' + (i === idx ? ' active' : '');
            d.onclick = () => goTo(i);
            dotsEl.appendChild(d);
          }});
        }}

        function show(i) {{
          slides.forEach((s, n) => s.classList.toggle('active', n === i));
          idx = i;
          renderDots();
        }}

        function nextSlide() {{
          show((idx + 1) % slides.length);
        }}

        function prevSlide() {{
          show((idx - 1 + slides.length) % slides.length);
        }}

        function goTo(i) {{
          show(i);
        }}

        // Init
        show(0);

        // Auto-rotate disabled: slides advance only via user navigation (arrows/dots)
                // Auto-resize Streamlit iframe height (prevents cropping on wide screens)
        (function () {{
          function sendHeight() {{
            const height = document.documentElement.scrollHeight;
            window.parent.postMessage(
              {{ isStreamlitMessage: true, type: "streamlit:setFrameHeight", height: height }},
              "*"
            );
          }}

          function start() {{
            sendHeight();
            if ("ResizeObserver" in window && document.body) {{
              const ro = new ResizeObserver(sendHeight);
              ro.observe(document.body);
            }} else {{
              // Fallback: ping a few times after load
              let n = 0;
              const t = setInterval(() => {{
                sendHeight();
                n += 1;
                if (n >= 12) clearInterval(t);
              }}, 250);
            }}
          }}

          window.addEventListener("load", start);
          window.addEventListener("resize", sendHeight);
        }})();
</script>
    </body>
    </html>
    """
    components.html(html, height=hero_iframe_height, scrolling=False)

def _slugify(value: str) -> str:
    """Convert an arbitrary filename/path into a stable, URL/DOM-safe id."""
    value = unicodedata.normalize("NFKD", value)
    value = value.encode("ascii", "ignore").decode("ascii")
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "-", value).strip("-")
    return value or "article"

def _humanize_filename(stem: str) -> str:
    """Turn a filename stem into a readable title."""
    # Replace underscores with spaces, collapse repeated whitespace, and trim.
    s = stem.replace("_", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _find_thumbnail_for_pdf(pdf_path: Path) -> Path:
    """Find a matching thumbnail for a given PDF.

    Convention: <pdf_stem>_thumb.<ext>
    """
    stem = pdf_path.stem
    for ext in (".png", ".jpg", ".jpeg", ".webp"):
        cand = pdf_path.with_name(f"{stem}_thumb{ext}")
        if cand.exists():
            return cand
    # Fallback (still renders a card even if thumbnail is missing)
    return _LOGO_PATH

def _get_featured_articles() -> List[Dict]:
    """Discover featured articles automatically from the ./assets folder.

    Rules:
    - Any *.pdf under ./assets (including ./assets/articles) is treated as a featured article.
    - Thumbnail is resolved by naming convention: <pdf_name>_thumb.png (also supports jpg/jpeg/webp).
      Example: my_article.pdf -> my_article_thumb.png
    """
    articles: List[Dict] = []

    if not _ASSETS_DIR.exists():
        return articles

    pdf_paths = sorted(_ASSETS_DIR.rglob("*.pdf"), key=lambda p: p.name.lower())
    for pdf_path in pdf_paths:
        if not pdf_path.is_file():
            continue

        # Use a stable id derived from the asset-relative path to avoid collisions.
        try:
            rel = pdf_path.relative_to(_ASSETS_DIR).as_posix()
        except Exception:
            rel = pdf_path.name
        article_id = _slugify(rel)

        articles.append(
            {
                "id": article_id,
                "title": _humanize_filename(pdf_path.stem),
                "pdf_path": pdf_path,
                "thumb_path": _find_thumbnail_for_pdf(pdf_path),
            }
        )

    return articles


def _render_featured_articles_carousel(articles: List[Dict]) -> None:
    """
    Horizontal, scrollable carousel of article cards.
    Clicking a card opens the PDF in a new browser tab.

    Implementation detail:
    - We DO NOT navigate directly to a huge `data:application/pdf;base64,...` URL.
      Some browsers/hosts will open a blank tab for large data URLs.
    - Instead, we open a `blob:` URL created client-side from the base64 PDF bytes.
      This is much more reliable and keeps the address bar sane.
    """
    # Filter only those where assets exist (prevents runtime surprises)
    safe_articles: List[Dict] = []
    for a in articles:
        if not a["pdf_path"].exists() or not a["thumb_path"].exists():
            continue
        safe_articles.append(a)

    if not safe_articles:
        st.info("No featured articles found under ./assets/articles yet.")
        return

    # Build card HTML + a JS map of {article_id: pdf_base64}
    cards_html: List[str] = []
    pdf_map = {}
    for a in safe_articles:
        article_id = a["id"]
        title = a["title"].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

        thumb_b64 = _file_to_b64(a["thumb_path"])
        pdf_b64 = _file_to_b64(a["pdf_path"])

        # Store raw base64 (NO data: prefix) so JS can build a Blob.
        pdf_map[article_id] = pdf_b64

        cards_html.append(
            f"""
            <button type="button" class="ta-article-card" onclick="openPdf('{article_id}')">
              <img class="ta-article-thumb" src="data:image/png;base64,{thumb_b64}" alt="{title}"/>
              <div class="ta-article-title">{title}</div>
            </button>
            """
        )

    pdf_map_json = json.dumps(pdf_map)

    html = f"""
    <html>
    <head>
      <style>
        .ta-articles {{
          width: 100%;
          max-width: 100%;
          margin: 0.25rem 0 0 0;
          padding: 0;
        }}

        .ta-scroll {{
          display: flex;
          gap: 18px;
          overflow-x: auto;
          padding: 6px 2px 12px 2px;
          scroll-snap-type: x mandatory;
          -webkit-overflow-scrolling: touch;
        }}
        .ta-scroll::-webkit-scrollbar {{
          height: 10px;
        }}

        .ta-article-card {{
          flex: 0 0 auto;
          width: 320px;
          border-radius: 16px;
          overflow: hidden;
          border: 1px solid rgba(0,0,0,0.08);
          box-shadow: 0 10px 24px rgba(0,0,0,0.06);
          background: #fff;
          scroll-snap-align: start;
          transition: transform 0.12s ease, box-shadow 0.12s ease;
          cursor: pointer;

          /* button reset */
          padding: 0;
          text-align: left;
          border: 1px solid rgba(0,0,0,0.08);
          outline: none;
        }}
        .ta-article-card:hover {{
          transform: translateY(-2px);
          box-shadow: 0 14px 30px rgba(0,0,0,0.10);
        }}

        .ta-article-thumb {{
          width: 100%;
          height: 180px;
          object-fit: cover;
          display: block;
          background: #f3f4f6;
        }}

        .ta-article-title {{
          padding: 12px 14px 14px 14px;
          font-family: -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
          font-size: 15px;
          font-weight: 650;
          line-height: 1.25;
        }}

        @media (max-width: 520px) {{
          .ta-scroll {{
            gap: 12px;
            padding: 6px 0 10px 0;
          }}

          .ta-article-card {{
            width: min(82vw, 320px);
            border-radius: 14px;
          }}

          .ta-article-thumb {{
            height: 160px;
          }}

          .ta-article-title {{
            padding: 10px 12px 12px 12px;
            font-size: 14px;
          }}
        }}
      </style>
      <script>
        const pdfMap = {pdf_map_json};

        function base64ToBlob(base64, contentType, sliceSize) {{
          contentType = contentType || "application/pdf";
          sliceSize = sliceSize || 1024;

          const byteCharacters = atob(base64);
          const byteArrays = [];

          for (let offset = 0; offset < byteCharacters.length; offset += sliceSize) {{
            const slice = byteCharacters.slice(offset, offset + sliceSize);
            const byteNumbers = new Array(slice.length);
            for (let i = 0; i < slice.length; i++) {{
              byteNumbers[i] = slice.charCodeAt(i);
            }}
            const byteArray = new Uint8Array(byteNumbers);
            byteArrays.push(byteArray);
          }}

          return new Blob(byteArrays, {{ type: contentType }});
        }}

        function openPdf(articleId) {{
          const b64 = pdfMap[articleId];
          if (!b64) {{
            alert("Sorry — that article could not be found.");
            return;
          }}

          const blob = base64ToBlob(b64, "application/pdf");
          const url = URL.createObjectURL(blob);

          // Open in a new tab.
          const w = window.open(url, "_blank", "noopener,noreferrer");
          if (!w) {{
            alert("Your browser blocked the popup. Please allow popups for this site and try again.");
          }}

          // Revoke after a bit (don't revoke immediately or the PDF may fail to load).
          setTimeout(() => URL.revokeObjectURL(url), 60 * 1000);
        }}
                // Auto-resize Streamlit iframe height (prevents cropping on wide screens)
        (function () {{
          function sendHeight() {{
            const height = document.documentElement.scrollHeight;
            window.parent.postMessage(
              {{ isStreamlitMessage: true, type: "streamlit:setFrameHeight", height: height }},
              "*"
            );
          }}

          function start() {{
            sendHeight();
            if ("ResizeObserver" in window && document.body) {{
              const ro = new ResizeObserver(sendHeight);
              ro.observe(document.body);
            }} else {{
              // Fallback: ping a few times after load
              let n = 0;
              const t = setInterval(() => {{
                sendHeight();
                n += 1;
                if (n >= 12) clearInterval(t);
              }}, 250);
            }}
          }}

          window.addEventListener("load", start);
          window.addEventListener("resize", sendHeight);
        }})();
</script>
    </head>
    <body>
      <div class="ta-articles">
        <div class="ta-scroll">
          {''.join(cards_html)}
        </div>
      </div>
    </body>
    </html>
    """
    components.html(html, height=260, scrolling=False)

def _render_home_body() -> None:
    # Hero banner carousel (3-slide carousel; CTA hooks reserved for future use)
    hero_images = [_HERO_BANNER_PATH, _HERO_BANNER_2_PATH, _HERO_BANNER_3_PATH]
    hero_meta: List[Dict] = [
        {
            "cta": None,
            "overlay_text": {
                "line1": "Select one or more company buckets and a year range to generate a volatility- and debt-aware composite score that blends P&L growth, balance-sheet strength, free-cash-flow momentum, and ROIC–WACC value creation.",
                "line2": "Get the ranking + the breakdown behind every score, with quick price-history sanity checks to spot the most consistent performers.",
            },
        },
        {
            "cta": {
                "kind": "navigate",
                "target": "combined_dashboard",
                "prepopulated_buckets": [
                    "Technology : Semiconductor",
                    "Technology : Semiconductor Equipment & Materials",
                ],
            }
        },
        {
            "cta": {
                "kind": "navigate",
                "target": "key_data",
                "subtab": "P&L Key Data",
            }
        },
    ]

    # Fallback: if the primary image isn't under assets yet, allow running from repo root.
    if not _HERO_BANNER_PATH.exists():
        alt = Path(__file__).parent / "Hero_Banner.png"
        if alt.exists():
            hero_images[0] = alt

    _render_hero_carousel(hero_images, slide_meta=hero_meta)

    st.markdown("### Featured Articles")
    st.caption("Click an article to open it in a new tab.")
    _render_featured_articles_carousel(_get_featured_articles())

def _render_equity_research_body() -> None:
    active_tab = lazy_tab_bar(
        [
            "Data Upload",
            "Value Creation Stability Score",
            "Quarterly Business Trend Score",
            "Through-the-Cycle Efficiency Score",
        ],
        key="equity_research_primary",
        default="Data Upload",
    )

    if active_tab == "Data Upload":
        from data_upload import render_data_upload_tab

        render_data_upload_tab()
        return

    if active_tab == "Value Creation Stability Score":
        active_value_tab = lazy_tab_bar(
            [
                "P&L Metrics Dashboard",
                "Balance Sheet Metrics and Dashboard",
                "Capital structure & Spread",
                "Cash Flow and Reinvestment",
                "Combined Dashboard",
                "Admin",
            ],
            key="value_creation_tabs",
            default="P&L Metrics Dashboard",
        )

        if active_value_tab == "P&L Metrics Dashboard":
            from pl_metrics import render_pl_metrics_tab

            render_pl_metrics_tab()
        elif active_value_tab == "Balance Sheet Metrics and Dashboard":
            from bs_metrics import render_balance_sheet_metrics_tab

            render_balance_sheet_metrics_tab()
        elif active_value_tab == "Capital structure & Spread":
            from cap_structure_cost import render_capital_structure_cost_of_capital_tab

            render_capital_structure_cost_of_capital_tab()
        elif active_value_tab == "Cash Flow and Reinvestment":
            from cash_flow_spread import render_cash_flow_and_spread_tab

            render_cash_flow_and_spread_tab()
        elif active_value_tab == "Combined Dashboard":
            from combined_dashboard import render_combined_dashboard_tab

            render_combined_dashboard_tab()
        elif active_value_tab == "Admin":
            from admin import render_admin_tab

            render_admin_tab()
        return

    if active_tab == "Quarterly Business Trend Score":
        from quarterly_business_trend import render_quarterly_business_trend_score_tab

        render_quarterly_business_trend_score_tab()
        return

    if active_tab == "Through-the-Cycle Efficiency Score":
        active_ttc_tab = lazy_tab_bar(
            [
                "Income Statement Efficiency Score",
                "Balance Sheet Strength Score",
                "Cash Flow Efficiency Score",
                "Working Capital Efficiency Score",
                "Combined Score",
                "Admin",
            ],
            key="ttc_tabs",
            default="Income Statement Efficiency Score",
        )

        if active_ttc_tab == "Income Statement Efficiency Score":
            from ttc_efficiency import render_through_the_cycle_income_statement_score_tab

            render_through_the_cycle_income_statement_score_tab()
        elif active_ttc_tab == "Balance Sheet Strength Score":
            from ttc_efficiency import render_through_the_cycle_balance_sheet_score_tab

            render_through_the_cycle_balance_sheet_score_tab()
        elif active_ttc_tab == "Cash Flow Efficiency Score":
            from ttc_efficiency import render_through_the_cycle_cash_flow_score_tab

            render_through_the_cycle_cash_flow_score_tab()
        elif active_ttc_tab == "Working Capital Efficiency Score":
            from ttc_efficiency import render_through_the_cycle_working_capital_score_tab

            render_through_the_cycle_working_capital_score_tab()
        elif active_ttc_tab == "Combined Score":
            from ttc_efficiency import render_through_the_cycle_combined_score_tab

            render_through_the_cycle_combined_score_tab()
        elif active_ttc_tab == "Admin":
            active_admin_tab = lazy_tab_bar(["Assumptions", "Formula"], key="ttc_admin_tabs", default="Assumptions")
            if active_admin_tab == "Assumptions":
                from ttc_efficiency import render_through_the_cycle_assumptions_tab

                render_through_the_cycle_assumptions_tab()
            else:
                from ttc_efficiency import render_through_the_cycle_formula_tab

                render_through_the_cycle_formula_tab()

def _render_key_data_body() -> None:
    from key_data import render_key_data_tab

    render_key_data_tab()

def _render_footer() -> None:
    # Intentionally empty for now (layout placeholder).
    st.markdown("", unsafe_allow_html=True)

active_top_tab = _render_header()

if active_top_tab == "Home":
    _render_home_body()
elif active_top_tab == "Key Data":
    _render_key_data_body()
elif active_top_tab == SEARCH_TAB_LABEL:
    from search_aggregate import render_search_aggregate_tab

    render_search_aggregate_tab()
elif active_top_tab == "Equity Research":
    _render_equity_research_body()
elif active_top_tab == "Valuations":
    from dcf_valuation import render_dcf_valuations_tab

    render_dcf_valuations_tab()

_render_footer()
