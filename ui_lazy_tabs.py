import re
from typing import Sequence

import streamlit as st


def _css_class_suffix(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]+", "-", value).strip("-")


def lazy_tab_bar(labels: Sequence[str], key: str, default: str | None = None) -> str:
    """Render a tab-like selector that only returns one active tab.

    Streamlit's native tabs eagerly render every tab body. This helper keeps the
    visual treatment close to the app's existing tab bar while allowing callers
    to render only the selected panel.
    """
    options = [str(label) for label in labels]
    if not options:
        raise ValueError("lazy_tab_bar requires at least one label")

    state_key = f"{key}_active"
    if st.session_state.get(state_key) not in options:
        st.session_state[state_key] = default if default in options else options[0]

    container_key = f"{key}_lazy_tabs"
    class_suffix = _css_class_suffix(container_key)
    st.markdown(
        f"""
        <style>
          .st-key-{class_suffix} [data-testid="stRadio"] > label {{
            display: none !important;
          }}

          .st-key-{class_suffix} [role="radiogroup"] {{
            display: flex !important;
            flex-direction: row !important;
            flex-wrap: nowrap !important;
            gap: 0.25rem !important;
            overflow-x: auto !important;
            -webkit-overflow-scrolling: touch !important;
            padding: 0 !important;
            border-bottom: 1px solid var(--tab-border-color) !important;
            scrollbar-width: thin !important;
          }}

          .st-key-{class_suffix} [role="radiogroup"] label {{
            flex: 0 0 auto !important;
            align-items: center !important;
            background: transparent !important;
            border: none !important;
            border-bottom: 2px solid transparent !important;
            border-radius: 0 !important;
            color: var(--tab-text-color) !important;
            cursor: pointer !important;
            font-family: var(--app-font-family) !important;
            font-size: 0.92rem !important;
            font-weight: 540 !important;
            line-height: 1.22 !important;
            margin: 0 !important;
            min-height: 2.4rem !important;
            padding: 0.66rem 0.9rem !important;
            white-space: nowrap !important;
            border-radius: 8px 8px 0 0 !important;
            transition: background-color 120ms ease, border-color 120ms ease, color 120ms ease !important;
          }}

          .st-key-{class_suffix} [role="radiogroup"] label:hover {{
            background: var(--tab-hover-bg) !important;
            color: var(--tab-hover-text-color) !important;
          }}

          .st-key-{class_suffix} [role="radiogroup"] label:has(input:checked) {{
            background: transparent !important;
            border-bottom-color: var(--tab-selected-border-color) !important;
            color: var(--tab-selected-text-color) !important;
            font-weight: 625 !important;
          }}

          .st-key-{class_suffix} [role="radiogroup"] label:focus-within {{
            outline: 2px solid var(--app-focus-ring) !important;
            outline-offset: -2px !important;
          }}

          .st-key-{class_suffix} [role="radiogroup"] label [data-testid="stMarkdownContainer"] p {{
            color: inherit !important;
            font-family: var(--app-font-family) !important;
            font-size: 0.92rem !important;
            font-weight: inherit !important;
            line-height: 1.22 !important;
            margin: 0 !important;
          }}

          .st-key-app_top_nav_lazy_tabs [role="radiogroup"] {{
            margin-top: 0.6rem !important;
          }}

          .st-key-app_top_nav_lazy_tabs [role="radiogroup"] label {{
            font-size: 0.96rem !important;
            min-height: 2.65rem !important;
            padding: 0.78rem 1rem !important;
          }}

          .st-key-app_top_nav_lazy_tabs [role="radiogroup"] label [data-testid="stMarkdownContainer"] p {{
            font-size: 0.96rem !important;
          }}

          .st-key-{class_suffix} [role="radiogroup"] input[type="radio"],
          .st-key-{class_suffix} [role="radiogroup"] [data-baseweb="radio"] > div:first-child,
          .st-key-{class_suffix} [role="radiogroup"] [data-testid="stRadio"] [data-testid="stMarkdownContainer"] + div,
          .st-key-{class_suffix} [role="radiogroup"] label > div:first-child:empty,
          .st-key-{class_suffix} [role="radiogroup"] label > div:first-child:not([data-testid="stMarkdownContainer"]) {{
            display: none !important;
            opacity: 0 !important;
            pointer-events: none !important;
            width: 0 !important;
            min-width: 0 !important;
            height: 0 !important;
            margin: 0 !important;
            padding: 0 !important;
          }}

          .st-key-{class_suffix} [role="radiogroup"] label > div:first-child {{
            display: none !important;
          }}

          @media (max-width: 640px) {{
            .st-key-{class_suffix} [role="radiogroup"] {{
              gap: 0.45rem !important;
              padding-left: 0 !important;
              padding-right: 0 !important;
            }}
            .st-key-{class_suffix} [role="radiogroup"] label {{
              min-height: 2.75rem !important;
            }}
          }}
        </style>
        """,
        unsafe_allow_html=True,
    )

    with st.container(key=container_key):
        selected = st.radio(
            "Tab",
            options=options,
            index=options.index(st.session_state[state_key]),
            horizontal=True,
            label_visibility="collapsed",
            key=state_key,
        )
    return str(selected)
