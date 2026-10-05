"""Percentage allocation controls and read-only score-weight visuals."""

from html import escape
import math

import streamlit as st

from core_backend import (
    get_score_weight_factors,
    normalize_score_weight_factors,
    save_score_weight_shares,
)


def render_score_weight_editor(conn, table: str, title: str, prefix: str) -> None:
    st.subheader(title)
    try:
        saved = get_score_weight_factors(conn, table, include_ids=True)
    except ValueError as error:
        st.error(str(error))
        return
    if saved.empty:
        st.info("No active weight factors found.")
        return

    # Percentage edits rerun immediately so implied values update before Save.
    shares = {}
    header_share, header_implied = st.columns([2, 1])
    header_share.markdown("**Share (%)**")
    header_implied.markdown("**Implied weight**")
    read_only = []
    for _, row in saved.iterrows():
        row_id = int(row["id"])
        share_column, implied_column = st.columns([2, 1])
        with share_column:
            shares[row_id] = st.number_input(
                str(row["factor"]), min_value=0.0, max_value=100.0,
                value=float(row["weight"]), step=0.01, format="%.6f",
                key=f"{prefix}_share_{row_id}",
            )
        read_only.append((row_id, implied_column))

    draft = saved.copy()
    draft["weight"] = [shares[int(row_id)] for row_id in draft["id"]]
    implied = normalize_score_weight_factors(draft)
    by_id = implied.set_index("id")["weight"].to_dict()
    for row_id, column in read_only:
        key = f"{prefix}_implied_{row_id}"
        st.session_state[key] = f"{by_id[row_id]:.6f}"
        with column:
            st.text_input("Implied weight", value=None, disabled=True,
                          label_visibility="hidden", key=key)

    total_share, total_implied = st.columns(2)
    total_share.metric("Total share", f"{math.fsum(shares.values()):.6f}%")
    total_implied.metric("Total implied weight", f"{math.fsum(by_id.values()):.6f}")
    label = "Save Growth Weights" if prefix == "growth" else "Save Standard Deviation Weights"
    if st.button(label, key=f"{prefix}_weights_save"):
        try:
            save_score_weight_shares(conn, table, shares)
        except ValueError as error:
            st.error(str(error))
        else:
            st.success(f"{title} saved.")


def render_score_weights_admin(conn) -> None:
    growth, stddev = st.columns(2)
    with growth:
        render_score_weight_editor(conn, "growth_weight_factors", "Growth weights", "growth")
    with stddev:
        render_score_weight_editor(conn, "stddev_weight_factors", "Standard deviation weights", "stddev")


def render_score_weight_summary(growth, stddev) -> None:
    """Display the calculation's saved weights without explanatory prose."""
    cards = []
    for title, frame, color in [("Growth weights", growth, "#6366f1"),
                                ("Standard deviation weights", stddev, "#0d9488")]:
        rows = []
        for _, row in frame.iterrows():
            weight = float(row["weight"])
            rows.append(
                '<div class="score-weight-row">'
                f'<span>{escape(str(row["factor"]))}</span><strong>{weight:.6f}</strong>'
                '<div class="score-weight-track">'
                f'<div style="width:{weight:.10f}%;background:{color}"></div></div></div>'
            )
        total = math.fsum(float(value) for value in frame["weight"])
        cards.append('<section class="score-weight-card">'
                     f'<div class="score-weight-heading"><strong>{title}</strong>'
                     f'<span class="score-weight-total">{total:.2f} / 100</span></div>'
                     + ''.join(rows) + '</section>')
    st.html('''<style>
        .score-weight-grid {display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px;margin:8px 0 18px}
        .score-weight-card {border:1px solid rgba(128,128,128,.25);border-radius:12px;padding:18px}
        .score-weight-heading {display:flex;justify-content:space-between;gap:12px;margin-bottom:16px}
        .score-weight-total {font-variant-numeric:tabular-nums;color:#64748b;white-space:nowrap}
        .score-weight-row {display:grid;grid-template-columns:1fr auto;gap:5px 12px;margin-top:12px;font-size:.9rem}
        .score-weight-row strong {font-variant-numeric:tabular-nums}
        .score-weight-track {grid-column:1 / -1;height:5px;border-radius:4px;background:rgba(128,128,128,.12);overflow:hidden}
        .score-weight-track div {height:100%;border-radius:4px}
        @media(max-width:640px) {.score-weight-grid {grid-template-columns:1fr}}
        </style><div class="score-weight-grid">''' + ''.join(cards) + '</div>')
