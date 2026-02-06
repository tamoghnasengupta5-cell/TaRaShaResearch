from __future__ import annotations

from typing import Dict, List, Tuple
import re

import numpy as np
import pandas as pd
import streamlit as st

from core import (
    TTC_SECTIONS,
    get_db,
    get_ttc_assumptions,
    list_companies,
    read_df,
    replace_ttc_assumptions_section,
)

_SECTIONS = TTC_SECTIONS


def _slug(text: str) -> str:
    return (
        text.lower()
        .replace("&", "and")
        .replace("/", " ")
        .replace("(", " ")
        .replace(")", " ")
        .replace("%", "pct")
        .replace("–", "-")
        .replace("—", "-")
        .replace("  ", " ")
        .strip()
        .replace(" ", "_")
    )


def _parse_assumptions_sections(conn) -> Dict[str, List[Dict[str, object]]]:
    return get_ttc_assumptions(conn, _SECTIONS)


def _get_section_df(section: str, defaults: List[Dict[str, object]]) -> pd.DataFrame:
    key = f"ttc_saved_{_slug(section)}"
    if key not in st.session_state:
        st.session_state[key] = pd.DataFrame(defaults)
    return st.session_state[key]


def _get_work_df(section: str, defaults: List[Dict[str, object]]) -> Tuple[str, str, pd.DataFrame]:
    section_key = _slug(section)
    saved_key = f"ttc_saved_{section_key}"
    work_key = f"ttc_work_{section_key}"
    if saved_key not in st.session_state:
        st.session_state[saved_key] = pd.DataFrame(defaults)
    if work_key not in st.session_state:
        st.session_state[work_key] = st.session_state[saved_key].copy()
    return saved_key, work_key, st.session_state[work_key]


def _ensure_ids(df: pd.DataFrame, section_key: str) -> pd.DataFrame:
    df = df.copy()
    if "_id" not in df.columns or df["_id"].isna().any():
        df["_id"] = list(range(1, len(df) + 1))
    df["_id"] = df["_id"].astype(int)
    next_id_key = f"ttc_next_id_{section_key}"
    if next_id_key not in st.session_state:
        st.session_state[next_id_key] = int(df["_id"].max()) + 1 if len(df) else 1
    return df


def _normalize_label(label: str) -> str:
    if label is None:
        return ""
    return (
        str(label)
        .lower()
        .replace("&", "and")
        .replace(",", "")
        .replace("(", "")
        .replace(")", "")
        .replace("-", "")
        .replace(" ", "")
    )


def _get_income_statement_params(sections: Dict[str, List[Dict[str, object]]]) -> Dict[str, Tuple[float, float]]:
    defaults = sections.get("Income Statement Efficiency Score", [])
    saved_key = f"ttc_saved_{_slug('Income Statement Efficiency Score')}"
    df = st.session_state.get(saved_key)
    if df is None or df.empty:
        df = pd.DataFrame(defaults)

    params = {
        "operating_margin": (0.0, 0.0),
        "gross_margin": (0.0, 0.0),
        "sga_ratio": (0.0, 0.0),
        "incremental_margin": (0.0, 0.0),
        "op_margin_volatility": (0.0, 0.0),
    }

    for _, row in df.iterrows():
        name = str(row.get("Metric/Component", "")).strip()
        w = float(row.get("Weight", 0.0) or 0.0)
        t = float(row.get("Threshold", 0.0) or 0.0)
        n = _normalize_label(name)

        if "volatility" in n or "stddev" in n:
            params["op_margin_volatility"] = (w, t)
        elif "operatingmargin" in n:
            params["operating_margin"] = (w, t)
        elif "grossmargin" in n:
            params["gross_margin"] = (w, t)
        elif "sga" in n or "sellinggeneral" in n:
            params["sga_ratio"] = (w, t)
        elif "incrementalmargin" in n:
            params["incremental_margin"] = (w, t)

    return params


def _get_balance_sheet_params(sections: Dict[str, List[Dict[str, object]]]) -> Dict[str, Tuple[float, float]]:
    defaults = sections.get("Balance Sheet Strength Score", [])
    saved_key = f"ttc_saved_{_slug('Balance Sheet Strength Score')}"
    df = st.session_state.get(saved_key)
    if df is None or df.empty:
        df = pd.DataFrame(defaults)

    params = {
        "net_debt_ebitda": (0.0, 0.0),
        "interest_coverage": (0.0, 0.0),
        "quick_ratio": (0.0, 0.0),
        "current_ratio": (0.0, 0.0),
        "debt_to_capitalization": (0.0, 0.0),
        "debt_maturity_pressure": (0.0, 0.0),
    }

    for _, row in df.iterrows():
        name = str(row.get("Metric/Component", "")).strip()
        w = float(row.get("Weight", 0.0) or 0.0)
        t = float(row.get("Threshold", 0.0) or 0.0)
        n = _normalize_label(name)

        if "netdebt" in n and "ebitda" in n:
            params["net_debt_ebitda"] = (w, t)
        elif "interestcoverage" in n:
            params["interest_coverage"] = (w, t)
        elif "quickratio" in n:
            params["quick_ratio"] = (w, t)
        elif "currentratio" in n:
            params["current_ratio"] = (w, t)
        elif "debt" in n and "capital" in n:
            params["debt_to_capitalization"] = (w, t)
        elif "maturity" in n:
            params["debt_maturity_pressure"] = (w, t)

    return params


def _parse_year_range(s: str, available_years: List[int]) -> Tuple[int, int]:
    s = (s or "").strip()
    if not available_years:
        raise ValueError("No annual years available.")
    most_recent = max(available_years)
    m_recent = re.match(r"^recent\s*[-–]\s*(\d{4})$", s, flags=re.IGNORECASE)
    m_two = re.match(r"^(\d{4})\s*[-–]\s*(\d{4})$", s)
    if m_recent:
        end = int(m_recent.group(1))
        return most_recent, end
    if m_two:
        start, end = int(m_two.group(1)), int(m_two.group(2))
        return start, end
    raise ValueError("Could not parse the year range. Use 'Recent - YYYY' or 'YYYY-YYYY'.")


def _median(values: List[float]) -> float | None:
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    return float(np.median(vals))


def _stdev_sample(values: List[float]) -> float:
    vals = [v for v in values if v is not None]
    if len(vals) < 2:
        return 0.0
    return float(np.std(vals, ddof=1))


def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def _score_income_statement(
    op_margin_med: float | None,
    op_margin_stdev: float | None,
    gross_margin_med: float | None,
    sga_ratio_med: float | None,
    incremental_margin_med: float | None,
    params: Dict[str, Tuple[float, float]],
) -> float | None:
    if (
        op_margin_med is None
        or gross_margin_med is None
        or sga_ratio_med is None
        or incremental_margin_med is None
        or op_margin_stdev is None
    ):
        return None

    def ratio_term(val: float, threshold: float) -> float:
        if threshold <= 0:
            return 0.0
        return _clamp01(max(0.0, val) / threshold)

    def inverse_term(val: float, threshold: float) -> float:
        if threshold <= 0:
            return 0.0
        return _clamp01(1.0 - (max(0.0, val) / threshold))

    w_op, t_op = params["operating_margin"]
    w_gm, t_gm = params["gross_margin"]
    w_sga, t_sga = params["sga_ratio"]
    w_inc, t_inc = params["incremental_margin"]
    w_vol, t_vol = params["op_margin_volatility"]

    score_raw = (
        w_op * ratio_term(op_margin_med, t_op)
        + w_gm * ratio_term(gross_margin_med, t_gm)
        + w_sga * inverse_term(sga_ratio_med, t_sga)
        + w_inc * ratio_term(incremental_margin_med, t_inc)
        + w_vol * inverse_term(op_margin_stdev, t_vol)
    )

    return 100.0 * _clamp01(score_raw)


def _score_balance_sheet(
    net_debt_ebitda_med: float | None,
    interest_coverage_med: float | None,
    quick_ratio_med: float | None,
    current_ratio_med: float | None,
    debt_to_cap_med: float | None,
    debt_maturity_med: float | None,
    nd_penalty: bool,
    params: Dict[str, Tuple[float, float]],
) -> float | None:
    if (
        net_debt_ebitda_med is None
        or interest_coverage_med is None
        or quick_ratio_med is None
        or current_ratio_med is None
        or debt_to_cap_med is None
        or debt_maturity_med is None
    ):
        return None

    def ratio_term(val: float, threshold: float) -> float:
        if threshold <= 0:
            return 0.0
        return _clamp01(val / threshold)

    def inverse_term(val: float, threshold: float) -> float:
        if threshold <= 0:
            return 0.0
        return _clamp01(1.0 - (val / threshold))

    w_nd, t_nd = params["net_debt_ebitda"]
    w_ic, t_ic = params["interest_coverage"]
    w_qr, t_qr = params["quick_ratio"]
    w_cr, t_cr = params["current_ratio"]
    w_dc, t_dc = params["debt_to_capitalization"]
    w_dm, t_dm = params["debt_maturity_pressure"]

    nd_term = 0.0 if nd_penalty else inverse_term(max(0.0, net_debt_ebitda_med), t_nd)

    score_raw = (
        w_nd * nd_term
        + w_ic * ratio_term(max(0.0, interest_coverage_med), t_ic)
        + w_qr * ratio_term(max(0.0, quick_ratio_med), t_qr)
        + w_cr * ratio_term(max(0.0, current_ratio_med), t_cr)
        + w_dc * inverse_term(max(0.0, debt_to_cap_med), t_dc)
        + w_dm * inverse_term(max(0.0, debt_maturity_med), t_dm)
    )

    return 100.0 * _clamp01(score_raw)


def _load_series(conn, table: str, value_col: str, company_id: int) -> Dict[int, float]:
    df = read_df(
        f"SELECT fiscal_year AS year, {value_col} AS value FROM {table} WHERE company_id = ?",
        conn,
        params=(company_id,),
    )
    if df is None or df.empty:
        return {}
    return {int(r["year"]): float(r["value"]) for _, r in df.iterrows() if pd.notna(r["value"])}


def _get_company_buckets(conn, company_ids: List[int]) -> Dict[int, str]:
    if not company_ids:
        return {}
    placeholders = ",".join(["?"] * len(company_ids))
    df = read_df(
        f"""
        SELECT m.company_id, g.name
        FROM company_group_members m
        JOIN company_groups g ON g.id = m.group_id
        WHERE m.company_id IN ({placeholders})
        ORDER BY g.name
        """,
        conn,
        params=company_ids,
    )
    buckets: Dict[int, List[str]] = {}
    if df is not None and not df.empty:
        for _, row in df.iterrows():
            cid = int(row["company_id"])
            buckets.setdefault(cid, []).append(str(row["name"]))
    return {cid: ", ".join(names) for cid, names in buckets.items()}


def render_through_the_cycle_income_statement_score_tab() -> None:
    conn = get_db()
    companies_df = list_companies(conn)
    if companies_df.empty:
        st.info("No companies in the database yet. Upload a spreadsheet under Equity Research → Data Upload.")
        return

    mode = st.radio(
        "Analyze by",
        ["Company", "Industry Bucket"],
        horizontal=True,
        key="ttc_income_statement_mode",
    )

    company_ids: List[int] = []

    if mode == "Company":
        options = [
            f"{row['name']} ({row['ticker']}) [id={row['id']}]"
            for _, row in companies_df.iterrows()
        ]
        selected = st.multiselect(
            "Select one or more companies",
            options=options,
            key="ttc_income_statement_companies",
        )
        for label in selected:
            m = re.search(r"id=(\d+)\]$", label)
            if m:
                company_ids.append(int(m.group(1)))
    else:
        groups_df = read_df(
            "SELECT id, name FROM company_groups ORDER BY name",
            conn,
        )
        if groups_df.empty:
            st.info("No industry buckets found yet.")
            return
        group_name_to_id = {str(row["name"]): int(row["id"]) for _, row in groups_df.iterrows()}
        bucket_names_selected = st.multiselect(
            "Select one or more industry buckets",
            options=list(group_name_to_id.keys()),
            key="ttc_income_statement_buckets",
        )
        if bucket_names_selected:
            group_ids = [group_name_to_id[name] for name in bucket_names_selected if name in group_name_to_id]
            placeholders = ",".join(["?"] * len(group_ids))
            bucket_members_df = read_df(
                f"SELECT DISTINCT company_id FROM company_group_members WHERE group_id IN ({placeholders})",
                conn,
                params=group_ids,
            )
            if bucket_members_df is not None and not bucket_members_df.empty:
                company_ids = [int(x) for x in bucket_members_df["company_id"].tolist()]

    if not company_ids:
        st.info("Select at least one company or industry bucket to compute scores.")
        return
    company_ids = sorted(set(company_ids))

    year_range = st.text_input(
        "Year range (e.g., 'Recent - 2020' or '2023-2018')",
        value="Recent - 2020",
        key="ttc_income_statement_year_range",
    )

    view_mode = st.radio(
        "View",
        ["Dashboard View", "Company Detailed View"],
        horizontal=True,
        key="ttc_income_statement_view_mode",
    )

    compute = st.button("Compute Score", type="primary", key="ttc_income_statement_compute")
    if not compute:
        return

    sections = _parse_assumptions_sections(conn)
    if not sections.get("Income Statement Efficiency Score"):
        st.error("No TTC assumptions found in the database. Add them in the Assumptions tab first.")
        return
    params = _get_income_statement_params(sections)

    buckets_map = _get_company_buckets(conn, company_ids)
    company_lookup = {int(row["id"]): row for _, row in companies_df.iterrows()}

    rows: List[Dict[str, object]] = []

    for cid in company_ids:
        row = company_lookup.get(cid)
        if row is None:
            continue

        revenue = _load_series(conn, "revenues_annual", "revenue", cid)
        if not revenue:
            continue

        cogs = _load_series(conn, "cost_of_revenue_annual", "cost_of_revenue", cid)
        sga = _load_series(conn, "sga_annual", "sga", cid)

        op_income = _load_series(conn, "operating_income_annual", "operating_income", cid)
        if not op_income:
            op_income = _load_series(conn, "ebit_annual", "ebit", cid)

        available_years = sorted(revenue.keys())
        try:
            yr_start, yr_end = _parse_year_range(year_range, available_years)
        except ValueError as e:
            st.error(str(e))
            return

        if yr_start < yr_end:
            yr_start, yr_end = yr_end, yr_start

        years_in_range = [y for y in available_years if yr_end <= y <= yr_start]

        op_margin_vals: List[float] = []
        gross_margin_vals: List[float] = []
        sga_ratio_vals: List[float] = []
        incremental_vals: List[float] = []

        for y in years_in_range:
            rev = revenue.get(y)
            oi = op_income.get(y)
            if rev is not None and rev != 0 and oi is not None:
                op_margin_vals.append(float(oi) / float(rev))

            c = cogs.get(y)
            if rev is not None and rev != 0 and c is not None:
                gross_margin_vals.append((float(rev) - float(c)) / float(rev))

            sg = sga.get(y)
            if rev is not None and rev != 0 and sg is not None:
                sga_ratio_vals.append(float(sg) / float(rev))

            if (y - 1) in revenue and (y - 1) in op_income and rev is not None and oi is not None:
                rev_prev = revenue.get(y - 1)
                oi_prev = op_income.get(y - 1)
                if rev_prev is not None and oi_prev is not None:
                    denom = float(rev) - float(rev_prev)
                    if denom != 0:
                        incremental_vals.append((float(oi) - float(oi_prev)) / denom)

        op_margin_med = _median(op_margin_vals)
        op_margin_std = _stdev_sample(op_margin_vals)
        gross_margin_med = _median(gross_margin_vals)
        sga_ratio_med = _median(sga_ratio_vals)
        incremental_med = _median(incremental_vals)

        score = _score_income_statement(
            op_margin_med,
            op_margin_std,
            gross_margin_med,
            sga_ratio_med,
            incremental_med,
            params,
        )

        rows.append(
            {
                "Ticker": row["ticker"],
                "Company Name": row["name"],
                "Industry Bucket": buckets_map.get(cid, "(no bucket)"),
                "Operating Margin %": op_margin_med,
                "Operating Margin Standard Deviation": op_margin_std,
                "Gross Margin %": gross_margin_med,
                "SG&A Ratio": sga_ratio_med,
                "Incremental Margin % [Pricing Power + Operating Leverage]": incremental_med,
                "Income Statement Efficiency Score (0-100)": score,
            }
        )

    if not rows:
        st.info("No scores could be computed for the selected input.")
        return

    df = pd.DataFrame(rows)
    df = df.sort_values(
        "Income Statement Efficiency Score (0-100)",
        ascending=False,
        na_position="last",
    )

    if view_mode == "Dashboard View":
        show_cols = [
            "Ticker",
            "Company Name",
            "Industry Bucket",
            "Income Statement Efficiency Score (0-100)",
        ]
    else:
        show_cols = [
            "Ticker",
            "Company Name",
            "Industry Bucket",
            "Operating Margin %",
            "Operating Margin Standard Deviation",
            "Gross Margin %",
            "SG&A Ratio",
            "Incremental Margin % [Pricing Power + Operating Leverage]",
            "Income Statement Efficiency Score (0-100)",
        ]

    if view_mode == "Company Detailed View":
        display_df = df.copy()
        for col in [
            "Operating Margin %",
            "Operating Margin Standard Deviation",
            "Gross Margin %",
            "SG&A Ratio",
            "Incremental Margin % [Pricing Power + Operating Leverage]",
        ]:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(
                    lambda v: f"{v * 100:.2f}%" if pd.notna(v) else ""
                )
        st.dataframe(
            display_df[show_cols],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.dataframe(
            df[show_cols],
            use_container_width=True,
            hide_index=True,
        )



def render_through_the_cycle_balance_sheet_score_tab() -> None:
    conn = get_db()
    companies_df = list_companies(conn)
    if companies_df.empty:
        st.info("No companies in the database yet. Upload a spreadsheet under Equity Research → Data Upload.")
        return

    mode = st.radio(
        "Analyze by",
        ["Company", "Industry Bucket"],
        horizontal=True,
        key="ttc_balance_sheet_mode",
    )

    company_ids: List[int] = []

    if mode == "Company":
        options = [
            f"{row['name']} ({row['ticker']}) [id={row['id']}]"
            for _, row in companies_df.iterrows()
        ]
        selected = st.multiselect(
            "Select one or more companies",
            options=options,
            key="ttc_balance_sheet_companies",
        )
        for label in selected:
            m = re.search(r"id=(\d+)\]$", label)
            if m:
                company_ids.append(int(m.group(1)))
    else:
        groups_df = read_df(
            "SELECT id, name FROM company_groups ORDER BY name",
            conn,
        )
        if groups_df.empty:
            st.info("No industry buckets found yet.")
            return
        group_name_to_id = {str(row["name"]): int(row["id"]) for _, row in groups_df.iterrows()}
        bucket_names_selected = st.multiselect(
            "Select one or more industry buckets",
            options=list(group_name_to_id.keys()),
            key="ttc_balance_sheet_buckets",
        )
        if bucket_names_selected:
            group_ids = [group_name_to_id[name] for name in bucket_names_selected if name in group_name_to_id]
            placeholders = ",".join(["?"] * len(group_ids))
            bucket_members_df = read_df(
                f"SELECT DISTINCT company_id FROM company_group_members WHERE group_id IN ({placeholders})",
                conn,
                params=group_ids,
            )
            if bucket_members_df is not None and not bucket_members_df.empty:
                company_ids = [int(x) for x in bucket_members_df["company_id"].tolist()]

    if not company_ids:
        st.info("Select at least one company or industry bucket to compute scores.")
        return
    company_ids = sorted(set(company_ids))

    year_range = st.text_input(
        "Year range (e.g., 'Recent - 2020' or '2023-2018')",
        value="Recent - 2020",
        key="ttc_balance_sheet_year_range",
    )

    view_mode = st.radio(
        "View",
        ["Dashboard View", "Company Detailed View"],
        horizontal=True,
        key="ttc_balance_sheet_view_mode",
    )

    compute = st.button("Compute Score", type="primary", key="ttc_balance_sheet_compute")
    if not compute:
        return

    sections = _parse_assumptions_sections(conn)
    if not sections.get("Balance Sheet Strength Score"):
        st.error("No TTC assumptions found in the database. Add them in the Assumptions tab first.")
        return
    params = _get_balance_sheet_params(sections)

    buckets_map = _get_company_buckets(conn, company_ids)
    company_lookup = {int(row["id"]): row for _, row in companies_df.iterrows()}

    rows: List[Dict[str, object]] = []

    for cid in company_ids:
        row = company_lookup.get(cid)
        if row is None:
            continue

        total_debt = _load_series(conn, "total_debt_annual", "total_debt", cid)
        if not total_debt:
            continue

        cash = _load_series(conn, "cash_and_cash_equivalents_annual", "cash_and_cash_equivalents", cid)
        short_term_inv = _load_series(conn, "short_term_investments_annual", "short_term_investments", cid)
        accounts_receivable = _load_series(conn, "accounts_receivable_annual", "accounts_receivable", cid)
        current_assets = _load_series(conn, "total_current_assets_annual", "total_current_assets", cid)
        current_liabilities = _load_series(conn, "total_current_liabilities_annual", "total_current_liabilities", cid)
        current_debt = _load_series(conn, "current_debt_annual", "current_debt", cid)
        shareholders_equity = _load_series(conn, "shareholders_equity_annual", "shareholders_equity", cid)
        ebitda = _load_series(conn, "ebitda_annual", "ebitda", cid)
        op_income = _load_series(conn, "operating_income_annual", "operating_income", cid)
        interest_expense = _load_series(conn, "interest_expense_annual", "interest_expense", cid)

        available_years = sorted(
            set(total_debt.keys())
            | set(cash.keys())
            | set(current_assets.keys())
            | set(current_liabilities.keys())
        )
        if not available_years:
            continue

        try:
            yr_start, yr_end = _parse_year_range(year_range, available_years)
        except ValueError as e:
            st.error(str(e))
            return

        if yr_start < yr_end:
            yr_start, yr_end = yr_end, yr_start

        years_in_range = [y for y in available_years if yr_end <= y <= yr_start]

        net_debt_ebitda_vals: List[float] = []
        interest_coverage_vals: List[float] = []
        quick_ratio_vals: List[float] = []
        current_ratio_vals: List[float] = []
        debt_to_cap_vals: List[float] = []
        debt_maturity_vals: List[float] = []
        nd_penalty = False
        ic_penalty = False

        for y in years_in_range:
            td = total_debt.get(y)
            if td is not None:
                cash_val = cash.get(y, 0.0)
                ebitda_val = ebitda.get(y)
                net_debt = float(td) - float(cash_val)
                if ebitda_val is not None and float(ebitda_val) <= 0 and net_debt > 0:
                    nd_penalty = True
                if ebitda_val is not None and ebitda_val != 0:
                    net_debt_ebitda_vals.append(net_debt / float(ebitda_val))

                se_val = shareholders_equity.get(y)
                if se_val is not None:
                    denom = float(td) + float(se_val)
                    if denom != 0:
                        debt_to_cap_vals.append(float(td) / denom)

                cd_val = current_debt.get(y)
                if cd_val is not None:
                    if float(td) == 0 or float(cd_val) == 0:
                        debt_maturity_vals.append(0.0)
                    else:
                        debt_maturity_vals.append(float(cd_val) / float(td))

            oi_val = op_income.get(y)
            ie_val = interest_expense.get(y)
            if oi_val is not None and float(oi_val) < 0:
                ic_penalty = True
            if oi_val is not None and ie_val is not None:
                if float(ie_val) != 0:
                    interest_coverage_vals.append(float(oi_val) / float(ie_val))
                else:
                    interest_coverage_vals.append(100.0 if float(oi_val) > 0 else 0.0)

            ca_val = current_assets.get(y)
            cl_val = current_liabilities.get(y)
            if ca_val is not None and cl_val is not None and float(cl_val) != 0:
                current_ratio_vals.append(float(ca_val) / float(cl_val))

            if cl_val is not None and float(cl_val) != 0:
                cash_val = cash.get(y, 0.0)
                ar_val = accounts_receivable.get(y, 0.0)
                quick_ratio_vals.append((float(cash_val) + float(ar_val)) / float(cl_val))

        net_debt_ebitda_med = _median(net_debt_ebitda_vals)
        interest_coverage_med = _median(interest_coverage_vals)
        quick_ratio_med = _median(quick_ratio_vals)
        current_ratio_med = _median(current_ratio_vals)
        debt_to_cap_med = _median(debt_to_cap_vals)
        debt_maturity_med = _median(debt_maturity_vals)

        score = _score_balance_sheet(
            net_debt_ebitda_med,
            interest_coverage_med,
            quick_ratio_med,
            current_ratio_med,
            debt_to_cap_med,
            debt_maturity_med,
            nd_penalty,
            params,
        )
        penalty_notes: List[str] = []
        if nd_penalty:
            penalty_notes.append("Penalty applied: EBITDA \u2264 0 with positive net debt")
        if ic_penalty:
            penalty_notes.append("Penalty applied: Operating income (EBIT) < 0")
        penalty_note = "; ".join(penalty_notes)

        rows.append(
            {
                "Ticker": row["ticker"],
                "Company Name": row["name"],
                "Industry Bucket": buckets_map.get(cid, "(no bucket)"),
                "Net Debt/EBITDA": net_debt_ebitda_med,
                "Interest Coverage": interest_coverage_med,
                "Quick Ratio": quick_ratio_med,
                "Current Ratio": current_ratio_med,
                "Debt to Capitalization": debt_to_cap_med,
                "Debt Maturity Pressure": debt_maturity_med,
                "Balance Sheet Strength Score": score,
                "Penalty Applied": nd_penalty or ic_penalty,
                "Penalty Note": penalty_note,
            }
        )

    if not rows:
        st.info("No scores could be computed for the selected input.")
        return

    df = pd.DataFrame(rows)
    df = df.sort_values(
        "Balance Sheet Strength Score",
        ascending=False,
        na_position="last",
    )

    if view_mode == "Dashboard View":
        show_cols = [
            "Ticker",
            "Company Name",
            "Industry Bucket",
            "Balance Sheet Strength Score",
        ]
    else:
        show_cols = [
            "Ticker",
            "Company Name",
            "Industry Bucket",
            "Net Debt/EBITDA",
            "Interest Coverage",
            "Quick Ratio",
            "Current Ratio",
            "Debt to Capitalization",
            "Debt Maturity Pressure",
            "Penalty Note",
            "Balance Sheet Strength Score",
        ]

    display_df = df[show_cols].copy()
    display_df["Penalty Applied"] = df["Penalty Applied"].values

    def _style_penalty(row):
        if row.get("Penalty Applied"):
            return [
                "color: #c62828; font-weight: 700"
                if col == "Balance Sheet Strength Score"
                else ""
                for col in row.index
            ]
        return [""] * len(row.index)

    styler = display_df.style.apply(_style_penalty, axis=1)
    if hasattr(styler, "hide"):
        styler = styler.hide(axis="columns", subset=["Penalty Applied"])
    elif hasattr(styler, "hide_columns"):
        styler = styler.hide_columns(["Penalty Applied"])

    st.dataframe(
        styler,
        use_container_width=True,
        hide_index=True,
    )


def render_through_the_cycle_assumptions_tab() -> None:
    conn = get_db()
    sections = _parse_assumptions_sections(conn)
    if all(not rows for rows in sections.values()):
        st.info("No TTC assumptions saved yet. Add metrics below to create them.")
    tabs = st.tabs(_SECTIONS)

    for tab, section in zip(tabs, _SECTIONS):
        with tab:
            defaults = sections.get(section, [])
            section_key = _slug(section)
            saved_key, work_key, work_df = _get_work_df(section, defaults)
            work_df = _ensure_ids(work_df, section_key)

            if "Delete" not in work_df.columns:
                work_df["Delete"] = False

            st.caption("Use +/- knobs to adjust Weight and Threshold. Edit metric names or units directly.")

            updated_rows: List[Dict[str, object]] = []
            work_df = work_df.sort_values("_id")
            for _, row in work_df.iterrows():
                rid = int(row["_id"])
                cols = st.columns([4.2, 0.7, 1.0, 0.7, 0.7, 1.0, 0.7, 1.4, 0.8])

                metric_val = cols[0].text_input(
                    "Metric/Component",
                    value=str(row.get("Metric/Component", "")),
                    label_visibility="collapsed",
                    key=f"{section_key}_metric_{rid}",
                )

                weight_val = float(row.get("Weight", 0.0) or 0.0)
                threshold_val = float(row.get("Threshold", 0.0) or 0.0)

                if cols[1].button("−", key=f"{section_key}_wminus_{rid}", use_container_width=True):
                    weight_val = max(0.0, weight_val - 0.01)
                cols[2].markdown(f"**{weight_val:.4f}**")
                if cols[3].button("＋", key=f"{section_key}_wplus_{rid}", use_container_width=True):
                    weight_val = weight_val + 0.01

                if cols[4].button("−", key=f"{section_key}_tminus_{rid}", use_container_width=True):
                    threshold_val = max(0.0, threshold_val - 0.01)
                cols[5].markdown(f"**{threshold_val:.4f}**")
                if cols[6].button("＋", key=f"{section_key}_tplus_{rid}", use_container_width=True):
                    threshold_val = threshold_val + 0.01

                units_val = cols[7].text_input(
                    "Units",
                    value=str(row.get("Units", "")),
                    label_visibility="collapsed",
                    key=f"{section_key}_units_{rid}",
                )

                delete_val = cols[8].checkbox(
                    "Delete",
                    value=bool(row.get("Delete", False)),
                    key=f"{section_key}_delete_{rid}",
                )

                updated_rows.append(
                    {
                        "_id": rid,
                        "Metric/Component": metric_val,
                        "Weight": weight_val,
                        "Threshold": threshold_val,
                        "Units": units_val,
                        "Delete": delete_val,
                    }
                )

            updated_df = pd.DataFrame(updated_rows)
            st.session_state[work_key] = updated_df

            add_col, save_col = st.columns([1, 1])
            if add_col.button("Add metric", key=f"{section_key}_add_row"):
                next_id_key = f"ttc_next_id_{section_key}"
                next_id = int(st.session_state.get(next_id_key, 1))
                new_row = {
                    "_id": next_id,
                    "Metric/Component": "",
                    "Weight": 0.0,
                    "Threshold": 0.0,
                    "Units": "",
                    "Delete": False,
                }
                st.session_state[next_id_key] = next_id + 1
                st.session_state[work_key] = pd.concat(
                    [updated_df, pd.DataFrame([new_row])], ignore_index=True
                )
                st.rerun()

            if save_col.button("Save changes", key=f"{section_key}_save"):
                df_to_save = st.session_state[work_key].copy()
                if "Delete" in df_to_save.columns:
                    df_to_save = df_to_save[df_to_save["Delete"] != True]  # noqa: E712
                weight_sum = float(df_to_save["Weight"].sum()) if not df_to_save.empty else 0.0
                if abs(weight_sum - 1.0) > 1e-6:
                    st.error(
                        f"Total weight must equal 1.0. Current total: {weight_sum:.4f}"
                    )
                else:
                    df_to_save = df_to_save.drop(columns=["Delete"], errors="ignore")
                    try:
                        replace_ttc_assumptions_section(
                            conn,
                            section,
                            df_to_save.to_dict("records"),
                        )
                    except Exception as e:
                        st.error(f"Save failed: {e}")
                    else:
                        st.session_state[saved_key] = df_to_save
                        st.session_state[work_key] = df_to_save.copy()
                        st.success("Saved.")
