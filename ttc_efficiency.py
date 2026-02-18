from __future__ import annotations

from typing import Dict, List, Optional, Tuple
import re

import numpy as np
import pandas as pd
import streamlit as st

from core import (
    TTC_SECTIONS,
    compute_and_store_cost_of_equity,
    compute_and_store_fcff_and_reinvestment_rate,
    compute_and_store_levered_beta,
    compute_and_store_pre_tax_cost_of_debt,
    compute_and_store_roic_wacc_spread,
    compute_and_store_total_equity_and_roe,
    compute_and_store_wacc,
    compute_growth_stats,
    compute_margin_growth_stats,
    compute_margin_stats,
    get_db,
    get_annual_accumulated_profit_series,
    get_annual_fcff_series,
    get_annual_interest_load_series,
    get_annual_net_income_series,
    get_annual_nopat_series,
    get_annual_op_margin_series,
    get_annual_pretax_income_series,
    get_annual_roe_series,
    get_annual_roce_series,
    get_annual_roic_wacc_spread_series,
    get_annual_series,
    get_ttc_assumptions,
    list_companies,
    read_df,
    replace_ttc_assumptions_section,
)

_SECTIONS = TTC_SECTIONS
_FILTER_COLUMNS = [
    "Total Scaled Volatility-Adjusted Score",
    "Total Debt-Adjusted Scaled Volatility-Adjusted Score",
    "Operating Margin%",
    "Operating Margin Expansion%",
    "Spread%",
    "Revenue Growth%",
    "ROE%",
    "ROCE%",
    "FCFF Growth%",
]


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


def _load_weight_maps(conn) -> Tuple[Dict[str, float], Dict[str, float]]:
    growth_weights_df = read_df("SELECT factor, weight FROM growth_weight_factors", conn)
    stddev_weights_df = read_df("SELECT factor, weight FROM stddev_weight_factors", conn)

    growth_weight_map: Dict[str, float] = {}
    if growth_weights_df is not None and not growth_weights_df.empty:
        for _, row in growth_weights_df.iterrows():
            nm = str(row["factor"])
            wt_val = row.get("weight")
            if pd.notna(wt_val):
                try:
                    growth_weight_map[nm] = float(wt_val)
                except Exception:
                    continue

    stddev_weight_map: Dict[str, float] = {}
    if stddev_weights_df is not None and not stddev_weights_df.empty:
        for _, row in stddev_weights_df.iterrows():
            nm = str(row["factor"])
            wt_val = row.get("weight")
            if pd.notna(wt_val):
                try:
                    stddev_weight_map[nm] = float(wt_val)
                except Exception:
                    continue

    return growth_weight_map, stddev_weight_map


def _get_factor_weight(weight_map: Dict[str, float], *names: str) -> Optional[float]:
    for nm in names:
        if nm in weight_map:
            return weight_map[nm]
    return None


def _weighted_score(pairs: List[Tuple[Optional[float], Optional[float]]]) -> Optional[float]:
    num = 0.0
    den = 0.0
    for val, wt in pairs:
        if val is not None and wt is not None and wt > 0:
            num += float(val) * float(wt)
            den += float(wt)
    if den == 0.0:
        return None
    return num / den


def _compute_level_stats(
    df: pd.DataFrame,
    yr_start: int,
    yr_end: int,
    *,
    value_col: str,
    stdev_sample: bool = True,
) -> Tuple[Optional[float], Optional[float]]:
    if df is None or df.empty:
        return None, None

    if yr_start < yr_end:
        yr_start, yr_end = yr_end, yr_start

    dff = df[(df["year"] <= yr_start) & (df["year"] >= yr_end)].copy()
    if dff.empty:
        return None, None

    vals = pd.to_numeric(dff[value_col], errors="coerce").dropna().astype(float).values
    if len(vals) == 0:
        return None, None

    med = float(np.median(vals))
    if len(vals) < 2:
        std = 0.0
    else:
        std = float(np.std(vals, ddof=1 if stdev_sample else 0))
    return med, std


def _to_pct_or_none(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    return float(x) * 100.0


def _to_margin_pct_or_none(x: Optional[float], values_are_fraction: bool) -> Optional[float]:
    if x is None:
        return None
    return float(x) * 100.0 if values_are_fraction else float(x)


def _compute_value_creation_filter_metrics(
    conn,
    company_id: int,
    yr_start: int,
    yr_end: int,
    growth_weight_map: Dict[str, float],
    stddev_weight_map: Dict[str, float],
) -> Dict[str, Optional[float]]:
    out: Dict[str, Optional[float]] = {
        "Total Scaled Volatility-Adjusted Score": None,
        "Total Debt-Adjusted Scaled Volatility-Adjusted Score": None,
        "Operating Margin%": None,
        "Operating Margin Expansion%": None,
        "Spread%": None,
        "Revenue Growth%": None,
        "ROE%": None,
        "ROCE%": None,
        "FCFF Growth%": None,
    }

    bs_scaled: Optional[float] = None
    bs_debt: Optional[float] = None
    pl_scaled: Optional[float] = None
    fs_scaled: Optional[float] = None

    # Balance sheet component for total score + ROE/ROCE filters
    try:
        compute_and_store_total_equity_and_roe(conn, company_id)

        ann_acc = get_annual_accumulated_profit_series(conn, company_id)
        ann_roe = get_annual_roe_series(conn, company_id)
        ann_roce = get_annual_roce_series(conn, company_id)
        ann_interest_load = get_annual_interest_load_series(conn, company_id)

        med_acc_g: Optional[float] = None
        std_acc_g: Optional[float] = None
        if ann_acc is not None and not ann_acc.empty:
            med_acc_g, std_acc_g = compute_growth_stats(
                ann_acc,
                yr_start,
                yr_end,
                stdev_sample=True,
                value_col="accumulated_profit",
                abs_denom=True,
            )

        med_roe: Optional[float] = None
        std_roe: Optional[float] = None
        if ann_roe is not None and not ann_roe.empty:
            df_roe = ann_roe[(ann_roe["year"] >= yr_end) & (ann_roe["year"] <= yr_start)].sort_values("year")
            vals_roe = df_roe["roe"].dropna().astype(float).values
            if vals_roe.size > 0:
                arr_roe = np.array(vals_roe, dtype=float)
                med_roe = float(np.median(arr_roe))
                ddof_roe = 1 if arr_roe.size > 1 else 0
                std_roe = float(np.std(arr_roe, ddof=ddof_roe))

        med_roce: Optional[float] = None
        std_roce: Optional[float] = None
        if ann_roce is not None and not ann_roce.empty:
            df_roce = ann_roce[(ann_roce["year"] >= yr_end) & (ann_roce["year"] <= yr_start)].sort_values("year")
            vals_roce = df_roce["roce"].dropna().astype(float).values
            if vals_roce.size > 0:
                arr_roce = np.array(vals_roce, dtype=float)
                med_roce = float(np.median(arr_roce))
                ddof_roce = 1 if arr_roce.size > 1 else 0
                std_roce = float(np.std(arr_roce, ddof=ddof_roce))

        median_interest_load_pct: Optional[float] = None
        if ann_interest_load is not None and not ann_interest_load.empty:
            df_il = ann_interest_load[
                (ann_interest_load["year"] >= yr_end) & (ann_interest_load["year"] <= yr_start)
            ].sort_values("year")
            vals_il = df_il["interest_load_pct"].dropna().astype(float).values
            if vals_il.size > 0:
                median_interest_load_pct = float(np.median(np.array(vals_il, dtype=float)))

        median_acc_pct = _to_pct_or_none(med_acc_g)
        std_acc_pct = _to_pct_or_none(std_acc_g)
        median_roe_pct = _to_pct_or_none(med_roe)
        std_roe_pct = _to_pct_or_none(std_roe)
        median_roce_pct = _to_pct_or_none(med_roce)
        std_roce_pct = _to_pct_or_none(std_roce)

        out["ROE%"] = median_roe_pct
        out["ROCE%"] = median_roce_pct

        gw_acc = _get_factor_weight(growth_weight_map, "Accumulated Equity Growth", "Accumulated Profit Growth")
        gw_roe = _get_factor_weight(growth_weight_map, "ROE")
        gw_roce = _get_factor_weight(growth_weight_map, "ROCE")

        sw_acc = _get_factor_weight(stddev_weight_map, "Accumulated Equity Growth", "Accumulated Profit Growth")
        sw_roe = _get_factor_weight(stddev_weight_map, "ROE")
        sw_roce = _get_factor_weight(stddev_weight_map, "ROCE")

        weighted_strength = _weighted_score(
            [(median_acc_pct, gw_acc), (median_roe_pct, gw_roe), (median_roce_pct, gw_roce)]
        )
        weighted_stddev_bs = _weighted_score(
            [(std_acc_pct, sw_acc), (std_roe_pct, sw_roe), (std_roce_pct, sw_roce)]
        )

        if weighted_strength is not None and weighted_stddev_bs is not None:
            bs_scaled = weighted_strength / (1.0 + weighted_stddev_bs)
        if bs_scaled is not None and median_interest_load_pct is not None:
            bs_debt = bs_scaled / (1.0 + (median_interest_load_pct / 100.0))
    except Exception:
        pass

    # P&L component for total score + margin/revenue filters
    try:
        ann_rev = get_annual_series(conn, company_id)
        med_rev_g: Optional[float] = None
        std_rev_g: Optional[float] = None
        if ann_rev is not None and not ann_rev.empty:
            med_rev_g, std_rev_g = compute_growth_stats(
                ann_rev,
                yr_start,
                yr_end,
                stdev_sample=True,
                value_col="revenue",
                abs_denom=True,
            )

        ann_pt = get_annual_pretax_income_series(conn, company_id)
        med_pt_g: Optional[float] = None
        std_pt_g: Optional[float] = None
        if ann_pt is not None and not ann_pt.empty:
            med_pt_g, std_pt_g = compute_growth_stats(
                ann_pt,
                yr_start,
                yr_end,
                stdev_sample=True,
                value_col="pretax_income",
                abs_denom=True,
            )

        ann_ni = get_annual_net_income_series(conn, company_id)
        med_ni_g: Optional[float] = None
        std_ni_g: Optional[float] = None
        if ann_ni is not None and not ann_ni.empty:
            med_ni_g, std_ni_g = compute_growth_stats(
                ann_ni,
                yr_start,
                yr_end,
                stdev_sample=True,
                value_col="net_income",
                abs_denom=True,
            )

        ann_nopat = get_annual_nopat_series(conn, company_id)
        med_nopat_g: Optional[float] = None
        std_nopat_g: Optional[float] = None
        if ann_nopat is not None and not ann_nopat.empty:
            med_nopat_g, std_nopat_g = compute_growth_stats(
                ann_nopat,
                yr_start,
                yr_end,
                stdev_sample=True,
                value_col="nopat",
                abs_denom=True,
            )

        ann_om = get_annual_op_margin_series(conn, company_id)
        med_om: Optional[float] = None
        std_om: Optional[float] = None
        om_is_fraction = True
        med_om_g: Optional[float] = None
        std_om_g: Optional[float] = None
        if ann_om is not None and not ann_om.empty:
            med_om, std_om, om_is_fraction = compute_margin_stats(
                ann_om,
                yr_start,
                yr_end,
                stdev_sample=True,
            )
            med_om_g, std_om_g = compute_margin_growth_stats(
                ann_om,
                yr_start,
                yr_end,
                stdev_sample=True,
            )

        median_rev_pct = _to_pct_or_none(med_rev_g)
        std_rev_pct = _to_pct_or_none(std_rev_g)
        median_pt_pct = _to_pct_or_none(med_pt_g)
        std_pt_pct = _to_pct_or_none(std_pt_g)
        median_ni_pct = _to_pct_or_none(med_ni_g)
        std_ni_pct = _to_pct_or_none(std_ni_g)
        median_nopat_pct = _to_pct_or_none(med_nopat_g)
        std_nopat_pct = _to_pct_or_none(std_nopat_g)
        median_om_pct = _to_margin_pct_or_none(med_om, om_is_fraction)
        std_om_pct = _to_margin_pct_or_none(std_om, om_is_fraction)
        median_yoy_om_pct = _to_pct_or_none(med_om_g)
        std_yoy_om_pct = _to_pct_or_none(std_om_g)

        out["Operating Margin%"] = median_om_pct
        out["Operating Margin Expansion%"] = median_yoy_om_pct
        out["Revenue Growth%"] = median_rev_pct

        gw_rev = _get_factor_weight(growth_weight_map, "Revenue Growth")
        gw_pt = _get_factor_weight(growth_weight_map, "Pretax Income Growth", "Profit Before Tax Growth")
        gw_ni = _get_factor_weight(growth_weight_map, "Net Income Growth", "Net Income  Growth")
        gw_nopat = _get_factor_weight(growth_weight_map, "NOPAT Growth")
        gw_om = _get_factor_weight(growth_weight_map, "Operating Margin")
        gw_yoy_om = _get_factor_weight(growth_weight_map, "YoY Operating Margin Growth")

        sw_rev = _get_factor_weight(stddev_weight_map, "Revenue Growth")
        sw_pt = _get_factor_weight(stddev_weight_map, "Pretax Income Growth", "Profit Before Tax Growth")
        sw_ni = _get_factor_weight(stddev_weight_map, "Net Income Growth", "Net Income  Growth")
        sw_nopat = _get_factor_weight(stddev_weight_map, "NOPAT Growth")
        sw_om = _get_factor_weight(stddev_weight_map, "Operating Margin")
        sw_yoy_om = _get_factor_weight(stddev_weight_map, "YoY Operating Margin Growth")

        weighted_growth = _weighted_score(
            [
                (median_rev_pct, gw_rev),
                (median_pt_pct, gw_pt),
                (median_ni_pct, gw_ni),
                (median_nopat_pct, gw_nopat),
                (median_om_pct, gw_om),
                (median_yoy_om_pct, gw_yoy_om),
            ]
        )
        weighted_stddev_pl = _weighted_score(
            [
                (std_rev_pct, sw_rev),
                (std_pt_pct, sw_pt),
                (std_ni_pct, sw_ni),
                (std_nopat_pct, sw_nopat),
                (std_om_pct, sw_om),
                (std_yoy_om_pct, sw_yoy_om),
            ]
        )
        if weighted_growth is not None and weighted_stddev_pl is not None:
            pl_scaled = weighted_growth / (1.0 + weighted_stddev_pl)
    except Exception:
        pass

    # FCFF + spread component for total score + spread/fcff filters
    try:
        compute_and_store_fcff_and_reinvestment_rate(conn, company_id)
        compute_and_store_levered_beta(conn, company_id)
        compute_and_store_cost_of_equity(conn, company_id)
        compute_and_store_pre_tax_cost_of_debt(conn, company_id)
        compute_and_store_wacc(conn, company_id)
        compute_and_store_roic_wacc_spread(conn, company_id)

        fcff_df = get_annual_fcff_series(conn, company_id)
        spread_df = get_annual_roic_wacc_spread_series(conn, company_id)

        med_fcff_g: Optional[float] = None
        std_fcff_g: Optional[float] = None
        if fcff_df is not None and not fcff_df.empty:
            med_fcff_g, std_fcff_g = compute_growth_stats(
                fcff_df,
                yr_start,
                yr_end,
                stdev_sample=True,
                value_col="fcff",
                abs_denom=True,
            )

        median_yoy_fcff_change_pct = _to_pct_or_none(med_fcff_g)
        std_yoy_fcff_change_pct = _to_pct_or_none(std_fcff_g)

        med_spread: Optional[float] = None
        std_spread: Optional[float] = None
        if spread_df is not None and not spread_df.empty:
            med_spread, std_spread = _compute_level_stats(
                spread_df,
                yr_start,
                yr_end,
                value_col="spread_pct",
                stdev_sample=True,
            )

        out["Spread%"] = med_spread
        out["FCFF Growth%"] = median_yoy_fcff_change_pct

        gw_fcff = _get_factor_weight(growth_weight_map, "FCFF Growth", "FCFE Growth")
        gw_spread = _get_factor_weight(growth_weight_map, "Spread")
        sw_fcff = _get_factor_weight(stddev_weight_map, "FCFF Growth", "FCFE Growth")
        sw_spread = _get_factor_weight(stddev_weight_map, "Spread")

        weighted_growth = _weighted_score(
            [
                (median_yoy_fcff_change_pct, gw_fcff),
                (med_spread, gw_spread),
            ]
        )
        weighted_stddev = _weighted_score(
            [
                (std_yoy_fcff_change_pct, sw_fcff),
                (std_spread, sw_spread),
            ]
        )
        if weighted_growth is not None and weighted_stddev is not None:
            fs_scaled = weighted_growth / (1.0 + weighted_stddev)
    except Exception:
        pass

    if bs_scaled is not None and pl_scaled is not None and fs_scaled is not None:
        out["Total Scaled Volatility-Adjusted Score"] = bs_scaled + pl_scaled + fs_scaled
    if bs_debt is not None and pl_scaled is not None and fs_scaled is not None:
        out["Total Debt-Adjusted Scaled Volatility-Adjusted Score"] = bs_debt + pl_scaled + fs_scaled

    return out


def _apply_ttc_filters(df: pd.DataFrame, key_prefix: str) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    with st.expander("Filters (selected year-range values)", expanded=False):
        # Shared across TTC dashboards so one saved group can be reused in each tab/view.
        presets_key = "ttc_filter_presets"
        if presets_key not in st.session_state:
            st.session_state[presets_key] = {}
        presets: Dict[str, Dict[str, Dict[str, float]]] = st.session_state[presets_key]

        bounds: Dict[str, Tuple[float, float]] = {}
        for col in _FILTER_COLUMNS:
            if col not in df.columns:
                continue
            series = pd.to_numeric(df[col], errors="coerce").dropna()
            if series.empty:
                continue
            lo_b = float(series.min())
            hi_b = float(series.max())
            if lo_b == hi_b:
                continue
            bounds[col] = (lo_b, hi_b)

        ctrl_cols = st.columns([2.4, 1.8, 1.0])
        preset_names = sorted(presets.keys())
        selected_preset = ctrl_cols[0].selectbox(
            "Saved filter group",
            options=["(none)"] + preset_names,
            key=f"{key_prefix}_preset_select",
        )
        preset_name_to_save = ctrl_cols[1].text_input(
            "Save current as",
            key=f"{key_prefix}_preset_name",
            placeholder="e.g. Quality Gate",
        ).strip()
        save_clicked = ctrl_cols[2].button("Save", key=f"{key_prefix}_preset_save")

        applied_marker_key = f"{key_prefix}_preset_applied"
        last_applied = st.session_state.get(applied_marker_key)
        if selected_preset != "(none)" and selected_preset in presets and selected_preset != last_applied:
            selected_values = presets[selected_preset]
            for col, (lo_b, hi_b) in bounds.items():
                pair = selected_values.get(col, {})
                lo = float(pair.get("min", lo_b))
                hi = float(pair.get("max", hi_b))
                if lo > hi:
                    lo, hi = hi, lo
                lo = max(lo_b, min(lo, hi_b))
                hi = max(lo_b, min(hi, hi_b))
                st.session_state[f"{key_prefix}_{_slug(col)}_min"] = lo
                st.session_state[f"{key_prefix}_{_slug(col)}_max"] = hi
            st.session_state[applied_marker_key] = selected_preset
            st.caption(f"Applied saved filter group: `{selected_preset}`")
        elif selected_preset == "(none)":
            st.session_state[applied_marker_key] = None

        if save_clicked:
            if not preset_name_to_save:
                st.warning("Enter a filter group name before saving.")
            else:
                values: Dict[str, Dict[str, float]] = {}
                for col, (lo_b, hi_b) in bounds.items():
                    lo_key = f"{key_prefix}_{_slug(col)}_min"
                    hi_key = f"{key_prefix}_{_slug(col)}_max"
                    lo = float(st.session_state.get(lo_key, lo_b))
                    hi = float(st.session_state.get(hi_key, hi_b))
                    if lo > hi:
                        lo, hi = hi, lo
                    lo = max(lo_b, min(lo, hi_b))
                    hi = max(lo_b, min(hi, hi_b))
                    values[col] = {"min": lo, "max": hi}
                presets[preset_name_to_save] = values
                st.session_state[presets_key] = presets
                st.session_state[f"{key_prefix}_preset_select"] = preset_name_to_save
                st.session_state[applied_marker_key] = preset_name_to_save
                st.success(f"Saved filter group: {preset_name_to_save}")

        mask = pd.Series(True, index=df.index)
        active = 0

        for col in _FILTER_COLUMNS:
            if col not in df.columns:
                continue

            series = pd.to_numeric(df[col], errors="coerce")
            valid = series.dropna()
            if valid.empty:
                continue

            min_default = float(valid.min())
            max_default = float(valid.max())
            if min_default == max_default:
                st.caption(f"{col}: fixed at {min_default:.4f}")
                continue

            c1, c2, c3 = st.columns([3.0, 1.2, 1.2])
            c1.markdown(f"`{col}`")
            lo = c2.number_input(
                "Min",
                key=f"{key_prefix}_{_slug(col)}_min",
                value=min_default,
                format="%.4f",
                label_visibility="collapsed",
            )
            hi = c3.number_input(
                "Max",
                key=f"{key_prefix}_{_slug(col)}_max",
                value=max_default,
                format="%.4f",
                label_visibility="collapsed",
            )

            if lo > hi:
                lo, hi = hi, lo

            if lo > min_default or hi < max_default:
                active += 1
                mask &= series.between(lo, hi, inclusive="both")

        if active == 0:
            st.caption("No filter constraints applied.")
            return df

        filtered = df[mask].copy()
        st.caption(f"{len(filtered)} of {len(df)} companies match active filters.")
        return filtered

    return df


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
    if compute:
        st.session_state["ttc_income_statement_has_run"] = True
    if not st.session_state.get("ttc_income_statement_has_run", False):
        return

    sections = _parse_assumptions_sections(conn)
    if not sections.get("Income Statement Efficiency Score"):
        st.error("No TTC assumptions found in the database. Add them in the Assumptions tab first.")
        return
    params = _get_income_statement_params(sections)
    growth_weight_map, stddev_weight_map = _load_weight_maps(conn)

    buckets_map = _get_company_buckets(conn, company_ids)
    company_lookup = {int(row["id"]): row for _, row in companies_df.iterrows()}

    rows: List[Dict[str, object]] = []
    progress_bar = st.progress(0, text="Starting income statement score computation...")
    progress_total = max(len(company_ids), 1)

    for idx, cid in enumerate(company_ids, start=1):
        row = company_lookup.get(cid)
        if row is None:
            progress_bar.progress(int((idx / progress_total) * 100), text=f"Computing company {idx}/{progress_total}...")
            continue
        progress_bar.progress(
            int((idx / progress_total) * 100),
            text=f"Computing {row['name']} ({idx}/{progress_total})...",
        )

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
        filter_metrics = _compute_value_creation_filter_metrics(
            conn,
            cid,
            yr_start,
            yr_end,
            growth_weight_map,
            stddev_weight_map,
        )

        rows.append(
            {
                "Ticker": row["ticker"],
                "Company Name": row["name"],
                "Industry Bucket": buckets_map.get(cid, "(no bucket)"),
                **filter_metrics,
                "Operating Margin %": op_margin_med,
                "Operating Margin Standard Deviation": op_margin_std,
                "Gross Margin %": gross_margin_med,
                "SG&A Ratio": sga_ratio_med,
                "Incremental Margin % [Pricing Power + Operating Leverage]": incremental_med,
                "Income Statement Efficiency Score (0-100)": score,
            }
        )

    if not rows:
        progress_bar.progress(100, text="No scores to display for selected input.")
        st.info("No scores could be computed for the selected input.")
        return

    df = pd.DataFrame(rows)
    df = df.sort_values(
        "Income Statement Efficiency Score (0-100)",
        ascending=False,
        na_position="last",
    )
    progress_bar.progress(100, text="Income statement score computation complete.")
    df = _apply_ttc_filters(df, "ttc_income_statement")
    if df.empty:
        st.info("No companies match the selected filters.")
        return

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
        pct_point_cols = [
            "Operating Margin%",
            "Operating Margin Expansion%",
            "Spread%",
            "Revenue Growth%",
            "ROE%",
            "ROCE%",
            "FCFF Growth%",
        ]
        ratio_cols = [
            "Operating Margin %",
            "Operating Margin Standard Deviation",
            "Gross Margin %",
            "SG&A Ratio",
            "Incremental Margin % [Pricing Power + Operating Leverage]",
        ]
        for col in pct_point_cols:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(
                    lambda v: f"{float(v):.2f}%" if pd.notna(v) else ""
                )
        for col in ratio_cols:
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
    if compute:
        st.session_state["ttc_balance_sheet_has_run"] = True
    if not st.session_state.get("ttc_balance_sheet_has_run", False):
        return

    sections = _parse_assumptions_sections(conn)
    if not sections.get("Balance Sheet Strength Score"):
        st.error("No TTC assumptions found in the database. Add them in the Assumptions tab first.")
        return
    params = _get_balance_sheet_params(sections)
    growth_weight_map, stddev_weight_map = _load_weight_maps(conn)

    buckets_map = _get_company_buckets(conn, company_ids)
    company_lookup = {int(row["id"]): row for _, row in companies_df.iterrows()}

    rows: List[Dict[str, object]] = []
    progress_bar = st.progress(0, text="Starting balance sheet score computation...")
    progress_total = max(len(company_ids), 1)

    for idx, cid in enumerate(company_ids, start=1):
        row = company_lookup.get(cid)
        if row is None:
            progress_bar.progress(int((idx / progress_total) * 100), text=f"Computing company {idx}/{progress_total}...")
            continue
        progress_bar.progress(
            int((idx / progress_total) * 100),
            text=f"Computing {row['name']} ({idx}/{progress_total})...",
        )

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
        filter_metrics = _compute_value_creation_filter_metrics(
            conn,
            cid,
            yr_start,
            yr_end,
            growth_weight_map,
            stddev_weight_map,
        )

        rows.append(
            {
                "Ticker": row["ticker"],
                "Company Name": row["name"],
                "Industry Bucket": buckets_map.get(cid, "(no bucket)"),
                **filter_metrics,
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
        progress_bar.progress(100, text="No scores to display for selected input.")
        st.info("No scores could be computed for the selected input.")
        return

    df = pd.DataFrame(rows)
    df = df.sort_values(
        "Balance Sheet Strength Score",
        ascending=False,
        na_position="last",
    )
    progress_bar.progress(100, text="Balance sheet score computation complete.")
    df = _apply_ttc_filters(df, "ttc_balance_sheet")
    if df.empty:
        st.info("No companies match the selected filters.")
        return

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
