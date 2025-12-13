
import re
from typing import Dict, List, Optional, Tuple

import streamlit as st

from core import *  # noqa: F401,F403




def _compute_level_stats(
    df: pd.DataFrame,
    yr_start: int,
    yr_end: int,
    *,
    value_col: str,
    stdev_sample: bool = True,
) -> Tuple[Optional[float], Optional[float]]:
    """Median + standard deviation of a level metric for years in-range (inclusive)."""
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


def render_combined_dashboard_tab() -> None:
    """Render the Combined Dashboard / Overall Score Dashboard tab."""
    st.title("Combined Dashboard")

    with st.expander("Overall Score Dashboard", expanded=True):
        conn = get_conn()
        init_db(conn)
        companies_df = list_companies(conn)
        if companies_df.empty:
            st.info("No companies in the database yet. Upload a spreadsheet above.")
            return

        # -----------------------------
        # User inputs for overall score
        # -----------------------------
        yr_input_overall = st.text_input(
            "Year range for Overall Scores (e.g., 'Recent - 2020' or '2023-2018')",
            value="Recent - 2020",
            key="combined_year_range",
        )

        stdev_mode_overall = st.radio(
            "Standard deviation mode for Overall Scores",
            ['Sample (ddof=1)', 'Population (ddof=0)'],
            horizontal=True,
            key="combined_stdev_mode",
        )
        sample_overall = (stdev_mode_overall == 'Sample (ddof=1)')

        def parse_range_overall(s: str, available_years: List[int]) -> Tuple[int, int]:
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

        # -----------------------------
        # Load growth / stddev weights
        # -----------------------------
        growth_weights_df = pd.read_sql_query(
            "SELECT factor, weight FROM growth_weight_factors",
            conn,
        )
        stddev_weights_df = pd.read_sql_query(
            "SELECT factor, weight FROM stddev_weight_factors",
            conn,
        )

        growth_weight_map: Dict[str, float] = {}
        if not growth_weights_df.empty:
            for _, row in growth_weights_df.iterrows():
                nm = str(row["factor"])
                wt_val = row.get("weight")
                if pd.notna(wt_val):
                    try:
                        growth_weight_map[nm] = float(wt_val)
                    except Exception:
                        continue

        stddev_weight_map: Dict[str, float] = {}
        if not stddev_weights_df.empty:
            for _, row in stddev_weights_df.iterrows():
                nm = str(row["factor"])
                wt_val = row.get("weight")
                if pd.notna(wt_val):
                    try:
                        stddev_weight_map[nm] = float(wt_val)
                    except Exception:
                        continue

        def get_factor_weight(weight_map: Dict[str, float], *names: str) -> Optional[float]:
            for nm in names:
                if nm in weight_map:
                    return weight_map[nm]
            return None

        def weighted_score(pairs: List[Tuple[Optional[float], Optional[float]]]) -> Optional[float]:
            num = 0.0
            den = 0.0
            for val, wt in pairs:
                if val is not None and wt is not None and wt > 0:
                    num += val * wt
                    den += wt
            if den == 0.0:
                return None
            return num / den


        def compute_price_cagr_for_range(
            price_df: pd.DataFrame,
            start_year: int,
            end_year: int,
        ) -> Optional[float]:
            """
            Compute the compound annual growth rate (CAGR) of price change
            between start_year and end_year (inclusive) using the annual
            price_change series stored as decimal returns.
            """
            df = price_df.copy()
            df = df[(df["year"] >= start_year) & (df["year"] <= end_year)].sort_values("year")
            if df.empty:
                return None
            vals = df["price_change"].dropna().astype(float).values
            if vals.size == 0:
                return None
            total_factor = 1.0
            for r in vals:
                factor = 1.0 + float(r)
                if factor <= 0.0:
                    # If any year is -100% or worse, CAGR is not meaningful
                    return None
                total_factor *= factor
            n_years = float(vals.size)
            try:
                cagr = total_factor ** (1.0 / n_years) - 1.0
            except Exception:
                return None
            # Return as percentage (e.g., 15.0 for 15%)
            return cagr * 100.0

        def compute_price_above_threshold_pct(
            price_df: pd.DataFrame,
            start_year: int,
            end_year: int,
            threshold: float = 0.15,
        ) -> Optional[float]:
            """
            Compute the percentage of years in [start_year, end_year] for which
            the annual price_change exceeds the given threshold.
            The threshold is in decimal form (0.15 == 15%).
            """
            df = price_df.copy()
            df = df[(df["year"] >= start_year) & (df["year"] <= end_year)].sort_values("year")
            if df.empty:
                return None
            vals = df["price_change"].dropna().astype(float).values
            if vals.size == 0:
                return None
            total_years = float(vals.size)
            count_above = float((vals > threshold).sum())
            pct = (count_above / total_years) * 100.0
            return pct


        # -----------------------------
        # Bucket / company selection
        # -----------------------------
        groups_df = pd.read_sql_query(
            "SELECT id, name FROM company_groups ORDER BY name",
            conn,
        )
        if groups_df.empty:
            st.info("No buckets defined yet. Define buckets in the P&L or Balance Sheet Metrics sections.")
            return

        group_name_to_id = {
            str(row["name"]): int(row["id"]) for _, row in groups_df.iterrows()
        }
        bucket_names_selected = st.multiselect(
            "Select one or more buckets for Overall Score computation",
            options=list(group_name_to_id.keys()),
            key="combined_score_bucket_select",
        )

        score_company_ids: List[int] = []
        if bucket_names_selected:
            group_ids = [group_name_to_id[name] for name in bucket_names_selected if name in group_name_to_id]
            if group_ids:
                placeholders = ",".join(["?"] * len(group_ids))
                bucket_members_df = pd.read_sql_query(
                    f"SELECT DISTINCT company_id FROM company_group_members WHERE group_id IN ({placeholders})",
                    conn,
                    params=group_ids,
                )
                if not bucket_members_df.empty:
                    score_company_ids = [int(x) for x in bucket_members_df["company_id"].tolist()]

        if not score_company_ids:
            st.info("Select at least one bucket to compute Overall Scores.")
            return

        compute_overall_scores = st.button(
            "Compute Overall Scores",
            type="primary",
            key="compute_overall_scores",
            disabled=(len(score_company_ids) == 0),
        )
        if not compute_overall_scores:
            return

        # -----------------------------
        # Score computation
        # -----------------------------
        rows: List[Dict[str, Optional[float]]] = []

        rows_breakup: List[Dict[str, Optional[float]]] = []

        for cid in score_company_ids:
            row_info = companies_df[companies_df["id"] == cid]
            if row_info.empty:
                continue
            row_info = row_info.iloc[0]
            name = row_info["name"]
            ticker = row_info["ticker"]


            # Breakup dashboard metrics (year-range aligned; percent fields are % points)
            breakup_total_scaled: Optional[float] = None
            breakup_total_debt_adjusted_scaled: Optional[float] = None
            breakup_median_spread_pct: Optional[float] = None
            breakup_std_spread_pct: Optional[float] = None
            breakup_median_operating_margin: Optional[float] = None
            breakup_std_operating_margin: Optional[float] = None
            breakup_median_yoy_operating_margin_growth: Optional[float] = None
            breakup_std_yoy_operating_margin_growth: Optional[float] = None
            breakup_median_revenue_growth: Optional[float] = None
            breakup_std_revenue_growth: Optional[float] = None
            breakup_median_roe: Optional[float] = None
            breakup_std_roe: Optional[float] = None
            breakup_median_yoy_fcff_change: Optional[float] = None

            bs_add: Optional[float] = None
            bs_scaled: Optional[float] = None
            bs_debt: Optional[float] = None
            pl_add: Optional[float] = None
            pl_scaled: Optional[float] = None

            # -------------------------
            # Balance Sheet score part
            # -------------------------
            try:
                # Ensure equity-derived metrics (Total Equity, Average Equity, ROE) are up-to-date
                compute_and_store_total_equity_and_roe(conn, cid)

                ann_acc = get_annual_accumulated_profit_series(conn, cid)
                ann_roe = get_annual_roe_series(conn, cid)
                ann_roce = get_annual_roce_series(conn, cid)
                ann_interest_load = get_annual_interest_load_series(conn, cid)

                available_years_bs: List[int] = []
                for df in (ann_acc, ann_roe, ann_roce, ann_interest_load):
                    if not df.empty:
                        available_years_bs.extend(df["year"].tolist())
                available_years_bs = sorted(set(available_years_bs))

                if available_years_bs:
                    yr_start_bs, yr_end_bs = parse_range_overall(yr_input_overall, available_years_bs)
                    if yr_start_bs < yr_end_bs:
                        yr_start_bs, yr_end_bs = yr_end_bs, yr_start_bs

                    # Accumulated Profit growth stats
                    med_acc_g: Optional[float] = None
                    std_acc_g: Optional[float] = None
                    if not ann_acc.empty:
                        med_acc_g, std_acc_g = compute_growth_stats(
                            ann_acc,
                            yr_start_bs,
                            yr_end_bs,
                            stdev_sample=sample_overall,
                            value_col="accumulated_profit",
                            abs_denom=True,
                        )

                    # ROE stats
                    med_roe_g: Optional[float] = None
                    std_roe_g: Optional[float] = None
                    if not ann_roe.empty:
                        df_roe = ann_roe.copy()
                        df_roe = df_roe[(df_roe["year"] >= yr_end_bs) & (df_roe["year"] <= yr_start_bs)].sort_values("year")
                        vals_roe = df_roe["roe"].dropna().astype(float).values
                        if vals_roe.size > 0:
                            arr_roe = np.array(vals_roe, dtype=float)
                            med_roe_g = float(np.median(arr_roe))
                            ddof_roe = 1 if sample_overall and arr_roe.size > 1 else 0
                            std_roe_g = float(np.std(arr_roe, ddof=ddof_roe)) if arr_roe.size > 1 or ddof_roe == 0 else None

                    # ROCE stats
                    med_roce: Optional[float] = None
                    std_roce: Optional[float] = None
                    if not ann_roce.empty:
                        df_roce = ann_roce.copy()
                        df_roce = df_roce[(df_roce["year"] >= yr_end_bs) & (df_roce["year"] <= yr_start_bs)].sort_values("year")
                        vals_roce = df_roce["roce"].dropna().astype(float).values
                        if vals_roce.size > 0:
                            arr_roce = np.array(vals_roce, dtype=float)
                            med_roce = float(np.median(arr_roce))
                            ddof_roce = 1 if sample_overall and arr_roce.size > 1 else 0
                            std_roce = float(np.std(arr_roce, ddof=ddof_roce)) if arr_roce.size > 1 or ddof_roce == 0 else None

                    # Interest Load % stats for score
                    median_interest_load_pct: Optional[float] = None
                    if not ann_interest_load.empty:
                        df_il = ann_interest_load.copy()
                        df_il = df_il[(df_il["year"] >= yr_end_bs) & (df_il["year"] <= yr_start_bs)].sort_values("year")
                        vals_il = df_il["interest_load_pct"].dropna().astype(float).values
                        if vals_il.size > 0:
                            arr_il = np.array(vals_il, dtype=float)
                            median_interest_load_pct = float(np.median(arr_il))

                    # Convert to percentage units where applicable
                    def to_pct_val(x: Optional[float]) -> Optional[float]:
                        if x is None:
                            return None
                        return x * 100.0

                    median_acc_pct = to_pct_val(med_acc_g) if med_acc_g is not None else None
                    std_acc_pct = to_pct_val(std_acc_g) if std_acc_g is not None else None
                    median_roe_pct = to_pct_val(med_roe_g) if med_roe_g is not None else None
                    std_roe_pct = to_pct_val(std_roe_g) if std_roe_g is not None else None

                    # Breakup capture (ROE in % points)
                    breakup_median_roe = median_roe_pct
                    breakup_std_roe = std_roe_pct
                    median_roce_pct = to_pct_val(med_roce) if med_roce is not None else None
                    std_roce_pct = to_pct_val(std_roce) if std_roce is not None else None

                    # Retrieve relevant weights
                    gw_acc = get_factor_weight(growth_weight_map, "Accumulated Equity Growth", "Accumulated Profit Growth")
                    gw_roe = get_factor_weight(growth_weight_map, "ROE")
                    gw_roce = get_factor_weight(growth_weight_map, "ROCE")

                    sw_acc = get_factor_weight(stddev_weight_map, "Accumulated Equity Growth", "Accumulated Profit Growth")
                    sw_roe = get_factor_weight(stddev_weight_map, "ROE")
                    sw_roce = get_factor_weight(stddev_weight_map, "ROCE")

                    strength_pairs = [
                        (median_acc_pct, gw_acc),
                        (median_roe_pct, gw_roe),
                        (median_roce_pct, gw_roce),
                    ]
                    stddev_pairs_bs = [
                        (std_acc_pct, sw_acc),
                        (std_roe_pct, sw_roe),
                        (std_roce_pct, sw_roce),
                    ]

                    weighted_strength = weighted_score(strength_pairs)
                    weighted_stddev_bs = weighted_score(stddev_pairs_bs)

                    if weighted_strength is not None and weighted_stddev_bs is not None:
                        bs_add = weighted_strength - weighted_stddev_bs
                        bs_scaled = weighted_strength / (1.0 + weighted_stddev_bs)

                    if bs_scaled is not None and median_interest_load_pct is not None:
                        bs_debt = bs_scaled / (1.0 + (median_interest_load_pct / 100.0))

            except Exception as e:
                st.warning(f"Skipping Balance Sheet score computation for company_id={cid}: {e}")

            # -------------------------
            # P&L score part
            # -------------------------
            try:
                ann_rev = get_annual_series(conn, cid)
                if not ann_rev.empty:
                    yr_start_pl, yr_end_pl = parse_range_overall(yr_input_overall, ann_rev["year"].tolist())
                    if yr_start_pl < yr_end_pl:
                        yr_start_pl, yr_end_pl = yr_end_pl, yr_start_pl

                    med_rev_g, std_rev_g = compute_growth_stats(
                        ann_rev,
                        yr_start_pl,
                        yr_end_pl,
                        stdev_sample=sample_overall,
                        value_col="revenue",
                        abs_denom=True,
                    )

                    ann_pt = get_annual_pretax_income_series(conn, cid)
                    med_pt_g: Optional[float] = None
                    std_pt_g: Optional[float] = None
                    if not ann_pt.empty:
                        med_pt_g, std_pt_g = compute_growth_stats(
                            ann_pt,
                            yr_start_pl,
                            yr_end_pl,
                            stdev_sample=sample_overall,
                            value_col="pretax_income",
                            abs_denom=True,
                        )

                    ann_ni = get_annual_net_income_series(conn, cid)
                    med_ni_g: Optional[float] = None
                    std_ni_g: Optional[float] = None
                    if not ann_ni.empty:
                        med_ni_g, std_ni_g = compute_growth_stats(
                            ann_ni,
                            yr_start_pl,
                            yr_end_pl,
                            stdev_sample=sample_overall,
                            value_col="net_income",
                            abs_denom=True,
                        )

                    ann_nopat = get_annual_nopat_series(conn, cid)
                    med_nopat_g: Optional[float] = None
                    std_nopat_g: Optional[float] = None
                    if not ann_nopat.empty:
                        med_nopat_g, std_nopat_g = compute_growth_stats(
                            ann_nopat,
                            yr_start_pl,
                            yr_end_pl,
                            stdev_sample=sample_overall,
                            value_col="nopat",
                            abs_denom=True,
                        )

                    ann_om = get_annual_op_margin_series(conn, cid)
                    med_om: Optional[float] = None
                    std_om: Optional[float] = None
                    om_is_fraction = True
                    med_om_g: Optional[float] = None
                    std_om_g: Optional[float] = None
                    if not ann_om.empty:
                        med_om, std_om, om_is_fraction = compute_margin_stats(
                            ann_om,
                            yr_start_pl,
                            yr_end_pl,
                            stdev_sample=sample_overall,
                        )
                        med_om_g, std_om_g = compute_margin_growth_stats(
                            ann_om,
                            yr_start_pl,
                            yr_end_pl,
                            stdev_sample=sample_overall,
                        )

                    def to_pct_val_pl(x: Optional[float]) -> Optional[float]:
                        if x is None:
                            return None
                        return x * 100.0

                    def to_pct_margin(x: Optional[float], assume_fraction: bool) -> Optional[float]:
                        if x is None:
                            return None
                        return x * 100.0 if assume_fraction else x

                    median_rev_pct = to_pct_val_pl(med_rev_g) if med_rev_g is not None else None
                    std_rev_pct = to_pct_val_pl(std_rev_g) if std_rev_g is not None else None
                    median_pt_pct = to_pct_val_pl(med_pt_g) if med_pt_g is not None else None
                    std_pt_pct = to_pct_val_pl(std_pt_g) if std_pt_g is not None else None
                    median_ni_pct = to_pct_val_pl(med_ni_g) if med_ni_g is not None else None
                    std_ni_pct = to_pct_val_pl(std_ni_g) if std_ni_g is not None else None
                    median_nopat_pct = to_pct_val_pl(med_nopat_g) if med_nopat_g is not None else None
                    std_nopat_pct = to_pct_val_pl(std_nopat_g) if std_nopat_g is not None else None
                    median_om_pct = to_pct_margin(med_om, om_is_fraction) if med_om is not None else None
                    std_om_pct = to_pct_margin(std_om, om_is_fraction) if std_om is not None else None
                    median_yoy_om_pct = to_pct_val_pl(med_om_g) if med_om_g is not None else None
                    std_yoy_om_pct = to_pct_val_pl(std_om_g) if std_om_g is not None else None


                    # Breakup capture (all in % points)
                    breakup_median_revenue_growth = median_rev_pct
                    breakup_std_revenue_growth = std_rev_pct
                    breakup_median_operating_margin = median_om_pct
                    breakup_std_operating_margin = std_om_pct
                    breakup_median_yoy_operating_margin_growth = median_yoy_om_pct
                    breakup_std_yoy_operating_margin_growth = std_yoy_om_pct
                    gw_rev = get_factor_weight(growth_weight_map, "Revenue Growth")
                    gw_pt = get_factor_weight(growth_weight_map, "Pretax Income Growth", "Profit Before Tax Growth")
                    # Net Income Growth in stddev table may have a double space; try both for robustness.
                    gw_ni = get_factor_weight(growth_weight_map, "Net Income Growth", "Net Income  Growth")
                    gw_nopat = get_factor_weight(growth_weight_map, "NOPAT Growth")
                    gw_om = get_factor_weight(growth_weight_map, "Operating Margin")
                    gw_yoy_om = get_factor_weight(growth_weight_map, "YoY Operating Margin Growth")

                    sw_rev = get_factor_weight(stddev_weight_map, "Revenue Growth")
                    sw_pt = get_factor_weight(stddev_weight_map, "Pretax Income Growth", "Profit Before Tax Growth")
                    sw_ni = get_factor_weight(stddev_weight_map, "Net Income Growth", "Net Income  Growth")
                    sw_nopat = get_factor_weight(stddev_weight_map, "NOPAT Growth")
                    sw_om = get_factor_weight(stddev_weight_map, "Operating Margin")
                    sw_yoy_om = get_factor_weight(stddev_weight_map, "YoY Operating Margin Growth")

                    growth_pairs_pl = [
                        (median_rev_pct, gw_rev),
                        (median_pt_pct, gw_pt),
                        (median_ni_pct, gw_ni),
                        (median_nopat_pct, gw_nopat),
                        (median_om_pct, gw_om),
                        (median_yoy_om_pct, gw_yoy_om),
                    ]

                    stddev_pairs_pl = [
                        (std_rev_pct, sw_rev),
                        (std_pt_pct, sw_pt),
                        (std_ni_pct, sw_ni),
                        (std_nopat_pct, sw_nopat),
                        (std_om_pct, sw_om),
                        (std_yoy_om_pct, sw_yoy_om),
                    ]

                    weighted_growth = weighted_score(growth_pairs_pl)
                    weighted_stddev_pl = weighted_score(stddev_pairs_pl)

                    if weighted_growth is not None and weighted_stddev_pl is not None:
                        pl_add = weighted_growth - weighted_stddev_pl
                        pl_scaled = weighted_growth / (1.0 + weighted_stddev_pl)

            except Exception as e:
                st.warning(f"Skipping P&L score computation for company_id={cid}: {e}")


            # -------------------------
            # FCFE + Spread score part
            # -------------------------
            fs_add: Optional[float] = None
            fs_scaled: Optional[float] = None
            try:
                # Ensure underlying metrics exist / are fresh
                compute_and_store_fcff_and_reinvestment_rate(conn, cid)
                compute_and_store_roic_wacc_spread(conn, cid)

                fcff_df = get_annual_fcff_series(conn, cid)
                spread_df = get_annual_roic_wacc_spread_series(conn, cid)

                available_years_fs: set = set()
                if fcff_df is not None and not fcff_df.empty and "year" in fcff_df.columns:
                    available_years_fs.update([int(y) for y in fcff_df["year"].dropna().tolist()])
                if spread_df is not None and not spread_df.empty and "year" in spread_df.columns:
                    available_years_fs.update([int(y) for y in spread_df["year"].dropna().tolist()])

                if available_years_fs:
                    yr_start_fs, yr_end_fs = parse_range_overall(yr_input_overall, sorted(available_years_fs))
                    if yr_start_fs < yr_end_fs:
                        yr_start_fs, yr_end_fs = yr_end_fs, yr_start_fs

                    # YoY FCFF change % stats
                    med_fcff_g: Optional[float] = None
                    std_fcff_g: Optional[float] = None
                    if fcff_df is not None and not fcff_df.empty:
                        med_fcff_g, std_fcff_g = compute_growth_stats(
                            fcff_df,
                            yr_start_fs,
                            yr_end_fs,
                            stdev_sample=sample_overall,
                            value_col="fcff",
                            abs_denom=True,
                        )

                    median_yoy_fcff_change_pct = (float(med_fcff_g) * 100.0) if med_fcff_g is not None else None
                    std_yoy_fcff_change_pct = (float(std_fcff_g) * 100.0) if std_fcff_g is not None else None

                    # Spread level stats (percentage points)
                    med_spread: Optional[float] = None
                    std_spread: Optional[float] = None
                    if spread_df is not None and not spread_df.empty:
                        med_spread, std_spread = _compute_level_stats(
                            spread_df,
                            yr_start_fs,
                            yr_end_fs,
                            value_col="spread_pct",
                            stdev_sample=sample_overall,
                        )


                    # Breakup capture
                    breakup_median_yoy_fcff_change = median_yoy_fcff_change_pct
                    breakup_median_spread_pct = med_spread
                    breakup_std_spread_pct = std_spread
                    gw_fcfe = get_factor_weight(growth_weight_map, "FCFE Growth")
                    gw_spread = get_factor_weight(growth_weight_map, "Spread")

                    sw_fcfe = get_factor_weight(stddev_weight_map, "FCFE Growth")
                    sw_spread = get_factor_weight(stddev_weight_map, "Spread")

                    growth_pairs = [
                        (median_yoy_fcff_change_pct, gw_fcfe),
                        (med_spread, gw_spread),
                    ]
                    stddev_pairs = [
                        (std_yoy_fcff_change_pct, sw_fcfe),
                        (std_spread, sw_spread),
                    ]

                    weighted_growth = weighted_score(growth_pairs)
                    weighted_stddev = weighted_score(stddev_pairs)

                    if weighted_growth is not None and weighted_stddev is not None:
                        fs_add = weighted_growth - weighted_stddev
                        fs_scaled = weighted_growth / (1.0 + weighted_stddev)

            except Exception as e:
                st.warning(f"Skipping FCFE and Spread score computation for company_id={cid}: {e}")


            # If everything is None, skip this company
            if all(v is None for v in [bs_add, bs_scaled, bs_debt, pl_add, pl_scaled, fs_add, fs_scaled]):
                continue


            # -------------------------
            # Price change CAGR and frequency stats
            # -------------------------
            price_cagr_2020_2025: Optional[float] = None
            price_cagr_2015_2020: Optional[float] = None
            price_cagr_2010_2015: Optional[float] = None
            price_above_15_pct: Optional[float] = None

            try:
                price_df = get_annual_price_change_series(conn, cid)
                if not price_df.empty:
                    price_df = price_df.dropna(subset=["price_change"]).copy()

                    # Fixed-window CAGRs (expressed as percentages)
                    price_cagr_2020_2025 = compute_price_cagr_for_range(price_df, 2020, 2025)
                    price_cagr_2015_2020 = compute_price_cagr_for_range(price_df, 2015, 2020)
                    price_cagr_2010_2015 = compute_price_cagr_for_range(price_df, 2010, 2015)

                    # Percentage of years with > 15% annual change between 2010 and 2025
                    price_above_15_pct = compute_price_above_threshold_pct(price_df, 2010, 2025, threshold=0.15)
            except Exception as e:
                # Don't block the overall scores if price data has issues
                st.warning(f"Skipping price metrics for company_id={cid}: {e}")
                price_cagr_2020_2025 = None
                price_cagr_2015_2020 = None
                price_cagr_2010_2015 = None
                price_above_15_pct = None

            total_additive = (
                (bs_add + pl_add + fs_add)
                if (bs_add is not None and pl_add is not None and fs_add is not None)
                else None
            )
            total_scaled = (
                (bs_scaled + pl_scaled + fs_scaled)
                if (bs_scaled is not None and pl_scaled is not None and fs_scaled is not None)
                else None
            )
            total_debt_adjusted_scaled = (
                (bs_debt + pl_scaled + fs_scaled)
                if (bs_debt is not None and pl_scaled is not None and fs_scaled is not None)
                else None
            )

            

            # Breakup capture (totals)
            breakup_total_scaled = total_scaled
            breakup_total_debt_adjusted_scaled = total_debt_adjusted_scaled
            rows_breakup.append(
                {
                    "Company Name": name,
                    "Ticker": ticker,
                    "Total Scaled Volatility-Adjusted Score": breakup_total_scaled,
                    "Total Debt-Adjusted Scaled Volatility-Adjusted Score": breakup_total_debt_adjusted_scaled,
                    "Median Spread % (ROIC - WACC)": breakup_median_spread_pct,
                    "Spread % Standard Deviation (ROIC - WACC)": breakup_std_spread_pct,
                    "Median Operating Margin": breakup_median_operating_margin,
                    "Operating Margin Standard Deviation": breakup_std_operating_margin,
                    "Median YoY Operating Margin Growth": breakup_median_yoy_operating_margin_growth,
                    "YoY Operating Margin Growth Standard Deviation": breakup_std_yoy_operating_margin_growth,
                    "Median Revenue Growth": breakup_median_revenue_growth,
                    "Revenue Growth Standard Deviation": breakup_std_revenue_growth,
                    "Median ROE": breakup_median_roe,
                    "ROE Standard Deviation": breakup_std_roe,
                    "Median YoY FCFF Change %": breakup_median_yoy_fcff_change,
                }
            )

            rows.append(
                {
                    "Company Name": name,
                    "Ticker": ticker,
                    "Additive Volatility-Adjusted Balance Sheet Strength Score": bs_add,
                    "Scaled Volatility-Adjusted Balance Sheet Strength Score": bs_scaled,
                    "Debt-Adjusted Balance Sheet Strength Score": bs_debt,
                    "Additive Volatility-Adjusted P&L Growth Score": pl_add,
                    "Scaled Volatility-Adjusted P&L Growth Score": pl_scaled,
                    "Additive FCFE and Spread Score": fs_add,
                    "Scaled FCFE and Spread Score": fs_scaled,
                    "Total Additive Volatility-Adjusted Score": total_additive,
                    "Total Scaled Volatility-Adjusted Score": total_scaled,
                    "Total Debt-Adjusted Scaled Volatility-Adjusted Score": total_debt_adjusted_scaled,
                    "2020-2025 CAGR": price_cagr_2020_2025,
                    "2015-2020 CAGR": price_cagr_2015_2020,
                    "2010-2015 CAGR": price_cagr_2010_2015,
                    "Above 15% Year%": price_above_15_pct,
                }
            )

        if not rows:
            st.info("No overall scores to display for the selected year range and buckets.")
            return


        df = pd.DataFrame(rows)

        # Sort by Total Scaled Volatility-Adjusted Score (if available),
        # otherwise by Total Additive Volatility-Adjusted Score.
        sort_col = None
        if (
            "Total Scaled Volatility-Adjusted Score" in df.columns
            and df["Total Scaled Volatility-Adjusted Score"].notna().any()
        ):
            sort_col = "Total Scaled Volatility-Adjusted Score"
        elif (
            "Total Additive Volatility-Adjusted Score" in df.columns
            and df["Total Additive Volatility-Adjusted Score"].notna().any()
        ):
            sort_col = "Total Additive Volatility-Adjusted Score"

        if sort_col:
            df_sorted = df.sort_values(by=sort_col, ascending=False)
        else:
            df_sorted = df

        # Columns to show with 2-decimal formatting (including price metrics).
        score_cols = [
            "Additive Volatility-Adjusted Balance Sheet Strength Score",
            "Scaled Volatility-Adjusted Balance Sheet Strength Score",
            "Debt-Adjusted Balance Sheet Strength Score",
            "Additive Volatility-Adjusted P&L Growth Score",
            "Scaled Volatility-Adjusted P&L Growth Score",
            "Additive FCFE and Spread Score",
            "Scaled FCFE and Spread Score",
            "Total Additive Volatility-Adjusted Score",
            "Total Scaled Volatility-Adjusted Score",
            "Total Debt-Adjusted Scaled Volatility-Adjusted Score",
            "2020-2025 CAGR",
            "2015-2020 CAGR",
            "2010-2015 CAGR",
            "Above 15% Year%",
        ]
        existing_score_cols = [c for c in score_cols if c in df_sorted.columns]

        def fmt_score_val(x: Optional[float]) -> str:
            if x is None or (isinstance(x, float) and pd.isna(x)):
                return "—"
            try:
                return f"{float(x):.2f}"
            except Exception:
                return "—"

        styler = df_sorted.style
        if existing_score_cols:
            styler = styler.format(fmt_score_val, subset=existing_score_cols)

        # Colour rules for the fixed-window CAGR columns.
        cagr_cols = [c for c in ["2020-2025 CAGR", "2015-2020 CAGR", "2010-2015 CAGR"] if c in df_sorted.columns]

        def style_cagr(val: Optional[float]) -> str:
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return ""
            try:
                v = float(val)
            except Exception:
                return ""
            # > 15%: green background, white text
            if v > 15.0:
                return "background-color: green; color: white;"
            # Between 15% and 0% (inclusive of 0%): white background, red text
            if v >= 0.0:
                return "background-color: white; color: red;"
            # Below 0%: red background, white text
            return "background-color: red; color: white;"

        if cagr_cols:
            styler = styler.applymap(style_cagr, subset=pd.IndexSlice[:, cagr_cols])

        # Colour rules for the 'Above 15% Year%' column.
        pct_year_cols = [c for c in ["Above 15% Year%"] if c in df_sorted.columns]

        def style_above_15_year(val: Optional[float]) -> str:
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return ""
            try:
                v = float(val)
            except Exception:
                return ""
            # > 60%: green background, white text
            if v > 60.0:
                return "background-color: green; color: white;"
            # Between 60% and 40% (inclusive of 40%): white background, red text
            if v >= 40.0:
                return "background-color: white; color: red;"
            # Below 40%: red background, white text
            return "background-color: red; color: white;"

        if pct_year_cols:
            styler = styler.applymap(style_above_15_year, subset=pd.IndexSlice[:, pct_year_cols])

        st.subheader("Overall Score Dashboard — Combined P&L and Balance Sheet Scores")
        st.dataframe(styler, use_container_width=True)



        # -----------------------------
        # Overall Score Dashboard Breakup
        # -----------------------------
        if rows_breakup:
            df_breakup = pd.DataFrame(rows_breakup)

            # Order columns exactly as requested (ignore any missing)
            breakup_cols = [
                "Company Name",
                "Ticker",
                "Total Scaled Volatility-Adjusted Score",
                "Total Debt-Adjusted Scaled Volatility-Adjusted Score",
                "Median Spread % (ROIC - WACC)",
                "Spread % Standard Deviation (ROIC - WACC)",
                "Median Operating Margin",
                "Operating Margin Standard Deviation",
                "Median YoY Operating Margin Growth",
                "YoY Operating Margin Growth Standard Deviation",
                "Median Revenue Growth",
                "Revenue Growth Standard Deviation",
                "Median ROE",
                "ROE Standard Deviation",
                "Median YoY FCFF Change %",
            ]
            breakup_cols_existing = [c for c in breakup_cols if c in df_breakup.columns]
            df_breakup = df_breakup[breakup_cols_existing]

            sort_col_b = None
            if (
                "Total Scaled Volatility-Adjusted Score" in df_breakup.columns
                and df_breakup["Total Scaled Volatility-Adjusted Score"].notna().any()
            ):
                sort_col_b = "Total Scaled Volatility-Adjusted Score"

            if sort_col_b:
                df_breakup = df_breakup.sort_values(by=sort_col_b, ascending=False)

            # Format all numeric columns as 2-decimal, leaving Company/Ticker untouched
            numeric_cols = [c for c in breakup_cols_existing if c not in ["Company Name", "Ticker"]]

            def fmt_break_val(x: Optional[float]) -> str:
                if x is None or (isinstance(x, float) and pd.isna(x)):
                    return "—"
                try:
                    return f"{float(x):.2f}"
                except Exception:
                    return "—"

            styler_break = df_breakup.style
            if numeric_cols:
                styler_break = styler_break.format(fmt_break_val, subset=numeric_cols)

            st.subheader("Overall Score Dashboard Breakup")
            st.dataframe(styler_break, use_container_width=True)
