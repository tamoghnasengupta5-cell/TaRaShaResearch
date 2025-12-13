import re
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
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


def render_capital_structure_cost_of_capital_tab() -> None:
    st.title("Capital Structure & Spread")

    with st.expander("Capital Structure & Spread Metrics", expanded=True):
        st.caption(
            "Debt/Market Capitalization (annual) = Total Debt / Market Capitalization."
        )

        conn = get_conn()
        init_db(conn)
        companies_df = list_companies(conn)

        if companies_df.empty:
            st.info("No companies in the database yet. Upload a spreadsheet under 'Data Upload'.")
            return

        # Base selection: individual companies
        all_company_options = [
            f"{row.name} ({row.ticker}) [id={row.id}]" for _, row in companies_df.iterrows()
        ]

        # Default selection: companies not present in any bucket (same behavior as other tabs)
        try:
            bucket_df = pd.read_sql_query("SELECT DISTINCT company_id FROM company_group_members", conn)
            bucketed_ids = {int(x) for x in bucket_df["company_id"].tolist()} if not bucket_df.empty else set()
        except Exception:
            bucketed_ids = set()

        default_company_options: List[str] = []
        if bucketed_ids:
            for _, row in companies_df.iterrows():
                if int(row.id) not in bucketed_ids:
                    default_company_options.append(f"{row.name} ({row.ticker}) [id={row.id}]")
        else:
            default_company_options = all_company_options

        # If a company was just ingested this session, override default to only that company
        last_company_id = st.session_state.get("last_ingested_company_id")
        if last_company_id is not None:
            selected_label = None
            for _, row in companies_df.iterrows():
                if int(row.id) == int(last_company_id):
                    selected_label = f"{row.name} ({row.ticker}) [id={row.id}]"
                    break
            if selected_label:
                default_company_options = [selected_label]

        options = st.multiselect(
            "Companies to analyze (for Capital Structure metrics)",
            options=all_company_options,
            default=default_company_options,
            key="cs_companies_to_analyze",
        )

        # Bucket save UI
        bucket_col1, bucket_col2 = st.columns([3, 1])
        with bucket_col1:
            bucket_name = st.text_input(
                "Save current selection as bucket (optional)",
                placeholder="e.g. Low_Leverage_Compounders",
                key="cs_bucket_name_input",
            )
        with bucket_col2:
            save_bucket = st.button("Save bucket", key="cs_save_bucket_button")

        if save_bucket:
            sel_company_ids: List[int] = []
            for opt in options:
                m_sel = re.search(r"\[id=(\d+)\]$", opt)
                if m_sel:
                    sel_company_ids.append(int(m_sel.group(1)))

            if not bucket_name or not bucket_name.strip():
                st.error("Please provide a non-empty bucket name.")
            elif not sel_company_ids:
                st.error("Please select at least one company before saving a bucket.")
            else:
                bname = bucket_name.strip()
                cur = conn.cursor()
                cur.execute("INSERT OR IGNORE INTO company_groups(name) VALUES(?)", (bname,))
                cur.execute("SELECT id FROM company_groups WHERE name = ?", (bname,))
                row = cur.fetchone()
                if row is None:
                    st.error("Unexpected error while creating bucket.")
                else:
                    gid = int(row[0])
                    cur.executemany(
                        "INSERT OR IGNORE INTO company_group_members(group_id, company_id) VALUES(?, ?)",
                        [(gid, cid) for cid in sel_company_ids],
                    )
                    conn.commit()
                    st.success(f"Saved bucket '{bname}' with {len(sel_company_ids)} companies.")

        # Bucket selection
        groups_df = pd.read_sql_query("SELECT id, name FROM company_groups ORDER BY name", conn)
        bucket_names_selected: List[str] = []
        group_name_to_id: Dict[str, int] = {}
        if not groups_df.empty:
            group_name_to_id = {str(row["name"]): int(row["id"]) for _, row in groups_df.iterrows()}
            bucket_names_selected = st.multiselect(
                "Or select one or more saved buckets",
                options=list(group_name_to_id.keys()),
                key="cs_bucket_select",
            )

        yr_input = st.text_input(
            "Year range (e.g., 'Recent - 2020' or '2023-2018')",
            value="Recent - 2020",
            key="cs_year_range_input",
        )

        stdev_mode = st.radio(
            "Standard deviation mode",
            ["Sample (ddof=1)", "Population (ddof=0)"],
            horizontal=True,
            key="cs_stdev_mode",
        )
        sample = (stdev_mode == "Sample (ddof=1)")

        def parse_range(s: str, available_years: List[int]) -> Tuple[int, int]:
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

        # Determine final set of company ids based on direct selection + buckets
        selected_company_ids: List[int] = []
        for opt in options:
            m = re.search(r"\[id=(\d+)\]$", opt)
            if m:
                selected_company_ids.append(int(m.group(1)))

        if bucket_names_selected:
            bucket_ids = [group_name_to_id[bname] for bname in bucket_names_selected if bname in group_name_to_id]
            if bucket_ids:
                q_marks = ",".join("?" for _ in bucket_ids)
                bucket_members_df = pd.read_sql_query(
                    f"SELECT DISTINCT company_id FROM company_group_members WHERE group_id IN ({q_marks})",
                    conn,
                    params=bucket_ids,
                )
                for _, r in bucket_members_df.iterrows():
                    cid = int(r["company_id"])
                    if cid not in selected_company_ids:
                        selected_company_ids.append(cid)

        if not selected_company_ids:
            st.info("Select at least one company (directly or via buckets) to see Capital Structure metrics.")
            return

        id_to_name_ticker = {
            int(r["id"]): (str(r["name"]), str(r["ticker"])) for _, r in companies_df.iterrows()
        }

        rows_out: List[Dict[str, Optional[float]]] = []
        for cid in selected_company_ids:
            try:
                # Ensure required derived series exist in DB (safe to call repeatedly)
                compute_and_store_debt_equity(conn, cid)
                try:
                    compute_and_store_levered_beta(conn, cid)
                except Exception:
                    pass
                try:
                    compute_and_store_cost_of_equity(conn, cid)
                except Exception:
                    pass
                try:
                    compute_and_store_default_spread(conn, cid)
                except Exception:
                    pass
                try:
                    compute_and_store_pre_tax_cost_of_debt(conn, cid)
                except Exception:
                    pass
                try:
                    compute_and_store_wacc(conn, cid)
                except Exception:
                    pass
                try:
                    compute_and_store_roic_wacc_spread(conn, cid)
                except Exception:
                    pass

        
                ann_de = get_annual_debt_equity_series(conn, cid)
                if ann_de.empty:
                    continue
        
                available_years = sorted(set(int(x) for x in ann_de["year"].tolist()))
                if not available_years:
                    continue
        
                yr_start, yr_end = parse_range(yr_input, available_years)
                if yr_start < yr_end:
                    yr_start, yr_end = yr_end, yr_start
        
                med_de, std_de = _compute_level_stats(
                    ann_de,
                    yr_start,
                    yr_end,
                    value_col="debt_equity",
                    stdev_sample=sample,
                )

                ann_roic = get_annual_roic_direct_upload_series(conn, cid)
                med_roic, std_roic = (None, None)
                if not ann_roic.empty:
                    med_roic, std_roic = _compute_level_stats(
                        ann_roic,
                        yr_start,
                        yr_end,
                        value_col="roic_pct",
                        stdev_sample=sample,
                    )

                ann_spread = get_annual_roic_wacc_spread_series(conn, cid)
                med_spread, std_spread = (None, None)
                if not ann_spread.empty:
                    med_spread, std_spread = _compute_level_stats(
                        ann_spread,
                        yr_start,
                        yr_end,
                        value_col="spread_pct",
                        stdev_sample=sample,
                    )


        
                # Latest-year cost-of-capital metrics (based on the selected year range)
                selected_year = int(yr_start)
                ann_coe = get_annual_cost_of_equity_series(conn, cid)
                ann_pcd = get_annual_pre_tax_cost_of_debt_series(conn, cid)
                ann_wacc = get_annual_wacc_series(conn, cid)
        
                def _value_for_year(df: pd.DataFrame, year: int, col: str) -> Optional[float]:
                    try:
                        if df is None or df.empty:
                            return None
                        sub = df[df["year"] == year]
                        if sub.empty:
                            return None
                        v = float(sub.iloc[-1][col])
                        return v if np.isfinite(v) else None
                    except Exception:
                        return None
        
                coe_latest = _value_for_year(ann_coe, selected_year, "cost_of_equity")
                pcd_latest = _value_for_year(ann_pcd, selected_year, "pre_tax_cost_of_debt")
                wacc_latest = _value_for_year(ann_wacc, selected_year, "wacc")
        
                name, ticker = id_to_name_ticker.get(int(cid), (f"id={cid}", ""))
                rows_out.append(
                    {
                        "Company": name,
                        "Ticker": ticker,
                        "As of Year (Latest in Range)": selected_year,
                        "Median Debt/Market Capitalization": med_de,
                        "Debt/Market Capitalization Standard Deviation": std_de,
                        "Median ROIC % (Direct Upload)": med_roic,
                        "ROIC % Standard Deviation (Direct Upload)": std_roic,
                        "Median Spread % (ROIC - WACC)": med_spread,
                        "Spread % Standard Deviation (ROIC - WACC)": std_spread,
                        "Cost of Equity (Latest in Range) %": coe_latest,
                        "Pre-Tax Cost of Debt (Latest in Range) %": pcd_latest,
                        "WACC (Latest in Range) %": wacc_latest,
                    }
                )
            except Exception as e:
                st.warning(f"Skipping company id={cid} for Capital Structure metrics: {e}")
        
        if not rows_out:
            st.info("No Capital Structure & Spread metrics to display for the selected year range.")
            return
        
        out_df = pd.DataFrame(rows_out)
        
        def fmt_ratio(x: Optional[float]) -> str:
            return "—" if x is None or pd.isna(x) else f"{float(x):.2f}"
        
        def fmt_pct(x: Optional[float]) -> str:
            # Stored as percentage points (e.g., 6.62 means 6.62%)
            return "—" if x is None or pd.isna(x) else f"{float(x):.2f}"
        
        show_df = out_df.copy()
        show_df["Median Debt/Market Capitalization"] = show_df["Median Debt/Market Capitalization"].map(fmt_ratio)
        show_df["Debt/Market Capitalization Standard Deviation"] = show_df["Debt/Market Capitalization Standard Deviation"].map(
            fmt_ratio
        )
        show_df["Median ROIC % (Direct Upload)"] = show_df["Median ROIC % (Direct Upload)"].map(fmt_pct)
        show_df["ROIC % Standard Deviation (Direct Upload)"] = show_df["ROIC % Standard Deviation (Direct Upload)"].map(fmt_pct)
        show_df["Median Spread % (ROIC - WACC)"] = show_df["Median Spread % (ROIC - WACC)"].map(fmt_pct)
        show_df["Spread % Standard Deviation (ROIC - WACC)"] = show_df["Spread % Standard Deviation (ROIC - WACC)"].map(fmt_pct)


        show_df["Cost of Equity (Latest in Range) %"] = show_df["Cost of Equity (Latest in Range) %"].map(fmt_pct)
        show_df["Pre-Tax Cost of Debt (Latest in Range) %"] = show_df["Pre-Tax Cost of Debt (Latest in Range) %"].map(fmt_pct)
        show_df["WACC (Latest in Range) %"] = show_df["WACC (Latest in Range) %"].map(fmt_pct)
        
        st.subheader("Results")
        st.dataframe(
            show_df,
            use_container_width=True,
            column_config={
                "As of Year (Latest in Range)": st.column_config.NumberColumn(
                    help="For each company, this is the latest year within the selected range for which the Cost of Capital metrics are shown.",
                    format="%d",
                ),
                "Median Debt/Market Capitalization": st.column_config.TextColumn(
                    help="Median of annual Total Debt / Market Capitalization over the selected range.",
                ),
                "Debt/Market Capitalization Standard Deviation": st.column_config.TextColumn(
                    help="Standard deviation of annual Total Debt / Market Capitalization over the selected range.",
                ),
                "Median ROIC % (Direct Upload)": st.column_config.TextColumn(
                    help="Median of annual ROIC (direct upload) over the selected range.",
                ),
                "ROIC % Standard Deviation (Direct Upload)": st.column_config.TextColumn(
                    help="Standard deviation of annual ROIC (direct upload) over the selected range.",
                ),
                "Median Spread % (ROIC - WACC)": st.column_config.TextColumn(
                    help="Median of annual Spread% (ROIC% - WACC%) over the selected range (stored as percentage points).",
                ),
                "Spread % Standard Deviation (ROIC - WACC)": st.column_config.TextColumn(
                    help="Standard deviation of annual Spread% (ROIC% - WACC%) over the selected range (stored as percentage points).",
                ),
                "Cost of Equity (Latest in Range) %": st.column_config.TextColumn(
                    help="Cost of Equity for the latest year in the selected range (stored as percentage points).",
                ),
                "Pre-Tax Cost of Debt (Latest in Range) %": st.column_config.TextColumn(
                    help="Pre-Tax Cost of Debt for the latest year in the selected range (stored as percentage points).",
                ),
                "WACC (Latest in Range) %": st.column_config.TextColumn(
                    help="Weighted Average Cost of Capital (WACC) for the latest year in the selected range (stored as percentage points).",
                ),
            },
        )
    st.markdown("---")

    with st.expander("FCFE and Spread Score Dashboard", expanded=True):
        conn = get_conn()
        init_db(conn)

        # -----------------------------
        # User inputs
        # -----------------------------
        yr_input = st.text_input(
            "Year range for FCFE and Spread Score (e.g., 'Recent - 2020' or '2023-2018')",
            value="Recent - 2020",
            key="cs_fcfe_spread_year_range",
        )

        stdev_mode = st.radio(
            "Standard deviation mode for FCFE and Spread Score",
            ['Sample (ddof=1)', 'Population (ddof=0)'],
            horizontal=True,
            key="cs_fcfe_spread_stdev_mode",
        )
        sample = (stdev_mode == 'Sample (ddof=1)')

        def parse_range(s: str, available_years: List[int]) -> Tuple[int, int]:
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
                    num += float(val) * float(wt)
                    den += float(wt)
            if den == 0.0:
                return None
            return num / den

        # -----------------------------
        # Bucket selection
        # -----------------------------
        groups_df = pd.read_sql_query("SELECT id, name FROM company_groups ORDER BY name", conn)
        if groups_df.empty:
            st.info("No buckets defined yet. Define buckets in the P&L or Balance Sheet Metrics sections.")
            return

        group_name_to_id = {str(row["name"]): int(row["id"]) for _, row in groups_df.iterrows()}
        bucket_names_selected = st.multiselect(
            "Select one or more buckets for FCFE and Spread Score computation",
            options=list(group_name_to_id.keys()),
            key="cs_fcfe_spread_bucket_select",
        )

        if not bucket_names_selected:
            st.info("Select at least one bucket to compute the FCFE and Spread Score Dashboard.")
            return

        group_ids = [group_name_to_id[name] for name in bucket_names_selected if name in group_name_to_id]
        placeholders = ",".join(["?"] * len(group_ids))
        bucket_members_df = pd.read_sql_query(
            f"SELECT DISTINCT company_id FROM company_group_members WHERE group_id IN ({placeholders})",
            conn,
            params=group_ids,
        )
        if bucket_members_df.empty:
            st.info("No companies found in the selected buckets.")
            return

        score_company_ids = [int(x) for x in bucket_members_df["company_id"].tolist()]

        companies_df = list_companies(conn)
        id_to_meta = {int(r["id"]): (str(r["name"]), str(r["ticker"])) for _, r in companies_df.iterrows()}

        # -----------------------------
        # Compute scores
        # -----------------------------
        gw_fcfe = get_factor_weight(growth_weight_map, "FCFE Growth")
        gw_spread = get_factor_weight(growth_weight_map, "Spread")

        sw_fcfe = get_factor_weight(stddev_weight_map, "FCFE Growth")
        sw_spread = get_factor_weight(stddev_weight_map, "Spread")

        rows: List[Dict[str, Optional[float]]] = []
        for cid in score_company_ids:
            if cid not in id_to_meta:
                continue
            name, ticker = id_to_meta[cid]

            try:
                # Ensure underlying metrics exist / are fresh
                compute_and_store_fcff_and_reinvestment_rate(conn, cid)
                compute_and_store_roic_wacc_spread(conn, cid)

                fcff_df = get_annual_fcff_series(conn, cid)
                spread_df = get_annual_roic_wacc_spread_series(conn, cid)

                available_years = set()
                if fcff_df is not None and not fcff_df.empty and "year" in fcff_df.columns:
                    available_years.update([int(y) for y in fcff_df["year"].dropna().tolist()])
                if spread_df is not None and not spread_df.empty and "year" in spread_df.columns:
                    available_years.update([int(y) for y in spread_df["year"].dropna().tolist()])

                if not available_years:
                    continue

                yr_start, yr_end = parse_range(yr_input, sorted(available_years))

                # YoY FCFF change % stats
                med_fcff_g, std_fcff_g = (None, None)
                if fcff_df is not None and not fcff_df.empty:
                    med_fcff_g, std_fcff_g = compute_growth_stats(
                        fcff_df,
                        yr_start,
                        yr_end,
                        stdev_sample=sample,
                        value_col="fcff",
                        abs_denom=True,
                    )

                median_yoy_fcff_change_pct = (float(med_fcff_g) * 100.0) if med_fcff_g is not None else None
                std_yoy_fcff_change_pct = (float(std_fcff_g) * 100.0) if std_fcff_g is not None else None

                # Spread level stats (percentage points)
                med_spread, std_spread = (None, None)
                if spread_df is not None and not spread_df.empty:
                    med_spread, std_spread = _compute_level_stats(
                        spread_df,
                        yr_start,
                        yr_end,
                        value_col="spread_pct",
                        stdev_sample=sample,
                    )

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

                if weighted_growth is None or weighted_stddev is None:
                    additive = None
                    scaled = None
                else:
                    additive = weighted_growth - weighted_stddev
                    scaled = weighted_growth / (1.0 + weighted_stddev)

                rows.append(
                    {
                        "Ticker": ticker,
                        "Company Name": name,
                        "Weighted FCFE and Spread Score": weighted_growth,
                        "Weighted FCFE and Spread Standard Deviation Score": weighted_stddev,
                        "Additive FCFE and Spread Score": additive,
                        "Scaled FCFE and Spread Score": scaled,
                    }
                )

            except Exception as e:
                st.warning(f"Skipping company {name} ({ticker}) in FCFE and Spread Score computation: {e}")

        if not rows:
            st.info("No score rows to display. Ensure FCFE and Spread data exist for the selected companies.")
            return

        df = pd.DataFrame(rows)
        df = df.sort_values(
            by="Scaled FCFE and Spread Score",
            ascending=False,
            na_position="last",
        )

        def fmt_score(x: Optional[float]) -> str:
            if x is None or pd.isna(x):
                return "—"
            return f"{float(x):.2f}"

        display_df = df.copy()
        for col in [
            "Weighted FCFE and Spread Score",
            "Weighted FCFE and Spread Standard Deviation Score",
            "Additive FCFE and Spread Score",
            "Scaled FCFE and Spread Score",
        ]:
            display_df[col] = display_df[col].map(fmt_score)

        st.subheader("FCFE and Spread Score — Ranked by Scaled FCFE and Spread Score")
        st.dataframe(
            display_df,
            use_container_width=True,
            column_config={
                "Weighted FCFE and Spread Score": st.column_config.TextColumn(
                    help="Formula: ((Median YoY FCFF Change % * FCFE Growth weight) + (Median Spread % * Spread weight)) / (FCFE Growth weight + Spread weight).",
                ),
                "Weighted FCFE and Spread Standard Deviation Score": st.column_config.TextColumn(
                    help="Formula: ((YoY FCFF % stdev * FCFE Growth stdev weight) + (Spread % stdev * Spread stdev weight)) / (FCFE Growth stdev weight + Spread stdev weight).",
                ),
                "Additive FCFE and Spread Score": st.column_config.TextColumn(
                    help="Formula: Weighted FCFE and Spread Score − Weighted FCFE and Spread Standard Deviation Score.",
                ),
                "Scaled FCFE and Spread Score": st.column_config.TextColumn(
                    help="Formula: Weighted FCFE and Spread Score / (1 + Weighted FCFE and Spread Standard Deviation Score).",
                ),
            },
        )
