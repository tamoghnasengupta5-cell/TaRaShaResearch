import re
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import streamlit as st

from core import (
    get_db,
  # noqa: F401
    compute_and_store_fcff_and_reinvestment_rate,
    compute_and_store_fcfe,
    compute_and_store_rd_spend_rate,
    compute_growth_stats,
    read_df,
    get_annual_fcff_series,
    get_annual_fcfe_series,
    get_annual_reinvestment_rate_series,
    get_annual_rd_spend_rate_series,
    get_annual_capital_expenditures_series,
    get_annual_depreciation_amortization_series,
    get_annual_non_cash_working_capital_series,
    get_annual_nopat_series,
    get_annual_net_income_series,
    get_annual_net_debt_issued_paid_series,
    get_conn,
    init_db,
    list_companies,
)


def render_cash_flow_and_spread_tab() -> None:
    st.title("Cash Flow and Reinvestment")

    with st.expander("Cash Flow and Reinvestment Metrics", expanded=True):
        conn = get_db()
        companies_df = list_companies(conn)
        if companies_df.empty:
            st.info("No companies in the database yet. Upload a spreadsheet in Data Upload tab.")
            return

        # Base selection: individual companies
        all_company_options = [
            f"{row.name} ({row.ticker}) [id={row.id}]" for _, row in companies_df.iterrows()
        ]

        # Default selection: companies not present in any bucket (same behavior as other tabs)
        try:
            bucket_df = read_df(
                "SELECT DISTINCT company_id FROM company_group_members",
                conn,
            )
            bucketed_ids = set(int(x) for x in bucket_df["company_id"].tolist()) if not bucket_df.empty else set()
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
            "Companies to analyze (for Cash Flow metrics)",
            options=all_company_options,
            default=default_company_options,
            key="cf_companies_to_analyze",
        )

        # Bucket selection: use previously saved buckets to drive the analysis
        groups_df = read_df(
            "SELECT id, name FROM company_groups ORDER BY name",
            conn,
        )
        bucket_names_selected: List[str] = []
        group_name_to_id: Dict[str, int] = {}
        if not groups_df.empty:
            group_name_to_id = {
                str(row["name"]): int(row["id"]) for _, row in groups_df.iterrows()
            }
            bucket_names_selected = st.multiselect(
                "Or select one or more saved buckets",
                options=list(group_name_to_id.keys()),
                key="cf_bucket_select",
            )

        yr_input = st.text_input(
            "Year range (e.g., 'Recent - 2020' or '2023-2018')",
            value="Recent - 2020",
            key="cf_year_range_input",
        )

        stdev_mode = st.radio(
            "Standard deviation mode",
            ['Sample (ddof=1)', 'Population (ddof=0)'],
            horizontal=True,
            key="cf_stdev_mode",
        )
        sample = (stdev_mode == 'Sample (ddof=1)')

        def parse_range(s: str, available_years: List[int]) -> Tuple[int, int]:
            s = (s or '').strip()
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

        # Add companies from selected buckets
        if bucket_names_selected:
            bucket_ids = [
                group_name_to_id[bname]
                for bname in bucket_names_selected
                if bname in group_name_to_id
            ]
            if bucket_ids:
                q_marks = ",".join("?" for _ in bucket_ids)
                bucket_members_df = read_df(
                    f"SELECT DISTINCT company_id FROM company_group_members WHERE group_id IN ({q_marks})",
                    conn,
                    params=bucket_ids,
                )
                for _, r in bucket_members_df.iterrows():
                    cid = int(r["company_id"])
                    if cid not in selected_company_ids:
                        selected_company_ids.append(cid)

        if not selected_company_ids:
            st.info("Select at least one company (directly or via buckets) to see Cash Flow metrics.")
            return

        rows_out: List[Dict[str, Optional[float]]] = []

        for cid in selected_company_ids:
            # Compute-and-store FCFF + Reinvestment Rate into DB based on currently available inputs
            compute_and_store_fcff_and_reinvestment_rate(conn, cid)
            compute_and_store_fcfe(conn, cid)
            compute_and_store_rd_spend_rate(conn, cid)

            # Pull stored series
            fcff_df = get_annual_fcff_series(conn, cid)
            fcfe_df = get_annual_fcfe_series(conn, cid)
            rr_df = get_annual_reinvestment_rate_series(conn, cid)
            rd_df = get_annual_rd_spend_rate_series(conn, cid)

            # For parsing the year range, try to use the broadest available set of years
            available_years: List[int] = []

            # Include base inputs too (in case computed series is sparse)
            try:
                nopat_df = get_annual_nopat_series(conn, cid)
                if not nopat_df.empty:
                    available_years.extend(nopat_df["year"].tolist())
            except Exception:
                pass

            try:
                ni_df = get_annual_net_income_series(conn, cid)
                if not ni_df.empty:
                    available_years.extend(ni_df["year"].tolist())
            except Exception:
                pass

            try:
                net_debt_map = get_annual_net_debt_issued_paid_series(conn, cid)
                if net_debt_map:
                    available_years.extend(list(net_debt_map.keys()))
            except Exception:
                pass

            try:
                ncwc_df = get_annual_non_cash_working_capital_series(conn, cid)
                if not ncwc_df.empty:
                    available_years.extend(ncwc_df["year"].tolist())
            except Exception:
                pass

            try:
                capex_map = get_annual_capital_expenditures_series(conn, cid)
                available_years.extend(list(capex_map.keys()))
            except Exception:
                pass

            try:
                da_map = get_annual_depreciation_amortization_series(conn, cid)
                available_years.extend(list(da_map.keys()))
            except Exception:
                pass

            if not available_years:
                continue

            available_years = sorted(set(int(x) for x in available_years if pd.notna(x)))

            try:
                yr_start, yr_end = parse_range(yr_input, available_years)
                if yr_start < yr_end:
                    yr_start, yr_end = yr_end, yr_start
            except Exception:
                # If user entered an invalid range, skip this company (avoid breaking the full table)
                continue

            # -----------------------
            # YoY FCFF change % stats
            # -----------------------
            med_fcff_g, std_fcff_g = (None, None)
            if not fcff_df.empty:
                med_fcff_g, std_fcff_g = compute_growth_stats(
                    fcff_df,
                    yr_start,
                    yr_end,
                    stdev_sample=sample,
                    value_col="fcff",
                    abs_denom=True,
                )

            # -----------------------
            # YoY FCFE change % stats
            # -----------------------
            med_fcfe_g, std_fcfe_g = (None, None)
            if not fcfe_df.empty:
                med_fcfe_g, std_fcfe_g = compute_growth_stats(
                    fcfe_df,
                    yr_start,
                    yr_end,
                    stdev_sample=sample,
                    value_col="fcfe",
                    abs_denom=True,
                )

            # ---------------------------------------
            # Reinvestment Rate stats (level, not YoY)
            # ---------------------------------------
            med_rr, std_rr = (None, None)
            if not rr_df.empty:
                df_rr = rr_df.copy()
                df_rr = df_rr[(df_rr["year"] >= yr_end) & (df_rr["year"] <= yr_start)].sort_values("year")
                vals = df_rr["reinvestment_rate"].dropna().astype(float).values
                if vals.size > 0:
                    arr = np.array(vals, dtype=float)
                    med_rr = float(np.median(arr))
                    ddof = 1 if sample and arr.size > 1 else 0
                    std_rr = float(np.std(arr, ddof=ddof)) if arr.size > 1 or ddof == 0 else None

            
            # ----------------------------------------
            # R&D Spend Rate stats (level, not YoY)
            # ----------------------------------------
            med_rd, std_rd = (None, None)
            if not rd_df.empty:
                df_rd = rd_df.copy()
                df_rd = df_rd[(df_rd["year"] >= yr_end) & (df_rd["year"] <= yr_start)].sort_values("year")
                vals = df_rd["rd_spend_rate"].dropna().astype(float).values
                if vals.size > 0:
                    arr = np.array(vals, dtype=float)
                    med_rd = float(np.median(arr))
                    ddof = 1 if sample and arr.size > 1 else 0
                    std_rd = float(np.std(arr, ddof=ddof)) if arr.size > 1 or ddof == 0 else None

            name, ticker = companies_df.loc[
                companies_df["id"] == cid, ["name", "ticker"]
            ].values[0]

            rows_out.append(
                {
                    "Company Name": name,
                    "Ticker": ticker,
                    "Median YoY FCFF Change %": (None if med_fcff_g is None else med_fcff_g * 100.0),
                    "YoY FCFF Standard Deviation %": (None if std_fcff_g is None else std_fcff_g * 100.0),
                    "Median YoY FCFE Change %": (None if med_fcfe_g is None else med_fcfe_g * 100.0),
                    "YoY FCFE Standard Deviation %": (None if std_fcfe_g is None else std_fcfe_g * 100.0),
                    "Median Reinvestment Rate %": (None if med_rr is None else med_rr * 100.0),
                    "Reinvestment Rate Standard Deviation %": (None if std_rr is None else std_rr * 100.0),
                    "Median R&D Spend Rate %": (None if med_rd is None else med_rd * 100.0),
                    "R&D Spend Rate Standard Deviation %": (None if std_rd is None else std_rd * 100.0),
                }
            )

        if not rows_out:
            st.info("No Cash Flow metrics could be computed for the selected companies (missing CapEx / D&A / NOPAT / NCWC data).")
            return

        out_df = pd.DataFrame(rows_out)

        # Nicer display order
        disp_cols = [
            "Company Name",
            "Ticker",
            "Median YoY FCFF Change %",
            "YoY FCFF Standard Deviation %",
            "Median YoY FCFE Change %",
            "YoY FCFE Standard Deviation %",
            "Median Reinvestment Rate %",
            "Reinvestment Rate Standard Deviation %",
            "Median R&D Spend Rate %",
            "R&D Spend Rate Standard Deviation %",
        ]
        out_df = out_df[disp_cols]

        st.dataframe(out_df, use_container_width=True)
