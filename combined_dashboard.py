
import io
import re
from typing import Dict, List, Optional, Tuple

import streamlit as st

from core import *  # noqa: F401,F403
from dashboard_category_filter import select_category_company_ids
from dashboard_spreadsheet_filter import select_spreadsheet_company_ids
from dashboard_upload_dates import UPLOAD_DATE_COLUMN, add_latest_upload_dates
from score_weights_ui import render_score_weight_summary
from score_range_dashboard import build_score_range_frame, render_score_range_dashboard
from ui_theme import DASHBOARD_COLUMN_LABELS, company_label_map, dashboard_section, display_table_frame, render_dashboard_table


def _add_dashboard_countries(conn, frame: pd.DataFrame, companies_df: pd.DataFrame) -> pd.DataFrame:
    """Attach each company's stored country by ID without changing score order."""
    out = frame.copy()
    out["Country"] = ""
    if out.empty or "_company_id" not in out:
        return out
    countries = companies_df
    if "country" not in countries.columns and conn is not None:
        company_ids = sorted({int(value) for value in out["_company_id"]})
        placeholders = ",".join("?" for _ in company_ids)
        countries = read_df(
            "SELECT * FROM companies WHERE id IN (" + placeholders + ")",
            conn, params=company_ids,
        )
    # Legacy company records can lack a country; never infer one from the ticker.
    if "country" in countries.columns:
        out["Country"] = out["_company_id"].map(countries.set_index("id")["country"]).fillna("")
    return out




def _combined_dashboard_excel_bytes(
    overall_df: pd.DataFrame,
    breakup_df: Optional[pd.DataFrame] = None,
) -> bytes:
    """Build the Combined Dashboard Excel export with one sheet per dashboard."""
    output = io.BytesIO()
    sheets: List[Tuple[str, pd.DataFrame]] = [
        ("Overall Score Dashboard", display_table_frame(overall_df)),
    ]
    if breakup_df is not None and not breakup_df.empty:
        sheets.append(("Overall Score Dashboard Breakup", display_table_frame(breakup_df)))

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet_name, sheet_df in sheets:
            sheet_df.to_excel(writer, index=False, sheet_name=sheet_name)
            worksheet = writer.sheets[sheet_name]
            if UPLOAD_DATE_COLUMN in sheet_df.columns:
                date_column = list(sheet_df.columns).index(UPLOAD_DATE_COLUMN) + 1
                for row_number in range(2, worksheet.max_row + 1):
                    worksheet.cell(row=row_number, column=date_column).number_format = "dd mmm yyyy"
            worksheet.freeze_panes = "A2"
            worksheet.auto_filter.ref = worksheet.dimensions

            for column_cells in worksheet.columns:
                header = str(column_cells[0].value or "")
                max_len = len(header)
                for cell in column_cells[1:]:
                    value = cell.value
                    if value is None:
                        continue
                    max_len = max(max_len, len(str(value)))
                worksheet.column_dimensions[column_cells[0].column_letter].width = min(max(max_len + 2, 10), 42)

    return output.getvalue()


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


def parse_range_overall(s: str, available_years: List[int]) -> Tuple[int, int]:
    s = (s or "").strip()
    if not available_years:
        raise ValueError("No annual years available.")
    most_recent = max(available_years)
    m_recent = re.match(r"^recent\s*[-â€“]\s*(\d{4})$", s, flags=re.IGNORECASE)
    m_two = re.match(r"^(\d{4})\s*[-â€“]\s*(\d{4})$", s)
    if m_recent:
        end = int(m_recent.group(1))
        return most_recent, end
    if m_two:
        start, end = int(m_two.group(1)), int(m_two.group(2))
        return start, end
    raise ValueError("Could not parse the year range. Use 'Recent - YYYY' or 'YYYY-YYYY'.")

def get_factor_weight(weight_map: Dict[str, float], *names: str) -> Optional[float]:
    for nm in names:
        if nm in weight_map:
            return weight_map[nm]
    return None

def weighted_score(pairs: List[Tuple[Optional[float], Optional[float]]]) -> Optional[float]:
    return compute_score_weighted_average(pairs)


def _compute_company_overall_scores(
    company_series: Dict[str, pd.DataFrame],
    yr_input_overall: str,
    growth_weight_map: Dict[str, float],
    stddev_weight_map: Dict[str, float],
    *,
    sample_overall: bool = True,
    debt_penalty_lambda: float = 1.0,
    cid: int = 0,
) -> dict:
    """Use the same component ranges, statistics, weights and debt rule for every window."""
    warnings = []
    missing_debt_penalty_count = 0
    # Breakup dashboard metrics (year-range aligned; percent fields are % points)
    breakup_total_scaled: Optional[float] = None
    breakup_total_debt_adjusted_scaled: Optional[float] = None
    included_years_for_calculation: Optional[str] = None
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
        ann_acc = company_series["accumulated_profit"]
        ann_acc_stats = exclude_recent_zero_accumulated_profit_for_stats(ann_acc)
        ann_roe = company_series["roe"]
        ann_roce = company_series["roce"]
        ann_interest_load = company_series["interest_load_pct"]

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
            if not ann_acc_stats.empty:
                med_acc_g, std_acc_g = compute_growth_stats(
                    ann_acc_stats,
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

            # Use raw annual inputs, not the legacy stored Interest Load %.
            # Keep the existing BS range so other component scores are unchanged.
            median_interest_penalty, included_year_count = compute_available_interest_penalty(
                company_series.get("operating_income"),
                company_series.get("interest_expense"),
                yr_start_bs,
                yr_end_bs,
            )
            selected_year_count = yr_start_bs - yr_end_bs + 1
            included_years_for_calculation = f"{included_year_count}/{selected_year_count}"

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
                bs_scaled = compute_scaled_volatility_score(weighted_strength, weighted_stddev_bs)

            if bs_scaled is not None:
                if median_interest_penalty is not None:
                    bs_debt = sum_finite_scores(bs_scaled, -debt_penalty_lambda * median_interest_penalty)
                else:
                    missing_debt_penalty_count += 1

    except Exception as e:
        warnings.append(f"Skipping Balance Sheet score computation for company_id={cid}: {e}")

    # -------------------------
    # P&L score part
    # -------------------------
    try:
        ann_rev = company_series["revenue"]
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

            ann_pt = company_series["pretax_income"]
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

            ann_ni = company_series["net_income"]
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

            ann_nopat = company_series["nopat"]
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

            ann_om = company_series["margin"]
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
                pl_scaled = compute_scaled_volatility_score(weighted_growth, weighted_stddev_pl)

    except Exception as e:
        warnings.append(f"Skipping P&L score computation for company_id={cid}: {e}")


    # -------------------------
    # FCFF + Spread score part
    # -------------------------
    fs_add: Optional[float] = None
    fs_scaled: Optional[float] = None
    try:
        fcff_df = company_series["fcff"]
        spread_df = company_series["spread_pct"]

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
            gw_fcff = get_factor_weight(growth_weight_map, "FCFF Growth", "FCFE Growth")
            gw_spread = get_factor_weight(growth_weight_map, "Spread")

            sw_fcff = get_factor_weight(stddev_weight_map, "FCFF Growth", "FCFE Growth")
            sw_spread = get_factor_weight(stddev_weight_map, "Spread")

            growth_pairs = [
                (median_yoy_fcff_change_pct, gw_fcff),
                (med_spread, gw_spread),
            ]
            stddev_pairs = [
                (std_yoy_fcff_change_pct, sw_fcff),
                (std_spread, sw_spread),
            ]

            weighted_growth = weighted_score(growth_pairs)
            weighted_stddev = weighted_score(stddev_pairs)

            if weighted_growth is not None and weighted_stddev is not None:
                fs_add = weighted_growth - weighted_stddev
                fs_scaled = compute_scaled_volatility_score(weighted_growth, weighted_stddev)

    except Exception as e:
        warnings.append(f"Skipping FCFF and Spread score computation for company_id={cid}: {e}")


    total_additive = (
        (bs_add + pl_add + fs_add)
        if (bs_add is not None and pl_add is not None and fs_add is not None)
        else None
    )
    total_scaled = sum_finite_scores(bs_scaled, pl_scaled, fs_scaled)
    total_debt_adjusted_scaled = sum_finite_scores(bs_debt, pl_scaled, fs_scaled)



    return {
        "scores": {
            "Additive Volatility-Adjusted Balance Sheet Strength Score": bs_add,
            "Scaled Volatility-Adjusted Balance Sheet Strength Score": bs_scaled,
            "Debt-Adjusted Balance Sheet Strength Score": bs_debt,
            "Additive Volatility-Adjusted P&L Growth Score": pl_add,
            "Scaled Volatility-Adjusted P&L Growth Score": pl_scaled,
            "Additive FCFF and Spread Score": fs_add,
            "Scaled FCFF and Spread Score": fs_scaled,
            "Total Additive Volatility-Adjusted Score": total_additive,
            "Total Scaled Volatility-Adjusted Score": total_scaled,
            "Total Debt-Adjusted Scaled Volatility-Adjusted Score": total_debt_adjusted_scaled,
        },
        "breakup": {
            "Total Scaled Volatility-Adjusted Score": total_scaled,
            "Total Debt-Adjusted Scaled Volatility-Adjusted Score": total_debt_adjusted_scaled,
            "Included Years for Calculation": included_years_for_calculation,
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
        },
        "has_scores": any(v is not None for v in (bs_add, bs_scaled, bs_debt, pl_add, pl_scaled, fs_add, fs_scaled)),
        "missing_debt_penalty_count": missing_debt_penalty_count,
        "warnings": warnings,
    }


def render_combined_dashboard_tab() -> None:
    """Render the Combined Dashboard / Overall Score Dashboard tab."""
    st.title("Combined Dashboard")

    with st.expander("Overall Score Dashboard", expanded=True):
        conn = get_db()
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

        # -----------------------------
        # Load growth / stddev weights
        # -----------------------------
        try:
            growth_weights_df = normalize_score_weight_factors(read_df(
                "SELECT factor, weight FROM growth_weight_factors", conn,
            ))
            stddev_weights_df = normalize_score_weight_factors(read_df(
                "SELECT factor, weight FROM stddev_weight_factors", conn,
            ))
        except ValueError as error:
            st.error(str(error))
            return

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
        selection_mode = st.radio(
            "Analyze by",
            ["Company", "Industry Bucket", "Category / Sub-Category", "Spreadsheet"],
            horizontal=True,
            key="combined_score_mode",
        )

        debt_penalty_lambda = st.number_input(
            "Debt penalty strength (Lambda)",
            min_value=0.01,
            value=1.0,
            step=0.1,
            format="%.2f",
            help=(
                "Maximum deduction in score points. The balance-sheet score is reduced by "
                "Lambda × the median annual interest penalty over the selected years."
            ),
            key="combined_debt_penalty_lambda",
        )
        if not np.isfinite(debt_penalty_lambda) or debt_penalty_lambda <= 0.0:
            st.error("Debt penalty strength (Lambda) must be a finite positive number.")
            return

        render_score_weight_summary(growth_weights_df, stddev_weights_df)

        score_company_ids: List[int] = []

        if selection_mode == "Company":
            labels = company_label_map(companies_df)
            selected_companies = st.multiselect(
                "Select one or more companies for Overall Score computation",
                options=list(labels.keys()),
                format_func=lambda company_id: labels.get(company_id, str(company_id)),
                key="combined_score_company_select",
            )
            score_company_ids = [int(company_id) for company_id in selected_companies]
        elif selection_mode == "Industry Bucket":
            groups_df = read_df(
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

            if bucket_names_selected:
                group_ids = [group_name_to_id[name] for name in bucket_names_selected if name in group_name_to_id]
                if group_ids:
                    placeholders = ",".join(["?"] * len(group_ids))
                    bucket_members_df = read_df(
                        f"SELECT DISTINCT company_id FROM company_group_members WHERE group_id IN ({placeholders})",
                        conn,
                        params=group_ids,
                    )
                    if not bucket_members_df.empty:
                        score_company_ids = [int(x) for x in bucket_members_df["company_id"].tolist()]
        elif selection_mode == "Spreadsheet":
            score_company_ids = select_spreadsheet_company_ids(
                conn, key="combined_score_spreadsheet",
                result_state_key="combined_score_results",
            )
        else:
            selected_ids = select_category_company_ids(
                conn,
                category_key="combined_score_categories_select",
                subcategory_key="combined_score_subcategory_select",
                subcategory_label="Select one or more sub-categories for Overall Score computation",
            )
            if selected_ids is None:
                return
            score_company_ids = selected_ids

        if not score_company_ids:
            if selection_mode != "Spreadsheet":
                st.info("Select at least one company, industry bucket, or sub-category to compute Overall Scores.")
            return

        score_company_ids = sorted(set(score_company_ids))

        company_buckets_map = get_company_valuation_industry_bucket_map(
            conn,
            score_company_ids,
        )

        compute_overall_scores = st.button(
            "Compute Overall Scores",
            type="primary",
            key="compute_overall_scores",
            disabled=(len(score_company_ids) == 0),
        )
        request_signature = (
            yr_input_overall.strip().lower(), selection_mode, tuple(score_company_ids),
            sample_overall, float(debt_penalty_lambda),
            tuple(sorted(growth_weight_map.items())), tuple(sorted(stddev_weight_map.items())),
        )
        cache = st.session_state.get("combined_score_results")
        if not compute_overall_scores and (cache is None or cache["signature"] != request_signature):
            st.session_state.pop("combined_score_results", None)
            return

        progress_bar = None

        if compute_overall_scores:
            progress_total = len(score_company_ids) + 2
            progress_step = 0
            progress_bar = st.progress(0, text="Starting overall score computation...")

            def bump_progress(message: str) -> None:
                nonlocal progress_step
                progress_step += 1
                pct = int((progress_step / progress_total) * 100)
                progress_bar.progress(pct, text=message)

            # -----------------------------
            # Score computation
            # -----------------------------
            rows: List[Dict[str, Optional[float]]] = []

            rows_breakup: List[Dict[str, Optional[float]]] = []
            base_results = {}
            missing_debt_penalty_count = 0

            bump_progress("Loading dashboard metrics...")
            try:
                dashboard_series = get_combined_dashboard_series_batch(conn, score_company_ids)
            except Exception as e:
                st.error(f"Unable to load dashboard metrics: {e}")
                progress_bar.empty()
                return

            for idx, cid in enumerate(score_company_ids, start=1):
                row_info = companies_df[companies_df["id"] == cid]
                if row_info.empty:
                    bump_progress(f"Computing scores for company {idx}/{len(score_company_ids)}")
                    continue
                row_info = row_info.iloc[0]
                name = row_info["name"]
                ticker = row_info["ticker"]
                company_series = dashboard_series.get(cid, {})

                bump_progress(f"Computing scores for {name} ({idx}/{len(score_company_ids)})")

                result = _compute_company_overall_scores(
                    company_series, yr_input_overall, growth_weight_map, stddev_weight_map,
                    sample_overall=sample_overall, debt_penalty_lambda=debt_penalty_lambda, cid=cid,
                )
                base_results[cid] = result
                for warning in result["warnings"]:
                    st.warning(warning)
                missing_debt_penalty_count += result["missing_debt_penalty_count"]
                if not result["has_scores"]:
                    continue

                # -------------------------
                # Price change CAGR and frequency stats
                # -------------------------
                price_cagr_2020_2025: Optional[float] = None
                price_cagr_2015_2020: Optional[float] = None
                price_cagr_2010_2015: Optional[float] = None
                price_above_15_pct: Optional[float] = None

                try:
                    price_df = company_series["price_change"]
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

                rows_breakup.append(
                    {
                        "_company_id": cid,
                        "Company Name": name,
                        "Ticker": ticker,
                        "Industry Bucket": company_buckets_map.get(cid, "(no bucket)"),
                        **result["breakup"],
                    }
                )

                rows.append(
                    {
                        "_company_id": cid,
                        "Company Name": name,
                        "Ticker": ticker,
                        "Industry Bucket": company_buckets_map.get(cid, "(no bucket)"),
                        **result["scores"],
                        "2020-2025 CAGR": price_cagr_2020_2025,
                        "2015-2020 CAGR": price_cagr_2015_2020,
                        "2010-2015 CAGR": price_cagr_2010_2015,
                        "Above 15% Year%": price_above_15_pct,
                    }
                )

            if not rows:
                st.session_state.pop("combined_score_results", None)
                progress_bar.progress(100, text="No overall scores to display for the selected filters.")
                st.info("No overall scores to display for the selected year range and filters.")
                return

            progress_bar.progress(int((progress_step / progress_total) * 100), text="Computing year-range comparisons...")
            range_frame, range_windows, range_coverage = build_score_range_frame(
                rows, dashboard_series, yr_input_overall, base_results,
                lambda cid, window: _compute_company_overall_scores(
                    dashboard_series[cid], window, growth_weight_map, stddev_weight_map,
                    sample_overall=sample_overall, debt_penalty_lambda=debt_penalty_lambda, cid=cid,
                ),
            )
            generation = st.session_state.get("combined_score_generation", 0) + 1
            st.session_state["combined_score_generation"] = generation
            cache = {
                "signature": request_signature, "rows": rows, "rows_breakup": rows_breakup,
                "missing_debt_penalty_count": missing_debt_penalty_count,
                "range_frame": range_frame, "range_windows": range_windows,
                "range_coverage": range_coverage, "generation": generation,
            }
            st.session_state["combined_score_results"] = cache
        else:
            rows = cache["rows"]
            rows_breakup = cache["rows_breakup"]
            missing_debt_penalty_count = cache["missing_debt_penalty_count"]

        if missing_debt_penalty_count:
            st.warning(
                f"Debt-adjusted scores are unavailable for {missing_debt_penalty_count} "
                "selected companies: no year in the selected balance-sheet range has "
                "one valid operating-income value and one nonnegative interest-expense value. "
                "Missing or invalid inputs are not treated as zero debt burden."
            )

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
        df_sorted = _add_dashboard_countries(conn, df_sorted, companies_df)
        df_sorted = add_latest_upload_dates(conn, df_sorted)
        df_sorted = add_company_taxonomy_columns(conn, df_sorted)

        ordered_main_cols = [
            "Company Name",
            "Ticker",
            "Country",
            UPLOAD_DATE_COLUMN,
            "Category",
            "Sub-Category",
            "Industry Bucket",
            "Additive Volatility-Adjusted Balance Sheet Strength Score",
            "Scaled Volatility-Adjusted Balance Sheet Strength Score",
            "Debt-Adjusted Balance Sheet Strength Score",
            "Additive Volatility-Adjusted P&L Growth Score",
            "Scaled Volatility-Adjusted P&L Growth Score",
            "Additive FCFF and Spread Score",
            "Scaled FCFF and Spread Score",
            "Total Additive Volatility-Adjusted Score",
            "Total Scaled Volatility-Adjusted Score",
            "Total Debt-Adjusted Scaled Volatility-Adjusted Score",
            "2020-2025 CAGR",
            "2015-2020 CAGR",
            "2010-2015 CAGR",
            "Above 15% Year%",
        ]
        df_sorted = df_sorted[[c for c in ordered_main_cols if c in df_sorted.columns]]

        # Columns to show with 2-decimal formatting (including price metrics).
        score_cols = [
            "Additive Volatility-Adjusted Balance Sheet Strength Score",
            "Scaled Volatility-Adjusted Balance Sheet Strength Score",
            "Debt-Adjusted Balance Sheet Strength Score",
            "Additive Volatility-Adjusted P&L Growth Score",
            "Scaled Volatility-Adjusted P&L Growth Score",
            "Additive FCFF and Spread Score",
            "Scaled FCFF and Spread Score",
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
                return "-"
            try:
                return f"{float(x):.2f}"
            except Exception:
                return "-"

        display_df = display_table_frame(df_sorted)
        display_score_cols = [DASHBOARD_COLUMN_LABELS.get(c, c) for c in existing_score_cols]
        overall_help = {
            DASHBOARD_COLUMN_LABELS.get(c, c): c
            for c in df_sorted.columns
            if DASHBOARD_COLUMN_LABELS.get(c, c) != c
        }

        styler = display_df.style
        if display_score_cols:
            styler = styler.format(fmt_score_val, subset=display_score_cols)

        # Colour rules for the fixed-window CAGR columns.
        cagr_cols = [
            DASHBOARD_COLUMN_LABELS.get(c, c)
            for c in ["2020-2025 CAGR", "2015-2020 CAGR", "2010-2015 CAGR"]
            if DASHBOARD_COLUMN_LABELS.get(c, c) in display_df.columns
        ]

        def style_cagr(val: Optional[float]) -> str:
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return ""
            try:
                v = float(val)
            except Exception:
                return ""
            # > 15%: green background, white text
            if v > 15.0:
                return "background-color: #DCFCE7; color: #166534; font-weight: 650;"
            # Between 15% and 0% (inclusive of 0%): neutral background, amber text
            if v >= 0.0:
                return "background-color: #FEF3C7; color: #92400E; font-weight: 600;"
            # Below 0%: soft red background
            return "background-color: #FEE2E2; color: #991B1B; font-weight: 650;"

        def apply_elementwise_style(current_styler, style_func, *, subset):
            """Apply scalar CSS styles across pandas versions."""
            if hasattr(current_styler, "map"):
                return current_styler.map(style_func, subset=subset)
            return current_styler.applymap(style_func, subset=subset)

        if cagr_cols:
            styler = apply_elementwise_style(
                styler,
                style_cagr,
                subset=pd.IndexSlice[:, cagr_cols],
            )

        # Colour rules for the 'Above 15% Year%' column.
        pct_year_cols = [
            DASHBOARD_COLUMN_LABELS.get(c, c)
            for c in ["Above 15% Year%"]
            if DASHBOARD_COLUMN_LABELS.get(c, c) in display_df.columns
        ]

        def style_above_15_year(val: Optional[float]) -> str:
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return ""
            try:
                v = float(val)
            except Exception:
                return ""
            # > 60%: green background, white text
            if v > 60.0:
                return "background-color: #DCFCE7; color: #166534; font-weight: 650;"
            # Between 60% and 40% (inclusive of 40%): neutral background, amber text
            if v >= 40.0:
                return "background-color: #FEF3C7; color: #92400E; font-weight: 600;"
            # Below 40%: soft red background
            return "background-color: #FEE2E2; color: #991B1B; font-weight: 650;"

        if pct_year_cols:
            styler = apply_elementwise_style(
                styler,
                style_above_15_year,
                subset=pd.IndexSlice[:, pct_year_cols],
            )

        dashboard_section(
            "Overall Score Dashboard",
            "Combined P&L, balance-sheet, and FCFF/spread scores with compact display labels.",
        )
        render_dashboard_table(
            styler,
            column_config={
                UPLOAD_DATE_COLUMN: st.column_config.DateColumn(
                    format="DD MMM YYYY",
                    help="Latest successful financial-data upload date in UTC. Blank means no recorded upload date.",
                ),
            },
            help_map=overall_help,
            key="combined_overall_score_dashboard_table",
        )
        st.caption("Latest Uploaded Date is the financial-data upload date in UTC. Blank dates have no recorded upload history.")
        if progress_bar is not None:
            progress_bar.progress(100, text="Overall score computation complete.")



        # -----------------------------
        # Overall Score Dashboard Breakup
        # -----------------------------
        export_breakup_df: Optional[pd.DataFrame] = None
        if rows_breakup:
            df_breakup = add_company_taxonomy_columns(conn, pd.DataFrame(rows_breakup))

            # Order columns exactly as requested (ignore any missing)
            breakup_cols = [
                "Company Name",
                "Ticker",
                "Category",
                "Sub-Category",
                "Total Scaled Volatility-Adjusted Score",
                "Total Debt-Adjusted Scaled Volatility-Adjusted Score",
                "Included Years for Calculation",
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
            export_breakup_df = df_breakup

            # Keep company details and year coverage as text; format scores as 2-decimal.
            numeric_cols = [
                c for c in breakup_cols_existing
                if c not in ["Company Name", "Ticker", "Category", "Sub-Category", "Included Years for Calculation"]
            ]

            def fmt_break_val(x: Optional[float]) -> str:
                if x is None or (isinstance(x, float) and pd.isna(x)):
                    return "-"
                try:
                    return f"{float(x):.2f}"
                except Exception:
                    return "-"

            display_breakup_df = display_table_frame(df_breakup)
            breakup_help = {
                DASHBOARD_COLUMN_LABELS.get(c, c): c
                for c in df_breakup.columns
                if DASHBOARD_COLUMN_LABELS.get(c, c) != c
            }
            breakup_help["Included Years for Calculation"] = (
                "Years with valid paired operating income and interest expense used for the debt penalty "
                "/ total years in the selected inclusive range."
            )
            display_numeric_cols = [DASHBOARD_COLUMN_LABELS.get(c, c) for c in numeric_cols]

            styler_break = display_breakup_df.style
            if display_numeric_cols:
                styler_break = styler_break.format(fmt_break_val, subset=display_numeric_cols)

            dashboard_section(
                "Overall Score Dashboard Breakup",
                "Source metrics behind the combined score, using the same compact table treatment.",
            )
            render_dashboard_table(
                styler_break,
                help_map=breakup_help,
                key="combined_overall_score_breakup_table",
            )

        export_year_range = re.sub(r"[^A-Za-z0-9]+", "_", yr_input_overall.strip()).strip("_") or "selected_years"
        st.download_button(
            "Download Overall Score Dashboards Excel",
            data=_combined_dashboard_excel_bytes(df_sorted, export_breakup_df),
            file_name=f"overall_score_dashboards_{export_year_range}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="combined_overall_score_excel_download",
            on_click="ignore",
        )

        render_score_range_dashboard(
            cache["range_frame"], cache["range_windows"], cache["range_coverage"], cache["generation"],
        )
