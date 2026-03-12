from __future__ import annotations

from dataclasses import dataclass
from html import escape
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd
import streamlit as st

from core import (
    compute_and_store_cost_of_equity,
    compute_and_store_fcfe,
    compute_and_store_fcff_and_reinvestment_rate,
    compute_and_store_levered_beta,
    compute_and_store_pre_tax_cost_of_debt,
    compute_and_store_roic_wacc_spread,
    compute_and_store_total_equity_and_roe,
    compute_and_store_wacc,
    compute_growth_stats,
    get_annual_fcfe_series,
    get_annual_roic_direct_upload_series,
    get_annual_roic_wacc_spread_series,
    get_annual_series,
    get_annual_wacc_series,
    get_db,
    list_companies,
    read_df,
)
from ttc_efficiency import (
    _build_cash_and_equivalents_series,
    _compute_level_stats,
    _compute_value_creation_filter_metrics,
    _get_balance_sheet_params,
    _get_cash_flow_params,
    _get_company_buckets,
    _get_income_statement_params,
    _get_overall_score_weights,
    _get_working_capital_params,
    _load_series,
    _load_weight_maps,
    _median,
    _merge_ttm_into_annual,
    _parse_assumptions_sections,
    _parse_year_range,
    _score_balance_sheet,
    _score_cash_flow,
    _score_income_statement,
    _score_working_capital,
    _stdev_sample,
)

SEARCH_TAB_LABEL = "Search Aggregate"
SEARCH_ACTIVATE_KEY = "search_aggregate_activate_tab"
SEARCH_QUERY_KEY = "search_aggregate_query"
SEARCH_SELECTED_IDS_KEY = "search_aggregate_selected_ids"
SEARCH_YEAR_RANGE_KEY = "search_aggregate_year_range"
DEFAULT_YEAR_RANGE = "Recent - 2020"
MANDATORY_FIELD_IDS = ("ticker", "company_name")
DEFAULT_FIELD_IDS = (
    "industry_bucket",
    "wacc",
    "revenue_growth",
    "operating_margin",
    "operating_margin_growth",
    "roic",
    "spread",
    "roe",
    "fcff_growth",
    "fcfe_growth",
    "ttc_overall_score",
    "value_creation_score",
)

DEFAULT_SORT_FIELD_IDS = (
    "spread",
    "revenue_growth",
    "operating_margin_growth",
)

PERCENT_FIELD_IDS = {
    "wacc",
    "revenue_growth",
    "operating_margin",
    "operating_margin_growth",
    "roic",
    "spread",
    "roe",
    "fcff_growth",
    "fcfe_growth",
}


@dataclass(frozen=True)
class SearchField:
    label: str
    kind: str
    help_text: str


SEARCH_RESULT_FIELDS: Dict[str, SearchField] = {
    "ticker": SearchField("Ticker", "text", "Mandatory field."),
    "company_name": SearchField("Company Name", "text", "Mandatory field."),
    "industry_bucket": SearchField("Industry Bucket", "text", "Current company bucket membership."),
    "wacc": SearchField("WACC", "decimal4", "Latest available WACC inside the selected year range, shown as a decimal."),
    "revenue_growth": SearchField("Revenue Growth", "decimal4", "Median annual revenue growth for the selected range."),
    "operating_margin": SearchField("Operating Margin", "decimal4", "Median operating margin for the selected range."),
    "operating_margin_growth": SearchField("Operating Margin Growth", "decimal4", "Median YoY operating-margin growth for the selected range."),
    "roic": SearchField("ROIC", "decimal4", "Median ROIC for the selected range, normalized to decimal form."),
    "spread": SearchField("Spread", "decimal4", "Median ROIC-WACC spread for the selected range, normalized to decimal form."),
    "roe": SearchField("ROE", "decimal4", "Median ROE for the selected range."),
    "fcff_growth": SearchField("FCFF", "decimal4", "Median YoY FCFF growth for the selected range."),
    "fcfe_growth": SearchField("FCFE", "decimal4", "Median YoY FCFE growth for the selected range."),
    "ttc_overall_score": SearchField("Overall Score (Through-the-Cycle Efficiency Score)", "score2", "Combined TTC score using the current assumptions."),
    "value_creation_score": SearchField("Total Debt-Adjusted Scaled Volatility-Adjusted Score", "score2", "Current debt-adjusted value-creation score."),
}


def get_header_search_context(conn) -> Tuple[pd.DataFrame, List[str]]:
    return list_companies(conn), _build_year_range_options(conn)


def resolve_company_query(query: str, companies_df: pd.DataFrame) -> Tuple[List[int], List[str]]:
    tokens = [token.strip() for token in str(query or "").replace(";", ",").split(",") if token.strip()]
    if not tokens or companies_df.empty:
        return [], []

    rows = []
    for _, row in companies_df.iterrows():
        rows.append(
            {
                "id": int(row["id"]),
                "name": str(row["name"]),
                "ticker": str(row["ticker"]),
                "name_key": str(row["name"]).strip().lower(),
                "ticker_key": str(row["ticker"]).strip().lower(),
            }
        )

    selected_ids: List[int] = []
    unresolved: List[str] = []
    for token in tokens:
        token_key = token.lower()
        exact = [row for row in rows if row["ticker_key"] == token_key or row["name_key"] == token_key]
        if len(exact) == 1:
            selected_ids.append(exact[0]["id"])
            continue

        partial = [row for row in rows if token_key in row["ticker_key"] or token_key in row["name_key"]]
        if len(partial) == 1:
            selected_ids.append(partial[0]["id"])
            continue

        unresolved.append(token)

    return sorted(set(selected_ids)), unresolved


def render_search_aggregate_tab() -> None:
    st.title(SEARCH_TAB_LABEL)

    conn = get_db()
    companies_df = list_companies(conn)
    if companies_df.empty:
        st.info("No companies are available yet. Upload company financials first.")
        return

    selected_ids = [int(x) for x in st.session_state.get(SEARCH_SELECTED_IDS_KEY, []) if str(x).isdigit()]
    if not selected_ids:
        st.info("Use the header search bar to enter one or more company names or tickers, then click Go.")
        return

    year_range = str(st.session_state.get(SEARCH_YEAR_RANGE_KEY, DEFAULT_YEAR_RANGE) or DEFAULT_YEAR_RANGE)
    lookup = {
        int(row["id"]): f"{row['name']} ({row['ticker']})"
        for _, row in companies_df.iterrows()
    }
    chosen = [lookup[company_id] for company_id in selected_ids if company_id in lookup]
    if not chosen:
        st.info("None of the selected companies are present in the current database.")
        return

    result_df = build_search_aggregate_results(conn, selected_ids, year_range)
    if result_df.empty:
        st.info("No aggregate results could be computed for the selected companies and year range.")
        return

    st.markdown(
        "<p style='color:#111827; font-size:0.92rem; font-weight:400; margin:0.25rem 0 0.9rem 0;'>"
        "All results display the Median of the values in the selected year range"
        "</p>",
        unsafe_allow_html=True,
    )
    result_df = _render_result_controls(result_df)
    if result_df.empty:
        st.info("No results match the current filters.")
        return
    _render_glass_results_table(result_df)


def build_search_aggregate_results(conn, company_ids: Sequence[int], year_range: str, field_ids: Optional[Sequence[str]] = None) -> pd.DataFrame:
    resolved_field_ids = _resolve_field_ids(field_ids)
    growth_weight_map, stddev_weight_map = _load_weight_maps(conn)
    company_buckets = _get_company_buckets(conn, list(company_ids))
    ttc_context = _build_ttc_context(conn)
    companies_df = list_companies(conn)
    company_rows = companies_df[companies_df["id"].isin(company_ids)].copy()
    if company_rows.empty:
        return pd.DataFrame(columns=[SEARCH_RESULT_FIELDS[field_id].label for field_id in resolved_field_ids])

    rows: List[Dict[str, object]] = []
    for _, company_row in company_rows.iterrows():
        metrics = _compute_company_metrics(
            conn=conn,
            company_row=company_row,
            year_range=year_range,
            company_buckets=company_buckets,
            growth_weight_map=growth_weight_map,
            stddev_weight_map=stddev_weight_map,
            ttc_context=ttc_context,
        )
        rows.append({SEARCH_RESULT_FIELDS[field_id].label: metrics.get(field_id) for field_id in resolved_field_ids})

    df = pd.DataFrame(rows)
    return _apply_default_result_sort(df).reset_index(drop=True)


def _compute_company_metrics(
    *,
    conn,
    company_row: pd.Series,
    year_range: str,
    company_buckets: Dict[int, str],
    growth_weight_map: Dict[str, float],
    stddev_weight_map: Dict[str, float],
    ttc_context: Dict[str, object],
) -> Dict[str, object]:
    company_id = int(company_row["id"])

    compute_and_store_total_equity_and_roe(conn, company_id)
    compute_and_store_fcff_and_reinvestment_rate(conn, company_id)
    compute_and_store_fcfe(conn, company_id)
    compute_and_store_levered_beta(conn, company_id)
    compute_and_store_cost_of_equity(conn, company_id)
    compute_and_store_pre_tax_cost_of_debt(conn, company_id)
    compute_and_store_wacc(conn, company_id)
    compute_and_store_roic_wacc_spread(conn, company_id)

    metric_years = _collect_metric_years(conn, company_id)
    yr_start, yr_end = _safe_year_bounds(year_range, metric_years)

    value_creation = _compute_value_creation_filter_metrics(
        conn,
        company_id,
        yr_start,
        yr_end,
        growth_weight_map,
        stddev_weight_map,
    )

    fcfe_df = get_annual_fcfe_series(conn, company_id)
    fcfe_growth: Optional[float] = None
    if fcfe_df is not None and not fcfe_df.empty:
        fcfe_start, fcfe_end = _safe_year_bounds(year_range, [int(y) for y in fcfe_df["year"].dropna().tolist()])
        fcfe_growth, _ = compute_growth_stats(
            fcfe_df,
            fcfe_start,
            fcfe_end,
            stdev_sample=True,
            value_col="fcfe",
            abs_denom=True,
        )

    return {
        "ticker": str(company_row["ticker"]),
        "company_name": str(company_row["name"]),
        "industry_bucket": company_buckets.get(company_id, "(no bucket)"),
        "wacc": _latest_wacc_decimal_in_range(get_annual_wacc_series(conn, company_id), yr_start, yr_end),
        "revenue_growth": _decimal_from_percent_metric(value_creation.get("Revenue Growth%")),
        "operating_margin": _decimal_from_percent_metric(value_creation.get("Operating Margin%")),
        "operating_margin_growth": _decimal_from_percent_metric(value_creation.get("Operating Margin Expansion%")),
        "roic": _normalized_ratio_from_df(get_annual_roic_direct_upload_series(conn, company_id), "roic_pct", year_range),
        "spread": _decimal_from_percent_metric(value_creation.get("Spread%")),
        "roe": _decimal_from_percent_metric(value_creation.get("ROE%")),
        "fcff_growth": _decimal_from_percent_metric(value_creation.get("FCFF Growth%")),
        "fcfe_growth": fcfe_growth,
        "ttc_overall_score": _compute_ttc_overall_score(conn, company_id, year_range, ttc_context),
        "value_creation_score": value_creation.get("Total Debt-Adjusted Scaled Volatility-Adjusted Score"),
    }


def _build_ttc_context(conn) -> Dict[str, object]:
    sections = _parse_assumptions_sections(conn)
    return {
        "income_params": _get_income_statement_params(sections),
        "balance_params": _get_balance_sheet_params(sections),
        "working_capital_params": _get_working_capital_params(sections),
        "cash_flow_params": _get_cash_flow_params(sections),
        "overall_weights": _get_overall_score_weights(sections),
    }


def _compute_ttc_overall_score(conn, company_id: int, year_range: str, ttc_context: Dict[str, object]) -> Optional[float]:
    revenue_ann = _load_series(conn, "revenues_annual", "revenue", company_id)
    cogs_ann = _load_series(conn, "cost_of_revenue_annual", "cost_of_revenue", company_id)
    sga_ann = _load_series(conn, "sga_annual", "sga", company_id)
    operating_income_ann = _load_series(conn, "operating_income_annual", "operating_income", company_id)
    total_debt_ann = _load_series(conn, "total_debt_annual", "total_debt", company_id)
    cash_ann = _load_series(conn, "cash_and_cash_equivalents_annual", "cash_and_cash_equivalents", company_id)
    accounts_receivable_ann = _load_series(conn, "accounts_receivable_annual", "accounts_receivable", company_id)
    inventory_ann = _load_series(conn, "inventory_annual", "inventory", company_id)
    accounts_payable_ann = _load_series(conn, "accounts_payable_annual", "accounts_payable", company_id)
    total_current_assets_ann = _load_series(conn, "total_current_assets_annual", "total_current_assets", company_id)
    total_current_liabilities_ann = _load_series(conn, "total_current_liabilities_annual", "total_current_liabilities", company_id)
    current_debt_ann = _load_series(conn, "current_debt_annual", "current_debt", company_id)
    shareholders_equity_ann = _load_series(conn, "shareholders_equity_annual", "shareholders_equity", company_id)
    ebitda_ann = _load_series(conn, "ebitda_annual", "ebitda", company_id)
    interest_expense_ann = _load_series(conn, "interest_expense_annual", "interest_expense", company_id)
    ocf_ann = _load_series(conn, "operating_cash_flow_annual", "operating_cash_flow", company_id)
    capex_ann = _load_series(conn, "capital_expenditures_annual", "capital_expenditures", company_id)
    net_income_ann = _load_series(conn, "net_income_annual", "net_income", company_id)
    net_ppe_ann = _load_series(conn, "net_ppe_annual", "net_ppe", company_id)
    short_term_investments_ann = _load_series(conn, "short_term_investments_annual", "short_term_investments", company_id)
    goodwill_ann = _load_series(conn, "goodwill_and_intangibles_annual", "goodwill_and_intangibles", company_id)
    other_lta_ann = _load_series(conn, "other_long_term_assets_annual", "other_long_term_assets", company_id)
    deferred_revenue_ann = _load_series(conn, "deferred_revenue_annual", "deferred_revenue", company_id)
    deferred_tax_ann = _load_series(conn, "deferred_tax_liabilities_annual", "deferred_tax_liabilities", company_id)
    other_ltl_ann = _load_series(conn, "other_long_term_liabilities_annual", "other_long_term_liabilities", company_id)

    revenue_wc = _merge_ttm_into_annual(conn, revenue_ann, "revenues_ttm", "revenue", company_id)
    cogs_wc = _merge_ttm_into_annual(conn, cogs_ann, "cost_of_revenue_ttm", "cost_of_revenue", company_id)
    ar_wc = _merge_ttm_into_annual(conn, accounts_receivable_ann, "accounts_receivable_ttm", "accounts_receivable", company_id)
    inventory_wc = _merge_ttm_into_annual(conn, inventory_ann, "inventory_ttm", "inventory", company_id)
    ap_wc = _merge_ttm_into_annual(conn, accounts_payable_ann, "accounts_payable_ttm", "accounts_payable", company_id)
    current_assets_wc = _merge_ttm_into_annual(conn, total_current_assets_ann, "total_current_assets_ttm", "total_current_assets", company_id)
    cash_wc = _merge_ttm_into_annual(conn, cash_ann, "cash_and_cash_equivalents_ttm", "cash_and_cash_equivalents", company_id)
    sti_wc = _merge_ttm_into_annual(conn, short_term_investments_ann, "short_term_investments_ttm", "short_term_investments", company_id)
    current_liabilities_wc = _merge_ttm_into_annual(conn, total_current_liabilities_ann, "total_current_liabilities_ttm", "total_current_liabilities", company_id)
    current_debt_wc = _merge_ttm_into_annual(conn, current_debt_ann, "current_debt_ttm", "current_debt", company_id)
    cash_equiv_wc = _build_cash_and_equivalents_series(cash_wc, sti_wc)

    revenue_cf = _merge_ttm_into_annual(conn, revenue_ann, "revenues_ttm", "revenue", company_id)
    ocf_cf = _merge_ttm_into_annual(conn, ocf_ann, "operating_cash_flow_ttm", "operating_cash_flow", company_id)
    capex_cf = _merge_ttm_into_annual(conn, capex_ann, "capital_expenditures_ttm", "capital_expenditures", company_id)
    net_income_cf = _merge_ttm_into_annual(conn, net_income_ann, "net_income_ttm", "net_income", company_id)
    net_ppe_cf = _merge_ttm_into_annual(conn, net_ppe_ann, "net_ppe_ttm", "net_ppe", company_id)
    current_assets_cf = _merge_ttm_into_annual(conn, total_current_assets_ann, "total_current_assets_ttm", "total_current_assets", company_id)
    current_liabilities_cf = _merge_ttm_into_annual(conn, total_current_liabilities_ann, "total_current_liabilities_ttm", "total_current_liabilities", company_id)
    current_debt_cf = _merge_ttm_into_annual(conn, current_debt_ann, "current_debt_ttm", "current_debt", company_id)
    cash_cf = _merge_ttm_into_annual(conn, cash_ann, "cash_and_cash_equivalents_ttm", "cash_and_cash_equivalents", company_id)
    sti_cf = _merge_ttm_into_annual(conn, short_term_investments_ann, "short_term_investments_ttm", "short_term_investments", company_id)
    goodwill_cf = _merge_ttm_into_annual(conn, goodwill_ann, "goodwill_and_intangibles_ttm", "goodwill_and_intangibles", company_id)
    other_lta_cf = _merge_ttm_into_annual(conn, other_lta_ann, "other_long_term_assets_ttm", "other_long_term_assets", company_id)
    deferred_revenue_cf = _merge_ttm_into_annual(conn, deferred_revenue_ann, "deferred_revenue_ttm", "deferred_revenue", company_id)
    deferred_tax_cf = _merge_ttm_into_annual(conn, deferred_tax_ann, "deferred_tax_liabilities_ttm", "deferred_tax_liabilities", company_id)
    other_ltl_cf = _merge_ttm_into_annual(conn, other_ltl_ann, "other_long_term_liabilities_ttm", "other_long_term_liabilities", company_id)
    cash_ex_sti_cf = _build_cash_and_equivalents_series(cash_cf, sti_cf)

    available_years = sorted(
        set(revenue_ann.keys()) | set(cogs_ann.keys()) | set(sga_ann.keys()) | set(operating_income_ann.keys())
        | set(total_debt_ann.keys()) | set(cash_ann.keys()) | set(accounts_receivable_ann.keys()) | set(inventory_ann.keys())
        | set(accounts_payable_ann.keys()) | set(total_current_assets_ann.keys()) | set(total_current_liabilities_ann.keys())
        | set(current_debt_ann.keys()) | set(shareholders_equity_ann.keys()) | set(ebitda_ann.keys()) | set(interest_expense_ann.keys())
        | set(revenue_wc.keys()) | set(cogs_wc.keys()) | set(ar_wc.keys()) | set(inventory_wc.keys()) | set(ap_wc.keys())
        | set(current_assets_wc.keys()) | set(cash_equiv_wc.keys()) | set(current_liabilities_wc.keys()) | set(current_debt_wc.keys())
        | set(revenue_cf.keys()) | set(ocf_cf.keys()) | set(capex_cf.keys()) | set(net_income_cf.keys()) | set(net_ppe_cf.keys())
        | set(current_assets_cf.keys()) | set(current_liabilities_cf.keys()) | set(current_debt_cf.keys()) | set(cash_ex_sti_cf.keys())
        | set(sti_cf.keys()) | set(goodwill_cf.keys()) | set(other_lta_cf.keys()) | set(deferred_revenue_cf.keys())
        | set(deferred_tax_cf.keys()) | set(other_ltl_cf.keys())
    )
    if not available_years:
        return None

    yr_start, yr_end = _safe_year_bounds(year_range, available_years)
    years = [year for year in available_years if yr_end <= year <= yr_start]
    if not years:
        return None

    op_margin_vals: List[float] = []
    gross_margin_vals: List[float] = []
    sga_ratio_vals: List[float] = []
    incremental_vals: List[float] = []
    for year in years:
        revenue = revenue_ann.get(year)
        op_income = operating_income_ann.get(year)
        if revenue not in (None, 0) and op_income is not None:
            op_margin_vals.append(float(op_income) / float(revenue))
        cogs = cogs_ann.get(year)
        if revenue not in (None, 0) and cogs is not None:
            gross_margin_vals.append((float(revenue) - float(cogs)) / float(revenue))
        sga = sga_ann.get(year)
        if revenue not in (None, 0) and sga is not None:
            sga_ratio_vals.append(float(sga) / float(revenue))
        prev_revenue = revenue_ann.get(year - 1)
        prev_op_income = operating_income_ann.get(year - 1)
        if revenue is not None and op_income is not None and prev_revenue is not None and prev_op_income is not None:
            denominator = float(revenue) - float(prev_revenue)
            if denominator != 0:
                incremental_vals.append((float(op_income) - float(prev_op_income)) / denominator)

    income_score = _score_income_statement(
        _median(op_margin_vals),
        _stdev_sample(op_margin_vals),
        _median(gross_margin_vals),
        _median(sga_ratio_vals),
        _median(incremental_vals),
        ttc_context["income_params"],
    )

    net_debt_ebitda_vals: List[float] = []
    interest_coverage_vals: List[float] = []
    quick_ratio_vals: List[float] = []
    current_ratio_vals: List[float] = []
    debt_to_cap_vals: List[float] = []
    debt_maturity_vals: List[float] = []
    nd_penalty = False
    for year in years:
        total_debt = total_debt_ann.get(year)
        if total_debt is not None:
            cash = cash_ann.get(year, 0.0)
            ebitda = ebitda_ann.get(year)
            net_debt = float(total_debt) - float(cash)
            if ebitda is not None and float(ebitda) <= 0 and net_debt > 0:
                nd_penalty = True
            if ebitda not in (None, 0):
                net_debt_ebitda_vals.append(net_debt / float(ebitda))
            equity = shareholders_equity_ann.get(year)
            if equity is not None:
                denominator = float(total_debt) + float(equity)
                if denominator != 0:
                    debt_to_cap_vals.append(float(total_debt) / denominator)
            current_debt = current_debt_ann.get(year)
            if current_debt is not None:
                debt_maturity_vals.append(0.0 if float(total_debt) == 0 else float(current_debt) / float(total_debt))
        op_income = operating_income_ann.get(year)
        interest_expense = interest_expense_ann.get(year)
        if op_income is not None and interest_expense is not None:
            if float(interest_expense) != 0:
                interest_coverage_vals.append(float(op_income) / float(interest_expense))
            else:
                interest_coverage_vals.append(100.0 if float(op_income) > 0 else 0.0)
        current_assets = total_current_assets_ann.get(year)
        current_liabilities = total_current_liabilities_ann.get(year)
        if current_assets is not None and current_liabilities not in (None, 0):
            current_ratio_vals.append(float(current_assets) / float(current_liabilities))
            quick_ratio_vals.append((float(cash_ann.get(year, 0.0)) + float(accounts_receivable_ann.get(year, 0.0))) / float(current_liabilities))

    balance_score = _score_balance_sheet(
        _median(net_debt_ebitda_vals),
        _median(interest_coverage_vals),
        _median(quick_ratio_vals),
        _median(current_ratio_vals),
        _median(debt_to_cap_vals),
        _median(debt_maturity_vals),
        nd_penalty,
        ttc_context["balance_params"],
    )

    ar_days_vals: List[float] = []
    dio_vals: List[float] = []
    dpo_vals: List[float] = []
    ccc_vals: List[float] = []
    nwc_pct_vals: List[float] = []
    wc_turnover_vals: List[float] = []
    ar_days_by_year: Dict[int, float] = {}
    dio_by_year: Dict[int, float] = {}
    dpo_by_year: Dict[int, float] = {}
    nwc_by_year: Dict[int, float] = {}

    for year in years:
        revenue = revenue_wc.get(year)
        cogs = cogs_wc.get(year)
        if revenue not in (None, 0) and year in ar_wc and (year - 1) in ar_wc:
            ar_days = 365.0 * (((float(ar_wc[year]) + float(ar_wc[year - 1])) / 2.0) / float(revenue))
            ar_days_by_year[year] = ar_days
            ar_days_vals.append(ar_days)
        if cogs not in (None, 0) and year in inventory_wc and (year - 1) in inventory_wc:
            dio = 365.0 * (((float(inventory_wc[year]) + float(inventory_wc[year - 1])) / 2.0) / float(cogs))
            dio_by_year[year] = dio
            dio_vals.append(dio)
        if cogs not in (None, 0) and year in ap_wc and (year - 1) in ap_wc:
            dpo = 365.0 * (((float(ap_wc[year]) + float(ap_wc[year - 1])) / 2.0) / float(cogs))
            dpo_by_year[year] = dpo
            dpo_vals.append(dpo)
        current_assets = current_assets_wc.get(year)
        cash_equiv = cash_equiv_wc.get(year)
        current_liabilities = current_liabilities_wc.get(year)
        current_debt = current_debt_wc.get(year)
        if current_assets is not None and cash_equiv is not None and current_liabilities is not None and current_debt is not None:
            nwc = (float(current_assets) - float(cash_equiv)) - (float(current_liabilities) - float(current_debt))
            nwc_by_year[year] = nwc
            if revenue not in (None, 0):
                nwc_pct_vals.append(nwc / float(revenue))
        if revenue not in (None, 0) and year in nwc_by_year and (year - 1) in nwc_by_year:
            avg_nwc = (float(nwc_by_year[year]) + float(nwc_by_year[year - 1])) / 2.0
            if avg_nwc != 0:
                wc_turnover_vals.append(float(revenue) / avg_nwc)

    for year in years:
        if year in ar_days_by_year and year in dio_by_year and year in dpo_by_year:
            ccc_vals.append(ar_days_by_year[year] + dio_by_year[year] - dpo_by_year[year])

    working_capital_score = _score_working_capital(
        _median(ar_days_vals),
        _median(dio_vals),
        _median(dpo_vals),
        _median(ccc_vals),
        _median(nwc_pct_vals),
        _median(wc_turnover_vals),
        _stdev_sample(ccc_vals),
        ttc_context["working_capital_params"],
    )

    ocf_margin_vals: List[float] = []
    fcf_margin_vals: List[float] = []
    cfo_net_income_vals: List[float] = []
    croic_vals: List[float] = []
    capital_intensity_vals: List[float] = []
    for year in years:
        revenue = revenue_cf.get(year)
        ocf = ocf_cf.get(year)
        capex = capex_cf.get(year)
        net_income = net_income_cf.get(year)
        free_cash_flow = float(ocf) + float(capex) if ocf is not None and capex is not None else None
        if revenue not in (None, 0) and ocf is not None:
            ocf_margin_vals.append(float(ocf) / float(revenue))
        if revenue not in (None, 0) and free_cash_flow is not None:
            fcf_margin_vals.append(float(free_cash_flow) / float(revenue))
        if ocf is not None and net_income not in (None, 0):
            cfo_net_income_vals.append(float(ocf) / float(net_income))
        if revenue not in (None, 0) and capex is not None:
            capital_intensity_vals.append((-1.0 * float(capex)) / float(revenue))

        net_ppe = net_ppe_cf.get(year)
        current_assets = current_assets_cf.get(year)
        current_liabilities = current_liabilities_cf.get(year)
        current_debt = current_debt_cf.get(year)
        cash_ex_sti = cash_ex_sti_cf.get(year)
        goodwill = goodwill_cf.get(year)
        other_lta = other_lta_cf.get(year)
        deferred_revenue = deferred_revenue_cf.get(year)
        deferred_tax = deferred_tax_cf.get(year, 0.0)
        other_ltl = other_ltl_cf.get(year)
        if None not in (free_cash_flow, net_ppe, current_assets, current_liabilities, current_debt, cash_ex_sti, goodwill, other_lta, deferred_revenue, other_ltl):
            current_operating_assets = float(current_assets) - float(cash_ex_sti) - float(sti_cf.get(year, 0.0))
            current_operating_liabilities = float(current_liabilities) - float(current_debt)
            invested_capital = (
                float(net_ppe)
                + (current_operating_assets - current_operating_liabilities)
                + float(goodwill)
                + float(other_lta)
                - (float(deferred_revenue) + float(deferred_tax) + float(other_ltl))
            )
            if invested_capital != 0:
                croic_vals.append(float(free_cash_flow) / invested_capital)

    cash_flow_score = _score_cash_flow(
        _median(ocf_margin_vals),
        _median(fcf_margin_vals),
        _median(cfo_net_income_vals),
        _median(croic_vals),
        _median(capital_intensity_vals),
        _stdev_sample(fcf_margin_vals),
        ttc_context["cash_flow_params"],
    )

    if None in (income_score, balance_score, working_capital_score, cash_flow_score):
        return None

    weights = ttc_context["overall_weights"]
    return (
        float(weights["income_statement"]) * float(income_score)
        + float(weights["balance_sheet"]) * float(balance_score)
        + float(weights["working_capital"]) * float(working_capital_score)
        + float(weights["cash_flow"]) * float(cash_flow_score)
    )


def _build_year_range_options(conn) -> List[str]:
    years_df = read_df("SELECT MIN(fiscal_year) AS min_year, MAX(fiscal_year) AS max_year FROM revenues_annual", conn)
    if years_df is None or years_df.empty:
        return [DEFAULT_YEAR_RANGE]
    min_year = years_df.iloc[0]["min_year"]
    max_year = years_df.iloc[0]["max_year"]
    if pd.isna(min_year) or pd.isna(max_year):
        return [DEFAULT_YEAR_RANGE]
    min_year = int(min_year)
    max_year = int(max_year)
    start_year = max(min_year, max_year - 10)
    options = [f"Recent - {year}" for year in range(max_year - 1, start_year - 1, -1)]
    if DEFAULT_YEAR_RANGE not in options:
        options.insert(0, DEFAULT_YEAR_RANGE)
    return options


def _resolve_field_ids(field_ids: Optional[Sequence[str]]) -> List[str]:
    optional_ids = list(field_ids or DEFAULT_FIELD_IDS)
    optional_ids = [field_id for field_id in optional_ids if field_id in SEARCH_RESULT_FIELDS and field_id not in MANDATORY_FIELD_IDS]
    return [*MANDATORY_FIELD_IDS, *optional_ids[:12]]


def _collect_metric_years(conn, company_id: int) -> List[int]:
    years: set[int] = set()
    for df in (
        get_annual_series(conn, company_id),
        get_annual_wacc_series(conn, company_id),
        get_annual_roic_direct_upload_series(conn, company_id),
        get_annual_roic_wacc_spread_series(conn, company_id),
        get_annual_fcfe_series(conn, company_id),
    ):
        if df is not None and not df.empty and "year" in df.columns:
            years.update(int(year) for year in df["year"].dropna().tolist())
    return sorted(years)


def _safe_year_bounds(year_range: str, available_years: Sequence[int]) -> Tuple[int, int]:
    years = sorted(int(year) for year in available_years)
    if not years:
        return 0, 0
    try:
        start_year, end_year = _parse_year_range(year_range, years)
    except Exception:
        start_year, end_year = max(years), min(years)
    return (start_year, end_year) if start_year >= end_year else (end_year, start_year)


def _latest_wacc_decimal_in_range(df: pd.DataFrame, yr_start: int, yr_end: int) -> Optional[float]:
    if df is None or df.empty:
        return None
    dff = df[(df["year"] >= yr_end) & (df["year"] <= yr_start)].sort_values("year")
    if dff.empty:
        dff = df.sort_values("year")
    if dff.empty or pd.isna(dff.iloc[-1]["wacc"]):
        return None
    return float(dff.iloc[-1]["wacc"]) / 100.0


def _normalized_ratio_from_df(df: pd.DataFrame, value_col: str, year_range: str) -> Optional[float]:
    if df is None or df.empty or value_col not in df.columns:
        return None
    yr_start, yr_end = _safe_year_bounds(year_range, [int(year) for year in df["year"].dropna().tolist()])
    median_value, _ = _compute_level_stats(df, yr_start, yr_end, value_col=value_col, stdev_sample=True)
    return _normalize_ratio_value(median_value)


def _normalize_ratio_value(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return float(value) / 100.0 if abs(float(value)) > 1.5 else float(value)


def _decimal_from_percent_metric(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return float(value) / 100.0


def _build_column_config(columns: Sequence[str]) -> Dict[str, object]:
    config: Dict[str, object] = {}
    for field in SEARCH_RESULT_FIELDS.values():
        if field.label not in columns:
            continue
        if field.kind == "text":
            config[field.label] = st.column_config.TextColumn(field.label, help=field.help_text)
        elif field.kind == "decimal4":
            config[field.label] = st.column_config.NumberColumn(field.label, format="%.4f", help=field.help_text)
        else:
            config[field.label] = st.column_config.NumberColumn(field.label, format="%.2f", help=field.help_text)
    return config


def _render_result_controls(result_df: pd.DataFrame) -> pd.DataFrame:
    with st.expander("Filter and Sort Results", expanded=False):
        st.caption("Percentage filters use whole-number inputs, e.g. `15` means 15%.")
        sort_specs = _render_sort_controls(result_df.columns)
        filter_state = _render_filter_controls(result_df)

    filtered_df = _apply_result_filters(result_df, filter_state)
    return _apply_sort_specs(filtered_df, sort_specs).reset_index(drop=True)


def _render_sort_controls(columns: Sequence[str]) -> List[Tuple[str, bool]]:
    st.markdown("**Sort order**")
    field_options = ["None", *columns]
    default_labels = _default_sort_labels(columns)
    sort_specs: List[Tuple[str, bool]] = []
    level_cols = st.columns(3)
    for idx, level in enumerate(("Primary", "Secondary", "Tertiary")):
        default_label = default_labels[idx] if idx < len(default_labels) else "None"
        with level_cols[idx]:
            selected_label = st.selectbox(
                f"{level} sort",
                options=field_options,
                index=field_options.index(default_label) if default_label in field_options else 0,
                key=f"search_aggregate_sort_field_{idx}",
            )
            direction = st.selectbox(
                f"{level} direction",
                options=["Descending", "Ascending"],
                index=0,
                key=f"search_aggregate_sort_direction_{idx}",
            )
        if selected_label != "None":
            sort_specs.append((selected_label, direction == "Ascending"))
    return _dedupe_sort_specs(sort_specs)


def _render_filter_controls(result_df: pd.DataFrame) -> Dict[str, Dict[str, str]]:
    st.markdown("**Field filters**")
    text_fields = [column for column in result_df.columns if (_search_field_by_label(column) or SearchField("", "text", "")).kind == "text"]
    numeric_fields = [column for column in result_df.columns if column not in text_fields]
    filter_state: Dict[str, Dict[str, str]] = {}

    if text_fields:
        st.markdown("Text fields")
        cols = st.columns(3)
        for idx, column in enumerate(text_fields):
            field_id = _search_field_id_by_label(column) or f"text_{idx}"
            with cols[idx % 3]:
                filter_state[column] = {
                    "contains": st.text_input(
                        column,
                        key=f"search_aggregate_filter_text_{field_id}",
                        placeholder="Contains...",
                    ).strip()
                }

    if numeric_fields:
        st.markdown("Numeric fields")
        cols = st.columns(3)
        for idx, column in enumerate(numeric_fields):
            field_id = _search_field_id_by_label(column) or f"numeric_{idx}"
            is_percent = field_id in PERCENT_FIELD_IDS
            with cols[idx % 3]:
                st.markdown(column)
                min_col, max_col = st.columns(2)
                with min_col:
                    min_value = st.text_input(
                        f"{column} min",
                        key=f"search_aggregate_filter_min_{field_id}",
                        placeholder="Min %" if is_percent else "Min",
                        label_visibility="collapsed",
                    ).strip()
                with max_col:
                    max_value = st.text_input(
                        f"{column} max",
                        key=f"search_aggregate_filter_max_{field_id}",
                        placeholder="Max %" if is_percent else "Max",
                        label_visibility="collapsed",
                    ).strip()
                filter_state[column] = {"min": min_value, "max": max_value}
    return filter_state


def _apply_result_filters(result_df: pd.DataFrame, filter_state: Dict[str, Dict[str, str]]) -> pd.DataFrame:
    filtered_df = result_df.copy()
    invalid_messages: List[str] = []
    for column, state in filter_state.items():
        field = _search_field_by_label(column)
        if field is None:
            continue
        if field.kind == "text":
            contains = state.get("contains", "").strip()
            if contains:
                filtered_df = filtered_df[filtered_df[column].fillna("").astype(str).str.contains(contains, case=False, na=False)]
            continue

        min_value = _parse_filter_number(state.get("min", ""), column, invalid_messages)
        max_value = _parse_filter_number(state.get("max", ""), column, invalid_messages)
        field_id = _search_field_id_by_label(column)
        scale = 100.0 if field_id in PERCENT_FIELD_IDS else 1.0
        if min_value is not None:
            filtered_df = filtered_df[filtered_df[column].notna() & (filtered_df[column].astype(float) >= (min_value / scale))]
        if max_value is not None:
            filtered_df = filtered_df[filtered_df[column].notna() & (filtered_df[column].astype(float) <= (max_value / scale))]

    if invalid_messages:
        st.warning("Ignored invalid numeric filters for: " + ", ".join(sorted(set(invalid_messages))))
    return filtered_df


def _parse_filter_number(raw_value: str, column: str, invalid_messages: List[str]) -> Optional[float]:
    cleaned = str(raw_value or "").strip().replace("%", "").replace(",", "")
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        invalid_messages.append(column)
        return None


def _apply_default_result_sort(df: pd.DataFrame) -> pd.DataFrame:
    return _apply_sort_specs(df, _default_sort_specs(df.columns))


def _default_sort_specs(columns: Sequence[str]) -> List[Tuple[str, bool]]:
    return [(label, False) for label in _default_sort_labels(columns)]


def _default_sort_labels(columns: Sequence[str]) -> List[str]:
    labels: List[str] = []
    for field_id in DEFAULT_SORT_FIELD_IDS:
        field = SEARCH_RESULT_FIELDS.get(field_id)
        if field and field.label in columns:
            labels.append(field.label)
    return labels


def _dedupe_sort_specs(sort_specs: Sequence[Tuple[str, bool]]) -> List[Tuple[str, bool]]:
    deduped: List[Tuple[str, bool]] = []
    seen: set[str] = set()
    for label, ascending in sort_specs:
        if label in seen:
            continue
        seen.add(label)
        deduped.append((label, ascending))
    return deduped


def _apply_sort_specs(df: pd.DataFrame, sort_specs: Sequence[Tuple[str, bool]]) -> pd.DataFrame:
    valid_specs = [(label, ascending) for label, ascending in sort_specs if label in df.columns]
    if not valid_specs or df.empty:
        return df
    sort_columns = [label for label, _ in valid_specs]
    ascending = [direction for _, direction in valid_specs]
    return df.sort_values(by=sort_columns, ascending=ascending, na_position="last")


def _render_glass_results_table(result_df: pd.DataFrame) -> None:
    st.markdown(_build_glass_results_table_html(result_df), unsafe_allow_html=True)


def _build_glass_results_table_html(result_df: pd.DataFrame) -> str:
    header_html = "".join(f"<th>{escape(str(column))}</th>" for column in result_df.columns)
    body_rows: List[str] = []
    for _, row in result_df.iterrows():
        cells: List[str] = []
        for column in result_df.columns:
            field = _search_field_by_label(column)
            cell_class = "sa-number" if field and field.kind != "text" else "sa-text"
            cells.append(f"<td class='{cell_class}'>{_format_result_cell(column, row[column])}</td>")
        body_rows.append(f"<tr>{''.join(cells)}</tr>")

    return f"""
    <style>
      .sa-glass-shell {{
        width: 100%;
        overflow-x: auto;
        border-radius: 20px;
        padding: 0.7rem;
        background:
          linear-gradient(135deg, rgba(255, 255, 255, 0.68), rgba(255, 255, 255, 0.34)),
          linear-gradient(180deg, rgba(208, 219, 232, 0.28), rgba(255, 255, 255, 0.12));
        border: 1px solid rgba(255, 255, 255, 0.58);
        box-shadow:
          0 22px 44px rgba(15, 23, 42, 0.12),
          inset 0 1px 0 rgba(255, 255, 255, 0.72);
        backdrop-filter: blur(18px);
        -webkit-backdrop-filter: blur(18px);
      }}

      .sa-glass-table {{
        width: 100%;
        min-width: 1100px;
        border-collapse: separate;
        border-spacing: 0;
        font-size: 0.94rem;
        color: #111827;
        overflow: hidden;
        border-radius: 16px;
      }}

      .sa-glass-table thead th {{
        position: sticky;
        top: 0;
        z-index: 1;
        padding: 0.9rem 0.95rem;
        text-align: left;
        font-weight: 700;
        font-size: 0.84rem;
        letter-spacing: 0.01em;
        color: #000000;
        background: linear-gradient(180deg, rgba(255, 255, 255, 0.86), rgba(241, 245, 249, 0.92));
        border-bottom: 1px solid rgba(148, 163, 184, 0.32);
        white-space: nowrap;
      }}

      .sa-glass-table tbody td {{
        padding: 0.82rem 0.95rem;
        color: #111827;
        background: rgba(255, 255, 255, 0.28);
        border-bottom: 1px solid rgba(148, 163, 184, 0.18);
        text-align: left;
        vertical-align: top;
      }}

      .sa-glass-table tbody tr:nth-child(even) td {{
        background: rgba(248, 250, 252, 0.52);
      }}

      .sa-glass-table tbody tr:hover td {{
        background: rgba(226, 232, 240, 0.42);
      }}

      .sa-glass-table tbody tr:last-child td {{
        border-bottom: none;
      }}

      .sa-glass-table .sa-number {{
        text-align: left;
        font-variant-numeric: tabular-nums;
      }}

      .sa-glass-table .sa-text {{
        text-align: left;
      }}
    </style>
    <div class="sa-glass-shell">
      <table class="sa-glass-table">
        <thead>
          <tr>{header_html}</tr>
        </thead>
        <tbody>
          {''.join(body_rows)}
        </tbody>
      </table>
    </div>
    """


def _search_field_by_label(label: str) -> Optional[SearchField]:
    for field in SEARCH_RESULT_FIELDS.values():
        if field.label == label:
            return field
    return None


def _format_result_cell(column: str, value: object) -> str:
    if pd.isna(value):
        return "&mdash;"

    field = _search_field_by_label(column)
    if field is None or field.kind == "text":
        return escape(str(value))

    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return escape(str(value))

    field_id = _search_field_id_by_label(column)
    if field_id in PERCENT_FIELD_IDS:
        return f"{numeric_value * 100.0:.4f}%"

    if field.kind == "decimal4":
        return f"{numeric_value:.4f}"
    return f"{numeric_value:.2f}"


def _search_field_id_by_label(label: str) -> Optional[str]:
    for field_id, field in SEARCH_RESULT_FIELDS.items():
        if field.label == label:
            return field_id
    return None
