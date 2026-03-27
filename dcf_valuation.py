from datetime import datetime
import csv
import io
import json
from typing import Dict, List, Optional, Tuple
import urllib.error
import urllib.request

import numpy as np
import pandas as pd
import streamlit as st

from core import (
    compute_and_store_cost_of_equity,
    compute_and_store_fcff_and_reinvestment_rate,
    compute_and_store_levered_beta,
    compute_and_store_pre_tax_cost_of_debt,
    compute_and_store_roic_wacc_spread,
    compute_and_store_wacc,
    get_db,
    get_dcf_company_valuation_settings,
    get_dcf_industry_valuation_settings,
    get_dcf_valuation_settings,
    list_companies,
    read_df,
    upsert_dcf_company_valuation_settings,
    upsert_dcf_industry_valuation_settings,
    upsert_dcf_valuation_settings,
)
from search_aggregate import _build_ttc_context, _compute_ttc_overall_score
from ttc_efficiency import (
    _compute_value_creation_filter_metrics,
    _get_company_buckets,
    _load_series,
    _load_weight_maps,
    _merge_ttm_into_annual,
)


_DCF_BUCKET_KEY = "dcf_selected_bucket_name"
_DCF_BUCKET_MULTI_KEY = "dcf_selected_bucket_names"
_DCF_COMPANY_KEY = "dcf_selected_company_ids"
_DCF_INDUSTRY_SETTINGS_SELECT_KEY = "dcf_industry_settings_select"
_DCF_COMPANY_SETTINGS_SELECT_KEY = "dcf_company_settings_select"
_DCF_RUN_CONFIG_VISIBLE_KEY = "dcf_industry_run_config_visible"
_DCF_RESULTS_KEY = "dcf_industry_results"
_DCF_RESULTS_META_KEY = "dcf_industry_results_meta"
_DCF_COMPANY_RUN_CONFIG_VISIBLE_KEY = "dcf_company_run_config_visible"
_DCF_COMPANY_RESULTS_KEY = "dcf_company_results"
_DCF_COMPANY_RESULTS_META_KEY = "dcf_company_results_meta"
_DCF_COMPANY_RESULT_SELECT_KEY = "dcf_company_result_selected_id"
_DCF_COMPANY_EXPANDED_RESULT_KEY = "dcf_company_expanded_result_id"

_SETTINGS_FIELDS = [
    "historical_years",
    "terminal_growth_usa",
    "terminal_growth_india",
    "terminal_growth_china",
    "terminal_growth_japan",
    "future_revenue_growth",
    "starting_projected_revenue_growth_cap",
    "ebidta_margin_growth",
    "da_percent_growth",
    "capex_percent_growth",
    "working_capital_days_growth",
    "wacc_direction",
]

_NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}


def _pct_to_decimal(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    return numeric / 100.0 if abs(numeric) > 1.0 else numeric


def _median(values: List[float]) -> Optional[float]:
    numeric_values = [float(v) for v in values if v is not None and pd.notna(v)]
    if not numeric_values:
        return None
    return float(np.median(numeric_values))


def _latest_n_values(series_map: Dict[int, float], n: int) -> List[Tuple[int, float]]:
    rows: List[Tuple[int, float]] = []
    for year, value in sorted(series_map.items()):
        if value is None or pd.isna(value):
            continue
        try:
            rows.append((int(year), float(value)))
        except Exception:
            continue
    return rows[-max(int(n), 1):]


def _latest_n_growths(series_map: Dict[int, float], n: int) -> List[float]:
    observations = _latest_n_values(series_map, max(int(n) + 1, 2))
    growths: List[Tuple[int, float]] = []
    for idx in range(1, len(observations)):
        prev_year, prev_value = observations[idx - 1]
        year, value = observations[idx]
        if year - prev_year != 1:
            continue
        if prev_value == 0:
            continue
        growths.append((year, (float(value) / float(prev_value)) - 1.0))
    return [growth for _, growth in growths[-max(int(n), 1):]]


def _latest_numeric_value(series_map: Dict[int, float]) -> Tuple[Optional[int], Optional[float]]:
    rows = _latest_n_values(series_map, 1)
    if not rows:
        return None, None
    return rows[-1]


def _load_latest_ttm_scalar(conn, table: str, value_col: str, company_id: int) -> Tuple[Optional[str], Optional[float]]:
    df = read_df(
        f"SELECT as_of, {value_col} AS value FROM {table} WHERE company_id = ? LIMIT 1",
        conn,
        params=(company_id,),
    )
    if df is None or df.empty:
        return None, None

    row = df.iloc[0]
    value = row.get("value")
    if pd.isna(value):
        return row.get("as_of"), None

    try:
        return row.get("as_of"), float(value)
    except Exception:
        return row.get("as_of"), None


def _normalize_quote_as_of(as_of: object) -> Optional[str]:
    if as_of is None or pd.isna(as_of):
        return None
    text = str(as_of).strip()
    if not text:
        return None
    try:
        return pd.to_datetime(text, errors="coerce").strftime("%Y-%m-%d")
    except Exception:
        return text


def _latest_annual_series_value(series_map: Dict[int, float]) -> Tuple[Optional[int], Optional[float]]:
    latest_year, latest_value = _latest_numeric_value(series_map)
    if latest_year is None or latest_value is None:
        return None, None
    try:
        return int(latest_year), float(latest_value)
    except Exception:
        return None, None


def _actual_year_label(year: int) -> str:
    return str(int(year))


def _projected_year_label(year: int) -> str:
    return f"FY{int(year)}"


def _safe_float(value: object) -> Optional[float]:
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _compute_series_ratio(numerator: object, denominator: object) -> Optional[float]:
    num = _safe_float(numerator)
    den = _safe_float(denominator)
    if num is None or den in (None, 0):
        return None
    return float(num) / float(den)


def _compute_growth_for_year(series_map: Dict[int, float], year: int) -> Optional[float]:
    current_value = _safe_float(series_map.get(int(year)))
    previous_value = _safe_float(series_map.get(int(year) - 1))
    if current_value is None or previous_value in (None, 0):
        return None
    return (float(current_value) / float(previous_value)) - 1.0


def _build_breakdown_table(columns: List[str], rows: List[Tuple[str, str, List[object]]]) -> Dict[str, object]:
    return {
        "columns": [str(column) for column in columns],
        "rows": [
            {"metric": metric, "format": value_format, "values": list(values)}
            for metric, value_format, values in rows
        ],
    }


def _format_breakdown_value(value: object, value_format: str) -> str:
    if value is None or pd.isna(value):
        return ""
    if value_format == "text":
        return str(value)

    numeric = _safe_float(value)
    if numeric is None:
        return str(value)

    if value_format == "percent":
        return f"{numeric * 100:,.2f}%"
    if value_format == "decimal4":
        return f"{numeric:,.4f}"
    if value_format == "integer":
        return f"{int(round(numeric)):,}"
    return f"{numeric:,.2f}"


def _render_breakdown_table(title: str, table_payload: Optional[Dict[str, object]]) -> None:
    if not table_payload:
        return

    columns = [str(column) for column in table_payload.get("columns", [])]
    rows = table_payload.get("rows", [])
    if not columns or not rows:
        return

    rendered_rows: List[Dict[str, str]] = []
    for row in rows:
        record = {"Metric": str(row.get("metric", ""))}
        values = list(row.get("values", []))
        value_format = str(row.get("format", "number"))
        for idx, column in enumerate(columns):
            record[column] = _format_breakdown_value(values[idx] if idx < len(values) else None, value_format)
        rendered_rows.append(record)

    st.markdown(f"**{title}**")
    st.dataframe(pd.DataFrame(rendered_rows), use_container_width=True, hide_index=True)


def _render_company_valuation_detail(detail_payload: Dict[str, object]) -> None:
    summary_rows = detail_payload.get("summary_rows") or []
    assumptions_rows = detail_payload.get("assumptions_rows") or []

    summary_df = pd.DataFrame(
        [
            {
                "Metric": row.get("metric", ""),
                "Value": _format_breakdown_value(row.get("value"), str(row.get("format", "text"))),
            }
            for row in summary_rows
        ]
    )
    assumptions_df = pd.DataFrame(
        [
            {
                "Input": row.get("metric", ""),
                "Value": _format_breakdown_value(row.get("value"), str(row.get("format", "text"))),
            }
            for row in assumptions_rows
        ]
    )

    summary_col, assumption_col = st.columns([1, 1])
    with summary_col:
        st.markdown("**Summary**")
        if not summary_df.empty:
            st.dataframe(summary_df, use_container_width=True, hide_index=True)
    with assumption_col:
        st.markdown("**Assumptions**")
        if not assumptions_df.empty:
            st.dataframe(assumptions_df, use_container_width=True, hide_index=True)

    _render_breakdown_table("Operating Model", detail_payload.get("operating_table"))
    _render_breakdown_table("Working Capital Bridge", detail_payload.get("working_capital_table"))
    _render_breakdown_table("Discounting and Equity Value", detail_payload.get("discounting_table"))


def _summarize_price_source(results_df: pd.DataFrame) -> Dict[str, int]:
    counts = {"Live": 0, "DB fallback": 0, "Unavailable": 0}
    if results_df is None or results_df.empty or "Price Source" not in results_df.columns:
        return counts
    for value in results_df["Price Source"].fillna("Unavailable").astype(str):
        counts[value] = counts.get(value, 0) + 1
    return counts


def _style_price_source(value: object) -> str:
    palette = {
        "Live": "color: #166534; background-color: #DCFCE7; font-weight: 600;",
        "DB fallback": "color: #92400E; background-color: #FEF3C7; font-weight: 600;",
        "Unavailable": "color: #374151; background-color: #F3F4F6; font-weight: 600;",
    }
    return palette.get(str(value), "")


def _normalize_country_for_quotes(country: Optional[str]) -> str:
    return str(country or "").strip().lower()


def _build_quote_symbol(ticker: str, country: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    clean_ticker = str(ticker or "").strip().upper()
    country_key = _normalize_country_for_quotes(country)
    if not clean_ticker:
        return None, "Ticker is blank."
    if country_key in {"india", "in"}:
        return clean_ticker, None
    if country_key in {"usa", "us", "united states", "united states of america"}:
        return f"{clean_ticker}.US", None
    return None, f"Live quote provider is not configured for country '{country or 'Unknown'}'."


def _build_nse_opener():
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor())
    opener.addheaders = list(_NSE_HEADERS.items())
    opener.open("https://www.nseindia.com/", timeout=15).read()
    return opener


def _fetch_nse_quote(symbol: str, opener=None) -> Dict[str, object]:
    try:
        local_opener = opener or _build_nse_opener()
        with local_opener.open(f"https://www.nseindia.com/api/quote-equity?symbol={symbol}", timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return {"price": None, "as_of": None, "source": "Unavailable", "detail": f"NSE quote request failed: HTTP {exc.code}."}
    except Exception as exc:
        return {"price": None, "as_of": None, "source": "Unavailable", "detail": f"NSE quote request failed: {type(exc).__name__}."}

    price = payload.get("priceInfo", {}).get("lastPrice")
    if price is None or pd.isna(price):
        return {"price": None, "as_of": None, "source": "Unavailable", "detail": "NSE quote did not include a live last price."}

    as_of = payload.get("metadata", {}).get("lastUpdateTime") or "Fetched now"
    return {"price": float(price), "as_of": str(as_of), "source": "Live", "detail": f"Live quote fetched from NSE for {symbol}."}


def _fetch_stooq_quote(symbol: str) -> Dict[str, object]:
    try:
        with urllib.request.urlopen(f"https://stooq.com/q/l/?s={symbol.lower()}&i=d", timeout=20) as response:
            raw = response.read().decode("utf-8").strip()
    except urllib.error.HTTPError as exc:
        return {"price": None, "as_of": None, "source": "Unavailable", "detail": f"Stooq quote request failed: HTTP {exc.code}."}
    except Exception as exc:
        return {"price": None, "as_of": None, "source": "Unavailable", "detail": f"Stooq quote request failed: {type(exc).__name__}."}

    if not raw:
        return {"price": None, "as_of": None, "source": "Unavailable", "detail": "Stooq quote response was empty."}

    reader = csv.reader(io.StringIO(raw))
    row = next(reader, [])
    if len(row) < 7 or row[0].endswith("N/D"):
        return {"price": None, "as_of": None, "source": "Unavailable", "detail": "Stooq quote was unavailable for this symbol."}

    try:
        price = float(row[6])
    except Exception:
        return {"price": None, "as_of": None, "source": "Unavailable", "detail": "Stooq returned a non-numeric close price."}

    as_of = None
    try:
        as_of = pd.to_datetime(f"{row[1]} {row[2]}", format="%Y%m%d %H%M%S", errors="coerce")
        as_of = None if pd.isna(as_of) else as_of.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        as_of = None
    return {"price": price, "as_of": as_of or "Fetched now", "source": "Live", "detail": f"Live quote fetched from Stooq for {symbol}."}


def _fetch_live_quote_for_company(company_row: pd.Series, nse_opener=None) -> Dict[str, object]:
    ticker = str(company_row.get("ticker") or "").strip().upper()
    country = company_row.get("country")
    symbol, symbol_error = _build_quote_symbol(ticker, country)
    if symbol_error:
        return {"price": None, "as_of": None, "source": "Unavailable", "detail": symbol_error}

    country_key = _normalize_country_for_quotes(country)
    if country_key in {"india", "in"}:
        return _fetch_nse_quote(symbol, opener=nse_opener)
    if country_key in {"usa", "us", "united states", "united states of america"}:
        return _fetch_stooq_quote(symbol)
    return {"price": None, "as_of": None, "source": "Unavailable", "detail": f"No live quote provider configured for country '{country or 'Unknown'}'."}


def _fetch_live_quotes_for_companies(members_df: pd.DataFrame) -> Dict[int, Dict[str, object]]:
    live_quotes: Dict[int, Dict[str, object]] = {}
    if members_df is None or members_df.empty:
        return live_quotes

    nse_opener = None
    india_mask = members_df["country"].astype(str).str.strip().str.lower().isin(["india", "in"])
    if india_mask.any():
        try:
            nse_opener = _build_nse_opener()
        except Exception:
            nse_opener = None

    for _, company_row in members_df.iterrows():
        company_id = int(company_row["id"])
        live_quotes[company_id] = _fetch_live_quote_for_company(company_row, nse_opener=nse_opener)
    return live_quotes


def _country_to_terminal_growth(settings: Dict[str, float], country: Optional[str]) -> Optional[float]:
    country_key = str(country or "").strip().lower()
    if country_key in {"india", "in"}:
        return _pct_to_decimal(settings.get("terminal_growth_india"))
    if country_key in {"china", "cn", "prc", "people's republic of china", "peoples republic of china"}:
        return _pct_to_decimal(settings.get("terminal_growth_china"))
    if country_key in {"japan", "jp"}:
        return _pct_to_decimal(settings.get("terminal_growth_japan"))
    return _pct_to_decimal(settings.get("terminal_growth_usa"))


def _build_non_cash_working_capital_series(conn, company_id: int) -> Dict[int, float]:
    annual_ncwc = _load_series(conn, "non_cash_working_capital_annual", "non_cash_working_capital", company_id)

    total_current_assets = _merge_ttm_into_annual(
        conn,
        _load_series(conn, "total_current_assets_annual", "total_current_assets", company_id),
        "total_current_assets_ttm",
        "total_current_assets",
        company_id,
    )
    cash_and_cash_equivalents = _merge_ttm_into_annual(
        conn,
        _load_series(conn, "cash_and_cash_equivalents_annual", "cash_and_cash_equivalents", company_id),
        "cash_and_cash_equivalents_ttm",
        "cash_and_cash_equivalents",
        company_id,
    )
    total_current_liabilities = _merge_ttm_into_annual(
        conn,
        _load_series(conn, "total_current_liabilities_annual", "total_current_liabilities", company_id),
        "total_current_liabilities_ttm",
        "total_current_liabilities",
        company_id,
    )
    current_debt = _merge_ttm_into_annual(
        conn,
        _load_series(conn, "current_debt_annual", "current_debt", company_id),
        "current_debt_ttm",
        "current_debt",
        company_id,
    )

    merged_ncwc = dict(annual_ncwc)
    all_years = (
        set(total_current_assets.keys())
        & set(cash_and_cash_equivalents.keys())
        & set(total_current_liabilities.keys())
        & set(current_debt.keys())
    )
    for year in all_years:
        merged_ncwc[int(year)] = (
            float(total_current_assets[year])
            - float(cash_and_cash_equivalents[year])
            - (float(total_current_liabilities[year]) - float(current_debt[year]))
        )
    return merged_ncwc


def _build_required_series(conn, company_id: int) -> Dict[str, Dict[int, float]]:
    return {
        "revenue": _merge_ttm_into_annual(conn, _load_series(conn, "revenues_annual", "revenue", company_id), "revenues_ttm", "revenue", company_id),
        "ebitda": _merge_ttm_into_annual(conn, _load_series(conn, "ebitda_annual", "ebitda", company_id), "ebitda_ttm", "ebitda", company_id),
        "da": _load_series(conn, "depreciation_amortization_annual", "depreciation_amortization", company_id),
        "capex": _load_series(conn, "capital_expenditures_annual", "capital_expenditures", company_id),
        "ncwc": _build_non_cash_working_capital_series(conn, company_id),
        "wacc": _load_series(conn, "wacc_annual", "wacc", company_id),
        "total_current_assets": _merge_ttm_into_annual(
            conn,
            _load_series(conn, "total_current_assets_annual", "total_current_assets", company_id),
            "total_current_assets_ttm",
            "total_current_assets",
            company_id,
        ),
        "cash": _merge_ttm_into_annual(
            conn,
            _load_series(conn, "cash_and_cash_equivalents_annual", "cash_and_cash_equivalents", company_id),
            "cash_and_cash_equivalents_ttm",
            "cash_and_cash_equivalents",
            company_id,
        ),
        "total_current_liabilities": _merge_ttm_into_annual(
            conn,
            _load_series(conn, "total_current_liabilities_annual", "total_current_liabilities", company_id),
            "total_current_liabilities_ttm",
            "total_current_liabilities",
            company_id,
        ),
        "current_debt": _merge_ttm_into_annual(
            conn,
            _load_series(conn, "current_debt_annual", "current_debt", company_id),
            "current_debt_ttm",
            "current_debt",
            company_id,
        ),
        "debt": _merge_ttm_into_annual(conn, _load_series(conn, "total_debt_annual", "total_debt", company_id), "total_debt_ttm", "total_debt", company_id),
        "shares": _merge_ttm_into_annual(
            conn,
            _load_series(conn, "shares_outstanding_basic_annual", "shares_outstanding_basic", company_id),
            "shares_outstanding_basic_ttm",
            "shares_outstanding_basic",
            company_id,
        ),
        "price": _merge_ttm_into_annual(
            conn,
            _load_series(conn, "last_close_price_annual", "last_close_price", company_id),
            "last_close_price_ttm",
            "last_close_price",
            company_id,
        ),
    }


def _get_effective_tax_rate_decimal(conn, country: Optional[str]) -> Optional[float]:
    country_key = str(country or "").strip().lower()
    country_variants = {
        "usa": ["USA", "US", "United States", "United States of America"],
        "united states": ["USA", "US", "United States", "United States of America"],
        "us": ["USA", "US", "United States", "United States of America"],
        "india": ["India", "IN"],
        "in": ["India", "IN"],
        "china": ["China", "CN", "PRC", "People's Republic of China", "Peoples Republic of China"],
        "cn": ["China", "CN", "PRC", "People's Republic of China", "Peoples Republic of China"],
        "japan": ["Japan", "JP"],
        "jp": ["Japan", "JP"],
    }
    search_keys = country_variants.get(country_key, [country or "USA", "USA", "US", "United States"])
    for key in search_keys:
        df = read_df(
            "SELECT effective_rate FROM marginal_corporate_tax_rates WHERE country = ? LIMIT 1",
            conn,
            params=(key,),
        )
        if df is not None and not df.empty and pd.notna(df.iloc[0]["effective_rate"]):
            return _pct_to_decimal(df.iloc[0]["effective_rate"])
    return None


def _compute_dcf_projection(
    *,
    conn,
    company_row: pd.Series,
    live_quote: Optional[Dict[str, object]],
    settings: Dict[str, float],
    terminal_year: int,
    score_context: Dict[str, object],
    growth_weight_map: Dict[str, float],
    stddev_weight_map: Dict[str, float],
    selected_bucket_map: Dict[int, str],
) -> Dict[str, object]:
    company_id = int(company_row["id"])
    company_name = str(company_row["name"])
    ticker = str(company_row["ticker"])
    country = company_row.get("country")
    series = _build_required_series(conn, company_id)
    revenue_series = series["revenue"]
    latest_actual_year, latest_revenue = _latest_numeric_value(revenue_series)

    detail_payload: Dict[str, object] = {
        "company_id": company_id,
        "company_name": company_name,
        "ticker": ticker,
        "country": country,
        "industry_bucket": selected_bucket_map.get(company_id, "(no bucket)"),
        "validation": None,
        "summary_rows": [],
        "assumptions_rows": [],
        "operating_table": None,
        "working_capital_table": None,
        "discounting_table": None,
    }
    base_row: Dict[str, object] = {
        "__company_id": company_id,
        "__valuation_detail": detail_payload,
        "Company Name": company_name,
        "Ticker": ticker,
        "Industry Bucket": selected_bucket_map.get(company_id, "(no bucket)"),
        "Total Scaled Volatility-Adjusted Score": None,
        "Total Debt-Adjusted Scaled Volatility-Adjusted Score": None,
        "Overall Score (0-400)": None,
        "Current Market Price": None,
        "Price Source": "Unavailable",
        "Quote As Of": None,
        "Price Source Detail": "No live quote or database price available.",
        "Intrinsic Value": None,
        "Difference %": None,
        "Validation": None,
    }

    def _fail(message: str) -> Dict[str, object]:
        base_row["Validation"] = message
        detail_payload["validation"] = message
        return base_row

    if latest_actual_year is None or latest_revenue is None:
        return _fail("Missing metric: Revenue")

    explicit_final_year = int(terminal_year) - 1
    if explicit_final_year <= int(latest_actual_year):
        return _fail(f"Terminal year {terminal_year} must be later than latest actual year {latest_actual_year}.")

    historical_years = int(settings.get("historical_years", 7) or 7)
    revenue_growth_sample = _latest_n_growths(revenue_series, historical_years)
    if not revenue_growth_sample:
        return _fail("Missing metric: Revenue growth history")

    recent_revenue_values = _latest_n_values(revenue_series, historical_years)
    actual_years = [int(year) for year, _ in recent_revenue_values]
    ebitda_margin_sample = [
        float(ebitda) / float(revenue)
        for year, revenue in recent_revenue_values
        for ebitda in [series["ebitda"].get(year)]
        if ebitda is not None and revenue not in (None, 0)
    ]
    if not ebitda_margin_sample:
        return _fail("Missing metric: EBITDA margin history")

    da_pct_sample = [
        float(da) / float(revenue)
        for year, revenue in recent_revenue_values
        for da in [series["da"].get(year)]
        if da is not None and revenue not in (None, 0)
    ]
    if not da_pct_sample:
        return _fail("Missing metric: D&A percent history")

    capex_pct_sample = [
        float(capex) / float(revenue)
        for year, revenue in recent_revenue_values
        for capex in [series["capex"].get(year)]
        if capex is not None and revenue not in (None, 0)
    ]
    if not capex_pct_sample:
        return _fail("Missing metric: CAPEX percent history")

    wc_days_sample = [
        (float(ncwc) * 365.0) / float(revenue)
        for year, revenue in recent_revenue_values
        for ncwc in [series["ncwc"].get(year)]
        if ncwc is not None and revenue not in (None, 0)
    ]
    if not wc_days_sample:
        return _fail("Missing metric: Working capital days history")

    wacc_history = [float(v) for _, v in _latest_n_values({y: v for y, v in series["wacc"].items() if y <= latest_actual_year}, historical_years)]
    if not wacc_history:
        return _fail("Missing metric: WACC")

    tax_rate = _get_effective_tax_rate_decimal(conn, country)
    if tax_rate is None:
        return _fail("Missing metric: Effective marginal corporate tax rate")

    terminal_growth = _country_to_terminal_growth(settings, country)
    if terminal_growth is None:
        return _fail("Missing metric: Terminal growth rate")

    _, latest_cash = _latest_numeric_value(series["cash"])
    _, latest_debt = _latest_numeric_value(series["debt"])
    _, latest_shares = _latest_numeric_value(series["shares"])
    latest_price_year, latest_price = _latest_annual_series_value(series["price"])
    latest_ttm_as_of, latest_ttm_price = _load_latest_ttm_scalar(conn, "last_close_price_ttm", "last_close_price", company_id)

    live_quote_price = None
    live_quote_as_of = None
    live_quote_detail = None
    if live_quote:
        try:
            price_val = live_quote.get("price")
            if price_val is not None and not pd.isna(price_val):
                live_quote_price = float(price_val)
        except Exception:
            live_quote_price = None
        live_quote_as_of = _normalize_quote_as_of(live_quote.get("as_of"))
        live_quote_detail = str(live_quote.get("detail") or "").strip() or None

    current_market_price = live_quote_price
    if current_market_price is not None:
        base_row["Current Market Price"] = float(current_market_price)
        base_row["Price Source"] = "Live"
        base_row["Quote As Of"] = live_quote_as_of or "Fetched now"
        base_row["Price Source Detail"] = live_quote_detail or "Live quote fetched during this run."
    else:
        current_market_price = latest_ttm_price if latest_ttm_price is not None else latest_price
        if current_market_price is not None:
            base_row["Current Market Price"] = float(current_market_price)
            base_row["Price Source"] = "DB fallback"
            if latest_ttm_price is not None:
                quote_as_of = _normalize_quote_as_of(latest_ttm_as_of) or "Stored TTM"
                base_row["Quote As Of"] = quote_as_of
                base_row["Price Source Detail"] = live_quote_detail or f"Using stored TTM price from database ({quote_as_of})."
            elif latest_price_year is not None:
                base_row["Quote As Of"] = str(latest_price_year)
                base_row["Price Source Detail"] = live_quote_detail or f"Using latest annual stored price from database ({latest_price_year})."
            else:
                base_row["Quote As Of"] = "Stored DB value"
                base_row["Price Source Detail"] = live_quote_detail or "Using stored market price from database."
        elif live_quote_detail:
            base_row["Price Source Detail"] = live_quote_detail

    revenue_growth = _median(revenue_growth_sample)
    ebitda_margin = _median(ebitda_margin_sample)
    da_pct = _median(da_pct_sample)
    capex_pct = _median(capex_pct_sample)
    working_capital_days = _median(wc_days_sample)
    wacc = _pct_to_decimal(_median(wacc_history))

    if None in (revenue_growth, ebitda_margin, da_pct, capex_pct, working_capital_days, wacc):
        return _fail("Missing metric: Required historical DCF inputs")

    revenue_growth_step = _pct_to_decimal(settings.get("future_revenue_growth")) or 0.0
    starting_revenue_growth_cap = _pct_to_decimal(settings.get("starting_projected_revenue_growth_cap"))
    ebitda_margin_step = _pct_to_decimal(settings.get("ebidta_margin_growth")) or 0.0
    da_pct_step = _pct_to_decimal(settings.get("da_percent_growth")) or 0.0
    capex_pct_step = _pct_to_decimal(settings.get("capex_percent_growth")) or 0.0
    working_capital_step = _pct_to_decimal(settings.get("working_capital_days_growth")) or 0.0
    wacc_step = _pct_to_decimal(settings.get("wacc_direction")) or 0.0

    if starting_revenue_growth_cap is not None:
        revenue_growth = min(float(revenue_growth), float(starting_revenue_growth_cap))

    projected_fcff: List[float] = []
    projected_waccs: List[float] = []
    projected_revenues: List[float] = []
    projected_growths: List[float] = []
    projected_ebitdas: List[float] = []
    projected_ebitda_margins: List[float] = []
    projected_da_values: List[float] = []
    projected_da_pcts: List[float] = []
    projected_ebits: List[float] = []
    projected_nopats: List[float] = []
    projected_capex_signeds: List[float] = []
    projected_capex_pcts: List[float] = []
    projected_ncwcs: List[float] = []
    projected_wc_days_values: List[float] = []
    projected_change_ncwcs: List[float] = []
    prev_revenue = float(latest_revenue)
    prev_ncwc = series["ncwc"].get(int(latest_actual_year))
    if prev_ncwc is None:
        return _fail("Missing metric: Latest non-cash working capital")
    prev_ncwc = float(prev_ncwc)

    projection_years = [int(year) for year in range(int(latest_actual_year) + 1, explicit_final_year + 1)]
    for idx, year in enumerate(projection_years, start=1):
        if idx > 1:
            revenue_growth = float(revenue_growth) * (1.0 + float(revenue_growth_step))
            ebitda_margin = float(ebitda_margin) * (1.0 + float(ebitda_margin_step))
            da_pct = float(da_pct) * (1.0 + float(da_pct_step))
            capex_pct = float(capex_pct) * (1.0 + float(capex_pct_step))
            working_capital_days = float(working_capital_days) * (1.0 + float(working_capital_step))
            wacc = float(wacc) * (1.0 + float(wacc_step))

        revenue = float(prev_revenue) * (1.0 + float(revenue_growth))
        ebitda = float(revenue) * float(ebitda_margin)
        da_value = float(revenue) * float(da_pct)
        ebit = float(ebitda) - float(da_value)
        nopat = float(ebit) * (1.0 - float(tax_rate))
        capex_signed = -float(revenue) * float(capex_pct)
        ncwc = (float(working_capital_days) * float(revenue)) / 365.0
        change_in_ncwc = float(prev_ncwc) - float(ncwc)
        fcff = float(nopat) + float(da_value) + float(capex_signed) + float(change_in_ncwc)

        projected_revenues.append(float(revenue))
        projected_growths.append(float(revenue_growth))
        projected_ebitdas.append(float(ebitda))
        projected_ebitda_margins.append(float(ebitda_margin))
        projected_da_values.append(float(da_value))
        projected_da_pcts.append(float(da_pct))
        projected_ebits.append(float(ebit))
        projected_nopats.append(float(nopat))
        projected_capex_signeds.append(float(capex_signed))
        projected_capex_pcts.append(float(capex_pct))
        projected_ncwcs.append(float(ncwc))
        projected_wc_days_values.append(float(working_capital_days))
        projected_change_ncwcs.append(float(change_in_ncwc))
        projected_fcff.append(float(fcff))
        projected_waccs.append(float(wacc))
        prev_revenue = float(revenue)
        prev_ncwc = float(ncwc)

    if not projected_fcff or not projected_waccs:
        return _fail("Unable to build explicit FCFF projection horizon.")

    final_wacc = float(projected_waccs[-1])
    if final_wacc <= float(terminal_growth):
        return _fail(f"Validation error: final projected WACC ({final_wacc:.4f}) must be greater than terminal growth ({terminal_growth:.4f}).")

    fcff_terminal_year = float(projected_fcff[-1]) * (1.0 + float(terminal_growth))
    terminal_value = float(fcff_terminal_year) / (float(final_wacc) - float(terminal_growth))

    pv_fcff_total = 0.0
    discount_factors: List[float] = []
    pv_fcff_values: List[float] = []
    cumulative_discount_factor = 1.0
    for fcff, year_wacc in zip(projected_fcff, projected_waccs):
        cumulative_discount_factor /= (1.0 + float(year_wacc))
        pv_fcff = float(fcff) * float(cumulative_discount_factor)
        discount_factors.append(float(cumulative_discount_factor))
        pv_fcff_values.append(float(pv_fcff))
        pv_fcff_total += float(pv_fcff)

    final_discount_factor = float(cumulative_discount_factor)
    pv_terminal_value = float(terminal_value) * float(final_discount_factor)
    enterprise_value = float(pv_fcff_total) + float(pv_terminal_value)
    equity_value = float(enterprise_value) + float(latest_cash or 0.0) - float(latest_debt or 0.0)
    base_row["Intrinsic Value"] = equity_value

    if latest_shares is None or float(latest_shares) == 0.0:
        base_row["Validation"] = "Shares outstanding is missing or zero; per-share value not produced."
    else:
        base_row["Intrinsic Value"] = float(equity_value) / float(latest_shares)

    if base_row["Intrinsic Value"] is not None and current_market_price not in (None, 0):
        base_row["Difference %"] = ((float(base_row["Intrinsic Value"]) - float(current_market_price)) / float(current_market_price)) * 100.0

    value_creation_metrics = _compute_value_creation_filter_metrics(
        conn,
        company_id,
        int(latest_actual_year),
        max(int(latest_actual_year) - int(historical_years) + 1, 0),
        growth_weight_map,
        stddev_weight_map,
    )
    base_row["Total Scaled Volatility-Adjusted Score"] = value_creation_metrics.get("Total Scaled Volatility-Adjusted Score")
    base_row["Total Debt-Adjusted Scaled Volatility-Adjusted Score"] = value_creation_metrics.get("Total Debt-Adjusted Scaled Volatility-Adjusted Score")
    base_row["Overall Score (0-400)"] = _compute_ttc_overall_score(
        conn,
        company_id,
        f"{int(latest_actual_year)}-{max(int(latest_actual_year) - int(historical_years) + 1, 0)}",
        score_context,
    )
    detail_payload["validation"] = base_row["Validation"]

    actual_columns = [_actual_year_label(year) for year in actual_years]
    projected_columns = [_projected_year_label(year) for year in projection_years]
    discounting_columns = projected_columns + ["Terminal Year"]

    actual_revenue_values = [_safe_float(revenue_series.get(year)) for year in actual_years]
    actual_growth_values = [None] + [_compute_growth_for_year(revenue_series, year) for year in actual_years[1:]]
    actual_ebitda_values = [_safe_float(series["ebitda"].get(year)) for year in actual_years]
    actual_ebitda_margin_values = [_compute_series_ratio(series["ebitda"].get(year), revenue_series.get(year)) for year in actual_years]
    actual_da_values = [_safe_float(series["da"].get(year)) for year in actual_years]
    actual_da_pct_values = [_compute_series_ratio(series["da"].get(year), revenue_series.get(year)) for year in actual_years]
    actual_ebit_values = [
        (
            float(ebitda_value) - float(da_value)
            if ebitda_value is not None and da_value is not None
            else None
        )
        for ebitda_value, da_value in zip(actual_ebitda_values, actual_da_values)
    ]
    actual_tax_rate_values = [float(tax_rate)] * len(actual_years)
    actual_nopat_values = [
        float(ebit_value) * (1.0 - float(tax_rate)) if ebit_value is not None else None
        for ebit_value in actual_ebit_values
    ]
    actual_capex_raw_values = [_safe_float(series["capex"].get(year)) for year in actual_years]
    actual_capex_outflows = [(-abs(float(capex_value)) if capex_value is not None else None) for capex_value in actual_capex_raw_values]
    actual_capex_pct_values = [
        (abs(float(capex_value)) / float(revenue_value) if capex_value is not None and revenue_value not in (None, 0) else None)
        for capex_value, revenue_value in zip(actual_capex_raw_values, actual_revenue_values)
    ]
    actual_total_current_assets = [_safe_float(series["total_current_assets"].get(year)) for year in actual_years]
    actual_cash_values = [_safe_float(series["cash"].get(year)) for year in actual_years]
    actual_total_current_liabilities = [_safe_float(series["total_current_liabilities"].get(year)) for year in actual_years]
    actual_current_debt = [_safe_float(series["current_debt"].get(year)) for year in actual_years]
    actual_net_current_assets = [
        (float(total_current_assets) - float(cash_value) if total_current_assets is not None and cash_value is not None else None)
        for total_current_assets, cash_value in zip(actual_total_current_assets, actual_cash_values)
    ]
    actual_net_current_liabilities = [
        (
            float(total_current_liabilities) - float(current_debt_value)
            if total_current_liabilities is not None and current_debt_value is not None
            else None
        )
        for total_current_liabilities, current_debt_value in zip(actual_total_current_liabilities, actual_current_debt)
    ]
    actual_ncwc_values = [_safe_float(series["ncwc"].get(year)) for year in actual_years]
    actual_wc_days_values = [
        ((float(ncwc_value) * 365.0) / float(revenue_value) if ncwc_value is not None and revenue_value not in (None, 0) else None)
        for ncwc_value, revenue_value in zip(actual_ncwc_values, actual_revenue_values)
    ]
    actual_change_ncwcs = [None]
    actual_fcff_values = [None]
    for idx in range(1, len(actual_years)):
        previous_ncwc = actual_ncwc_values[idx - 1]
        current_ncwc = actual_ncwc_values[idx]
        change_in_ncwc = float(previous_ncwc) - float(current_ncwc) if previous_ncwc is not None and current_ncwc is not None else None
        actual_change_ncwcs.append(change_in_ncwc)
        if None in (actual_nopat_values[idx], actual_da_values[idx], actual_capex_outflows[idx], change_in_ncwc):
            actual_fcff_values.append(None)
        else:
            actual_fcff_values.append(
                float(actual_nopat_values[idx]) + float(actual_da_values[idx]) + float(actual_capex_outflows[idx]) + float(change_in_ncwc)
            )

    difference_pct_value = _safe_float(base_row.get("Difference %"))
    detail_payload["summary_rows"] = [
        {"metric": "Current Market Price", "value": base_row.get("Current Market Price"), "format": "number"},
        {"metric": "Intrinsic Value", "value": base_row.get("Intrinsic Value"), "format": "number"},
        {"metric": "Difference %", "value": (difference_pct_value / 100.0 if difference_pct_value is not None else None), "format": "percent"},
        {"metric": "Price Source", "value": base_row.get("Price Source"), "format": "text"},
        {"metric": "Quote As Of", "value": base_row.get("Quote As Of"), "format": "text"},
        {"metric": "Price Source Detail", "value": base_row.get("Price Source Detail"), "format": "text"},
        {"metric": "Overall Score (0-400)", "value": base_row.get("Overall Score (0-400)"), "format": "number"},
        {
            "metric": "Debt-Adjusted Volatility Score",
            "value": base_row.get("Total Debt-Adjusted Scaled Volatility-Adjusted Score"),
            "format": "number",
        },
    ]
    detail_payload["assumptions_rows"] = [
        {"metric": "Historical Years Used", "value": historical_years, "format": "integer"},
        {"metric": "Revenue Growth Descend/Ascend", "value": revenue_growth_step, "format": "percent"},
        {"metric": "Starting Projected Revenue Growth Cap", "value": starting_revenue_growth_cap, "format": "percent"},
        {"metric": "EBITDA Margin Descend/Ascend", "value": ebitda_margin_step, "format": "percent"},
        {"metric": "D&A Percent Descend/Ascend", "value": da_pct_step, "format": "percent"},
        {"metric": "CAPEX Percent Descend/Ascend", "value": capex_pct_step, "format": "percent"},
        {"metric": "Working Capital Days Descend/Ascend", "value": working_capital_step, "format": "percent"},
        {"metric": "WACC Descend/Ascend", "value": wacc_step, "format": "percent"},
        {"metric": f"Median WACC ({len(wacc_history)} yrs)", "value": _pct_to_decimal(_median(wacc_history)), "format": "percent"},
        {
            "metric": "WACC History",
            "value": ", ".join(
                f"{normalized_value * 100:.2f}%"
                for value in wacc_history
                for normalized_value in [_pct_to_decimal(value)]
                if normalized_value is not None
            ),
            "format": "text",
        },
        {"metric": "Effective Tax Rate", "value": tax_rate, "format": "percent"},
        {"metric": "Terminal Growth Rate", "value": terminal_growth, "format": "percent"},
        {"metric": "Terminal Year", "value": str(int(terminal_year)), "format": "text"},
    ]
    detail_payload["operating_table"] = _build_breakdown_table(
        actual_columns + projected_columns,
        [
            ("Revenue", "number", actual_revenue_values + projected_revenues),
            ("Growth", "percent", actual_growth_values + projected_growths),
            ("EBITDA", "number", actual_ebitda_values + projected_ebitdas),
            ("EBITDA Margin", "percent", actual_ebitda_margin_values + projected_ebitda_margins),
            ("D&A", "number", actual_da_values + projected_da_values),
            ("D&A %", "percent", actual_da_pct_values + projected_da_pcts),
            ("EBIT", "number", actual_ebit_values + projected_ebits),
            ("Tax Rate", "percent", actual_tax_rate_values + ([float(tax_rate)] * len(projection_years))),
            ("NOPAT", "number", actual_nopat_values + projected_nopats),
            ("CAPEX", "number", actual_capex_outflows + projected_capex_signeds),
            ("Capex %", "percent", actual_capex_pct_values + projected_capex_pcts),
            ("FCFF", "number", actual_fcff_values + projected_fcff),
        ],
    )
    detail_payload["working_capital_table"] = _build_breakdown_table(
        actual_columns + projected_columns,
        [
            ("Total Current Assets", "number", actual_total_current_assets + ([None] * len(projection_years))),
            ("Cash & Cash Equivalents", "number", actual_cash_values + ([None] * len(projection_years))),
            ("Net Current Assets", "number", actual_net_current_assets + ([None] * len(projection_years))),
            ("Total Current Liabilities", "number", actual_total_current_liabilities + ([None] * len(projection_years))),
            ("Current Debt", "number", actual_current_debt + ([None] * len(projection_years))),
            ("Net Current Liabilities", "number", actual_net_current_liabilities + ([None] * len(projection_years))),
            ("Non-Cash Working Capital", "number", actual_ncwc_values + projected_ncwcs),
            ("Working Capital Days", "number", actual_wc_days_values + projected_wc_days_values),
            ("Change in NCWC", "number", actual_change_ncwcs + projected_change_ncwcs),
        ],
    )
    detail_payload["discounting_table"] = _build_breakdown_table(
        discounting_columns,
        [
            ("Future Year", "integer", list(range(1, len(projection_years) + 1)) + [None]),
            ("Projected Year WACC", "percent", projected_waccs + [None]),
            ("Discount Factor", "decimal4", discount_factors + [None]),
            ("FCFF", "number", projected_fcff + [fcff_terminal_year]),
            ("PV of FCFF", "number", pv_fcff_values + [None]),
            ("Terminal Value", "number", ([None] * len(projection_years)) + [terminal_value]),
            ("PV of Terminal Value", "number", ([None] * len(projection_years)) + [pv_terminal_value]),
            ("Enterprise Value", "number", ([None] * len(projection_years)) + [enterprise_value]),
            ("Less: Debt", "number", ([None] * len(projection_years)) + [-(float(latest_debt or 0.0))]),
            ("Plus: Cash & Cash Equivalents", "number", ([None] * len(projection_years)) + [float(latest_cash or 0.0)]),
            ("Equity Value", "number", ([None] * len(projection_years)) + [equity_value]),
            ("Total Shares Outstanding", "number", ([None] * len(projection_years)) + [latest_shares]),
            (
                "Price per Share",
                "number",
                ([None] * len(projection_years))
                + [None if latest_shares in (None, 0) else base_row.get("Intrinsic Value")],
            ),
        ],
    )
    return base_row


def _run_industry_dcf_valuations(conn, selected_bucket_names: List[str], terminal_year: int) -> pd.DataFrame:
    groups_df = read_df(
        f"SELECT id, name FROM company_groups WHERE name IN ({','.join(['?'] * len(selected_bucket_names))}) ORDER BY name",
        conn,
        params=tuple(selected_bucket_names),
    )
    if groups_df is None or groups_df.empty:
        return pd.DataFrame()

    selected_group_ids = groups_df["id"].astype(int).tolist()
    members_df = read_df(
        f"""
        SELECT DISTINCT c.id, c.name, c.ticker, c.country
        FROM company_group_members m
        JOIN companies c ON c.id = m.company_id
        WHERE m.group_id IN ({','.join(['?'] * len(selected_group_ids))})
        ORDER BY c.name
        """,
        conn,
        params=tuple(selected_group_ids),
    )
    if members_df is None or members_df.empty:
        return pd.DataFrame()

    memberships_df = _get_company_group_memberships(conn, members_df["id"].astype(int).tolist())
    industry_overrides_df = get_dcf_industry_valuation_settings(conn, selected_group_ids)
    company_overrides_df = get_dcf_company_valuation_settings(conn, members_df["id"].astype(int).tolist())
    general_settings = _get_general_settings_dict(conn)
    bucket_map_all = _get_company_buckets(conn, members_df["id"].astype(int).tolist())
    selected_bucket_set = set(selected_bucket_names)
    selected_bucket_map: Dict[int, str] = {}
    for _, row in memberships_df.iterrows():
        cid = int(row["company_id"])
        group_name = str(row["group_name"])
        if group_name not in selected_bucket_set:
            continue
        selected_bucket_map.setdefault(cid, [])
        selected_bucket_map[cid].append(group_name)
    selected_bucket_map = {cid: ", ".join(sorted(set(names))) for cid, names in selected_bucket_map.items()}
    for cid, names in bucket_map_all.items():
        selected_bucket_map.setdefault(cid, names)

    growth_weight_map, stddev_weight_map = _load_weight_maps(conn)
    score_context = _build_ttc_context(conn)
    live_quote_map = _fetch_live_quotes_for_companies(members_df)

    progress_bar = st.progress(0, text="Preparing industry DCF valuation run...")
    total = max(len(members_df), 1)
    rows: List[Dict[str, object]] = []

    for idx, (_, company_row) in enumerate(members_df.iterrows(), start=1):
        company_id = int(company_row["id"])
        progress_bar.progress(
            int((idx / total) * 100),
            text=f"Running DCF valuation for {company_row['name']} ({idx}/{total})...",
        )

        compute_and_store_fcff_and_reinvestment_rate(conn, company_id)
        compute_and_store_levered_beta(conn, company_id)
        compute_and_store_cost_of_equity(conn, company_id)
        compute_and_store_pre_tax_cost_of_debt(conn, company_id)
        compute_and_store_wacc(conn, company_id)
        compute_and_store_roic_wacc_spread(conn, company_id)

        effective_settings = _get_effective_company_settings(
            company_id,
            general_settings,
            industry_overrides_df,
            company_overrides_df,
            memberships_df,
        )
        rows.append(
            _compute_dcf_projection(
                conn=conn,
                company_row=company_row,
                live_quote=live_quote_map.get(company_id),
                settings=effective_settings,
                terminal_year=int(terminal_year),
                score_context=score_context,
                growth_weight_map=growth_weight_map,
                stddev_weight_map=stddev_weight_map,
                selected_bucket_map=selected_bucket_map,
            )
        )

    progress_bar.progress(100, text="Industry DCF valuation run complete.")
    results_df = pd.DataFrame(rows)
    if results_df.empty:
        return results_df

    sort_col = "Total Debt-Adjusted Scaled Volatility-Adjusted Score"
    if sort_col in results_df.columns:
        results_df = results_df.sort_values(by=sort_col, ascending=False, na_position="last").reset_index(drop=True)
    return results_df


def _run_company_dcf_valuations(conn, selected_company_ids: List[int], terminal_year: int) -> pd.DataFrame:
    if not selected_company_ids:
        return pd.DataFrame()

    members_df = read_df(
        f"""
        SELECT DISTINCT c.id, c.name, c.ticker, c.country
        FROM companies c
        WHERE c.id IN ({','.join(['?'] * len(selected_company_ids))})
        ORDER BY c.name
        """,
        conn,
        params=tuple(int(company_id) for company_id in selected_company_ids),
    )
    if members_df is None or members_df.empty:
        return pd.DataFrame()

    memberships_df = _get_company_group_memberships(conn, members_df["id"].astype(int).tolist())
    selected_group_ids = sorted({int(group_id) for group_id in memberships_df.get("group_id", pd.Series(dtype=int)).dropna().tolist()})
    industry_overrides_df = get_dcf_industry_valuation_settings(conn, selected_group_ids) if selected_group_ids else pd.DataFrame()
    company_overrides_df = get_dcf_company_valuation_settings(conn, members_df["id"].astype(int).tolist())
    general_settings = _get_general_settings_dict(conn)
    selected_bucket_map = _get_company_buckets(conn, members_df["id"].astype(int).tolist())

    growth_weight_map, stddev_weight_map = _load_weight_maps(conn)
    score_context = _build_ttc_context(conn)
    live_quote_map = _fetch_live_quotes_for_companies(members_df)

    progress_bar = st.progress(0, text="Preparing company DCF valuation run...")
    total = max(len(members_df), 1)
    rows: List[Dict[str, object]] = []

    for idx, (_, company_row) in enumerate(members_df.iterrows(), start=1):
        company_id = int(company_row["id"])
        progress_bar.progress(
            int((idx / total) * 100),
            text=f"Running DCF valuation for {company_row['name']} ({idx}/{total})...",
        )

        compute_and_store_fcff_and_reinvestment_rate(conn, company_id)
        compute_and_store_levered_beta(conn, company_id)
        compute_and_store_cost_of_equity(conn, company_id)
        compute_and_store_pre_tax_cost_of_debt(conn, company_id)
        compute_and_store_wacc(conn, company_id)
        compute_and_store_roic_wacc_spread(conn, company_id)

        effective_settings = _get_effective_company_settings(
            company_id,
            general_settings,
            industry_overrides_df,
            company_overrides_df,
            memberships_df,
        )
        rows.append(
            _compute_dcf_projection(
                conn=conn,
                company_row=company_row,
                live_quote=live_quote_map.get(company_id),
                settings=effective_settings,
                terminal_year=int(terminal_year),
                score_context=score_context,
                growth_weight_map=growth_weight_map,
                stddev_weight_map=stddev_weight_map,
                selected_bucket_map=selected_bucket_map,
            )
        )

    progress_bar.progress(100, text="Company DCF valuation run complete.")
    results_df = pd.DataFrame(rows)
    if results_df.empty:
        return results_df

    sort_col = "Total Debt-Adjusted Scaled Volatility-Adjusted Score"
    if sort_col in results_df.columns:
        results_df = results_df.sort_values(by=sort_col, ascending=False, na_position="last").reset_index(drop=True)
    return results_df


def _render_dcf_results(display_df: pd.DataFrame, *, caption_text: str, download_label: str, download_file_name: str, download_key: str) -> None:
    visible_df = display_df[[column for column in display_df.columns if not str(column).startswith("__")]].copy()
    st.markdown("---")
    st.caption(caption_text)
    st.caption("Live = fetched during this run. DB fallback = stored price from database.")

    price_source_counts = _summarize_price_source(visible_df)
    st.caption(
        f"{price_source_counts.get('Live', 0)} live quotes, "
        f"{price_source_counts.get('DB fallback', 0)} DB fallbacks, "
        f"{price_source_counts.get('Unavailable', 0)} unavailable."
    )

    styled_df = visible_df.style.map(_style_price_source, subset=["Price Source"]) if "Price Source" in visible_df.columns else visible_df
    st.dataframe(
        styled_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Total Scaled Volatility-Adjusted Score": st.column_config.NumberColumn(format="%.2f"),
            "Total Debt-Adjusted Scaled Volatility-Adjusted Score": st.column_config.NumberColumn(format="%.2f"),
            "Overall Score (0-400)": st.column_config.NumberColumn(format="%.2f"),
            "Current Market Price": st.column_config.NumberColumn(format="%.2f"),
            "Price Source": st.column_config.TextColumn(
                help="Live means fetched during this run. DB fallback means the stored market price from the database was used."
            ),
            "Quote As Of": st.column_config.TextColumn(
                help="Timestamp or period associated with the market price shown for this row."
            ),
            "Price Source Detail": st.column_config.TextColumn(
                help="Additional detail explaining why this market price source was used."
            ),
            "Intrinsic Value": st.column_config.NumberColumn(format="%.2f"),
            "Difference %": st.column_config.NumberColumn(format="%.2f%%"),
        },
    )

    export_csv = visible_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        download_label,
        data=export_csv,
        file_name=download_file_name,
        mime="text/csv",
        key=download_key,
    )

    if "Validation" in visible_df.columns and visible_df["Validation"].notna().any():
        st.warning("Some companies completed with validation issues. Review the Validation column for details.")


def _get_latest_risk_free_rates(conn) -> Optional[Dict[str, float]]:
    df = read_df(
        """
        SELECT
            year,
            usa_rf,
            india_rf,
            china_rf,
            japan_rf
        FROM risk_free_rates
        ORDER BY year DESC
        LIMIT 1
        """,
        conn,
    )
    if df.empty:
        return None

    row = df.iloc[0]
    return {
        "year": int(row["year"]),
        "USA": float(row["usa_rf"]),
        "India": float(row["india_rf"]),
        "China": float(row["china_rf"]),
        "Japan": float(row["japan_rf"]),
    }


def _get_bucket_members(conn, bucket_name: str) -> pd.DataFrame:
    return read_df(
        """
        SELECT
            c.id,
            c.name,
            c.ticker,
            c.country
        FROM company_group_members m
        JOIN company_groups g ON g.id = m.group_id
        JOIN companies c ON c.id = m.company_id
        WHERE g.name = ?
        ORDER BY c.name
        """,
        conn,
        params=(bucket_name,),
    )


def _get_company_group_memberships(conn, company_ids: Optional[List[int]] = None) -> pd.DataFrame:
    sql = """
        SELECT
            m.company_id,
            g.id AS group_id,
            g.name AS group_name
        FROM company_group_members m
        JOIN company_groups g ON g.id = m.group_id
    """
    params = None
    if company_ids:
        placeholders = ",".join(["?"] * len(company_ids))
        sql += f" WHERE m.company_id IN ({placeholders})"
        params = tuple(int(x) for x in company_ids)
    sql += " ORDER BY g.name"
    return read_df(sql, conn, params=params)


def _row_to_settings_dict(row) -> Dict[str, float]:
    defaults = {
        "starting_projected_revenue_growth_cap": 25.0,
    }
    return {field: row.get(field, defaults.get(field)) for field in _SETTINGS_FIELDS}


def _get_general_settings_dict(conn) -> Dict[str, float]:
    saved_df = get_dcf_valuation_settings(conn)
    default_rates = _get_latest_risk_free_rates(conn)
    if saved_df.empty:
        return {
            "historical_years": 7,
            "terminal_growth_usa": float(default_rates["USA"]) if default_rates else 0.0,
            "terminal_growth_india": float(default_rates["India"]) if default_rates else 0.0,
            "terminal_growth_china": float(default_rates["China"]) if default_rates else 0.0,
            "terminal_growth_japan": float(default_rates["Japan"]) if default_rates else 0.0,
            "future_revenue_growth": 0.0,
            "starting_projected_revenue_growth_cap": 25.0,
            "ebidta_margin_growth": 0.0,
            "da_percent_growth": 0.0,
            "capex_percent_growth": 0.0,
            "working_capital_days_growth": 0.0,
            "wacc_direction": 0.0,
        }
    row = saved_df.iloc[0]
    return {
        "historical_years": int(row["historical_years"]),
        "terminal_growth_usa": float(row["terminal_growth_usa"]),
        "terminal_growth_india": float(row["terminal_growth_india"]),
        "terminal_growth_china": float(row["terminal_growth_china"]),
        "terminal_growth_japan": float(row["terminal_growth_japan"]),
        "future_revenue_growth": float(row["future_revenue_growth"]),
        "starting_projected_revenue_growth_cap": float(row.get("starting_projected_revenue_growth_cap", 25.0)),
        "ebidta_margin_growth": float(row["ebidta_margin_growth"]),
        "da_percent_growth": float(row["da_percent_growth"]),
        "capex_percent_growth": float(row["capex_percent_growth"]),
        "working_capital_days_growth": float(row["working_capital_days_growth"]),
        "wacc_direction": float(row["wacc_direction"]),
    }


def _render_settings_form(initial_values: Dict[str, float], save_label: str, form_key: str) -> Optional[Dict[str, float]]:
    with st.form(form_key):
        historical_years = st.number_input(
            "Number of historical years to consider",
            min_value=3,
            max_value=10,
            value=int(initial_values["historical_years"]),
            step=1,
            help="Controls how many historical annual observations are used when calculating the DCF starting point.",
        )

        st.markdown("---")
        st.markdown("**Terminal Growth Rate**")
        tg_col1, tg_col2, tg_col3, tg_col4 = st.columns(4)
        with tg_col1:
            terminal_growth_usa = st.number_input("USA Terminal Growth %", value=float(initial_values["terminal_growth_usa"]), step=0.10, format="%.2f")
        with tg_col2:
            terminal_growth_india = st.number_input("India Terminal Growth %", value=float(initial_values["terminal_growth_india"]), step=0.10, format="%.2f")
        with tg_col3:
            terminal_growth_china = st.number_input("China Terminal Growth %", value=float(initial_values["terminal_growth_china"]), step=0.10, format="%.2f")
        with tg_col4:
            terminal_growth_japan = st.number_input("Japan Terminal Growth %", value=float(initial_values["terminal_growth_japan"]), step=0.10, format="%.2f")

        st.markdown("---")
        col1, col2 = st.columns(2)
        with col1:
            future_revenue_growth = st.number_input(
                "Future Revenue Growth %",
                value=float(initial_values["future_revenue_growth"]),
                step=0.10,
                format="%.2f",
                help="This percentage reflects how the median of the historical revenue growth for each company descends or ascends into the future.",
            )
            starting_projected_revenue_growth_cap = st.number_input(
                "Starting Projected Revenue Growth Cap %",
                value=float(initial_values["starting_projected_revenue_growth_cap"]),
                step=0.10,
                format="%.2f",
                help="Caps the first projected year's revenue growth after the historical median is derived. Later projected years continue from that starting point using Future Revenue Growth %.",
            )
            ebidta_margin_growth = st.number_input(
                "EBIDTA Margin Growth %",
                value=float(initial_values["ebidta_margin_growth"]),
                step=0.10,
                format="%.2f",
                help="This percentage reflects how the median of the historical EBIDTA Margin for each company descends or ascends into the future.",
            )
            da_percent_growth = st.number_input(
                "D&A Percent Growth %",
                value=float(initial_values["da_percent_growth"]),
                step=0.10,
                format="%.2f",
                help="This percentage reflects how the median of the historical D&A Percent for each company descends or ascends into the future.",
            )
        with col2:
            capex_percent_growth = st.number_input(
                "CAPEX Percent Growth %",
                value=float(initial_values["capex_percent_growth"]),
                step=0.10,
                format="%.2f",
                help="This percentage reflects how the median of the historical CAPEX Percent for each company descends or ascends into the future.",
            )
            working_capital_days_growth = st.number_input(
                "Working Capital Days Growth %",
                value=float(initial_values["working_capital_days_growth"]),
                step=0.10,
                format="%.2f",
                help="This percentage reflects how the median of the historical Working Capital Days for each company descends or ascends into the future.",
            )
            wacc_direction = st.number_input(
                "WACC direction %",
                value=float(initial_values["wacc_direction"]),
                step=0.10,
                format="%.2f",
                help="This percentage reflects how the median of the historical WACC for each company descends or ascends into the future.",
            )

        submitted = st.form_submit_button(save_label, type="primary")

    if not submitted:
        return None

    return {
        "historical_years": int(historical_years),
        "terminal_growth_usa": float(terminal_growth_usa),
        "terminal_growth_india": float(terminal_growth_india),
        "terminal_growth_china": float(terminal_growth_china),
        "terminal_growth_japan": float(terminal_growth_japan),
        "future_revenue_growth": float(future_revenue_growth),
        "starting_projected_revenue_growth_cap": float(starting_projected_revenue_growth_cap),
        "ebidta_margin_growth": float(ebidta_margin_growth),
        "da_percent_growth": float(da_percent_growth),
        "capex_percent_growth": float(capex_percent_growth),
        "working_capital_days_growth": float(working_capital_days_growth),
        "wacc_direction": float(wacc_direction),
    }


def _render_industry_tab(conn) -> None:
    st.subheader("Industry")

    buckets_df = read_df("SELECT id, name FROM company_groups ORDER BY name", conn)
    if buckets_df.empty:
        st.info("No industry buckets are available yet. Create or assign buckets in Equity Research -> Value Creation Stability Score -> Admin -> Buckets.")
        return

    bucket_names = buckets_df["name"].astype(str).tolist()
    current_bucket = st.session_state.get(_DCF_BUCKET_KEY)
    if current_bucket not in bucket_names:
        current_bucket = None

    selected_bucket_names = st.multiselect(
        "Industry bucket(s)",
        options=bucket_names,
        default=st.session_state.get(_DCF_BUCKET_MULTI_KEY, []),
        key="dcf_industry_bucket_multi_select",
        help="Select one or more industry buckets to run the DCF valuation workflow across the combined company set.",
    )
    if selected_bucket_names:
        st.session_state[_DCF_BUCKET_KEY] = selected_bucket_names[0]
    st.session_state[_DCF_BUCKET_MULTI_KEY] = selected_bucket_names

    if not selected_bucket_names:
        st.caption("Select one or more industry buckets to inspect membership and run valuations.")
        st.session_state[_DCF_RUN_CONFIG_VISIBLE_KEY] = False
        return

    selected_group_ids = buckets_df.loc[buckets_df["name"].isin(selected_bucket_names), "id"].astype(int).tolist()
    members_df = read_df(
        f"""
        SELECT DISTINCT c.id, c.name, c.ticker, c.country
        FROM company_group_members m
        JOIN companies c ON c.id = m.company_id
        WHERE m.group_id IN ({','.join(['?'] * len(selected_group_ids))})
        ORDER BY c.name
        """,
        conn,
        params=tuple(selected_group_ids),
    )

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Companies in scope", int(len(members_df)))
    with col2:
        countries = sorted({str(x) for x in members_df["country"].dropna().tolist() if str(x).strip()})
        st.metric("Countries represented", int(len(countries)))
    with col3:
        st.metric("Selected industries", int(len(selected_bucket_names)))

    if members_df.empty:
        st.warning("The selected industry bucket set currently has no assigned companies.")
        return

    st.caption("Current company membership that will feed the industry-level DCF workflow.")
    st.dataframe(
        members_df.rename(columns={"name": "Company", "ticker": "Ticker", "country": "Country"}),
        use_container_width=True,
        hide_index=True,
    )

    if st.button("Run Valuations for Industry Bucket", type="primary", key="dcf_prepare_industry_run"):
        st.session_state[_DCF_RUN_CONFIG_VISIBLE_KEY] = True

    if not st.session_state.get(_DCF_RUN_CONFIG_VISIBLE_KEY, False):
        return

    current_year = datetime.now().year
    terminal_year_options = [year for year in range(current_year + 1, current_year + 11)]
    with st.form("dcf_industry_run_form"):
        terminal_year = st.selectbox(
            "What should be the terminal year?",
            options=terminal_year_options,
            index=min(6, len(terminal_year_options) - 1),
            help="The model projects explicit FCFF through terminal year minus 1, then extends FCFF into the terminal year for terminal value.",
        )
        run_submitted = st.form_submit_button("Start Industry Valuations", type="primary")

    if run_submitted:
        st.session_state[_DCF_RESULTS_KEY] = _run_industry_dcf_valuations(conn, selected_bucket_names, int(terminal_year))
        st.session_state[_DCF_RESULTS_META_KEY] = {
            "bucket_names": list(selected_bucket_names),
            "terminal_year": int(terminal_year),
        }

    results_df = st.session_state.get(_DCF_RESULTS_KEY)
    results_meta = st.session_state.get(_DCF_RESULTS_META_KEY, {})
    if not isinstance(results_df, pd.DataFrame) or results_df.empty:
        return

    if results_meta.get("bucket_names") != list(selected_bucket_names):
        st.info("Run valuations again to refresh the dashboard for the newly selected industry set.")
        return

    display_df = results_df.copy()
    _render_dcf_results(
        display_df,
        caption_text=(
            f"Results for {', '.join(selected_bucket_names)} "
            f"with terminal year {results_meta.get('terminal_year', terminal_year_options[0])}."
        ),
        download_label="Download Industry DCF Results (CSV)",
        download_file_name="industry_dcf_results.csv",
        download_key="dcf_industry_results_download",
    )


def _render_company_tab(conn) -> None:
    st.subheader("Company")

    companies_df = list_companies(conn)
    if companies_df.empty:
        st.info("No companies are available yet. Upload company financials first.")
        return

    filtered_df = companies_df.copy()
    filtered_df["ticker"] = filtered_df["ticker"].fillna("").astype(str)
    filtered_df["company_label"] = filtered_df.apply(
        lambda row: f"{row['name']} ({row['ticker']})" if row["ticker"].strip() else str(row["name"]),
        axis=1,
    )

    duplicate_labels = filtered_df["company_label"].value_counts()
    if (duplicate_labels > 1).any() and "country" in filtered_df.columns:
        filtered_df["country"] = filtered_df["country"].fillna("").astype(str)
        filtered_df.loc[
            filtered_df["company_label"].isin(duplicate_labels[duplicate_labels > 1].index),
            "company_label",
        ] = filtered_df.apply(
            lambda row: (
                f"{row['company_label']} - {row['country']}"
                if duplicate_labels.get(row["company_label"], 0) > 1 and row["country"].strip()
                else row["company_label"]
            ),
            axis=1,
        )

    label_to_id = {str(row.company_label): int(row.id) for _, row in filtered_df.iterrows()}
    options = filtered_df["company_label"].astype(str).tolist()
    stored_selection_ids = st.session_state.get(_DCF_COMPANY_KEY, [])
    default_selection = [
        label
        for label, company_id in label_to_id.items()
        if company_id in stored_selection_ids
    ]

    selected_company_labels = st.multiselect(
        "Companies for DCF valuation",
        options=options,
        default=default_selection,
        key="dcf_company_multi_select",
        help="Select one or more companies to include in the DCF valuation workflow.",
    )
    selected_company_ids = [label_to_id[label] for label in selected_company_labels if label in label_to_id]
    st.session_state[_DCF_COMPANY_KEY] = selected_company_ids

    st.caption("Search and select one or more companies independently of the Industry tab.")
    if not selected_company_ids:
        st.warning("Select at least one company to proceed with company-level DCF valuation.")
        return

    selected_df = filtered_df[filtered_df["id"].isin(selected_company_ids)].copy()
    st.dataframe(
        selected_df.rename(columns={"name": "Company", "ticker": "Ticker"}),
        use_container_width=True,
        hide_index=True,
    )

    if st.button("Run Valuations", type="primary", key="dcf_prepare_company_run"):
        st.session_state[_DCF_COMPANY_RUN_CONFIG_VISIBLE_KEY] = True

    if not st.session_state.get(_DCF_COMPANY_RUN_CONFIG_VISIBLE_KEY, False):
        return

    current_year = datetime.now().year
    terminal_year_options = [year for year in range(current_year + 1, current_year + 11)]
    with st.form("dcf_company_run_form"):
        terminal_year = st.selectbox(
            "What should be the terminal year?",
            options=terminal_year_options,
            index=min(6, len(terminal_year_options) - 1),
            help="The model projects explicit FCFF through terminal year minus 1, then extends FCFF into the terminal year for terminal value.",
            key="dcf_company_terminal_year",
        )
        run_submitted = st.form_submit_button("Start Company Valuations", type="primary")

    if run_submitted:
        st.session_state[_DCF_COMPANY_RESULTS_KEY] = _run_company_dcf_valuations(conn, selected_company_ids, int(terminal_year))
        st.session_state[_DCF_COMPANY_RESULTS_META_KEY] = {
            "company_ids": list(selected_company_ids),
            "company_names": selected_df["name"].astype(str).tolist(),
            "terminal_year": int(terminal_year),
        }
        st.session_state[_DCF_COMPANY_EXPANDED_RESULT_KEY] = None

    results_df = st.session_state.get(_DCF_COMPANY_RESULTS_KEY)
    results_meta = st.session_state.get(_DCF_COMPANY_RESULTS_META_KEY, {})
    if not isinstance(results_df, pd.DataFrame) or results_df.empty:
        return

    if results_meta.get("company_ids") != list(selected_company_ids):
        st.info("Run valuations again to refresh the dashboard for the newly selected company set.")
        return

    display_df = results_df.copy()
    _render_dcf_results(
        display_df,
        caption_text=(
            f"Results for {', '.join(results_meta.get('company_names', selected_df['name'].astype(str).tolist()))} "
            f"with terminal year {results_meta.get('terminal_year', terminal_year_options[0])}."
        ),
        download_label="Download Company DCF Results (CSV)",
        download_file_name="company_dcf_results.csv",
        download_key="dcf_company_results_download",
    )

    result_rows = []
    for _, row in results_df.iterrows():
        company_id = row.get("__company_id")
        if company_id is None or pd.isna(company_id):
            continue
        company_id = int(company_id)
        label = str(row.get("Company Name") or company_id)
        ticker_value = str(row.get("Ticker") or "").strip()
        if ticker_value:
            label = f"{label} ({ticker_value})"
        result_rows.append((company_id, label))

    if not result_rows:
        return

    option_map = dict(result_rows)
    option_ids = list(option_map.keys())
    stored_selected_result_id = st.session_state.get(_DCF_COMPANY_RESULT_SELECT_KEY)
    if stored_selected_result_id not in option_map:
        stored_selected_result_id = option_ids[0]
        st.session_state[_DCF_COMPANY_RESULT_SELECT_KEY] = stored_selected_result_id

    st.caption("Select one company from the output and expand the valuation mechanics below the results table.")
    selector_col, action_col = st.columns([3, 1])
    with selector_col:
        selected_result_company_id = st.radio(
            "Company valuation detail",
            options=option_ids,
            index=option_ids.index(stored_selected_result_id),
            format_func=lambda company_id: option_map.get(company_id, str(company_id)),
            key=_DCF_COMPANY_RESULT_SELECT_KEY,
        )
    with action_col:
        st.write("")
        st.write("")
        if st.button("Expand Valuation", key="dcf_company_expand_detail"):
            st.session_state[_DCF_COMPANY_EXPANDED_RESULT_KEY] = int(selected_result_company_id)

    expanded_company_id = st.session_state.get(_DCF_COMPANY_EXPANDED_RESULT_KEY)
    if expanded_company_id not in option_map:
        return

    expanded_row_df = results_df[results_df["__company_id"] == int(expanded_company_id)]
    if expanded_row_df.empty:
        return

    expanded_row = expanded_row_df.iloc[0]
    detail_payload = expanded_row.get("__valuation_detail")
    validation_message = expanded_row.get("Validation")

    st.markdown("---")
    st.markdown(f"**Expanded Valuation: {option_map[int(expanded_company_id)]}**")
    if validation_message and (not isinstance(detail_payload, dict) or not detail_payload.get("operating_table")):
        st.warning(str(validation_message))
        return
    if not isinstance(detail_payload, dict):
        st.info("Detailed valuation mechanics are not available for this company in the current run.")
        return

    _render_company_valuation_detail(detail_payload)


def _render_general_settings_tab(conn) -> None:
    st.subheader("General")

    default_rates = _get_latest_risk_free_rates(conn)
    saved_df = get_dcf_valuation_settings(conn)
    initial_values = _get_general_settings_dict(conn)

    if not saved_df.empty and saved_df.iloc[0]["updated_at"]:
        st.caption(f"Last saved: {saved_df.iloc[0]['updated_at']}")
    elif default_rates is not None:
        st.caption(f"Initial terminal-growth defaults come from the latest risk-free-rate row in the database ({default_rates['year']}).")

    values = _render_settings_form(initial_values, "Save General Settings", "dcf_general_settings_form")
    if values is None:
        st.markdown("---")
        st.caption("General settings are the fallback layer used when neither industry nor company overrides exist.")
        return

    upsert_dcf_valuation_settings(
        conn,
        historical_years=values["historical_years"],
        terminal_growth_usa=values["terminal_growth_usa"],
        terminal_growth_india=values["terminal_growth_india"],
        terminal_growth_china=values["terminal_growth_china"],
        terminal_growth_japan=values["terminal_growth_japan"],
        future_revenue_growth=values["future_revenue_growth"],
        starting_projected_revenue_growth_cap=values["starting_projected_revenue_growth_cap"],
        ebidta_margin_growth=values["ebidta_margin_growth"],
        da_percent_growth=values["da_percent_growth"],
        capex_percent_growth=values["capex_percent_growth"],
        working_capital_days_growth=values["working_capital_days_growth"],
        wacc_direction=values["wacc_direction"],
        updated_at=datetime.utcnow().isoformat(),
    )
    st.success("General DCF settings saved.")


def _get_effective_industry_settings(group_id: int, general_settings: Dict[str, float], industry_overrides_df: pd.DataFrame) -> Dict[str, float]:
    if industry_overrides_df is None or industry_overrides_df.empty or "group_id" not in industry_overrides_df.columns:
        return dict(general_settings)
    match_df = industry_overrides_df[industry_overrides_df["group_id"] == int(group_id)]
    if match_df.empty:
        return dict(general_settings)
    return _row_to_settings_dict(match_df.iloc[0])


def _render_industry_settings_tab(conn) -> None:
    st.subheader("Industry - Level")

    groups_df = read_df("SELECT id, name FROM company_groups ORDER BY name", conn)
    if groups_df.empty:
        st.info("No industry buckets are available yet.")
        return

    general_settings = _get_general_settings_dict(conn)
    selected_group_ids = st.multiselect(
        "Industries",
        options=groups_df["id"].astype(int).tolist(),
        default=[],
        format_func=lambda group_id: groups_df.loc[groups_df["id"] == group_id, "name"].iloc[0],
        key=_DCF_INDUSTRY_SETTINGS_SELECT_KEY,
        help="Select one or more industries. Saving applies the same shown settings to every selected industry.",
    )
    if not selected_group_ids:
        st.caption("Select one or more industries to view or override their DCF settings.")
        return

    industry_overrides_df = get_dcf_industry_valuation_settings(conn, selected_group_ids)
    selected_groups_df = groups_df[groups_df["id"].isin(selected_group_ids)].copy()
    selected_groups_df["Setting Source"] = selected_groups_df["id"].apply(
        lambda group_id: "Industry Override" if not industry_overrides_df[industry_overrides_df["group_id"] == int(group_id)].empty else "General"
    )
    st.dataframe(
        selected_groups_df.rename(columns={"name": "Industry"})[["Industry", "Setting Source"]],
        use_container_width=True,
        hide_index=True,
    )

    baseline_group_id = int(selected_group_ids[0])
    baseline_settings = _get_effective_industry_settings(baseline_group_id, general_settings, industry_overrides_df)
    baseline_name = selected_groups_df.loc[selected_groups_df["id"] == baseline_group_id, "name"].iloc[0]
    st.caption(f"The form is prefilled from '{baseline_name}'. Saving applies the displayed values to all selected industries.")

    values = _render_settings_form(baseline_settings, "Save Industry Settings", "dcf_industry_settings_form")
    if values is None:
        return

    timestamp = datetime.utcnow().isoformat()
    rows = [
        (
            int(group_id),
            int(values["historical_years"]),
            float(values["terminal_growth_usa"]),
            float(values["terminal_growth_india"]),
            float(values["terminal_growth_china"]),
            float(values["terminal_growth_japan"]),
            float(values["future_revenue_growth"]),
            float(values["starting_projected_revenue_growth_cap"]),
            float(values["ebidta_margin_growth"]),
            float(values["da_percent_growth"]),
            float(values["capex_percent_growth"]),
            float(values["working_capital_days_growth"]),
            float(values["wacc_direction"]),
            timestamp,
        )
        for group_id in selected_group_ids
    ]
    upsert_dcf_industry_valuation_settings(conn, rows)
    st.success(f"Saved industry-level DCF settings for {len(selected_group_ids)} industry bucket(s).")


def _get_effective_company_settings(
    company_id: int,
    general_settings: Dict[str, float],
    industry_overrides_df: pd.DataFrame,
    company_overrides_df: pd.DataFrame,
    memberships_df: pd.DataFrame,
) -> Dict[str, float]:
    if company_overrides_df is not None and not company_overrides_df.empty and "company_id" in company_overrides_df.columns:
        company_match_df = company_overrides_df[company_overrides_df["company_id"] == int(company_id)]
    else:
        company_match_df = pd.DataFrame()
    if not company_match_df.empty:
        return _row_to_settings_dict(company_match_df.iloc[0])

    company_memberships_df = memberships_df[memberships_df["company_id"] == int(company_id)]
    for _, membership in company_memberships_df.iterrows():
        if industry_overrides_df is None or industry_overrides_df.empty or "group_id" not in industry_overrides_df.columns:
            continue
        industry_match_df = industry_overrides_df[industry_overrides_df["group_id"] == int(membership["group_id"])]
        if not industry_match_df.empty:
            return _row_to_settings_dict(industry_match_df.iloc[0])

    return dict(general_settings)


def _get_company_effective_source(
    company_id: int,
    industry_overrides_df: pd.DataFrame,
    company_overrides_df: pd.DataFrame,
    memberships_df: pd.DataFrame,
) -> str:
    if company_overrides_df is not None and not company_overrides_df.empty and "company_id" in company_overrides_df.columns:
        company_match_df = company_overrides_df[company_overrides_df["company_id"] == int(company_id)]
    else:
        company_match_df = pd.DataFrame()
    if not company_match_df.empty:
        return "Company Override"

    company_memberships_df = memberships_df[memberships_df["company_id"] == int(company_id)]
    for _, membership in company_memberships_df.iterrows():
        if industry_overrides_df is None or industry_overrides_df.empty or "group_id" not in industry_overrides_df.columns:
            continue
        industry_match_df = industry_overrides_df[industry_overrides_df["group_id"] == int(membership["group_id"])]
        if not industry_match_df.empty:
            return f"Industry Override: {membership['group_name']}"

    return "General"


def _render_company_settings_tab(conn) -> None:
    st.subheader("Company - Level")

    companies_df = list_companies(conn)
    if companies_df.empty:
        st.info("No companies are available yet.")
        return

    label_map = {
        int(row.id): f"{row.name} ({row.ticker})"
        for _, row in companies_df.iterrows()
    }
    selected_company_ids = st.multiselect(
        "Companies",
        options=companies_df["id"].astype(int).tolist(),
        default=[],
        format_func=lambda company_id: label_map.get(company_id, str(company_id)),
        key=_DCF_COMPANY_SETTINGS_SELECT_KEY,
        help="Select one or more companies. Saving applies the same shown settings to every selected company.",
    )
    if not selected_company_ids:
        st.caption("Select one or more companies to view or override their DCF settings.")
        return

    general_settings = _get_general_settings_dict(conn)
    memberships_df = _get_company_group_memberships(conn, selected_company_ids)
    group_ids = memberships_df["group_id"].astype(int).unique().tolist() if not memberships_df.empty else []
    industry_overrides_df = get_dcf_industry_valuation_settings(conn, group_ids)
    company_overrides_df = get_dcf_company_valuation_settings(conn, selected_company_ids)

    selected_companies_df = companies_df[companies_df["id"].isin(selected_company_ids)].copy()
    selected_companies_df["Setting Source"] = selected_companies_df["id"].apply(
        lambda company_id: _get_company_effective_source(company_id, industry_overrides_df, company_overrides_df, memberships_df)
    )
    st.dataframe(
        selected_companies_df.rename(columns={"name": "Company", "ticker": "Ticker"})[["Company", "Ticker", "Setting Source"]],
        use_container_width=True,
        hide_index=True,
    )

    baseline_company_id = int(selected_company_ids[0])
    baseline_settings = _get_effective_company_settings(
        baseline_company_id,
        general_settings,
        industry_overrides_df,
        company_overrides_df,
        memberships_df,
    )
    baseline_label = label_map.get(baseline_company_id, str(baseline_company_id))
    st.caption(f"The form is prefilled from '{baseline_label}'. Saving applies the displayed values to all selected companies.")

    values = _render_settings_form(baseline_settings, "Save Company Settings", "dcf_company_settings_form")
    if values is None:
        return

    timestamp = datetime.utcnow().isoformat()
    rows = [
        (
            int(company_id),
            int(values["historical_years"]),
            float(values["terminal_growth_usa"]),
            float(values["terminal_growth_india"]),
            float(values["terminal_growth_china"]),
            float(values["terminal_growth_japan"]),
            float(values["future_revenue_growth"]),
            float(values["starting_projected_revenue_growth_cap"]),
            float(values["ebidta_margin_growth"]),
            float(values["da_percent_growth"]),
            float(values["capex_percent_growth"]),
            float(values["working_capital_days_growth"]),
            float(values["wacc_direction"]),
            timestamp,
        )
        for company_id in selected_company_ids
    ]
    upsert_dcf_company_valuation_settings(conn, rows)
    st.success(f"Saved company-level DCF settings for {len(selected_company_ids)} compan(y/ies).")


def _render_settings_tab(conn) -> None:
    tab_general, tab_industry_level, tab_company_level = st.tabs(["General", "Industry - Level", "Company - Level"])
    with tab_general:
        _render_general_settings_tab(conn)
    with tab_industry_level:
        _render_industry_settings_tab(conn)
    with tab_company_level:
        _render_company_settings_tab(conn)


def render_dcf_valuations_tab() -> None:
    st.title("Valuations")

    conn = get_db()
    tab_dcf = st.tabs(["Discounted Cash Flow"])[0]

    with tab_dcf:
        tab_industry, tab_company, tab_settings = st.tabs(["Industry", "Company", "Settings"])

        with tab_industry:
            _render_industry_tab(conn)

        with tab_company:
            _render_company_tab(conn)

        with tab_settings:
            _render_settings_tab(conn)
