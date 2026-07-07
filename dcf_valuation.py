from datetime import datetime
import csv
import io
import json
import re
import time
from typing import Dict, List, Optional, Tuple
import urllib.error
import urllib.parse
import urllib.request

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

from core import (
    calculate_business_quarter_trend_details,
    compute_and_store_cost_of_equity,
    compute_and_store_fcff_and_reinvestment_rate,
    compute_and_store_levered_beta,
    compute_and_store_pre_tax_cost_of_debt,
    compute_and_store_roic_wacc_spread,
    compute_and_store_wacc,
    get_business_quarter_trend_weight_map,
    get_db,
    get_dcf_company_valuation_settings,
    get_dcf_industry_valuation_settings,
    get_dcf_valuation_settings,
    get_quarterly_business_trend_inputs,
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
from db_config import get_db_url, get_sqlite_path, is_sqlite_url
from ui_lazy_tabs import lazy_tab_bar
from ui_theme import company_label_map, dashboard_section, display_table_frame, format_company_option, render_dashboard_table


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
_DCF_COMPANY_DETAIL_VIEW_KEY = "dcf_company_detail_view"
_DCF_COMPANY_ASSUMPTION_UPLOAD_VISIBLE_KEY = "dcf_company_assumption_upload_visible"
_VALUATION_DASHBOARD_RESULTS_KEY = "valuation_dashboard_results"
_VALUATION_DASHBOARD_META_KEY = "valuation_dashboard_results_meta"
_VALUATION_DASHBOARD_SAVE_MODE_KEY = "valuation_dashboard_save_mode"
_VALUATION_DASHBOARD_SAVED_TABLE = "valuation_saved_dashboards"

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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}

_QUOTE_HEADERS = {
    "User-Agent": _NSE_HEADERS["User-Agent"],
    "Accept": "application/json,text/csv,text/plain,text/html;q=0.8,*/*;q=0.5",
    "Accept-Language": "en-US,en;q=0.9",
}

_PROJECTION_PATH_METRICS = [
    ("future_revenue_growth", "Revenue Growth %"),
    ("ebidta_margin_growth", "EBITDA Margin Growth %"),
    ("da_percent_growth", "D&A Percent Growth %"),
    ("capex_percent_growth", "CAPEX Percent Growth %"),
    ("working_capital_days_growth", "Working Capital Days Growth %"),
    ("wacc_direction", "WACC Direction %"),
]

_PREVIEW_METRIC_FORMATS = {
    "future_revenue_growth": "money",
    "ebidta_margin_growth": "percent",
    "da_percent_growth": "percent",
    "capex_percent_growth": "percent",
    "working_capital_days_growth": "days",
    "wacc_direction": "percent",
}

_ANCHOR_CONFIG_KEY = "__anchor_config"
_DEFAULT_FORECAST_YEAR_LIMIT = 10

_ASSUMPTION_UPLOAD_COLUMN_ALIASES = {
    "year": ["year"],
    "future_revenue_growth": ["revenuegrowth", "revenuegrowthpct", "revenuegrowthpercent", "futurerevenuegrowth"],
    "ebidta_margin_growth": ["ebitdamargin", "ebitdamarginpct", "ebitdamarginpercent", "ebidtamargin"],
    "da_percent_growth": ["dapercentofrevenue", "dandapercentofrevenue", "dandaofrevenue", "dapercent", "dandapercent", "dapercentageofrevenue"],
    "capex_percent_growth": ["capexofrevenue", "capexpercentofrevenue", "capexpercent", "capexpercentageofrevenue"],
    "working_capital_days_growth": ["workingcapitaldays", "nwcday", "ncwcdays"],
    "wacc_direction": ["waccpct", "waccpercent", "wacc"],
}

_ASSUMPTION_UPLOAD_FIELD_LABELS = {
    "year": "Year",
    "future_revenue_growth": "Revenue Growth %",
    "ebidta_margin_growth": "EBITDA Margin %",
    "da_percent_growth": "D&A % of Revenue",
    "capex_percent_growth": "CAPEX % of Revenue",
    "working_capital_days_growth": "Working Capital Days",
    "wacc_direction": "WACC %",
}


def _pct_to_decimal(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    return numeric / 100.0 if abs(numeric) > 1.0 else numeric


def _yoy_settings_pct_to_decimal(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value) / 100.0
    except Exception:
        return None


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


def _compute_average_ncwc(ncwc_series: Dict[int, float], year: int) -> Optional[float]:
    current_ncwc = _safe_float(ncwc_series.get(int(year)))
    previous_ncwc = _safe_float(ncwc_series.get(int(year) - 1))
    if current_ncwc is None or previous_ncwc is None:
        return None
    return (float(previous_ncwc) + float(current_ncwc)) / 2.0


def _compute_average_ncwc_days(ncwc_series: Dict[int, float], year: int, revenue: object) -> Optional[float]:
    avg_ncwc = _compute_average_ncwc(ncwc_series, int(year))
    revenue_value = _safe_float(revenue)
    if avg_ncwc is None or revenue_value in (None, 0):
        return None
    return (float(avg_ncwc) * 365.0) / float(revenue_value)


def _compute_growth_for_year(series_map: Dict[int, float], year: int) -> Optional[float]:
    current_value = _safe_float(series_map.get(int(year)))
    previous_value = _safe_float(series_map.get(int(year) - 1))
    if current_value is None or previous_value in (None, 0):
        return None
    return (float(current_value) / float(previous_value)) - 1.0


def _get_company_country(conn, company_id: int, company_row: Optional[pd.Series] = None) -> Optional[str]:
    if company_row is not None and "country" in company_row.index:
        country = company_row.get("country")
        if country is not None and not pd.isna(country) and str(country).strip():
            return str(country).strip()

    df = read_df("SELECT country FROM companies WHERE id = ? LIMIT 1", conn, params=(int(company_id),))
    if df is None or df.empty:
        return None
    country = df.iloc[0].get("country")
    if country is None or pd.isna(country):
        return None
    country_text = str(country).strip()
    return country_text or None


def _parse_projection_path_config(value: object) -> Dict[str, object]:
    if value is None or pd.isna(value):
        return {}
    if isinstance(value, dict):
        return value
    text = str(value).strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _default_single_path_config(step_pct: object) -> Dict[str, object]:
    return {
        "mode": "single",
        "annual_step_pct": _safe_float(step_pct) or 0.0,
        "years": 10,
    }


def _metric_path_config(settings: Dict[str, object], metric_key: str) -> Optional[Dict[str, object]]:
    path_config = _parse_projection_path_config(settings.get("projection_path_config"))
    metric_config = path_config.get(metric_key)
    return metric_config if isinstance(metric_config, dict) else None


def _anchor_config_from_settings(settings: Dict[str, object]) -> Dict[str, object]:
    path_config = _parse_projection_path_config(settings.get("projection_path_config"))
    anchor_config = path_config.get(_ANCHOR_CONFIG_KEY)
    return anchor_config if isinstance(anchor_config, dict) else {}


def _normalize_forecast_year_limit(value: object, default: int = _DEFAULT_FORECAST_YEAR_LIMIT) -> int:
    try:
        return min(max(int(value), 1), 10)
    except Exception:
        return min(max(int(default), 1), 10)


def _forecast_year_limit_from_settings(settings: Dict[str, object]) -> int:
    anchor_config = _anchor_config_from_settings(settings)
    return _normalize_forecast_year_limit(anchor_config.get("forecast_year_limit"), _DEFAULT_FORECAST_YEAR_LIMIT)


def _settings_with_anchor_config(settings: Dict[str, object], anchor_config: Dict[str, object]) -> Dict[str, object]:
    updated_settings = dict(settings)
    path_config = _parse_projection_path_config(updated_settings.get("projection_path_config"))
    path_config[_ANCHOR_CONFIG_KEY] = anchor_config
    updated_settings["projection_path_config"] = json.dumps(path_config)
    return updated_settings


def _anchor_enabled(settings: Dict[str, object]) -> bool:
    return bool(_anchor_config_from_settings(settings).get("use_base_historical_year"))


def _baseline_overrides_enabled(settings: Dict[str, object]) -> bool:
    return bool(_anchor_config_from_settings(settings).get("use_baseline_overrides"))


def _anchor_year_from_settings(settings: Dict[str, object], default_year: int) -> int:
    anchor_config = _anchor_config_from_settings(settings)
    if not anchor_config.get("use_base_historical_year"):
        return int(default_year)
    try:
        return int(anchor_config.get("base_historical_year"))
    except Exception:
        return int(default_year)


def _series_through_year(series_map: Dict[int, float], anchor_year: int) -> Dict[int, float]:
    return {int(year): value for year, value in series_map.items() if int(year) <= int(anchor_year)}


def _baseline_override_decimal(settings: Dict[str, object], metric_key: str) -> Optional[float]:
    anchor_config = _anchor_config_from_settings(settings)
    if not anchor_config.get("use_baseline_overrides"):
        return None
    overrides = anchor_config.get("baseline_overrides")
    if not isinstance(overrides, dict):
        return None
    value = _safe_float(overrides.get(metric_key))
    if value is None:
        return None
    if metric_key == "working_capital_days_growth":
        return float(value)
    return float(value) / 100.0


def _project_assumption_value(
    base_value: float,
    projected_year_index: int,
    metric_key: str,
    settings: Dict[str, object],
    legacy_step: float,
) -> float:
    metric_config = _metric_path_config(settings, metric_key)
    idx = max(int(projected_year_index), 1)
    baseline_override = _baseline_override_decimal(settings, metric_key)
    if baseline_override is not None and metric_key == "future_revenue_growth" and idx == 1:
        return float(baseline_override)
    if not metric_config:
        apply_count = max(idx - 1, 0)
        return float(base_value) * ((1.0 + float(legacy_step)) ** apply_count)

    mode = str(metric_config.get("mode") or "single")
    if mode == "year_by_year":
        raw_steps = metric_config.get("steps_pct") or []
        if not isinstance(raw_steps, list):
            raw_steps = []
        steps = [_yoy_settings_pct_to_decimal(step) or 0.0 for step in raw_steps[:10]]
        while len(steps) < 10:
            steps.append(0.0)
        if metric_key == "future_revenue_growth":
            return float(steps[min(idx, 10) - 1])
        value = float(base_value)
        for step in steps[: min(idx, 10)]:
            value *= 1.0 + float(step)
        return float(value)

    years = metric_config.get("years", 10)
    try:
        years_int = min(max(int(years), 1), 10)
    except Exception:
        years_int = 10
    step = _yoy_settings_pct_to_decimal(metric_config.get("annual_step_pct")) or 0.0
    if metric_key == "future_revenue_growth":
        return float(step)
    apply_count = min(idx, years_int)
    return float(base_value) * ((1.0 + float(step)) ** apply_count)


def _projection_path_growth_rate(metric_config: Dict[str, object], projected_year_index: int) -> float:
    idx = max(int(projected_year_index), 1)
    mode = str(metric_config.get("mode") or "single")
    if mode == "year_by_year":
        raw_steps = metric_config.get("steps_pct") or []
        if not isinstance(raw_steps, list):
            raw_steps = []
        steps = [_yoy_settings_pct_to_decimal(step) or 0.0 for step in raw_steps[:10]]
        while len(steps) < 10:
            steps.append(0.0)
        return float(steps[min(idx, 10) - 1])

    years = metric_config.get("years", 10)
    try:
        years_int = min(max(int(years), 1), 10)
    except Exception:
        years_int = 10
    if idx > years_int:
        return 0.0
    return float(_yoy_settings_pct_to_decimal(metric_config.get("annual_step_pct")) or 0.0)


def _compute_growth_from_values(previous_value: object, current_value: object) -> Optional[float]:
    previous = _safe_float(previous_value)
    current = _safe_float(current_value)
    if current is None or previous in (None, 0):
        return None
    return (float(current) / float(previous)) - 1.0


def _growths_from_values(values: List[object]) -> List[Optional[float]]:
    growths: List[Optional[float]] = []
    previous_value = None
    for value in values:
        growths.append(_compute_growth_from_values(previous_value, value))
        previous_value = value
    return growths


def _median_growth_from_values(values: List[object]) -> Optional[float]:
    growths = [growth for growth in _growths_from_values(values) if growth is not None and pd.notna(growth)]
    return _median(growths)


def _format_preview_value(value: object, value_format: str, country: Optional[str] = None) -> str:
    if value is None or pd.isna(value):
        return ""
    numeric = _safe_float(value)
    if numeric is None:
        return str(value)
    if value_format == "percent":
        return f"{numeric * 100:,.2f}%"
    if value_format == "money":
        unit_label, scale = _country_money_display(country)
        return f"{numeric * scale:,.2f} {unit_label}"
    if value_format == "days":
        return f"{numeric:,.2f} days"
    return f"{numeric:,.2f}"


def _format_preview_pct(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    numeric = _safe_float(value)
    if numeric is None:
        return str(value)
    return f"{numeric * 100:,.2f}%"


def _build_company_assumption_preview_context(
    conn,
    company_row: pd.Series,
    historical_years: int,
    starting_projected_revenue_growth_cap: object,
    settings: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    company_id = int(company_row["id"])
    country = _get_company_country(conn, company_id, company_row)
    series = _build_required_series_for_preview(company_id)
    revenue_series = series["revenue"]
    raw_latest_actual_year, _ = _latest_numeric_value(revenue_series)
    if raw_latest_actual_year is None:
        return {"error": "Missing revenue history for the selected company."}
    anchor_year = _anchor_year_from_settings(settings or {}, int(raw_latest_actual_year))
    if anchor_year > int(raw_latest_actual_year):
        anchor_year = int(raw_latest_actual_year)
    anchor_revenue_series = _series_through_year(revenue_series, anchor_year)
    latest_actual_year, latest_revenue = _latest_numeric_value(anchor_revenue_series)
    if latest_actual_year is None or latest_revenue is None:
        return {"error": "Missing revenue history for the selected company."}

    actual_years = [int(year) for year, _ in _latest_n_values(anchor_revenue_series, historical_years)]
    if not actual_years:
        return {"error": "No annual history is available for the selected company."}

    revenue_values = [_safe_float(revenue_series.get(year)) for year in actual_years]
    ebitda_margin_values = [_compute_series_ratio(series["ebitda"].get(year), revenue_series.get(year)) for year in actual_years]
    da_pct_values = [_compute_series_ratio(series["da"].get(year), revenue_series.get(year)) for year in actual_years]
    capex_pct_values = [
        (
            abs(float(capex_value)) / float(revenue_value)
            if capex_value is not None and revenue_value not in (None, 0)
            else None
        )
        for capex_value, revenue_value in [
            (_safe_float(series["capex"].get(year)), _safe_float(revenue_series.get(year)))
            for year in actual_years
        ]
    ]
    working_capital_days_values = [
        _compute_average_ncwc_days(series["ncwc"], year, revenue_series.get(year))
        for year in actual_years
    ]
    wacc_values = [_pct_to_decimal(_safe_float(series["wacc"].get(year))) for year in actual_years]

    revenue_growth_sample = _latest_n_growths(anchor_revenue_series, historical_years)
    base_revenue_growth = _median(revenue_growth_sample)
    starting_revenue_growth_cap = _pct_to_decimal(starting_projected_revenue_growth_cap)
    if base_revenue_growth is not None and starting_revenue_growth_cap is not None:
        base_revenue_growth = min(float(base_revenue_growth), float(starting_revenue_growth_cap))

    metric_values = {
        "future_revenue_growth": revenue_values,
        "ebidta_margin_growth": ebitda_margin_values,
        "da_percent_growth": da_pct_values,
        "capex_percent_growth": capex_pct_values,
        "working_capital_days_growth": working_capital_days_values,
        "wacc_direction": wacc_values,
    }
    metric_base_values = {
        "future_revenue_growth": base_revenue_growth,
        "ebidta_margin_growth": _median([value for value in ebitda_margin_values if value is not None]),
        "da_percent_growth": _median([value for value in da_pct_values if value is not None]),
        "capex_percent_growth": _median([value for value in capex_pct_values if value is not None]),
        "working_capital_days_growth": _median([value for value in working_capital_days_values if value is not None]),
        "wacc_direction": _median([value for value in wacc_values if value is not None]),
    }
    metric_base_growths = {
        "future_revenue_growth": base_revenue_growth,
        "ebidta_margin_growth": _median_growth_from_values(ebitda_margin_values),
        "da_percent_growth": _median_growth_from_values(da_pct_values),
        "capex_percent_growth": _median_growth_from_values(capex_pct_values),
        "working_capital_days_growth": _median_growth_from_values(working_capital_days_values),
        "wacc_direction": _median_growth_from_values(wacc_values),
    }
    baseline_overrides_for_display: Dict[str, float] = {}
    baseline_anchor_year = None
    if settings:
        anchor_config = _anchor_config_from_settings(settings)
        baseline_anchor_year = anchor_config.get("baseline_anchor_year")
        for metric_key, _ in _PROJECTION_PATH_METRICS:
            override_value = _baseline_override_decimal(settings, metric_key)
            if override_value is None:
                continue
            metric_base_values[metric_key] = override_value
            baseline_overrides_for_display[metric_key] = override_value
            if metric_key == "future_revenue_growth":
                metric_base_growths[metric_key] = override_value

    return {
        "actual_years": actual_years,
        "country": country,
        "latest_actual_year": int(latest_actual_year),
        "projection_start_year": int(latest_actual_year) + 1,
        "latest_revenue": float(latest_revenue),
        "metric_values": metric_values,
        "metric_base_values": metric_base_values,
        "metric_base_growths": metric_base_growths,
        "forecast_year_limit": _forecast_year_limit_from_settings(settings or {}),
        "baseline_anchor_year": baseline_anchor_year,
        "baseline_overrides": baseline_overrides_for_display,
    }


def _settings_with_preview_path_config(
    initial_values: Dict[str, object],
    metric_key: str,
    metric_config: Dict[str, object],
) -> Dict[str, object]:
    settings = dict(initial_values)
    path_config = _parse_projection_path_config(settings.get("projection_path_config"))
    path_config[metric_key] = metric_config
    settings["projection_path_config"] = json.dumps(path_config)
    settings[metric_key] = _path_config_legacy_step(metric_config)
    return settings


def _build_assumption_preview_rows(
    preview_context: Dict[str, object],
    initial_values: Dict[str, object],
    metric_key: str,
    metric_config: Dict[str, object],
) -> pd.DataFrame:
    actual_years = list(preview_context.get("actual_years", []))
    metric_values = dict(preview_context.get("metric_values", {}))
    actual_values = list(metric_values.get(metric_key, []))
    actual_growths = _growths_from_values(actual_values)
    projection_start_year = preview_context.get("projection_start_year")
    forecast_year_limit = _normalize_forecast_year_limit(preview_context.get("forecast_year_limit"), _DEFAULT_FORECAST_YEAR_LIMIT)

    rows: List[Dict[str, object]] = []
    order = 0
    for year, growth, value in zip(actual_years, actual_growths, actual_values):
        rows.append(
            {
                "Period": "Historical",
                "Year/FY": str(year),
                "Growth %": growth,
                "Actual / Projected Value": value,
                "__order": order,
            }
        )
        order += 1

    metric_base_values = dict(preview_context.get("metric_base_values", {}))
    base_value = _safe_float(metric_base_values.get(metric_key))
    if base_value is None:
        return pd.DataFrame(rows)

    preview_settings = _settings_with_preview_path_config(initial_values, metric_key, metric_config)
    legacy_step = _yoy_settings_pct_to_decimal(preview_settings.get(metric_key)) or 0.0

    if metric_key == "future_revenue_growth":
        previous_revenue = _safe_float(preview_context.get("latest_revenue"))
        for idx in range(1, forecast_year_limit + 1):
            projected_growth = _project_assumption_value(float(base_value), idx, metric_key, preview_settings, legacy_step)
            projected_revenue = None
            if previous_revenue is not None:
                projected_revenue = float(previous_revenue) * (1.0 + float(projected_growth))
                previous_revenue = projected_revenue
            rows.append(
                {
                    "Period": "Projected",
                    "Year/FY": f"FY{idx} ({int(projection_start_year) + idx - 1})" if projection_start_year else f"FY{idx}",
                    "Growth %": projected_growth,
                    "Actual / Projected Value": projected_revenue,
                    "__order": order,
                }
            )
            order += 1
        return pd.DataFrame(rows)

    previous_value = actual_values[-1] if actual_values else None
    for idx in range(1, forecast_year_limit + 1):
        projected_value = _project_assumption_value(float(base_value), idx, metric_key, preview_settings, legacy_step)
        projected_growth = _projection_path_growth_rate(metric_config, idx)
        rows.append(
            {
                "Period": "Projected",
                "Year/FY": f"FY{idx} ({int(projection_start_year) + idx - 1})" if projection_start_year else f"FY{idx}",
                "Growth %": projected_growth,
                "Actual / Projected Value": projected_value,
                "__order": order,
            }
        )
        previous_value = projected_value
        order += 1
    return pd.DataFrame(rows)


def _preview_chart_value(value: object, value_format: str, country: Optional[str]) -> Optional[float]:
    numeric = _safe_float(value)
    if numeric is None:
        return None
    if value_format == "percent":
        return float(numeric) * 100.0
    if value_format == "money":
        _, scale = _country_money_display(country)
        return float(numeric) * float(scale)
    return float(numeric)


def _preview_value_axis_title(value_format: str, country: Optional[str]) -> str:
    if value_format == "percent":
        return "Value %"
    if value_format == "money":
        unit_label, _ = _country_money_display(country)
        return f"Value ({unit_label})"
    if value_format == "days":
        return "Value (days)"
    return "Value"


def _preview_year_sort(df: pd.DataFrame) -> List[str]:
    if df.empty or "__order" not in df.columns or "Year/FY" not in df.columns:
        return []
    ordered_labels: List[str] = []
    for _, row in df.sort_values("__order").iterrows():
        label = str(row.get("Year/FY"))
        if label not in ordered_labels:
            ordered_labels.append(label)
    return ordered_labels


def _render_assumption_value_preview_chart(
    preview_df: pd.DataFrame,
    value_format: str,
    country: Optional[str],
) -> None:
    rows: List[Dict[str, object]] = []
    for _, row in preview_df.iterrows():
        raw_value = row.get("Actual / Projected Value")
        chart_value = _preview_chart_value(raw_value, value_format, country)
        if chart_value is None:
            continue
        rows.append(
            {
                "Period": row.get("Period"),
                "Year/FY": row.get("Year/FY"),
                "Value": chart_value,
                "Display": _format_preview_value(raw_value, value_format, country),
                "Growth %": row.get("Growth %"),
                "__order": row.get("__order"),
            }
        )

    if not rows:
        return

    chart_df = pd.DataFrame(rows)
    year_sort = _preview_year_sort(chart_df)
    st.markdown("**Actual / Projected Value Preview**")
    bars = (
        alt.Chart(chart_df)
        .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3, opacity=0.82)
        .encode(
            x=alt.X("Year/FY:N", sort=year_sort, title="Year / Forecast Year"),
            y=alt.Y("Value:Q", title=_preview_value_axis_title(value_format, country)),
            color=alt.Color(
                "Period:N",
                scale=alt.Scale(domain=["Historical", "Projected"], range=["#2563EB", "#16A34A"]),
                legend=alt.Legend(title="Series"),
            ),
            tooltip=[
                alt.Tooltip("Period:N"),
                alt.Tooltip("Year/FY:N"),
                alt.Tooltip("Display:N", title="Actual / Projected Value"),
            ],
        )
    )
    st.altair_chart(bars.properties(height=260), use_container_width=True)


def _render_assumption_base_snapshot(preview_context: Optional[Dict[str, object]]) -> None:
    if not preview_context:
        return
    if preview_context.get("error"):
        st.info(str(preview_context["error"]))
        return

    metric_base_growths = dict(preview_context.get("metric_base_growths", {}))
    metric_base_values = dict(preview_context.get("metric_base_values", {}))
    baseline_overrides = dict(preview_context.get("baseline_overrides", {}))
    baseline_anchor_year = preview_context.get("baseline_anchor_year")
    country = preview_context.get("country")
    rows: List[Dict[str, str]] = []
    for metric_key, label in _PROJECTION_PATH_METRICS:
        value_format = _PREVIEW_METRIC_FORMATS.get(metric_key, "number")
        base_value = preview_context.get("latest_revenue") if metric_key == "future_revenue_growth" else metric_base_values.get(metric_key)
        base_source = str(preview_context.get("latest_actual_year", "")) if metric_key == "future_revenue_growth" else "Historical median"
        if metric_key in baseline_overrides:
            base_source = f"Override: {baseline_anchor_year or ''}".strip()
        rows.append(
            {
                "Assumption Parameter": label,
                "Starting Growth %": _format_preview_pct(metric_base_growths.get(metric_key)),
                "Starting Actual Value": _format_preview_value(base_value, value_format, country),
                "Base Source": base_source,
            }
        )

    st.markdown("**Base Assumption Snapshot**")
    st.caption("Preview source is the first selected company, matching the company used to prefill the form.")
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def _render_assumption_path_preview(
    preview_context: Optional[Dict[str, object]],
    initial_values: Dict[str, object],
    metric_key: str,
    label: str,
    metric_config: Dict[str, object],
) -> None:
    if not preview_context or preview_context.get("error"):
        return

    preview_df = _build_assumption_preview_rows(preview_context, initial_values, metric_key, metric_config)
    if preview_df.empty:
        st.info(f"No historical preview data is available for {label}.")
        return

    st.markdown(f"**{label} Preview**")
    chart_df = preview_df.dropna(subset=["Growth %"]).copy()
    if not chart_df.empty:
        historical_chart_df = chart_df[chart_df["Period"] == "Historical"]
        projected_chart_df = chart_df[chart_df["Period"] == "Projected"]
        if not historical_chart_df.empty and not projected_chart_df.empty:
            bridge_row = historical_chart_df.sort_values("__order").iloc[-1].copy()
            bridge_row["Period"] = "Projected"
            chart_df = pd.concat([chart_df, pd.DataFrame([bridge_row])], ignore_index=True)

        chart_df["Growth %"] = chart_df["Growth %"].astype(float) * 100.0
        year_sort = _preview_year_sort(chart_df)
        chart = (
            alt.Chart(chart_df)
            .mark_line(point=True, strokeWidth=3)
            .encode(
                x=alt.X("Year/FY:N", sort=year_sort, title="Year / Forecast Year"),
                y=alt.Y("Growth %:Q", title="Growth %"),
                color=alt.Color(
                    "Period:N",
                    scale=alt.Scale(domain=["Historical", "Projected"], range=["#2563EB", "#16A34A"]),
                    legend=alt.Legend(title="Series"),
                ),
                tooltip=[
                    alt.Tooltip("Period:N"),
                    alt.Tooltip("Year/FY:N"),
                    alt.Tooltip("Growth %:Q", format=",.2f"),
                ],
            )
            .properties(height=260)
        )
        st.altair_chart(chart, use_container_width=True)

    value_format = _PREVIEW_METRIC_FORMATS.get(metric_key, "number")
    country = preview_context.get("country")
    _render_assumption_value_preview_chart(preview_df, value_format, country)
    display_df = pd.DataFrame(
        [
            {
                "Period": row["Period"],
                "Year/FY": row["Year/FY"],
                "Growth %": _format_preview_pct(row["Growth %"]),
                "Actual / Projected Value": _format_preview_value(row["Actual / Projected Value"], value_format, country),
            }
            for _, row in preview_df.iterrows()
        ]
    )
    st.dataframe(display_df, use_container_width=True, hide_index=True)


def _build_breakdown_table(columns: List[str], rows: List[Tuple[str, str, List[object]]]) -> Dict[str, object]:
    return {
        "columns": [str(column) for column in columns],
        "rows": [
            {"metric": metric, "format": value_format, "values": list(values)}
            for metric, value_format, values in rows
        ],
    }


def _country_money_display(country: Optional[str]) -> Tuple[str, float]:
    normalized = str(country or "").strip().lower()
    if normalized in ("india", "in", "republic of india"):
        return "INR Cr", 0.1
    if normalized in ("usa", "us", "united states", "united states of america"):
        return "USD M", 1.0
    return "M", 1.0


def _relative_ev_display_value(value: object, country: Optional[str]) -> Optional[float]:
    numeric = _safe_float(value)
    if numeric is None:
        return None
    _, scale = _country_money_display(country)
    return float(numeric) * float(scale)


def _relative_ev_display_unit(country: Optional[str]) -> str:
    unit, _ = _country_money_display(country)
    return {
        "INR Cr": "INR Crores",
        "USD M": "USD Millions",
        "M": "Millions",
    }.get(unit, unit)


def _format_breakdown_value(value: object, value_format: str, country: Optional[str] = None) -> str:
    if value is None or pd.isna(value):
        return ""
    if value_format == "text":
        return str(value)

    numeric = _safe_float(value)
    if numeric is None:
        return str(value)

    if value_format == "percent":
        return f"{numeric * 100:,.2f}%"
    if value_format == "money":
        _, scale = _country_money_display(country)
        return f"{numeric * scale:,.2f}"
    if value_format == "decimal4":
        return f"{numeric:,.4f}"
    if value_format == "integer":
        return f"{int(round(numeric)):,}"
    return f"{numeric:,.2f}"


def _render_breakdown_table(title: str, table_payload: Optional[Dict[str, object]], country: Optional[str] = None) -> None:
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
            record[column] = _format_breakdown_value(values[idx] if idx < len(values) else None, value_format, country)
        rendered_rows.append(record)

    has_money_rows = any(str(row.get("format", "number")) == "money" for row in rows)
    if has_money_rows:
        unit_label, _ = _country_money_display(country)
        dashboard_section(f"{title} ({unit_label})")
    else:
        dashboard_section(title)
    render_dashboard_table(
        pd.DataFrame(rendered_rows),
        key=f"dcf_breakdown_{re.sub(r'[^a-z0-9]+', '_', title.lower()).strip('_')}",
    )


def _payload_metric_series(table_payload: Optional[Dict[str, object]], metric_name: str) -> Tuple[List[str], List[object]]:
    if not table_payload:
        return [], []
    columns = [str(column) for column in table_payload.get("columns", [])]
    for row in table_payload.get("rows", []):
        if str(row.get("metric", "")) == metric_name:
            return columns, list(row.get("values", []))
    return [], []


def _payload_metric_value(table_payload: Optional[Dict[str, object]], metric_name: str, column_name: str) -> Optional[object]:
    columns, values = _payload_metric_series(table_payload, metric_name)
    if not columns:
        return None
    try:
        idx = columns.index(str(column_name))
    except ValueError:
        return None
    return values[idx] if idx < len(values) else None


def _fcff_derivation_year_options(detail_payload: Dict[str, object]) -> List[str]:
    operating_table = detail_payload.get("operating_table")
    columns = [str(column) for column in (operating_table or {}).get("columns", [])]
    return [
        column
        for column in columns
        if _payload_metric_value(operating_table, "FCFF", column) is not None
    ]


def _render_metric_tile(label: str, value: object, value_format: str, country: Optional[str], help_text: Optional[str] = None) -> None:
    st.metric(label, _format_breakdown_value(value, value_format, country) or "N/A", help=help_text)


def _signed_money(value: object, country: Optional[str]) -> str:
    numeric = _safe_float(value)
    if numeric is None:
        return "N/A"
    formatted = _format_breakdown_value(abs(float(numeric)), "money", country)
    sign = "+" if float(numeric) >= 0 else "-"
    return f"{sign}{formatted}"


def _render_fcff_equation_strip(
    *,
    year_label: str,
    nopat: object,
    da_value: object,
    capex_signed: object,
    change_ncwc: object,
    fcff: object,
    country: Optional[str],
) -> None:
    capex_abs = abs(float(capex_signed)) if _safe_float(capex_signed) is not None else None
    unit_label, _ = _country_money_display(country)
    st.markdown(
        f"""
<div style="border:1px solid #E5E7EB;border-radius:8px;padding:1rem 1.1rem;margin:0.35rem 0 1rem 0;background:#FFFFFF;">
  <div style="font-size:0.82rem;color:#6B7280;margin-bottom:0.55rem;">FCFF Derivation ({year_label}, {unit_label})</div>
  <div style="font-size:1.05rem;font-weight:700;color:#111827;margin-bottom:0.55rem;">
    FCFF = NOPAT + D&amp;A - CapEx + Change in NCWC
  </div>
  <div style="font-size:1.15rem;color:#111827;line-height:1.8;">
    <strong>{_format_breakdown_value(fcff, "money", country) or "N/A"}</strong>
    =
    {_format_breakdown_value(nopat, "money", country) or "N/A"}
    + {_format_breakdown_value(da_value, "money", country) or "N/A"}
    - {_format_breakdown_value(capex_abs, "money", country) or "N/A"}
    + {_format_breakdown_value(change_ncwc, "money", country) or "N/A"}
  </div>
</div>
""",
        unsafe_allow_html=True,
    )


def _render_fcff_waterfall(
    *,
    year_label: str,
    nopat: object,
    da_value: object,
    capex_signed: object,
    change_ncwc: object,
    fcff: object,
    country: Optional[str],
) -> None:
    components = [
        ("NOPAT", _safe_float(nopat)),
        ("D&A", _safe_float(da_value)),
        ("CapEx", _safe_float(capex_signed)),
        ("Change in NCWC", _safe_float(change_ncwc)),
    ]
    if any(value is None for _, value in components) or _safe_float(fcff) is None:
        st.info("FCFF component visualization is unavailable because one or more component values are missing.")
        return

    running = 0.0
    rows: List[Dict[str, object]] = []
    for label, value in components:
        start = running
        running += float(value)
        rows.append(
            {
                "Component": label,
                "Start": min(start, running),
                "End": max(start, running),
                "Value": float(value),
                "Type": "Increase" if float(value) >= 0 else "Decrease",
                "Display": _signed_money(value, country),
            }
        )
    rows.append(
        {
            "Component": "FCFF",
            "Start": min(0.0, float(_safe_float(fcff) or 0.0)),
            "End": max(0.0, float(_safe_float(fcff) or 0.0)),
            "Value": float(_safe_float(fcff) or 0.0),
            "Type": "Total",
            "Display": _format_breakdown_value(fcff, "money", country),
        }
    )
    chart_df = pd.DataFrame(rows)
    chart = (
        alt.Chart(chart_df)
        .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
        .encode(
            x=alt.X("Component:N", sort=None, title=None),
            y=alt.Y("Start:Q", title=f"{_country_money_display(country)[0]}"),
            y2="End:Q",
            color=alt.Color(
                "Type:N",
                scale=alt.Scale(domain=["Increase", "Decrease", "Total"], range=["#15803D", "#B91C1C", "#1D4ED8"]),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("Component:N"),
                alt.Tooltip("Display:N", title="Value"),
            ],
        )
        .properties(height=260, title=f"FCFF Component Waterfall - {year_label}")
    )
    st.altair_chart(chart, use_container_width=True)


def _render_fcff_timeline(detail_payload: Dict[str, object], country: Optional[str]) -> None:
    operating_table = detail_payload.get("operating_table")
    columns, fcff_values = _payload_metric_series(operating_table, "FCFF")
    if not columns:
        return
    rows = []
    last_historical_row = None
    for idx, column in enumerate(columns):
        value = fcff_values[idx] if idx < len(fcff_values) else None
        numeric = _safe_float(value)
        if numeric is None:
            continue
        period = "Projected" if str(column).startswith("FY") else "Historical"
        row = {
            "Year": str(column),
            "FCFF": float(numeric),
            "Period": period,
            "Display": _format_breakdown_value(value, "money", country),
        }
        rows.append(
            row
        )
        if period == "Historical":
            last_historical_row = row
    if not rows:
        return
    projected_bridge_rows = list(rows)
    if last_historical_row is not None and any(row["Period"] == "Projected" for row in rows):
        projected_bridge_rows.append({**last_historical_row, "Period": "Projected"})

    historical_df = pd.DataFrame([row for row in rows if row["Period"] == "Historical"])
    projected_df = pd.DataFrame([row for row in projected_bridge_rows if row["Period"] == "Projected"])
    point_df = pd.DataFrame(rows)

    historical_line = (
        alt.Chart(historical_df)
        .mark_line(point=False, color="#2563EB")
        .encode(
            x=alt.X("Year:N", sort=None, title=None),
            y=alt.Y("FCFF:Q", title=f"FCFF ({_country_money_display(country)[0]})"),
            tooltip=[
                alt.Tooltip("Year:N"),
                alt.Tooltip("Period:N"),
                alt.Tooltip("Display:N", title="FCFF"),
            ],
        )
    )
    projected_line = (
        alt.Chart(projected_df)
        .mark_line(point=False, color="#F97316")
        .encode(
            x=alt.X("Year:N", sort=None, title=None),
            y=alt.Y("FCFF:Q", title=f"FCFF ({_country_money_display(country)[0]})"),
            tooltip=[
                alt.Tooltip("Year:N"),
                alt.Tooltip("Period:N"),
                alt.Tooltip("Display:N", title="FCFF"),
            ],
        )
    )
    points = (
        alt.Chart(point_df)
        .mark_point(filled=True, size=55)
        .encode(
            x=alt.X("Year:N", sort=None, title=None),
            y=alt.Y("FCFF:Q", title=f"FCFF ({_country_money_display(country)[0]})"),
            color=alt.Color("Period:N", scale=alt.Scale(domain=["Historical", "Projected"], range=["#2563EB", "#F97316"])),
            tooltip=[
                alt.Tooltip("Year:N"),
                alt.Tooltip("Period:N"),
                alt.Tooltip("Display:N", title="FCFF"),
            ],
        )
    )
    chart = (
        (historical_line + projected_line + points)
        .resolve_scale(color="independent")
        .properties(height=260)
    )
    st.altair_chart(chart, use_container_width=True)


def _render_fcff_derivation_view(detail_payload: Dict[str, object], country: Optional[str]) -> None:
    operating_table = detail_payload.get("operating_table")
    working_capital_table = detail_payload.get("working_capital_table")
    year_options = _fcff_derivation_year_options(detail_payload)
    if not year_options:
        st.info("FCFF derivation is unavailable because no FCFF rows are present in the valuation detail.")
        return

    default_idx = len(year_options) - 1
    historical_options = [idx for idx, label in enumerate(year_options) if not str(label).startswith("FY")]
    if historical_options:
        default_idx = historical_options[-1]
    selected_year = st.selectbox(
        "Year",
        options=year_options,
        index=default_idx,
        key="dcf_fcff_derivation_year",
        help="Select a historical or projected year to inspect the FCFF mechanics.",
    )

    revenue = _payload_metric_value(operating_table, "Revenue", selected_year)
    ebitda_margin = _payload_metric_value(operating_table, "EBITDA Margin", selected_year)
    ebitda = _payload_metric_value(operating_table, "EBITDA", selected_year)
    da_value = _payload_metric_value(operating_table, "D&A", selected_year)
    ebit = _payload_metric_value(operating_table, "EBIT", selected_year)
    tax_rate = _payload_metric_value(operating_table, "Tax Rate", selected_year)
    nopat = _payload_metric_value(operating_table, "NOPAT", selected_year)
    capex_signed = _payload_metric_value(operating_table, "CAPEX", selected_year)
    capex_pct = _payload_metric_value(operating_table, "Capex %", selected_year)
    fcff = _payload_metric_value(operating_table, "FCFF", selected_year)
    wc_columns = [str(column) for column in (working_capital_table or {}).get("columns", [])]
    previous_year = None
    try:
        selected_idx = wc_columns.index(str(selected_year))
        previous_year = wc_columns[selected_idx - 1] if selected_idx > 0 else None
    except ValueError:
        previous_year = None
    previous_ncwc = (
        _payload_metric_value(working_capital_table, "Non-Cash Working Capital", previous_year)
        if previous_year is not None
        else None
    )
    ncwc = _payload_metric_value(working_capital_table, "Non-Cash Working Capital", selected_year)
    avg_ncwc = _payload_metric_value(working_capital_table, "Average Non-Cash Working Capital", selected_year)
    wc_days = _payload_metric_value(working_capital_table, "Working Capital Days", selected_year)
    change_ncwc = _payload_metric_value(working_capital_table, "Change in NCWC", selected_year)

    _render_fcff_equation_strip(
        year_label=str(selected_year),
        nopat=nopat,
        da_value=da_value,
        capex_signed=capex_signed,
        change_ncwc=change_ncwc,
        fcff=fcff,
        country=country,
    )

    top_cols = st.columns(4)
    with top_cols[0]:
        _render_metric_tile("Revenue", revenue, "money", country)
    with top_cols[1]:
        _render_metric_tile("EBITDA Margin", ebitda_margin, "percent", country)
    with top_cols[2]:
        _render_metric_tile("CapEx %", capex_pct, "percent", country)
    with top_cols[3]:
        _render_metric_tile("FCFF", fcff, "money", country)

    bridge_col, wc_col = st.columns([1, 1])
    with bridge_col:
        dashboard_section("Operating Profit Bridge")
        bridge_df = pd.DataFrame(
            [
                {"Step": "Revenue", "Value": _format_breakdown_value(revenue, "money", country)},
                {"Step": "EBITDA", "Value": _format_breakdown_value(ebitda, "money", country)},
                {"Step": "D&A", "Value": _format_breakdown_value(da_value, "money", country)},
                {"Step": "EBIT", "Value": _format_breakdown_value(ebit, "money", country)},
                {"Step": "Tax Rate", "Value": _format_breakdown_value(tax_rate, "percent", country)},
                {"Step": "NOPAT", "Value": _format_breakdown_value(nopat, "money", country)},
            ]
        )
        st.dataframe(bridge_df, use_container_width=True, hide_index=True)

    with wc_col:
        dashboard_section("Working Capital Bridge")
        wc_df = pd.DataFrame(
            [
                {"Step": "Prior Non-Cash Working Capital", "Value": _format_breakdown_value(previous_ncwc, "money", country)},
                {"Step": "Non-Cash Working Capital", "Value": _format_breakdown_value(ncwc, "money", country)},
                {"Step": "Average Non-Cash Working Capital", "Value": _format_breakdown_value(avg_ncwc, "money", country)},
                {"Step": "Working Capital Days", "Value": _format_breakdown_value(wc_days, "number", country)},
                {"Step": "Change in NCWC", "Value": _format_breakdown_value(change_ncwc, "money", country)},
            ]
        )
        st.dataframe(wc_df, use_container_width=True, hide_index=True)

    _render_fcff_waterfall(
        year_label=str(selected_year),
        nopat=nopat,
        da_value=da_value,
        capex_signed=capex_signed,
        change_ncwc=change_ncwc,
        fcff=fcff,
        country=country,
    )

    dashboard_section("FCFF Timeline")
    _render_fcff_timeline(detail_payload, country)


def _split_actual_projected_points(columns: List[str], values: List[object]) -> Tuple[List[Tuple[str, float]], List[Tuple[str, float]]]:
    actual_points: List[Tuple[str, float]] = []
    projected_points: List[Tuple[str, float]] = []
    for idx, column in enumerate(columns):
        value = values[idx] if idx < len(values) else None
        numeric = _safe_float(value)
        if numeric is None:
            continue
        if str(column).startswith("FY"):
            projected_points.append((str(column), float(numeric)))
        else:
            actual_points.append((str(column), float(numeric)))
    return actual_points, projected_points


def _extract_year_from_label(label: str) -> Optional[int]:
    match = re.search(r"(?:FY)?(\d{4})", str(label))
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


def _period_label(points: List[Tuple[str, float]]) -> str:
    if not points:
        return "N/A"
    return str(points[0][0]) if len(points) == 1 else f"{points[0][0]}-{points[-1][0]}"


def _period_years(points: List[Tuple[str, float]]) -> Optional[int]:
    if len(points) < 2:
        return None
    start_year = _extract_year_from_label(points[0][0])
    end_year = _extract_year_from_label(points[-1][0])
    if start_year is not None and end_year is not None and end_year > start_year:
        return int(end_year - start_year)
    fallback_years = len(points) - 1
    return fallback_years if fallback_years > 0 else None


def _positive_endpoint_points(points: List[Tuple[str, float]]) -> List[Tuple[str, float]]:
    return [(label, value) for label, value in points if value is not None and pd.notna(value) and float(value) > 0.0]


def _cagr_from_points(points: List[Tuple[str, float]], *, positive_only: bool = True) -> Optional[float]:
    candidate_points = _positive_endpoint_points(points) if positive_only else points
    if len(candidate_points) < 2:
        return None
    start_value = float(candidate_points[0][1])
    end_value = float(candidate_points[-1][1])
    years = _period_years(candidate_points)
    if years is None or years <= 0 or start_value <= 0.0 or end_value <= 0.0:
        return None
    return (end_value / start_value) ** (1.0 / float(years)) - 1.0


def _multiplier_from_points(points: List[Tuple[str, float]], *, positive_only: bool = False) -> Optional[float]:
    candidate_points = _positive_endpoint_points(points) if positive_only else [(label, value) for label, value in points if value is not None and pd.notna(value)]
    if len(candidate_points) < 2:
        return None
    start_value = float(candidate_points[0][1])
    end_value = float(candidate_points[-1][1])
    if start_value == 0.0:
        return None
    return end_value / start_value


def _median_from_points(points: List[Tuple[str, float]]) -> Optional[float]:
    values = [float(value) for _, value in points if value is not None and pd.notna(value)]
    return _median(values)


def _assumption_row_value(detail_payload: Dict[str, object], metric_prefix: str) -> Optional[float]:
    for row in detail_payload.get("assumptions_rows") or []:
        metric = str(row.get("metric", ""))
        if metric.startswith(metric_prefix):
            return _safe_float(row.get("value"))
    return None


def _format_insight_value(value: object, value_format: str) -> str:
    numeric = _safe_float(value)
    if numeric is None:
        return "N/A"
    if value_format == "percent":
        return f"{numeric * 100:,.2f}%"
    if value_format == "multiple":
        return f"{numeric:,.2f}x"
    if value_format == "days":
        return f"{numeric:,.2f} days"
    return f"{numeric:,.2f}"


def _growth_intensity_formula_text() -> str:
    return (
        "For each available metric: normalized delta = (Projected - Historical) / max(abs(Historical), floor). "
        "For D&A %, Capex %, Working Capital Days, and WACC %, the delta is inverted because lower projected values are more valuation-optimistic. "
        "Each delta is capped between -50% and +100%, converted to a 0-100 score as (capped delta + 50%) / 150%, then combined using weighted averages. "
        "Weights: Revenue CAGR 20%, Revenue Multiplier 10%, FCFF CAGR 20%, FCFF Multiplier 10%, EBITDA Margin 15%, D&A % 5%, Capex % 5%, Working Capital Days 5%, WACC % 10%. "
        "N/A metrics are excluded and remaining weights are re-normalized."
    )


def _growth_intensity_classification(score: Optional[float]) -> Tuple[str, str]:
    if score is None:
        return "N/A", "#6B7280"
    if score <= 0.25:
        return "Low", "#64748B"
    if score <= 0.50:
        return "Moderate", "#16A34A"
    if score <= 0.75:
        return "High", "#D97706"
    return "Extremely High", "#DC2626"


def _growth_intensity_floor(metric: str) -> float:
    if "Multiplier" in metric:
        return 0.25
    if "Working Capital Days" in metric:
        return 30.0
    return 0.01


def _growth_intensity_invert(metric: str) -> bool:
    return metric in {"D&A % of Revenue", "Capex % of Revenue", "Working Capital Days", "WACC %"}


def _growth_intensity_weight(metric: str) -> float:
    return {
        "Revenue CAGR": 0.20,
        "Revenue Multiplier": 0.10,
        "FCFF CAGR": 0.20,
        "FCFF Multiplier": 0.10,
        "EBITDA Margin %": 0.15,
        "D&A % of Revenue": 0.05,
        "Capex % of Revenue": 0.05,
        "Working Capital Days": 0.05,
        "WACC %": 0.10,
    }.get(metric, 0.0)


def _growth_intensity_metric_format(metric: str) -> str:
    if "Multiplier" in metric:
        return "multiple"
    if metric == "Working Capital Days":
        return "days"
    return "percent"


def _score_growth_intensity_metric(metric: str, historical: object, projected: object) -> Optional[Dict[str, object]]:
    historical_value = _safe_float(historical)
    projected_value = _safe_float(projected)
    if historical_value is None or projected_value is None:
        return None

    denominator = max(abs(float(historical_value)), _growth_intensity_floor(metric))
    delta = (float(projected_value) - float(historical_value)) / denominator
    if _growth_intensity_invert(metric):
        delta = -delta
    capped_delta = min(max(float(delta), -0.50), 1.00)
    metric_score = (capped_delta + 0.50) / 1.50
    weight = _growth_intensity_weight(metric)
    if weight <= 0:
        return None
    value_format = _growth_intensity_metric_format(metric)
    return {
        "Metric": metric,
        "Historical": historical_value,
        "Projected": projected_value,
        "Delta": delta,
        "Capped Delta": capped_delta,
        "Metric Score": metric_score,
        "Weight": weight,
        "Format": value_format,
        "Historical Display": _format_insight_value(historical_value, value_format),
        "Projected Display": _format_insight_value(projected_value, value_format),
        "Delta Impact": "Positive" if delta > 0.05 else ("Negative" if delta < -0.05 else "Neutral"),
    }


def _build_growth_intensity_summary(
    growth_rows: List[Dict[str, object]],
    multiplier_rows: List[Dict[str, object]],
    median_rows: List[Dict[str, object]],
) -> Tuple[Optional[float], str, str, List[Dict[str, object]]]:
    driver_rows: List[Dict[str, object]] = []
    source_rows = growth_rows + multiplier_rows
    for row in source_rows:
        scored = _score_growth_intensity_metric(str(row.get("Metric", "")), row.get("Historical"), row.get("Projected"))
        if scored:
            driver_rows.append(scored)
    for row in median_rows:
        scored = _score_growth_intensity_metric(str(row.get("Metric", "")), row.get("Historical Median"), row.get("Projected Median"))
        if scored:
            driver_rows.append(scored)

    total_weight = sum(float(row["Weight"]) for row in driver_rows)
    if total_weight <= 0:
        label, color = _growth_intensity_classification(None)
        return None, label, color, []

    weighted_score = sum(float(row["Metric Score"]) * float(row["Weight"]) for row in driver_rows) / total_weight
    for row in driver_rows:
        row["Normalized Weight"] = float(row["Weight"]) / total_weight
        row["Score Contribution"] = float(row["Metric Score"]) * float(row["Normalized Weight"])
    label, color = _growth_intensity_classification(weighted_score)
    return weighted_score, label, color, driver_rows


def _render_growth_intensity_summary(
    growth_rows: List[Dict[str, object]],
    multiplier_rows: List[Dict[str, object]],
    median_rows: List[Dict[str, object]],
) -> None:
    score, label, color, driver_rows = _build_growth_intensity_summary(growth_rows, multiplier_rows, median_rows)

    st.markdown("**Embedded Growth Intensity**", help=_growth_intensity_formula_text())
    score_text = "N/A" if score is None else f"{score * 100:,.0f}%"
    st.markdown(
        f"""
        <div style="border: 1px solid #E5E7EB; border-radius: 12px; padding: 16px; margin-bottom: 12px;">
            <div style="font-size: 0.9rem; color: #6B7280;">Score</div>
            <div style="display: flex; align-items: center; gap: 16px;">
                <div style="font-size: 2rem; font-weight: 700;">{score_text}</div>
                <div style="background: {color}; color: white; border-radius: 999px; padding: 6px 12px; font-weight: 700;">{label}</div>
            </div>
            <div style="height: 10px; background: #E5E7EB; border-radius: 999px; margin-top: 12px; overflow: hidden;">
                <div style="height: 10px; width: {0 if score is None else min(max(score * 100, 0), 100):.0f}%; background: {color};"></div>
            </div>
            <div style="font-size: 0.85rem; color: #6B7280; margin-top: 8px;">
                Low: 0-25% | Moderate: 26-50% | High: 51-75% | Extremely High: 76-100%
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if not driver_rows:
        st.info("Embedded growth intensity could not be calculated because the required insight metrics are unavailable.")
        return

    driver_display = pd.DataFrame(
        [
            {
                "Driver": row["Metric"],
                "Historical": row["Historical Display"],
                "Projected": row["Projected Display"],
                "Normalized Delta": _format_insight_value(row["Delta"], "percent"),
                "Metric Score": f"{float(row['Metric Score']) * 100:,.0f}%",
                "Weight Used": f"{float(row['Normalized Weight']) * 100:,.0f}%",
                "Delta Impact": row["Delta Impact"],
            }
            for row in sorted(driver_rows, key=lambda item: abs(float(item["Score Contribution"])), reverse=True)
        ]
    )
    st.markdown("**Growth Intensity Drivers**")
    st.dataframe(driver_display, use_container_width=True, hide_index=True)

    chart_df = pd.DataFrame(
        [
            {
                "Driver": row["Metric"],
                "Contribution": float(row["Score Contribution"]) * 100.0,
                "Metric Score": f"{float(row['Metric Score']) * 100:,.0f}%",
                "Weight Used": f"{float(row['Normalized Weight']) * 100:,.0f}%",
            }
            for row in driver_rows
        ]
    )
    if not chart_df.empty:
        chart = (
            alt.Chart(chart_df)
            .mark_bar()
            .encode(
                x=alt.X("Driver:N", title=None, axis=alt.Axis(labelAngle=-25)),
                y=alt.Y("Contribution:Q", title="Score Contribution"),
                color=alt.Color("Contribution:Q", scale=alt.Scale(scheme="goldred"), legend=None),
                tooltip=[
                    alt.Tooltip("Driver:N"),
                    alt.Tooltip("Contribution:Q", format=",.2f"),
                    alt.Tooltip("Metric Score:N"),
                    alt.Tooltip("Weight Used:N"),
                ],
            )
            .properties(height=280)
        )
        st.altair_chart(chart, use_container_width=True)


def _insight_chart_df(rows: List[Dict[str, object]], historical_col: str, projected_col: str, value_format: str) -> pd.DataFrame:
    chart_rows: List[Dict[str, object]] = []
    for row in rows:
        for series_label, col_name in (("Historical", historical_col), ("Projected", projected_col)):
            value = _safe_float(row.get(col_name))
            if value is None:
                continue
            row_format = str(row.get("Format", value_format))
            chart_value = value * 100.0 if row_format == "percent" else value
            chart_rows.append(
                {
                    "Metric": row.get("Metric"),
                    "Series": series_label,
                    "Value": chart_value,
                    "Display": _format_insight_value(value, row_format),
                }
            )
    return pd.DataFrame(chart_rows)


def _render_insight_bar_chart(rows: List[Dict[str, object]], historical_col: str, projected_col: str, value_format: str, y_title: str) -> None:
    chart_df = _insight_chart_df(rows, historical_col, projected_col, value_format)
    if chart_df.empty:
        return
    chart = (
        alt.Chart(chart_df)
        .mark_bar()
        .encode(
            x=alt.X("Metric:N", title=None, axis=alt.Axis(labelAngle=-20)),
            xOffset=alt.XOffset("Series:N"),
            y=alt.Y("Value:Q", title=y_title),
            color=alt.Color(
                "Series:N",
                scale=alt.Scale(domain=["Historical", "Projected"], range=["#2563EB", "#16A34A"]),
                legend=alt.Legend(title="Series"),
            ),
            tooltip=[
                alt.Tooltip("Metric:N"),
                alt.Tooltip("Series:N"),
                alt.Tooltip("Display:N", title="Value"),
            ],
        )
        .properties(height=280)
    )
    st.altair_chart(chart, use_container_width=True)


def _valuation_insight_rows(detail_payload: Dict[str, object]) -> Tuple[List[Dict[str, object]], List[Dict[str, object]], List[Dict[str, object]]]:
    operating_table = detail_payload.get("operating_table")
    working_capital_table = detail_payload.get("working_capital_table")
    discounting_table = detail_payload.get("discounting_table")

    revenue_columns, revenue_values = _payload_metric_series(operating_table, "Revenue")
    revenue_actual, revenue_projected = _split_actual_projected_points(revenue_columns, revenue_values)
    fcff_columns, fcff_values = _payload_metric_series(operating_table, "FCFF")
    fcff_actual, fcff_projected = _split_actual_projected_points(fcff_columns, fcff_values)

    growth_rows = [
        {
            "Metric": "Revenue CAGR",
            "Historical Period": _period_label(revenue_actual),
            "Historical": _cagr_from_points(revenue_actual, positive_only=True),
            "Projected Period": _period_label(revenue_projected),
            "Projected": _cagr_from_points(revenue_projected, positive_only=True),
        },
        {
            "Metric": "FCFF CAGR",
            "Historical Period": _period_label(_positive_endpoint_points(fcff_actual)),
            "Historical": _cagr_from_points(fcff_actual, positive_only=True),
            "Projected Period": _period_label(_positive_endpoint_points(fcff_projected)),
            "Projected": _cagr_from_points(fcff_projected, positive_only=True),
        },
    ]
    multiplier_rows = [
        {
            "Metric": "Revenue Multiplier",
            "Historical Period": _period_label(revenue_actual),
            "Historical": _multiplier_from_points(revenue_actual),
            "Projected Period": _period_label(revenue_projected),
            "Projected": _multiplier_from_points(revenue_projected),
        },
        {
            "Metric": "FCFF Multiplier",
            "Historical Period": _period_label(_positive_endpoint_points(fcff_actual)),
            "Historical": _multiplier_from_points(fcff_actual, positive_only=True),
            "Projected Period": _period_label(_positive_endpoint_points(fcff_projected)),
            "Projected": _multiplier_from_points(fcff_projected, positive_only=True),
        },
    ]

    median_specs = [
        ("EBITDA Margin %", operating_table, "EBITDA Margin", "percent"),
        ("D&A % of Revenue", operating_table, "D&A %", "percent"),
        ("Capex % of Revenue", operating_table, "Capex %", "percent"),
        ("Working Capital Days", working_capital_table, "Working Capital Days", "days"),
    ]
    median_rows: List[Dict[str, object]] = []
    for label, table_payload, metric_name, value_format in median_specs:
        columns, values = _payload_metric_series(table_payload, metric_name)
        actual_points, projected_points = _split_actual_projected_points(columns, values)
        median_rows.append(
            {
                "Metric": label,
                "Historical Median": _median_from_points(actual_points),
                "Projected Median": _median_from_points(projected_points),
                "Format": value_format,
            }
        )

    wacc_columns, wacc_values = _payload_metric_series(discounting_table, "Projected Year WACC")
    _, wacc_projected = _split_actual_projected_points(wacc_columns, wacc_values)
    median_rows.append(
        {
            "Metric": "WACC %",
            "Historical Median": _assumption_row_value(detail_payload, "Median WACC"),
            "Projected Median": _median_from_points(wacc_projected),
            "Format": "percent",
        }
    )
    return growth_rows, multiplier_rows, median_rows


def _render_company_valuation_insights(detail_payload: Dict[str, object]) -> None:
    if not detail_payload or not detail_payload.get("operating_table"):
        st.info("Valuation insights are not available for this company in the current run.")
        return

    company_name = str(detail_payload.get("company_name") or "Selected Company")
    ticker = str(detail_payload.get("ticker") or "").strip()
    label = f"{company_name} ({ticker})" if ticker else company_name
    dashboard_section(f"Valuation Insights: {label}")

    growth_rows, multiplier_rows, median_rows = _valuation_insight_rows(detail_payload)
    _render_growth_intensity_summary(growth_rows, multiplier_rows, median_rows)

    dashboard_section("Growth Snapshot")
    growth_display = pd.DataFrame(
        [
            {
                "Metric": row["Metric"],
                "Historical Period": row["Historical Period"],
                "Historical": _format_insight_value(row["Historical"], "percent"),
                "Projected Period": row["Projected Period"],
                "Projected": _format_insight_value(row["Projected"], "percent"),
            }
            for row in growth_rows
        ]
    )
    render_dashboard_table(growth_display, key="dcf_insights_growth_snapshot")
    _render_insight_bar_chart(growth_rows, "Historical", "Projected", "percent", "CAGR %")

    dashboard_section("Multiplier Snapshot")
    multiplier_display = pd.DataFrame(
        [
            {
                "Metric": row["Metric"],
                "Historical Period": row["Historical Period"],
                "Historical": _format_insight_value(row["Historical"], "multiple"),
                "Projected Period": row["Projected Period"],
                "Projected": _format_insight_value(row["Projected"], "multiple"),
            }
            for row in multiplier_rows
        ]
    )
    render_dashboard_table(multiplier_display, key="dcf_insights_multiplier_snapshot")
    _render_insight_bar_chart(multiplier_rows, "Historical", "Projected", "multiple", "Multiplier")

    dashboard_section("Median Assumption Snapshot")
    median_display = pd.DataFrame(
        [
            {
                "Metric": row["Metric"],
                "Historical Median": _format_insight_value(row["Historical Median"], str(row.get("Format", "number"))),
                "Projected Median": _format_insight_value(row["Projected Median"], str(row.get("Format", "number"))),
            }
            for row in median_rows
        ]
    )
    render_dashboard_table(median_display, key="dcf_insights_median_snapshot")
    _render_insight_bar_chart(median_rows, "Historical Median", "Projected Median", "mixed", "Value (% or days)")


def _render_company_valuation_detail(detail_payload: Dict[str, object]) -> None:
    country = detail_payload.get("country")
    summary_rows = detail_payload.get("summary_rows") or []
    assumptions_rows = detail_payload.get("assumptions_rows") or []

    summary_df = pd.DataFrame(
        [
            {
                "Metric": row.get("metric", ""),
                "Value": _format_breakdown_value(row.get("value"), str(row.get("format", "text")), country),
            }
            for row in summary_rows
        ]
    )
    assumptions_df = pd.DataFrame(
        [
            {
                "Input": row.get("metric", ""),
                "Value": _format_breakdown_value(row.get("value"), str(row.get("format", "text")), country),
            }
            for row in assumptions_rows
        ]
    )

    fcff_tab, summary_tab, raw_tab = st.tabs(["FCFF Derivation", "Summary & Assumptions", "Raw Tables"])

    with fcff_tab:
        _render_fcff_derivation_view(detail_payload, country)

    with summary_tab:
        summary_col, assumption_col = st.columns([1, 1])
        with summary_col:
            dashboard_section("Summary")
            if not summary_df.empty:
                render_dashboard_table(summary_df, key="dcf_expanded_summary")
        with assumption_col:
            dashboard_section("Assumptions")
            if not assumptions_df.empty:
                render_dashboard_table(assumptions_df, key="dcf_expanded_assumptions")

    with raw_tab:
        _render_breakdown_table("Operating Model", detail_payload.get("operating_table"), country)
        _render_breakdown_table("Working Capital Bridge", detail_payload.get("working_capital_table"), country)
        _render_breakdown_table("Discounting and Equity Value", detail_payload.get("discounting_table"), country)


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


def _quote_failure(detail: str) -> Dict[str, object]:
    return {"price": None, "as_of": None, "source": "Unavailable", "detail": detail}


def _is_live_quote(result: Optional[Dict[str, object]]) -> bool:
    if not result or result.get("price") is None:
        return False
    try:
        return pd.notna(float(result.get("price")))
    except Exception:
        return False


def _normalize_quote_ticker(ticker: str) -> str:
    clean_ticker = str(ticker or "").strip().upper()
    if ":" in clean_ticker:
        exchange, symbol = clean_ticker.split(":", 1)
        if exchange.strip() in {"NSE", "BSE", "NYSE", "NASDAQ", "AMEX", "OTC"}:
            clean_ticker = symbol.strip()
    return clean_ticker


def _build_quote_symbol(ticker: str, country: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    clean_ticker = _normalize_quote_ticker(ticker)
    country_key = _normalize_country_for_quotes(country)
    if not clean_ticker:
        return None, "Ticker is blank."
    if country_key in {"india", "in"}:
        return clean_ticker, None
    if country_key in {"usa", "us", "united states", "united states of america"}:
        return f"{clean_ticker}.US", None
    return None, f"Live quote provider is not configured for country '{country or 'Unknown'}'."


def _build_yahoo_quote_symbols(ticker: str, country: Optional[str]) -> List[str]:
    clean_ticker = _normalize_quote_ticker(ticker)
    if not clean_ticker:
        return []

    country_key = _normalize_country_for_quotes(country)
    if country_key in {"india", "in"}:
        if clean_ticker.endswith((".NS", ".BO")):
            return [clean_ticker]
        if str(ticker or "").strip().upper().startswith("BSE:"):
            return [f"{clean_ticker}.BO", f"{clean_ticker}.NS"]
        return [f"{clean_ticker}.NS", f"{clean_ticker}.BO"]

    if country_key in {"usa", "us", "united states", "united states of america"}:
        if clean_ticker.endswith(".US"):
            clean_ticker = clean_ticker[:-3]
        return [clean_ticker.replace(".", "-")]

    return []


def _open_url_with_retries(url: str, *, headers: Optional[Dict[str, str]] = None, timeout: int = 15, retries: int = 2) -> Tuple[str, str]:
    last_exc: Optional[Exception] = None
    for attempt in range(max(int(retries), 1)):
        try:
            req = urllib.request.Request(url, headers=headers or _QUOTE_HEADERS)
            with urllib.request.urlopen(req, timeout=timeout) as response:
                return response.read().decode("utf-8", errors="replace"), str(response.getheader("content-type") or "")
        except Exception as exc:
            last_exc = exc
            if attempt < max(int(retries), 1) - 1:
                time.sleep(0.5 * (attempt + 1))
    if isinstance(last_exc, urllib.error.HTTPError):
        raise last_exc
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("Request failed.")


def _format_yahoo_as_of(meta: Dict[str, object]) -> str:
    market_time = meta.get("regularMarketTime")
    if market_time is None or pd.isna(market_time):
        return "Fetched now"
    try:
        ts = pd.to_datetime(float(market_time), unit="s", utc=True)
        exchange_tz = str(meta.get("exchangeTimezoneName") or "").strip()
        if exchange_tz:
            ts = ts.tz_convert(exchange_tz)
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "Fetched now"


def _last_numeric_from_chart(result: Dict[str, object]) -> Optional[float]:
    quote_rows = (result.get("indicators") or {}).get("quote") or []
    if not quote_rows:
        return None
    closes = quote_rows[0].get("close") or []
    for value in reversed(closes):
        if value is None or pd.isna(value):
            continue
        try:
            return float(value)
        except Exception:
            continue
    return None


def _fetch_yahoo_chart_quote(symbol: str) -> Dict[str, object]:
    encoded_symbol = urllib.parse.quote(str(symbol or "").strip(), safe="")
    if not encoded_symbol:
        return _quote_failure("Yahoo quote symbol is blank.")
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded_symbol}?range=1d&interval=1m"
    try:
        raw, content_type = _open_url_with_retries(url, timeout=15, retries=2)
    except urllib.error.HTTPError as exc:
        return _quote_failure(f"Yahoo chart quote request failed for {symbol}: HTTP {exc.code}.")
    except Exception as exc:
        return _quote_failure(f"Yahoo chart quote request failed for {symbol}: {type(exc).__name__}.")

    if "json" not in content_type.lower() and raw.lstrip().startswith("<"):
        return _quote_failure(f"Yahoo chart quote returned an HTML verification page for {symbol}.")

    try:
        payload = json.loads(raw)
    except Exception:
        return _quote_failure(f"Yahoo chart quote returned non-JSON data for {symbol}.")

    chart = payload.get("chart") or {}
    error = chart.get("error")
    if error:
        description = error.get("description") if isinstance(error, dict) else str(error)
        return _quote_failure(f"Yahoo chart quote was unavailable for {symbol}: {description}.")

    results = chart.get("result") or []
    if not results:
        return _quote_failure(f"Yahoo chart quote did not include data for {symbol}.")

    result = results[0]
    meta = result.get("meta") or {}
    price = meta.get("regularMarketPrice")
    if price is None or pd.isna(price):
        price = _last_numeric_from_chart(result)
    if price is None or pd.isna(price):
        return _quote_failure(f"Yahoo chart quote did not include a current price for {symbol}.")

    return {
        "price": float(price),
        "as_of": _format_yahoo_as_of(meta),
        "source": "Live",
        "detail": f"Live quote fetched from Yahoo Finance chart for {symbol}.",
    }


def _build_nse_opener():
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor())
    opener.addheaders = list(_NSE_HEADERS.items())
    opener.open("https://www.nseindia.com/market-data/live-equity-market?symbol=NIFTY%2050", timeout=15).read()
    return opener


def _fetch_nse_quote(symbol: str, opener=None) -> Dict[str, object]:
    try:
        local_opener = opener or _build_nse_opener()
        encoded_symbol = urllib.parse.quote(str(symbol or "").strip().upper(), safe="")
        req = urllib.request.Request(
            f"https://www.nseindia.com/api/quote-equity?symbol={encoded_symbol}",
            headers={**_NSE_HEADERS, "Referer": f"https://www.nseindia.com/get-quotes/equity?symbol={encoded_symbol}"},
        )
        with local_opener.open(req, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return _quote_failure(f"NSE quote request failed: HTTP {exc.code}.")
    except Exception as exc:
        return _quote_failure(f"NSE quote request failed: {type(exc).__name__}.")

    price = payload.get("priceInfo", {}).get("lastPrice")
    if price is None or pd.isna(price):
        return _quote_failure("NSE quote did not include a live last price.")

    as_of = payload.get("metadata", {}).get("lastUpdateTime") or "Fetched now"
    return {"price": float(price), "as_of": str(as_of), "source": "Live", "detail": f"Live quote fetched from NSE for {symbol}."}


def _fetch_stooq_quote(symbol: str) -> Dict[str, object]:
    try:
        encoded_symbol = urllib.parse.quote(str(symbol or "").strip().lower(), safe="")
        raw, content_type = _open_url_with_retries(f"https://stooq.com/q/l/?s={encoded_symbol}&i=d", timeout=15, retries=2)
        raw = raw.strip()
    except urllib.error.HTTPError as exc:
        return _quote_failure(f"Stooq quote request failed: HTTP {exc.code}.")
    except Exception as exc:
        return _quote_failure(f"Stooq quote request failed: {type(exc).__name__}.")

    if not raw:
        return _quote_failure("Stooq quote response was empty.")
    if "html" in content_type.lower() or raw.lstrip().startswith("<"):
        return _quote_failure("Stooq quote returned an HTML verification page.")

    rows = list(csv.DictReader(io.StringIO(raw)))
    if not rows:
        return _quote_failure("Stooq quote response did not include a data row.")
    row = rows[0]
    if str(row.get("Symbol") or "").endswith("N/D"):
        return _quote_failure("Stooq quote was unavailable for this symbol.")

    try:
        price = float(row.get("Close"))
    except Exception:
        return _quote_failure("Stooq returned a non-numeric close price.")

    as_of = None
    try:
        as_of = pd.to_datetime(f"{row.get('Date')} {row.get('Time')}", format="%Y%m%d %H%M%S", errors="coerce")
        as_of = None if pd.isna(as_of) else as_of.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        as_of = None
    return {"price": price, "as_of": as_of or "Fetched now", "source": "Live", "detail": f"Live quote fetched from Stooq for {symbol}."}


def _fetch_live_quote_for_company(company_row: pd.Series, nse_opener=None) -> Dict[str, object]:
    ticker = str(company_row.get("ticker") or "").strip().upper()
    country = company_row.get("country")
    symbol, symbol_error = _build_quote_symbol(ticker, country)
    if symbol_error:
        return _quote_failure(symbol_error)

    attempts: List[Dict[str, object]] = []
    country_key = _normalize_country_for_quotes(country)
    if country_key in {"india", "in"}:
        for yahoo_symbol in _build_yahoo_quote_symbols(ticker, country):
            result = _fetch_yahoo_chart_quote(yahoo_symbol)
            if _is_live_quote(result):
                return result
            attempts.append(result)
        result = _fetch_nse_quote(symbol, opener=nse_opener)
        if _is_live_quote(result):
            return result
        attempts.append(result)
        details = " | ".join(str(item.get("detail") or "").strip() for item in attempts if item.get("detail"))
        return _quote_failure(details or "All India live quote providers failed.")

    if country_key in {"usa", "us", "united states", "united states of america"}:
        for yahoo_symbol in _build_yahoo_quote_symbols(ticker, country):
            result = _fetch_yahoo_chart_quote(yahoo_symbol)
            if _is_live_quote(result):
                return result
            attempts.append(result)
        result = _fetch_stooq_quote(symbol)
        if _is_live_quote(result):
            return result
        attempts.append(result)
        details = " | ".join(str(item.get("detail") or "").strip() for item in attempts if item.get("detail"))
        return _quote_failure(details or "All USA live quote providers failed.")

    return _quote_failure(f"No live quote provider configured for country '{country or 'Unknown'}'.")


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


def _preview_cache_token() -> str:
    db_url = get_db_url()
    if is_sqlite_url(db_url):
        db_path = get_sqlite_path()
        try:
            stat = db_path.stat()
            return f"sqlite:{db_path.resolve()}:{stat.st_mtime_ns}:{stat.st_size}"
        except Exception:
            return f"sqlite:{db_path}"
    return f"uncached:{time.time_ns()}"


@st.cache_data(show_spinner=False)
def _cached_required_series_for_preview(company_id: int, cache_token: str) -> Dict[str, Dict[int, float]]:
    conn = get_db()
    return _build_required_series(conn, int(company_id))


def _build_required_series_for_preview(company_id: int) -> Dict[str, Dict[int, float]]:
    return _cached_required_series_for_preview(int(company_id), _preview_cache_token())


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
    compute_overall_score: bool = True,
) -> Dict[str, object]:
    company_id = int(company_row["id"])
    company_name = str(company_row["name"])
    ticker = str(company_row["ticker"])
    country = company_row.get("country")
    series = _build_required_series(conn, company_id)
    revenue_series = series["revenue"]
    raw_latest_actual_year, raw_latest_revenue = _latest_numeric_value(revenue_series)

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

    if raw_latest_actual_year is None or raw_latest_revenue is None:
        return _fail("Missing metric: Revenue")

    latest_actual_year = _anchor_year_from_settings(settings, int(raw_latest_actual_year))
    if int(latest_actual_year) > int(raw_latest_actual_year):
        latest_actual_year = int(raw_latest_actual_year)
    anchored_revenue_series = _series_through_year(revenue_series, int(latest_actual_year))
    latest_actual_year, latest_revenue = _latest_numeric_value(anchored_revenue_series)
    if latest_actual_year is None or latest_revenue is None:
        return _fail("Missing metric: Revenue for selected historical anchor")

    explicit_final_year = int(terminal_year) - 1
    if explicit_final_year <= int(latest_actual_year):
        return _fail(f"Terminal year {terminal_year} must be later than latest actual year {latest_actual_year}.")

    historical_years = int(settings.get("historical_years", 7) or 7)
    revenue_growth_sample = _latest_n_growths(anchored_revenue_series, historical_years)
    if not revenue_growth_sample:
        return _fail("Missing metric: Revenue growth history")

    recent_revenue_values = _latest_n_values(anchored_revenue_series, historical_years)
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
        days
        for year, revenue in recent_revenue_values
        for days in [_compute_average_ncwc_days(series["ncwc"], int(year), revenue)]
        if days is not None
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

    revenue_growth_step = _yoy_settings_pct_to_decimal(settings.get("future_revenue_growth")) or 0.0
    starting_revenue_growth_cap = _pct_to_decimal(settings.get("starting_projected_revenue_growth_cap"))
    ebitda_margin_step = _yoy_settings_pct_to_decimal(settings.get("ebidta_margin_growth")) or 0.0
    da_pct_step = _yoy_settings_pct_to_decimal(settings.get("da_percent_growth")) or 0.0
    capex_pct_step = _yoy_settings_pct_to_decimal(settings.get("capex_percent_growth")) or 0.0
    working_capital_step = _yoy_settings_pct_to_decimal(settings.get("working_capital_days_growth")) or 0.0
    wacc_step = _yoy_settings_pct_to_decimal(settings.get("wacc_direction")) or 0.0

    if starting_revenue_growth_cap is not None:
        revenue_growth = min(float(revenue_growth), float(starting_revenue_growth_cap))
    base_revenue_growth = float(revenue_growth)
    base_ebitda_margin = float(ebitda_margin)
    base_da_pct = float(da_pct)
    base_capex_pct = float(capex_pct)
    base_working_capital_days = float(working_capital_days)
    base_wacc = float(wacc)
    base_overrides = {
        metric_key: _baseline_override_decimal(settings, metric_key)
        for metric_key, _ in _PROJECTION_PATH_METRICS
    }
    if base_overrides["future_revenue_growth"] is not None:
        base_revenue_growth = float(base_overrides["future_revenue_growth"])
    if base_overrides["ebidta_margin_growth"] is not None:
        base_ebitda_margin = float(base_overrides["ebidta_margin_growth"])
    if base_overrides["da_percent_growth"] is not None:
        base_da_pct = float(base_overrides["da_percent_growth"])
    if base_overrides["capex_percent_growth"] is not None:
        base_capex_pct = float(base_overrides["capex_percent_growth"])
    if base_overrides["working_capital_days_growth"] is not None:
        base_working_capital_days = float(base_overrides["working_capital_days_growth"])
    if base_overrides["wacc_direction"] is not None:
        base_wacc = float(base_overrides["wacc_direction"])

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
    projected_avg_ncwcs: List[float] = []
    projected_wc_days_values: List[float] = []
    projected_change_ncwcs: List[float] = []
    prev_revenue = float(latest_revenue)
    prev_ncwc = series["ncwc"].get(int(latest_actual_year))
    if prev_ncwc is None:
        return _fail("Missing metric: Latest non-cash working capital")
    prev_ncwc = float(prev_ncwc)

    projection_years = [int(year) for year in range(int(latest_actual_year) + 1, explicit_final_year + 1)]
    for idx, year in enumerate(projection_years, start=1):
        revenue_growth = _project_assumption_value(base_revenue_growth, idx, "future_revenue_growth", settings, revenue_growth_step)
        ebitda_margin = _project_assumption_value(base_ebitda_margin, idx, "ebidta_margin_growth", settings, ebitda_margin_step)
        da_pct = _project_assumption_value(base_da_pct, idx, "da_percent_growth", settings, da_pct_step)
        capex_pct = _project_assumption_value(base_capex_pct, idx, "capex_percent_growth", settings, capex_pct_step)
        working_capital_days = _project_assumption_value(
            base_working_capital_days,
            idx,
            "working_capital_days_growth",
            settings,
            working_capital_step,
        )
        wacc = _project_assumption_value(base_wacc, idx, "wacc_direction", settings, wacc_step)

        revenue = float(prev_revenue) * (1.0 + float(revenue_growth))
        ebitda = float(revenue) * float(ebitda_margin)
        da_value = float(revenue) * float(da_pct)
        ebit = float(ebitda) - float(da_value)
        nopat = float(ebit) * (1.0 - float(tax_rate))
        capex_signed = -float(revenue) * float(capex_pct)
        avg_ncwc = (float(working_capital_days) * float(revenue)) / 365.0
        ncwc = (2.0 * float(avg_ncwc)) - float(prev_ncwc)
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
        projected_avg_ncwcs.append(float(avg_ncwc))
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
    if compute_overall_score:
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
    actual_avg_ncwc_values = [_compute_average_ncwc(series["ncwc"], year) for year in actual_years]
    actual_wc_days_values = [
        _compute_average_ncwc_days(series["ncwc"], year, revenue_value)
        for year, revenue_value in zip(actual_years, actual_revenue_values)
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
        {"metric": "Historical Anchor Year", "value": str(int(latest_actual_year)), "format": "text"},
        {"metric": "Projection FY1 Year", "value": str(int(latest_actual_year) + 1), "format": "text"},
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
            ("Revenue", "money", actual_revenue_values + projected_revenues),
            ("Growth", "percent", actual_growth_values + projected_growths),
            ("EBITDA", "money", actual_ebitda_values + projected_ebitdas),
            ("EBITDA Margin", "percent", actual_ebitda_margin_values + projected_ebitda_margins),
            ("D&A", "money", actual_da_values + projected_da_values),
            ("D&A %", "percent", actual_da_pct_values + projected_da_pcts),
            ("EBIT", "money", actual_ebit_values + projected_ebits),
            ("Tax Rate", "percent", actual_tax_rate_values + ([float(tax_rate)] * len(projection_years))),
            ("NOPAT", "money", actual_nopat_values + projected_nopats),
            ("CAPEX", "money", actual_capex_outflows + projected_capex_signeds),
            ("Capex %", "percent", actual_capex_pct_values + projected_capex_pcts),
            ("FCFF", "money", actual_fcff_values + projected_fcff),
        ],
    )
    detail_payload["working_capital_table"] = _build_breakdown_table(
        actual_columns + projected_columns,
        [
            ("Total Current Assets", "money", actual_total_current_assets + ([None] * len(projection_years))),
            ("Cash & Cash Equivalents", "money", actual_cash_values + ([None] * len(projection_years))),
            ("Net Current Assets", "money", actual_net_current_assets + ([None] * len(projection_years))),
            ("Total Current Liabilities", "money", actual_total_current_liabilities + ([None] * len(projection_years))),
            ("Current Debt", "money", actual_current_debt + ([None] * len(projection_years))),
            ("Net Current Liabilities", "money", actual_net_current_liabilities + ([None] * len(projection_years))),
            ("Non-Cash Working Capital", "money", actual_ncwc_values + projected_ncwcs),
            ("Average Non-Cash Working Capital", "money", actual_avg_ncwc_values + projected_avg_ncwcs),
            ("Working Capital Days", "number", actual_wc_days_values + projected_wc_days_values),
            ("Change in NCWC", "money", actual_change_ncwcs + projected_change_ncwcs),
        ],
    )
    detail_payload["discounting_table"] = _build_breakdown_table(
        discounting_columns,
        [
            ("Future Year", "integer", list(range(1, len(projection_years) + 1)) + [None]),
            ("Projected Year WACC", "percent", projected_waccs + [None]),
            ("Discount Factor", "decimal4", discount_factors + [None]),
            ("FCFF", "money", projected_fcff + [fcff_terminal_year]),
            ("PV of FCFF", "money", pv_fcff_values + [None]),
            ("Terminal Value", "money", ([None] * len(projection_years)) + [terminal_value]),
            ("PV of Terminal Value", "money", ([None] * len(projection_years)) + [pv_terminal_value]),
            ("Enterprise Value", "money", ([None] * len(projection_years)) + [enterprise_value]),
            ("Less: Debt", "money", ([None] * len(projection_years)) + [-(float(latest_debt or 0.0))]),
            ("Plus: Cash & Cash Equivalents", "money", ([None] * len(projection_years)) + [float(latest_cash or 0.0)]),
            ("Equity Value", "money", ([None] * len(projection_years)) + [equity_value]),
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

    display_visible_df = display_table_frame(visible_df)
    styled_df = (
        display_visible_df.style.map(_style_price_source, subset=["Price Source"])
        if "Price Source" in display_visible_df.columns
        else display_visible_df
    )
    render_dashboard_table(
        styled_df,
        help_map={
            "Total Scaled Score": "Total Scaled Volatility-Adjusted Score",
            "Debt-Adj. Total Score": "Total Debt-Adjusted Scaled Volatility-Adjusted Score",
            "Overall Score": "Overall Score (0-400)",
            "Market Price": "Current Market Price",
            "Price Source": "Live means fetched during this run. DB fallback means the stored market price from the database was used.",
            "Quote Date": "Timestamp or period associated with the market price shown for this row.",
            "Price Detail": "Additional detail explaining why this market price source was used.",
            "Intrinsic Value": "Intrinsic Value",
            "Upside / Downside": "Difference %",
        },
        column_config={
            "Upside / Downside": st.column_config.NumberColumn(format="%.2f%%"),
        },
        key=f"{download_key}_table",
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


def _valuation_dashboard_excel_bytes(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Valuation Dashboard")
        worksheet = writer.sheets["Valuation Dashboard"]
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


_RELATIVE_MARKET_METRICS_TABLE = "relative_valuation_market_metrics"
_RELATIVE_VALUATION_RESULTS_KEY = "relative_valuation_dashboard_results"
_RELATIVE_VALUATION_META_KEY = "relative_valuation_dashboard_meta"
_RELATIVE_VALUATION_SCHEMA_VERSION = 2


def _ensure_relative_market_metrics_table(conn) -> None:
    if is_sqlite_url(get_db_url()):
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {_RELATIVE_MARKET_METRICS_TABLE} (
            company_id INTEGER PRIMARY KEY,
            enterprise_value REAL,
            enterprise_value_source TEXT,
            enterprise_value_as_of TEXT,
            enterprise_value_detail TEXT,
            trailing_pe REAL,
            trailing_pe_source TEXT,
            trailing_pe_as_of TEXT,
            trailing_pe_detail TEXT,
            forward_pe REAL,
            forward_pe_source TEXT,
            forward_pe_as_of TEXT,
            forward_pe_detail TEXT,
            updated_at TEXT
        )
        """
    else:
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {_RELATIVE_MARKET_METRICS_TABLE} (
            company_id INTEGER PRIMARY KEY REFERENCES companies(id) ON DELETE CASCADE,
            enterprise_value DOUBLE PRECISION,
            enterprise_value_source TEXT,
            enterprise_value_as_of TEXT,
            enterprise_value_detail TEXT,
            trailing_pe DOUBLE PRECISION,
            trailing_pe_source TEXT,
            trailing_pe_as_of TEXT,
            trailing_pe_detail TEXT,
            forward_pe DOUBLE PRECISION,
            forward_pe_source TEXT,
            forward_pe_as_of TEXT,
            forward_pe_detail TEXT,
            updated_at TEXT
        )
        """
    conn.execute(create_sql)
    _commit_db(conn)


def _load_relative_market_metrics(conn, company_ids: List[int]) -> pd.DataFrame:
    _ensure_relative_market_metrics_table(conn)
    if not company_ids:
        return pd.DataFrame()
    ids = sorted({int(company_id) for company_id in company_ids})
    placeholders = ",".join(["?"] * len(ids))
    return read_df(
        f"""
        SELECT *
        FROM {_RELATIVE_MARKET_METRICS_TABLE}
        WHERE company_id IN ({placeholders})
        """,
        conn,
        params=tuple(ids),
    )


def _upsert_relative_market_metric(conn, row: Dict[str, object]) -> None:
    _ensure_relative_market_metrics_table(conn)
    params = (
        int(row["company_id"]),
        row.get("enterprise_value"),
        row.get("enterprise_value_source"),
        row.get("enterprise_value_as_of"),
        row.get("enterprise_value_detail"),
        row.get("trailing_pe"),
        row.get("trailing_pe_source"),
        row.get("trailing_pe_as_of"),
        row.get("trailing_pe_detail"),
        row.get("forward_pe"),
        row.get("forward_pe_source"),
        row.get("forward_pe_as_of"),
        row.get("forward_pe_detail"),
        row.get("updated_at"),
    )
    conn.execute(
        f"""
        INSERT INTO {_RELATIVE_MARKET_METRICS_TABLE}(
            company_id,
            enterprise_value,
            enterprise_value_source,
            enterprise_value_as_of,
            enterprise_value_detail,
            trailing_pe,
            trailing_pe_source,
            trailing_pe_as_of,
            trailing_pe_detail,
            forward_pe,
            forward_pe_source,
            forward_pe_as_of,
            forward_pe_detail,
            updated_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(company_id) DO UPDATE SET
            enterprise_value=excluded.enterprise_value,
            enterprise_value_source=excluded.enterprise_value_source,
            enterprise_value_as_of=excluded.enterprise_value_as_of,
            enterprise_value_detail=excluded.enterprise_value_detail,
            trailing_pe=excluded.trailing_pe,
            trailing_pe_source=excluded.trailing_pe_source,
            trailing_pe_as_of=excluded.trailing_pe_as_of,
            trailing_pe_detail=excluded.trailing_pe_detail,
            forward_pe=excluded.forward_pe,
            forward_pe_source=excluded.forward_pe_source,
            forward_pe_as_of=excluded.forward_pe_as_of,
            forward_pe_detail=excluded.forward_pe_detail,
            updated_at=excluded.updated_at
        """,
        params,
    )
    _commit_db(conn)


def _latest_series_scalar(
    conn,
    annual_table: str,
    value_col: str,
    company_id: int,
    *,
    ttm_table: Optional[str] = None,
) -> Tuple[Optional[str], Optional[float]]:
    as_of, ttm_value = _load_latest_ttm_scalar(conn, ttm_table, value_col, company_id) if ttm_table else (None, None)
    if ttm_value is not None:
        return _normalize_quote_as_of(as_of) or "Stored TTM", ttm_value
    latest_year, latest_value = _latest_annual_series_value(_load_series(conn, annual_table, value_col, company_id))
    if latest_value is None:
        return None, None
    return str(latest_year), latest_value


def _calculated_market_metrics(conn, company_id: int) -> Dict[str, object]:
    mc_as_of, market_cap = _latest_series_scalar(conn, "market_capitalization_annual", "market_capitalization", company_id)
    debt_as_of, total_debt = _latest_series_scalar(conn, "total_debt_annual", "total_debt", company_id, ttm_table="total_debt_ttm")
    cash_as_of, cash = _latest_series_scalar(conn, "cash_and_cash_equivalents_annual", "cash_and_cash_equivalents", company_id, ttm_table="cash_and_cash_equivalents_ttm")
    ni_as_of, net_income = _latest_series_scalar(conn, "net_income_annual", "net_income", company_id, ttm_table="net_income_ttm")
    forward_year, forward_net_income, forward_net_income_detail = _calculated_forward_net_income(conn, company_id)

    ev = None
    ev_source = "unavailable"
    ev_as_of = None
    ev_detail = "Missing stored market capitalization, total debt, or cash."
    if market_cap is not None and total_debt is not None and cash is not None:
        ev = float(market_cap) + float(total_debt) - float(cash)
        ev_source = "calculated"
        ev_as_of = mc_as_of or debt_as_of or cash_as_of
        ev_detail = "Calculated from stored Market Capitalization + Total Debt - Cash & Cash Equivalents."

    trailing_pe = None
    trailing_source = "unavailable"
    trailing_as_of = None
    trailing_detail = "Missing stored market capitalization or TTM net income."
    if market_cap is not None and net_income not in (None, 0):
        trailing_pe = float(market_cap) / float(net_income)
        trailing_source = "calculated"
        trailing_as_of = mc_as_of or ni_as_of
        trailing_detail = "Calculated from stored Market Capitalization / TTM Net Income."

    forward_pe = None
    forward_source = "unavailable"
    forward_as_of = None
    forward_detail = forward_net_income_detail or "Forward P/E requires a forward earnings estimate."
    if market_cap is not None and forward_net_income not in (None, 0):
        forward_pe = float(market_cap) / float(forward_net_income)
        forward_source = "calculated"
        forward_as_of = str(forward_year) if forward_year is not None else mc_as_of
        forward_detail = (
            "Calculated from stored Market Capitalization / projected FY1 net income. "
            f"{forward_net_income_detail}"
        )

    return {
        "enterprise_value": ev,
        "enterprise_value_source": ev_source,
        "enterprise_value_as_of": ev_as_of,
        "enterprise_value_detail": ev_detail,
        "trailing_pe": trailing_pe,
        "trailing_pe_source": trailing_source,
        "trailing_pe_as_of": trailing_as_of,
        "trailing_pe_detail": trailing_detail,
        "forward_pe": forward_pe,
        "forward_pe_source": forward_source,
        "forward_pe_as_of": forward_as_of,
        "forward_pe_detail": forward_detail,
    }


def _latest_debt_cash_for_ev(conn, company_id: int) -> Tuple[Optional[str], Optional[float], Optional[str], Optional[float]]:
    debt_as_of, total_debt = _latest_series_scalar(
        conn,
        "total_debt_annual",
        "total_debt",
        company_id,
        ttm_table="total_debt_ttm",
    )
    cash_as_of, cash = _latest_series_scalar(
        conn,
        "cash_and_cash_equivalents_annual",
        "cash_and_cash_equivalents",
        company_id,
        ttm_table="cash_and_cash_equivalents_ttm",
    )
    return debt_as_of, total_debt, cash_as_of, cash


def _safe_fast_info_value(fast_info: object, *keys: str) -> Optional[float]:
    for key in keys:
        try:
            value = fast_info[key]  # yfinance FastInfo supports dict-style access.
        except Exception:
            try:
                value = getattr(fast_info, key)
            except Exception:
                continue
        numeric = _safe_float(value)
        if numeric is not None:
            return numeric
    return None


def _latest_yfinance_shares(ticker_obj: object, fast_info: object) -> Optional[float]:
    shares = _safe_fast_info_value(fast_info, "shares")
    if shares is not None:
        return shares
    try:
        shares_series = ticker_obj.get_shares_full()
    except Exception:
        return None
    if shares_series is None or len(shares_series) == 0:
        return None
    try:
        return _safe_float(shares_series.dropna().iloc[-1])
    except Exception:
        return None


def _market_metrics_from_market_cap(
    conn,
    company_id: int,
    market_cap: object,
    *,
    source: str,
    as_of: Optional[str],
    detail: str,
) -> Dict[str, object]:
    market_cap_value = _safe_float(market_cap)
    debt_as_of, total_debt, cash_as_of, cash = _latest_debt_cash_for_ev(conn, int(company_id))
    _, net_income = _latest_series_scalar(conn, "net_income_annual", "net_income", int(company_id), ttm_table="net_income_ttm")
    forward_year, forward_net_income, forward_net_income_detail = _calculated_forward_net_income(conn, int(company_id))

    ev = None
    ev_source = "unavailable"
    ev_as_of = None
    ev_detail = "Missing live market capitalization, stored total debt, or stored cash."
    if market_cap_value is not None and total_debt is not None and cash is not None:
        ev = float(market_cap_value) + float(total_debt) - float(cash)
        ev_source = source
        ev_as_of = as_of or debt_as_of or cash_as_of
        ev_detail = (
            f"{detail} Enterprise Value calculated as live Market Cap + stored Total Debt - stored Cash & Cash Equivalents."
        )

    trailing_pe = None
    trailing_source = "unavailable"
    trailing_as_of = None
    trailing_detail = "Missing live market capitalization or stored TTM net income."
    if market_cap_value is not None and net_income not in (None, 0):
        trailing_pe = float(market_cap_value) / float(net_income)
        trailing_source = source
        trailing_as_of = as_of
        trailing_detail = f"{detail} Trailing P/E calculated as live Market Cap / stored TTM Net Income."

    forward_pe = None
    forward_source = "unavailable"
    forward_as_of = None
    forward_detail = forward_net_income_detail or "Forward P/E requires a forward earnings estimate."
    if market_cap_value is not None and forward_net_income not in (None, 0):
        forward_pe = float(market_cap_value) / float(forward_net_income)
        forward_source = source
        forward_as_of = as_of or (str(forward_year) if forward_year is not None else None)
        forward_detail = (
            f"{detail} Forward P/E calculated as live Market Cap / projected FY1 net income. "
            f"{forward_net_income_detail}"
        )

    return {
        "enterprise_value": ev,
        "enterprise_value_source": ev_source,
        "enterprise_value_as_of": ev_as_of,
        "enterprise_value_detail": ev_detail,
        "trailing_pe": trailing_pe,
        "trailing_pe_source": trailing_source,
        "trailing_pe_as_of": trailing_as_of,
        "trailing_pe_detail": trailing_detail,
        "forward_pe": forward_pe,
        "forward_pe_source": forward_source,
        "forward_pe_as_of": forward_as_of,
        "forward_pe_detail": forward_detail,
    }


def _fetch_market_cap_api_metrics(conn, company_id: int, company_row: pd.Series) -> Dict[str, object]:
    ticker = str(company_row.get("ticker") or "")
    country = company_row.get("country")
    symbols = _build_yahoo_quote_symbols(ticker, country) or [_normalize_quote_ticker(ticker)]

    try:
        import yfinance as yf
    except Exception as exc:
        return {
            "enterprise_value": None,
            "enterprise_value_source": "unavailable",
            "enterprise_value_as_of": None,
            "enterprise_value_detail": f"Market-cap API fallback unavailable: yfinance import failed with {type(exc).__name__}.",
            "trailing_pe": None,
            "trailing_pe_source": "unavailable",
            "trailing_pe_as_of": None,
            "trailing_pe_detail": f"Market-cap API fallback unavailable: yfinance import failed with {type(exc).__name__}.",
            "forward_pe": None,
            "forward_pe_source": "unavailable",
            "forward_pe_as_of": None,
            "forward_pe_detail": f"Market-cap API fallback unavailable: yfinance import failed with {type(exc).__name__}.",
        }

    last_detail = "No yfinance symbol candidates were available for market-cap fallback."
    for symbol in [s for s in symbols if str(s or "").strip()]:
        try:
            ticker_obj = yf.Ticker(symbol)
            fast_info = ticker_obj.fast_info
            market_cap_raw = _safe_fast_info_value(fast_info, "marketCap", "market_cap")
        except Exception as exc:
            last_detail = f"Market-cap API fallback failed for {symbol}: {type(exc).__name__}."
            continue
        if market_cap_raw is None:
            last_detail = f"Market-cap API fallback did not return market cap for {symbol}."
            continue
        return _market_metrics_from_market_cap(
            conn,
            int(company_id),
            float(market_cap_raw) / 1_000_000.0,
            source="market_cap_api",
            as_of=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            detail=f"Pulled live market cap from yfinance fast_info for {symbol}; stored in local-currency millions.",
        )

    return {
        "enterprise_value": None,
        "enterprise_value_source": "unavailable",
        "enterprise_value_as_of": None,
        "enterprise_value_detail": last_detail,
        "trailing_pe": None,
        "trailing_pe_source": "unavailable",
        "trailing_pe_as_of": None,
        "trailing_pe_detail": last_detail,
        "forward_pe": None,
        "forward_pe_source": "unavailable",
        "forward_pe_as_of": None,
        "forward_pe_detail": last_detail,
    }


def _fetch_live_price_shares_metrics(conn, company_id: int, company_row: pd.Series) -> Dict[str, object]:
    ticker = str(company_row.get("ticker") or "")
    country = company_row.get("country")
    symbols = _build_yahoo_quote_symbols(ticker, country) or [_normalize_quote_ticker(ticker)]

    try:
        import yfinance as yf
    except Exception as exc:
        detail = f"Live price x shares fallback unavailable: yfinance import failed with {type(exc).__name__}."
        symbols = []
        yf = None
    else:
        detail = "No yfinance symbol candidates were available for shares outstanding fallback."

    shares = None
    share_symbol = None
    if yf is not None:
        for symbol in [s for s in symbols if str(s or "").strip()]:
            try:
                ticker_obj = yf.Ticker(symbol)
                fast_info = ticker_obj.fast_info
                shares = _latest_yfinance_shares(ticker_obj, fast_info)
            except Exception as exc:
                detail = f"Shares outstanding fallback failed for {symbol}: {type(exc).__name__}."
                continue
            if shares is not None:
                share_symbol = symbol
                break
            detail = f"Shares outstanding fallback did not return shares for {symbol}."

    quote = _fetch_live_quote_for_company(company_row)
    price = _safe_float(quote.get("price"))
    if price is None or shares is None:
        quote_detail = str(quote.get("detail") or "Live quote was unavailable.")
        missing = []
        if price is None:
            missing.append(quote_detail)
        if shares is None:
            missing.append(detail)
        combined_detail = " | ".join(missing)
        return {
            "enterprise_value": None,
            "enterprise_value_source": "unavailable",
            "enterprise_value_as_of": None,
            "enterprise_value_detail": combined_detail,
            "trailing_pe": None,
            "trailing_pe_source": "unavailable",
            "trailing_pe_as_of": None,
            "trailing_pe_detail": combined_detail,
            "forward_pe": None,
            "forward_pe_source": "unavailable",
            "forward_pe_as_of": None,
            "forward_pe_detail": combined_detail,
        }

    market_cap_millions = (float(price) * float(shares)) / 1_000_000.0
    quote_as_of = str(quote.get("as_of") or datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return _market_metrics_from_market_cap(
        conn,
        int(company_id),
        market_cap_millions,
        source="live_price_shares",
        as_of=quote_as_of,
        detail=(
            f"Calculated live market cap from live price {price:,.4f} and shares outstanding "
            f"{float(shares):,.0f}"
            f"{f' from {share_symbol}' if share_symbol else ''}; stored in local-currency millions."
        ),
    )


def _effective_valuation_settings_for_company(conn, company_id: int) -> Dict[str, object]:
    memberships_df = _get_company_group_memberships(conn, [int(company_id)])
    selected_group_ids = (
        sorted({int(group_id) for group_id in memberships_df.get("group_id", pd.Series(dtype=int)).dropna().tolist()})
        if memberships_df is not None and not memberships_df.empty
        else []
    )
    industry_overrides_df = get_dcf_industry_valuation_settings(conn, selected_group_ids) if selected_group_ids else pd.DataFrame()
    company_overrides_df = get_dcf_company_valuation_settings(conn, [int(company_id)])
    general_settings = _get_general_settings_dict(conn)
    return _get_effective_company_settings(
        int(company_id),
        general_settings,
        industry_overrides_df,
        company_overrides_df,
        memberships_df if memberships_df is not None else pd.DataFrame(),
    )


def _calculated_forward_net_income(conn, company_id: int) -> Tuple[Optional[int], Optional[float], str]:
    settings = _effective_valuation_settings_for_company(conn, int(company_id))
    revenue_series = _merge_ttm_into_annual(
        conn,
        _load_series(conn, "revenues_annual", "revenue", company_id),
        "revenues_ttm",
        "revenue",
        company_id,
    )
    net_income_series = _merge_ttm_into_annual(
        conn,
        _load_series(conn, "net_income_annual", "net_income", company_id),
        "net_income_ttm",
        "net_income",
        company_id,
    )

    raw_latest_year, raw_latest_revenue = _latest_numeric_value(revenue_series)
    if raw_latest_year is None or raw_latest_revenue is None:
        return None, None, "Missing stored revenue history for local forward net income estimate."

    latest_actual_year = _anchor_year_from_settings(settings, int(raw_latest_year))
    if int(latest_actual_year) > int(raw_latest_year):
        latest_actual_year = int(raw_latest_year)
    anchored_revenue_series = _series_through_year(revenue_series, int(latest_actual_year))
    latest_actual_year, latest_revenue = _latest_numeric_value(anchored_revenue_series)
    if latest_actual_year is None or latest_revenue is None:
        return None, None, "Missing stored revenue for the selected DCF historical anchor."

    historical_years = int(settings.get("historical_years", 7) or 7)
    revenue_growth_sample = _latest_n_growths(anchored_revenue_series, historical_years)
    if not revenue_growth_sample:
        return None, None, "Missing revenue growth history for local forward net income estimate."

    recent_revenue_values = _latest_n_values(anchored_revenue_series, historical_years)
    net_income_margin_sample = [
        float(net_income_series[year]) / float(revenue)
        for year, revenue in recent_revenue_values
        if year in net_income_series and revenue not in (None, 0)
    ]
    net_income_margin = _median(net_income_margin_sample)
    if net_income_margin is None:
        return None, None, "Missing net income margin history for local forward net income estimate."

    revenue_growth = _median(revenue_growth_sample)
    if revenue_growth is None:
        return None, None, "Missing revenue growth history for local forward net income estimate."

    starting_revenue_growth_cap = _pct_to_decimal(settings.get("starting_projected_revenue_growth_cap"))
    if starting_revenue_growth_cap is not None:
        revenue_growth = min(float(revenue_growth), float(starting_revenue_growth_cap))

    base_override = _baseline_override_decimal(settings, "future_revenue_growth")
    if base_override is not None:
        revenue_growth = float(base_override)

    revenue_growth_step = _yoy_settings_pct_to_decimal(settings.get("future_revenue_growth")) or 0.0
    projected_revenue_growth = _project_assumption_value(
        float(revenue_growth),
        1,
        "future_revenue_growth",
        settings,
        revenue_growth_step,
    )
    forward_year = int(latest_actual_year) + 1
    projected_revenue = float(latest_revenue) * (1.0 + float(projected_revenue_growth))
    projected_net_income = float(projected_revenue) * float(net_income_margin)
    return (
        forward_year,
        projected_net_income,
        (
            f"FY{forward_year} net income estimate uses stored revenue projected at "
            f"{projected_revenue_growth * 100:.2f}% and historical median net income margin "
            f"{net_income_margin * 100:.2f}%."
        ),
    )


def _fetch_yfinance_market_metrics(company_row: pd.Series) -> Dict[str, object]:
    ticker = str(company_row.get("ticker") or "")
    country = company_row.get("country")
    symbols = _build_yahoo_quote_symbols(ticker, country) or [_normalize_quote_ticker(ticker)]
    now_label = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        import yfinance as yf
    except Exception as exc:
        return {
            "enterprise_value": None,
            "enterprise_value_source": "unavailable",
            "enterprise_value_as_of": None,
            "enterprise_value_detail": f"yfinance unavailable: {type(exc).__name__}.",
            "trailing_pe": None,
            "trailing_pe_source": "unavailable",
            "trailing_pe_as_of": None,
            "trailing_pe_detail": f"yfinance unavailable: {type(exc).__name__}.",
            "forward_pe": None,
            "forward_pe_source": "unavailable",
            "forward_pe_as_of": None,
            "forward_pe_detail": f"yfinance unavailable: {type(exc).__name__}.",
        }

    last_detail = "No yfinance symbol candidates were available."
    for symbol in [s for s in symbols if str(s or "").strip()]:
        try:
            info = yf.Ticker(symbol).get_info()
        except Exception as exc:
            last_detail = f"yfinance failed for {symbol}: {type(exc).__name__}."
            continue
        if not isinstance(info, dict) or not info:
            last_detail = f"yfinance returned no info for {symbol}."
            continue

        ev_raw = _safe_float(info.get("enterpriseValue"))
        trailing_pe = _safe_float(info.get("trailingPE"))
        forward_pe = _safe_float(info.get("forwardPE"))
        return {
            "enterprise_value": (ev_raw / 1_000_000.0) if ev_raw is not None else None,
            "enterprise_value_source": "yfinance_direct" if ev_raw is not None else "unavailable",
            "enterprise_value_as_of": now_label if ev_raw is not None else None,
            "enterprise_value_detail": (
                f"Pulled direct Enterprise Value from yfinance for {symbol}; stored in local-currency millions."
                if ev_raw is not None
                else f"yfinance did not return Enterprise Value for {symbol}."
            ),
            "trailing_pe": trailing_pe,
            "trailing_pe_source": "yfinance_direct" if trailing_pe is not None else "unavailable",
            "trailing_pe_as_of": now_label if trailing_pe is not None else None,
            "trailing_pe_detail": (
                f"Pulled trailing P/E from yfinance for {symbol}."
                if trailing_pe is not None
                else f"yfinance did not return trailing P/E for {symbol}."
            ),
            "forward_pe": forward_pe,
            "forward_pe_source": "yfinance_direct" if forward_pe is not None else "unavailable",
            "forward_pe_as_of": now_label if forward_pe is not None else None,
            "forward_pe_detail": (
                f"Pulled forward P/E from yfinance for {symbol}."
                if forward_pe is not None
                else f"yfinance did not return forward P/E for {symbol}."
            ),
        }

    return {
        "enterprise_value": None,
        "enterprise_value_source": "unavailable",
        "enterprise_value_as_of": None,
        "enterprise_value_detail": last_detail,
        "trailing_pe": None,
        "trailing_pe_source": "unavailable",
        "trailing_pe_as_of": None,
        "trailing_pe_detail": last_detail,
        "forward_pe": None,
        "forward_pe_source": "unavailable",
        "forward_pe_as_of": None,
        "forward_pe_detail": last_detail,
    }


def _refresh_relative_market_metrics(conn, members_df: pd.DataFrame) -> Dict[str, int]:
    counts = {
        "ev_yfinance_direct": 0,
        "ev_market_cap_api": 0,
        "ev_live_price_shares": 0,
        "ev_calculated": 0,
        "ev_unavailable": 0,
        "trailing_pe_yfinance_direct": 0,
        "trailing_pe_market_cap_api": 0,
        "trailing_pe_live_price_shares": 0,
        "trailing_pe_calculated": 0,
        "trailing_pe_unavailable": 0,
        "forward_pe_yfinance_direct": 0,
        "forward_pe_market_cap_api": 0,
        "forward_pe_live_price_shares": 0,
        "forward_pe_calculated": 0,
        "forward_pe_unavailable": 0,
    }
    if members_df is None or members_df.empty:
        return counts

    progress_bar = st.progress(0, text="Refreshing EV and P/E data...")
    total = max(len(members_df), 1)
    for idx, (_, company_row) in enumerate(members_df.iterrows(), start=1):
        company_id = int(company_row["id"])
        progress_bar.progress(int((idx / total) * 100), text=f"Refreshing {company_row['name']} ({idx}/{total})...")
        source_factories = [
            lambda: _fetch_yfinance_market_metrics(company_row),
            lambda: _fetch_market_cap_api_metrics(conn, company_id, company_row),
            lambda: _fetch_live_price_shares_metrics(conn, company_id, company_row),
            lambda: _calculated_market_metrics(conn, company_id),
        ]
        sources: List[Dict[str, object]] = []

        def source_at(source_idx: int) -> Dict[str, object]:
            while len(sources) <= source_idx:
                sources.append(source_factories[len(sources)]())
            return sources[source_idx]

        row = {"company_id": company_id, "updated_at": datetime.now().isoformat(timespec="seconds")}
        for prefix in ["enterprise_value", "trailing_pe", "forward_pe"]:
            selected_source: Dict[str, object] = {}
            for source_idx in range(len(source_factories)):
                candidate = source_at(source_idx)
                if candidate.get(prefix) is not None or source_idx == len(source_factories) - 1:
                    selected_source = candidate
                    break
            row[prefix] = selected_source.get(prefix)
            row[f"{prefix}_source"] = selected_source.get(f"{prefix}_source")
            row[f"{prefix}_as_of"] = selected_source.get(f"{prefix}_as_of")
            row[f"{prefix}_detail"] = selected_source.get(f"{prefix}_detail")
        ev_source = str(row.get("enterprise_value_source") or "unavailable")
        trailing_source = str(row.get("trailing_pe_source") or "unavailable")
        forward_source = str(row.get("forward_pe_source") or "unavailable")
        counts[f"ev_{ev_source}"] = counts.get(f"ev_{ev_source}", 0) + 1
        counts[f"trailing_pe_{trailing_source}"] = counts.get(f"trailing_pe_{trailing_source}", 0) + 1
        counts[f"forward_pe_{forward_source}"] = counts.get(f"forward_pe_{forward_source}", 0) + 1
        _upsert_relative_market_metric(conn, row)
    progress_bar.empty()
    return counts


def _relative_refresh_source_summary(counts: Dict[str, int], prefix: str) -> str:
    labels = [
        ("yfinance direct", counts.get(f"{prefix}_yfinance_direct", 0)),
        ("market-cap API", counts.get(f"{prefix}_market_cap_api", 0)),
        ("live price x shares", counts.get(f"{prefix}_live_price_shares", 0)),
        ("stored fallback", counts.get(f"{prefix}_calculated", 0)),
        ("unavailable", counts.get(f"{prefix}_unavailable", 0)),
    ]
    return ", ".join(f"{count} {label}" for label, count in labels)


def _fill_missing_calculated_market_metrics(conn, company_id: int, market_row: Optional[pd.Series]) -> pd.Series:
    calculated = _calculated_market_metrics(conn, int(company_id))
    row: Dict[str, object] = {
        "company_id": int(company_id),
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }
    for prefix in ["enterprise_value", "trailing_pe", "forward_pe"]:
        existing_value = _safe_float(market_row.get(prefix)) if market_row is not None else None
        if existing_value is not None:
            row[prefix] = existing_value
            row[f"{prefix}_source"] = market_row.get(f"{prefix}_source")
            row[f"{prefix}_as_of"] = market_row.get(f"{prefix}_as_of")
            row[f"{prefix}_detail"] = market_row.get(f"{prefix}_detail")
        else:
            row[prefix] = calculated.get(prefix)
            row[f"{prefix}_source"] = calculated.get(f"{prefix}_source")
            row[f"{prefix}_as_of"] = calculated.get(f"{prefix}_as_of")
            row[f"{prefix}_detail"] = calculated.get(f"{prefix}_detail")
    _upsert_relative_market_metric(conn, row)
    return pd.Series(row)


def _relative_selection_company_ids(conn, companies_df: pd.DataFrame) -> Tuple[List[int], Dict[str, object]]:
    selection_mode = st.radio(
        "Analyze by",
        ["Company", "Industry Bucket", "Category / Sub-Category"],
        horizontal=True,
        key="relative_valuation_search_mode",
    )
    meta: Dict[str, object] = {"selection_mode": selection_mode}

    if selection_mode == "Company":
        labels = company_label_map(companies_df)
        selected_company_ids = st.multiselect(
            "Companies",
            options=list(labels.keys()),
            format_func=lambda company_id: labels.get(int(company_id), str(company_id)),
            key="relative_valuation_company_select",
        )
        return sorted({int(company_id) for company_id in selected_company_ids}), meta

    if selection_mode == "Industry Bucket":
        groups_df = read_df("SELECT id, name FROM company_groups ORDER BY name", conn)
        if groups_df is None or groups_df.empty:
            st.info("No industry buckets are available yet.")
            return [], meta
        group_name_to_id = {str(row["name"]): int(row["id"]) for _, row in groups_df.iterrows()}
        selected_bucket_names = st.multiselect(
            "Industry bucket(s)",
            options=list(group_name_to_id.keys()),
            key="relative_valuation_bucket_select",
        )
        meta["bucket_names"] = list(selected_bucket_names)
        group_ids = [group_name_to_id[name] for name in selected_bucket_names if name in group_name_to_id]
        if not group_ids:
            return [], meta
        placeholders = ",".join(["?"] * len(group_ids))
        members_df = read_df(
            f"""
            SELECT DISTINCT company_id
            FROM company_group_members
            WHERE group_id IN ({placeholders})
            """,
            conn,
            params=tuple(group_ids),
        )
        if members_df is None or members_df.empty:
            return [], meta
        return sorted({int(company_id) for company_id in members_df["company_id"].dropna().tolist()}), meta

    categories_df = read_df(
        """
        SELECT
            c.name AS master_category,
            s.id AS subcategory_id,
            s.name AS subcategory
        FROM relative_valuation_categories c
        JOIN relative_valuation_subcategories s
            ON s.category_id = c.id
        ORDER BY c.name, s.name
        """,
        conn,
    )
    if categories_df is None or categories_df.empty:
        st.info("No categories are available yet.")
        return [], meta

    master_categories = sorted(categories_df["master_category"].dropna().astype(str).unique().tolist())
    selected_master = st.selectbox(
        "Category",
        options=master_categories,
        key="relative_valuation_category_select",
    )
    subcategory_rows = categories_df[categories_df["master_category"] == selected_master].copy()
    subcategory_id_to_name = {
        int(row["subcategory_id"]): str(row["subcategory"])
        for _, row in subcategory_rows.iterrows()
    }
    selected_subcategory_ids = st.multiselect(
        "Sub-category",
        options=list(subcategory_id_to_name.keys()),
        format_func=lambda subcategory_id: subcategory_id_to_name.get(int(subcategory_id), str(subcategory_id)),
        key="relative_valuation_subcategory_select",
    )
    meta["category"] = selected_master
    meta["subcategory_ids"] = sorted({int(subcategory_id) for subcategory_id in selected_subcategory_ids})
    if not selected_subcategory_ids:
        return [], meta

    placeholders = ",".join(["?"] * len(selected_subcategory_ids))
    members_df = read_df(
        f"""
        SELECT DISTINCT company_id
        FROM relative_valuation_company_assignments
        WHERE subcategory_id IN ({placeholders})
        """,
        conn,
        params=tuple(int(subcategory_id) for subcategory_id in selected_subcategory_ids),
    )
    if members_df is None or members_df.empty:
        return [], meta
    return sorted({int(company_id) for company_id in members_df["company_id"].dropna().tolist()}), meta


def _members_for_company_ids(conn, company_ids: List[int]) -> pd.DataFrame:
    ids = sorted({int(company_id) for company_id in company_ids})
    if not ids:
        return pd.DataFrame()
    placeholders = ",".join(["?"] * len(ids))
    return read_df(
        f"""
        SELECT DISTINCT id, name, ticker, country
        FROM companies
        WHERE id IN ({placeholders})
        ORDER BY name
        """,
        conn,
        params=tuple(ids),
    )


def _relative_category_map(conn, company_ids: List[int]) -> Dict[int, Tuple[str, str]]:
    if not company_ids:
        return {}
    placeholders = ",".join(["?"] * len(company_ids))
    df = read_df(
        f"""
        SELECT
            a.company_id,
            c.name AS category,
            s.name AS subcategory
        FROM relative_valuation_company_assignments a
        JOIN relative_valuation_subcategories s
            ON s.id = a.subcategory_id
        JOIN relative_valuation_categories c
            ON c.id = s.category_id
        WHERE a.company_id IN ({placeholders})
        ORDER BY c.name, s.name
        """,
        conn,
        params=tuple(int(company_id) for company_id in company_ids),
    )
    out: Dict[int, Tuple[str, str]] = {}
    if df is None or df.empty:
        return out
    grouped: Dict[int, Tuple[List[str], List[str]]] = {}
    for _, row in df.iterrows():
        company_id = int(row["company_id"])
        categories, subcategories = grouped.setdefault(company_id, ([], []))
        categories.append(str(row["category"]))
        subcategories.append(str(row["subcategory"]))
    for company_id, (categories, subcategories) in grouped.items():
        out[company_id] = (", ".join(sorted(set(categories))), ", ".join(sorted(set(subcategories))))
    return out


def _latest_metric_value(conn, company_id: int, annual_table: str, value_col: str, ttm_table: Optional[str] = None) -> Optional[float]:
    _, value = _latest_series_scalar(conn, annual_table, value_col, company_id, ttm_table=ttm_table)
    return value


def _business_quarter_trend_score_for_company(conn, company_id: int, quarter_range: int, component_weights: Dict[str, float]) -> Optional[float]:
    try:
        details = calculate_business_quarter_trend_details(
            get_quarterly_business_trend_inputs(conn, company_id),
            quarter_range=int(quarter_range),
            component_weights=component_weights,
        )
        return _safe_float(details.get("business_quarter_trend_score"))
    except Exception:
        return None


def _relative_valuation_excel_bytes(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Relative Valuation Dashboard")
        worksheet = writer.sheets["Relative Valuation Dashboard"]
        worksheet.freeze_panes = "A2"
        worksheet.auto_filter.ref = worksheet.dimensions
        for column_cells in worksheet.columns:
            header = str(column_cells[0].value or "")
            max_len = len(header)
            for cell in column_cells[1:]:
                if cell.value is not None:
                    max_len = max(max_len, len(str(cell.value)))
            worksheet.column_dimensions[column_cells[0].column_letter].width = min(max(max_len + 2, 10), 42)
    return output.getvalue()


def _run_relative_valuation_dashboard(
    conn,
    selected_company_ids: List[int],
    *,
    score_year_range: str,
    quarter_range: int,
    terminal_year: int,
) -> pd.DataFrame:
    members_df = _members_for_company_ids(conn, selected_company_ids)
    if members_df is None or members_df.empty:
        return pd.DataFrame()

    dcf_df = _run_valuation_dashboard(
        conn,
        selected_company_ids,
        score_year_range=score_year_range,
        terminal_year=int(terminal_year),
    )
    dcf_lookup = {}
    if dcf_df is not None and not dcf_df.empty:
        for _, row in dcf_df.iterrows():
            dcf_lookup[(str(row.get("Company Name")), str(row.get("Ticker")))] = row

    company_ids = [int(x) for x in members_df["id"].tolist()]
    category_map = _relative_category_map(conn, company_ids)
    market_df = _load_relative_market_metrics(conn, company_ids)
    market_by_company = {
        int(row["company_id"]): row
        for _, row in market_df.iterrows()
    } if market_df is not None and not market_df.empty else {}
    component_weights = get_business_quarter_trend_weight_map(conn)

    rows: List[Dict[str, object]] = []
    progress_bar = st.progress(0, text="Building relative valuation dashboard...")
    total = max(len(members_df), 1)
    for idx, (_, company_row) in enumerate(members_df.iterrows(), start=1):
        company_id = int(company_row["id"])
        progress_bar.progress(int((idx / total) * 100), text=f"Building row for {company_row['name']} ({idx}/{total})...")

        market_row = market_by_company.get(company_id)
        if market_row is None or _safe_float(market_row.get("forward_pe")) is None:
            market_row = _fill_missing_calculated_market_metrics(conn, company_id, market_row)

        enterprise_value = _safe_float(market_row.get("enterprise_value")) if market_row is not None else None
        country = company_row.get("country")
        operating_profit = _latest_metric_value(conn, company_id, "operating_income_annual", "operating_income", "operating_income_ttm")
        sales = _latest_metric_value(conn, company_id, "revenues_annual", "revenue", "revenues_ttm")
        ev_operating_profit = _compute_series_ratio(enterprise_value, operating_profit)
        ev_sales = _compute_series_ratio(enterprise_value, sales)
        bqt_score = _business_quarter_trend_score_for_company(conn, company_id, quarter_range, component_weights)

        dcf_row = dcf_lookup.get((str(company_row["name"]), str(company_row["ticker"])))
        category, subcategory = category_map.get(company_id, ("", ""))
        rows.append(
            {
                "Company": company_row["name"],
                "Ticker": company_row["ticker"],
                "Country": country,
                "Category": category,
                "Sub-Category": subcategory,
                "Enterprise Value": _relative_ev_display_value(enterprise_value, country),
                "Enterprise Value Unit": _relative_ev_display_unit(country),
                "Debt-Adj. Total Score": _safe_float(dcf_row.get("Total Debt-Adjusted Scaled Volatility-Adjusted Score")) if dcf_row is not None else None,
                "Business Quarter Trend Score": bqt_score,
                "DCF Upside/Downside": _safe_float(dcf_row.get("Upside/Downside")) if dcf_row is not None else None,
                "Enterprise Value/Operating Profit": ev_operating_profit,
                "Enterprise Value/Sales": ev_sales,
                "Trailing Price-to-Earnings (P/E)": _safe_float(market_row.get("trailing_pe")) if market_row is not None else None,
                "Forward Price-to-Earnings (P/E)": _safe_float(market_row.get("forward_pe")) if market_row is not None else None,
            }
        )
    progress_bar.empty()

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values("Debt-Adj. Total Score", ascending=False, na_position="last").reset_index(drop=True)


def _render_relative_valuation_dashboard_tab(conn) -> None:
    st.subheader("Relative Valuation Dashboard")
    companies_df = list_companies(conn)
    if companies_df.empty:
        st.info("No companies are available yet. Upload company financials first.")
        return

    score_year_range = st.text_input(
        "Year range for Debt-Adj. Total Score (e.g., 'Recent - 2020' or '2023-2018')",
        value="Recent - 2020",
        key="relative_valuation_year_range",
    )
    quarter_range = int(
        st.number_input(
            "Quarter Range for Business Quarter Trend Score Calculation",
            min_value=8,
            value=16,
            step=4,
            key="relative_valuation_quarter_range",
        )
    )
    if quarter_range <= 4 or quarter_range % 4 != 0:
        st.error("Quarter range must be greater than 4 and a multiple of 4.")
        return

    selected_company_ids, selection_meta = _relative_selection_company_ids(conn, companies_df)
    selected_company_ids = sorted(set(selected_company_ids))
    if not selected_company_ids:
        st.info("Select at least one company, industry bucket, or category/sub-category to build the dashboard.")
        return

    st.caption(f"{len(selected_company_ids)} compan(y/ies) selected.")
    members_df = _members_for_company_ids(conn, selected_company_ids)
    if st.button("Refresh EV and P/E Data", key="relative_valuation_refresh_market_metrics"):
        counts = _refresh_relative_market_metrics(conn, members_df)
        st.session_state.pop(_RELATIVE_VALUATION_RESULTS_KEY, None)
        st.session_state.pop(_RELATIVE_VALUATION_META_KEY, None)
        st.success(
            "Refreshed EV and P/E data. "
            f"EV: {_relative_refresh_source_summary(counts, 'ev')}. "
            f"Forward P/E: {_relative_refresh_source_summary(counts, 'forward_pe')}."
        )
        st.info("Run the relative valuation dashboard again to display the refreshed EV and P/E values.")

    terminal_year = datetime.now().year + 7
    run_dashboard = st.button(
        "Run Relative Valuation Dashboard",
        type="primary",
        key="relative_valuation_run_dashboard",
    )
    if run_dashboard:
        st.session_state[_RELATIVE_VALUATION_RESULTS_KEY] = _run_relative_valuation_dashboard(
            conn,
            selected_company_ids,
            score_year_range=score_year_range,
            quarter_range=int(quarter_range),
            terminal_year=int(terminal_year),
        )
        st.session_state[_RELATIVE_VALUATION_META_KEY] = {
            "schema_version": _RELATIVE_VALUATION_SCHEMA_VERSION,
            "company_ids": list(selected_company_ids),
            "score_year_range": score_year_range,
            "quarter_range": int(quarter_range),
            "selection": selection_meta,
        }

    dashboard_df = st.session_state.get(_RELATIVE_VALUATION_RESULTS_KEY)
    dashboard_meta = st.session_state.get(_RELATIVE_VALUATION_META_KEY, {})
    if not isinstance(dashboard_df, pd.DataFrame) or dashboard_df.empty:
        return
    if (
        dashboard_meta.get("schema_version") != _RELATIVE_VALUATION_SCHEMA_VERSION
        or dashboard_meta.get("company_ids") != list(selected_company_ids)
        or dashboard_meta.get("score_year_range") != score_year_range
        or int(dashboard_meta.get("quarter_range", quarter_range)) != int(quarter_range)
        or dashboard_meta.get("selection") != selection_meta
    ):
        st.info("Run the relative valuation dashboard again to refresh results for the current filters.")
        return

    render_dashboard_table(
        dashboard_df,
        column_config={
            "Enterprise Value": st.column_config.NumberColumn(format="%.2f"),
            "Debt-Adj. Total Score": st.column_config.NumberColumn(format="%.2f"),
            "Business Quarter Trend Score": st.column_config.NumberColumn(format="%.1f"),
            "DCF Upside/Downside": st.column_config.NumberColumn(format="%.2f%%"),
            "Enterprise Value/Operating Profit": st.column_config.NumberColumn(format="%.2f"),
            "Enterprise Value/Sales": st.column_config.NumberColumn(format="%.2f"),
            "Trailing Price-to-Earnings (P/E)": st.column_config.NumberColumn(format="%.2f"),
            "Forward Price-to-Earnings (P/E)": st.column_config.NumberColumn(format="%.2f"),
        },
        key="relative_valuation_dashboard_table",
    )
    st.download_button(
        "Download Relative Valuation Dashboard (Excel)",
        data=_relative_valuation_excel_bytes(dashboard_df),
        file_name="relative_valuation_dashboard.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="relative_valuation_excel_download",
    )


def _commit_db(conn) -> None:
    try:
        conn.commit()
    except Exception:
        session = getattr(conn, "session", None)
        if session is not None:
            session.commit()
            return
        raise


def _ensure_valuation_saved_dashboards_table(conn) -> None:
    if is_sqlite_url(get_db_url()):
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {_VALUATION_DASHBOARD_SAVED_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            company_ids_json TEXT NOT NULL,
            result_json TEXT,
            score_year_range TEXT,
            terminal_year INTEGER,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    else:
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {_VALUATION_DASHBOARD_SAVED_TABLE} (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            company_ids_json TEXT NOT NULL,
            result_json TEXT,
            score_year_range TEXT,
            terminal_year INTEGER,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    conn.execute(create_sql)
    _commit_db(conn)


def _list_saved_valuation_dashboards(conn) -> pd.DataFrame:
    _ensure_valuation_saved_dashboards_table(conn)
    df = read_df(
        f"""
        SELECT id, name, company_ids_json, score_year_range, terminal_year, updated_at
        FROM {_VALUATION_DASHBOARD_SAVED_TABLE}
        ORDER BY name
        """,
        conn,
    )
    return df if df is not None else pd.DataFrame()


def _saved_dashboard_company_ids(row: pd.Series) -> List[int]:
    try:
        raw_ids = json.loads(str(row.get("company_ids_json") or "[]"))
    except Exception:
        raw_ids = []
    company_ids: List[int] = []
    for raw_id in raw_ids:
        try:
            company_ids.append(int(raw_id))
        except Exception:
            continue
    return sorted(set(company_ids))


def _valuation_dashboard_result_json(dashboard_df: pd.DataFrame) -> str:
    if not isinstance(dashboard_df, pd.DataFrame) or dashboard_df.empty:
        return "[]"
    safe_df = dashboard_df.astype(object).where(pd.notna(dashboard_df), None)
    return json.dumps(safe_df.to_dict("records"), default=str)


def _merge_valuation_dashboard_result_json(existing_json: object, append_df: pd.DataFrame) -> str:
    try:
        existing_rows = json.loads(str(existing_json or "[]"))
    except Exception:
        existing_rows = []
    if not isinstance(existing_rows, list):
        existing_rows = []

    try:
        append_rows = json.loads(_valuation_dashboard_result_json(append_df))
    except Exception:
        append_rows = []
    if not isinstance(append_rows, list):
        append_rows = []

    merged_by_key: Dict[str, Dict[str, object]] = {}
    fallback_idx = 0
    for row in [*existing_rows, *append_rows]:
        if not isinstance(row, dict):
            continue
        key = str(row.get("Ticker") or row.get("Company Name") or "").strip().upper()
        if not key:
            fallback_idx += 1
            key = f"__ROW_{fallback_idx}"
        merged_by_key[key] = row
    return json.dumps(list(merged_by_key.values()), default=str)


def _save_new_valuation_dashboard(
    conn,
    *,
    name: str,
    company_ids: List[int],
    dashboard_df: pd.DataFrame,
    score_year_range: str,
    terminal_year: int,
) -> Tuple[bool, str]:
    clean_name = str(name or "").strip()
    if not clean_name:
        return False, "Enter a dashboard name."
    if len(clean_name) > 20:
        return False, "Dashboard name must be 20 characters or fewer."
    clean_company_ids = sorted(set(int(company_id) for company_id in company_ids))
    if not clean_company_ids:
        return False, "No companies are available to save."

    _ensure_valuation_saved_dashboards_table(conn)
    existing_df = read_df(
        f"SELECT id FROM {_VALUATION_DASHBOARD_SAVED_TABLE} WHERE LOWER(name) = LOWER(?) LIMIT 1",
        conn,
        params=(clean_name,),
    )
    if existing_df is not None and not existing_df.empty:
        return False, "A saved dashboard with this name already exists. Use Update Dashboard instead."

    now = datetime.now().isoformat(timespec="seconds")
    conn.execute(
        f"""
        INSERT INTO {_VALUATION_DASHBOARD_SAVED_TABLE} (
            name,
            company_ids_json,
            result_json,
            score_year_range,
            terminal_year,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            clean_name,
            json.dumps(clean_company_ids),
            _valuation_dashboard_result_json(dashboard_df),
            str(score_year_range or ""),
            int(terminal_year),
            now,
            now,
        ),
    )
    _commit_db(conn)
    return True, f"Saved dashboard '{clean_name}'."


def _update_valuation_dashboard(
    conn,
    *,
    dashboard_id: int,
    company_ids: List[int],
    dashboard_df: pd.DataFrame,
    score_year_range: str,
    terminal_year: int,
) -> Tuple[bool, str]:
    clean_company_ids = sorted(set(int(company_id) for company_id in company_ids))
    if not clean_company_ids:
        return False, "No companies are available to save."

    _ensure_valuation_saved_dashboards_table(conn)
    saved_df = read_df(
        f"SELECT name FROM {_VALUATION_DASHBOARD_SAVED_TABLE} WHERE id = ? LIMIT 1",
        conn,
        params=(int(dashboard_id),),
    )
    if saved_df is None or saved_df.empty:
        return False, "Select a saved dashboard to update."

    now = datetime.now().isoformat(timespec="seconds")
    conn.execute(
        f"""
        UPDATE {_VALUATION_DASHBOARD_SAVED_TABLE}
        SET
            company_ids_json = ?,
            result_json = ?,
            score_year_range = ?,
            terminal_year = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            json.dumps(clean_company_ids),
            _valuation_dashboard_result_json(dashboard_df),
            str(score_year_range or ""),
            int(terminal_year),
            now,
            int(dashboard_id),
        ),
    )
    _commit_db(conn)
    return True, f"Updated dashboard '{saved_df.iloc[0]['name']}'."


def _append_valuation_dashboard(
    conn,
    *,
    dashboard_id: int,
    company_ids: List[int],
    dashboard_df: pd.DataFrame,
    score_year_range: str,
    terminal_year: int,
) -> Tuple[bool, str]:
    append_company_ids = sorted(set(int(company_id) for company_id in company_ids))
    if not append_company_ids:
        return False, "No companies are available to append."

    _ensure_valuation_saved_dashboards_table(conn)
    saved_df = read_df(
        f"""
        SELECT name, company_ids_json, result_json
        FROM {_VALUATION_DASHBOARD_SAVED_TABLE}
        WHERE id = ?
        LIMIT 1
        """,
        conn,
        params=(int(dashboard_id),),
    )
    if saved_df is None or saved_df.empty:
        return False, "Select a saved dashboard to append."

    saved_row = saved_df.iloc[0]
    existing_company_ids = _saved_dashboard_company_ids(saved_row)
    merged_company_ids = sorted(set(existing_company_ids) | set(append_company_ids))
    added_count = len(merged_company_ids) - len(set(existing_company_ids))
    if added_count <= 0:
        return False, "All selected companies are already in this saved dashboard."

    now = datetime.now().isoformat(timespec="seconds")
    conn.execute(
        f"""
        UPDATE {_VALUATION_DASHBOARD_SAVED_TABLE}
        SET
            company_ids_json = ?,
            result_json = ?,
            score_year_range = ?,
            terminal_year = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            json.dumps(merged_company_ids),
            _merge_valuation_dashboard_result_json(saved_row.get("result_json"), dashboard_df),
            str(score_year_range or ""),
            int(terminal_year),
            now,
            int(dashboard_id),
        ),
    )
    _commit_db(conn)
    return True, f"Appended {added_count} compan(y/ies) to dashboard '{saved_row['name']}'."


def _render_valuation_dashboard_save_controls(
    conn,
    *,
    dashboard_df: pd.DataFrame,
    company_ids: List[int],
    score_year_range: str,
    terminal_year: int,
) -> None:
    st.markdown("---")
    save_mode = st.radio(
        "Save dashboard",
        ["Save as new Dashboard", "Update Dashboard", "Append Dashboard"],
        horizontal=True,
        key=_VALUATION_DASHBOARD_SAVE_MODE_KEY,
    )
    if save_mode == "Save as new Dashboard":
        dashboard_name = st.text_input(
            "Dashboard name",
            max_chars=20,
            key="valuation_dashboard_new_name",
        )
        if st.button("Save as new Dashboard", key="valuation_dashboard_save_new"):
            ok, message = _save_new_valuation_dashboard(
                conn,
                name=dashboard_name,
                company_ids=company_ids,
                dashboard_df=dashboard_df,
                score_year_range=score_year_range,
                terminal_year=int(terminal_year),
            )
            (st.success if ok else st.error)(message)
    elif save_mode == "Update Dashboard":
        saved_dashboards_df = _list_saved_valuation_dashboards(conn)
        if saved_dashboards_df.empty:
            st.info("No saved valuation dashboards are available yet.")
            return

        name_to_id = {str(row["name"]): int(row["id"]) for _, row in saved_dashboards_df.iterrows()}
        selected_name = st.selectbox(
            "Saved dashboard",
            options=list(name_to_id.keys()),
            key="valuation_dashboard_update_select",
        )
        if st.button("Update Dashboard", key="valuation_dashboard_update_existing"):
            ok, message = _update_valuation_dashboard(
                conn,
                dashboard_id=name_to_id[selected_name],
                company_ids=company_ids,
                dashboard_df=dashboard_df,
                score_year_range=score_year_range,
                terminal_year=int(terminal_year),
            )
            (st.success if ok else st.error)(message)
    else:
        saved_dashboards_df = _list_saved_valuation_dashboards(conn)
        if saved_dashboards_df.empty:
            st.info("No saved valuation dashboards are available yet.")
            return

        name_to_id = {str(row["name"]): int(row["id"]) for _, row in saved_dashboards_df.iterrows()}
        selected_name = st.selectbox(
            "Saved dashboard",
            options=list(name_to_id.keys()),
            key="valuation_dashboard_append_select",
        )
        if st.button("Append Dashboard", key="valuation_dashboard_append_existing"):
            ok, message = _append_valuation_dashboard(
                conn,
                dashboard_id=name_to_id[selected_name],
                company_ids=company_ids,
                dashboard_df=dashboard_df,
                score_year_range=score_year_range,
                terminal_year=int(terminal_year),
            )
            (st.success if ok else st.error)(message)


def _parse_valuation_dashboard_year_range(year_range: str, available_years: List[int]) -> Tuple[int, int]:
    text = (year_range or "").strip()
    if not available_years:
        raise ValueError("No annual years available.")

    normalized = text.replace("–", "-").replace("—", "-").replace("â€“", "-").replace("â€”", "-")
    most_recent = max(int(year) for year in available_years)
    recent_match = re.match(r"^recent\s*-\s*(\d{4})$", normalized, flags=re.IGNORECASE)
    two_year_match = re.match(r"^(\d{4})\s*-\s*(\d{4})$", normalized)
    if recent_match:
        return most_recent, int(recent_match.group(1))
    if two_year_match:
        return int(two_year_match.group(1)), int(two_year_match.group(2))
    raise ValueError("Could not parse the year range. Use 'Recent - YYYY' or 'YYYY-YYYY'.")


def _available_value_creation_years(conn, company_id: int) -> List[int]:
    tables = [
        "revenues_annual",
        "accumulated_profit_annual",
        "roe_annual",
        "roce_annual",
        "interest_load_annual",
        "op_margin_annual",
        "fcff_annual",
        "roic_wacc_spread_annual",
    ]
    years: set[int] = set()
    for table in tables:
        try:
            df = read_df(
                f"SELECT fiscal_year AS year FROM {table} WHERE company_id = ?",
                conn,
                params=(int(company_id),),
            )
        except Exception:
            continue
        if df is None or df.empty:
            continue
        years.update(int(year) for year in df["year"].dropna().tolist())
    return sorted(years)


def _percentage_points(value: object) -> Optional[float]:
    numeric = _safe_float(value)
    if numeric is None:
        return None
    return float(numeric) * 100.0


def _metric_cagrs_from_payload(
    detail_payload: Dict[str, object],
    metric_name: str,
    *,
    positive_only: bool = True,
) -> Tuple[Optional[float], Optional[float]]:
    columns, values = _payload_metric_series(detail_payload.get("operating_table"), metric_name)
    actual_points, projected_points = _split_actual_projected_points(columns, values)
    return (
        _cagr_from_points(actual_points, positive_only=positive_only),
        _cagr_from_points(projected_points, positive_only=positive_only),
    )


def _latest_projected_metric_value(detail_payload: Dict[str, object], metric_name: str) -> Optional[float]:
    columns, values = _payload_metric_series(detail_payload.get("discounting_table"), metric_name)
    _, projected_points = _split_actual_projected_points(columns, values)
    if not projected_points:
        return None
    return _safe_float(projected_points[-1][1])


def _valuation_dashboard_composite_score(debt_adjusted_score: object, upside_downside: object) -> Optional[float]:
    quality_score = _safe_float(debt_adjusted_score)
    upside_pct = _safe_float(upside_downside)
    if quality_score is None or upside_pct is None:
        return None

    quality_component = min(max(float(quality_score), 0.0), 20.0) / 20.0
    valuation_component = 1.0 / (1.0 + np.exp(-float(upside_pct) / 25.0))
    return 10.0 * (quality_component ** 0.6) * (valuation_component ** 0.4)


def _valuation_dashboard_row(
    projection_row: pd.Series,
    *,
    debt_adjusted_score: Optional[float],
    terminal_year: int,
) -> Dict[str, object]:
    detail_payload = projection_row.get("__valuation_detail")
    if not isinstance(detail_payload, dict):
        detail_payload = {}

    growth_rows, multiplier_rows, median_rows = _valuation_insight_rows(detail_payload)
    embedded_growth_score, _, _, _ = _build_growth_intensity_summary(growth_rows, multiplier_rows, median_rows)
    historical_revenue_cagr, projected_revenue_cagr = _metric_cagrs_from_payload(detail_payload, "Revenue")
    historical_fcff_cagr, projected_fcff_cagr = _metric_cagrs_from_payload(detail_payload, "FCFF")
    historical_ebit_cagr, projected_ebit_cagr = _metric_cagrs_from_payload(detail_payload, "EBIT")
    terminal_growth_rate = _assumption_row_value(detail_payload, "Terminal Growth Rate")
    historical_years = _assumption_row_value(detail_payload, "Historical Years Used")
    terminal_year_wacc = _latest_projected_metric_value(detail_payload, "Projected Year WACC")
    upside_downside = _safe_float(projection_row.get("Difference %"))

    return {
        "Company Name": projection_row.get("Company Name"),
        "Ticker": projection_row.get("Ticker"),
        "Industry Bucket": projection_row.get("Industry Bucket"),
        "Current Market Price": _safe_float(projection_row.get("Current Market Price")),
        "Intrinsic Value": _safe_float(projection_row.get("Intrinsic Value")),
        "Upside/Downside": upside_downside,
        "Embedded Growth Intensity": _percentage_points(embedded_growth_score),
        "Number of Historical Years Considered": int(historical_years) if historical_years is not None else None,
        "Total Debt-Adjusted Scaled Volatility-Adjusted Score": debt_adjusted_score,
        "Composite Score": _valuation_dashboard_composite_score(debt_adjusted_score, upside_downside),
        "Overall Through-the-Cycle-Efficiency Score": _safe_float(projection_row.get("Overall Through-the-Cycle-Efficiency Score")),
        "Terminal Year": int(terminal_year),
        "Terminal Year WACC%": _percentage_points(terminal_year_wacc),
        "Terminal Growth Rate%": _percentage_points(terminal_growth_rate),
        "Historical Revenue CAGR%": _percentage_points(historical_revenue_cagr),
        "Projected Revenue CAGR%": _percentage_points(projected_revenue_cagr),
        "Historical FCFF CAGR%": _percentage_points(historical_fcff_cagr),
        "Projected FCFF CAGR%": _percentage_points(projected_fcff_cagr),
        "Historic EBIT CAGR%": _percentage_points(historical_ebit_cagr),
        "Projected EBIT CAGR%": _percentage_points(projected_ebit_cagr),
    }


def _run_valuation_dashboard(
    conn,
    selected_company_ids: List[int],
    *,
    score_year_range: str,
    terminal_year: int,
) -> pd.DataFrame:
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
    score_context: Dict[str, object] = {}
    ttc_context = _build_ttc_context(conn)
    live_quote_map = _fetch_live_quotes_for_companies(members_df)

    progress_bar = st.progress(0, text="Preparing valuation dashboard...")
    total = max(len(members_df), 1)
    rows: List[Dict[str, object]] = []

    for idx, (_, company_row) in enumerate(members_df.iterrows(), start=1):
        company_id = int(company_row["id"])
        progress_bar.progress(
            int((idx / total) * 100),
            text=f"Computing valuation dashboard row for {company_row['name']} ({idx}/{total})...",
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
        projection_row = _compute_dcf_projection(
            conn=conn,
            company_row=company_row,
            live_quote=live_quote_map.get(company_id),
            settings=effective_settings,
            terminal_year=int(terminal_year),
            score_context=score_context,
            growth_weight_map=growth_weight_map,
            stddev_weight_map=stddev_weight_map,
            selected_bucket_map=selected_bucket_map,
            compute_overall_score=False,
        )

        debt_adjusted_score = None
        available_years = _available_value_creation_years(conn, company_id)
        try:
            yr_start, yr_end = _parse_valuation_dashboard_year_range(score_year_range, available_years)
            if yr_start < yr_end:
                yr_start, yr_end = yr_end, yr_start
            score_metrics = _compute_value_creation_filter_metrics(
                conn,
                company_id,
                int(yr_start),
                int(yr_end),
                growth_weight_map,
                stddev_weight_map,
            )
            debt_adjusted_score = score_metrics.get("Total Debt-Adjusted Scaled Volatility-Adjusted Score")
        except Exception:
            debt_adjusted_score = None

        try:
            projection_row["Overall Through-the-Cycle-Efficiency Score"] = _compute_ttc_overall_score(
                conn,
                company_id,
                score_year_range,
                ttc_context,
            )
        except Exception:
            projection_row["Overall Through-the-Cycle-Efficiency Score"] = None

        rows.append(
            _valuation_dashboard_row(
                pd.Series(projection_row),
                debt_adjusted_score=debt_adjusted_score,
                terminal_year=int(terminal_year),
            )
        )

    progress_bar.progress(100, text="Valuation dashboard complete.")
    dashboard_df = pd.DataFrame(rows)
    if dashboard_df.empty:
        return dashboard_df
    return dashboard_df.sort_values(by="Composite Score", ascending=False, na_position="last").reset_index(drop=True)


def _render_valuation_dashboard_tab(conn) -> None:
    st.subheader("Valuation Dashboard")

    companies_df = list_companies(conn)
    if companies_df.empty:
        st.info("No companies are available yet. Upload company financials first.")
        return

    score_year_range = st.text_input(
        "Year range for Overall Scores (e.g., 'Recent - 2020' or '2023-2018')",
        value="Recent - 2020",
        key="valuation_dashboard_year_range",
    )
    terminal_year = datetime.now().year + 7

    selection_mode = st.radio(
        "Search by",
        ["Company", "Industry Bucket", "Saved Dashboard"],
        horizontal=True,
        key="valuation_dashboard_search_mode",
    )

    selected_company_ids: List[int] = []
    selected_saved_dashboard_id: Optional[int] = None
    if selection_mode == "Company":
        filtered_df = companies_df.copy()
        labels = company_label_map(filtered_df)
        selected_company_ids = st.multiselect(
            "Companies",
            options=list(labels.keys()),
            format_func=lambda company_id: labels.get(company_id, str(company_id)),
            key="valuation_dashboard_company_select",
        )
    elif selection_mode == "Industry Bucket":
        groups_df = read_df("SELECT id, name FROM company_groups ORDER BY name", conn)
        if groups_df.empty:
            st.info("No industry buckets are available yet.")
            return

        group_name_to_id = {str(row["name"]): int(row["id"]) for _, row in groups_df.iterrows()}
        selected_bucket_names = st.multiselect(
            "Industry bucket(s)",
            options=list(group_name_to_id.keys()),
            key="valuation_dashboard_bucket_select",
        )
        if selected_bucket_names:
            group_ids = [group_name_to_id[name] for name in selected_bucket_names if name in group_name_to_id]
            if group_ids:
                members_df = read_df(
                    f"""
                    SELECT DISTINCT company_id
                    FROM company_group_members
                    WHERE group_id IN ({','.join(['?'] * len(group_ids))})
                    """,
                    conn,
                    params=tuple(group_ids),
                )
                if members_df is not None and not members_df.empty:
                    selected_company_ids = [int(company_id) for company_id in members_df["company_id"].dropna().tolist()]
    else:
        saved_dashboards_df = _list_saved_valuation_dashboards(conn)
        if saved_dashboards_df.empty:
            st.info("No saved valuation dashboards are available yet. Run a Company search and save a dashboard first.")
            return

        saved_name_to_idx = {str(row["name"]): idx for idx, row in saved_dashboards_df.iterrows()}
        selected_saved_name = st.selectbox(
            "Saved dashboard",
            options=list(saved_name_to_idx.keys()),
            key="valuation_dashboard_saved_select",
        )
        saved_row = saved_dashboards_df.loc[saved_name_to_idx[selected_saved_name]]
        selected_saved_dashboard_id = int(saved_row["id"])
        selected_company_ids = _saved_dashboard_company_ids(saved_row)
        st.caption(f"Saved dashboard last updated: {saved_row.get('updated_at') or 'N/A'}")

    selected_company_ids = sorted(set(selected_company_ids))
    if not selected_company_ids:
        st.info("Select at least one company, industry bucket, or saved dashboard to build the valuation dashboard.")
        return

    st.caption(f"{len(selected_company_ids)} compan(y/ies) selected.")
    run_dashboard = st.button(
        "Run Valuation Dashboard",
        type="primary",
        key="valuation_dashboard_run",
    )
    if run_dashboard:
        st.session_state[_VALUATION_DASHBOARD_RESULTS_KEY] = _run_valuation_dashboard(
            conn,
            selected_company_ids,
            score_year_range=score_year_range,
            terminal_year=int(terminal_year),
        )
        st.session_state[_VALUATION_DASHBOARD_META_KEY] = {
            "company_ids": list(selected_company_ids),
            "score_year_range": score_year_range,
            "terminal_year": int(terminal_year),
            "selection_mode": selection_mode,
            "saved_dashboard_id": selected_saved_dashboard_id,
        }

    dashboard_df = st.session_state.get(_VALUATION_DASHBOARD_RESULTS_KEY)
    dashboard_meta = st.session_state.get(_VALUATION_DASHBOARD_META_KEY, {})
    if not isinstance(dashboard_df, pd.DataFrame) or dashboard_df.empty:
        return

    if (
        dashboard_meta.get("company_ids") != list(selected_company_ids)
        or dashboard_meta.get("score_year_range") != score_year_range
        or int(dashboard_meta.get("terminal_year", terminal_year)) != int(terminal_year)
        or dashboard_meta.get("selection_mode") != selection_mode
        or dashboard_meta.get("saved_dashboard_id") != selected_saved_dashboard_id
    ):
        st.info("Run the valuation dashboard again to refresh results for the current filters.")
        return

    composite_help = (
        "Quality component: Q = MIN(MAX(Total Debt-Adjusted Scaled Volatility-Adjusted Score, 0), 20) / 20. "
        "Valuation component: V = 1 / (1 + EXP(-Upside/Downside / 25)). "
        "Composite Score = 10 * (Q ^ 0.6) * (V ^ 0.4)."
    )
    ttc_score_help = (
        "Source: Equity Research -> Through-the-Cycle Efficiency -> Combined Score -> Overall Score (0-400). "
        "Computed with the valuation dashboard year range and current TTC assumptions."
    )
    render_dashboard_table(
        dashboard_df,
        help_map={
            "Composite Score": composite_help,
            "Overall Through-the-Cycle-Efficiency Score": ttc_score_help,
        },
        column_config={
            "Number of Historical Years Considered": st.column_config.NumberColumn(format="%d"),
            "Terminal Year": st.column_config.NumberColumn(format="%d"),
        },
        key="valuation_dashboard_table",
    )

    if selection_mode == "Company":
        _render_valuation_dashboard_save_controls(
            conn,
            dashboard_df=dashboard_df,
            company_ids=selected_company_ids,
            score_year_range=score_year_range,
            terminal_year=int(terminal_year),
        )

    st.download_button(
        "Download Valuation Dashboard (Excel)",
        data=_valuation_dashboard_excel_bytes(dashboard_df),
        file_name="valuation_dashboard.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="valuation_dashboard_excel_download",
    )


def _render_valuation_dashboards_tab(conn) -> None:
    active_dashboard_tab = lazy_tab_bar(
        ["DCF Valuation Dashboard", "Relative Valuation Dashboard"],
        key="valuation_dashboard_subtabs",
        default="DCF Valuation Dashboard",
    )
    if active_dashboard_tab == "DCF Valuation Dashboard":
        _render_valuation_dashboard_tab(conn)
    elif active_dashboard_tab == "Relative Valuation Dashboard":
        _render_relative_valuation_dashboard_tab(conn)


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
    settings = {field: row.get(field, defaults.get(field)) for field in _SETTINGS_FIELDS}
    if "projection_path_config" in row:
        settings["projection_path_config"] = row.get("projection_path_config")
    return settings


def _get_general_settings_dict(
    conn,
    saved_df: Optional[pd.DataFrame] = None,
    default_rates: Optional[Dict[str, float]] = None,
) -> Dict[str, float]:
    if saved_df is None:
        saved_df = get_dcf_valuation_settings(conn)
    if default_rates is None:
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


def _projection_path_defaults(initial_values: Dict[str, object]) -> Dict[str, Dict[str, object]]:
    saved_config = _parse_projection_path_config(initial_values.get("projection_path_config"))
    defaults: Dict[str, Dict[str, object]] = {}
    for metric_key, _ in _PROJECTION_PATH_METRICS:
        metric_config = saved_config.get(metric_key)
        if isinstance(metric_config, dict):
            defaults[metric_key] = metric_config
        else:
            defaults[metric_key] = _default_single_path_config(initial_values.get(metric_key))
    return defaults


def _path_config_legacy_step(config: Dict[str, object]) -> float:
    if str(config.get("mode") or "single") == "year_by_year":
        steps = config.get("steps_pct") or []
        if isinstance(steps, list) and steps:
            return _safe_float(steps[0]) or 0.0
        return 0.0
    return _safe_float(config.get("annual_step_pct")) or 0.0


def _normalize_upload_column_label(label: object) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(label or "").strip().lower().replace("&", "and"))


def _coerce_upload_numeric(value: object) -> Optional[float]:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, str):
        text = value.strip().replace(",", "")
        if text.endswith("%"):
            text = text[:-1].strip()
        if not text:
            return None
        try:
            return float(text)
        except Exception:
            return None
    try:
        return float(value)
    except Exception:
        return None


def _upload_absolute_to_model_value(metric_key: str, value: object) -> Optional[float]:
    numeric = _coerce_upload_numeric(value)
    if numeric is None:
        return None
    if metric_key == "working_capital_days_growth":
        return float(numeric)
    return float(numeric) / 100.0


def _model_value_to_upload_display(metric_key: str, value: object) -> Optional[float]:
    numeric = _safe_float(value)
    if numeric is None:
        return None
    if metric_key == "working_capital_days_growth":
        return float(numeric)
    return float(numeric) * 100.0


def _compute_assumption_upload_steps_pct(
    assumptions_df: pd.DataFrame,
    preview_context: Dict[str, object],
) -> Tuple[Optional[Dict[str, List[float]]], List[str]]:
    errors: List[str] = []
    if not preview_context or preview_context.get("error"):
        return None, [str(preview_context.get("error") or "Could not calculate base assumptions for the selected company.")]

    metric_base_values = dict(preview_context.get("metric_base_values", {}))
    metric_base_growths = dict(preview_context.get("metric_base_growths", {}))
    steps_by_metric: Dict[str, List[float]] = {}

    for metric_key, _ in _PROJECTION_PATH_METRICS:
        absolute_values = [
            _upload_absolute_to_model_value(metric_key, value)
            for value in assumptions_df[metric_key].tolist()
        ]
        if any(value is None for value in absolute_values):
            errors.append(f"Could not parse uploaded absolute values for {_ASSUMPTION_UPLOAD_FIELD_LABELS.get(metric_key, metric_key)}.")
            continue

        if metric_key == "future_revenue_growth":
            steps_by_metric[metric_key] = [float(value) * 100.0 for value in absolute_values if value is not None]
            continue

        base_value = _safe_float(metric_base_values.get(metric_key))
        if base_value is None:
            errors.append(f"Could not calculate the base value for {_ASSUMPTION_UPLOAD_FIELD_LABELS.get(metric_key, metric_key)}.")
            continue

        previous_value = float(base_value)
        metric_steps: List[float] = []
        for idx, target_value in enumerate(absolute_values, start=1):
            if target_value is None:
                continue
            target = float(target_value)
            if previous_value == 0.0:
                if target == 0.0:
                    step_pct = 0.0
                else:
                    errors.append(
                        f"Cannot compute FY{idx} ascend/descend % for {_ASSUMPTION_UPLOAD_FIELD_LABELS.get(metric_key, metric_key)} "
                        "because the prior base/projected value is zero."
                    )
                    break
            else:
                step_pct = ((target / previous_value) - 1.0) * 100.0
            metric_steps.append(float(step_pct))
            previous_value = target
        steps_by_metric[metric_key] = metric_steps

    if errors:
        return None, errors
    return steps_by_metric, []


def _computed_upload_steps_display_df(steps_by_metric: Dict[str, List[float]], years: List[int]) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    for idx, year in enumerate(years):
        row: Dict[str, object] = {"Year": int(year)}
        for metric_key, _ in _PROJECTION_PATH_METRICS:
            steps = steps_by_metric.get(metric_key, [])
            row[dict(_PROJECTION_PATH_METRICS).get(metric_key, metric_key)] = steps[idx] if idx < len(steps) else None
        rows.append(row)
    return pd.DataFrame(rows)


def _read_company_assumption_upload(uploaded_file, base_anchor_year: int) -> Tuple[Optional[pd.DataFrame], List[str]]:
    errors: List[str] = []
    try:
        raw_df = pd.read_excel(uploaded_file)
    except Exception as exc:
        return None, [f"Could not read the uploaded spreadsheet: {exc}"]

    if raw_df is None or raw_df.empty:
        return None, ["The uploaded spreadsheet is empty."]

    normalized_to_original = {_normalize_upload_column_label(col): col for col in raw_df.columns}
    selected_columns: Dict[str, object] = {}
    for field, aliases in _ASSUMPTION_UPLOAD_COLUMN_ALIASES.items():
        original_col = None
        for alias in aliases:
            normalized_alias = _normalize_upload_column_label(alias)
            if normalized_alias in normalized_to_original:
                original_col = normalized_to_original[normalized_alias]
                break
        if original_col is None:
            label = _ASSUMPTION_UPLOAD_FIELD_LABELS.get(field, field)
            errors.append(f"Missing required column: {label}")
        else:
            selected_columns[field] = original_col
    if errors:
        return None, errors

    parsed_df = pd.DataFrame({field: raw_df[col] for field, col in selected_columns.items()})
    parsed_df = parsed_df.dropna(how="all")
    parsed_df = parsed_df.dropna(how="all", subset=["year"] + [metric_key for metric_key, _ in _PROJECTION_PATH_METRICS])
    if parsed_df.empty:
        return None, ["The uploaded spreadsheet does not contain any assumption rows."]
    if len(parsed_df) > 10:
        errors.append("The uploaded spreadsheet can contain at most 10 forecast rows.")

    parsed_df["year"] = parsed_df["year"].apply(_coerce_upload_numeric)
    if parsed_df["year"].isna().any():
        errors.append("Every assumption row must have a numeric Year value.")
    else:
        non_integer_years = [year for year in parsed_df["year"].tolist() if int(year) != float(year)]
        if non_integer_years:
            errors.append("Year values must be whole years.")
        parsed_df["year"] = parsed_df["year"].astype(int)

    for metric_key, label in _PROJECTION_PATH_METRICS:
        parsed_df[metric_key] = parsed_df[metric_key].apply(_coerce_upload_numeric)
        if parsed_df[metric_key].isna().any():
            errors.append(f"Every assumption row must have a numeric value for {_ASSUMPTION_UPLOAD_FIELD_LABELS.get(metric_key, label)}.")

    if errors:
        return None, errors

    parsed_df = parsed_df.sort_values("year").reset_index(drop=True)
    years = parsed_df["year"].astype(int).tolist()
    duplicate_years = sorted({year for year in years if years.count(year) > 1})
    if duplicate_years:
        errors.append(f"Duplicate Year values are not allowed: {', '.join(str(year) for year in duplicate_years)}")

    expected_years = list(range(int(base_anchor_year) + 1, int(base_anchor_year) + 1 + len(parsed_df)))
    if years != expected_years:
        errors.append(
            "Uploaded years must be consecutive and start immediately after the selected base historical year anchor "
            f"({int(base_anchor_year)}). Expected: {', '.join(str(year) for year in expected_years)}."
        )

    if errors:
        return None, errors

    return parsed_df[["year"] + [metric_key for metric_key, _ in _PROJECTION_PATH_METRICS]], []


def _build_company_assumption_upload_values(
    initial_values: Dict[str, object],
    historical_years: int,
    base_anchor_year: int,
    assumptions_df: pd.DataFrame,
    steps_by_metric: Dict[str, List[float]],
) -> Dict[str, object]:
    projection_path_config = _parse_projection_path_config(initial_values.get("projection_path_config"))
    for metric_key, _ in _PROJECTION_PATH_METRICS:
        projection_path_config[metric_key] = {
            "mode": "year_by_year",
            "steps_pct": [float(value) for value in steps_by_metric.get(metric_key, [])],
        }
    projection_path_config[_ANCHOR_CONFIG_KEY] = {
        "use_base_historical_year": True,
        "base_historical_year": int(base_anchor_year),
        "forecast_year_limit": len(assumptions_df),
        "use_baseline_overrides": False,
        "baseline_anchor_year": None,
        "baseline_overrides": {},
    }

    result = {
        "historical_years": int(historical_years),
        "terminal_growth_usa": float(_safe_float(initial_values.get("terminal_growth_usa")) or 0.0),
        "terminal_growth_india": float(_safe_float(initial_values.get("terminal_growth_india")) or 0.0),
        "terminal_growth_china": float(_safe_float(initial_values.get("terminal_growth_china")) or 0.0),
        "terminal_growth_japan": float(_safe_float(initial_values.get("terminal_growth_japan")) or 0.0),
        "starting_projected_revenue_growth_cap": float(_safe_float(initial_values.get("starting_projected_revenue_growth_cap")) or 25.0),
        "projection_path_config": json.dumps(projection_path_config),
    }
    for metric_key, _ in _PROJECTION_PATH_METRICS:
        result[metric_key] = _path_config_legacy_step(projection_path_config[metric_key])
    return result


def _uploaded_assumptions_display_df(assumptions_df: pd.DataFrame) -> pd.DataFrame:
    display_df = assumptions_df.rename(
        columns={
            "year": "Year",
            **{metric_key: _ASSUMPTION_UPLOAD_FIELD_LABELS.get(metric_key, label) for metric_key, label in _PROJECTION_PATH_METRICS},
        }
    )
    return display_df



def _company_revenue_years(conn, company_row: Optional[pd.Series]) -> List[int]:
    if conn is None or company_row is None:
        return []
    try:
        company_id = int(company_row["id"])
    except Exception:
        return []
    series = _build_required_series(conn, company_id).get("revenue", {})
    return [int(year) for year, _ in _latest_n_values(series, 100)]


def _calculated_baseline_display_rows(preview_context: Optional[Dict[str, object]]) -> Dict[str, str]:
    if not preview_context or preview_context.get("error"):
        return {}
    metric_base_values = dict(preview_context.get("metric_base_values", {}))
    country = preview_context.get("country")
    rows: Dict[str, str] = {}
    for metric_key, _ in _PROJECTION_PATH_METRICS:
        value_format = _PREVIEW_METRIC_FORMATS.get(metric_key, "number")
        base_value = preview_context.get("latest_revenue") if metric_key == "future_revenue_growth" else metric_base_values.get(metric_key)
        if metric_key == "future_revenue_growth":
            base_value = dict(preview_context.get("metric_base_growths", {})).get(metric_key)
            rows[metric_key] = _format_preview_pct(base_value)
        else:
            rows[metric_key] = _format_preview_value(base_value, value_format, country)
    return rows


def _render_projection_anchor_controls(
    initial_values: Dict[str, object],
    form_key: str,
    *,
    preview_conn=None,
    preview_company_row: Optional[pd.Series] = None,
    historical_years: int,
    starting_projected_revenue_growth_cap: float,
) -> Dict[str, object]:
    saved_config = _anchor_config_from_settings(initial_values)
    available_years = _company_revenue_years(preview_conn, preview_company_row)
    if not available_years:
        return {
            "use_base_historical_year": False,
            "base_historical_year": None,
            "forecast_year_limit": _normalize_forecast_year_limit(saved_config.get("forecast_year_limit"), _DEFAULT_FORECAST_YEAR_LIMIT),
            "use_baseline_overrides": False,
            "baseline_anchor_year": None,
            "baseline_overrides": {},
        }

    latest_year = max(available_years)
    saved_anchor_year = saved_config.get("base_historical_year")
    try:
        saved_anchor_year = int(saved_anchor_year)
    except Exception:
        saved_anchor_year = latest_year
    if saved_anchor_year not in available_years:
        saved_anchor_year = latest_year

    st.markdown("---")
    st.markdown("**Projection Anchor Settings**")
    use_anchor = st.checkbox(
        "Use a specific base historical year anchor",
        value=bool(saved_config.get("use_base_historical_year")),
        key=f"{form_key}_use_base_historical_year",
        help="When enabled, historical calculations stop at the selected year and FY1 begins in the following year.",
    )
    anchor_year = latest_year
    if use_anchor:
        anchor_year = st.selectbox(
            "Base historical year anchor",
            options=available_years,
            index=available_years.index(int(saved_anchor_year)),
            key=f"{form_key}_base_historical_year",
            help="Projection FY1 starts in the year immediately after this anchor.",
        )
        st.caption(f"Historical calculations stop at {anchor_year}. FY1 represents {int(anchor_year) + 1}.")

    forecast_year_limit = st.number_input(
        "Forecast year limit",
        min_value=1,
        max_value=10,
        value=_normalize_forecast_year_limit(saved_config.get("forecast_year_limit"), _DEFAULT_FORECAST_YEAR_LIMIT),
        step=1,
        key=f"{form_key}_forecast_year_limit",
        help="Controls how many projected years are included in the Growth % and Actual / Projected Value previews.",
    )

    try:
        baseline_anchor_default = int(saved_config.get("baseline_anchor_year") or (int(anchor_year) + 1))
    except Exception:
        baseline_anchor_default = int(anchor_year) + 1
    baseline_year_options = sorted(set(available_years + [int(anchor_year) + 1, baseline_anchor_default]))
    use_baseline_overrides = st.checkbox(
        "Override base actual historical baseline values",
        value=bool(saved_config.get("use_baseline_overrides")),
        key=f"{form_key}_use_baseline_overrides",
        help="When enabled, all six assumption baseline overrides are required and saved together.",
    )

    baseline_anchor_year = baseline_anchor_default
    overrides: Dict[str, float] = {}
    if use_baseline_overrides:
        baseline_anchor_year = st.selectbox(
            "Base actual historical baseline anchor",
            options=baseline_year_options,
            index=baseline_year_options.index(baseline_anchor_default),
            key=f"{form_key}_baseline_anchor_year",
            help="This labels the baseline year whose assumption values are being overridden.",
        )

        calculated_settings = _settings_with_anchor_config(
            initial_values,
            {
                "use_base_historical_year": bool(use_anchor),
                "base_historical_year": int(anchor_year) if use_anchor else None,
                "forecast_year_limit": int(forecast_year_limit),
                "use_baseline_overrides": False,
                "baseline_anchor_year": int(baseline_anchor_year),
                "baseline_overrides": {},
            },
        )
        calculated_context = None
        if preview_conn is not None and preview_company_row is not None:
            calculated_context = _build_company_assumption_preview_context(
                preview_conn,
                preview_company_row,
                int(historical_years),
                float(starting_projected_revenue_growth_cap),
                calculated_settings,
            )
        calculated_display = _calculated_baseline_display_rows(calculated_context)
        saved_overrides = saved_config.get("baseline_overrides") if isinstance(saved_config.get("baseline_overrides"), dict) else {}

        st.caption("All baseline override inputs are required when this option is enabled.")
        header_cols = st.columns([2.0, 1.3, 1.2])
        header_cols[0].markdown("**Assumption Parameter**")
        header_cols[1].markdown("**Application Calculated Base**")
        header_cols[2].markdown("**Override Base Value**")
        for metric_key, label in _PROJECTION_PATH_METRICS:
            row_cols = st.columns([2.0, 1.3, 1.2])
            row_cols[0].write(label)
            row_cols[1].write(calculated_display.get(metric_key, ""))
            saved_value = _safe_float(saved_overrides.get(metric_key))
            if saved_value is None:
                calculated_decimal = None
                if calculated_context and not calculated_context.get("error"):
                    if metric_key == "future_revenue_growth":
                        calculated_decimal = dict(calculated_context.get("metric_base_growths", {})).get(metric_key)
                    else:
                        calculated_decimal = dict(calculated_context.get("metric_base_values", {})).get(metric_key)
                if calculated_decimal is not None:
                    saved_value = float(calculated_decimal) if metric_key == "working_capital_days_growth" else float(calculated_decimal) * 100.0
                else:
                    saved_value = 0.0
            with row_cols[2]:
                overrides[metric_key] = float(
                    st.number_input(
                        "Override Base Value",
                        value=float(saved_value),
                        step=0.10,
                        format="%.2f",
                        key=f"{form_key}_{metric_key}_baseline_override",
                        label_visibility="collapsed",
                    )
                )

    return {
        "use_base_historical_year": bool(use_anchor),
        "base_historical_year": int(anchor_year) if use_anchor else None,
        "forecast_year_limit": int(forecast_year_limit),
        "use_baseline_overrides": bool(use_baseline_overrides),
        "baseline_anchor_year": int(baseline_anchor_year) if use_baseline_overrides else None,
        "baseline_overrides": overrides if use_baseline_overrides else {},
    }


def _render_projection_path_controls(
    initial_values: Dict[str, object],
    form_key: str,
    *,
    preview_context: Optional[Dict[str, object]] = None,
) -> Dict[str, Dict[str, object]]:
    configs = _projection_path_defaults(initial_values)
    rendered_configs: Dict[str, Dict[str, object]] = {}

    st.markdown("**Assumption Path Controls**")
    st.caption(
        "Choose one path per assumption. Entered forecast percentages are treated as absolute YoY growth rates, "
        "not as growth over the prior year's growth rate."
    )

    for metric_key, label in _PROJECTION_PATH_METRICS:
        config = configs.get(metric_key, _default_single_path_config(initial_values.get(metric_key)))
        mode_value = str(config.get("mode") or "single")
        mode_label = "Year-by-year ascend/descend %" if mode_value == "year_by_year" else "Single annual ascend/descend % for N years"

        with st.expander(label, expanded=(metric_key == "future_revenue_growth")):
            selected_mode = st.radio(
                "Mode",
                options=["Single annual ascend/descend % for N years", "Year-by-year ascend/descend %"],
                index=1 if mode_label == "Year-by-year ascend/descend %" else 0,
                horizontal=True,
                key=f"{form_key}_{metric_key}_mode",
            )

            if selected_mode == "Year-by-year ascend/descend %":
                raw_steps = config.get("steps_pct") if mode_value == "year_by_year" else None
                steps = raw_steps if isinstance(raw_steps, list) else []
                steps = [(_safe_float(step) or 0.0) for step in steps[:10]]
                while len(steps) < 10:
                    steps.append(0.0)

                rendered_steps: List[float] = []
                for row_start in (0, 5):
                    cols = st.columns(5)
                    for offset, col in enumerate(cols):
                        idx = row_start + offset
                        with col:
                            rendered_steps.append(
                                float(
                                    st.number_input(
                                        f"FY{idx + 1}",
                                        value=float(steps[idx]),
                                        step=0.10,
                                        format="%.2f",
                                        key=f"{form_key}_{metric_key}_fy_{idx + 1}",
                                    )
                                )
                            )
                rendered_configs[metric_key] = {
                    "mode": "year_by_year",
                    "steps_pct": rendered_steps,
                }
            else:
                annual_step = _safe_float(config.get("annual_step_pct")) if mode_value == "single" else None
                years = config.get("years", 10) if mode_value == "single" else 10
                try:
                    years_value = min(max(int(years), 1), 10)
                except Exception:
                    years_value = 10

                col1, col2 = st.columns([1, 1])
                with col1:
                    annual_step_value = st.number_input(
                        "Annual absolute growth %",
                        value=float(annual_step or 0.0),
                        step=0.10,
                        format="%.2f",
                        key=f"{form_key}_{metric_key}_single_step",
                    )
                with col2:
                    years_input = st.number_input(
                        "Apply through future year",
                        min_value=1,
                        max_value=10,
                        value=int(years_value),
                        step=1,
                        key=f"{form_key}_{metric_key}_single_years",
                        help="The entered absolute growth rate applies through this future year; later years hold the resulting assumption value.",
                    )
                rendered_configs[metric_key] = {
                    "mode": "single",
                    "annual_step_pct": float(annual_step_value),
                    "years": int(years_input),
                }
            _render_assumption_path_preview(
                preview_context,
                initial_values,
                metric_key,
                label,
                rendered_configs[metric_key],
            )

    return rendered_configs


def _render_settings_form(
    initial_values: Dict[str, object],
    save_label: str,
    form_key: str,
    *,
    company_level_paths: bool = False,
    preview_conn=None,
    preview_company_row: Optional[pd.Series] = None,
) -> Optional[Dict[str, object]]:
    form_container = st.container() if company_level_paths else st.form(form_key)
    with form_container:
        historical_years = st.number_input(
            "Number of historical years to consider",
            min_value=3,
            max_value=10,
            value=int(initial_values["historical_years"]),
            step=1,
            key=f"{form_key}_historical_years",
            help="Controls how many historical annual observations are used when calculating the DCF starting point.",
        )

        st.markdown("---")
        st.markdown("**Terminal Growth Rate**")
        tg_col1, tg_col2, tg_col3, tg_col4 = st.columns(4)
        with tg_col1:
            terminal_growth_usa = st.number_input(
                "USA Terminal Growth %",
                value=float(initial_values["terminal_growth_usa"]),
                step=0.10,
                format="%.2f",
                key=f"{form_key}_terminal_growth_usa",
            )
        with tg_col2:
            terminal_growth_india = st.number_input(
                "India Terminal Growth %",
                value=float(initial_values["terminal_growth_india"]),
                step=0.10,
                format="%.2f",
                key=f"{form_key}_terminal_growth_india",
            )
        with tg_col3:
            terminal_growth_china = st.number_input(
                "China Terminal Growth %",
                value=float(initial_values["terminal_growth_china"]),
                step=0.10,
                format="%.2f",
                key=f"{form_key}_terminal_growth_china",
            )
        with tg_col4:
            terminal_growth_japan = st.number_input(
                "Japan Terminal Growth %",
                value=float(initial_values["terminal_growth_japan"]),
                step=0.10,
                format="%.2f",
                key=f"{form_key}_terminal_growth_japan",
            )

        st.markdown("---")
        starting_projected_revenue_growth_cap = st.number_input(
            "Starting Projected Revenue Growth Cap %",
            value=float(initial_values["starting_projected_revenue_growth_cap"]),
            step=0.10,
            format="%.2f",
            key=f"{form_key}_starting_projected_revenue_growth_cap",
            help="Caps the first projected year's revenue growth after the historical median is derived.",
        )

        projection_path_config: Optional[Dict[str, Dict[str, object]]] = None
        anchor_config: Optional[Dict[str, object]] = None
        anchored_initial_values = dict(initial_values)
        if company_level_paths:
            anchor_config = _render_projection_anchor_controls(
                initial_values,
                form_key,
                preview_conn=preview_conn,
                preview_company_row=preview_company_row,
                historical_years=int(historical_years),
                starting_projected_revenue_growth_cap=float(starting_projected_revenue_growth_cap),
            )
            anchored_initial_values = _settings_with_anchor_config(initial_values, anchor_config)
            preview_context = None
            if preview_conn is not None and preview_company_row is not None:
                preview_context = _build_company_assumption_preview_context(
                    preview_conn,
                    preview_company_row,
                    int(historical_years),
                    float(starting_projected_revenue_growth_cap),
                    anchored_initial_values,
                )
                _render_assumption_base_snapshot(preview_context)
            projection_path_config = _render_projection_path_controls(
                anchored_initial_values,
                form_key,
                preview_context=preview_context,
            )
            future_revenue_growth = _path_config_legacy_step(projection_path_config["future_revenue_growth"])
            ebidta_margin_growth = _path_config_legacy_step(projection_path_config["ebidta_margin_growth"])
            da_percent_growth = _path_config_legacy_step(projection_path_config["da_percent_growth"])
            capex_percent_growth = _path_config_legacy_step(projection_path_config["capex_percent_growth"])
            working_capital_days_growth = _path_config_legacy_step(projection_path_config["working_capital_days_growth"])
            wacc_direction = _path_config_legacy_step(projection_path_config["wacc_direction"])
        else:
            col1, col2 = st.columns(2)
            with col1:
                future_revenue_growth = st.number_input(
                    "Future Revenue Growth %",
                    value=float(initial_values["future_revenue_growth"]),
                    step=0.10,
                    format="%.2f",
                    key=f"{form_key}_future_revenue_growth",
                    help="Applied as a year-over-year percentage adjustment for future projected years.",
                )
                ebidta_margin_growth = st.number_input(
                    "EBIDTA Margin Growth %",
                    value=float(initial_values["ebidta_margin_growth"]),
                    step=0.10,
                    format="%.2f",
                    key=f"{form_key}_ebidta_margin_growth",
                    help="Applied as a year-over-year percentage adjustment for future projected years.",
                )
                da_percent_growth = st.number_input(
                    "D&A Percent Growth %",
                    value=float(initial_values["da_percent_growth"]),
                    step=0.10,
                    format="%.2f",
                    key=f"{form_key}_da_percent_growth",
                    help="Applied as a year-over-year percentage adjustment for future projected years.",
                )
            with col2:
                capex_percent_growth = st.number_input(
                    "CAPEX Percent Growth %",
                    value=float(initial_values["capex_percent_growth"]),
                    step=0.10,
                    format="%.2f",
                    key=f"{form_key}_capex_percent_growth",
                    help="Applied as a year-over-year percentage adjustment for future projected years.",
                )
                working_capital_days_growth = st.number_input(
                    "Working Capital Days Growth %",
                    value=float(initial_values["working_capital_days_growth"]),
                    step=0.10,
                    format="%.2f",
                    key=f"{form_key}_working_capital_days_growth",
                    help="Applied as a year-over-year percentage adjustment for future projected years.",
                )
                wacc_direction = st.number_input(
                    "WACC direction %",
                    value=float(initial_values["wacc_direction"]),
                    step=0.10,
                    format="%.2f",
                    key=f"{form_key}_wacc_direction",
                    help="Applied as a year-over-year percentage adjustment for future projected years.",
                )

        if company_level_paths:
            submitted = st.button(save_label, type="primary", key=f"{form_key}_submit")
        else:
            submitted = st.form_submit_button(save_label, type="primary")

    if not submitted:
        return None

    result = {
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
    if projection_path_config is not None:
        if anchor_config is not None:
            projection_path_config[_ANCHOR_CONFIG_KEY] = anchor_config
        result["projection_path_config"] = json.dumps(projection_path_config)
    return result


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
    labels = company_label_map(filtered_df)
    options = list(labels.keys())
    stored_selection_ids = st.session_state.get(_DCF_COMPANY_KEY, [])
    default_selection = [
        company_id
        for company_id in options
        if company_id in stored_selection_ids
    ]

    selected_company_ids = st.multiselect(
        "Companies for DCF valuation",
        options=options,
        default=default_selection,
        format_func=lambda company_id: labels.get(company_id, str(company_id)),
        key="dcf_company_multi_select",
        help="Select one or more companies to include in the DCF valuation workflow.",
    )
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
        st.session_state[_DCF_COMPANY_DETAIL_VIEW_KEY] = "Expanded Valuation"

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
        label = format_company_option(label, ticker_value)
        result_rows.append((company_id, label))

    if not result_rows:
        return

    option_map = dict(result_rows)
    option_ids = list(option_map.keys())
    stored_selected_result_id = st.session_state.get(_DCF_COMPANY_RESULT_SELECT_KEY)
    if stored_selected_result_id not in option_map:
        st.session_state[_DCF_COMPANY_RESULT_SELECT_KEY] = option_ids[0]

    st.caption("Select one company from the output and expand the valuation mechanics below the results table.")
    selector_col, action_col = st.columns([3, 2])
    with selector_col:
        selected_result_company_id = st.radio(
            "Company valuation detail",
            options=option_ids,
            format_func=lambda company_id: option_map.get(company_id, str(company_id)),
            key=_DCF_COMPANY_RESULT_SELECT_KEY,
        )
    with action_col:
        st.write("")
        st.write("")
        expand_col, insights_col = st.columns(2)
        with expand_col:
            if st.button("Expand Valuation", key="dcf_company_expand_detail"):
                st.session_state[_DCF_COMPANY_EXPANDED_RESULT_KEY] = int(selected_result_company_id)
                st.session_state[_DCF_COMPANY_DETAIL_VIEW_KEY] = "Expanded Valuation"
        with insights_col:
            if st.button("Valuation Insights", key="dcf_company_insights_detail"):
                st.session_state[_DCF_COMPANY_EXPANDED_RESULT_KEY] = int(selected_result_company_id)
                st.session_state[_DCF_COMPANY_DETAIL_VIEW_KEY] = "Valuation Insights"

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

    detail_view_options = ["Expanded Valuation", "Valuation Insights"]
    if st.session_state.get(_DCF_COMPANY_DETAIL_VIEW_KEY) not in detail_view_options:
        st.session_state[_DCF_COMPANY_DETAIL_VIEW_KEY] = "Expanded Valuation"

    detail_view = st.segmented_control(
        "Detail view",
        options=detail_view_options,
        key=_DCF_COMPANY_DETAIL_VIEW_KEY,
    )
    if detail_view == "Valuation Insights":
        _render_company_valuation_insights(detail_payload)
    else:
        _render_company_valuation_detail(detail_payload)


def _render_general_settings_tab(conn) -> None:
    st.subheader("General")

    default_rates = _get_latest_risk_free_rates(conn)
    saved_df = get_dcf_valuation_settings(conn)
    initial_values = _get_general_settings_dict(conn, saved_df=saved_df, default_rates=default_rates)

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
    industry_override_ids = (
        set(industry_overrides_df["group_id"].astype(int).tolist())
        if industry_overrides_df is not None and not industry_overrides_df.empty and "group_id" in industry_overrides_df.columns
        else set()
    )
    selected_groups_df["Setting Source"] = selected_groups_df["id"].apply(
        lambda group_id: "Industry Override" if int(group_id) in industry_override_ids else "General"
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


def _render_company_assumption_upload_workflow(
    conn,
    baseline_settings: Dict[str, object],
    baseline_company_row: pd.Series,
    form_key: str,
) -> Optional[Dict[str, object]]:
    st.markdown("---")
    st.markdown("**Upload Assumptions**")

    if st.button("Cancel upload", key=f"{form_key}_cancel_upload"):
        st.session_state[_DCF_COMPANY_ASSUMPTION_UPLOAD_VISIBLE_KEY] = False
        return {"__cancel_upload": True}

    available_years = _company_revenue_years(conn, baseline_company_row)
    if not available_years:
        st.warning("No annual revenue history is available for the selected baseline company. Upload assumptions require a base historical year anchor.")
        return None

    saved_anchor_config = _anchor_config_from_settings(baseline_settings)
    latest_year = max(available_years)
    saved_anchor_year = saved_anchor_config.get("base_historical_year")
    try:
        saved_anchor_year = int(saved_anchor_year)
    except Exception:
        saved_anchor_year = latest_year
    if saved_anchor_year not in available_years:
        saved_anchor_year = latest_year

    col1, col2 = st.columns(2)
    with col1:
        historical_years = st.number_input(
            "Number of historical years to consider",
            min_value=3,
            max_value=10,
            value=int(baseline_settings["historical_years"]),
            step=1,
            key=f"{form_key}_upload_historical_years",
            help="Controls how many historical annual observations are used when calculating the DCF starting point.",
        )
    with col2:
        base_anchor_year = st.selectbox(
            "Base historical year anchor",
            options=available_years,
            index=available_years.index(int(saved_anchor_year)),
            key=f"{form_key}_upload_base_historical_year",
            help="The uploaded Year column must start in the year immediately after this anchor.",
        )

    st.caption(
        "Upload absolute forecast assumptions in the spreadsheet format. The app will compute and save the required Year-by-year ascend/descend %. "
        f"The first uploaded year must be {int(base_anchor_year) + 1}."
    )
    uploaded_file = st.file_uploader(
        "Assumptions spreadsheet",
        type=["xlsx", "xls"],
        key=f"{form_key}_assumption_upload_file",
        help="Expected columns: Year, Revenue Growth %, EBITDA Margin %, D&A % of Revenue, CAPEX % of Revenue, Working Capital Days, WACC %.",
    )

    parsed_df = None
    steps_by_metric = None
    validation_errors: List[str] = []
    if uploaded_file is not None:
        try:
            uploaded_file.seek(0)
        except Exception:
            pass
        parsed_df, validation_errors = _read_company_assumption_upload(uploaded_file, int(base_anchor_year))
        if validation_errors:
            for error in validation_errors:
                st.error(error)
        elif parsed_df is not None:
            anchor_config = {
                "use_base_historical_year": True,
                "base_historical_year": int(base_anchor_year),
                "forecast_year_limit": len(parsed_df),
                "use_baseline_overrides": False,
                "baseline_anchor_year": None,
                "baseline_overrides": {},
            }
            anchored_settings = _settings_with_anchor_config(baseline_settings, anchor_config)
            preview_context = _build_company_assumption_preview_context(
                conn,
                baseline_company_row,
                int(historical_years),
                float(_safe_float(baseline_settings.get("starting_projected_revenue_growth_cap")) or 25.0),
                anchored_settings,
            )
            steps_by_metric, conversion_errors = _compute_assumption_upload_steps_pct(parsed_df, preview_context)
            if conversion_errors:
                validation_errors.extend(conversion_errors)
                for error in conversion_errors:
                    st.error(error)

        if parsed_df is not None:
            st.markdown("**Uploaded absolute assumptions**")
            st.dataframe(_uploaded_assumptions_display_df(parsed_df), use_container_width=True, hide_index=True)
        if parsed_df is not None and steps_by_metric is not None and not validation_errors:
            st.markdown("**Computed Year-by-year ascend/descend % to be saved**")
            st.dataframe(
                _computed_upload_steps_display_df(steps_by_metric, parsed_df["year"].astype(int).tolist()),
                use_container_width=True,
                hide_index=True,
            )

    go_clicked = st.button(
        "Go",
        type="primary",
        key=f"{form_key}_apply_assumption_upload",
        disabled=uploaded_file is None or bool(validation_errors),
    )
    if not go_clicked:
        return None
    if parsed_df is None:
        st.error("Upload a valid assumptions spreadsheet before clicking Go.")
        return None
    if steps_by_metric is None:
        st.error("Could not compute Year-by-year ascend/descend % from the uploaded absolute assumptions.")
        return None

    return _build_company_assumption_upload_values(
        baseline_settings,
        int(historical_years),
        int(base_anchor_year),
        parsed_df,
        steps_by_metric,
    )


def _render_company_settings_tab(conn) -> None:
    st.subheader("Company - Level")

    companies_df = list_companies(conn)
    if companies_df.empty:
        st.info("No companies are available yet.")
        return

    label_map = company_label_map(companies_df)
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
    company_override_ids = (
        set(company_overrides_df["company_id"].astype(int).tolist())
        if company_overrides_df is not None and not company_overrides_df.empty and "company_id" in company_overrides_df.columns
        else set()
    )
    industry_override_ids = (
        set(industry_overrides_df["group_id"].astype(int).tolist())
        if industry_overrides_df is not None and not industry_overrides_df.empty and "group_id" in industry_overrides_df.columns
        else set()
    )
    company_source_map: Dict[int, str] = {int(company_id): "Company Override" for company_id in company_override_ids}
    if memberships_df is not None and not memberships_df.empty:
        for _, membership in memberships_df.iterrows():
            company_id = int(membership["company_id"])
            if company_id in company_source_map:
                continue
            group_id = int(membership["group_id"])
            if group_id in industry_override_ids:
                company_source_map[company_id] = f"Industry Override: {membership['group_name']}"
    selected_companies_df["Setting Source"] = selected_companies_df["id"].apply(
        lambda company_id: company_source_map.get(int(company_id), "General")
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

    baseline_company_row = selected_companies_df[selected_companies_df["id"] == baseline_company_id].iloc[0]
    if not st.session_state.get(_DCF_COMPANY_ASSUMPTION_UPLOAD_VISIBLE_KEY, False):
        if st.button("Upload assumptions", key="dcf_company_show_assumption_upload"):
            st.session_state[_DCF_COMPANY_ASSUMPTION_UPLOAD_VISIBLE_KEY] = True

    values = None
    if st.session_state.get(_DCF_COMPANY_ASSUMPTION_UPLOAD_VISIBLE_KEY, False):
        values = _render_company_assumption_upload_workflow(
            conn,
            baseline_settings,
            baseline_company_row,
            "dcf_company_assumption_upload",
        )
        if values is None:
            return
        if values.pop("__cancel_upload", False):
            values = None

    if values is None:
        values = _render_settings_form(
            baseline_settings,
            "Save Company Settings",
            "dcf_company_settings_form",
            company_level_paths=True,
            preview_conn=conn,
            preview_company_row=baseline_company_row,
        )
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
            values.get("projection_path_config"),
        )
        for company_id in selected_company_ids
    ]
    upsert_dcf_company_valuation_settings(conn, rows)
    st.session_state[_DCF_COMPANY_ASSUMPTION_UPLOAD_VISIBLE_KEY] = False
    st.success(f"Saved company-level DCF settings for {len(selected_company_ids)} compan(y/ies).")


def _render_settings_tab(conn) -> None:
    active_settings_tab = lazy_tab_bar(
        ["General", "Industry - Level", "Company - Level"],
        key="dcf_settings_tabs",
        default="General",
    )
    if active_settings_tab == "General":
        _render_general_settings_tab(conn)
    elif active_settings_tab == "Industry - Level":
        _render_industry_settings_tab(conn)
    elif active_settings_tab == "Company - Level":
        _render_company_settings_tab(conn)


def render_dcf_valuations_tab() -> None:
    st.title("Valuations")

    conn = get_db()
    active_model_tab = lazy_tab_bar(
        ["Discounted Cash Flow", "Valuation Dashboard"],
        key="valuation_model_tabs",
        default="Discounted Cash Flow",
    )

    if active_model_tab == "Discounted Cash Flow":
        active_dcf_tab = lazy_tab_bar(["Industry", "Company", "Settings"], key="dcf_primary_tabs", default="Industry")
        if active_dcf_tab == "Industry":
            _render_industry_tab(conn)
        elif active_dcf_tab == "Company":
            _render_company_tab(conn)
        elif active_dcf_tab == "Settings":
            _render_settings_tab(conn)
    elif active_model_tab == "Valuation Dashboard":
        _render_valuation_dashboards_tab(conn)
