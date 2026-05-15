from datetime import datetime
import csv
import io
import json
import re
from typing import Dict, List, Optional, Tuple
import urllib.error
import urllib.request

import altair as alt
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
from ui_theme import dashboard_section, display_table_frame, render_dashboard_table


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
    series = _build_required_series(conn, company_id)
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
        (
            (float(ncwc_value) * 365.0) / float(revenue_value)
            if ncwc_value is not None and revenue_value not in (None, 0)
            else None
        )
        for ncwc_value, revenue_value in [
            (_safe_float(series["ncwc"].get(year)), _safe_float(revenue_series.get(year)))
            for year in actual_years
        ]
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
        for idx in range(1, 11):
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
    for idx in range(1, 11):
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
        chart = (
            alt.Chart(chart_df)
            .mark_line(point=True, strokeWidth=3)
            .encode(
                x=alt.X("Year/FY:N", sort=alt.SortField(field="__order", order="ascending"), title="Year / Forecast Year"),
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

    summary_col, assumption_col = st.columns([1, 1])
    with summary_col:
        dashboard_section("Summary")
        if not summary_df.empty:
            render_dashboard_table(summary_df, key="dcf_expanded_summary")
    with assumption_col:
        dashboard_section("Assumptions")
        if not assumptions_df.empty:
            render_dashboard_table(assumptions_df, key="dcf_expanded_assumptions")

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
    selector_col, action_col = st.columns([3, 2])
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

    detail_view = st.segmented_control(
        "Detail view",
        options=["Expanded Valuation", "Valuation Insights"],
        default=st.session_state.get(_DCF_COMPANY_DETAIL_VIEW_KEY, "Expanded Valuation"),
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

    values = _render_settings_form(
        baseline_settings,
        "Save Company Settings",
        "dcf_company_settings_form",
        company_level_paths=True,
        preview_conn=conn,
        preview_company_row=selected_companies_df[selected_companies_df["id"] == baseline_company_id].iloc[0],
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
