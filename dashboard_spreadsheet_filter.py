"""Ticker-list selection shared by the three equity research dashboards."""

from dataclasses import dataclass, field
from hashlib import sha256
from io import BytesIO
from pathlib import Path

import pandas as pd
import streamlit as st

from core_backend import _canonicalize_country, read_df


def _text(value) -> str:
    return "" if value is None or pd.isna(value) else str(value).strip()


def _country(value) -> str:
    # An omitted country must never inherit the ingestion default of USA.
    value = _text(value)
    return _canonicalize_country(value) if value else ""


def parse_ticker_spreadsheet(data: bytes, filename: str) -> pd.DataFrame:
    """Read the first worksheet or a CSV, preserving ticker text and source rows."""
    extension = Path(filename).suffix.lower()
    try:
        if extension == ".xlsx":
            raw = pd.read_excel(
                BytesIO(data), header=None, dtype=str, keep_default_na=False,
                engine="openpyxl",
            )
        elif extension == ".csv":
            raw = pd.read_csv(
                BytesIO(data), header=None, dtype=str, keep_default_na=False,
                encoding="utf-8-sig", skip_blank_lines=False,
            )
        else:
            raise ValueError("Upload an Excel (.xlsx) or CSV (.csv) file.")
    except ValueError:
        raise
    except Exception as error:
        raise ValueError("The spreadsheet could not be read. Upload a valid .xlsx or .csv file.") from error

    if raw.empty:
        raise ValueError("The spreadsheet is empty. Include a Ticker column and at least one ticker.")
    headers = [_text(value).lstrip("\ufeff").casefold() for value in raw.iloc[0]]
    if "ticker" not in headers:
        raise ValueError("The spreadsheet must contain a Ticker column. Country is optional.")
    for name in ("ticker", "country"):
        if headers.count(name) > 1:
            raise ValueError(f"The spreadsheet contains more than one {name.title()} column.")
    ticker_index = headers.index("ticker")
    country_index = headers.index("country") if "country" in headers else None
    rows = []
    missing = []
    for index, row in raw.iloc[1:].iterrows():
        if not any(_text(value) for value in row):
            continue
        ticker = _text(row.iloc[ticker_index]).upper()
        if not ticker:
            missing.append(str(index + 1))
            continue
        rows.append({
            "Row": index + 1,
            "Ticker": ticker,
            "Country": _country(row.iloc[country_index]) if country_index is not None else "",
        })
    if missing:
        raise ValueError("Ticker is required on every non-empty row. Missing on row(s): " + ", ".join(missing) + ".")
    if not rows:
        raise ValueError("The spreadsheet has no tickers. Add at least one ticker below the header.")
    return pd.DataFrame(rows, columns=["Row", "Ticker", "Country"])


@dataclass
class TickerSelection:
    company_ids: list[int] = field(default_factory=list)
    preview: pd.DataFrame = field(default_factory=pd.DataFrame)
    country_conflicts: dict = field(default_factory=dict)
    company_conflicts: dict = field(default_factory=dict)
    unmatched_count: int = 0


def resolve_ticker_rows(requests, companies, *, country_choices=None, company_choices=None):
    """Resolve exact tickers and optional countries, never choosing an ambiguous match."""
    country_choices = country_choices or {}
    company_choices = company_choices or {}
    catalog = companies.copy()
    catalog["_ticker"] = catalog["ticker"].map(lambda value: _text(value).upper())
    catalog["_country"] = catalog["country"].map(_country)
    by_ticker = {ticker: group for ticker, group in catalog.groupby("_ticker", sort=False)}
    result = TickerSelection()
    seen = set()
    preview = []
    for row in requests.to_dict("records"):
        ticker, supplied_country = row["Ticker"], row["Country"]
        key = (ticker, supplied_country.casefold())
        candidates = by_ticker.get(ticker, catalog.iloc[:0])
        if supplied_country:
            candidates = candidates[candidates["_country"].str.casefold() == supplied_country.casefold()]
        country = supplied_country
        name = ""
        company_id = None
        if candidates.empty:
            status = "Ticker / Country not found" if supplied_country else "Ticker not found"
            result.unmatched_count += 1
        else:
            countries = sorted(set(candidates["_country"].replace("", "(not recorded)")))
            if not supplied_country and len(countries) > 1:
                chosen_country = country_choices.get(key)
                if chosen_country not in countries:
                    result.country_conflicts[key] = countries
                    status = "Country required"
                    preview.append({**row, "Company": name, "Status": status})
                    continue
                candidates = candidates[candidates["_country"].replace("", "(not recorded)") == chosen_country]
            if len(candidates) > 1:
                ids = {int(value) for value in candidates["id"]}
                chosen_company = company_choices.get(key)
                if chosen_company not in ids:
                    result.company_conflicts[key] = candidates[["id", "name", "ticker", "country"]].to_dict("records")
                    status = "Company selection required"
                    preview.append({**row, "Company": name, "Status": status})
                    continue
                candidates = candidates[candidates["id"] == chosen_company]
            match = candidates.iloc[0]
            company_id = int(match["id"])
            name, country = _text(match["name"]), match["_country"]
            if company_id in seen:
                status = "Duplicate — ignored"
            else:
                status = "Matched"
                seen.add(company_id)
                result.company_ids.append(company_id)
        preview.append({**row, "Country": country, "Company": name, "Status": status})
    result.company_ids.sort()
    result.preview = pd.DataFrame(preview, columns=["Row", "Ticker", "Country", "Company", "Status"])
    return result


def _clear_result(result_state_key):
    st.session_state.pop(result_state_key, None)


def select_spreadsheet_company_ids(conn, *, key, result_state_key):
    """Return validated IDs, blocking unresolved conflicts and unacknowledged omissions."""
    uploaded = st.file_uploader(
        "Upload ticker spreadsheet", type=["xlsx", "csv"], key=f"{key}_file",
        help="Ticker is required. Country is optional and requested only for unresolved company matches.",
        on_change=_clear_result, args=(result_state_key,),
    )
    st.caption("Ticker is required. Country is optional. The list selects existing company data; it does not upload financials.")
    if uploaded is None:
        return []
    data = uploaded.getvalue()
    try:
        requests = parse_ticker_spreadsheet(data, uploaded.name)
    except ValueError as error:
        st.error(str(error))
        return []

    companies = read_df("SELECT id, name, ticker, country FROM companies ORDER BY name, id", conn)
    fingerprint = sha256(data).hexdigest()[:16]

    def widget_key(kind, request):
        token = sha256(repr(request).encode()).hexdigest()[:16]
        return f"{key}_{fingerprint}_{kind}_{token}"

    base = resolve_ticker_rows(requests, companies)
    country_choices = {}
    for request, countries in base.country_conflicts.items():
        country_choices[request] = st.selectbox(
            f"Country for {request[0]}", countries, index=None,
            placeholder="Select Country to resolve this ticker",
            key=widget_key("country", request),
            on_change=_clear_result, args=(result_state_key,),
        )
    narrowed = resolve_ticker_rows(requests, companies, country_choices=country_choices)
    company_choices = {}
    for request, matches in narrowed.company_conflicts.items():
        labels = {int(match["id"]): f"{match['name']} ({match['ticker']}, {match['country']})" for match in matches}
        company_choices[request] = st.selectbox(
            f"Company for {request[0]}", list(labels), index=None,
            format_func=lambda company_id, labels=labels: labels[company_id],
            placeholder="Select the company for this ticker",
            key=widget_key("company", (request, tuple(labels))),
            on_change=_clear_result, args=(result_state_key,),
        )
    result = resolve_ticker_rows(
        requests, companies, country_choices=country_choices, company_choices=company_choices,
    )
    duplicate_count = int((result.preview["Status"] == "Duplicate — ignored").sum())
    st.caption(f"{len(result.company_ids)} matched companies · {duplicate_count} duplicate rows ignored · {result.unmatched_count} unmatched rows")
    unresolved = bool(result.country_conflicts or result.company_conflicts)
    with st.expander("Ticker preview", expanded=unresolved or bool(result.unmatched_count)):
        st.dataframe(result.preview, hide_index=True, use_container_width=True)
    if result.unmatched_count:
        unmatched = result.preview.loc[
            result.preview["Status"].isin(["Ticker not found", "Ticker / Country not found"]),
            ["Row", "Ticker", "Country", "Status"],
        ].copy()
        unmatched["Country"] = unmatched["Country"].replace("", "Not provided")
        st.markdown("**Unmatched rows**")
        st.dataframe(
            unmatched, hide_index=True, use_container_width=True,
            key=f"{key}_unmatched_rows",
        )
    if unresolved:
        st.warning("Resolve the ticker conflicts above before computing scores.")
        return []
    if not result.company_ids:
        st.warning("No companies match this ticker list.")
        return []
    if result.unmatched_count:
        st.warning("Some rows do not match existing companies. Review the ticker preview before continuing.")
        if not st.checkbox(
            f"Continue with {len(result.company_ids)} matched companies and skip {result.unmatched_count} unmatched rows",
            key=f"{key}_{fingerprint}_skip_{sha256(repr(result.company_ids).encode()).hexdigest()[:16]}",
            on_change=_clear_result, args=(result_state_key,),
        ):
            return []
    return result.company_ids
