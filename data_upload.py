import streamlit as st
import math
import io
from pathlib import Path
from datetime import datetime
import pandas as pd
from core import *  # noqa: F401,F403


def _bulk_upload_dir() -> Path:
    return Path(__file__).resolve().parent / "Bulk_Upload_Financials"


def _find_manifest_file(folder: Path) -> Path:
    manifest_candidates = [
        p for p in folder.iterdir()
        if p.is_file() and p.suffix.lower() == ".xlsx" and p.stem.lower().startswith("manifest")
    ]
    if not manifest_candidates:
        raise FileNotFoundError("No manifest .xlsx file found. Expected a file named like 'manifest.xlsx'.")
    if len(manifest_candidates) > 1:
        names = ", ".join(sorted(p.name for p in manifest_candidates))
        raise ValueError(f"Multiple manifest .xlsx files found: {names}. Keep only one manifest file.")
    return manifest_candidates[0]


def _load_manifest_df(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix != ".xlsx":
        raise ValueError("Manifest must be an .xlsx file.")
    df = pd.read_excel(path)
    if df is None or df.empty:
        raise ValueError("Manifest file is empty.")
    return df


def _normalize_manifest_df(df: pd.DataFrame) -> pd.DataFrame:
    col_map = {str(c).strip().lower(): c for c in df.columns}

    def _pick(*names: str) -> str | None:
        for name in names:
            key = name.strip().lower()
            if key in col_map:
                return col_map[key]
        return None

    ticker_col = _pick("ticker")
    company_col = _pick("company name", "company", "company_name", "name")
    bucket_col = _pick("industry bucket", "industry", "bucket", "industry_bucket")
    country_col = _pick("country", "country/region", "nation")

    missing = [label for label, col in {
        "Ticker": ticker_col,
        "Company Name": company_col,
        "Industry Bucket": bucket_col,
        "Country": country_col,
    }.items() if col is None]
    if missing:
        raise ValueError(f"Manifest is missing required column(s): {', '.join(missing)}.")

    out = df[[ticker_col, company_col, bucket_col, country_col]].copy()
    out.columns = ["ticker", "company_name", "industry_bucket", "country"]
    out = out.fillna("")
    out["ticker"] = out["ticker"].astype(str).str.strip().str.upper()
    out["company_name"] = out["company_name"].astype(str).str.strip()
    out["industry_bucket"] = out["industry_bucket"].astype(str).str.strip()
    out["country"] = out["country"].astype(str).str.strip().str.upper()
    out = out[out["ticker"] != ""]

    if (out["country"] == "").any():
        raise ValueError("Manifest Country column must be populated for every row.")

    dupes = out[out["ticker"].duplicated()]["ticker"].unique().tolist()
    if dupes:
        raise ValueError(f"Duplicate tickers in manifest: {', '.join(sorted(dupes))}.")
    return out


def _bucket_has_betas(conn, bucket_name: str) -> bool:
    if not bucket_name:
        return False
    sql = """
        SELECT COUNT(*)
        FROM industry_betas
        WHERE user_industry_bucket = ?
          AND unlevered_beta IS NOT NULL
          AND cash_adjusted_beta IS NOT NULL
    """
    df = read_df(sql, conn, params=(bucket_name,))
    if df.empty:
        return False
    return int(df.iloc[0, 0]) > 0


def _render_bulk_status(placeholder, company_label: str, status: str, percent: str, color: str) -> None:
    safe_label = company_label or "Unknown"
    html = (
        "<div style='padding:4px 0;'>"
        f"<span style='font-weight:600'>{safe_label}</span> - "
        f"<span style='color:{color};font-weight:600'>{status}</span> - "
        f"<span style='color:{color};font-weight:600'>{percent}</span>"
        "</div>"
    )
    placeholder.markdown(html, unsafe_allow_html=True)


def ingest_financials_bytes(
    bytes_data: bytes,
    company: str,
    ticker: str,
    conn,
    country: Optional[str] = None,
) -> Dict[str, object]:
    # Extract Revenue (annual + TTM)
    annual_rev = extract_annual_revenue_series(bytes_data)
    as_of_rev, ttm_rev = extract_latest_ttm_revenue(bytes_data)

    # Merge current-year TTM into the annual revenue series
    try:
        ttm_year = int(str(as_of_rev)[:4])
        annual_rev[ttm_year] = float(ttm_rev)
    except Exception:
        pass

    # Extract Cost of Revenue / COGS (annual + TTM)
    annual_cogs = extract_annual_cost_of_revenue_series(bytes_data)
    as_of_cogs, ttm_cogs = extract_latest_ttm_cost_of_revenue(bytes_data)

    # Merge current-year TTM into the annual COGS series
    try:
        ttm_cogs_year = int(str(as_of_cogs)[:4])
        annual_cogs[ttm_cogs_year] = float(ttm_cogs)
    except Exception:
        pass

    # Extract SG&A (annual + TTM)
    annual_sga = extract_annual_sga_series(bytes_data)
    as_of_sga, ttm_sga = extract_latest_ttm_sga(bytes_data)

    # Merge current-year TTM into the annual SG&A series
    try:
        ttm_sga_year = int(str(as_of_sga)[:4])
        annual_sga[ttm_sga_year] = float(ttm_sga)
    except Exception:
        pass

    # Extract Operating Margin (annual + TTM)
    annual_om = extract_annual_operating_margin_series(bytes_data)
    as_of_om, ttm_om = extract_latest_ttm_operating_margin(bytes_data)

    # Merge current-year TTM into the annual operating margin series
    try:
        ttm_om_year = int(str(as_of_om)[:4])
        annual_om[ttm_om_year] = float(ttm_om)
    except Exception:
        pass

    # Extract Pretax Income (annual + TTM)
    annual_pt = extract_annual_pretax_income_series(bytes_data)
    as_of_pt, ttm_pt = extract_latest_ttm_pretax_income(bytes_data)

    # Merge current-year TTM into the annual pretax income series
    try:
        ttm_pt_year = int(str(as_of_pt)[:4])
        annual_pt[ttm_pt_year] = float(ttm_pt)
    except Exception:
        pass

    # Extract Net Income (annual + TTM)
    annual_ni = extract_annual_net_income_series(bytes_data)
    as_of_ni, ttm_ni = extract_latest_ttm_net_income(bytes_data)

    # Merge current-year TTM into the annual net income series
    try:
        ttm_ni_year = int(str(as_of_ni)[:4])
        annual_ni[ttm_ni_year] = float(ttm_ni)
    except Exception:
        pass

    # Extract Effective Tax Rate (annual + TTM)
    annual_tax = extract_annual_effective_tax_rate_series(bytes_data)
    as_of_tax, ttm_tax = extract_latest_ttm_effective_tax_rate(bytes_data)

    # Merge current-year TTM into the annual effective tax rate series
    try:
        ttm_tax_year = int(str(as_of_tax)[:4])
        annual_tax[ttm_tax_year] = float(ttm_tax)
    except Exception:
        pass

    # Extract EBIT (annual + TTM)
    annual_ebit = extract_annual_ebit_series(bytes_data)
    as_of_ebit, ttm_ebit = extract_latest_ttm_ebit(bytes_data)

    # Merge current-year TTM into the annual EBIT series
    try:
        ttm_ebit_year = int(str(as_of_ebit)[:4])
        annual_ebit[ttm_ebit_year] = float(ttm_ebit)
    except Exception:
        pass

    # Extract EBITDA (annual + TTM)
    annual_ebitda = extract_annual_ebitda_series(bytes_data)
    as_of_ebitda, ttm_ebitda = extract_latest_ttm_ebitda(bytes_data)

    # Merge current-year TTM into the annual EBITDA series
    try:
        ttm_ebitda_year = int(str(as_of_ebitda)[:4])
        annual_ebitda[ttm_ebitda_year] = float(ttm_ebitda)
    except Exception:
        pass

    # Extract Interest Expense (annual + TTM)
    annual_interest_expense = extract_annual_interest_expense_series(bytes_data)
    as_of_interest_expense, ttm_interest_expense = extract_latest_ttm_interest_expense(bytes_data)

    # Merge current-year TTM into the annual Interest Expense series
    try:
        ttm_ie_year = int(str(as_of_interest_expense)[:4])
        annual_interest_expense[ttm_ie_year] = float(ttm_interest_expense)
    except Exception:
        pass

    # Extract Operating Income (annual + TTM)
    annual_operating_income = extract_annual_operating_income_series(bytes_data)
    as_of_operating_income, ttm_operating_income = extract_latest_ttm_operating_income(bytes_data)

    # Merge current-year TTM into the annual Operating Income series
    try:
        ttm_oi_year = int(str(as_of_operating_income)[:4])
        annual_operating_income[ttm_oi_year] = float(ttm_operating_income)
    except Exception:
        pass

    # Extract Research & Development Expense (annual + TTM) from Income
    annual_rd = extract_annual_research_and_development_expense_series(bytes_data)
    as_of_rd, ttm_rd = extract_latest_ttm_research_and_development_expense(bytes_data)

    # Merge current-year TTM into the annual R&D expense series
    try:
        ttm_rd_year = int(str(as_of_rd)[:4])
        annual_rd[ttm_rd_year] = float(ttm_rd)
    except Exception:
        pass

    # Defensive: keep R&D expense positive
    annual_rd = {y: abs(float(v)) for y, v in annual_rd.items()}

    # Extract Capital Expenditures (annual + TTM) from Cash Flow
    annual_capex = extract_annual_capital_expenditures_series(bytes_data)
    as_of_capex, ttm_capex = extract_latest_ttm_capital_expenditures(bytes_data)

    # Merge current-year TTM into the annual CapEx series
    try:
        ttm_capex_year = int(str(as_of_capex)[:4])
        annual_capex[ttm_capex_year] = float(ttm_capex)
    except Exception:
        pass

    # Store CapEx as positive numbers (sheet often shows outflows as negatives)
    annual_capex = {y: -float(v) for y, v in annual_capex.items()}

    # Extract Depreciation & Amortization (annual + TTM) from Cash Flow
    annual_da = extract_annual_depreciation_amortization_series(bytes_data)
    as_of_da, ttm_da = extract_latest_ttm_depreciation_amortization(bytes_data)

    # Merge current-year TTM into the annual D&A series
    try:
        ttm_da_year = int(str(as_of_da)[:4])
        annual_da[ttm_da_year] = float(ttm_da)
    except Exception:
        pass

    # Extract Net Debt Issued/Paid (annual + TTM) from Cash Flow
    annual_net_debt_issued_paid = extract_annual_net_debt_issued_paid_series(bytes_data)
    as_of_net_debt_issued_paid, ttm_net_debt_issued_paid = extract_latest_ttm_net_debt_issued_paid(bytes_data)

    # Merge current-year TTM into the annual Net Debt Issued/Paid series
    try:
        ttm_nd_year = int(str(as_of_net_debt_issued_paid)[:4])
        annual_net_debt_issued_paid[ttm_nd_year] = float(ttm_net_debt_issued_paid)
    except Exception:
        pass

    # Extract Shareholders Equity (annual + TTM) from Balance Sheet
    annual_se = extract_annual_shareholders_equity_series(bytes_data)
    as_of_se, ttm_se = extract_latest_ttm_shareholders_equity(bytes_data)

    # Merge current-year TTM into the annual Shareholders Equity series
    try:
        ttm_se_year = int(str(as_of_se)[:4])
        annual_se[ttm_se_year] = float(ttm_se)
    except Exception:
        pass

    # Extract Short-Term Investments (annual + TTM) from Balance Sheet
    annual_short_term_investments = extract_annual_short_term_investments_series(bytes_data)
    as_of_short_term_investments, ttm_short_term_investments = extract_latest_ttm_short_term_investments(bytes_data)

    # Merge current-year TTM into the annual Short-Term Investments series
    try:
        ttm_sti_year = int(str(as_of_short_term_investments)[:4])
        annual_short_term_investments[ttm_sti_year] = float(ttm_short_term_investments)
    except Exception:
        pass

    # Extract Accounts Receivable (annual + TTM) from Balance Sheet
    annual_accounts_receivable = extract_annual_accounts_receivable_series(bytes_data)
    as_of_accounts_receivable, ttm_accounts_receivable = extract_latest_ttm_accounts_receivable(bytes_data)

    # Merge current-year TTM into the annual Accounts Receivable series
    try:
        ttm_ar_year = int(str(as_of_accounts_receivable)[:4])
        annual_accounts_receivable[ttm_ar_year] = float(ttm_accounts_receivable)
    except Exception:
        pass

    # Extract Retained Earnings (annual + TTM) from Balance Sheet
    annual_re = extract_annual_retained_earnings_series(bytes_data)
    as_of_re, ttm_re = extract_latest_ttm_retained_earnings(bytes_data)

    # Merge current-year TTM into the annual Retained Earnings series
    try:
        ttm_re_year = int(str(as_of_re)[:4])
        annual_re[ttm_re_year] = float(ttm_re)
    except Exception:
        pass

    # Extract Comprehensive Income (annual + TTM) from Balance Sheet
    annual_ci = extract_annual_comprehensive_income_series(bytes_data)
    as_of_ci, ttm_ci = extract_latest_ttm_comprehensive_income(bytes_data)

    # Merge current-year TTM into the annual Comprehensive Income series
    try:
        ttm_ci_year = int(str(as_of_ci)[:4])
        annual_ci[ttm_ci_year] = float(ttm_ci)
    except Exception:
        pass

    # Extract Total Assets (annual + TTM) from Balance Sheet
    annual_total_assets = extract_annual_total_assets_series(bytes_data)
    as_of_total_assets, ttm_total_assets = extract_latest_ttm_total_assets(bytes_data)
    try:
        ttm_ta_year = int(str(as_of_total_assets)[:4])
        annual_total_assets[ttm_ta_year] = float(ttm_total_assets)
    except Exception:
        pass

    # Extract Total Current Assets (annual + TTM) from Balance Sheet
    annual_total_current_assets = extract_annual_total_current_assets_series(bytes_data)
    as_of_total_current_assets, ttm_total_current_assets = extract_latest_ttm_total_current_assets(bytes_data)
    try:
        ttm_tca_year = int(str(as_of_total_current_assets)[:4])
        annual_total_current_assets[ttm_tca_year] = float(ttm_total_current_assets)
    except Exception:
        pass

    # Extract Total Current Liabilities (annual + TTM) from Balance Sheet
    annual_total_current_liabilities = extract_annual_total_current_liabilities_series(bytes_data)
    as_of_total_current_liabilities, ttm_total_current_liabilities = extract_latest_ttm_total_current_liabilities(bytes_data)
    try:
        ttm_tcl_year = int(str(as_of_total_current_liabilities)[:4])
        annual_total_current_liabilities[ttm_tcl_year] = float(ttm_total_current_liabilities)
    except Exception:
        pass

    # Extract Total Long-Term Liabilities (annual + TTM) from Balance Sheet
    annual_total_long_term_liabilities = extract_annual_total_long_term_liabilities_series(bytes_data)
    as_of_total_long_term_liabilities, ttm_total_long_term_liabilities = extract_latest_ttm_total_long_term_liabilities(bytes_data)
    try:
        ttm_tltl_year = int(str(as_of_total_long_term_liabilities)[:4])
        annual_total_long_term_liabilities[ttm_tltl_year] = float(ttm_total_long_term_liabilities)
    except Exception:
        pass

    # Extract Total Debt (annual + TTM) from Balance Sheet
    annual_total_debt = extract_annual_total_debt_series(bytes_data)
    as_of_total_debt, ttm_total_debt = extract_latest_ttm_total_debt(bytes_data)
    try:
        ttm_td_year = int(str(as_of_total_debt)[:4])
        annual_total_debt[ttm_td_year] = float(ttm_total_debt)
    except Exception:
        pass

    # Extract Market Capitalization (annual + latest TTM) from Ratios
    annual_market_capitalization = extract_annual_market_capitalization_series(bytes_data)
    as_of_market_capitalization, ttm_market_capitalization = extract_latest_ttm_market_capitalization(bytes_data)

    # Use annual ratios up to the prior fiscal year, and the latest Ratios-TTM value for the current year
    try:
        ttm_mc_year = int(str(as_of_market_capitalization)[:4])
        prev_year = ttm_mc_year - 1
        annual_market_capitalization = {
            int(y): float(v)
            for y, v in annual_market_capitalization.items()
            if int(y) <= int(prev_year)
        }
        annual_market_capitalization[int(ttm_mc_year)] = float(ttm_market_capitalization)
    except Exception:
        pass

    # Extract Return on Invested Capital (ROIC)% (annual + latest TTM) from Ratios
    annual_roic_direct_upload = extract_annual_roic_direct_upload_series(bytes_data)
    as_of_roic, ttm_roic = extract_latest_ttm_roic_direct_upload(bytes_data)

    def _to_pct_points(v: float) -> float:
        fv = float(v)
        return fv * 100.0 if abs(fv) <= 1.5 else fv

    # Use annual ratios up to the prior fiscal year, and the latest Ratios-TTM value for the current year
    try:
        ttm_roic_year = int(str(as_of_roic)[:4])
        prev_year = ttm_roic_year - 1
        annual_roic_direct_upload = {
            int(y): _to_pct_points(v)
            for y, v in annual_roic_direct_upload.items()
            if int(y) <= int(prev_year)
        }
        annual_roic_direct_upload[int(ttm_roic_year)] = _to_pct_points(ttm_roic)
    except Exception:
        annual_roic_direct_upload = {int(y): _to_pct_points(v) for y, v in annual_roic_direct_upload.items()}

    # Extract Current Debt (annual + TTM) from Balance Sheet
    annual_current_debt = extract_annual_current_debt_series(bytes_data)
    as_of_current_debt, ttm_current_debt = extract_latest_ttm_current_debt(bytes_data)
    try:
        ttm_cd_year = int(str(as_of_current_debt)[:4])
        annual_current_debt[ttm_cd_year] = float(ttm_current_debt)
    except Exception:
        pass

    # Extract Cash & Cash Equivalents (annual + TTM) from Balance Sheet
    annual_cash_and_cash_equivalents = extract_annual_cash_and_cash_equivalents_series(bytes_data)
    as_of_cash_and_cash_equivalents, ttm_cash_and_cash_equivalents = extract_latest_ttm_cash_and_cash_equivalents(bytes_data)
    try:
        ttm_cce_year = int(str(as_of_cash_and_cash_equivalents)[:4])
        annual_cash_and_cash_equivalents[ttm_cce_year] = float(ttm_cash_and_cash_equivalents)
    except Exception:
        pass

    # Extract Long-Term Investments (annual + TTM) from Balance Sheet
    annual_long_term_investments = extract_annual_long_term_investments_series(bytes_data)
    as_of_long_term_investments, ttm_long_term_investments = extract_latest_ttm_long_term_investments(bytes_data)
    try:
        ttm_lti_year = int(str(as_of_long_term_investments)[:4])
        annual_long_term_investments[ttm_lti_year] = float(ttm_long_term_investments)
    except Exception:
        pass

    # Compute NOPAT (Net Operating Profit After Tax) per year
    annual_nopat: Dict[int, float] = {}
    for year, ebit_val in annual_ebit.items():
        tax_rate = annual_tax.get(year)
        if tax_rate is None:
            continue
        try:
            annual_nopat[year] = float(ebit_val) * (1.0 - float(tax_rate))
        except Exception:
            continue
        pass

    # Compute Interest Coverage Ratio (Operating Income / Interest Expense) per year
    annual_interest_coverage: Dict[int, float] = {}
    ic_years = set(annual_operating_income.keys())
    for year in ic_years:
        try:
            raw_ie = annual_interest_expense.get(year)
            if raw_ie is None:
                ie_val = 0.01
            else:
                ie_float = float(raw_ie)
                if math.isnan(ie_float) or ie_float == 0.0:
                    ie_val = 0.01
                else:
                    ie_val = ie_float
            oi_val = float(annual_operating_income.get(year, 0.0))
            annual_interest_coverage[year] = oi_val / ie_val
        except Exception:
            continue

    # Compute Interest Load % per year from Interest Coverage Ratio
    annual_interest_load_pct: Dict[int, float] = {}
    for year, cov in annual_interest_coverage.items():
        try:
            cov_val = float(cov)
            if cov_val == 0.0 or math.isnan(cov_val):
                annual_interest_load_pct[year] = 0.001
            else:
                annual_interest_load_pct[year] = (1.0 / cov_val) * 100.0
        except Exception:
            annual_interest_load_pct[year] = 0.001

    # Compute Accumulated Profit (Retained Earnings + Comprehensive Income) per year
    annual_accumulated_profit: Dict[int, float] = {}
    common_years = set(annual_re.keys()) & set(annual_ci.keys())
    for year in common_years:
        try:
            re_val = float(annual_re[year])
            ci_val = float(annual_ci[year])
            annual_accumulated_profit[year] = re_val + ci_val
        except Exception:
            continue

    # Compute Total Equity per year (Total Equity = Shareholders Equity)
    annual_total_equity: Dict[int, float] = {}
    for year, se_raw in annual_se.items():
        try:
            annual_total_equity[int(year)] = float(se_raw)
        except Exception:
            continue

    # Compute Average Equity per year
    annual_average_equity: Dict[int, float] = {}
    for year in sorted(annual_total_equity.keys()):
        prev_year = year - 1
        if prev_year in annual_total_equity:
            try:
                avg_eq = 0.5 * (float(annual_total_equity[prev_year]) + float(annual_total_equity[year]))
                annual_average_equity[year] = avg_eq
            except Exception:
                continue

    # Compute ROE per year = Net Income / Average Equity
    annual_roe: Dict[int, float] = {}
    for year, avg_eq in annual_average_equity.items():
        ni_val = annual_ni.get(year)
        if ni_val is None:
            continue
        try:
            avg_eq_f = float(avg_eq)
            if avg_eq_f != 0.0:
                annual_roe[year] = float(ni_val) / avg_eq_f
        except Exception:
            continue

    # Compute Capital Employed per year = Shareholders Equity + Total Long-Term Liabilities
    annual_capital_employed: Dict[int, float] = {}
    common_ce_years = set(annual_se.keys()) & set(annual_total_long_term_liabilities.keys())
    for year in common_ce_years:
        try:
            se_val = float(annual_se[year])
            tlt_liab_val = float(annual_total_long_term_liabilities[year])
            annual_capital_employed[year] = se_val + tlt_liab_val
        except Exception:
            continue

    # Compute ROCE per year = EBIT / Average Capital Employed
    annual_roce: Dict[int, float] = {}
    ce_years_sorted = sorted(annual_capital_employed.keys())
    for year in ce_years_sorted:
        prev_year = year - 1
        if prev_year not in annual_capital_employed:
            continue
        ebit_val = annual_ebit.get(year)
        if ebit_val is None:
            continue
        try:
            ce_current = float(annual_capital_employed[year])
            ce_prev = float(annual_capital_employed[prev_year])
            avg_ce = 0.5 * (ce_current + ce_prev)
            if avg_ce != 0.0:
                annual_roce[year] = float(ebit_val) / avg_ce
        except Exception:
            continue

    # Compute Invested Capital per year
    annual_invested_capital: Dict[int, float] = {}
    common_ic_years = (
        set(annual_se.keys())
        & set(annual_total_debt.keys())
        & set(annual_cash_and_cash_equivalents.keys())
        & set(annual_long_term_investments.keys())
    )
    for year in common_ic_years:
        try:
            se_val = float(annual_se[year])
            debt_val = float(annual_total_debt[year])
            cash_val = float(annual_cash_and_cash_equivalents[year])
            lti_val = float(annual_long_term_investments[year])
            annual_invested_capital[year] = se_val + debt_val - cash_val - lti_val
        except Exception:
            continue

    # Compute Non-Cash Working Capital per year
    annual_non_cash_working_capital: Dict[int, float] = {}
    common_ncwc_years = (
        set(annual_total_current_assets.keys())
        & set(annual_cash_and_cash_equivalents.keys())
        & set(annual_total_current_liabilities.keys())
        & set(annual_current_debt.keys())
    )
    for year in common_ncwc_years:
        try:
            tca_val = float(annual_total_current_assets[year])
            cce_val = float(annual_cash_and_cash_equivalents[year])
            tcl_val = float(annual_total_current_liabilities[year])
            cd_val = float(annual_current_debt[year])
            annual_non_cash_working_capital[year] = (tca_val - cce_val) - (tcl_val - cd_val)
        except Exception:
            continue

    # Compute Revenue Yield of Non-Cash Working Capital % per year
    annual_revenue_yield_non_cash_working_capital: Dict[int, float] = {}
    common_ry_years = set(annual_non_cash_working_capital.keys()) & set(annual_rev.keys())
    for year in common_ry_years:
        try:
            ncwc_val = float(annual_non_cash_working_capital[year])
            rev_val = float(annual_rev[year])
            if rev_val == 0:
                continue
            annual_revenue_yield_non_cash_working_capital[year] = 1.0 - (ncwc_val / rev_val)
        except Exception:
            continue

    # Store
    cid = upsert_company(conn, company, ticker, country=country)

    upsert_annual_revenues(conn, cid, annual_rev)
    upsert_annual_cost_of_revenue(conn, cid, annual_cogs)
    upsert_ttm_cost_of_revenue(conn, cid, as_of_cogs, ttm_cogs)
    upsert_annual_sga(conn, cid, annual_sga)
    upsert_ttm_sga(conn, cid, as_of_sga, ttm_sga)
    upsert_annual_ebitda(conn, cid, annual_ebitda)
    upsert_ttm_ebitda(conn, cid, as_of_ebitda, ttm_ebitda)
    upsert_ttm(conn, cid, as_of_rev, ttm_rev)

    upsert_annual_op_margin(conn, cid, annual_om)
    upsert_ttm_op_margin(conn, cid, as_of_om, ttm_om)

    upsert_annual_pretax_income(conn, cid, annual_pt)
    upsert_ttm_pretax_income(conn, cid, as_of_pt, ttm_pt)

    upsert_annual_net_income(conn, cid, annual_ni)
    upsert_ttm_net_income(conn, cid, as_of_ni, ttm_ni)

    upsert_annual_eff_tax_rate(conn, cid, annual_tax)
    upsert_ttm_eff_tax_rate(conn, cid, as_of_tax, ttm_tax)

    upsert_annual_ebit(conn, cid, annual_ebit)
    upsert_ttm_ebit(conn, cid, as_of_ebit, ttm_ebit)

    upsert_annual_interest_expense(conn, cid, annual_interest_expense)
    upsert_ttm_interest_expense(conn, cid, as_of_interest_expense, ttm_interest_expense)

    upsert_annual_operating_income(conn, cid, annual_operating_income)
    upsert_ttm_operating_income(conn, cid, as_of_operating_income, ttm_operating_income)

    upsert_annual_interest_coverage(conn, cid, annual_interest_coverage)
    upsert_annual_interest_load(conn, cid, annual_interest_load_pct)

    try:
        compute_and_store_default_spread(conn, cid)
    except Exception:
        pass

    try:
        compute_and_store_pre_tax_cost_of_debt(conn, cid)
    except Exception:
        pass

    upsert_annual_nopat(conn, cid, annual_nopat)

    upsert_annual_shareholders_equity(conn, cid, annual_se)
    upsert_ttm_shareholders_equity(conn, cid, as_of_se, ttm_se)
    upsert_annual_short_term_investments(conn, cid, annual_short_term_investments)
    upsert_ttm_short_term_investments(conn, cid, as_of_short_term_investments, ttm_short_term_investments)
    upsert_annual_accounts_receivable(conn, cid, annual_accounts_receivable)
    upsert_ttm_accounts_receivable(conn, cid, as_of_accounts_receivable, ttm_accounts_receivable)

    upsert_annual_retained_earnings(conn, cid, annual_re)
    upsert_ttm_retained_earnings(conn, cid, as_of_re, ttm_re)

    upsert_annual_comprehensive_income(conn, cid, annual_ci)
    upsert_ttm_comprehensive_income(conn, cid, as_of_ci, ttm_ci)

    upsert_annual_accumulated_profit(conn, cid, annual_accumulated_profit)
    upsert_annual_total_equity(conn, cid, annual_total_equity)
    upsert_annual_average_equity(conn, cid, annual_average_equity)
    upsert_annual_roe(conn, cid, annual_roe)

    upsert_annual_total_assets(conn, cid, annual_total_assets)
    upsert_ttm_total_assets(conn, cid, as_of_total_assets, ttm_total_assets)

    upsert_annual_total_current_assets(conn, cid, annual_total_current_assets)
    upsert_ttm_total_current_assets(conn, cid, as_of_total_current_assets, ttm_total_current_assets)

    upsert_annual_total_current_liabilities(conn, cid, annual_total_current_liabilities)
    upsert_ttm_total_current_liabilities(conn, cid, as_of_total_current_liabilities, ttm_total_current_liabilities)

    upsert_annual_total_long_term_liabilities(conn, cid, annual_total_long_term_liabilities)
    upsert_ttm_total_long_term_liabilities(conn, cid, as_of_total_long_term_liabilities, ttm_total_long_term_liabilities)

    upsert_annual_total_debt(conn, cid, annual_total_debt)
    upsert_ttm_total_debt(conn, cid, as_of_total_debt, ttm_total_debt)

    upsert_annual_market_capitalization(conn, cid, annual_market_capitalization)

    if annual_roic_direct_upload:
        upsert_annual_roic_direct_upload(conn, cid, annual_roic_direct_upload)

    # Debt/Equity (Derived) = Total Debt / Market Capitalization
    annual_debt_equity: Dict[int, float] = {}
    try:
        common_de_years = sorted(
            set(annual_total_debt.keys())
            & set(annual_market_capitalization.keys())
        )
        for y in common_de_years:
            denom = float(annual_market_capitalization[y])
            if denom == 0.0:
                continue
            annual_debt_equity[int(y)] = float(annual_total_debt[y]) / denom
    except Exception:
        annual_debt_equity = {}

    if annual_debt_equity:
        upsert_annual_debt_equity(conn, cid, annual_debt_equity)

    # Levered Beta (Derived)
    try:
        compute_and_store_levered_beta(conn, cid)
    except Exception:
        pass

    # Derived: Cost of Equity
    try:
        compute_and_store_cost_of_equity(conn, cid)
    except Exception:
        pass

    # Derived: WACC
    try:
        compute_and_store_wacc(conn, cid)
    except Exception:
        pass

    # Derived: Spread% = ROIC% - WACC%
    try:
        compute_and_store_roic_wacc_spread(conn, cid)
    except Exception:
        pass

    upsert_annual_current_debt(conn, cid, annual_current_debt)
    upsert_ttm_current_debt(conn, cid, as_of_current_debt, ttm_current_debt)

    upsert_annual_cash_and_cash_equivalents(conn, cid, annual_cash_and_cash_equivalents)
    upsert_ttm_cash_and_cash_equivalents(conn, cid, as_of_cash_and_cash_equivalents, ttm_cash_and_cash_equivalents)

    upsert_annual_long_term_investments(conn, cid, annual_long_term_investments)
    upsert_ttm_long_term_investments(conn, cid, as_of_long_term_investments, ttm_long_term_investments)

    upsert_annual_capital_employed(conn, cid, annual_capital_employed)
    upsert_annual_roce(conn, cid, annual_roce)
    upsert_annual_invested_capital(conn, cid, annual_invested_capital)
    upsert_annual_non_cash_working_capital(conn, cid, annual_non_cash_working_capital)
    upsert_annual_revenue_yield_non_cash_working_capital(conn, cid, annual_revenue_yield_non_cash_working_capital)

    upsert_annual_research_and_development_expense(conn, cid, annual_rd)
    upsert_annual_net_debt_issued_paid(conn, cid, annual_net_debt_issued_paid)
    upsert_annual_capital_expenditures(conn, cid, annual_capex)
    upsert_annual_depreciation_amortization(conn, cid, annual_da)

    return {
        "company_id": cid,
        "annual_rev": annual_rev,
        "as_of_rev": as_of_rev,
        "ttm_rev": ttm_rev,
        "annual_om": annual_om,
        "as_of_om": as_of_om,
        "ttm_om": ttm_om,
        "annual_pt": annual_pt,
        "as_of_pt": as_of_pt,
        "ttm_pt": ttm_pt,
        "annual_ni": annual_ni,
        "as_of_ni": as_of_ni,
        "ttm_ni": ttm_ni,
        "annual_nopat": annual_nopat,
    }

def render_data_upload_tab():
    st.title("Revenue Growth & Operating Margin — Upload, Store, and Analyze")

    tab_fundamentals, tab_bulk, tab_price = st.tabs(["Fundamentals", "Bulk Upload", "Price"])

    with tab_fundamentals:
        with st.expander("1) Upload spreadsheet and store into the app database", expanded=True):
            st.write("Upload the spreadsheet in the same format. Provide the company and ticker as 'Company Name (TICKER)'.")
            col1, col2 = st.columns([2, 3])
            with col1:
                comp_input = st.text_input("Company (Ticker)", placeholder="Acme Corp (ACME)")
            with col2:
                file = st.file_uploader("Upload Excel (.xlsx)", type=["xlsx"], accept_multiple_files=False)

            if st.button("Ingest into DB", type="primary", disabled=not (comp_input and file)):
                try:
                    company, ticker = parse_company_and_ticker(comp_input)
                    bytes_data = file.getvalue()

                    # Extract Revenue (annual + TTM)
                    annual_rev = extract_annual_revenue_series(bytes_data)
                    as_of_rev, ttm_rev = extract_latest_ttm_revenue(bytes_data)

                    # Merge current-year TTM into the annual revenue series
                    try:
                        ttm_year = int(str(as_of_rev)[:4])
                        annual_rev[ttm_year] = float(ttm_rev)
                    except Exception:
                        pass

                    # Extract Cost of Revenue / COGS (annual + TTM)
                    annual_cogs = extract_annual_cost_of_revenue_series(bytes_data)
                    as_of_cogs, ttm_cogs = extract_latest_ttm_cost_of_revenue(bytes_data)

                    # Merge current-year TTM into the annual COGS series
                    try:
                        ttm_cogs_year = int(str(as_of_cogs)[:4])
                        annual_cogs[ttm_cogs_year] = float(ttm_cogs)
                    except Exception:
                        pass

                    # Extract SG&A (annual + TTM)
                    annual_sga = extract_annual_sga_series(bytes_data)
                    as_of_sga, ttm_sga = extract_latest_ttm_sga(bytes_data)

                    # Merge current-year TTM into the annual SG&A series
                    try:
                        ttm_sga_year = int(str(as_of_sga)[:4])
                        annual_sga[ttm_sga_year] = float(ttm_sga)
                    except Exception:
                        pass

                    # Extract Operating Margin (annual + TTM)
                    annual_om = extract_annual_operating_margin_series(bytes_data)
                    as_of_om, ttm_om = extract_latest_ttm_operating_margin(bytes_data)

                    # Merge current-year TTM into the annual operating margin series
                    try:
                        ttm_om_year = int(str(as_of_om)[:4])
                        annual_om[ttm_om_year] = float(ttm_om)
                    except Exception:
                        pass

                    # Extract Pretax Income (annual + TTM)
                    annual_pt = extract_annual_pretax_income_series(bytes_data)
                    as_of_pt, ttm_pt = extract_latest_ttm_pretax_income(bytes_data)

                    # Merge current-year TTM into the annual pretax income series
                    try:
                        ttm_pt_year = int(str(as_of_pt)[:4])
                        annual_pt[ttm_pt_year] = float(ttm_pt)
                    except Exception:
                        pass

                    # Extract Net Income (annual + TTM)
                    annual_ni = extract_annual_net_income_series(bytes_data)
                    as_of_ni, ttm_ni = extract_latest_ttm_net_income(bytes_data)

                    # Merge current-year TTM into the annual net income series
                    try:
                        ttm_ni_year = int(str(as_of_ni)[:4])
                        annual_ni[ttm_ni_year] = float(ttm_ni)
                    except Exception:
                        pass

                    # Extract Effective Tax Rate (annual + TTM)
                    annual_tax = extract_annual_effective_tax_rate_series(bytes_data)
                    as_of_tax, ttm_tax = extract_latest_ttm_effective_tax_rate(bytes_data)

                    # Merge current-year TTM into the annual effective tax rate series
                    try:
                        ttm_tax_year = int(str(as_of_tax)[:4])
                        annual_tax[ttm_tax_year] = float(ttm_tax)
                    except Exception:
                        pass

                    # Extract EBIT (annual + TTM)
                    annual_ebit = extract_annual_ebit_series(bytes_data)
                    as_of_ebit, ttm_ebit = extract_latest_ttm_ebit(bytes_data)

                    # Merge current-year TTM into the annual EBIT series
                    try:
                        ttm_ebit_year = int(str(as_of_ebit)[:4])
                        annual_ebit[ttm_ebit_year] = float(ttm_ebit)
                    except Exception:
                        pass

                    # Extract EBITDA (annual + TTM)
                    annual_ebitda = extract_annual_ebitda_series(bytes_data)
                    as_of_ebitda, ttm_ebitda = extract_latest_ttm_ebitda(bytes_data)

                    # Merge current-year TTM into the annual EBITDA series
                    try:
                        ttm_ebitda_year = int(str(as_of_ebitda)[:4])
                        annual_ebitda[ttm_ebitda_year] = float(ttm_ebitda)
                    except Exception:
                        pass


            
                    # Extract Interest Expense (annual + TTM)
                    annual_interest_expense = extract_annual_interest_expense_series(bytes_data)
                    as_of_interest_expense, ttm_interest_expense = extract_latest_ttm_interest_expense(bytes_data)

                    # Merge current-year TTM into the annual Interest Expense series
                    try:
                        ttm_ie_year = int(str(as_of_interest_expense)[:4])
                        annual_interest_expense[ttm_ie_year] = float(ttm_interest_expense)
                    except Exception:
                        pass

                    # Extract Operating Income (annual + TTM)
                    annual_operating_income = extract_annual_operating_income_series(bytes_data)
                    as_of_operating_income, ttm_operating_income = extract_latest_ttm_operating_income(bytes_data)

                    # Merge current-year TTM into the annual Operating Income series
                    try:
                        ttm_oi_year = int(str(as_of_operating_income)[:4])
                        annual_operating_income[ttm_oi_year] = float(ttm_operating_income)
                    except Exception:
                        pass


                    

                    # Extract Research & Development Expense (annual + TTM) from Income
                    annual_rd = extract_annual_research_and_development_expense_series(bytes_data)
                    as_of_rd, ttm_rd = extract_latest_ttm_research_and_development_expense(bytes_data)

                    # Merge current-year TTM into the annual R&D expense series
                    try:
                        ttm_rd_year = int(str(as_of_rd)[:4])
                        annual_rd[ttm_rd_year] = float(ttm_rd)
                    except Exception:
                        pass

                    # Defensive: keep R&D expense positive
                    annual_rd = {y: abs(float(v)) for y, v in annual_rd.items()}

# Extract Capital Expenditures (annual + TTM) from Cash Flow
                    annual_capex = extract_annual_capital_expenditures_series(bytes_data)
                    as_of_capex, ttm_capex = extract_latest_ttm_capital_expenditures(bytes_data)

                    # Merge current-year TTM into the annual CapEx series
                    try:
                        ttm_capex_year = int(str(as_of_capex)[:4])
                        annual_capex[ttm_capex_year] = float(ttm_capex)
                    except Exception:
                        pass

                    # Store CapEx as positive numbers (sheet often shows outflows as negatives)
                    annual_capex = {y: -float(v) for y, v in annual_capex.items()}

                    # Extract Depreciation & Amortization (annual + TTM) from Cash Flow
                    annual_da = extract_annual_depreciation_amortization_series(bytes_data)
                    as_of_da, ttm_da = extract_latest_ttm_depreciation_amortization(bytes_data)

                    # Merge current-year TTM into the annual D&A series
                    try:
                        ttm_da_year = int(str(as_of_da)[:4])
                        annual_da[ttm_da_year] = float(ttm_da)
                    except Exception:
                        pass


                    # Extract Net Debt Issued/Paid (annual + TTM) from Cash Flow
                    annual_net_debt_issued_paid = extract_annual_net_debt_issued_paid_series(bytes_data)
                    as_of_net_debt_issued_paid, ttm_net_debt_issued_paid = extract_latest_ttm_net_debt_issued_paid(bytes_data)

                    # Merge current-year TTM into the annual Net Debt Issued/Paid series
                    try:
                        ttm_nd_year = int(str(as_of_net_debt_issued_paid)[:4])
                        annual_net_debt_issued_paid[ttm_nd_year] = float(ttm_net_debt_issued_paid)
                    except Exception:
                        pass

                    # Extract Shareholders Equity (annual + TTM) from Balance Sheet
                    annual_se = extract_annual_shareholders_equity_series(bytes_data)
                    as_of_se, ttm_se = extract_latest_ttm_shareholders_equity(bytes_data)

                    # Merge current-year TTM into the annual Shareholders Equity series
                    try:
                        ttm_se_year = int(str(as_of_se)[:4])
                        annual_se[ttm_se_year] = float(ttm_se)
                    except Exception:
                        pass

                    # Extract Short-Term Investments (annual + TTM) from Balance Sheet
                    annual_short_term_investments = extract_annual_short_term_investments_series(bytes_data)
                    as_of_short_term_investments, ttm_short_term_investments = extract_latest_ttm_short_term_investments(bytes_data)

                    # Merge current-year TTM into the annual Short-Term Investments series
                    try:
                        ttm_sti_year = int(str(as_of_short_term_investments)[:4])
                        annual_short_term_investments[ttm_sti_year] = float(ttm_short_term_investments)
                    except Exception:
                        pass

                    # Extract Accounts Receivable (annual + TTM) from Balance Sheet
                    annual_accounts_receivable = extract_annual_accounts_receivable_series(bytes_data)
                    as_of_accounts_receivable, ttm_accounts_receivable = extract_latest_ttm_accounts_receivable(bytes_data)

                    # Merge current-year TTM into the annual Accounts Receivable series
                    try:
                        ttm_ar_year = int(str(as_of_accounts_receivable)[:4])
                        annual_accounts_receivable[ttm_ar_year] = float(ttm_accounts_receivable)
                    except Exception:
                        pass

                    # Extract Retained Earnings (annual + TTM) from Balance Sheet
                    annual_re = extract_annual_retained_earnings_series(bytes_data)
                    as_of_re, ttm_re = extract_latest_ttm_retained_earnings(bytes_data)

                    # Merge current-year TTM into the annual Retained Earnings series
                    try:
                        ttm_re_year = int(str(as_of_re)[:4])
                        annual_re[ttm_re_year] = float(ttm_re)
                    except Exception:
                        pass

                    # Extract Comprehensive Income (annual + TTM) from Balance Sheet
                    annual_ci = extract_annual_comprehensive_income_series(bytes_data)
                    as_of_ci, ttm_ci = extract_latest_ttm_comprehensive_income(bytes_data)

                    # Merge current-year TTM into the annual Comprehensive Income series
                    try:
                        ttm_ci_year = int(str(as_of_ci)[:4])
                        annual_ci[ttm_ci_year] = float(ttm_ci)
                    except Exception:
                        pass



                    # Extract Total Assets (annual + TTM) from Balance Sheet
                    annual_total_assets = extract_annual_total_assets_series(bytes_data)
                    as_of_total_assets, ttm_total_assets = extract_latest_ttm_total_assets(bytes_data)
                    try:
                        ttm_ta_year = int(str(as_of_total_assets)[:4])
                        annual_total_assets[ttm_ta_year] = float(ttm_total_assets)
                    except Exception:
                        pass


                    # Extract Total Current Assets (annual + TTM) from Balance Sheet
                    annual_total_current_assets = extract_annual_total_current_assets_series(bytes_data)
                    as_of_total_current_assets, ttm_total_current_assets = extract_latest_ttm_total_current_assets(bytes_data)
                    try:
                        ttm_tca_year = int(str(as_of_total_current_assets)[:4])
                        annual_total_current_assets[ttm_tca_year] = float(ttm_total_current_assets)
                    except Exception:
                        pass

                    # Extract Total Current Liabilities (annual + TTM) from Balance Sheet
                    annual_total_current_liabilities = extract_annual_total_current_liabilities_series(bytes_data)
                    as_of_total_current_liabilities, ttm_total_current_liabilities = extract_latest_ttm_total_current_liabilities(bytes_data)
                    try:
                        ttm_tcl_year = int(str(as_of_total_current_liabilities)[:4])
                        annual_total_current_liabilities[ttm_tcl_year] = float(ttm_total_current_liabilities)
                    except Exception:
                        pass

                    # Extract Total Long-Term Liabilities (annual + TTM) from Balance Sheet
                    annual_total_long_term_liabilities = extract_annual_total_long_term_liabilities_series(bytes_data)
                    as_of_total_long_term_liabilities, ttm_total_long_term_liabilities = extract_latest_ttm_total_long_term_liabilities(bytes_data)
                    try:
                        ttm_tltl_year = int(str(as_of_total_long_term_liabilities)[:4])
                        annual_total_long_term_liabilities[ttm_tltl_year] = float(ttm_total_long_term_liabilities)
                    except Exception:
                        pass

                    # Extract Total Debt (annual + TTM) from Balance Sheet
                    annual_total_debt = extract_annual_total_debt_series(bytes_data)
                    as_of_total_debt, ttm_total_debt = extract_latest_ttm_total_debt(bytes_data)
                    try:
                        ttm_td_year = int(str(as_of_total_debt)[:4])
                        annual_total_debt[ttm_td_year] = float(ttm_total_debt)
                    except Exception:
                        pass

                    # Extract Market Capitalization (annual + latest TTM) from Ratios
                    annual_market_capitalization = extract_annual_market_capitalization_series(bytes_data)
                    as_of_market_capitalization, ttm_market_capitalization = extract_latest_ttm_market_capitalization(bytes_data)

                    # Use annual ratios up to the prior fiscal year, and the latest Ratios-TTM value for the current year
                    try:
                        ttm_mc_year = int(str(as_of_market_capitalization)[:4])
                        prev_year = ttm_mc_year - 1
                        # Keep only years up to the prior year from Ratios-Annual
                        annual_market_capitalization = {
                            int(y): float(v)
                            for y, v in annual_market_capitalization.items()
                            if int(y) <= int(prev_year)
                        }
                        annual_market_capitalization[int(ttm_mc_year)] = float(ttm_market_capitalization)
                    except Exception:
                        pass

                    # Extract Return on Invested Capital (ROIC)% (annual + latest TTM) from Ratios
                    annual_roic_direct_upload = extract_annual_roic_direct_upload_series(bytes_data)
                    as_of_roic, ttm_roic = extract_latest_ttm_roic_direct_upload(bytes_data)

                    def _to_pct_points(v: float) -> float:
                        # Many ratios come as fractions (0.26 == 26%). For ROIC we store percentage points.
                        fv = float(v)
                        return fv * 100.0 if abs(fv) <= 1.5 else fv

                    # Use annual ratios up to the prior fiscal year, and the latest Ratios-TTM value for the current year
                    try:
                        ttm_roic_year = int(str(as_of_roic)[:4])
                        prev_year = ttm_roic_year - 1
                        annual_roic_direct_upload = {
                            int(y): _to_pct_points(v)
                            for y, v in annual_roic_direct_upload.items()
                            if int(y) <= int(prev_year)
                        }
                        annual_roic_direct_upload[int(ttm_roic_year)] = _to_pct_points(ttm_roic)
                    except Exception:
                        annual_roic_direct_upload = {int(y): _to_pct_points(v) for y, v in annual_roic_direct_upload.items()}

                    # Extract Current Debt (annual + TTM) from Balance Sheet
                    annual_current_debt = extract_annual_current_debt_series(bytes_data)
                    as_of_current_debt, ttm_current_debt = extract_latest_ttm_current_debt(bytes_data)
                    try:
                        ttm_cd_year = int(str(as_of_current_debt)[:4])
                        annual_current_debt[ttm_cd_year] = float(ttm_current_debt)
                    except Exception:
                        pass

                    # Extract Cash & Cash Equivalents (annual + TTM) from Balance Sheet
                    annual_cash_and_cash_equivalents = extract_annual_cash_and_cash_equivalents_series(bytes_data)
                    as_of_cash_and_cash_equivalents, ttm_cash_and_cash_equivalents = extract_latest_ttm_cash_and_cash_equivalents(bytes_data)
                    try:
                        ttm_cce_year = int(str(as_of_cash_and_cash_equivalents)[:4])
                        annual_cash_and_cash_equivalents[ttm_cce_year] = float(ttm_cash_and_cash_equivalents)
                    except Exception:
                        pass

                    # Extract Long-Term Investments (annual + TTM) from Balance Sheet
                    annual_long_term_investments = extract_annual_long_term_investments_series(bytes_data)
                    as_of_long_term_investments, ttm_long_term_investments = extract_latest_ttm_long_term_investments(bytes_data)
                    try:
                        ttm_lti_year = int(str(as_of_long_term_investments)[:4])
                        annual_long_term_investments[ttm_lti_year] = float(ttm_long_term_investments)
                    except Exception:
                        pass

                    # Compute NOPAT (Net Operating Profit After Tax) per year
                    annual_nopat: Dict[int, float] = {}
                    for year, ebit_val in annual_ebit.items():
                        tax_rate = annual_tax.get(year)
                        if tax_rate is None:
                            continue
                        try:
                            annual_nopat[year] = float(ebit_val) * (1.0 - float(tax_rate))
                        except Exception:
                            continue
                        pass


            
                    # Compute Interest Coverage Ratio (Operating Income / Interest Expense) per year
                    annual_interest_coverage: Dict[int, float] = {}
                    # Use all years where we have Operating Income; treat blank or zero Interest Expense as 0.01
                    ic_years = set(annual_operating_income.keys())
                    for year in ic_years:
                        try:
                            raw_ie = annual_interest_expense.get(year)
                            if raw_ie is None:
                                ie_val = 0.01
                            else:
                                ie_float = float(raw_ie)
                                if math.isnan(ie_float) or ie_float == 0.0:
                                    ie_val = 0.01
                                else:
                                    ie_val = ie_float
                            oi_val = float(annual_operating_income.get(year, 0.0))
                            annual_interest_coverage[year] = oi_val / ie_val
                        except Exception:
                            continue

                    # Compute Interest Load % per year from Interest Coverage Ratio
                    annual_interest_load_pct: Dict[int, float] = {}
                    for year, cov in annual_interest_coverage.items():
                        try:
                            cov_val = float(cov)
                            if cov_val == 0.0 or math.isnan(cov_val):
                                # Sentinel for zero or undefined coverage
                                annual_interest_load_pct[year] = 0.001
                            else:
                                annual_interest_load_pct[year] = (1.0 / cov_val) * 100.0
                        except Exception:
                            # If any conversion or math error occurs, fall back to a tiny sentinel value
                            annual_interest_load_pct[year] = 0.001

        # Compute Accumulated Profit (Retained Earnings + Comprehensive Income) per year
                    annual_accumulated_profit: Dict[int, float] = {}
                    common_years = set(annual_re.keys()) & set(annual_ci.keys())
                    for year in common_years:
                        try:
                            re_val = float(annual_re[year])
                            ci_val = float(annual_ci[year])
                            annual_accumulated_profit[year] = re_val + ci_val
                        except Exception:
                            continue

                                        # Compute Total Equity per year (Total Equity = Shareholders Equity)
                    annual_total_equity: Dict[int, float] = {}
                    for year, se_raw in annual_se.items():
                        try:
                            annual_total_equity[int(year)] = float(se_raw)
                        except Exception:
                            continue

                    # Compute Average Equity per year:
                    # Average Equity (year Y) = 0.5 × (Shareholders Equity(Y−1) + Shareholders Equity(Y))
                    annual_average_equity: Dict[int, float] = {}
                    for year in sorted(annual_total_equity.keys()):
                        prev_year = year - 1
                        if prev_year in annual_total_equity:
                            try:
                                avg_eq = 0.5 * (float(annual_total_equity[prev_year]) + float(annual_total_equity[year]))
                                annual_average_equity[year] = avg_eq
                            except Exception:
                                continue

# Compute ROE (Return on Equity) per year = Net Income of current year / Average Equity of current year
                    annual_roe: Dict[int, float] = {}
                    for year, avg_eq in annual_average_equity.items():
                        ni_val = annual_ni.get(year)
                        if ni_val is None:
                            continue
                        try:
                            avg_eq_f = float(avg_eq)
                            if avg_eq_f != 0.0:
                                annual_roe[year] = float(ni_val) / avg_eq_f
                        except Exception:
                            continue




                    # Compute Capital Employed per year = Shareholders Equity + Total Long-Term Liabilities
                    annual_capital_employed: Dict[int, float] = {}
                    common_ce_years = set(annual_se.keys()) & set(annual_total_long_term_liabilities.keys())
                    for year in common_ce_years:
                        try:
                            se_val = float(annual_se[year])
                            tlt_liab_val = float(annual_total_long_term_liabilities[year])
                            annual_capital_employed[year] = se_val + tlt_liab_val
                        except Exception:
                            continue

            
                    # Compute ROCE (Return on Capital Employed) per year = EBIT ÷ Average Capital Employed
                    annual_roce: Dict[int, float] = {}
                    ce_years_sorted = sorted(annual_capital_employed.keys())
                    for year in ce_years_sorted:
                        prev_year = year - 1
                        if prev_year not in annual_capital_employed:
                            continue
                        ebit_val = annual_ebit.get(year)
                        if ebit_val is None:
                            continue
                        try:
                            ce_current = float(annual_capital_employed[year])
                            ce_prev = float(annual_capital_employed[prev_year])
                            avg_ce = 0.5 * (ce_current + ce_prev)
                            if avg_ce != 0.0:
                                annual_roce[year] = float(ebit_val) / avg_ce
                        except Exception:
                            continue

        # Compute Invested Capital per year = Shareholders Equity + Total Debt − Cash & Cash Equivalents − Long-Term Investments
                    annual_invested_capital: Dict[int, float] = {}
                    common_ic_years = (
                        set(annual_se.keys())
                        & set(annual_total_debt.keys())
                        & set(annual_cash_and_cash_equivalents.keys())
                        & set(annual_long_term_investments.keys())
                    )
                    for year in common_ic_years:
                        try:
                            se_val = float(annual_se[year])
                            debt_val = float(annual_total_debt[year])
                            cash_val = float(annual_cash_and_cash_equivalents[year])
                            lti_val = float(annual_long_term_investments[year])
                            annual_invested_capital[year] = se_val + debt_val - cash_val - lti_val
                        except Exception:
                            continue


                    # Compute Non-Cash Working Capital per year
                    annual_non_cash_working_capital: Dict[int, float] = {}
                    common_ncwc_years = (
                        set(annual_total_current_assets.keys())
                        & set(annual_cash_and_cash_equivalents.keys())
                        & set(annual_total_current_liabilities.keys())
                        & set(annual_current_debt.keys())
                    )
                    for year in common_ncwc_years:
                        try:
                            tca_val = float(annual_total_current_assets[year])
                            cce_val = float(annual_cash_and_cash_equivalents[year])
                            tcl_val = float(annual_total_current_liabilities[year])
                            cd_val = float(annual_current_debt[year])
                            annual_non_cash_working_capital[year] = (tca_val - cce_val) - (tcl_val - cd_val)
                        except Exception:
                            continue


                    # Compute Revenue Yield of Non-Cash Working Capital % per year
                    # Revenue Yield of Non-Cash Working Capital % = 1 - (Non-Cash Working Capital / Revenue)
                    annual_revenue_yield_non_cash_working_capital: Dict[int, float] = {}
                    common_ry_years = set(annual_non_cash_working_capital.keys()) & set(annual_rev.keys())
                    for year in common_ry_years:
                        try:
                            ncwc_val = float(annual_non_cash_working_capital[year])
                            rev_val = float(annual_rev[year])
                            if rev_val == 0:
                                continue
                            annual_revenue_yield_non_cash_working_capital[year] = 1.0 - (ncwc_val / rev_val)
                        except Exception:
                            continue

                    # Store
                    conn = get_db()
                    cid = upsert_company(conn, company, ticker)
            
                    # Remember last ingested company for default selection in dropdown
                    st.session_state["last_ingested_company_id"] = cid

                    upsert_annual_revenues(conn, cid, annual_rev)
                    upsert_annual_cost_of_revenue(conn, cid, annual_cogs)
                    upsert_ttm_cost_of_revenue(conn, cid, as_of_cogs, ttm_cogs)
                    upsert_annual_sga(conn, cid, annual_sga)
                    upsert_ttm_sga(conn, cid, as_of_sga, ttm_sga)
                    upsert_annual_ebitda(conn, cid, annual_ebitda)
                    upsert_ttm_ebitda(conn, cid, as_of_ebitda, ttm_ebitda)
                    upsert_ttm(conn, cid, as_of_rev, ttm_rev)

                    upsert_annual_op_margin(conn, cid, annual_om)
                    upsert_ttm_op_margin(conn, cid, as_of_om, ttm_om)

                    upsert_annual_pretax_income(conn, cid, annual_pt)
                    upsert_ttm_pretax_income(conn, cid, as_of_pt, ttm_pt)

                    upsert_annual_net_income(conn, cid, annual_ni)
                    upsert_ttm_net_income(conn, cid, as_of_ni, ttm_ni)

                    upsert_annual_eff_tax_rate(conn, cid, annual_tax)
                    upsert_ttm_eff_tax_rate(conn, cid, as_of_tax, ttm_tax)

                    upsert_annual_ebit(conn, cid, annual_ebit)
                    upsert_ttm_ebit(conn, cid, as_of_ebit, ttm_ebit)

                    upsert_annual_interest_expense(conn, cid, annual_interest_expense)
                    upsert_ttm_interest_expense(conn, cid, as_of_interest_expense, ttm_interest_expense)

                    upsert_annual_operating_income(conn, cid, annual_operating_income)
                    upsert_ttm_operating_income(conn, cid, as_of_operating_income, ttm_operating_income)

                    upsert_annual_interest_coverage(conn, cid, annual_interest_coverage)
                    upsert_annual_interest_load(conn, cid, annual_interest_load_pct)

                    # Derived: Default Spread (synthetic) from Interest Coverage Ratio
                    try:
                        compute_and_store_default_spread(conn, cid)
                    except Exception:
                        pass

                    # Derived: Pre-Tax Cost of Debt = US Risk Free Rate + Default Spread
                    try:
                        compute_and_store_pre_tax_cost_of_debt(conn, cid)
                    except Exception:
                        pass

                    upsert_annual_nopat(conn, cid, annual_nopat)

                    upsert_annual_shareholders_equity(conn, cid, annual_se)
                    upsert_ttm_shareholders_equity(conn, cid, as_of_se, ttm_se)
                    upsert_annual_short_term_investments(conn, cid, annual_short_term_investments)
                    upsert_ttm_short_term_investments(conn, cid, as_of_short_term_investments, ttm_short_term_investments)
                    upsert_annual_accounts_receivable(conn, cid, annual_accounts_receivable)
                    upsert_ttm_accounts_receivable(conn, cid, as_of_accounts_receivable, ttm_accounts_receivable)

                    upsert_annual_retained_earnings(conn, cid, annual_re)
                    upsert_ttm_retained_earnings(conn, cid, as_of_re, ttm_re)

                    upsert_annual_comprehensive_income(conn, cid, annual_ci)
                    upsert_ttm_comprehensive_income(conn, cid, as_of_ci, ttm_ci)

                    upsert_annual_accumulated_profit(conn, cid, annual_accumulated_profit)
                    upsert_annual_total_equity(conn, cid, annual_total_equity)
                    upsert_annual_average_equity(conn, cid, annual_average_equity)
                    upsert_annual_roe(conn, cid, annual_roe)


                    upsert_annual_total_assets(conn, cid, annual_total_assets)
                    upsert_ttm_total_assets(conn, cid, as_of_total_assets, ttm_total_assets)

                    upsert_annual_total_current_assets(conn, cid, annual_total_current_assets)
                    upsert_ttm_total_current_assets(conn, cid, as_of_total_current_assets, ttm_total_current_assets)

                    upsert_annual_total_current_liabilities(conn, cid, annual_total_current_liabilities)
                    upsert_ttm_total_current_liabilities(conn, cid, as_of_total_current_liabilities, ttm_total_current_liabilities)

                    upsert_annual_total_long_term_liabilities(conn, cid, annual_total_long_term_liabilities)
                    upsert_ttm_total_long_term_liabilities(conn, cid, as_of_total_long_term_liabilities, ttm_total_long_term_liabilities)

                    upsert_annual_total_debt(conn, cid, annual_total_debt)
                    upsert_ttm_total_debt(conn, cid, as_of_total_debt, ttm_total_debt)

                    upsert_annual_market_capitalization(conn, cid, annual_market_capitalization)


                    if annual_roic_direct_upload:
                        upsert_annual_roic_direct_upload(conn, cid, annual_roic_direct_upload)

                    # Debt/Equity (Derived) = Total Debt / Market Capitalization
                    annual_debt_equity: Dict[int, float] = {}
                    try:
                        common_de_years = sorted(
                            set(annual_total_debt.keys())
                            & set(annual_market_capitalization.keys())
                        )
                        for y in common_de_years:
                            denom = float(annual_market_capitalization[y])
                            if denom == 0.0:
                                continue
                            annual_debt_equity[int(y)] = float(annual_total_debt[y]) / denom
                    except Exception:
                        annual_debt_equity = {}

                    if annual_debt_equity:
                        upsert_annual_debt_equity(conn, cid, annual_debt_equity)


                    # Levered Beta (Derived) = Unlevered Beta(bucket) * (1 + (1 - TaxRateUSA) * Debt/Equity)
                    try:
                        compute_and_store_levered_beta(conn, cid)
                    except Exception:
                        pass

                    # Derived: Cost of Equity = US Risk Free Rate + Levered Beta * US Implied ERP
                    try:
                        compute_and_store_cost_of_equity(conn, cid)
                    except Exception:
                        pass

                    # Derived: WACC = (D/V)*Rd*(1-T) + (E/V)*Re
                    try:
                        compute_and_store_wacc(conn, cid)
                    except Exception:
                        pass

                    # Derived: Spread% = ROIC% - WACC% (both stored as percentage points)
                    try:
                        compute_and_store_roic_wacc_spread(conn, cid)
                    except Exception:
                        pass




                    upsert_annual_current_debt(conn, cid, annual_current_debt)
                    upsert_ttm_current_debt(conn, cid, as_of_current_debt, ttm_current_debt)

                    upsert_annual_cash_and_cash_equivalents(conn, cid, annual_cash_and_cash_equivalents)
                    upsert_ttm_cash_and_cash_equivalents(conn, cid, as_of_cash_and_cash_equivalents, ttm_cash_and_cash_equivalents)

                    upsert_annual_long_term_investments(conn, cid, annual_long_term_investments)
                    upsert_ttm_long_term_investments(conn, cid, as_of_long_term_investments, ttm_long_term_investments)

                    upsert_annual_capital_employed(conn, cid, annual_capital_employed)
                    upsert_annual_roce(conn, cid, annual_roce)
                    upsert_annual_invested_capital(conn, cid, annual_invested_capital)
                    upsert_annual_non_cash_working_capital(conn, cid, annual_non_cash_working_capital)
                    upsert_annual_revenue_yield_non_cash_working_capital(conn, cid, annual_revenue_yield_non_cash_working_capital)

                    upsert_annual_research_and_development_expense(conn, cid, annual_rd)
                    upsert_annual_net_debt_issued_paid(conn, cid, annual_net_debt_issued_paid)
                    upsert_annual_capital_expenditures(conn, cid, annual_capex)
                    upsert_annual_depreciation_amortization(conn, cid, annual_da)
                    st.success(
                        f"Ingested {company} ({ticker}). "
                        f"Annual revenue years (with TTM year merged): {len(annual_rev)}; "
                        f"TTM Rev as of {as_of_rev}: {ttm_rev:,.2f}. "
                        f"Annual OpMargin years: {len(annual_om)}; TTM OpMargin as of {as_of_om}: {ttm_om}. "
                        f"Annual Pretax Income years: {len(annual_pt)}; TTM Pretax as of {as_of_pt}: {ttm_pt:,.2f}. "
                        f"Annual Net Income years: {len(annual_ni)}; TTM Net Income as of {as_of_ni}: {ttm_ni:,.2f}. "
                        f"NOPAT years (derived from EBIT and Effective Tax Rate): {len(annual_nopat)}"
                    )
                except Exception as e:
                    st.error(f"Upload failed: {e}")

        st.markdown("---")



    with tab_bulk:
        with st.expander("Bulk upload from local folder", expanded=True):
            bulk_dir = _bulk_upload_dir()
            st.write(
                "Place all company financials in a single folder and upload them in bulk. "
                "Each spreadsheet must follow the naming convention: <ticker>-financials.xlsx."
            )
            st.write("Bulk upload folder (create this locally if it does not exist):")
            st.code(str(bulk_dir))
            st.write("Manifest file requirements (Excel .xlsx columns):")
            st.code("Ticker\nCompany Name\nIndustry Bucket\nCountry")

            if st.button("Run bulk upload", type="primary"):
                if not bulk_dir.exists():
                    st.error("Bulk upload folder not found. Create the folder and try again.")
                else:
                    try:
                        manifest_path = _find_manifest_file(bulk_dir)
                        manifest_df = _normalize_manifest_df(_load_manifest_df(manifest_path))
                    except Exception as e:
                        st.error(f"Manifest error: {e}")
                    else:
                        manifest_map = {
                            row["ticker"]: {
                                "company_name": row["company_name"],
                                "industry_bucket": row["industry_bucket"],
                                "country": row["country"],
                            }
                            for _, row in manifest_df.iterrows()
                        }

                        financial_files = sorted(
                            [
                                p for p in bulk_dir.iterdir()
                                if p.is_file() and p.name.lower().endswith("-financials.xlsx")
                            ],
                            key=lambda p: p.name.lower(),
                        )
                        if not financial_files:
                            st.error("No financial spreadsheets found in the bulk upload folder.")
                        else:
                            st.markdown("### Upload progress")
                            errors: list[str] = []
                            log_lines: list[str] = []
                            conn = get_db()

                            for file_path in financial_files:
                                ticker_raw = file_path.name[:-len("-financials.xlsx")]
                                ticker = ticker_raw.strip().upper()
                                manifest_row = manifest_map.get(ticker)
                                company_label = manifest_row["company_name"] if manifest_row else ticker

                                status_placeholder = st.empty()
                                progress = st.progress(0)
                                _render_bulk_status(status_placeholder, company_label, "In progress", "0%", "#d4a106")

                                try:
                                    if manifest_row is None:
                                        raise ValueError("Ticker not found in manifest.")
                                    if manifest_row["country"] not in {
                                        "USA",
                                        "INDIA",
                                        "CHINA",
                                        "JAPAN",
                                        "UK",
                                        "UAE",
                                    }:
                                        raise ValueError(
                                            "Country is not supported (allowed: USA, India, China, Japan, UK, UAE)."
                                        )
                                    if not _bucket_has_betas(conn, manifest_row["industry_bucket"]):
                                        raise ValueError("Industry bucket is missing stored betas.")

                                    bytes_data = file_path.read_bytes()
                                    result = ingest_financials_bytes(
                                        bytes_data,
                                        manifest_row["company_name"],
                                        ticker,
                                        conn,
                                        country=manifest_row["country"],
                                    )

                                    bucket_id = get_company_group_id(conn, manifest_row["industry_bucket"], create=True)
                                    if bucket_id is None:
                                        raise ValueError("Failed to create or find industry bucket.")
                                    add_company_group_members(conn, bucket_id, [result["company_id"]])

                                    progress.progress(100)
                                    _render_bulk_status(status_placeholder, company_label, "Success", "100%", "#1f7a1f")
                                    log_lines.append(
                                        f"{ticker}\t{company_label}\tSUCCESS\t100\t{file_path.name}"
                                    )
                                except Exception as e:
                                    progress.empty()
                                    _render_bulk_status(status_placeholder, company_label, "Error", "NA", "#c62828")
                                    errors.append(f"{ticker}: {e}")
                                    log_lines.append(
                                        f"{ticker}\t{company_label}\tERROR\tNA\t{file_path.name}\t{e}"
                                    )

                            if errors:
                                st.error("Some uploads failed:")
                                for msg in errors:
                                    st.write(f"- {msg}")
                            if log_lines:
                                log_dir = bulk_dir / "Upload_logs"
                                log_dir.mkdir(parents=True, exist_ok=True)
                                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                                log_path = log_dir / f"bulk_upload_{timestamp}.log"
                                header = "Ticker\tCompany\tStatus\tPercent\tFile\tMessage"
                                log_path.write_text("\n".join([header, *log_lines]), encoding="utf-8")
                                st.success(f"Saved upload log to: {log_path}")

    with tab_price:
        with st.expander("Price upload", expanded=True):
            st.write(
                "Upload annual price change spreadsheets for a single company. "
                "Use the same format as the 'Export' sheet in the price history file "
                "(Year, Open, High, Low, Close, Adj. Close, Change, Volume). "
                "Provide the company and ticker as 'Company Name (TICKER)' (for example, "
                "'Automatic Data Processing, Inc. (ADP)')."
            )
            col1, col2 = st.columns([2, 3])
            with col1:
                price_comp_input = st.text_input(
                    "Company (Ticker) for price upload",
                    placeholder="Automatic Data Processing, Inc. (ADP)",
                    key="price_company_input",
                )
            with col2:
                price_file = st.file_uploader(
                    "Upload price Excel (.xlsx)",
                    type=["xlsx"],
                    accept_multiple_files=False,
                    key="price_file_uploader",
                )

            ingest_price_disabled = not (price_comp_input and price_file)

            if st.button(
                "Ingest price into DB",
                type="primary",
                disabled=ingest_price_disabled,
                key="btn_ingest_price",
            ):
                try:
                    company_p, ticker_p = parse_company_and_ticker(price_comp_input)
                    bytes_price = price_file.getvalue()

                    # Read annual price changes from the 'Export' sheet
                    df_price = pd.read_excel(io.BytesIO(bytes_price), sheet_name="Export")

                    # Build mapping: fiscal_year -> price_change (from 'Change' column)
                    year_to_change = {}
                    for _, row in df_price.iterrows():
                        year = row.get("Year")
                        change = row.get("Change")
                        if pd.isna(year) or pd.isna(change):
                            continue
                        try:
                            year_int = int(year)
                            change_float = float(change)
                        except Exception:
                            continue
                        year_to_change[year_int] = change_float

                    if not year_to_change:
                        raise ValueError("No valid Year/Change rows found in the 'Export' sheet.")

                    conn = get_db()
                    cid_price = upsert_company(conn, company_p, ticker_p)

                    upsert_annual_price_change(conn, cid_price, year_to_change)

                    st.success(
                        f"Ingested annual price change data for {company_p} ({ticker_p}) "
                        f"with {len(year_to_change)} year(s)."
                    )
                except Exception as e:
                    st.error(f"Price upload failed: {e}")
