import io
import os
import shutil
import threading
import re
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import inspect

import db_models
from db_orm import (
    Companies,
    CompanyGroups,
    CompanyGroupMembers,
    GrowthWeightFactors,
    StddevWeightFactors,
    RiskFreeRates,
    IndexAnnualPriceMovement,
    ImpliedEquityRiskPremiumUsa,
    MarginalCorporateTaxRates,
    IndustryBetas,
)
from db_orm import (
    RevenuesAnnual, RevenuesTtm,
    OpMarginAnnual, OpMarginTtm,
    PretaxIncomeAnnual, PretaxIncomeTtm,
    NetIncomeAnnual, NetIncomeTtm,
    EffTaxRateAnnual, EffTaxRateTtm,
    EbitAnnual, EbitTtm,
    InterestExpenseAnnual, InterestExpenseTtm,
    OperatingIncomeAnnual, OperatingIncomeTtm,
    NopatAnnual,
    PriceChangeAnnual,
    TotalAssetsAnnual, TotalAssetsTtm,
    TotalCurrentLiabilitiesAnnual, TotalCurrentLiabilitiesTtm,
    TotalLongTermLiabilitiesAnnual, TotalLongTermLiabilitiesTtm,
    TotalDebtAnnual, TotalDebtTtm,
    MarketCapitalizationAnnual,
    RoicDirectUploadAnnual,
    DebtEquityAnnual,
    LeveredBetaAnnual,
    TotalCurrentAssetsAnnual, TotalCurrentAssetsTtm,
    CurrentDebtAnnual, CurrentDebtTtm,
    CashAndCashEquivalentsAnnual, CashAndCashEquivalentsTtm,
    LongTermInvestmentsAnnual, LongTermInvestmentsTtm,
    CapitalEmployedAnnual,
    InvestedCapitalAnnual,
    ShareholdersEquityAnnual, ShareholdersEquityTtm,
    RetainedEarningsAnnual, RetainedEarningsTtm,
    ComprehensiveIncomeAnnual, ComprehensiveIncomeTtm,
    AccumulatedProfitAnnual,
    TotalEquityAnnual,
    AverageEquityAnnual,
    RoeAnnual,
    RoceAnnual,
    InterestCoverageAnnual,
    InterestLoadAnnual,
    DefaultSpreadAnnual,
    NonCashWorkingCapitalAnnual,
    RevenueYieldNonCashWorkingCapitalAnnual,
    ResearchAndDevelopmentExpenseAnnual,
    CapitalExpendituresAnnual,
    DepreciationAmortizationAnnual,
    NetDebtIssuedPaidAnnual,
    FcffAnnual,
    FcfeAnnual,
    ReinvestmentRateAnnual,
    RdSpendRateAnnual,
)
from db_config import get_sqlite_path, get_db_url, is_sqlite_url
from db_session import DbCompat, execute, read_sql_df


# ---------------------------
# Database helpers (SQLite + Postgres)
# ---------------------------

def _is_azure_app_service() -> bool:
    """Detect Azure App Service environment."""
    return bool(os.environ.get("WEBSITE_SITE_NAME") or os.environ.get("WEBSITE_INSTANCE_ID"))


def get_db_path() -> str:
    """Return the resolved SQLite DB path used by the app.

    Precedence:
      1) TARASHA_DB_PATH (supports absolute or relative paths)
      2) Azure App Service default: $HOME/app.db (persistent disk)
      3) Local default: <repo_dir>/app.db
    """
    return str(get_sqlite_path())


# Guard seeding in multi-threaded / multi-request environments
_DB_SEED_LOCK = threading.Lock()
_DB_SEED_STATUS: set[str] = set()


def _sqlite_companies_count(db_path: str) -> int | None:
    """Return COUNT(*) from companies table, or None if db is missing/invalid."""
    if not db_path or not os.path.exists(db_path):
        return None
    try:
        # Open read-only when possible (avoid locking the writer connection)
        uri = f"file:{db_path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
        try:
            row = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='companies'"
            ).fetchone()
            if row is None:
                return 0
            n = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
            return int(n or 0)
        finally:
            conn.close()
    except Exception:
        return None


def _seed_azure_db_if_needed(target_db_path: str) -> None:
    """On Azure, seed the persistent DB from the repo DB once (if needed)."""
    if not is_sqlite_url(get_db_url()):
        return
    if not _is_azure_app_service():
        return

    if not target_db_path:
        return

    # Only seed once per process per target path
    if target_db_path in _DB_SEED_STATUS:
        return

    with _DB_SEED_LOCK:
        if target_db_path in _DB_SEED_STATUS:
            return

        seed_db_path = str(get_sqlite_path())

        # Nothing to seed from (repo DB not packaged / not committed)
        if not os.path.exists(seed_db_path):
            _DB_SEED_STATUS.add(target_db_path)
            return

        # If target DB already has data, do not overwrite it.
        target_count = _sqlite_companies_count(target_db_path)
        if target_count is not None and target_count > 0:
            _DB_SEED_STATUS.add(target_db_path)
            return

        seed_count = _sqlite_companies_count(seed_db_path) or 0
        if seed_count <= 0:
            _DB_SEED_STATUS.add(target_db_path)
            return

        # Ensure target directory exists
        target_dir = os.path.dirname(target_db_path) or "."
        os.makedirs(target_dir, exist_ok=True)

        # Copy via temp + atomic replace
        tmp_path = target_db_path + ".tmp"
        try:
            shutil.copy2(seed_db_path, tmp_path)
            os.replace(tmp_path, target_db_path)
        finally:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass

        _DB_SEED_STATUS.add(target_db_path)


def get_conn(db_path: str | None = None) -> sqlite3.Connection:
    """
    Create a SQLite connection with cloud-friendly settings.

    Notes:
    - timeout/busy_timeout helps avoid 'database is locked' when the DB is briefly busy.
    - journal_mode=DELETE is more reliable than WAL on some cloud-mounted filesystems.
    """
    db_path = db_path or get_db_path()

    # On Azure, ensure the persistent DB file is seeded from the repo DB (once) if needed
    _seed_azure_db_if_needed(db_path)

    conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30)
    conn.execute("PRAGMA journal_mode=DELETE;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")  # 30 seconds
    return conn


def _ensure_companies_country_column(conn) -> None:
    """Backwards-compatible schema migration: ensure companies.country exists and is populated."""
    try:
        inspector = inspect(conn.get_bind())
        cols = [c["name"] for c in inspector.get_columns("companies")]
        if "country" not in cols:
            execute(conn, "ALTER TABLE companies ADD COLUMN country TEXT NOT NULL DEFAULT 'USA'")
            conn.commit()

        # Backfill any blanks/nulls
        execute(conn, "UPDATE companies SET country = 'USA' WHERE country IS NULL OR TRIM(country) = ''")
        conn.commit()
    except Exception:
        return


def _read_df(sql: str, conn, params=None) -> pd.DataFrame:
    session = getattr(conn, "session", None)
    if session is not None:
        return read_sql_df(session, sql, params=params)
    return pd.read_sql_query(sql, conn, params=params)


def read_df(sql: str, conn, params=None) -> pd.DataFrame:
    """Public helper for reading dataframes using the active DB backend."""
    return _read_df(sql, conn, params=params)


def get_company_group_id(conn, name: str, create: bool = False) -> Optional[int]:
    """Return company_groups.id for the given name, optionally creating it."""
    bname = (name or "").strip()
    if not bname:
        return None

    session = getattr(conn, "session", None)
    if session is not None:
        row = session.query(CompanyGroups).filter(CompanyGroups.name == bname).first()
        if row is not None:
            return int(row.id)
        if not create:
            return None

        obj = CompanyGroups(name=bname)
        session.add(obj)
        try:
            session.flush()
        except Exception:
            session.rollback()
            row = session.query(CompanyGroups).filter(CompanyGroups.name == bname).first()
            if row is None:
                return None
            return int(row.id)
        session.commit()
        return int(obj.id)

    cur = conn.cursor()
    cur.execute("SELECT id FROM company_groups WHERE name = ?", (bname,))
    row = cur.fetchone()
    if row is not None:
        return int(row[0])
    if not create:
        return None

    cur.execute(
        "INSERT INTO company_groups(name) VALUES(?) ON CONFLICT DO NOTHING",
        (bname,),
    )
    conn.commit()
    cur.execute("SELECT id FROM company_groups WHERE name = ?", (bname,))
    row = cur.fetchone()
    if row is None:
        return None
    return int(row[0])


def add_company_group_members(conn, group_id: int, company_ids: List[int]) -> int:
    """Add company IDs to a group. Returns number of requested inserts."""
    if not company_ids:
        return 0
    ids = sorted({int(x) for x in company_ids})
    if not ids:
        return 0

    session = getattr(conn, "session", None)
    if session is not None:
        existing = {
            int(r[0])
            for r in session.query(CompanyGroupMembers.company_id)
            .filter(CompanyGroupMembers.group_id == group_id)
            .filter(CompanyGroupMembers.company_id.in_(ids))
            .all()
        }
        new_ids = [cid for cid in ids if cid not in existing]
        if new_ids:
            session.add_all(
                [CompanyGroupMembers(group_id=group_id, company_id=cid) for cid in new_ids]
            )
            session.commit()
        return len(ids)

    cur = conn.cursor()
    cur.executemany(
        "INSERT INTO company_group_members(group_id, company_id) VALUES(?, ?) ON CONFLICT DO NOTHING",
        [(group_id, cid) for cid in ids],
    )
    conn.commit()
    return len(ids)


def remove_company_group_members(conn, group_id: int, company_ids: List[int]) -> int:
    """Remove company IDs from a group. Returns count of requested removals."""
    if not company_ids:
        return 0
    ids = sorted({int(x) for x in company_ids})
    if not ids:
        return 0

    session = getattr(conn, "session", None)
    if session is not None:
        session.query(CompanyGroupMembers).filter(
            CompanyGroupMembers.group_id == group_id,
            CompanyGroupMembers.company_id.in_(ids),
        ).delete(synchronize_session=False)
        session.commit()
        return len(ids)

    placeholders = ",".join(["?"] * len(ids))
    cur = conn.cursor()
    cur.execute(
        f"DELETE FROM company_group_members WHERE group_id = ? AND company_id IN ({placeholders})",
        [group_id, *ids],
    )
    conn.commit()
    return len(ids)


def delete_company_group(conn, group_id: int) -> None:
    """Delete a company group by id."""
    session = getattr(conn, "session", None)
    if session is not None:
        session.query(CompanyGroups).filter(CompanyGroups.id == group_id).delete()
        session.commit()
        return

    cur = conn.cursor()
    cur.execute("DELETE FROM company_groups WHERE id = ?", (group_id,))
    conn.commit()


def update_growth_weight_factors(conn, weights: Dict[int, float]) -> None:
    """Update growth_weight_factors weights by id."""
    if not weights:
        return

    session = getattr(conn, "session", None)
    if session is not None:
        for row_id, weight in weights.items():
            obj = session.get(GrowthWeightFactors, int(row_id))
            if obj is not None:
                obj.weight = float(weight)
        session.commit()
        return

    cur = conn.cursor()
    for row_id, weight in weights.items():
        cur.execute(
            "UPDATE growth_weight_factors SET weight = ? WHERE id = ?",
            (float(weight), int(row_id)),
        )
    conn.commit()


def update_stddev_weight_factors(conn, weights: Dict[int, float]) -> None:
    """Update stddev_weight_factors weights by id."""
    if not weights:
        return

    session = getattr(conn, "session", None)
    if session is not None:
        for row_id, weight in weights.items():
            obj = session.get(StddevWeightFactors, int(row_id))
            if obj is not None:
                obj.weight = float(weight)
        session.commit()
        return

    cur = conn.cursor()
    for row_id, weight in weights.items():
        cur.execute(
            "UPDATE stddev_weight_factors SET weight = ? WHERE id = ?",
            (float(weight), int(row_id)),
        )
    conn.commit()


def upsert_risk_free_rate(
    conn,
    year: int,
    usa_rf: float,
    india_rf: float,
    china_rf: float,
    japan_rf: float,
    updated_at: str,
) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        obj = session.get(RiskFreeRates, int(year))
        if obj is None:
            obj = RiskFreeRates(
                year=int(year),
                usa_rf=float(usa_rf),
                india_rf=float(india_rf),
                china_rf=float(china_rf),
                japan_rf=float(japan_rf),
                updated_at=updated_at,
            )
            session.add(obj)
        else:
            obj.usa_rf = float(usa_rf)
            obj.india_rf = float(india_rf)
            obj.china_rf = float(china_rf)
            obj.japan_rf = float(japan_rf)
            obj.updated_at = updated_at
        session.commit()
        return

    cur = conn.cursor()
    cur.execute(
        """
        UPDATE risk_free_rates
        SET usa_rf = ?, india_rf = ?, china_rf = ?, japan_rf = ?, updated_at = ?
        WHERE year = ?
        """,
        (float(usa_rf), float(india_rf), float(china_rf), float(japan_rf), updated_at, int(year)),
    )
    if cur.rowcount == 0:
        cur.execute(
            """
            INSERT INTO risk_free_rates(year, usa_rf, india_rf, china_rf, japan_rf, updated_at)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (int(year), float(usa_rf), float(india_rf), float(china_rf), float(japan_rf), updated_at),
        )
    conn.commit()


def upsert_index_annual_price_movement(
    conn,
    year: int,
    nasdaq_composite: float,
    sp500: float,
    updated_at: str,
) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        obj = session.get(IndexAnnualPriceMovement, int(year))
        if obj is None:
            obj = IndexAnnualPriceMovement(
                year=int(year),
                nasdaq_composite=float(nasdaq_composite),
                sp500=float(sp500),
                updated_at=updated_at,
            )
            session.add(obj)
        else:
            obj.nasdaq_composite = float(nasdaq_composite)
            obj.sp500 = float(sp500)
            obj.updated_at = updated_at
        session.commit()
        return

    cur = conn.cursor()
    cur.execute(
        """
        UPDATE index_annual_price_movement
        SET nasdaq_composite = ?, sp500 = ?, updated_at = ?
        WHERE year = ?
        """,
        (float(nasdaq_composite), float(sp500), updated_at, int(year)),
    )
    if cur.rowcount == 0:
        cur.execute(
            """
            INSERT INTO index_annual_price_movement(year, nasdaq_composite, sp500, updated_at)
            VALUES(?, ?, ?, ?)
            """,
            (int(year), float(nasdaq_composite), float(sp500), updated_at),
        )
    conn.commit()


def update_implied_equity_risk_premium(
    conn,
    year: int,
    implied_erp: float,
    updated_at: str,
) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        obj = session.get(ImpliedEquityRiskPremiumUsa, int(year))
        if obj is None:
            obj = ImpliedEquityRiskPremiumUsa(
                year=int(year),
                implied_erp=float(implied_erp),
                updated_at=updated_at,
            )
            session.add(obj)
        else:
            obj.implied_erp = float(implied_erp)
            obj.updated_at = updated_at
        session.commit()
        return

    cur = conn.cursor()
    cur.execute(
        "UPDATE implied_equity_risk_premium_usa SET implied_erp = ?, updated_at = ? WHERE year = ?",
        (float(implied_erp), updated_at, int(year)),
    )
    if cur.rowcount == 0:
        cur.execute(
            "INSERT INTO implied_equity_risk_premium_usa(year, implied_erp, updated_at) VALUES(?, ?, ?)",
            (int(year), float(implied_erp), updated_at),
        )
    conn.commit()


def replace_marginal_corporate_tax_rates(
    conn,
    rows: List[Tuple[str, float, Optional[str], str]],
) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        session.query(MarginalCorporateTaxRates).delete(synchronize_session=False)
        session.add_all(
            [
                MarginalCorporateTaxRates(
                    country=str(country),
                    effective_rate=float(rate),
                    notes=notes,
                    updated_at=updated_at,
                )
                for country, rate, notes, updated_at in rows
            ]
        )
        session.commit()
        return

    cur = conn.cursor()
    cur.execute("DELETE FROM marginal_corporate_tax_rates")
    cur.executemany(
        "INSERT INTO marginal_corporate_tax_rates(country, effective_rate, notes, updated_at) VALUES(?, ?, ?, ?)",
        rows,
    )
    conn.commit()


def replace_industry_betas(
    conn,
    rows: List[Tuple[str, str, float, float, str]],
) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        session.query(IndustryBetas).delete(synchronize_session=False)
        session.add_all(
            [
                IndustryBetas(
                    user_industry_bucket=str(bucket),
                    mapped_sector=str(sector),
                    unlevered_beta=float(unlevered),
                    cash_adjusted_beta=float(cash_adjusted),
                    updated_at=updated_at,
                )
                for bucket, sector, unlevered, cash_adjusted, updated_at in rows
            ]
        )
        session.commit()
        return

    cur = conn.cursor()
    cur.execute("DELETE FROM industry_betas")
    cur.executemany(
        "INSERT INTO industry_betas(user_industry_bucket, mapped_sector, unlevered_beta, cash_adjusted_beta, updated_at) VALUES(?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()


def _canonicalize_country(country: Optional[str]) -> str:
    """Normalize country values used by the app to the dropdown canonical set."""
    if country is None:
        return "USA"
    c = str(country).strip()
    if not c:
        return "USA"
    cu = c.upper()

    if cu in ("USA", "US", "UNITED STATES", "UNITED STATES OF AMERICA"):
        return "USA"
    if cu in ("INDIA", "IN", "REPUBLIC OF INDIA"):
        return "India"
    if cu in ("CHINA", "CN", "PRC", "PEOPLE'S REPUBLIC OF CHINA", "PEOPLES REPUBLIC OF CHINA"):
        return "China"
    if cu in ("JAPAN", "JP"):
        return "Japan"
    if cu in ("UK", "UNITED KINGDOM", "GREAT BRITAIN", "BRITAIN", "GB"):
        return "UK"
    if cu in ("UAE", "UNITED ARAB EMIRATES", "AE"):
        return "UAE"

    # Unknown country string: keep as-is (trimmed)
    return c


def _get_company_country(conn: sqlite3.Connection, company_id: int) -> str:
    """Return the stored country for the company, falling back to USA."""
    try:
        session = getattr(conn, "session", None)
        if session is not None:
            row = session.query(Companies.country).filter(Companies.id == company_id).first()
        else:
            row = conn.execute("SELECT country FROM companies WHERE id = ?", (company_id,)).fetchone()
        if row is not None and row[0] is not None and str(row[0]).strip():
            return _canonicalize_country(str(row[0]))
    except Exception:
        pass
    return "USA"
def init_db(conn) -> None:
    db_models.metadata.create_all(conn.get_bind())

    # Backwards-compatible schema migration
    _ensure_companies_country_column(conn)
    conn = DbCompat(conn)

    # Seed default rows if these tables are empty
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM growth_weight_factors")
    row = cur.fetchone()
    cnt_growth = int(row[0]) if row and row[0] is not None else 0
    if cnt_growth == 0:
        growth_defaults = [
            ("Accumulated Equity Growth", 12.0),
            ("Pretax Income Growth", 12.0),
            ("ROCE", 15.0),
            ("Net Income Growth", 20.0),
            ("ROE", 20.0),
            ("Revenue Growth", 15.0),
            ("Operating Margin", 20.0),
            ("YoY Operating Margin Growth", 20.0),
            ("NOPAT Growth", 15.0),
            ("FCFE Growth", 15.0),
            ("Earnings Power Change %", 20.0),
            ("Change in EP Delta", 20.0),
            ("Spread", 20.0),
        ]
        cur.executemany(
            "INSERT INTO growth_weight_factors(factor, weight) VALUES(?, ?)",
            growth_defaults,
        )

    cur.execute("SELECT COUNT(*) FROM stddev_weight_factors")
    row = cur.fetchone()
    cnt_stddev = int(row[0]) if row and row[0] is not None else 0
    if cnt_stddev == 0:
        stddev_defaults = [
            ("Revenue Growth", 20.0),
            ("Net Income  Growth", 20.0),
            ("Operating Margin", 20.0),
            ("ROE", 20.0),
            ("ROCE", 20.0),
            ("Pretax Income Growth", 15.0),
            ("Accumulated Equity Growth", 15.0),
            ("NOPAT Growth", 15.0),
            ("YoY Operating Margin Growth", 12.0),
            ("Earnings Power Change %", 10.0),
            ("Change in EP Delta", 10.0),
            ("Spread", 10.0),
            ("FCFE Growth", 10.0),
        ]
        cur.executemany(
            "INSERT INTO stddev_weight_factors(factor, weight) VALUES(?, ?)",
            stddev_defaults,
        )


    
    # Seed default risk-free rates if table is empty
    cur.execute("SELECT COUNT(*) FROM risk_free_rates")
    row = cur.fetchone()
    cnt_rfr = int(row[0]) if row and row[0] is not None else 0
    if cnt_rfr == 0:
        rfr_defaults = [
            (2015, 2.14, 7.70, 3.40, 0.36),
            (2016, 1.84, 6.95, 2.90, -0.06),
            (2017, 2.33, 6.70, 3.60, 0.05),
            (2018, 2.91, 7.70, 3.60, 0.07),
            (2019, 2.14, 6.70, 3.20, -0.10),
            (2020, 0.89, 5.95, 2.90, 0.01),
            (2021, 1.45, 6.20, 2.95, 0.07),
            (2022, 2.95, 7.29, 2.75, 0.23),
            (2023, 3.96, 7.18, 2.70, 0.56),
            (2024, 4.25, 6.95, 2.30, 0.92),
            (2025, 4.00, 6.49, 1.83, 1.80),
        ]
        ts = datetime.utcnow().isoformat()
        cur.executemany(
            "INSERT INTO risk_free_rates(year, usa_rf, india_rf, china_rf, japan_rf, updated_at) VALUES(?, ?, ?, ?, ?, ?)",
            [(y, usa, ind, chn, jpn, ts) for (y, usa, ind, chn, jpn) in rfr_defaults],
        )

        # Seed default index annual price movement if table is empty
    cur.execute("SELECT COUNT(*) FROM index_annual_price_movement")
    row = cur.fetchone()
    cnt_iapm = int(row[0]) if row and row[0] is not None else 0
    if cnt_iapm == 0:
        idx_defaults = [
            (2015, 5.70, -0.70),
            (2016, 7.50, 9.50),
            (2017, 28.20, 19.40),
            (2018, -3.90, -6.20),
            (2019, 35.20, 28.90),
            (2020, 43.60, 16.30),
            (2021, 21.40, 26.90),
            (2022, -33.10, -19.40),
            (2023, 43.40, 24.20),
            (2024, 28.60, 23.30),
            (2025, 19.20, 15.00),
        ]
        ts_idx = datetime.utcnow().isoformat()
        cur.executemany(
            "INSERT INTO index_annual_price_movement(year, nasdaq_composite, sp500, updated_at) VALUES(?, ?, ?, ?)",
            [(y, nas, sp, ts_idx) for (y, nas, sp) in idx_defaults],
        )


# Seed default implied equity risk premium (USA) if table is empty
    cur.execute("SELECT COUNT(*) FROM implied_equity_risk_premium_usa")
    row = cur.fetchone()
    cnt_erp = int(row[0]) if row and row[0] is not None else 0
    if cnt_erp == 0:
        erp_defaults = [
        (2010, 4.36, 'Recovery from 2008 Financial Crisis.'),
        (2011, 5.20, 'Recovery from 2010 flash crash/jitters.'),
        (2012, 6.01, 'Eurozone debt crisis fears.'),
        (2013, 5.78, 'Fiscal cliff concerns early in the year.'),
        (2014, 4.96, 'Low volatility environment.'),
        (2015, 5.78, 'Steady recovery pricing.'),
        (2016, 6.12, 'Concerns over China growth and oil price crash.'),
        (2017, 5.69, 'Post-election uncertainty and growth hopes.'),
        (2018, 5.08, 'Tax cuts enacted; steady growth expectations.'),
        (2019, 5.96, 'Higher risk pricing following late 2018 market drop.'),
        (2020, 5.20, 'Pre-pandemic level (spiked to >6.0% in March 2020).'),
        (2021, 4.72, 'Post-COVID recovery optimism.'),
        (2022, 4.24, 'Low ERP at start of year before inflation/rates spiked.'),
        (2023, 5.94, 'Spike due to high inflation and aggressive Fed hikes.'),
        (2024, 4.60, 'Decreased from 2023 as inflation fears eased.'),
        (2025, 4.33, 'Market priced for "soft landing" despite high rates.'),
    ]
        ts_erp = datetime.utcnow().isoformat()
        cur.executemany(
            "INSERT INTO implied_equity_risk_premium_usa(year, implied_erp, notes, updated_at) VALUES(?, ?, ?, ?)",
            [(y, erp, note, ts_erp) for (y, erp, note) in erp_defaults],
        )




# Seed default country risk premium if table is empty
    cur.execute("SELECT COUNT(*) FROM Country_Risk_Premium")
    row = cur.fetchone()
    cnt_crp = int(row[0]) if row and row[0] is not None else 0
    if cnt_crp == 0:
        crp_defaults = [
            (2015, 3.46, 0.95, 1.11, 0.00, 0.63, 0.78),
            (2016, 3.13, 0.86, 1.00, 0.00, 0.56, 0.71),
            (2017, 2.19, 0.81, 0.81, 0.00, 0.57, 0.57),
            (2018, 2.64, 0.98, 0.98, 0.00, 0.69, 0.69),
            (2019, 1.88, 0.69, 0.69, 0.00, 0.49, 0.49),
            (2020, 2.13, 0.68, 0.68, 0.00, 0.59, 0.48),
            (2021, 2.18, 0.70, 0.70, 0.00, 0.60, 0.49),
            (2022, 3.79, 1.22, 1.22, 0.00, 1.03, 0.85),
            (2023, 3.21, 1.03, 1.03, 0.00, 0.88, 0.72),
            (2024, 2.93, 0.94, 0.94, 0.00, 0.80, 0.66),
            (2025, 2.85, 0.91, 0.91, 0.23, 0.78, 0.64),
        ]
        ts_crp = datetime.utcnow().isoformat()
        cur.executemany(
            "INSERT INTO Country_Risk_Premium(year, india, china, japan, us, uk, uae, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
            [(y, ind, chn, jpn, us, uk, uae, ts_crp) for (y, ind, chn, jpn, us, uk, uae) in crp_defaults],
        )

# Seed default marginal corporate tax rates if table is empty
    cur.execute("SELECT COUNT(*) FROM marginal_corporate_tax_rates")
    row = cur.fetchone()
    cnt_mctr = int(row[0]) if row and row[0] is not None else 0
    if cnt_mctr == 0:
        mctr_defaults = [
            (
                "USA",
                25.70,
                "Federal (21%) + State Tax (~4–5%). While the Federal rate is a flat 21%, state taxes vary by state; Texas 0%, California 8.84%. For a diversified US company, 25.7% is the standard OECD composite rate used by analysts.",
            ),
            (
                "India",
                25.17,
                "Base (22%) + Surcharge (10%) + Cess (4%). Most major domestic companies have opted for the Section 115BAA regime. If the company is old and has not opted for this (rare for large caps now), use 34.94%.",
            ),
            (
                "China",
                25.00,
                "Standard national rate. China has a 25% standard rate. Use 15% only if the specific company is a qualified “High-Tech Enterprise”.",
            ),
            (
                "Japan",
                30.62,
                "National + Local Inhabitant + Enterprise Tax. Japan's headline rate is 23.2%, but local taxes are significant. The effective statutory tax rate for valuation is widely cited at 30.62%.",
            ),
        ]
        ts2 = datetime.utcnow().isoformat()
        cur.executemany(
            "INSERT INTO marginal_corporate_tax_rates(country, effective_rate, notes, updated_at) VALUES(?, ?, ?, ?)",
            [(c, r, n, ts2) for (c, r, n) in mctr_defaults],
        )


    # Seed default industry betas if table is empty
    cur.execute("SELECT COUNT(*) FROM industry_betas")
    row = cur.fetchone()
    cnt_ib = int(row[0]) if row and row[0] is not None else 0
    if cnt_ib == 0:
        industry_beta_defaults = [
            ("Technology : Internet Content & Info", "Software (Internet)", 1.63, 1.69),
            ("Technology : Semiconductors", "Semiconductor", 1.36, 1.45),
            ("Technology : Semi. Equip & Materials", "Semiconductor Equip", 1.35, 1.44),
            ("Technology : Software - Infrastructure", "Software (System & Application)", 1.20, 1.22),
            ("Technology : Software - Application", "Software (System & Application)", 1.20, 1.22),
            ("Technology : Scientific Instruments", "Electrical Equipment", 1.20, 1.23),
            ("Technology : Computer Hardware", "Computers/Peripherals", 1.10, 1.12),
            ("Technology : IT Services", "Computer Services", 1.03, 1.09),
            ("Technology : Electronic Components", "Electronics (General)", 1.01, 1.03),
            ("Technology : Comm. Equipment", "Telecom. Equipment", 0.91, 0.95),
            ("Technology : Electronics & Distribution", "Retail (Distributors)", 0.91, 0.93),
            ("Technology : Consumer Electronics", "Electronics (Consumer & Office)", 0.84, 0.95),
            ("Technology : Solar", "Green & Renewable Energy", 0.49, 0.50),
            ("Technology : Telecom (Wireless)", "Telecom (Wireless)", 0.57, 0.59),
        ]
        ts_ib = datetime.utcnow().isoformat()
        cur.executemany(
            "INSERT INTO industry_betas(user_industry_bucket, mapped_sector, unlevered_beta, cash_adjusted_beta, updated_at) VALUES(?, ?, ?, ?, ?)",
            [(b, s, ub, cb, ts_ib) for (b, s, ub, cb) in industry_beta_defaults],
        )

    # Refresh/backfill Cost of Equity (derived) using current Levered Beta + US RFR + US ERP
    refresh_cost_of_equity_all_companies(conn)

    # Refresh/backfill Default Spread (derived) using Interest Coverage Ratio
    refresh_default_spread_all_companies(conn)

    # Refresh/backfill Pre-Tax Cost of Debt (derived) using US RFR + Default Spread
    refresh_pre_tax_cost_of_debt_all_companies(conn)

    # Refresh/backfill WACC (derived) using CoE + Pre-Tax CoD + Market Cap + Total Debt + USA tax rate
    refresh_wacc_all_companies(conn)

    # Refresh/backfill ROIC - WACC Spread (derived) using uploaded ROIC and computed WACC
    refresh_roic_wacc_spread_all_companies(conn)

    conn.commit()





def compute_and_store_wacc(conn: sqlite3.Connection, company_id: int) -> None:
    """Compute and store annual Weighted Average Cost of Capital (WACC) for a company.

    Standard form (percentage points):
        WACC(y) = (D/V) * Rd(y) * (1 - T) + (E/V) * Re(y)

    Where:
      - D = Total Debt (total_debt_annual)
      - E = Market Capitalization (market_capitalization_annual)
      - V = D + E
      - Rd = Pre-Tax Cost of Debt (pre_tax_cost_of_debt_annual)
      - Re = Cost of Equity (cost_of_equity_annual)
      - T = country-specific effective marginal corporate tax rate (marginal_corporate_tax_rates)

    Notes:
      - Values are stored as percentage points (e.g., 6.62 means 6.62%).
      - Uses the country-specific tax-rate value in the DB (not year-specific).
    """
    try:
        country = _get_company_country(conn, company_id)
        tax_rate = _tax_rate_to_decimal(_get_effective_marginal_tax_rate_for_country(conn, country))
        if tax_rate is None:
            return

        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO wacc_annual(company_id, fiscal_year, wacc)
            SELECT
                td.company_id,
                td.fiscal_year,
                (pcd.pre_tax_cost_of_debt * (td.total_debt * (1.0 - ?) / (td.total_debt + mc.market_capitalization)))
                + (coe.cost_of_equity * (mc.market_capitalization / (td.total_debt + mc.market_capitalization)))
            FROM total_debt_annual td
            JOIN market_capitalization_annual mc
                ON mc.company_id = td.company_id AND mc.fiscal_year = td.fiscal_year
            JOIN pre_tax_cost_of_debt_annual pcd
                ON pcd.company_id = td.company_id AND pcd.fiscal_year = td.fiscal_year
            JOIN cost_of_equity_annual coe
                ON coe.company_id = td.company_id AND coe.fiscal_year = td.fiscal_year
            WHERE td.company_id = ?
              AND (td.total_debt + mc.market_capitalization) != 0.0
            ON CONFLICT (company_id, fiscal_year) DO UPDATE SET wacc=excluded.wacc
            """,
            (tax_rate, company_id),
        )
        conn.commit()
    except Exception:
        # Derived metric; do not break the app if inputs are missing
        return


def refresh_wacc_all_companies(conn: sqlite3.Connection) -> None:
    """Refresh/backfill WACC for all companies (safe to call repeatedly)."""
    try:
        cur = conn.cursor()
        rows = cur.execute("SELECT id FROM companies").fetchall()
        for r in rows:
            try:
                compute_and_store_wacc(conn, int(r[0]))
            except Exception:
                continue
    except Exception:
        return


def get_annual_wacc_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
    """Retrieve the stored WACC series for a given company."""
    return _read_df(
        """
        SELECT fiscal_year AS year, wacc
        FROM wacc_annual
        WHERE company_id = ?
        ORDER BY fiscal_year
        """,
        conn,
        params=(company_id,),
    )





def compute_and_store_roic_wacc_spread(conn: sqlite3.Connection, company_id: int) -> None:
    """Compute and store annual Spread% = ROIC% - WACC% for a given company (safe to call repeatedly)."""
    try:
        conn.execute(
            """
            INSERT INTO roic_wacc_spread_annual (company_id, fiscal_year, spread_pct)
            SELECT r.company_id, r.fiscal_year, (r.roic_pct - w.wacc) AS spread_pct
            FROM roic_direct_upload_annual r
            JOIN wacc_annual w
                ON w.company_id = r.company_id AND w.fiscal_year = r.fiscal_year
            WHERE r.company_id = ?
            ON CONFLICT (company_id, fiscal_year) DO UPDATE SET spread_pct=excluded.spread_pct
            """,
            (company_id,),
        )
        conn.commit()
    except Exception:
        # Derived metric; do not break the app if inputs are missing
        return


def refresh_roic_wacc_spread_all_companies(conn: sqlite3.Connection) -> None:
    """Refresh/backfill Spread% = ROIC% - WACC% for all companies (safe to call repeatedly)."""
    try:
        conn.execute(
            """
            INSERT INTO roic_wacc_spread_annual (company_id, fiscal_year, spread_pct)
            SELECT r.company_id, r.fiscal_year, (r.roic_pct - w.wacc) AS spread_pct
            FROM roic_direct_upload_annual r
            JOIN wacc_annual w
                ON w.company_id = r.company_id AND w.fiscal_year = r.fiscal_year
            ON CONFLICT (company_id, fiscal_year) DO UPDATE SET spread_pct=excluded.spread_pct
            """
        )
        conn.commit()
    except Exception:
        return


def get_annual_roic_wacc_spread_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
    """Retrieve the stored Spread series for a given company."""
    return _read_df(
        """
        SELECT fiscal_year AS year, spread_pct
        FROM roic_wacc_spread_annual
        WHERE company_id = ?
        ORDER BY fiscal_year
        """,
        conn,
        params=(company_id,),
    )



# ---------------------------
# Spreadsheet parsing helpers
# ---------------------------

def parse_company_and_ticker(s: str) -> Tuple[str, str]:
    m = re.match(r"\s*(.*?)\s*\(([^)]+)\)\s*$", s or "")
    if not m:
        raise ValueError("Please use the format: Company Name (TICKER)")
    company = m.group(1).strip()
    ticker = m.group(2).strip().upper()
    if not company or not ticker:
        raise ValueError("Company or ticker could not be parsed.")
    return company, ticker

def _read_sheet(file_bytes: bytes, sheet_name: str) -> pd.DataFrame:
    return pd.read_excel(io.BytesIO(file_bytes), engine="openpyxl", sheet_name=sheet_name)

def _normalize_first_column_to_date(df: pd.DataFrame) -> pd.DataFrame:
    if 'Date' not in df.columns:
        df = df.copy()
        df.columns = list(df.columns)
        df.rename(columns={df.columns[0]: 'Date'}, inplace=True)
    return df

def _parse_date_label(label: str) -> Optional[pd.Timestamp]:
    try:
        dt = pd.to_datetime(str(label), errors="coerce")
        if pd.isna(dt):
            return None
        return dt
    except Exception:
        return None

def _find_row(df: pd.DataFrame, targets: List[str]) -> pd.Series:
    df = _normalize_first_column_to_date(df)
    date_col = df['Date'].astype(str).str.strip().str.lower()
    targets_lc = [t.lower() for t in targets]
    idx = date_col[date_col.isin(targets_lc)].index
    if len(idx) == 0:
        raise ValueError(f"Couldn't find any of {targets} in the sheet.")
    return df.loc[idx[0]]

def extract_annual_series_by_rowlabel(file_bytes: bytes, rowlabel: str, fallbacks: Optional[List[str]] = None) -> Dict[int, float]:
    df = _read_sheet(file_bytes, "Income-Annual")
    targets = [rowlabel] + (fallbacks or [])
    row = _find_row(df, targets)
    series = {}
    for col in df.columns:
        if col == 'Date':
            continue
        try:
            year = int(str(col)[:4])
            val = row[col]
            if pd.notna(val):
                series[year] = float(val)
        except Exception:
            continue
    return dict(sorted(series.items(), key=lambda kv: kv[0]))

def extract_latest_ttm_by_rowlabel(file_bytes: bytes, rowlabel: str, fallbacks: Optional[List[str]] = None) -> Tuple[str, float]:
    df = _read_sheet(file_bytes, "Income-TTM")
    targets = [rowlabel] + (fallbacks or [])
    row = _find_row(df, targets)

    non_empty = []
    for col in df.columns:
        if col == 'Date':
            continue
        val = row[col]
        if pd.notna(val):
            non_empty.append((str(col), float(val), _parse_date_label(col)))

    if not non_empty:
        raise ValueError(f"Could not find a non-empty TTM cell for {rowlabel}.")

    dated = [x for x in non_empty if x[2] is not None]
    if dated:
        latest = max(dated, key=lambda t: t[2])
        return latest[0], latest[1]

    return non_empty[-1][0], non_empty[-1][1]



def extract_annual_market_capitalization_series(file_bytes: bytes) -> Dict[int, float]:
    """Extract Market Capitalization by fiscal year from the 'Ratios-Annual' sheet."""
    fallbacks = [
        "Market Cap",
        "Market cap",
        "Market capitalization",
        "Market Cap (in millions)",
        "Market Capitalization (in millions)",
    ]
    return extract_annual_ratios_series_by_rowlabel(file_bytes, "Market Capitalization", fallbacks=fallbacks)


def extract_latest_ttm_market_capitalization(file_bytes: bytes) -> Tuple[str, float]:
    """Extract latest Market Capitalization value from the 'Ratios-TTM' sheet."""
    fallbacks = [
        "Market Cap",
        "Market cap",
        "Market capitalization",
        "Market Cap (in millions)",
        "Market Capitalization (in millions)",
    ]
    return extract_latest_ttm_ratios_by_rowlabel(file_bytes, "Market Capitalization", fallbacks=fallbacks)


def extract_annual_roic_direct_upload_series(file_bytes: bytes) -> Dict[int, float]:
    """Extract Return on Invested Capital (ROIC) from the 'Ratios-Annual' sheet.

    Note: Many providers store ROIC as a fraction (e.g., 0.26 for 26%). This function
    DOES NOT normalize—normalization is done at ingestion so you can control storage.
    """
    fallbacks = [
        "Return on Invested Capital (ROIC)%",
        "Return on Invested Capital (ROIC) %",
        "Return on Invested Capital %",
        "Return on Invested Capital",
        "ROIC",
    ]
    return extract_annual_ratios_series_by_rowlabel(file_bytes, "Return on Invested Capital (ROIC)", fallbacks=fallbacks)


def extract_latest_ttm_roic_direct_upload(file_bytes: bytes) -> Tuple[str, float]:
    """Extract latest Return on Invested Capital (ROIC) value from the 'Ratios-TTM' sheet."""
    fallbacks = [
        "Return on Invested Capital (ROIC)%",
        "Return on Invested Capital (ROIC) %",
        "Return on Invested Capital %",
        "Return on Invested Capital",
        "ROIC",
    ]
    return extract_latest_ttm_ratios_by_rowlabel(file_bytes, "Return on Invested Capital (ROIC)", fallbacks=fallbacks)



def extract_annual_ratios_series_by_rowlabel(
    file_bytes: bytes, rowlabel: str, fallbacks: Optional[List[str]] = None
) -> Dict[int, float]:
    df = _read_sheet(file_bytes, "Ratios-Annual")
    targets = [rowlabel] + (fallbacks or [])
    row = _find_row(df, targets)
    series: Dict[int, float] = {}
    for col in df.columns:
        if col == "Date":
            continue
        try:
            year = int(str(col)[:4])
            val = row[col]
            if pd.notna(val):
                series[year] = float(val)
        except Exception:
            continue
    return dict(sorted(series.items(), key=lambda kv: kv[0]))


def extract_latest_ttm_ratios_by_rowlabel(
    file_bytes: bytes, rowlabel: str, fallbacks: Optional[List[str]] = None
) -> Tuple[str, float]:
    df = _read_sheet(file_bytes, "Ratios-TTM")
    targets = [rowlabel] + (fallbacks or [])
    row = _find_row(df, targets)

    non_empty: List[Tuple[str, float, Optional[pd.Timestamp]]] = []
    for col in df.columns:
        if col == "Date":
            continue
        val = row[col]
        if pd.notna(val):
            non_empty.append((str(col), float(val), _parse_date_label(col)))

    if not non_empty:
        raise ValueError(f"Could not find a non-empty TTM cell for {rowlabel}.")

    dated = [x for x in non_empty if x[2] is not None]
    if dated:
        latest = max(dated, key=lambda t: t[2])
        return latest[0], latest[1]

    return non_empty[-1][0], non_empty[-1][1]
def extract_annual_revenue_series(file_bytes: bytes) -> Dict[int, float]:
    return extract_annual_series_by_rowlabel(file_bytes, "Revenue", fallbacks=["Total Revenue", "Sales"])

def extract_latest_ttm_revenue(file_bytes: bytes) -> Tuple[str, float]:
    return extract_latest_ttm_by_rowlabel(file_bytes, "Revenue", fallbacks=["Total Revenue", "Sales"])

def extract_annual_operating_margin_series(file_bytes: bytes) -> Dict[int, float]:
    fallbacks = ["Operating Margin %", "Operating Income Margin", "Op Margin", "Operating margin"]
    return extract_annual_series_by_rowlabel(file_bytes, "Operating Margin", fallbacks=fallbacks)

def extract_latest_ttm_operating_margin(file_bytes: bytes) -> Tuple[str, float]:
    fallbacks = ["Operating Margin %", "Operating Income Margin", "Op Margin", "Operating margin"]
    return extract_latest_ttm_by_rowlabel(file_bytes, "Operating Margin", fallbacks=fallbacks)


def extract_annual_pretax_income_series(file_bytes: bytes) -> Dict[int, float]:
    fallbacks = ["Pre-tax Income", "Pre Tax Income", "Earnings Before Tax", "Income Before Tax"]
    return extract_annual_series_by_rowlabel(file_bytes, "Pretax Income", fallbacks=fallbacks)

def extract_latest_ttm_pretax_income(file_bytes: bytes) -> Tuple[str, float]:
    fallbacks = ["Pre-tax Income", "Pre Tax Income", "Earnings Before Tax", "Income Before Tax"]
    return extract_latest_ttm_by_rowlabel(file_bytes, "Pretax Income", fallbacks=fallbacks)


def extract_annual_net_income_series(file_bytes: bytes) -> Dict[int, float]:
    fallbacks = ["Net income", "Net Income (Loss)", "Net Earnings", "Net Profit"]
    return extract_annual_series_by_rowlabel(file_bytes, "Net Income", fallbacks=fallbacks)

def extract_latest_ttm_net_income(file_bytes: bytes) -> Tuple[str, float]:
    fallbacks = ["Net income", "Net Income (Loss)", "Net Earnings", "Net Profit"]
    return extract_latest_ttm_by_rowlabel(file_bytes, "Net Income", fallbacks=fallbacks)

def extract_annual_effective_tax_rate_series(file_bytes: bytes) -> Dict[int, float]:
    fallbacks = ["Effective Tax Rate %", "Tax Rate", "Effective tax rate"]
    return extract_annual_series_by_rowlabel(file_bytes, "Effective Tax Rate", fallbacks=fallbacks)

def extract_latest_ttm_effective_tax_rate(file_bytes: bytes) -> Tuple[str, float]:
    fallbacks = ["Effective Tax Rate %", "Tax Rate", "Effective tax rate"]
    return extract_latest_ttm_by_rowlabel(file_bytes, "Effective Tax Rate", fallbacks=fallbacks)

def extract_annual_ebit_series(file_bytes: bytes) -> Dict[int, float]:
    fallbacks = ["EBIT", "Operating Income", "Earnings Before Interest and Taxes"]
    return extract_annual_series_by_rowlabel(file_bytes, "EBIT", fallbacks=fallbacks)

def extract_latest_ttm_ebit(file_bytes: bytes) -> Tuple[str, float]:
    fallbacks = ["EBIT", "Operating Income", "Earnings Before Interest and Taxes"]
    return extract_latest_ttm_by_rowlabel(file_bytes, "EBIT", fallbacks=fallbacks)




def extract_annual_interest_expense_series(file_bytes: bytes) -> Dict[int, float]:
    fallbacks = ["Interest Expense / Income", "Interest Expense", "Interest expense / income"]
    return extract_annual_series_by_rowlabel(file_bytes, "Interest Expense / Income", fallbacks=fallbacks)

def extract_latest_ttm_interest_expense(file_bytes: bytes) -> Tuple[str, float]:
    fallbacks = ["Interest Expense / Income", "Interest Expense", "Interest expense / income"]
    return extract_latest_ttm_by_rowlabel(file_bytes, "Interest Expense / Income", fallbacks=fallbacks)

def extract_annual_operating_income_series(file_bytes: bytes) -> Dict[int, float]:
    fallbacks = ["Operating Income", "Operating income", "EBIT"]
    return extract_annual_series_by_rowlabel(file_bytes, "Operating Income", fallbacks=fallbacks)

def extract_latest_ttm_operating_income(file_bytes: bytes) -> Tuple[str, float]:
    fallbacks = ["Operating Income", "Operating income", "EBIT"]
    return extract_latest_ttm_by_rowlabel(file_bytes, "Operating Income", fallbacks=fallbacks)


def extract_bs_annual_series_by_rowlabel(file_bytes: bytes, rowlabel: str, fallbacks: Optional[List[str]] = None) -> Dict[int, float]:
    """Extract an annual series from the Balance-Sheet-Annual sheet by row label."""
    df = _read_sheet(file_bytes, "Balance-Sheet-Annual")
    targets = [rowlabel] + (fallbacks or [])
    row = _find_row(df, targets)
    series: Dict[int, float] = {}
    for col in df.columns:
        if col == 'Date':
            continue
        try:
            year = int(str(col)[:4])
            val = row[col]
            if pd.notna(val):
                series[year] = float(val)
        except Exception:
            continue
    return dict(sorted(series.items(), key=lambda kv: kv[0]))

def extract_bs_latest_ttm_by_rowlabel(file_bytes: bytes, rowlabel: str, fallbacks: Optional[List[str]] = None) -> Tuple[str, float]:
    """Extract the most recent TTM value from the Balance-Sheet-TTM sheet by row label."""
    df = _read_sheet(file_bytes, "Balance-Sheet-TTM")
    targets = [rowlabel] + (fallbacks or [])
    row = _find_row(df, targets)

    non_empty: List[Tuple[str, float, Optional[pd.Timestamp]]] = []
    for col in df.columns:
        if col == 'Date':
            continue
        val = row[col]
        if pd.notna(val):
            non_empty.append((str(col), float(val), _parse_date_label(col)))

    if not non_empty:
        raise ValueError(f"Could not find a non-empty TTM cell for {rowlabel} in Balance-Sheet-TTM.")

    dated = [x for x in non_empty if x[2] is not None]
    if dated:
        latest = max(dated, key=lambda t: t[2])
        return latest[0], latest[1]

    # Fallback: last non-empty column if date parsing fails
    last = non_empty[-1]
    return last[0], last[1]



def extract_cf_annual_series_by_rowlabel(file_bytes: bytes, rowlabel: str, fallbacks: Optional[List[str]] = None) -> Dict[int, float]:
    """Extract an annual series from the Cash-Flow-Annual sheet by row label."""
    df = _read_sheet(file_bytes, "Cash-Flow-Annual")
    targets = [rowlabel] + (fallbacks or [])
    row = _find_row(df, targets)
    series: Dict[int, float] = {}
    for col in df.columns:
        if col == 'Date':
            continue
        try:
            year = int(str(col)[:4])
            val = row[col]
            if pd.notna(val):
                series[year] = float(val)
        except Exception:
            continue
    return dict(sorted(series.items(), key=lambda kv: kv[0]))

def extract_cf_latest_ttm_by_rowlabel(file_bytes: bytes, rowlabel: str, fallbacks: Optional[List[str]] = None) -> Tuple[str, float]:
    """Extract the most recent TTM value from the Cash-Flow-TTM sheet by row label."""
    df = _read_sheet(file_bytes, "Cash-Flow-TTM")
    targets = [rowlabel] + (fallbacks or [])
    row = _find_row(df, targets)

    non_empty: List[Tuple[str, float, Optional[pd.Timestamp]]] = []
    for col in df.columns:
        if col == 'Date':
            continue
        val = row[col]
        if pd.notna(val):
            non_empty.append((str(col), float(val), _parse_date_label(col)))

    if not non_empty:
        raise ValueError(f"Could not find a non-empty TTM cell for {rowlabel} in Cash-Flow-TTM.")

    dated = [x for x in non_empty if x[2] is not None]
    if dated:
        latest = max(dated, key=lambda t: t[2])
        return latest[0], latest[1]

    # Fallback: last non-empty column if date parsing fails
    last = non_empty[-1]
    return last[0], last[1]


def extract_annual_research_and_development_expense_series(file_bytes: bytes) -> Dict[int, float]:
    fallbacks = [
        "Research & Development",
        "Research and Development",
        "Research & development",
        "Research and development",
        "R&D",
        "R & D",
    ]
    return extract_annual_series_by_rowlabel(file_bytes, "Research & Development", fallbacks=fallbacks)

def extract_latest_ttm_research_and_development_expense(file_bytes: bytes) -> Tuple[str, float]:
    fallbacks = [
        "Research & Development",
        "Research and Development",
        "Research & development",
        "Research and development",
        "R&D",
        "R & D",
    ]
    return extract_latest_ttm_by_rowlabel(file_bytes, "Research & Development", fallbacks=fallbacks)

def extract_annual_capital_expenditures_series(file_bytes: bytes) -> Dict[int, float]:
    fallbacks = [
        "Capital Expenditures",
        "Capital expenditures",
        "Capital Expenditure",
        "CapEx",
        "Purchase of Property, Plant & Equipment",
        "Purchase of property, plant & equipment",
    ]
    return extract_cf_annual_series_by_rowlabel(file_bytes, "Capital Expenditures", fallbacks=fallbacks)

def extract_latest_ttm_capital_expenditures(file_bytes: bytes) -> Tuple[str, float]:
    fallbacks = [
        "Capital Expenditures",
        "Capital expenditures",
        "Capital Expenditure",
        "CapEx",
        "Purchase of Property, Plant & Equipment",
        "Purchase of property, plant & equipment",
    ]
    return extract_cf_latest_ttm_by_rowlabel(file_bytes, "Capital Expenditures", fallbacks=fallbacks)

def extract_annual_depreciation_amortization_series(file_bytes: bytes) -> Dict[int, float]:
    fallbacks = [
        "Depreciation & Amortization",
        "Depreciation and Amortization",
        "Depreciation & amortization",
        "Depreciation",
        "Depreciation/Amortization",
        "D&A",
    ]
    return extract_cf_annual_series_by_rowlabel(file_bytes, "Depreciation & Amortization", fallbacks=fallbacks)

def extract_latest_ttm_depreciation_amortization(file_bytes: bytes) -> Tuple[str, float]:
    fallbacks = [
        "Depreciation & Amortization",
        "Depreciation and Amortization",
        "Depreciation & amortization",
        "Depreciation",
        "Depreciation/Amortization",
        "D&A",
    ]
    return extract_cf_latest_ttm_by_rowlabel(file_bytes, "Depreciation & Amortization", fallbacks=fallbacks)




def extract_annual_net_debt_issued_paid_series(file_bytes: bytes) -> Dict[int, float]:
    fallbacks = [
        "Debt Issued / Paid",
        "Debt issued / paid",
        "Debt Issued/ Paid",
        "Debt Issued/Paid",
    ]
    # Optional field: many non-U.S. templates don't include a dedicated debt-issuance line.
    # If the row is missing, return an empty series (callers may apply a fallback).
    try:
        return extract_cf_annual_series_by_rowlabel(file_bytes, "Debt Issued / Paid", fallbacks=fallbacks)
    except Exception:
        return {}


def extract_latest_ttm_net_debt_issued_paid(file_bytes: bytes) -> Tuple[str, float]:
    fallbacks = [
        "Debt Issued / Paid",
        "Debt issued / paid",
        "Debt Issued/ Paid",
        "Debt Issued/Paid",
    ]
    # Optional field: return a safe default when missing.
    try:
        return extract_cf_latest_ttm_by_rowlabel(file_bytes, "Debt Issued / Paid", fallbacks=fallbacks)
    except Exception:
        return ("", 0.0)


def extract_annual_total_assets_series(file_bytes: bytes) -> Dict[int, float]:
    fallbacks = ["Total Assets", "Total assets"]
    return extract_bs_annual_series_by_rowlabel(file_bytes, "Total Assets", fallbacks=fallbacks)

def extract_latest_ttm_total_assets(file_bytes: bytes) -> Tuple[str, float]:
    fallbacks = ["Total Assets", "Total assets"]
    return extract_bs_latest_ttm_by_rowlabel(file_bytes, "Total Assets", fallbacks=fallbacks)


def extract_annual_total_current_assets_series(file_bytes: bytes) -> Dict[int, float]:
    fallbacks = ["Total Current Assets", "Total current assets"]
    return extract_bs_annual_series_by_rowlabel(file_bytes, "Total Current Assets", fallbacks=fallbacks)

def extract_latest_ttm_total_current_assets(file_bytes: bytes) -> Tuple[str, float]:
    fallbacks = ["Total Current Assets", "Total current assets"]
    return extract_bs_latest_ttm_by_rowlabel(file_bytes, "Total Current Assets", fallbacks=fallbacks)

def extract_annual_total_current_liabilities_series(file_bytes: bytes) -> Dict[int, float]:
    fallbacks = ["Total Current Liabilities", "Total current liabilities"]
    return extract_bs_annual_series_by_rowlabel(file_bytes, "Total Current Liabilities", fallbacks=fallbacks)

def extract_latest_ttm_total_current_liabilities(file_bytes: bytes) -> Tuple[str, float]:
    fallbacks = ["Total Current Liabilities", "Total current liabilities"]
    return extract_bs_latest_ttm_by_rowlabel(file_bytes, "Total Current Liabilities", fallbacks=fallbacks)

def extract_annual_total_long_term_liabilities_series(file_bytes: bytes) -> Dict[int, float]:
    fallbacks = ["Total Long-Term Liabilities", "Total Long Term Liabilities", "Total long-term liabilities"]
    return extract_bs_annual_series_by_rowlabel(file_bytes, "Total Long-Term Liabilities", fallbacks=fallbacks)

def extract_latest_ttm_total_long_term_liabilities(file_bytes: bytes) -> Tuple[str, float]:
    fallbacks = ["Total Long-Term Liabilities", "Total Long Term Liabilities", "Total long-term liabilities"]
    return extract_bs_latest_ttm_by_rowlabel(file_bytes, "Total Long-Term Liabilities", fallbacks=fallbacks)

def extract_annual_total_debt_series(file_bytes: bytes) -> Dict[int, float]:
    fallbacks = ["Total Debt", "Total debt"]
    return extract_bs_annual_series_by_rowlabel(file_bytes, "Total Debt", fallbacks=fallbacks)

def extract_latest_ttm_total_debt(file_bytes: bytes) -> Tuple[str, float]:
    fallbacks = ["Total Debt", "Total debt"]
    return extract_bs_latest_ttm_by_rowlabel(file_bytes, "Total Debt", fallbacks=fallbacks)


def extract_annual_current_debt_series(file_bytes: bytes) -> Dict[int, float]:
    fallbacks = ["Current Debt", "Current debt", "Short-Term Debt", "Short-term debt", "Short Term Debt"]
    return extract_bs_annual_series_by_rowlabel(file_bytes, "Current Debt", fallbacks=fallbacks)

def extract_latest_ttm_current_debt(file_bytes: bytes) -> Tuple[str, float]:
    fallbacks = ["Current Debt", "Current debt", "Short-Term Debt", "Short-term debt", "Short Term Debt"]
    return extract_bs_latest_ttm_by_rowlabel(file_bytes, "Current Debt", fallbacks=fallbacks)

def extract_annual_cash_and_cash_equivalents_series(file_bytes: bytes) -> Dict[int, float]:
    fallbacks = ["Cash & Cash Equivalents", "Cash and Cash Equivalents", "Cash & cash equivalents"]
    return extract_bs_annual_series_by_rowlabel(file_bytes, "Cash & Cash Equivalents", fallbacks=fallbacks)

def extract_latest_ttm_cash_and_cash_equivalents(file_bytes: bytes) -> Tuple[str, float]:
    fallbacks = ["Cash & Cash Equivalents", "Cash and Cash Equivalents", "Cash & cash equivalents"]
    return extract_bs_latest_ttm_by_rowlabel(file_bytes, "Cash & Cash Equivalents", fallbacks=fallbacks)

def extract_annual_long_term_investments_series(file_bytes: bytes) -> Dict[int, float]:
    fallbacks = ["Long-Term Investments", "Long Term Investments", "Long-term investments"]
    return extract_bs_annual_series_by_rowlabel(file_bytes, "Long-Term Investments", fallbacks=fallbacks)

def extract_latest_ttm_long_term_investments(file_bytes: bytes) -> Tuple[str, float]:
    fallbacks = ["Long-Term Investments", "Long Term Investments", "Long-term investments"]
    return extract_bs_latest_ttm_by_rowlabel(file_bytes, "Long-Term Investments", fallbacks=fallbacks)


def extract_annual_shareholders_equity_series(file_bytes: bytes) -> Dict[int, float]:
    fallbacks = ["Total Equity", "Total Shareholders' Equity", "Total Stockholders' Equity"]
    return extract_bs_annual_series_by_rowlabel(file_bytes, "Shareholders Equity", fallbacks=fallbacks)

def extract_latest_ttm_shareholders_equity(file_bytes: bytes) -> Tuple[str, float]:
    fallbacks = ["Total Equity", "Total Shareholders' Equity", "Total Stockholders' Equity"]
    return extract_bs_latest_ttm_by_rowlabel(file_bytes, "Shareholders Equity", fallbacks=fallbacks)

def extract_annual_retained_earnings_series(file_bytes: bytes) -> Dict[int, float]:
    fallbacks: List[str] = []
    return extract_bs_annual_series_by_rowlabel(file_bytes, "Retained Earnings", fallbacks=fallbacks)

def extract_latest_ttm_retained_earnings(file_bytes: bytes) -> Tuple[str, float]:
    fallbacks: List[str] = []
    return extract_bs_latest_ttm_by_rowlabel(file_bytes, "Retained Earnings", fallbacks=fallbacks)

def extract_annual_comprehensive_income_series(file_bytes: bytes) -> Dict[int, float]:
    fallbacks: List[str] = []
    return extract_bs_annual_series_by_rowlabel(file_bytes, "Comprehensive Income", fallbacks=fallbacks)

def extract_latest_ttm_comprehensive_income(file_bytes: bytes) -> Tuple[str, float]:
    fallbacks: List[str] = []
    return extract_bs_latest_ttm_by_rowlabel(file_bytes, "Comprehensive Income", fallbacks=fallbacks)

# ---------------------------
# Ingest & compute
# ---------------------------

def upsert_company(conn: sqlite3.Connection, name: str, ticker: str, country: Optional[str] = None) -> int:
    """Insert company row if missing and optionally persist its country.

    - country is stored in companies.country (canonicalized to the dropdown set).
    - If country is None, we do not overwrite any existing stored country.
    """
    session = getattr(conn, "session", None)
    if session is not None:
        existing = session.query(Companies).filter(
            Companies.name == name, Companies.ticker == ticker
        ).one_or_none()
        if existing is None:
            existing = Companies(name=name, ticker=ticker, country=_canonicalize_country(country) if country else "USA")
            session.add(existing)
            session.flush()
        elif country is not None:
            existing.country = _canonicalize_country(country)
        session.commit()
        return int(existing.id)

    cur = conn.cursor()
    cur.execute("""INSERT INTO companies(name, ticker) VALUES(?, ?) ON CONFLICT DO NOTHING""", (name, ticker))
    conn.commit()
    if country is not None:
        try:
            canonical = _canonicalize_country(country)
            cur.execute(
                """UPDATE companies SET country = ? WHERE name = ? AND ticker = ?""",
                (canonical, name, ticker),
            )
            conn.commit()
        except Exception:
            pass
    cur.execute("SELECT id FROM companies WHERE name = ? AND ticker = ?", (name, ticker))
    row = cur.fetchone()
    assert row, "Company upsert failed."
    return int(row[0])

def upsert_annual_revenues(conn: sqlite3.Connection, company_id: int, year_to_rev: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, rev in year_to_rev.items():
            existing = session.query(RevenuesAnnual).filter(
                RevenuesAnnual.company_id == company_id,
                RevenuesAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(RevenuesAnnual(company_id=company_id, fiscal_year=year, revenue=rev))
            else:
                existing.revenue = rev
        session.commit()
        return

    cur = conn.cursor()
    for year, rev in year_to_rev.items():
        cur.execute(
            """
            INSERT INTO revenues_annual(company_id, fiscal_year, revenue)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year) DO UPDATE SET revenue=excluded.revenue
            """,
            (company_id, year, rev)
        )
    conn.commit()


def upsert_ttm(conn: sqlite3.Connection, company_id: int, as_of: str, revenue: float) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        existing = session.query(RevenuesTtm).filter(RevenuesTtm.company_id == company_id).one_or_none()
        if existing is None:
            session.add(RevenuesTtm(company_id=company_id, as_of=as_of, revenue=revenue))
        else:
            existing.as_of = as_of
            existing.revenue = revenue
        session.commit()
        return

    conn.execute(
        """
        INSERT INTO revenues_ttm(company_id, as_of, revenue)
        VALUES(?, ?, ?)
        ON CONFLICT(company_id) DO UPDATE SET as_of=excluded.as_of, revenue=excluded.revenue
        """, (company_id, as_of, revenue)
    )
    conn.commit()


def upsert_annual_op_margin(conn: sqlite3.Connection, company_id: int, year_to_margin: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, margin in year_to_margin.items():
            existing = session.query(OpMarginAnnual).filter(
                OpMarginAnnual.company_id == company_id,
                OpMarginAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(OpMarginAnnual(company_id=company_id, fiscal_year=year, margin=margin))
            else:
                existing.margin = margin
        session.commit()
        return

    cur = conn.cursor()
    for year, margin in year_to_margin.items():
        cur.execute(
            """
            INSERT INTO op_margin_annual(company_id, fiscal_year, margin)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year) DO UPDATE SET margin=excluded.margin
            """, (company_id, year, margin)
        )
    conn.commit()


def upsert_ttm_op_margin(conn: sqlite3.Connection, company_id: int, as_of: str, margin: float) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        existing = session.query(OpMarginTtm).filter(OpMarginTtm.company_id == company_id).one_or_none()
        if existing is None:
            session.add(OpMarginTtm(company_id=company_id, as_of=as_of, margin=margin))
        else:
            existing.as_of = as_of
            existing.margin = margin
        session.commit()
        return

    conn.execute(
        """
        INSERT INTO op_margin_ttm(company_id, as_of, margin)
        VALUES(?, ?, ?)
        ON CONFLICT(company_id) DO UPDATE SET as_of=excluded.as_of, margin=excluded.margin
        """, (company_id, as_of, margin)
    )
    conn.commit()

def upsert_annual_pretax_income(conn: sqlite3.Connection, company_id: int, year_to_pretax: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_pretax.items():
            existing = session.query(PretaxIncomeAnnual).filter(
                PretaxIncomeAnnual.company_id == company_id,
                PretaxIncomeAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(PretaxIncomeAnnual(company_id=company_id, fiscal_year=year, pretax_income=val))
            else:
                existing.pretax_income = val
        session.commit()
        return

    cur = conn.cursor()
    for year, val in year_to_pretax.items():
        cur.execute(
            """
            INSERT INTO pretax_income_annual(company_id, fiscal_year, pretax_income)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year) DO UPDATE SET pretax_income=excluded.pretax_income
            """, (company_id, year, val)
        )
    conn.commit()


def upsert_ttm_pretax_income(conn: sqlite3.Connection, company_id: int, as_of: str, pretax: float) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        existing = session.query(PretaxIncomeTtm).filter(PretaxIncomeTtm.company_id == company_id).one_or_none()
        if existing is None:
            session.add(PretaxIncomeTtm(company_id=company_id, as_of=as_of, pretax_income=pretax))
        else:
            existing.as_of = as_of
            existing.pretax_income = pretax
        session.commit()
        return

    conn.execute(
        """
        INSERT INTO pretax_income_ttm(company_id, as_of, pretax_income)
        VALUES(?, ?, ?)
        ON CONFLICT(company_id) DO UPDATE SET as_of=excluded.as_of, pretax_income=excluded.pretax_income
        """, (company_id, as_of, pretax)
    )
    conn.commit()

def get_annual_pretax_income_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
    return _read_df(
        """
        SELECT fiscal_year AS year, pretax_income
        FROM pretax_income_annual
        WHERE company_id = ?
        ORDER BY year
        """, conn, params=(company_id,)
    )

def upsert_annual_net_income(conn: sqlite3.Connection, company_id: int, year_to_net: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_net.items():
            existing = session.query(NetIncomeAnnual).filter(
                NetIncomeAnnual.company_id == company_id,
                NetIncomeAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(NetIncomeAnnual(company_id=company_id, fiscal_year=year, net_income=val))
            else:
                existing.net_income = val
        session.commit()
        return

    cur = conn.cursor()
    for year, val in year_to_net.items():
        cur.execute(
            """
            INSERT INTO net_income_annual(company_id, fiscal_year, net_income)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year) DO UPDATE SET net_income=excluded.net_income
            """,
            (company_id, year, val)
        )
    conn.commit()


def upsert_ttm_net_income(conn: sqlite3.Connection, company_id: int, as_of: str, net_income: float) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        existing = session.query(NetIncomeTtm).filter(NetIncomeTtm.company_id == company_id).one_or_none()
        if existing is None:
            session.add(NetIncomeTtm(company_id=company_id, as_of=as_of, net_income=net_income))
        else:
            existing.as_of = as_of
            existing.net_income = net_income
        session.commit()
        return

    conn.execute(
        """
        INSERT INTO net_income_ttm(company_id, as_of, net_income)
        VALUES(?, ?, ?)
        ON CONFLICT(company_id) DO UPDATE SET as_of=excluded.as_of, net_income=excluded.net_income
        """, (company_id, as_of, net_income)
    )
    conn.commit()

def get_annual_net_income_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
    return _read_df(
        """
        SELECT fiscal_year AS year, net_income
        FROM net_income_annual
        WHERE company_id = ?
        ORDER BY year
        """, conn, params=(company_id,)
    )

def upsert_annual_eff_tax_rate(conn: sqlite3.Connection, company_id: int, year_to_rate: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_rate.items():
            existing = session.query(EffTaxRateAnnual).filter(
                EffTaxRateAnnual.company_id == company_id,
                EffTaxRateAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(EffTaxRateAnnual(company_id=company_id, fiscal_year=year, eff_tax_rate=val))
            else:
                existing.eff_tax_rate = val
        session.commit()
        return

    cur = conn.cursor()
    for year, val in year_to_rate.items():
        cur.execute(
            """
            INSERT INTO eff_tax_rate_annual(company_id, fiscal_year, eff_tax_rate)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year) DO UPDATE SET eff_tax_rate=excluded.eff_tax_rate
            """,
            (company_id, year, val)
        )
    conn.commit()


def upsert_ttm_eff_tax_rate(conn: sqlite3.Connection, company_id: int, as_of: str, eff_tax_rate: float) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        existing = session.query(EffTaxRateTtm).filter(EffTaxRateTtm.company_id == company_id).one_or_none()
        if existing is None:
            session.add(EffTaxRateTtm(company_id=company_id, as_of=as_of, eff_tax_rate=eff_tax_rate))
        else:
            existing.as_of = as_of
            existing.eff_tax_rate = eff_tax_rate
        session.commit()
        return

    conn.execute(
        """
        INSERT INTO eff_tax_rate_ttm(company_id, as_of, eff_tax_rate)
        VALUES(?, ?, ?)
        ON CONFLICT(company_id) DO UPDATE SET as_of=excluded.as_of, eff_tax_rate=excluded.eff_tax_rate
        """, (company_id, as_of, eff_tax_rate)
    )
    conn.commit()

def upsert_annual_ebit(conn: sqlite3.Connection, company_id: int, year_to_ebit: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_ebit.items():
            existing = session.query(EbitAnnual).filter(
                EbitAnnual.company_id == company_id,
                EbitAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(EbitAnnual(company_id=company_id, fiscal_year=year, ebit=val))
            else:
                existing.ebit = val
        session.commit()
        return

    cur = conn.cursor()
    for year, val in year_to_ebit.items():
        cur.execute(
            """
            INSERT INTO ebit_annual(company_id, fiscal_year, ebit)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year) DO UPDATE SET ebit=excluded.ebit
            """,
            (company_id, year, val)
        )
    conn.commit()


def upsert_ttm_ebit(conn: sqlite3.Connection, company_id: int, as_of: str, ebit: float) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        existing = session.query(EbitTtm).filter(EbitTtm.company_id == company_id).one_or_none()
        if existing is None:
            session.add(EbitTtm(company_id=company_id, as_of=as_of, ebit=ebit))
        else:
            existing.as_of = as_of
            existing.ebit = ebit
        session.commit()
        return

    conn.execute(
        """
        INSERT INTO ebit_ttm(company_id, as_of, ebit)
        VALUES(?, ?, ?)
        ON CONFLICT(company_id) DO UPDATE SET as_of=excluded.as_of, ebit=excluded.ebit
        """, (company_id, as_of, ebit)
    )
    conn.commit()


def upsert_annual_interest_expense(conn: sqlite3.Connection, company_id: int, year_to_interest: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_interest.items():
            existing = session.query(InterestExpenseAnnual).filter(
                InterestExpenseAnnual.company_id == company_id,
                InterestExpenseAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(InterestExpenseAnnual(company_id=company_id, fiscal_year=year, interest_expense=val))
            else:
                existing.interest_expense = val
        session.commit()
        return

    cur = conn.cursor()
    for year, val in year_to_interest.items():
        cur.execute(
            """
            INSERT INTO interest_expense_annual(company_id, fiscal_year, interest_expense)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year) DO UPDATE SET interest_expense=excluded.interest_expense
            """,
            (company_id, year, val),
        )
    conn.commit()


def upsert_ttm_interest_expense(conn: sqlite3.Connection, company_id: int, as_of: str, interest_expense: float) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        existing = session.query(InterestExpenseTtm).filter(InterestExpenseTtm.company_id == company_id).one_or_none()
        if existing is None:
            session.add(InterestExpenseTtm(company_id=company_id, as_of=as_of, interest_expense=interest_expense))
        else:
            existing.as_of = as_of
            existing.interest_expense = interest_expense
        session.commit()
        return

    conn.execute(
        """
        INSERT INTO interest_expense_ttm(company_id, as_of, interest_expense)
        VALUES(?, ?, ?)
        ON CONFLICT(company_id) DO UPDATE SET as_of=excluded.as_of, interest_expense=excluded.interest_expense
        """,
        (company_id, as_of, interest_expense),
    )
    conn.commit()

def upsert_annual_operating_income(conn: sqlite3.Connection, company_id: int, year_to_operating_income: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_operating_income.items():
            existing = session.query(OperatingIncomeAnnual).filter(
                OperatingIncomeAnnual.company_id == company_id,
                OperatingIncomeAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(OperatingIncomeAnnual(company_id=company_id, fiscal_year=year, operating_income=val))
            else:
                existing.operating_income = val
        session.commit()
        return

    cur = conn.cursor()
    for year, val in year_to_operating_income.items():
        cur.execute(
            """
            INSERT INTO operating_income_annual(company_id, fiscal_year, operating_income)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year) DO UPDATE SET operating_income=excluded.operating_income
            """,
            (company_id, year, val),
        )
    conn.commit()


def upsert_ttm_operating_income(conn: sqlite3.Connection, company_id: int, as_of: str, operating_income: float) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        existing = session.query(OperatingIncomeTtm).filter(OperatingIncomeTtm.company_id == company_id).one_or_none()
        if existing is None:
            session.add(OperatingIncomeTtm(company_id=company_id, as_of=as_of, operating_income=operating_income))
        else:
            existing.as_of = as_of
            existing.operating_income = operating_income
        session.commit()
        return

    conn.execute(
        """
        INSERT INTO operating_income_ttm(company_id, as_of, operating_income)
        VALUES(?, ?, ?)
        ON CONFLICT(company_id) DO UPDATE SET as_of=excluded.as_of, operating_income=excluded.operating_income
        """,
        (company_id, as_of, operating_income),
    )
    conn.commit()

def upsert_annual_nopat(conn: sqlite3.Connection, company_id: int, year_to_nopat: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_nopat.items():
            existing = session.query(NopatAnnual).filter(
                NopatAnnual.company_id == company_id,
                NopatAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(NopatAnnual(company_id=company_id, fiscal_year=year, nopat=val))
            else:
                existing.nopat = val
        session.commit()
        return

    cur = conn.cursor()
    for year, val in year_to_nopat.items():
        cur.execute(
            """
            INSERT INTO nopat_annual(company_id, fiscal_year, nopat)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year) DO UPDATE SET nopat=excluded.nopat
            """,
            (company_id, year, val)
        )
    conn.commit()

def get_annual_nopat_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
    return _read_df(
        """
        SELECT fiscal_year AS year, nopat
        FROM nopat_annual
        WHERE company_id = ?
        ORDER BY year
        """, conn, params=(company_id,)
    )




def upsert_annual_price_change(conn: sqlite3.Connection, company_id: int, year_to_price_change: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_price_change.items():
            existing = session.query(PriceChangeAnnual).filter(
                PriceChangeAnnual.company_id == company_id,
                PriceChangeAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(PriceChangeAnnual(company_id=company_id, fiscal_year=year, price_change=val))
            else:
                existing.price_change = val
        session.commit()
        return

    
    cur = conn.cursor()
    for year, val in year_to_price_change.items():
        cur.execute(
            """
            INSERT INTO price_change_annual(company_id, fiscal_year, price_change)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year) DO UPDATE SET price_change=excluded.price_change
            """,  # noqa: E501
            (company_id, year, val),
        )
    conn.commit()


def get_annual_price_change_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
    return _read_df(
        """
        SELECT fiscal_year AS year, price_change
        FROM price_change_annual
        WHERE company_id = ?
        ORDER BY year
        """,  # noqa: E501
        conn,
        params=(company_id,),
    )


def upsert_annual_total_assets(conn: sqlite3.Connection, company_id: int, year_to_ta: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_ta.items():
            existing = session.query(TotalAssetsAnnual).filter(
                TotalAssetsAnnual.company_id == company_id,
                TotalAssetsAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(TotalAssetsAnnual(company_id=company_id, fiscal_year=year, total_assets=val))
            else:
                existing.total_assets = val
        session.commit()
        return

    
    cur = conn.cursor()
    for year, val in year_to_ta.items():
        cur.execute(
            "INSERT INTO total_assets_annual(company_id, fiscal_year, total_assets) "
            "VALUES(?, ?, ?) "
            "ON CONFLICT(company_id, fiscal_year) DO UPDATE SET total_assets=excluded.total_assets",
            (company_id, year, val),
        )
    conn.commit()

def upsert_ttm_total_assets(conn: sqlite3.Connection, company_id: int, as_of: str, total_assets: float) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        existing = session.query(TotalAssetsTtm).filter(TotalAssetsTtm.company_id == company_id).one_or_none()
        if existing is None:
            session.add(TotalAssetsTtm(company_id=company_id, as_of=as_of, total_assets=total_assets))
        else:
            existing.as_of = as_of
            existing.total_assets = total_assets
        session.commit()
        return

    
    conn.execute(
        "INSERT INTO total_assets_ttm(company_id, as_of, total_assets) "
        "VALUES(?, ?, ?) "
        "ON CONFLICT(company_id) DO UPDATE SET as_of=excluded.as_of, total_assets=excluded.total_assets",
        (company_id, as_of, total_assets),
    )
    conn.commit()

def upsert_annual_total_current_liabilities(conn: sqlite3.Connection, company_id: int, year_to_tcl: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_tcl.items():
            existing = session.query(TotalCurrentLiabilitiesAnnual).filter(
                TotalCurrentLiabilitiesAnnual.company_id == company_id,
                TotalCurrentLiabilitiesAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(TotalCurrentLiabilitiesAnnual(company_id=company_id, fiscal_year=year, total_current_liabilities=val))
            else:
                existing.total_current_liabilities = val
        session.commit()
        return

    
    cur = conn.cursor()
    for year, val in year_to_tcl.items():
        cur.execute(
            "INSERT INTO total_current_liabilities_annual(company_id, fiscal_year, total_current_liabilities) "
            "VALUES(?, ?, ?) "
            "ON CONFLICT(company_id, fiscal_year) DO UPDATE SET total_current_liabilities=excluded.total_current_liabilities",
            (company_id, year, val),
        )
    conn.commit()

def upsert_ttm_total_current_liabilities(conn: sqlite3.Connection, company_id: int, as_of: str, total_current_liabilities: float) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        existing = session.query(TotalCurrentLiabilitiesTtm).filter(TotalCurrentLiabilitiesTtm.company_id == company_id).one_or_none()
        if existing is None:
            session.add(TotalCurrentLiabilitiesTtm(company_id=company_id, as_of=as_of, total_current_liabilities=total_current_liabilities))
        else:
            existing.as_of = as_of
            existing.total_current_liabilities = total_current_liabilities
        session.commit()
        return

    
    conn.execute(
        "INSERT INTO total_current_liabilities_ttm(company_id, as_of, total_current_liabilities) "
        "VALUES(?, ?, ?) "
        "ON CONFLICT(company_id) DO UPDATE SET as_of=excluded.as_of, total_current_liabilities=excluded.total_current_liabilities",
        (company_id, as_of, total_current_liabilities),
    )
    conn.commit()

def upsert_annual_total_long_term_liabilities(conn: sqlite3.Connection, company_id: int, year_to_tltl: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_tltl.items():
            existing = session.query(TotalLongTermLiabilitiesAnnual).filter(
                TotalLongTermLiabilitiesAnnual.company_id == company_id,
                TotalLongTermLiabilitiesAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(TotalLongTermLiabilitiesAnnual(company_id=company_id, fiscal_year=year, total_long_term_liabilities=val))
            else:
                existing.total_long_term_liabilities = val
        session.commit()
        return

    
    cur = conn.cursor()
    for year, val in year_to_tltl.items():
        cur.execute(
            "INSERT INTO total_long_term_liabilities_annual(company_id, fiscal_year, total_long_term_liabilities) "
            "VALUES(?, ?, ?) "
            "ON CONFLICT(company_id, fiscal_year) DO UPDATE SET total_long_term_liabilities=excluded.total_long_term_liabilities",
            (company_id, year, val),
        )
    conn.commit()

def upsert_ttm_total_long_term_liabilities(conn: sqlite3.Connection, company_id: int, as_of: str, total_long_term_liabilities: float) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        existing = session.query(TotalLongTermLiabilitiesTtm).filter(TotalLongTermLiabilitiesTtm.company_id == company_id).one_or_none()
        if existing is None:
            session.add(TotalLongTermLiabilitiesTtm(company_id=company_id, as_of=as_of, total_long_term_liabilities=total_long_term_liabilities))
        else:
            existing.as_of = as_of
            existing.total_long_term_liabilities = total_long_term_liabilities
        session.commit()
        return

    
    conn.execute(
        "INSERT INTO total_long_term_liabilities_ttm(company_id, as_of, total_long_term_liabilities) "
        "VALUES(?, ?, ?) "
        "ON CONFLICT(company_id) DO UPDATE SET as_of=excluded.as_of, total_long_term_liabilities=excluded.total_long_term_liabilities",
        (company_id, as_of, total_long_term_liabilities),
    )
    conn.commit()

def upsert_annual_total_debt(conn: sqlite3.Connection, company_id: int, year_to_debt: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_debt.items():
            existing = session.query(TotalDebtAnnual).filter(
                TotalDebtAnnual.company_id == company_id,
                TotalDebtAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(TotalDebtAnnual(company_id=company_id, fiscal_year=year, total_debt=val))
            else:
                existing.total_debt = val
        session.commit()
        return

    
    cur = conn.cursor()
    for year, val in year_to_debt.items():
        cur.execute(
            "INSERT INTO total_debt_annual(company_id, fiscal_year, total_debt) "
            "VALUES(?, ?, ?) "
            "ON CONFLICT(company_id, fiscal_year) DO UPDATE SET total_debt=excluded.total_debt",
            (company_id, year, val),
        )
    conn.commit()

def upsert_ttm_total_debt(conn: sqlite3.Connection, company_id: int, as_of: str, total_debt: float) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        existing = session.query(TotalDebtTtm).filter(TotalDebtTtm.company_id == company_id).one_or_none()
        if existing is None:
            session.add(TotalDebtTtm(company_id=company_id, as_of=as_of, total_debt=total_debt))
        else:
            existing.as_of = as_of
            existing.total_debt = total_debt
        session.commit()
        return

    
    conn.execute(
        "INSERT INTO total_debt_ttm(company_id, as_of, total_debt) "
        "VALUES(?, ?, ?) "
        "ON CONFLICT(company_id) DO UPDATE SET as_of=excluded.as_of, total_debt=excluded.total_debt",
        (company_id, as_of, total_debt),
    )
    conn.commit()


def get_annual_total_debt_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
    return _read_df(
        """
        SELECT fiscal_year AS year, total_debt
        FROM total_debt_annual
        WHERE company_id = ?
        ORDER BY year
        """,
        conn,
        params=(company_id,),
    )


def get_annual_market_capitalization_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
    return _read_df(
        """
        SELECT fiscal_year AS year, market_capitalization
        FROM market_capitalization_annual
        WHERE company_id = ?
        ORDER BY year
        """,
        conn,
        params=(company_id,),
    )


def get_annual_roic_direct_upload_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
    return _read_df(
        """
        SELECT fiscal_year AS year, roic_pct
        FROM roic_direct_upload_annual
        WHERE company_id = ?
        ORDER BY fiscal_year
        """,
        conn,
        params=(company_id,),
    )



def upsert_annual_market_capitalization(
    conn: sqlite3.Connection, company_id: int, year_to_market_cap: Dict[int, float]
) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_market_cap.items():
            existing = session.query(MarketCapitalizationAnnual).filter(
                MarketCapitalizationAnnual.company_id == company_id,
                MarketCapitalizationAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(MarketCapitalizationAnnual(company_id=company_id, fiscal_year=year, market_capitalization=val))
            else:
                existing.market_capitalization = val
        session.commit()
        return

    cur = conn.cursor()
    for year, val in year_to_market_cap.items():
        cur.execute(
            "INSERT INTO market_capitalization_annual(company_id, fiscal_year, market_capitalization) "
            "VALUES(?, ?, ?) "
            "ON CONFLICT(company_id, fiscal_year) DO UPDATE SET market_capitalization=excluded.market_capitalization",
            (company_id, year, val),
        )
    conn.commit()


def upsert_annual_roic_direct_upload(
    conn: sqlite3.Connection, company_id: int, year_to_roic_pct: Dict[int, float]
) -> None:
    """Upsert ROIC values (percentage points) that were directly uploaded from the spreadsheet."""
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_roic_pct.items():
            existing = session.query(RoicDirectUploadAnnual).filter(
                RoicDirectUploadAnnual.company_id == company_id,
                RoicDirectUploadAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(RoicDirectUploadAnnual(company_id=company_id, fiscal_year=year, roic_pct=val))
            else:
                existing.roic_pct = val
        session.commit()
        return

    cur = conn.cursor()
    for year, val in year_to_roic_pct.items():
        cur.execute(
            "INSERT INTO roic_direct_upload_annual(company_id, fiscal_year, roic_pct) "
            "VALUES(?, ?, ?) "
            "ON CONFLICT(company_id, fiscal_year) DO UPDATE SET roic_pct=excluded.roic_pct",
            (company_id, year, val),
        )
    conn.commit()



def upsert_annual_debt_equity(conn: sqlite3.Connection, company_id: int, year_to_de: Dict[int, float]) -> None:
    """Store annual Debt/Equity ratios for a company.

    Debt/Equity is defined as:
        Total Debt / (Shareholders Equity + Retained Earnings + Comprehensive Income)
    """
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_de.items():
            existing = session.query(DebtEquityAnnual).filter(
                DebtEquityAnnual.company_id == company_id,
                DebtEquityAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(DebtEquityAnnual(company_id=company_id, fiscal_year=year, debt_equity=float(val)))
            else:
                existing.debt_equity = float(val)
        session.commit()
        return

    cur = conn.cursor()
    for year, val in year_to_de.items():
        cur.execute(
            """
            INSERT INTO debt_equity_annual(company_id, fiscal_year, debt_equity)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year) DO UPDATE SET debt_equity=excluded.debt_equity
            """,
            (company_id, year, float(val)),
        )
    conn.commit()


def get_annual_debt_equity_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
    return _read_df(
        """
        SELECT fiscal_year AS year, debt_equity
        FROM debt_equity_annual
        WHERE company_id = ?
        ORDER BY year
        """,
        conn,
        params=(company_id,),
    )

def upsert_annual_levered_beta(conn: sqlite3.Connection, company_id: int, year_to_beta: Dict[int, float]) -> None:
    """Store annual Levered Beta for a company.

    Levered Beta(y) = Unlevered Beta(bucket) * (1 + (1 - tax_rate) * Debt/Equity(y))
    """
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_beta.items():
            existing = session.query(LeveredBetaAnnual).filter(
                LeveredBetaAnnual.company_id == company_id,
                LeveredBetaAnnual.fiscal_year == int(year),
            ).one_or_none()
            if existing is None:
                session.add(LeveredBetaAnnual(company_id=company_id, fiscal_year=int(year), levered_beta=float(val)))
            else:
                existing.levered_beta = float(val)
        session.commit()
        return

    cur = conn.cursor()
    for year, val in year_to_beta.items():
        cur.execute(
            """
            INSERT INTO levered_beta_annual(company_id, fiscal_year, levered_beta)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year) DO UPDATE SET levered_beta=excluded.levered_beta
            """,
            (company_id, int(year), float(val)),
        )
    conn.commit()


def get_annual_levered_beta_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
    return _read_df(
        """
        SELECT fiscal_year AS year, levered_beta
        FROM levered_beta_annual
        WHERE company_id = ?
        ORDER BY year
        """,
        conn,
        params=(company_id,),
    )


def upsert_annual_total_current_assets(conn: sqlite3.Connection, company_id: int, year_to_tca: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_tca.items():
            existing = session.query(TotalCurrentAssetsAnnual).filter(
                TotalCurrentAssetsAnnual.company_id == company_id,
                TotalCurrentAssetsAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(TotalCurrentAssetsAnnual(company_id=company_id, fiscal_year=year, total_current_assets=val))
            else:
                existing.total_current_assets = val
        session.commit()
        return

    cur = conn.cursor()
    for year, val in year_to_tca.items():
        cur.execute(
            "INSERT INTO total_current_assets_annual(company_id, fiscal_year, total_current_assets) "
            "VALUES(?, ?, ?) "
            "ON CONFLICT(company_id, fiscal_year) DO UPDATE SET total_current_assets=excluded.total_current_assets",
            (company_id, year, val),
        )
    conn.commit()


def upsert_ttm_total_current_assets(conn: sqlite3.Connection, company_id: int, as_of: str, total_current_assets: float) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        existing = session.query(TotalCurrentAssetsTtm).filter(TotalCurrentAssetsTtm.company_id == company_id).one_or_none()
        if existing is None:
            session.add(TotalCurrentAssetsTtm(company_id=company_id, as_of=as_of, total_current_assets=total_current_assets))
        else:
            existing.as_of = as_of
            existing.total_current_assets = total_current_assets
        session.commit()
        return

    conn.execute(
        "INSERT INTO total_current_assets_ttm(company_id, as_of, total_current_assets) "
        "VALUES(?, ?, ?) "
        "ON CONFLICT(company_id) DO UPDATE SET as_of=excluded.as_of, total_current_assets=excluded.total_current_assets",
        (company_id, as_of, total_current_assets),
    )
    conn.commit()

def upsert_annual_current_debt(conn: sqlite3.Connection, company_id: int, year_to_cd: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_cd.items():
            existing = session.query(CurrentDebtAnnual).filter(
                CurrentDebtAnnual.company_id == company_id,
                CurrentDebtAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(CurrentDebtAnnual(company_id=company_id, fiscal_year=year, current_debt=val))
            else:
                existing.current_debt = val
        session.commit()
        return

    cur = conn.cursor()
    for year, val in year_to_cd.items():
        cur.execute(
            "INSERT INTO current_debt_annual(company_id, fiscal_year, current_debt) "
            "VALUES(?, ?, ?) "
            "ON CONFLICT(company_id, fiscal_year) DO UPDATE SET current_debt=excluded.current_debt",
            (company_id, year, val),
        )
    conn.commit()


def upsert_ttm_current_debt(conn: sqlite3.Connection, company_id: int, as_of: str, current_debt: float) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        existing = session.query(CurrentDebtTtm).filter(CurrentDebtTtm.company_id == company_id).one_or_none()
        if existing is None:
            session.add(CurrentDebtTtm(company_id=company_id, as_of=as_of, current_debt=current_debt))
        else:
            existing.as_of = as_of
            existing.current_debt = current_debt
        session.commit()
        return

    conn.execute(
        "INSERT INTO current_debt_ttm(company_id, as_of, current_debt) "
        "VALUES(?, ?, ?) "
        "ON CONFLICT(company_id) DO UPDATE SET as_of=excluded.as_of, current_debt=excluded.current_debt",
        (company_id, as_of, current_debt),
    )
    conn.commit()

def upsert_annual_cash_and_cash_equivalents(conn: sqlite3.Connection, company_id: int, year_to_cash: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_cash.items():
            existing = session.query(CashAndCashEquivalentsAnnual).filter(
                CashAndCashEquivalentsAnnual.company_id == company_id,
                CashAndCashEquivalentsAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(CashAndCashEquivalentsAnnual(company_id=company_id, fiscal_year=year, cash_and_cash_equivalents=val))
            else:
                existing.cash_and_cash_equivalents = val
        session.commit()
        return

    cur = conn.cursor()
    for year, val in year_to_cash.items():
        cur.execute(
            "INSERT INTO cash_and_cash_equivalents_annual(company_id, fiscal_year, cash_and_cash_equivalents) "
            "VALUES(?, ?, ?) "
            "ON CONFLICT(company_id, fiscal_year) DO UPDATE SET cash_and_cash_equivalents=excluded.cash_and_cash_equivalents",
            (company_id, year, val),
        )
    conn.commit()


def upsert_ttm_cash_and_cash_equivalents(conn: sqlite3.Connection, company_id: int, as_of: str, cash_and_cash_equivalents: float) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        existing = session.query(CashAndCashEquivalentsTtm).filter(CashAndCashEquivalentsTtm.company_id == company_id).one_or_none()
        if existing is None:
            session.add(CashAndCashEquivalentsTtm(company_id=company_id, as_of=as_of, cash_and_cash_equivalents=cash_and_cash_equivalents))
        else:
            existing.as_of = as_of
            existing.cash_and_cash_equivalents = cash_and_cash_equivalents
        session.commit()
        return

    conn.execute(
        "INSERT INTO cash_and_cash_equivalents_ttm(company_id, as_of, cash_and_cash_equivalents) "
        "VALUES(?, ?, ?) "
        "ON CONFLICT(company_id) DO UPDATE SET as_of=excluded.as_of, cash_and_cash_equivalents=excluded.cash_and_cash_equivalents",
        (company_id, as_of, cash_and_cash_equivalents),
    )
    conn.commit()

def upsert_annual_long_term_investments(conn: sqlite3.Connection, company_id: int, year_to_lti: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_lti.items():
            existing = session.query(LongTermInvestmentsAnnual).filter(
                LongTermInvestmentsAnnual.company_id == company_id,
                LongTermInvestmentsAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(LongTermInvestmentsAnnual(company_id=company_id, fiscal_year=year, long_term_investments=val))
            else:
                existing.long_term_investments = val
        session.commit()
        return

    cur = conn.cursor()
    for year, val in year_to_lti.items():
        cur.execute(
            "INSERT INTO long_term_investments_annual(company_id, fiscal_year, long_term_investments) "
            "VALUES(?, ?, ?) "
            "ON CONFLICT(company_id, fiscal_year) DO UPDATE SET long_term_investments=excluded.long_term_investments",
            (company_id, year, val),
        )
    conn.commit()


def upsert_ttm_long_term_investments(conn: sqlite3.Connection, company_id: int, as_of: str, long_term_investments: float) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        existing = session.query(LongTermInvestmentsTtm).filter(LongTermInvestmentsTtm.company_id == company_id).one_or_none()
        if existing is None:
            session.add(LongTermInvestmentsTtm(company_id=company_id, as_of=as_of, long_term_investments=long_term_investments))
        else:
            existing.as_of = as_of
            existing.long_term_investments = long_term_investments
        session.commit()
        return

    conn.execute(
        "INSERT INTO long_term_investments_ttm(company_id, as_of, long_term_investments) "
        "VALUES(?, ?, ?) "
        "ON CONFLICT(company_id) DO UPDATE SET as_of=excluded.as_of, long_term_investments=excluded.long_term_investments",
        (company_id, as_of, long_term_investments),
    )
    conn.commit()

def upsert_annual_capital_employed(conn: sqlite3.Connection, company_id: int, year_to_ce: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_ce.items():
            existing = session.query(CapitalEmployedAnnual).filter(
                CapitalEmployedAnnual.company_id == company_id,
                CapitalEmployedAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(CapitalEmployedAnnual(company_id=company_id, fiscal_year=year, capital_employed=val))
            else:
                existing.capital_employed = val
        session.commit()
        return

    
    cur = conn.cursor()
    for year, val in year_to_ce.items():
        cur.execute(
            "INSERT INTO capital_employed_annual(company_id, fiscal_year, capital_employed) "
            "VALUES(?, ?, ?) "
            "ON CONFLICT(company_id, fiscal_year) DO UPDATE SET capital_employed=excluded.capital_employed",
            (company_id, year, val),
        )
    conn.commit()

def upsert_annual_invested_capital(conn: sqlite3.Connection, company_id: int, year_to_ic: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_ic.items():
            existing = session.query(InvestedCapitalAnnual).filter(
                InvestedCapitalAnnual.company_id == company_id,
                InvestedCapitalAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(InvestedCapitalAnnual(company_id=company_id, fiscal_year=year, invested_capital=val))
            else:
                existing.invested_capital = val
        session.commit()
        return

    
    cur = conn.cursor()
    for year, val in year_to_ic.items():
        cur.execute(
            "INSERT INTO invested_capital_annual(company_id, fiscal_year, invested_capital) "
            "VALUES(?, ?, ?) "
            "ON CONFLICT(company_id, fiscal_year) DO UPDATE SET invested_capital=excluded.invested_capital",
            (company_id, year, val),
        )
    conn.commit()


def upsert_annual_shareholders_equity(conn: sqlite3.Connection, company_id: int, year_to_equity: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_equity.items():
            existing = session.query(ShareholdersEquityAnnual).filter(
                ShareholdersEquityAnnual.company_id == company_id,
                ShareholdersEquityAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(ShareholdersEquityAnnual(company_id=company_id, fiscal_year=year, shareholders_equity=val))
            else:
                existing.shareholders_equity = val
        session.commit()
        return

    
    cur = conn.cursor()
    for year, val in year_to_equity.items():
        cur.execute(
            """
            INSERT INTO shareholders_equity_annual(company_id, fiscal_year, shareholders_equity)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year) DO UPDATE SET shareholders_equity=excluded.shareholders_equity
            """,
            (company_id, year, val),
        )
    conn.commit()

def upsert_ttm_shareholders_equity(conn: sqlite3.Connection, company_id: int, as_of: str, equity: float) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        existing = session.query(ShareholdersEquityTtm).filter(ShareholdersEquityTtm.company_id == company_id).one_or_none()
        if existing is None:
            session.add(ShareholdersEquityTtm(company_id=company_id, as_of=as_of, shareholders_equity=equity))
        else:
            existing.as_of = as_of
            existing.shareholders_equity = equity
        session.commit()
        return

    
    conn.execute(
        """
        INSERT INTO shareholders_equity_ttm(company_id, as_of, shareholders_equity)
        VALUES(?, ?, ?)
        ON CONFLICT(company_id) DO UPDATE SET as_of=excluded.as_of, shareholders_equity=excluded.shareholders_equity
        """,
        (company_id, as_of, equity),
    )
    conn.commit()


def get_annual_shareholders_equity_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
    return _read_df(
        """
        SELECT fiscal_year AS year, shareholders_equity
        FROM shareholders_equity_annual
        WHERE company_id = ?
        ORDER BY year
        """,
        conn,
        params=(company_id,),
    )

def upsert_annual_retained_earnings(conn: sqlite3.Connection, company_id: int, year_to_re: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_re.items():
            existing = session.query(RetainedEarningsAnnual).filter(
                RetainedEarningsAnnual.company_id == company_id,
                RetainedEarningsAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(RetainedEarningsAnnual(company_id=company_id, fiscal_year=year, retained_earnings=val))
            else:
                existing.retained_earnings = val
        session.commit()
        return

    
    cur = conn.cursor()
    for year, val in year_to_re.items():
        cur.execute(
            """
            INSERT INTO retained_earnings_annual(company_id, fiscal_year, retained_earnings)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year) DO UPDATE SET retained_earnings=excluded.retained_earnings
            """,
            (company_id, year, val),
        )
    conn.commit()

def upsert_ttm_retained_earnings(conn: sqlite3.Connection, company_id: int, as_of: str, retained_earnings: float) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        existing = session.query(RetainedEarningsTtm).filter(RetainedEarningsTtm.company_id == company_id).one_or_none()
        if existing is None:
            session.add(RetainedEarningsTtm(company_id=company_id, as_of=as_of, retained_earnings=retained_earnings))
        else:
            existing.as_of = as_of
            existing.retained_earnings = retained_earnings
        session.commit()
        return

    
    conn.execute(
        """
        INSERT INTO retained_earnings_ttm(company_id, as_of, retained_earnings)
        VALUES(?, ?, ?)
        ON CONFLICT(company_id) DO UPDATE SET as_of=excluded.as_of, retained_earnings=excluded.retained_earnings
        """,
        (company_id, as_of, retained_earnings),
    )
    conn.commit()


def get_annual_retained_earnings_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
    return _read_df(
        """
        SELECT fiscal_year AS year, retained_earnings
        FROM retained_earnings_annual
        WHERE company_id = ?
        ORDER BY year
        """,
        conn,
        params=(company_id,),
    )

def upsert_annual_comprehensive_income(conn: sqlite3.Connection, company_id: int, year_to_ci: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_ci.items():
            existing = session.query(ComprehensiveIncomeAnnual).filter(
                ComprehensiveIncomeAnnual.company_id == company_id,
                ComprehensiveIncomeAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(ComprehensiveIncomeAnnual(company_id=company_id, fiscal_year=year, comprehensive_income=val))
            else:
                existing.comprehensive_income = val
        session.commit()
        return

    
    cur = conn.cursor()
    for year, val in year_to_ci.items():
        cur.execute(
            """
            INSERT INTO comprehensive_income_annual(company_id, fiscal_year, comprehensive_income)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year) DO UPDATE SET comprehensive_income=excluded.comprehensive_income
            """,
            (company_id, year, val),
        )
    conn.commit()

def upsert_ttm_comprehensive_income(conn: sqlite3.Connection, company_id: int, as_of: str, comprehensive_income: float) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        existing = session.query(ComprehensiveIncomeTtm).filter(ComprehensiveIncomeTtm.company_id == company_id).one_or_none()
        if existing is None:
            session.add(ComprehensiveIncomeTtm(company_id=company_id, as_of=as_of, comprehensive_income=comprehensive_income))
        else:
            existing.as_of = as_of
            existing.comprehensive_income = comprehensive_income
        session.commit()
        return

    
    conn.execute(
        """
        INSERT INTO comprehensive_income_ttm(company_id, as_of, comprehensive_income)
        VALUES(?, ?, ?)
        ON CONFLICT(company_id) DO UPDATE SET as_of=excluded.as_of, comprehensive_income=excluded.comprehensive_income
        """,
        (company_id, as_of, comprehensive_income),
    )
    conn.commit()


def get_annual_comprehensive_income_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
    return _read_df(
        """
        SELECT fiscal_year AS year, comprehensive_income
        FROM comprehensive_income_annual
        WHERE company_id = ?
        ORDER BY year
        """,
        conn,
        params=(company_id,),
    )

def upsert_annual_accumulated_profit(conn: sqlite3.Connection, company_id: int, year_to_accumulated: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_accumulated.items():
            existing = session.query(AccumulatedProfitAnnual).filter(
                AccumulatedProfitAnnual.company_id == company_id,
                AccumulatedProfitAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(AccumulatedProfitAnnual(company_id=company_id, fiscal_year=year, accumulated_profit=val))
            else:
                existing.accumulated_profit = val
        session.commit()
        return

    
    cur = conn.cursor()
    for year, val in year_to_accumulated.items():
        cur.execute(
            """
            INSERT INTO accumulated_profit_annual(company_id, fiscal_year, accumulated_profit)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year) DO UPDATE SET accumulated_profit=excluded.accumulated_profit
            """,
            (company_id, year, val),
        )
    conn.commit()

def get_annual_accumulated_profit_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
    return _read_df(
        """
        SELECT fiscal_year AS year, accumulated_profit
        FROM accumulated_profit_annual
        WHERE company_id = ?
        ORDER BY year
        """,
        conn,
        params=(company_id,),
    )


def upsert_annual_total_equity(conn: sqlite3.Connection, company_id: int, year_to_total: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_total.items():
            existing = session.query(TotalEquityAnnual).filter(
                TotalEquityAnnual.company_id == company_id,
                TotalEquityAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(TotalEquityAnnual(company_id=company_id, fiscal_year=year, total_equity=val))
            else:
                existing.total_equity = val
        session.commit()
        return

    
    cur = conn.cursor()
    for year, val in year_to_total.items():
        cur.execute(
            """
            INSERT INTO total_equity_annual(company_id, fiscal_year, total_equity)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year) DO UPDATE SET total_equity=excluded.total_equity
            """,
            (company_id, year, val),
        )
    conn.commit()


def upsert_annual_average_equity(conn: sqlite3.Connection, company_id: int, year_to_avg: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_avg.items():
            existing = session.query(AverageEquityAnnual).filter(
                AverageEquityAnnual.company_id == company_id,
                AverageEquityAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(AverageEquityAnnual(company_id=company_id, fiscal_year=year, average_equity=val))
            else:
                existing.average_equity = val
        session.commit()
        return

    
    cur = conn.cursor()
    for year, val in year_to_avg.items():
        cur.execute(
            """
            INSERT INTO average_equity_annual(company_id, fiscal_year, average_equity)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year) DO UPDATE SET average_equity=excluded.average_equity
            """,
            (company_id, year, val),
        )
    conn.commit()


def upsert_annual_roe(conn: sqlite3.Connection, company_id: int, year_to_roe: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_roe.items():
            existing = session.query(RoeAnnual).filter(
                RoeAnnual.company_id == company_id,
                RoeAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(RoeAnnual(company_id=company_id, fiscal_year=year, roe=val))
            else:
                existing.roe = val
        session.commit()
        return

    
    cur = conn.cursor()
    for year, val in year_to_roe.items():
        cur.execute(
            """
            INSERT INTO roe_annual(company_id, fiscal_year, roe)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year) DO UPDATE SET roe=excluded.roe
            """,
            (company_id, year, val),
        )
    conn.commit()


def get_annual_roe_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
    return _read_df(
        """
        SELECT fiscal_year AS year, roe
        FROM roe_annual
        WHERE company_id = ?
        ORDER BY year
        """,
        conn,
        params=(company_id,),
    )




def upsert_annual_roce(conn: sqlite3.Connection, company_id: int, year_to_roce: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_roce.items():
            existing = session.query(RoceAnnual).filter(
                RoceAnnual.company_id == company_id,
                RoceAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(RoceAnnual(company_id=company_id, fiscal_year=year, roce=val))
            else:
                existing.roce = val
        session.commit()
        return

    
    cur = conn.cursor()
    for year, val in year_to_roce.items():
        cur.execute(
            """
            INSERT INTO roce_annual(company_id, fiscal_year, roce)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year) DO UPDATE SET roce=excluded.roce
            """,
            (company_id, year, val),
        )
    conn.commit()


def get_annual_roce_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
    return _read_df(
        """
        SELECT fiscal_year AS year, roce
        FROM roce_annual
        WHERE company_id = ?
        ORDER BY year
        """,
        conn,
        params=(company_id,),
    )



def upsert_annual_interest_coverage(conn: sqlite3.Connection, company_id: int, year_to_ratio: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_ratio.items():
            existing = session.query(InterestCoverageAnnual).filter(
                InterestCoverageAnnual.company_id == company_id,
                InterestCoverageAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(InterestCoverageAnnual(company_id=company_id, fiscal_year=year, interest_coverage_ratio=val))
            else:
                existing.interest_coverage_ratio = val
        session.commit()
        return

    
    cur = conn.cursor()
    for year, val in year_to_ratio.items():
        cur.execute(
            """
            INSERT INTO interest_coverage_annual(company_id, fiscal_year, interest_coverage_ratio)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year) DO UPDATE SET interest_coverage_ratio=excluded.interest_coverage_ratio
            """,
            (company_id, year, val),
        )
    conn.commit()

def get_annual_interest_coverage_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
    return _read_df(
        """
        SELECT fiscal_year AS year, interest_coverage_ratio
        FROM interest_coverage_annual
        WHERE company_id = ?
        ORDER BY year
        """,
        conn,
        params=(company_id,),
    )


def list_companies(conn: sqlite3.Connection) -> pd.DataFrame:
    session = getattr(conn, "session", None)
    if session is not None:
        rows = session.query(Companies.id, Companies.name, Companies.ticker).order_by(Companies.name).all()
        return pd.DataFrame(rows, columns=["id", "name", "ticker"])
    return _read_df("SELECT id, name, ticker FROM companies ORDER BY name", conn)



def upsert_annual_interest_load(conn: sqlite3.Connection, company_id: int, year_to_load: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_load.items():
            existing = session.query(InterestLoadAnnual).filter(
                InterestLoadAnnual.company_id == company_id,
                InterestLoadAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(InterestLoadAnnual(company_id=company_id, fiscal_year=year, interest_load_pct=val))
            else:
                existing.interest_load_pct = val
        session.commit()
        return

    
    """
    Store per-year Interest Load % values for a company.

    Interest Load % is defined as (1 / Interest Coverage Ratio) * 100,
    with a small sentinel value used when the coverage ratio is 0 or NaN.
    """
    cur = conn.cursor()
    for year, val in year_to_load.items():
        cur.execute(
            """
            INSERT INTO interest_load_annual(company_id, fiscal_year, interest_load_pct)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year) DO UPDATE SET interest_load_pct=excluded.interest_load_pct
            """,
            (company_id, year, val),
        )
    conn.commit()


def get_annual_interest_load_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
    """
    Retrieve the stored Interest Load % series for a given company.

    Returns a DataFrame with columns:
        - year
        - interest_load_pct
    """
    return _read_df(
        """
        SELECT fiscal_year AS year, interest_load_pct
        FROM interest_load_annual
        WHERE company_id = ?
        ORDER BY year
        """,
        conn,
        params=(company_id,),
    )





def upsert_annual_default_spread(conn: sqlite3.Connection, company_id: int, year_to_spread: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_spread.items():
            existing = session.query(DefaultSpreadAnnual).filter(
                DefaultSpreadAnnual.company_id == company_id,
                DefaultSpreadAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(DefaultSpreadAnnual(company_id=company_id, fiscal_year=year, default_spread=val))
            else:
                existing.default_spread = val
        session.commit()
        return

    
    """Store per-year Default Spread values for a company.

    Notes:
      - Default Spread is stored in *percentage points* (e.g., 1.21 means 1.21%).
      - Values are derived from Interest Coverage Ratio using Damodaran-style buckets.
    """
    cur = conn.cursor()
    for year, val in year_to_spread.items():
        cur.execute(
            """
            INSERT INTO default_spread_annual(company_id, fiscal_year, default_spread)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year) DO UPDATE SET default_spread=excluded.default_spread
            """,
            (company_id, int(year), float(val)),
        )
    conn.commit()


def get_annual_default_spread_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
    """Retrieve the stored Default Spread series for a given company."""
    return _read_df(
        """
        SELECT fiscal_year AS year, default_spread
        FROM default_spread_annual
        WHERE company_id = ?
        ORDER BY year
        """,
        conn,
        params=(company_id,),
    )



def upsert_annual_non_cash_working_capital(conn: sqlite3.Connection, company_id: int, year_to_ncwc: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_ncwc.items():
            existing = session.query(NonCashWorkingCapitalAnnual).filter(
                NonCashWorkingCapitalAnnual.company_id == company_id,
                NonCashWorkingCapitalAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(NonCashWorkingCapitalAnnual(company_id=company_id, fiscal_year=year, non_cash_working_capital=val))
            else:
                existing.non_cash_working_capital = val
        session.commit()
        return

    
    """
    Store per-year Non-Cash Working Capital values for a company.

    Non-Cash Working Capital is defined as:
        (Total Current Assets - Cash and Cash Equivalents)
        minus
        (Total Current Liabilities - Current Debt)
    """
    cur = conn.cursor()
    for year, val in year_to_ncwc.items():
        cur.execute(
            """
            INSERT INTO non_cash_working_capital_annual(company_id, fiscal_year, non_cash_working_capital)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year)
            DO UPDATE SET non_cash_working_capital=excluded.non_cash_working_capital
            """,
            (company_id, year, val),
        )
    conn.commit()


def get_annual_non_cash_working_capital_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
    """
    Retrieve the stored Non-Cash Working Capital series for a given company.

    Returns a DataFrame with columns:
        - year
        - non_cash_working_capital
    """
    return _read_df(
        """
        SELECT fiscal_year AS year, non_cash_working_capital
        FROM non_cash_working_capital_annual
        WHERE company_id = ?
        ORDER BY year
        """,
        conn,
        params=(company_id,),
    )


def upsert_annual_revenue_yield_non_cash_working_capital(
    conn: sqlite3.Connection,
    company_id: int,
    year_to_revenue_yield: Dict[int, float],
) -> None:
    """
    Store per-year Revenue Yield of Non-Cash Working Capital values for a company.

    Revenue Yield of Non-Cash Working Capital is defined as:
        1 - (Non-Cash Working Capital / Revenue)

    Values are stored as decimals (for example, 0.40 for 40%).
    """
    if not year_to_revenue_yield:
        return

    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_revenue_yield.items():
            existing = session.query(RevenueYieldNonCashWorkingCapitalAnnual).filter(
                RevenueYieldNonCashWorkingCapitalAnnual.company_id == company_id,
                RevenueYieldNonCashWorkingCapitalAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(
                    RevenueYieldNonCashWorkingCapitalAnnual(
                        company_id=company_id,
                        fiscal_year=year,
                        revenue_yield_ncwc=val,
                    )
                )
            else:
                existing.revenue_yield_ncwc = val
        session.commit()
        return

    cur = conn.cursor()
    for year, val in year_to_revenue_yield.items():
        cur.execute(
            """
            INSERT INTO revenue_yield_non_cash_working_capital_annual(
                company_id,
                fiscal_year,
                revenue_yield_ncwc
            )
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year)
            DO UPDATE SET revenue_yield_ncwc=excluded.revenue_yield_ncwc
            """,
            (company_id, year, val),
        )
    conn.commit()



def upsert_annual_research_and_development_expense(
    conn: sqlite3.Connection, company_id: int, year_to_rd: Dict[int, float]
) -> None:
    """Store per-year Research & Development (R&D) expense values for a company."""
    if not year_to_rd:
        return

    session = getattr(conn, "session", None)
    if session is not None:
        for year, rd_expense in year_to_rd.items():
            existing = session.query(ResearchAndDevelopmentExpenseAnnual).filter(
                ResearchAndDevelopmentExpenseAnnual.company_id == company_id,
                ResearchAndDevelopmentExpenseAnnual.fiscal_year == int(year),
            ).one_or_none()
            if existing is None:
                session.add(
                    ResearchAndDevelopmentExpenseAnnual(
                        company_id=company_id,
                        fiscal_year=int(year),
                        research_and_development_expense=float(rd_expense),
                    )
                )
            else:
                existing.research_and_development_expense = float(rd_expense)
        session.commit()
        return

    cur = conn.cursor()
    for year, rd_expense in year_to_rd.items():
        cur.execute(
            """
            INSERT INTO research_and_development_expense_annual(company_id, fiscal_year, research_and_development_expense)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year) DO UPDATE SET research_and_development_expense=excluded.research_and_development_expense
            """,
            (company_id, int(year), float(rd_expense)),
        )
    conn.commit()

def get_annual_research_and_development_expense_series(conn: sqlite3.Connection, company_id: int) -> Dict[int, float]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT fiscal_year, research_and_development_expense
        FROM research_and_development_expense_annual
        WHERE company_id=?
        ORDER BY fiscal_year
        """,
        (company_id,),
    )
    rows = cur.fetchall()
    return {int(y): float(v) for y, v in rows}

def upsert_annual_capital_expenditures(conn: sqlite3.Connection, company_id: int, year_to_capex: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_capex.items():
            existing = session.query(CapitalExpendituresAnnual).filter(
                CapitalExpendituresAnnual.company_id == company_id,
                CapitalExpendituresAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(CapitalExpendituresAnnual(company_id=company_id, fiscal_year=year, capital_expenditures=val))
            else:
                existing.capital_expenditures = val
        session.commit()
        return

    
    """Store per-year Capital Expenditures (CapEx) values for a company.

    Note: in many cash flow statements CapEx is shown as a negative cash outflow.
    This app stores CapEx with the spreadsheet sign flipped (outflow becomes positive).
    """
    if not year_to_capex:
        return
    cur = conn.cursor()
    for year, capex in year_to_capex.items():
        cur.execute(
            """
            INSERT INTO capital_expenditures_annual(company_id, fiscal_year, capital_expenditures)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year) DO UPDATE SET capital_expenditures=excluded.capital_expenditures
            """,
            (company_id, int(year), float(capex)),
        )
    conn.commit()

def get_annual_capital_expenditures_series(conn: sqlite3.Connection, company_id: int) -> Dict[int, float]:
    cur = conn.cursor()
    cur.execute(
        "SELECT fiscal_year, capital_expenditures FROM capital_expenditures_annual WHERE company_id=? ORDER BY fiscal_year",
        (company_id,),
    )
    rows = cur.fetchall()
    return {int(y): float(v) for y, v in rows}

def upsert_annual_depreciation_amortization(conn: sqlite3.Connection, company_id: int, year_to_da: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_da.items():
            existing = session.query(DepreciationAmortizationAnnual).filter(
                DepreciationAmortizationAnnual.company_id == company_id,
                DepreciationAmortizationAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(DepreciationAmortizationAnnual(company_id=company_id, fiscal_year=year, depreciation_amortization=val))
            else:
                existing.depreciation_amortization = val
        session.commit()
        return

    
    """Store per-year Depreciation & Amortization (D&A) values for a company."""
    if not year_to_da:
        return
    cur = conn.cursor()
    for year, da in year_to_da.items():
        cur.execute(
            """
            INSERT INTO depreciation_amortization_annual(company_id, fiscal_year, depreciation_amortization)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year) DO UPDATE SET depreciation_amortization=excluded.depreciation_amortization
            """,
            (company_id, int(year), float(da)),
        )
    conn.commit()

def get_annual_depreciation_amortization_series(conn: sqlite3.Connection, company_id: int) -> Dict[int, float]:
    cur = conn.cursor()
    cur.execute(
        "SELECT fiscal_year, depreciation_amortization FROM depreciation_amortization_annual WHERE company_id=? ORDER BY fiscal_year",
        (company_id,),
    )
    rows = cur.fetchall()
    return {int(y): float(v) for y, v in rows}



def upsert_annual_net_debt_issued_paid(
    conn: sqlite3.Connection, company_id: int, year_to_net_debt: Dict[int, float]
) -> None:
    """Store per-year Net Debt Issued/Paid values for a company."""
    if not year_to_net_debt:
        return

    session = getattr(conn, "session", None)
    if session is not None:
        for year, net_debt in year_to_net_debt.items():
            existing = session.query(NetDebtIssuedPaidAnnual).filter(
                NetDebtIssuedPaidAnnual.company_id == company_id,
                NetDebtIssuedPaidAnnual.fiscal_year == int(year),
            ).one_or_none()
            if existing is None:
                session.add(
                    NetDebtIssuedPaidAnnual(
                        company_id=company_id,
                        fiscal_year=int(year),
                        net_debt_issued_paid=float(net_debt),
                    )
                )
            else:
                existing.net_debt_issued_paid = float(net_debt)
        session.commit()
        return

    cur = conn.cursor()
    for year, net_debt in year_to_net_debt.items():
        cur.execute(
            """
            INSERT INTO net_debt_issued_paid_annual(company_id, fiscal_year, net_debt_issued_paid)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year) DO UPDATE SET net_debt_issued_paid=excluded.net_debt_issued_paid
            """,
            (company_id, int(year), float(net_debt)),
        )
    conn.commit()


def get_annual_net_debt_issued_paid_series(conn: sqlite3.Connection, company_id: int) -> Dict[int, float]:
    cur = conn.cursor()
    cur.execute(
        "SELECT fiscal_year, net_debt_issued_paid FROM net_debt_issued_paid_annual WHERE company_id=? ORDER BY fiscal_year",
        (company_id,),
    )
    rows = cur.fetchall()
    return {int(y): float(v) for y, v in rows}


def get_annual_revenue_yield_non_cash_working_capital_series(
    conn: sqlite3.Connection,
    company_id: int,
) -> pd.DataFrame:
    """
    Retrieve the stored Revenue Yield of Non-Cash Working Capital series for a company.

    Returns a DataFrame with columns:
        - year
        - revenue_yield_ncwc
    """
    return _read_df(
        """
        SELECT fiscal_year AS year, revenue_yield_ncwc
        FROM revenue_yield_non_cash_working_capital_annual
        WHERE company_id = ?
        ORDER BY year
        """,
        conn,
        params=(company_id,),
    )


def get_annual_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
    return _read_df(
        """
        SELECT fiscal_year AS year, revenue
        FROM revenues_annual
        WHERE company_id = ?
        ORDER BY year
        """, conn, params=(company_id,)
    )

def get_annual_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
    return _read_df(
        """
        SELECT fiscal_year AS year, revenue
        FROM revenues_annual
        WHERE company_id = ?
        ORDER BY year
        """, conn, params=(company_id,)
    )



def get_annual_price_change_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
    """
    Return the annual price change series for a company as a DataFrame with:
        - year
        - price_change (stored as a decimal, e.g. 0.15 for 15%)
    """
    return _read_df(
        """
        SELECT fiscal_year AS year, price_change
        FROM price_change_annual
        WHERE company_id = ?
        ORDER BY year
        """,
        conn,
        params=(company_id,),
    )

def get_annual_op_margin_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
    return _read_df(
        """
        SELECT fiscal_year AS year, margin
        FROM op_margin_annual
        WHERE company_id = ?
        ORDER BY year
        """, conn, params=(company_id,)
    )


def compute_growth_stats(
    df_in: pd.DataFrame,
    yr_start: int,
    yr_end: int,
    stdev_sample: bool = True,
    value_col: str = "revenue",
    abs_denom: bool = False,
) -> Tuple[Optional[float], Optional[float]]:
    """
    Compute year-over-year growth statistics for a given value column.

    Growth is computed as:
        (current_value - previous_value) / (denominator)

    If abs_denom is True, the denominator is abs(previous_value); otherwise it is
    previous_value itself. Rows with a zero denominator are skipped.
    """
    df = df_in.copy()
    df = df[(df["year"] >= yr_end) & (df["year"] <= yr_start)].sort_values("year")
    growths: List[float] = []
    for i in range(1, len(df)):
        prev = df.iloc[i - 1]
        cur = df.iloc[i]
        prev_val = prev.get(value_col)
        cur_val = cur.get(value_col)
        if pd.notna(prev_val) and pd.notna(cur_val):
            prev_f = float(prev_val)
            cur_f = float(cur_val)
            denom = abs(prev_f) if abs_denom else prev_f
            if denom != 0.0:
                g = (cur_f - prev_f) / denom
                growths.append(float(g))
    if not growths:
        return None, None
    arr = np.array(growths, dtype=float)
    median = float(np.median(arr))
    ddof = 1 if stdev_sample and arr.size > 1 else 0
    stdev = float(np.std(arr, ddof=ddof)) if arr.size > 1 or ddof == 0 else None
    return median, stdev


def compute_margin_stats(
    margins: pd.DataFrame,
    yr_start: int,
    yr_end: int,
    stdev_sample: bool = True,
) -> Tuple[Optional[float], Optional[float], bool]:
    """
    Compute median and standard deviation of operating margin over a year range.

    Returns (median_margin, stdev_margin, values_are_fraction).

    values_are_fraction indicates whether the margin values look like fractions
    (for example 0.25 for 25%) or already-percent values (for example 25.0).
    """
    df = margins.copy()
    df = df[(df["year"] >= yr_end) & (df["year"] <= yr_start)].sort_values("year")
    vals = df["margin"].dropna().astype(float).values
    if vals.size == 0:
        return None, None, True
    values_are_fraction = bool(np.nanmax(np.abs(vals)) <= 1.5)
    arr = np.array(vals, dtype=float)
    median = float(np.median(arr))
    ddof = 1 if stdev_sample and arr.size > 1 else 0
    stdev = float(np.std(arr, ddof=ddof)) if arr.size > 1 or ddof == 0 else None
    return median, stdev, values_are_fraction


def compute_margin_growth_stats(
    margins: pd.DataFrame,
    yr_start: int,
    yr_end: int,
    stdev_sample: bool = True,
) -> Tuple[Optional[float], Optional[float]]:
    """
    Compute year-over-year growth statistics for operating margin, using:

        (current_margin - previous_margin) / abs(previous_margin)

    This focuses on the volatility of margin changes rather than their level.
    """
    df = margins.copy()
    df = df[(df["year"] >= yr_end) & (df["year"] <= yr_start)].sort_values("year")
    growths: List[float] = []
    for i in range(1, len(df)):
        prev = df.iloc[i - 1]
        cur = df.iloc[i]
        prev_val = prev.get("margin")
        cur_val = cur.get("margin")
        if pd.notna(prev_val) and pd.notna(cur_val):
            prev_val_f = float(prev_val)
            cur_val_f = float(cur_val)
            denom = abs(prev_val_f)
            if denom != 0.0:
                g = (cur_val_f - prev_val_f) / denom
                growths.append(float(g))
    if not growths:
        return None, None
    arr = np.array(growths, dtype=float)
    median = float(np.median(arr))
    ddof = 1 if stdev_sample and arr.size > 1 else 0
    stdev = float(np.std(arr, ddof=ddof)) if arr.size > 1 or ddof == 0 else None
    return median, stdev


# ---------------------------
# UI
# ---------------------------


# ---------------------------
# Cash Flow (computed): FCFF & Reinvestment Rate
# ---------------------------

def upsert_annual_fcff(conn: sqlite3.Connection, company_id: int, year_to_fcff: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_fcff.items():
            existing = session.query(FcffAnnual).filter(
                FcffAnnual.company_id == company_id,
                FcffAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(FcffAnnual(company_id=company_id, fiscal_year=year, fcff=val))
            else:
                existing.fcff = val
        session.commit()
        return

    
    """Store per-year Free Cash Flow to the Firm (FCFF) values for a company."""
    if not year_to_fcff:
        return
    cur = conn.cursor()
    for year, val in year_to_fcff.items():
        cur.execute(
            """
            INSERT INTO fcff_annual(company_id, fiscal_year, fcff)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year) DO UPDATE SET fcff=excluded.fcff
            """,
            (company_id, int(year), float(val) if val is not None else None),
        )
    conn.commit()


def get_annual_fcff_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
    """Retrieve stored FCFF series for a company (year, fcff)."""
    return _read_df(
        """
        SELECT fiscal_year AS year, fcff
        FROM fcff_annual
        WHERE company_id = ?
        ORDER BY year
        """,
        conn,
        params=(company_id,),
    )



def upsert_annual_fcfe(conn: sqlite3.Connection, company_id: int, year_to_fcfe: Dict[int, float]) -> None:
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_fcfe.items():
            existing = session.query(FcfeAnnual).filter(
                FcfeAnnual.company_id == company_id,
                FcfeAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(FcfeAnnual(company_id=company_id, fiscal_year=year, fcfe=val))
            else:
                existing.fcfe = val
        session.commit()
        return

    
    """Store per-year Free Cash Flow to Equity (FCFE) values for a company."""
    if not year_to_fcfe:
        return
    cur = conn.cursor()
    for year, val in year_to_fcfe.items():
        cur.execute(
            """
            INSERT INTO fcfe_annual(company_id, fiscal_year, fcfe)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year) DO UPDATE SET fcfe=excluded.fcfe
            """,
            (company_id, int(year), float(val) if val is not None else None),
        )
    conn.commit()


def get_annual_fcfe_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
    """Retrieve stored FCFE series for a company (year, fcfe)."""
    return _read_df(
        """
        SELECT fiscal_year AS year, fcfe
        FROM fcfe_annual
        WHERE company_id = ?
        ORDER BY year
        """,
        conn,
        params=(company_id,),
    )

def upsert_annual_reinvestment_rate(
    conn: sqlite3.Connection, company_id: int, year_to_rate: Dict[int, float]
) -> None:
    """Store annual Reinvestment Rate values for a company."""
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_rate.items():
            existing = session.query(ReinvestmentRateAnnual).filter(
                ReinvestmentRateAnnual.company_id == company_id,
                ReinvestmentRateAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(ReinvestmentRateAnnual(company_id=company_id, fiscal_year=year, reinvestment_rate=val))
            else:
                existing.reinvestment_rate = val
        session.commit()
        return

    cur = conn.cursor()
    for year, val in year_to_rate.items():
        cur.execute(
            """
            INSERT INTO reinvestment_rate_annual(company_id, fiscal_year, reinvestment_rate)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year) DO UPDATE SET reinvestment_rate=excluded.reinvestment_rate
            """, (company_id, year, val)
        )
    conn.commit()


def get_annual_reinvestment_rate_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
    """Retrieve stored Reinvestment Rate series for a company (year, reinvestment_rate)."""
    return _read_df(
        """
        SELECT fiscal_year AS year, reinvestment_rate
        FROM reinvestment_rate_annual
        WHERE company_id = ?
        ORDER BY year
        """,
        conn,
        params=(company_id,),
    )



def upsert_annual_rd_spend_rate(
    conn: sqlite3.Connection, company_id: int, year_to_rate: Dict[int, float]
) -> None:
    """Store annual R&D Spend Rate values for a company."""
    session = getattr(conn, "session", None)
    if session is not None:
        for year, val in year_to_rate.items():
            existing = session.query(RdSpendRateAnnual).filter(
                RdSpendRateAnnual.company_id == company_id,
                RdSpendRateAnnual.fiscal_year == year,
            ).one_or_none()
            if existing is None:
                session.add(RdSpendRateAnnual(company_id=company_id, fiscal_year=year, rd_spend_rate=val))
            else:
                existing.rd_spend_rate = val
        session.commit()
        return

    cur = conn.cursor()
    for year, val in year_to_rate.items():
        cur.execute(
            """
            INSERT INTO rd_spend_rate_annual(company_id, fiscal_year, rd_spend_rate)
            VALUES(?, ?, ?)
            ON CONFLICT(company_id, fiscal_year) DO UPDATE SET rd_spend_rate=excluded.rd_spend_rate
            """, (company_id, year, val)
        )
    conn.commit()


def get_annual_rd_spend_rate_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
    """Retrieve stored R&D Spend Rate series for a company (year, rd_spend_rate)."""
    return _read_df(
        """
        SELECT fiscal_year AS year, rd_spend_rate
        FROM rd_spend_rate_annual
        WHERE company_id = ?
        ORDER BY year
        """,
        conn,
        params=(company_id,),
    )




def compute_and_store_total_equity_and_roe(conn: sqlite3.Connection, company_id: int) -> None:
    """Compute and store annual Total Equity, Average Equity, and Return on Equity (ROE).

    Total Equity (annual) = Shareholders Equity

    Average Equity (year Y) = 0.5 × (Shareholders Equity(Y−1) + Shareholders Equity(Y))

    ROE (year Y) = Net Income (Y) / Average Equity (Y)

    This uses values already stored in the DB.
    """
    try:
        se_df = get_annual_shareholders_equity_series(conn, company_id)
    except Exception:
        se_df = pd.DataFrame(columns=["year", "shareholders_equity"])

    if se_df is None or se_df.empty:
        return

    se_map: Dict[int, float] = {}
    for _, r in se_df.dropna(subset=["shareholders_equity"]).iterrows():
        try:
            y = int(r["year"])
            v = float(r["shareholders_equity"])
            if np.isnan(v) or np.isinf(v):
                continue
            se_map[y] = v
        except Exception:
            continue

    if not se_map:
        return

    # Total Equity = Shareholders Equity
    year_to_total = {int(y): float(v) for y, v in se_map.items()}

    # Average Equity(y) = 0.5 × (SE(y-1) + SE(y))
    year_to_avg: Dict[int, float] = {}
    for y in sorted(se_map.keys()):
        prev = y - 1
        if prev not in se_map:
            continue
        try:
            avg = 0.5 * (float(se_map[prev]) + float(se_map[y]))
            if np.isnan(avg) or np.isinf(avg):
                continue
            year_to_avg[int(y)] = float(avg)
        except Exception:
            continue

    # ROE(y) = Net Income(y) / Average Equity(y)
    try:
        ni_df = get_annual_net_income_series(conn, company_id)
    except Exception:
        ni_df = pd.DataFrame(columns=["year", "net_income"])

    ni_map: Dict[int, float] = {}
    if ni_df is not None and not ni_df.empty:
        for _, r in ni_df.dropna(subset=["net_income"]).iterrows():
            try:
                y = int(r["year"])
                v = float(r["net_income"])
                if np.isnan(v) or np.isinf(v):
                    continue
                ni_map[y] = v
            except Exception:
                continue

    year_to_roe: Dict[int, float] = {}
    for y, avg_eq in year_to_avg.items():
        if y not in ni_map:
            continue
        try:
            if float(avg_eq) == 0.0:
                continue
            roe = float(ni_map[y]) / float(avg_eq)
            if np.isnan(roe) or np.isinf(roe):
                continue
            year_to_roe[int(y)] = float(roe)
        except Exception:
            continue

    upsert_annual_total_equity(conn, company_id, year_to_total)
    if year_to_avg:
        upsert_annual_average_equity(conn, company_id, year_to_avg)
    if year_to_roe:
        upsert_annual_roe(conn, company_id, year_to_roe)



def compute_and_store_debt_equity(conn: sqlite3.Connection, company_id: int) -> None:
    """Compute and store annual Debt/Equity ratios for a company.

    Debt/Equity is defined as:
        Total Debt / Market Capitalization

    This uses values already stored in the DB.
    """
    try:
        debt_df = get_annual_total_debt_series(conn, company_id)
    except Exception:
        debt_df = pd.DataFrame(columns=["year", "total_debt"])

    try:
        mc_df = get_annual_market_capitalization_series(conn, company_id)
    except Exception:
        mc_df = pd.DataFrame(columns=["year", "market_capitalization"])

    if debt_df is None or debt_df.empty or mc_df is None or mc_df.empty:
        return

    # Map year -> values
    year_to_debt = {}
    for _, r in debt_df.iterrows():
        try:
            year_to_debt[int(r["year"])] = float(r["total_debt"])
        except Exception:
            continue

    year_to_mc = {}
    for _, r in mc_df.iterrows():
        try:
            year_to_mc[int(r["year"])] = float(r["market_capitalization"])
        except Exception:
            continue

    common_years = sorted(set(year_to_debt.keys()) & set(year_to_mc.keys()))
    if not common_years:
        return

    year_to_de: Dict[int, float] = {}
    for y in common_years:
        mc = year_to_mc.get(int(y))
        debt = year_to_debt.get(int(y))
        if mc is None or debt is None:
            continue
        try:
            mc_f = float(mc)
            debt_f = float(debt)
        except Exception:
            continue
        if mc_f == 0.0:
            continue

        de = debt_f / mc_f
        if np.isfinite(de):
            year_to_de[int(y)] = float(de)

    if year_to_de:
        upsert_annual_debt_equity(conn, company_id, year_to_de)

def _tax_rate_to_decimal(rate: Optional[float]) -> Optional[float]:
    if rate is None:
        return None
    try:
        r = float(rate)
    except Exception:
        return None
    # Stored as percentage (e.g., 21.0), but allow decimal (e.g., 0.21)
    return (r / 100.0) if r > 1.0 else r


def _get_effective_marginal_tax_rate_usa(conn: sqlite3.Connection) -> Optional[float]:
    """Return the USA effective marginal corporate tax rate.

    Values in the DB are stored as percentages (e.g., 21.0 for 21%).
    """
    try:
        cur = conn.cursor()
        for key in ("USA", "US", "United States"):
            row = cur.execute(
                "SELECT effective_rate FROM marginal_corporate_tax_rates WHERE country = ?",
                (key,),
            ).fetchone()
            if row is not None and row[0] is not None:
                return float(row[0])
    except Exception:
        return None
    return None


def _get_effective_marginal_tax_rate_for_country(conn: sqlite3.Connection, country: Optional[str]) -> Optional[float]:
    """Return the effective marginal corporate tax rate for the given country.

    Values in the DB are stored as percentages (e.g., 21.0 for 21%).
    Falls back to USA if the requested country is not present.
    """
    try:
        canonical = _canonicalize_country(country)
        keys: List[str]
        if canonical == "USA":
            keys = ["USA", "US", "United States", "United States of America"]
        elif canonical == "India":
            keys = ["India", "IN"]
        elif canonical == "China":
            keys = ["China", "CN", "PRC", "People's Republic of China", "Peoples Republic of China"]
        elif canonical == "Japan":
            keys = ["Japan", "JP"]
        elif canonical == "UK":
            keys = ["UK", "United Kingdom", "Great Britain", "Britain", "GB"]
        elif canonical == "UAE":
            keys = ["UAE", "United Arab Emirates", "AE"]
        else:
            keys = [canonical]

        cur = conn.cursor()
        for key in keys:
            row = cur.execute(
                "SELECT effective_rate FROM marginal_corporate_tax_rates WHERE country = ?",
                (key,),
            ).fetchone()
            if row is not None and row[0] is not None:
                return float(row[0])

        # fallback
        return _get_effective_marginal_tax_rate_usa(conn)
    except Exception:
        return None


def _get_company_unlevered_beta_from_bucket(conn: sqlite3.Connection, company_id: int) -> Optional[float]:
    """Unlevered beta for the *industry bucket* the company belongs to.

    We infer the bucket by matching the company's bucket membership (company_groups.name)
    to rows in industry_betas.user_industry_bucket. If a company belongs to multiple
    buckets with defined industry betas, we average across them.
    """
    try:
        row = conn.execute(
            """
            SELECT AVG(ib.unlevered_beta) AS unlevered_beta
            FROM company_group_members m
            JOIN company_groups g ON g.id = m.group_id
            JOIN industry_betas ib ON ib.user_industry_bucket = g.name
            WHERE m.company_id = ?
            """,
            (company_id,),
        ).fetchone()
        if row is None or row[0] is None:
            return None
        return float(row[0])
    except Exception:
        return None


def compute_and_store_levered_beta(conn: sqlite3.Connection, company_id: int) -> None:
    """Compute and store annual Levered Beta for a company.

    Formula:
        Levered Beta(y) =
            Unlevered Beta(bucket) * (1 + (1 - TaxRate(country)) * Debt/Equity(y))
    """
    ub = _get_company_unlevered_beta_from_bucket(conn, company_id)
    if ub is None or not np.isfinite(ub):
        return

    country = _get_company_country(conn, company_id)
    tax_pct = _get_effective_marginal_tax_rate_for_country(conn, country)
    tax = _tax_rate_to_decimal(tax_pct)
    if tax is None or not np.isfinite(tax):
        return

    try:
        de_df = get_annual_debt_equity_series(conn, company_id)
    except Exception:
        return

    if de_df is None or de_df.empty:
        return

    year_to_beta: Dict[int, float] = {}
    for _, r in de_df.iterrows():
        try:
            y = int(r["year"])
            de = float(r["debt_equity"])
        except Exception:
            continue
        if not np.isfinite(de):
            continue

        beta = float(ub) * (1.0 + (1.0 - float(tax)) * float(de))
        if np.isfinite(beta):
            year_to_beta[int(y)] = float(beta)

    if year_to_beta:
        upsert_annual_levered_beta(conn, company_id, year_to_beta)


def compute_and_store_cost_of_equity(conn: sqlite3.Connection, company_id: int) -> None:
    """Compute and store annual Cost of Equity for a company.

    Formula (percentage points):
        Cost of Equity(y) =
            Risk Free Rate(country, y)
            + Levered Beta(y) * US Implied Equity Risk Premium(y)
            + Country Risk Premium(country, y)

    Notes:
      - Risk Free Rate is pulled from risk_free_rates by the company's stored country:
          USA -> usa_rf, India -> india_rf, China -> china_rf, Japan -> japan_rf
        (UK/UAE fall back to usa_rf since no uk_rf/uae_rf columns exist today.)
      - US implied equity risk premium is pulled from implied_equity_risk_premium_usa.
      - Country risk premiums are pulled from Country_Risk_Premium (columns: us, india, china, japan, uk, uae).
      - Values are stored as percentage points (e.g., 6.25 means 6.25%).
    """
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO cost_of_equity_annual(company_id, fiscal_year, cost_of_equity)
            SELECT
                lb.company_id,
                lb.fiscal_year,
                (
                    /* Country-specific risk-free rate (UK/UAE fallback to USA) */
                    CASE
                        WHEN UPPER(c.country) IN ('INDIA','IN') THEN r.india_rf
                        WHEN UPPER(c.country) IN ('CHINA','CN','PRC') THEN r.china_rf
                        WHEN UPPER(c.country) IN ('JAPAN','JP') THEN r.japan_rf
                        ELSE r.usa_rf
                    END
                    + (lb.levered_beta * erp.implied_erp)
                    + COALESCE(
                        CASE
                            WHEN UPPER(c.country) IN ('INDIA','IN') THEN crp.india
                            WHEN UPPER(c.country) IN ('CHINA','CN','PRC') THEN crp.china
                            WHEN UPPER(c.country) IN ('JAPAN','JP') THEN crp.japan
                            WHEN UPPER(c.country) IN ('UK','UNITED KINGDOM','GREAT BRITAIN','BRITAIN','GB') THEN crp.uk
                            WHEN UPPER(c.country) IN ('UAE','UNITED ARAB EMIRATES','AE') THEN crp.uae
                            ELSE crp.us
                        END,
                        0.0
                    )
                ) AS cost_of_equity
            FROM levered_beta_annual lb
            JOIN companies c
                ON c.id = lb.company_id
            JOIN implied_equity_risk_premium_usa erp
                ON erp.year = lb.fiscal_year
            JOIN risk_free_rates r
                ON r.year = lb.fiscal_year
            LEFT JOIN Country_Risk_Premium crp
                ON crp.year = lb.fiscal_year
            WHERE lb.company_id = ?
            ON CONFLICT (company_id, fiscal_year) DO UPDATE SET cost_of_equity=excluded.cost_of_equity
            """,
            (company_id,),
        )
        conn.commit()
    except Exception:
        # Derived metric; do not break the app if inputs are missing
        return


def refresh_cost_of_equity_all_companies(conn: sqlite3.Connection) -> None:
    """Refresh/backfill Cost of Equity for all companies (safe to call repeatedly)."""
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO cost_of_equity_annual(company_id, fiscal_year, cost_of_equity)
            SELECT
                lb.company_id,
                lb.fiscal_year,
                (
                    CASE
                        WHEN UPPER(c.country) IN ('INDIA','IN') THEN r.india_rf
                        WHEN UPPER(c.country) IN ('CHINA','CN','PRC') THEN r.china_rf
                        WHEN UPPER(c.country) IN ('JAPAN','JP') THEN r.japan_rf
                        ELSE r.usa_rf
                    END
                    + (lb.levered_beta * erp.implied_erp)
                    + COALESCE(
                        CASE
                            WHEN UPPER(c.country) IN ('INDIA','IN') THEN crp.india
                            WHEN UPPER(c.country) IN ('CHINA','CN','PRC') THEN crp.china
                            WHEN UPPER(c.country) IN ('JAPAN','JP') THEN crp.japan
                            WHEN UPPER(c.country) IN ('UK','UNITED KINGDOM','GREAT BRITAIN','BRITAIN','GB') THEN crp.uk
                            WHEN UPPER(c.country) IN ('UAE','UNITED ARAB EMIRATES','AE') THEN crp.uae
                            ELSE crp.us
                        END,
                        0.0
                    )
                ) AS cost_of_equity
            FROM levered_beta_annual lb
            JOIN companies c
                ON c.id = lb.company_id
            JOIN implied_equity_risk_premium_usa erp
                ON erp.year = lb.fiscal_year
            JOIN risk_free_rates r
                ON r.year = lb.fiscal_year
            LEFT JOIN Country_Risk_Premium crp
                ON crp.year = lb.fiscal_year
            ON CONFLICT (company_id, fiscal_year) DO UPDATE SET cost_of_equity=excluded.cost_of_equity
            """
        )
        conn.commit()
    except Exception:
        return


def compute_and_store_default_spread(conn: sqlite3.Connection, company_id: int) -> None:
    """Compute and store annual Default Spread for a company, from Interest Coverage Ratio.

    Rule (percent points):
      - interest_coverage_ratio <= 0.499999  => 20.00
      - <= 0.799999                         => 17.00
      - <= 1.249999                         => 11.78
      - <= 1.499999                         => 8.51
      - <= 1.999999                         => 5.24
      - <= 2.499999                         => 3.61
      - <= 2.999999                         => 3.14
      - <= 3.499999                         => 2.21
      - <= 3.9999999                        => 1.74
      - <= 4.499999                         => 1.47
      - <= 5.999999                         => 1.21
      - <= 7.499999                         => 1.07
      - <= 9.499999                         => 0.92
      - <= 12.499999                        => 0.70
      - else                                => 0.59
    """
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO default_spread_annual(company_id, fiscal_year, default_spread)
            SELECT
                ic.company_id,
                ic.fiscal_year,
                CASE
                    WHEN ic.interest_coverage_ratio <= 0.499999 THEN 20.00
                    WHEN ic.interest_coverage_ratio <= 0.799999 THEN 17.00
                    WHEN ic.interest_coverage_ratio <= 1.249999 THEN 11.78
                    WHEN ic.interest_coverage_ratio <= 1.499999 THEN 8.51
                    WHEN ic.interest_coverage_ratio <= 1.999999 THEN 5.24
                    WHEN ic.interest_coverage_ratio <= 2.499999 THEN 3.61
                    WHEN ic.interest_coverage_ratio <= 2.999999 THEN 3.14
                    WHEN ic.interest_coverage_ratio <= 3.499999 THEN 2.21
                    WHEN ic.interest_coverage_ratio <= 3.9999999 THEN 1.74
                    WHEN ic.interest_coverage_ratio <= 4.499999 THEN 1.47
                    WHEN ic.interest_coverage_ratio <= 5.999999 THEN 1.21
                    WHEN ic.interest_coverage_ratio <= 7.499999 THEN 1.07
                    WHEN ic.interest_coverage_ratio <= 9.499999 THEN 0.92
                    WHEN ic.interest_coverage_ratio <= 12.499999 THEN 0.70
                    ELSE 0.59
                END AS default_spread
            FROM interest_coverage_annual ic
            WHERE ic.company_id = ?
            ON CONFLICT (company_id, fiscal_year) DO UPDATE SET default_spread=excluded.default_spread
            """,
            (company_id,),
        )
        conn.commit()
    except Exception:
        return


def refresh_default_spread_all_companies(conn: sqlite3.Connection) -> None:
    """Refresh/backfill Default Spread for all companies (safe to call repeatedly)."""
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO default_spread_annual(company_id, fiscal_year, default_spread)
            SELECT
                ic.company_id,
                ic.fiscal_year,
                CASE
                    WHEN ic.interest_coverage_ratio <= 0.499999 THEN 20.00
                    WHEN ic.interest_coverage_ratio <= 0.799999 THEN 17.00
                    WHEN ic.interest_coverage_ratio <= 1.249999 THEN 11.78
                    WHEN ic.interest_coverage_ratio <= 1.499999 THEN 8.51
                    WHEN ic.interest_coverage_ratio <= 1.999999 THEN 5.24
                    WHEN ic.interest_coverage_ratio <= 2.499999 THEN 3.61
                    WHEN ic.interest_coverage_ratio <= 2.999999 THEN 3.14
                    WHEN ic.interest_coverage_ratio <= 3.499999 THEN 2.21
                    WHEN ic.interest_coverage_ratio <= 3.9999999 THEN 1.74
                    WHEN ic.interest_coverage_ratio <= 4.499999 THEN 1.47
                    WHEN ic.interest_coverage_ratio <= 5.999999 THEN 1.21
                    WHEN ic.interest_coverage_ratio <= 7.499999 THEN 1.07
                    WHEN ic.interest_coverage_ratio <= 9.499999 THEN 0.92
                    WHEN ic.interest_coverage_ratio <= 12.499999 THEN 0.70
                    ELSE 0.59
                END AS default_spread
            FROM interest_coverage_annual ic
            ON CONFLICT (company_id, fiscal_year) DO UPDATE SET default_spread=excluded.default_spread
            """
        )
        conn.commit()
    except Exception:
        return


def compute_and_store_pre_tax_cost_of_debt(conn: sqlite3.Connection, company_id: int) -> None:
    """Compute and store annual Pre-Tax Cost of Debt for a company.

    Formula (percentage points):
        Pre-Tax Cost of Debt(y) = Risk Free Rate(country, y) + Default Spread(y)

    Notes:
      - Risk Free Rate is pulled from risk_free_rates by the company's stored country:
          USA -> usa_rf, India -> india_rf, China -> china_rf, Japan -> japan_rf
        (UK/UAE fall back to usa_rf since no uk_rf/uae_rf columns exist today.)
      - Default Spread is pulled from default_spread_annual for the same year.
      - Values are stored as percentage points (e.g., 4.10 means 4.10%).
    """
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO pre_tax_cost_of_debt_annual(company_id, fiscal_year, pre_tax_cost_of_debt)
            SELECT
                ds.company_id,
                ds.fiscal_year,
                (
                    CASE
                        WHEN UPPER(c.country) IN ('INDIA','IN') THEN r.india_rf
                        WHEN UPPER(c.country) IN ('CHINA','CN','PRC') THEN r.china_rf
                        WHEN UPPER(c.country) IN ('JAPAN','JP') THEN r.japan_rf
                        ELSE r.usa_rf
                    END
                    + ds.default_spread
                ) AS pre_tax_cost_of_debt
            FROM default_spread_annual ds
            JOIN companies c
                ON c.id = ds.company_id
            JOIN risk_free_rates r
                ON r.year = ds.fiscal_year
            WHERE ds.company_id = ?
            ON CONFLICT (company_id, fiscal_year) DO UPDATE SET pre_tax_cost_of_debt=excluded.pre_tax_cost_of_debt
            """,
            (company_id,),
        )
        conn.commit()
    except Exception:
        return


def refresh_pre_tax_cost_of_debt_all_companies(conn: sqlite3.Connection) -> None:
    """Refresh/backfill Pre-Tax Cost of Debt for all companies (safe to call repeatedly)."""
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO pre_tax_cost_of_debt_annual(company_id, fiscal_year, pre_tax_cost_of_debt)
            SELECT
                ds.company_id,
                ds.fiscal_year,
                (
                    CASE
                        WHEN UPPER(c.country) IN ('INDIA','IN') THEN r.india_rf
                        WHEN UPPER(c.country) IN ('CHINA','CN','PRC') THEN r.china_rf
                        WHEN UPPER(c.country) IN ('JAPAN','JP') THEN r.japan_rf
                        ELSE r.usa_rf
                    END
                    + ds.default_spread
                ) AS pre_tax_cost_of_debt
            FROM default_spread_annual ds
            JOIN companies c
                ON c.id = ds.company_id
            JOIN risk_free_rates r
                ON r.year = ds.fiscal_year
            ON CONFLICT (company_id, fiscal_year) DO UPDATE SET pre_tax_cost_of_debt=excluded.pre_tax_cost_of_debt
            """
        )
        conn.commit()
    except Exception:
        return


def get_annual_pre_tax_cost_of_debt_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
    """Retrieve the stored Pre-Tax Cost of Debt series for a given company."""
    return _read_df(
        """
        SELECT fiscal_year AS year, pre_tax_cost_of_debt
        FROM pre_tax_cost_of_debt_annual
        WHERE company_id = ?
        ORDER BY year
        """,
        conn,
        params=(company_id,),
    )



def get_annual_cost_of_equity_series(conn: sqlite3.Connection, company_id: int) -> pd.DataFrame:
    return _read_df(
        """
        SELECT fiscal_year AS year, cost_of_equity
        FROM cost_of_equity_annual
        WHERE company_id = ?
        ORDER BY year
        """,
        conn,
        params=(company_id,),
    )


def compute_and_store_rd_spend_rate(conn: sqlite3.Connection, company_id: int) -> None:
    """Compute and store annual R&D Spend Rate using data already in the DB.

    R&D Spend Rate % = Research & Development Expense / NOPAT.
    """
    try:
        nopat_df = get_annual_nopat_series(conn, company_id)
    except Exception:
        return

    try:
        rd_map = get_annual_research_and_development_expense_series(conn, company_id)
    except Exception:
        rd_map = {}

    if nopat_df.empty or not rd_map:
        return

    nopat_map = {
        int(r["year"]): float(r["nopat"])
        for _, r in nopat_df.dropna(subset=["nopat"]).iterrows()
    }

    years = sorted(set(nopat_map.keys()) & set(int(y) for y in rd_map.keys()))
    if not years:
        return

    year_to_rate: Dict[int, float] = {}
    for y in years:
        try:
            denom = float(nopat_map[y])
            if denom == 0.0:
                continue
            rd_val = float(rd_map[y])
            year_to_rate[int(y)] = rd_val / denom
        except Exception:
            continue

    upsert_annual_rd_spend_rate(conn, company_id, year_to_rate)


def compute_and_store_fcff_and_reinvestment_rate(conn: sqlite3.Connection, company_id: int) -> None:
    """Compute and store annual FCFF and Reinvestment Rate using data already in the DB.

    FCFF (Damodaran-style): FCFF = NOPAT + D&A - CapEx - ΔNCWC
    Net CapEx = CapEx - D&A
    Reinvestment Rate = (Net CapEx + ΔNCWC) / NOPAT
    """
    try:
        nopat_df = get_annual_nopat_series(conn, company_id)
    except Exception:
        nopat_df = pd.DataFrame(columns=["year", "nopat"])

    try:
        ncwc_df = get_annual_non_cash_working_capital_series(conn, company_id)
    except Exception:
        ncwc_df = pd.DataFrame(columns=["year", "non_cash_working_capital"])

    try:
        capex_map = get_annual_capital_expenditures_series(conn, company_id)
    except Exception:
        capex_map = {}

    try:
        da_map = get_annual_depreciation_amortization_series(conn, company_id)
    except Exception:
        da_map = {}

    if nopat_df.empty or ncwc_df.empty or not capex_map or not da_map:
        return

    nopat_map = {int(r["year"]): float(r["nopat"]) for _, r in nopat_df.dropna(subset=["nopat"]).iterrows()}
    ncwc_map = {int(r["year"]): float(r["non_cash_working_capital"]) for _, r in ncwc_df.dropna(subset=["non_cash_working_capital"]).iterrows()}

    years = sorted(set(nopat_map.keys()) & set(ncwc_map.keys()) & set(capex_map.keys()) & set(da_map.keys()))
    if not years:
        return

    # For ΔNCWC, use the prior available NCWC year (not necessarily year-1).
    ncwc_years_sorted = sorted(ncwc_map.keys())

    def prev_ncwc_year(y: int) -> Optional[int]:
        prevs = [py for py in ncwc_years_sorted if py < y]
        return prevs[-1] if prevs else None

    year_to_fcff: Dict[int, float] = {}
    year_to_rr: Dict[int, float] = {}

    for y in years:
        nopat = nopat_map.get(y)
        capex = capex_map.get(y)
        da = da_map.get(y)
        ncwc = ncwc_map.get(y)
        if nopat is None or capex is None or da is None or ncwc is None:
            continue

        py = prev_ncwc_year(y)
        if py is None or py not in ncwc_map:
            delta_ncwc = 0.0
        else:
            delta_ncwc = float(ncwc) - float(ncwc_map[py])

        # Net CapEx = CapEx - D&A  (CapEx is stored as positive outflow)
        net_capex = float(capex) - float(da)

        # FCFF = NOPAT - Net CapEx - ΔNCWC
        fcff = float(nopat) - float(net_capex) - float(delta_ncwc)
        year_to_fcff[int(y)] = float(fcff)

        # Reinvestment Rate = (Net CapEx + ΔNCWC) / NOPAT
        denom = float(nopat)
        if denom != 0.0:
            reinvestment = float(net_capex) + float(delta_ncwc)
            year_to_rr[int(y)] = float(reinvestment) / denom

    upsert_annual_fcff(conn, company_id, year_to_fcff)
    upsert_annual_reinvestment_rate(conn, company_id, year_to_rr)


def compute_and_store_fcfe(conn: sqlite3.Connection, company_id: int) -> None:
    """Compute and store annual Free Cash Flow to Equity (FCFE) using data already in the DB.

    FCFE (Damodaran-style):
        FCFE = Net Income + Depreciation & Amortization - CapEx - ΔNCWC + Net Debt Issued/Paid

    Notes:
    - CapEx is stored as a positive outflow in this app's DB.
    - ΔNCWC uses the prior available NCWC year (not necessarily year-1).
    - If Net Debt Issued/Paid is missing for a year, it is treated as 0 for that year.
    """
    try:
        ni_df = get_annual_net_income_series(conn, company_id)
    except Exception:
        ni_df = pd.DataFrame(columns=["year", "net_income"])

    try:
        ncwc_df = get_annual_non_cash_working_capital_series(conn, company_id)
    except Exception:
        ncwc_df = pd.DataFrame(columns=["year", "non_cash_working_capital"])

    try:
        capex_map = get_annual_capital_expenditures_series(conn, company_id)
    except Exception:
        capex_map = {}

    try:
        da_map = get_annual_depreciation_amortization_series(conn, company_id)
    except Exception:
        da_map = {}

    # Preferred input for Net Debt Issued/Paid is the Cash Flow line item "Debt Issued / Paid".
    # Some templates (e.g., certain India company exports) don't include this row; in that case we fall
    # back to a balance-sheet approximation:
    #   Net Debt Issued/Paid ≈ Total Debt(current year) - Total Debt(previous year)
    try:
        net_debt_map = get_annual_net_debt_issued_paid_series(conn, company_id)
    except Exception:
        net_debt_map = {}

    total_debt_map: Dict[int, float] = {}
    if not net_debt_map:
        try:
            td_df = get_annual_total_debt_series(conn, company_id)
            if not td_df.empty:
                total_debt_map = {
                    int(r["year"]): float(r["total_debt"]) for _, r in td_df.dropna(subset=["total_debt"]).iterrows()
                }
        except Exception:
            total_debt_map = {}

    if ni_df.empty or ncwc_df.empty or not capex_map or not da_map:
        return

    ni_map = {int(r["year"]): float(r["net_income"]) for _, r in ni_df.dropna(subset=["net_income"]).iterrows()}
    ncwc_map = {int(r["year"]): float(r["non_cash_working_capital"]) for _, r in ncwc_df.dropna(subset=["non_cash_working_capital"]).iterrows()}

    years = sorted(set(ni_map.keys()) & set(ncwc_map.keys()) & set(capex_map.keys()) & set(da_map.keys()))
    if not years:
        return

    ncwc_years_sorted = sorted(ncwc_map.keys())

    def prev_ncwc_year(y: int) -> Optional[int]:
        prevs = [py for py in ncwc_years_sorted if py < y]
        return prevs[-1] if prevs else None

    year_to_fcfe: Dict[int, float] = {}

    total_debt_years_sorted = sorted(total_debt_map.keys())

    def prev_total_debt_year(y: int) -> Optional[int]:
        prevs = [py for py in total_debt_years_sorted if py < y]
        return prevs[-1] if prevs else None

    for y in years:
        ni = ni_map.get(y)
        capex = capex_map.get(y)
        da = da_map.get(y)
        ncwc = ncwc_map.get(y)
        if ni is None or capex is None or da is None or ncwc is None:
            continue

        py = prev_ncwc_year(y)
        if py is None or py not in ncwc_map:
            delta_ncwc = 0.0
        else:
            delta_ncwc = float(ncwc) - float(ncwc_map[py])

        # Net Debt Issued/Paid:
        # - Prefer the direct cash-flow series when present.
        # - If the cash-flow series is absent entirely, approximate using the change in Total Debt.
        if net_debt_map:
            net_debt = float(net_debt_map.get(y, 0.0))
        else:
            py_td = prev_total_debt_year(int(y))
            if py_td is None or int(y) not in total_debt_map or py_td not in total_debt_map:
                net_debt = 0.0
            else:
                net_debt = float(total_debt_map[int(y)]) - float(total_debt_map[int(py_td)])

        # FCFE = Net Income + D&A - CapEx - ΔNCWC + Net Debt Issued/Paid
        fcfe = float(ni) + float(da) - float(capex) - float(delta_ncwc) + float(net_debt)
        year_to_fcfe[int(y)] = float(fcfe)

    upsert_annual_fcfe(conn, company_id, year_to_fcfe)
