#!/usr/bin/env python3
"""Apply the Industrials USA workbook taxonomy to the shared Research database."""

from __future__ import annotations

import argparse
import math
from pathlib import Path
import sys
from typing import Dict, Iterable, List, Tuple

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db_session import DbCompat, SessionLocal, get_engine


REQUIRED_INDUSTRY_COLUMNS = {
    "Industry Bucket",
    "Mapped Sector",
    "Unlevered Beta",
    "Cash-Adjusted Beta",
    "Beta Source Date",
}
REQUIRED_SUBCATEGORY_COLUMNS = {"Category", "Sub-Category", "Companies", "Industry_Buckets"}
REQUIRED_UNIVERSE_COLUMNS = {"Company", "Ticker", "Industry Bucket", "Category", "Sub-Category"}


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_ticker(value: object) -> str:
    return _normalize_text(value).upper()


def _require_columns(frame: pd.DataFrame, required: set[str], sheet: str) -> None:
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"{sheet} is missing required columns: {', '.join(missing)}")


def load_and_validate_workbook(path: Path) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    industry = pd.read_excel(path, sheet_name="Industry Buckets")
    subcategories = pd.read_excel(path, sheet_name="Sub-Categories")
    universe = pd.read_excel(path, sheet_name="Universe")
    _require_columns(industry, REQUIRED_INDUSTRY_COLUMNS, "Industry Buckets")
    _require_columns(subcategories, REQUIRED_SUBCATEGORY_COLUMNS, "Sub-Categories")
    _require_columns(universe, REQUIRED_UNIVERSE_COLUMNS, "Universe")

    for column in ("Industry Bucket", "Mapped Sector"):
        industry[column] = industry[column].map(_normalize_text)
    industry["Unlevered Beta"] = pd.to_numeric(industry["Unlevered Beta"], errors="coerce")
    industry["Cash-Adjusted Beta"] = pd.to_numeric(industry["Cash-Adjusted Beta"], errors="coerce")
    industry["Beta Source Date"] = pd.to_datetime(industry["Beta Source Date"], errors="coerce")

    for column in ("Category", "Sub-Category"):
        subcategories[column] = subcategories[column].map(_normalize_text)
    subcategories["Companies"] = pd.to_numeric(subcategories["Companies"], errors="coerce")
    subcategories["Industry_Buckets"] = pd.to_numeric(
        subcategories["Industry_Buckets"], errors="coerce"
    )

    for column in ("Company", "Industry Bucket", "Category", "Sub-Category"):
        universe[column] = universe[column].map(_normalize_text)
    universe["Ticker"] = universe["Ticker"].map(_normalize_ticker)

    if industry.empty or subcategories.empty or universe.empty:
        raise ValueError("The three controlling workbook sheets must not be empty.")
    if industry[["Industry Bucket", "Mapped Sector"]].eq("").any().any():
        raise ValueError("Industry Buckets contains blank bucket or mapped-sector values.")
    if industry["Industry Bucket"].duplicated().any():
        duplicates = sorted(industry.loc[industry["Industry Bucket"].duplicated(False), "Industry Bucket"].unique())
        raise ValueError(f"Industry bucket names must be unique: {duplicates}")
    if industry[["Unlevered Beta", "Cash-Adjusted Beta", "Beta Source Date"]].isna().any().any():
        raise ValueError("Industry Buckets contains invalid beta or source-date values.")
    if not industry["Unlevered Beta"].map(math.isfinite).all() or not industry[
        "Cash-Adjusted Beta"
    ].map(math.isfinite).all():
        raise ValueError("Industry Buckets contains non-finite beta values.")
    if (industry[["Unlevered Beta", "Cash-Adjusted Beta"]] <= 0).any().any():
        raise ValueError("Industry beta values must be positive.")

    if subcategories[["Category", "Sub-Category"]].eq("").any().any():
        raise ValueError("Sub-Categories contains blank category values.")
    if subcategories[["Category", "Sub-Category"]].duplicated().any():
        raise ValueError("Sub-Categories contains duplicate category/sub-category pairs.")
    if universe[["Company", "Ticker", "Industry Bucket", "Category", "Sub-Category"]].eq("").any().any():
        raise ValueError("Universe contains blank required values.")
    if universe["Ticker"].duplicated().any():
        duplicates = sorted(universe.loc[universe["Ticker"].duplicated(False), "Ticker"].unique())
        raise ValueError(f"Universe tickers must be unique: {duplicates}")

    valid_buckets = set(industry["Industry Bucket"])
    unknown_buckets = sorted(set(universe["Industry Bucket"]) - valid_buckets)
    if unknown_buckets:
        raise ValueError(f"Universe references undefined industry buckets: {unknown_buckets}")
    valid_categories = set(zip(subcategories["Category"], subcategories["Sub-Category"]))
    unknown_categories = sorted(
        set(zip(universe["Category"], universe["Sub-Category"])) - valid_categories
    )
    if unknown_categories:
        raise ValueError(f"Universe references undefined category pairs: {unknown_categories}")

    universe_counts = universe.groupby(["Category", "Sub-Category"]).size()
    universe_bucket_counts = universe.groupby(["Category", "Sub-Category"])[
        "Industry Bucket"
    ].nunique()
    for row in subcategories.itertuples(index=False):
        key = (row.Category, getattr(row, "_1"))
        expected_companies = int(row.Companies)
        expected_buckets = int(row.Industry_Buckets)
        if int(universe_counts.get(key, 0)) != expected_companies:
            raise ValueError(f"Company count does not reconcile for {key}.")
        if int(universe_bucket_counts.get(key, 0)) != expected_buckets:
            raise ValueError(f"Industry-bucket count does not reconcile for {key}.")

    return industry, subcategories, universe


def _read_df(conn: DbCompat, sql: str, params: Iterable[object] = ()) -> pd.DataFrame:
    from core_backend import read_df

    return read_df(sql, conn, params=tuple(params))


def resolve_live_companies(conn: DbCompat, universe: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    companies = _read_df(
        conn,
        "SELECT id, name, ticker, country FROM companies WHERE UPPER(country) = 'USA'",
    )
    companies["Ticker"] = companies["ticker"].map(_normalize_ticker)
    companies = companies[companies["Ticker"].isin(set(universe["Ticker"]))].copy()
    duplicate_usa = companies[companies["Ticker"].duplicated(False)]
    relevant_duplicates = sorted(set(duplicate_usa["Ticker"]) & set(universe["Ticker"]))
    if relevant_duplicates:
        raise ValueError(f"Ambiguous USA company tickers in the database: {relevant_duplicates}")

    matched = universe.merge(
        companies[["id", "name", "Ticker"]],
        on="Ticker",
        how="left",
        validate="one_to_one",
    )
    matched_rows = matched[matched["id"].notna()].copy()
    matched_rows["id"] = matched_rows["id"].astype(int)
    unmatched_rows = matched[matched["id"].isna()][
        ["Company", "Ticker", "Industry Bucket", "Category", "Sub-Category"]
    ].copy()
    return matched_rows, unmatched_rows


def _values_clause(count: int) -> str:
    if count <= 0:
        raise ValueError("At least one affected company is required.")
    return ", ".join("(?)" for _ in range(count))


def refresh_valuation_for_companies(conn: DbCompat, company_ids: List[int]) -> Dict[str, int]:
    ids = sorted({int(company_id) for company_id in company_ids})
    affected_values = _values_clause(len(ids))
    parameters = tuple(ids)
    row_counts: Dict[str, int] = {}

    levered = conn.execute(
        f"""
        WITH affected(company_id) AS (VALUES {affected_values}),
        company_beta AS (
            SELECT affected.company_id, AVG(ib.unlevered_beta) AS unlevered_beta
            FROM affected
            JOIN company_group_members membership
                ON membership.company_id = affected.company_id
            JOIN company_groups groups
                ON groups.id = membership.group_id
            JOIN industry_betas ib
                ON ib.user_industry_bucket = groups.name
            GROUP BY affected.company_id
        ),
        company_tax AS (
            SELECT
                affected.company_id,
                CASE
                    WHEN ABS(COALESCE(tax.effective_rate, usa_tax.effective_rate, 0.0)) > 1.0
                    THEN COALESCE(tax.effective_rate, usa_tax.effective_rate, 0.0) / 100.0
                    ELSE COALESCE(tax.effective_rate, usa_tax.effective_rate, 0.0)
                END AS tax_rate
            FROM affected
            JOIN companies company ON company.id = affected.company_id
            LEFT JOIN marginal_corporate_tax_rates tax
                ON UPPER(tax.country) = UPPER(company.country)
            LEFT JOIN marginal_corporate_tax_rates usa_tax
                ON UPPER(usa_tax.country) = 'USA'
        )
        INSERT INTO levered_beta_annual(company_id, fiscal_year, levered_beta)
        SELECT
            debt_equity.company_id,
            debt_equity.fiscal_year,
            company_beta.unlevered_beta
                * (1.0 + (1.0 - company_tax.tax_rate) * debt_equity.debt_equity)
        FROM debt_equity_annual debt_equity
        JOIN company_beta ON company_beta.company_id = debt_equity.company_id
        JOIN company_tax ON company_tax.company_id = debt_equity.company_id
        ON CONFLICT (company_id, fiscal_year) DO UPDATE
        SET levered_beta = excluded.levered_beta
        """,
        parameters,
    )
    row_counts["levered_beta"] = max(0, int(levered.rowcount or 0))

    cost_of_equity = conn.execute(
        f"""
        WITH affected(company_id) AS (VALUES {affected_values})
        INSERT INTO cost_of_equity_annual(company_id, fiscal_year, cost_of_equity)
        SELECT
            lb.company_id,
            lb.fiscal_year,
            risk_free.usa_rf + (lb.levered_beta * erp.implied_erp) + COALESCE(crp.us, 0.0)
        FROM affected
        JOIN levered_beta_annual lb ON lb.company_id = affected.company_id
        JOIN implied_equity_risk_premium_usa erp ON erp.year = lb.fiscal_year
        JOIN risk_free_rates risk_free ON risk_free.year = lb.fiscal_year
        LEFT JOIN Country_Risk_Premium crp ON crp.year = lb.fiscal_year
        ON CONFLICT (company_id, fiscal_year) DO UPDATE
        SET cost_of_equity = excluded.cost_of_equity
        """,
        parameters,
    )
    row_counts["cost_of_equity"] = max(0, int(cost_of_equity.rowcount or 0))

    wacc = conn.execute(
        f"""
        WITH affected(company_id) AS (VALUES {affected_values}),
        usa_tax AS (
            SELECT CASE WHEN ABS(effective_rate) > 1.0 THEN effective_rate / 100.0 ELSE effective_rate END tax_rate
            FROM marginal_corporate_tax_rates
            WHERE UPPER(country) = 'USA'
            LIMIT 1
        )
        INSERT INTO wacc_annual(company_id, fiscal_year, wacc)
        SELECT
            debt.company_id,
            debt.fiscal_year,
            (cost_debt.pre_tax_cost_of_debt
                * (debt.total_debt * (1.0 - COALESCE(usa_tax.tax_rate, 0.0))
                / (debt.total_debt + market_cap.market_capitalization)))
            + (cost_equity.cost_of_equity
                * (market_cap.market_capitalization
                / (debt.total_debt + market_cap.market_capitalization)))
        FROM affected
        JOIN total_debt_annual debt ON debt.company_id = affected.company_id
        JOIN market_capitalization_annual market_cap
            ON market_cap.company_id = debt.company_id
            AND market_cap.fiscal_year = debt.fiscal_year
        JOIN pre_tax_cost_of_debt_annual cost_debt
            ON cost_debt.company_id = debt.company_id
            AND cost_debt.fiscal_year = debt.fiscal_year
        JOIN cost_of_equity_annual cost_equity
            ON cost_equity.company_id = debt.company_id
            AND cost_equity.fiscal_year = debt.fiscal_year
        CROSS JOIN usa_tax
        WHERE (debt.total_debt + market_cap.market_capitalization) != 0.0
        ON CONFLICT (company_id, fiscal_year) DO UPDATE SET wacc = excluded.wacc
        """,
        parameters,
    )
    row_counts["wacc"] = max(0, int(wacc.rowcount or 0))

    spread = conn.execute(
        f"""
        WITH affected(company_id) AS (VALUES {affected_values})
        INSERT INTO roic_wacc_spread_annual(company_id, fiscal_year, spread_pct)
        SELECT roic.company_id, roic.fiscal_year, roic.roic_pct - wacc.wacc
        FROM affected
        JOIN roic_direct_upload_annual roic ON roic.company_id = affected.company_id
        JOIN wacc_annual wacc
            ON wacc.company_id = roic.company_id
            AND wacc.fiscal_year = roic.fiscal_year
        ON CONFLICT (company_id, fiscal_year) DO UPDATE SET spread_pct = excluded.spread_pct
        """,
        parameters,
    )
    row_counts["spread"] = max(0, int(spread.rowcount or 0))
    return row_counts


def apply_update(
    conn: DbCompat,
    industry: pd.DataFrame,
    subcategories: pd.DataFrame,
    matched: pd.DataFrame,
) -> Dict[str, object]:
    company_ids = sorted(matched["id"].astype(int).unique().tolist())
    affected_values = _values_clause(len(company_ids))
    affected_params = tuple(company_ids)
    industry_rows = [
        (
            row["Industry Bucket"],
            row["Mapped Sector"],
            float(row["Unlevered Beta"]),
            float(row["Cash-Adjusted Beta"]),
            row["Beta Source Date"].date().isoformat(),
        )
        for _, row in industry.iterrows()
    ]

    with conn.transaction():
        conn.execute("DELETE FROM industry_betas WHERE user_industry_bucket LIKE 'Industrials : %'")
        conn.executemany(
            """
            INSERT INTO industry_betas(
                user_industry_bucket, mapped_sector, unlevered_beta, cash_adjusted_beta, updated_at
            ) VALUES(?, ?, ?, ?, ?)
            """,
            industry_rows,
        )
        conn.executemany(
            "INSERT INTO company_groups(name) VALUES(?) ON CONFLICT DO NOTHING",
            [(bucket,) for bucket in industry["Industry Bucket"].tolist()],
        )

        removed_groups = conn.execute(
            f"""
            DELETE FROM company_group_members
            WHERE company_id IN (SELECT company_id FROM (VALUES {affected_values}) AS affected(company_id))
              AND group_id IN (SELECT id FROM company_groups WHERE name LIKE 'Industrials : %')
            """,
            affected_params,
        )
        group_ids = {
            str(name): int(group_id)
            for group_id, name in conn.execute(
                "SELECT id, name FROM company_groups WHERE name LIKE 'Industrials : %'"
            ).fetchall()
        }
        membership_rows = [
            (group_ids[row["Industry Bucket"]], int(row["id"]))
            for _, row in matched.iterrows()
        ]
        conn.executemany(
            """
            INSERT INTO company_group_members(group_id, company_id)
            VALUES(?, ?) ON CONFLICT DO NOTHING
            """,
            membership_rows,
        )

        category_name = str(subcategories["Category"].iloc[0])
        if set(subcategories["Category"]) != {category_name}:
            raise ValueError("This scoped updater expects exactly one workbook category.")
        conn.execute(
            "INSERT INTO relative_valuation_categories(name) VALUES(?) ON CONFLICT DO NOTHING",
            (category_name,),
        )
        category_id = int(
            conn.execute(
                "SELECT id FROM relative_valuation_categories WHERE name = ?",
                (category_name,),
            ).fetchone()[0]
        )
        desired_subcategories = subcategories["Sub-Category"].tolist()
        conn.executemany(
            """
            INSERT INTO relative_valuation_subcategories(category_id, name)
            VALUES(?, ?) ON CONFLICT DO NOTHING
            """,
            [(category_id, name) for name in desired_subcategories],
        )
        desired_placeholders = ",".join(["?"] * len(desired_subcategories))
        conn.execute(
            f"""
            DELETE FROM relative_valuation_company_assignments
            WHERE subcategory_id IN (
                SELECT id FROM relative_valuation_subcategories
                WHERE category_id = ? AND name NOT IN ({desired_placeholders})
            )
            """,
            (category_id, *desired_subcategories),
        )
        conn.execute(
            f"""
            DELETE FROM relative_valuation_subcategories
            WHERE category_id = ? AND name NOT IN ({desired_placeholders})
            """,
            (category_id, *desired_subcategories),
        )
        removed_categories = conn.execute(
            f"""
            DELETE FROM relative_valuation_company_assignments
            WHERE company_id IN (SELECT company_id FROM (VALUES {affected_values}) AS affected(company_id))
              AND subcategory_id IN (
                  SELECT id FROM relative_valuation_subcategories WHERE category_id = ?
              )
            """,
            (*affected_params, category_id),
        )
        subcategory_ids = {
            str(name): int(subcategory_id)
            for subcategory_id, name in conn.execute(
                "SELECT id, name FROM relative_valuation_subcategories WHERE category_id = ?",
                (category_id,),
            ).fetchall()
        }
        assignment_rows = [
            (int(row["id"]), subcategory_ids[row["Sub-Category"]])
            for _, row in matched.iterrows()
        ]
        conn.executemany(
            """
            INSERT INTO relative_valuation_company_assignments(company_id, subcategory_id)
            VALUES(?, ?) ON CONFLICT DO NOTHING
            """,
            assignment_rows,
        )

        valuation_rows = refresh_valuation_for_companies(conn, company_ids)

    return {
        "companies": len(company_ids),
        "industry_betas": len(industry_rows),
        "subcategories": len(desired_subcategories),
        "removed_industrials_memberships": max(0, int(removed_groups.rowcount or 0)),
        "removed_industrials_category_assignments": max(0, int(removed_categories.rowcount or 0)),
        "valuation_rows": valuation_rows,
    }


def verify_update(
    conn: DbCompat,
    industry: pd.DataFrame,
    subcategories: pd.DataFrame,
    matched: pd.DataFrame,
) -> Dict[str, int]:
    company_ids = sorted(matched["id"].astype(int).unique().tolist())
    affected_values = _values_clause(len(company_ids))
    affected_params = tuple(company_ids)
    stored_betas = _read_df(
        conn,
        """
        SELECT user_industry_bucket, mapped_sector, unlevered_beta, cash_adjusted_beta
        FROM industry_betas
        WHERE user_industry_bucket LIKE 'Industrials : %'
        ORDER BY user_industry_bucket
        """,
    )
    beta_count = len(stored_betas)
    expected_betas = industry[
        ["Industry Bucket", "Mapped Sector", "Unlevered Beta", "Cash-Adjusted Beta"]
    ].copy()
    expected_betas.columns = stored_betas.columns
    expected_betas = expected_betas.sort_values("user_industry_bucket").reset_index(drop=True)
    stored_betas = stored_betas.reset_index(drop=True)
    if not stored_betas.equals(expected_betas.astype(stored_betas.dtypes.to_dict())):
        raise RuntimeError("Stored Industrials beta rows do not exactly match the workbook.")

    stored_memberships = _read_df(
        conn,
        f"""
        SELECT membership.company_id, groups.name AS industry_bucket
        FROM company_group_members membership
        JOIN company_groups groups ON groups.id = membership.group_id
        WHERE membership.company_id IN (
            SELECT company_id FROM (VALUES {affected_values}) AS affected(company_id)
        )
          AND groups.name LIKE 'Industrials : %'
        """,
        affected_params,
    )
    expected_memberships = {
        (int(row["id"]), str(row["Industry Bucket"])) for _, row in matched.iterrows()
    }
    actual_memberships = {
        (int(row["company_id"]), str(row["industry_bucket"]))
        for _, row in stored_memberships.iterrows()
    }
    if actual_memberships != expected_memberships:
        raise RuntimeError("Stored Industrials company memberships do not match the Universe sheet.")

    category_name = str(subcategories["Category"].iloc[0])
    category_rows = _read_df(
        conn,
        """
        SELECT assignment.company_id, sub.name AS subcategory
        FROM relative_valuation_categories category
        JOIN relative_valuation_subcategories sub ON sub.category_id = category.id
        JOIN relative_valuation_company_assignments assignment ON assignment.subcategory_id = sub.id
        WHERE category.name = ?
        """,
        (category_name,),
    )
    stored_subcategory_count = int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM relative_valuation_subcategories sub
            JOIN relative_valuation_categories category ON category.id = sub.category_id
            WHERE category.name = ?
            """,
            (category_name,),
        ).fetchone()[0]
    )
    expected_assignments = {
        (int(row["id"]), str(row["Sub-Category"])) for _, row in matched.iterrows()
    }
    actual_assignments = {
        (int(row["company_id"]), str(row["subcategory"]))
        for _, row in category_rows.iterrows()
        if int(row["company_id"]) in set(company_ids)
    }
    if actual_assignments != expected_assignments:
        raise RuntimeError("Stored Industrials category assignments do not match the Universe sheet.")

    valuation_counts = conn.execute(
        f"""
        WITH affected(company_id) AS (VALUES {affected_values})
        SELECT
            (SELECT COUNT(*) FROM wacc_annual wacc JOIN affected ON affected.company_id = wacc.company_id),
            (SELECT COUNT(DISTINCT wacc.company_id) FROM wacc_annual wacc JOIN affected ON affected.company_id = wacc.company_id),
            (SELECT COUNT(*) FROM roic_wacc_spread_annual spread JOIN affected ON affected.company_id = spread.company_id),
            (SELECT COUNT(DISTINCT spread.company_id) FROM roic_wacc_spread_annual spread JOIN affected ON affected.company_id = spread.company_id)
        """,
        affected_params,
    ).fetchone()
    if beta_count != len(industry):
        raise RuntimeError(f"Expected {len(industry)} Industrials beta rows, found {beta_count}.")
    if stored_subcategory_count != len(subcategories):
        raise RuntimeError(
            f"Expected {len(subcategories)} Industrials subcategories, found {stored_subcategory_count}."
        )
    return {
        "industry_beta_rows": beta_count,
        "industry_memberships": len(actual_memberships),
        "industry_subcategories": stored_subcategory_count,
        "industry_category_assignments": len(actual_assignments),
        "wacc_rows": int(valuation_counts[0]),
        "wacc_companies": int(valuation_counts[1]),
        "spread_rows": int(valuation_counts[2]),
        "spread_companies": int(valuation_counts[3]),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--workbook",
        type=Path,
        default=Path("Bulk_Upload_Financials/Industrials_USA_Universe_and_Taxonomy_2026-07-16.xlsx"),
    )
    parser.add_argument("--apply", action="store_true", help="Commit the validated update.")
    args = parser.parse_args()

    industry, subcategories, universe = load_and_validate_workbook(args.workbook.resolve())
    conn = DbCompat(SessionLocal())
    try:
        matched, unmatched = resolve_live_companies(conn, universe)
        name_mismatches = int(
            (matched["Company"].str.casefold() != matched["name"].str.casefold()).sum()
        )
        profile = {
            "database": get_engine().dialect.name,
            "universe_rows": len(universe),
            "matched_usa_companies": len(matched),
            "unmatched_companies": len(unmatched),
            "company_name_mismatches": name_mismatches,
            "industry_buckets": len(industry),
            "categories": int(subcategories["Category"].nunique()),
            "subcategories": len(subcategories),
        }
        print("Validated plan:", profile)
        if not unmatched.empty:
            print("Unmatched tickers:", ", ".join(unmatched["Ticker"].tolist()))
        if not args.apply:
            print("Dry run only. Re-run with --apply to commit the update.")
            return 0

        outcome = apply_update(conn, industry, subcategories, matched)
        verification = verify_update(conn, industry, subcategories, matched)
        print("Applied:", outcome)
        print("Verified:", verification)
        return 0
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
