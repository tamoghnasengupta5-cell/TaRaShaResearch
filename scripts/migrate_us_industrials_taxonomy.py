#!/usr/bin/env python3
"""Migrate the US Industrials taxonomy from 25 legacy buckets to 12 buckets.

The command is dry-run only unless ``--apply`` is supplied.  SQLite databases
are copied to a timestamped backup before an apply.  PostgreSQL applies require
``--confirm-backup`` because the database backup is owned by the operator.
"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import and_, delete, func, insert, inspect, select, update
from sqlalchemy.engine import Connection, Engine, make_url


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db_config import get_db_url, is_sqlite_url  # noqa: E402
from db_models import (  # noqa: E402
    companies,
    company_group_members,
    company_groups,
    dcf_industry_valuation_settings,
    industry_betas,
    metadata,
    relative_valuation_categories,
    relative_valuation_company_assignments,
    relative_valuation_subcategories,
)
from sqlalchemy import create_engine  # noqa: E402


DEFAULT_UNIVERSE = ROOT / "data" / "industrials" / "us_industrials_universe_2026-07-16.csv"
DEFAULT_BETAS = ROOT / "data" / "industrials" / "us_industrials_bucket_definitions_2026-01-09.csv"


@dataclass(frozen=True)
class BucketPlan:
    target: str
    canonical_legacy: str
    legacy_sources: tuple[str, ...]
    subcategory: str


BUCKET_PLANS = (
    BucketPlan(
        "Industrials : Aerospace & Defense",
        "Industrials : Aerospace & Defense",
        ("Industrials : Aerospace & Defense",),
        "Aerospace & Defense",
    ),
    BucketPlan(
        "Industrials : Electrical, Automation & Grid Equipment",
        "Industrials : Electrical Equipment & Parts",
        ("Industrials : Electrical Equipment & Parts",),
        "Electrical & Automation",
    ),
    BucketPlan(
        "Industrials : Industrial Machinery & Components",
        "Industrials : Specialty Industrial Machinery",
        (
            "Industrials : Specialty Industrial Machinery",
            "Industrials : Metal Fabrication",
            "Industrials : Tools & Accessories",
        ),
        "Machinery & Industrial Components",
    ),
    BucketPlan(
        "Industrials : Construction, Engineering & Building Systems",
        "Industrials : Engineering & Construction",
        (
            "Industrials : Engineering & Construction",
            "Industrials : Building Products & Equipment",
        ),
        "Construction & Building Systems",
    ),
    BucketPlan(
        "Industrials : Heavy Equipment & Industrial Rentals",
        "Industrials : Farm & Heavy Construction Machinery",
        (
            "Industrials : Farm & Heavy Construction Machinery",
            "Industrials : Rental & Leasing Services",
        ),
        "Machinery & Industrial Components",
    ),
    BucketPlan(
        "Industrials : Freight, Logistics & Rail",
        "Industrials : Integrated Freight & Logistics",
        (
            "Industrials : Integrated Freight & Logistics",
            "Industrials : Railroads",
            "Industrials : Trucking",
        ),
        "Freight, Rail & Marine Logistics",
    ),
    BucketPlan(
        "Industrials : Aviation & Airport Services",
        "Industrials : Airlines",
        (
            "Industrials : Airlines",
            "Industrials : Airports & Air Services",
        ),
        "Airlines & Airport Services",
    ),
    BucketPlan(
        "Industrials : Marine Transportation",
        "Industrials : Marine Shipping",
        ("Industrials : Marine Shipping",),
        "Freight, Rail & Marine Logistics",
    ),
    BucketPlan(
        "Industrials : Environmental & Waste Services",
        "Industrials : Waste Management",
        (
            "Industrials : Waste Management",
            "Industrials : Pollution & Treatment Controls",
        ),
        "Environmental Services",
    ),
    BucketPlan(
        "Industrials : Professional, Security & Workforce Services",
        "Industrials : Specialty Business Services",
        (
            "Industrials : Specialty Business Services",
            "Industrials : Consulting Services",
            "Industrials : Security & Protection Services",
            "Industrials : Staffing & Employment Services",
        ),
        "Professional & Workforce Services",
    ),
    BucketPlan(
        "Industrials : Industrial Distribution & Business Equipment",
        "Industrials : Industrial Distribution",
        (
            "Industrials : Industrial Distribution",
            "Industrials : Business Equipment & Supplies",
        ),
        "Distribution & Diversified Industrials",
    ),
    BucketPlan(
        "Industrials : Diversified Industrials & Infrastructure",
        "Industrials : Conglomerates",
        (
            "Industrials : Conglomerates",
            "Industrials : Infrastructure Operations",
        ),
        "Distribution & Diversified Industrials",
    ),
)

TARGET_BUCKETS = tuple(plan.target for plan in BUCKET_PLANS)
LEGACY_BUCKETS = tuple(dict.fromkeys(name for plan in BUCKET_PLANS for name in plan.legacy_sources))
SUBCATEGORIES = tuple(sorted({plan.subcategory for plan in BUCKET_PLANS}))
BUCKET_TO_SUBCATEGORY = {plan.target: plan.subcategory for plan in BUCKET_PLANS}


def _chunks(values: Iterable[int], size: int = 500) -> Iterable[list[int]]:
    values = list(values)
    for start in range(0, len(values), size):
        yield values[start : start + size]


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def load_inputs(universe_path: Path, beta_path: Path) -> tuple[list[dict[str, str]], list[dict[str, Any]]]:
    universe = _read_csv(universe_path)
    required = {"Company", "Ticker", "Industry Bucket", "Category", "Sub-Category"}
    if not universe or not required.issubset(universe[0]):
        raise ValueError(f"Universe must contain columns: {', '.join(sorted(required))}")
    if len(universe) != 709:
        raise ValueError(f"Strict Industrials universe must contain 709 rows; found {len(universe)}")

    tickers = [row["Ticker"].strip().upper() for row in universe]
    if any(not ticker for ticker in tickers):
        raise ValueError("Universe contains a blank ticker")
    if len(set(tickers)) != len(tickers):
        raise ValueError("Universe contains duplicate normalized tickers")

    for row in universe:
        if row["Category"].strip() != "Industrials":
            raise ValueError(f"Unexpected category for {row['Ticker']}: {row['Category']}")
        if row["Industry Bucket"].strip() not in TARGET_BUCKETS:
            raise ValueError(f"Unexpected bucket for {row['Ticker']}: {row['Industry Bucket']}")
        expected_subcategory = BUCKET_TO_SUBCATEGORY[row["Industry Bucket"].strip()]
        if row["Sub-Category"].strip() != expected_subcategory:
            raise ValueError(
                f"Sub-category mismatch for {row['Ticker']}: expected {expected_subcategory}, "
                f"found {row['Sub-Category']}"
            )

    raw_betas = _read_csv(beta_path)
    beta_rows: list[dict[str, Any]] = []
    for row in raw_betas:
        beta_rows.append(
            {
                "user_industry_bucket": row["Industry Bucket"].strip(),
                "mapped_sector": row["Mapped Sector"].strip(),
                "unlevered_beta": float(row["Unlevered Beta"]),
                "cash_adjusted_beta": float(row["Cash-Adjusted Beta"]),
                "updated_at": row["Beta Source Date"].strip(),
            }
        )
    if {row["user_industry_bucket"] for row in beta_rows} != set(TARGET_BUCKETS):
        raise ValueError("Beta file must contain exactly one row for each of the 12 target buckets")
    if len(beta_rows) != len(TARGET_BUCKETS):
        raise ValueError("Beta file contains duplicate target buckets")
    return universe, beta_rows


def _table_names(engine: Engine) -> set[str]:
    return set(inspect(engine).get_table_names())


def _group_map(conn: Connection) -> dict[str, int]:
    return {
        str(name): int(group_id)
        for group_id, name in conn.execute(select(company_groups.c.id, company_groups.c.name))
    }


def _company_matches(conn: Connection, universe: list[dict[str, str]]) -> tuple[dict[str, int], list[str]]:
    candidates: dict[str, list[tuple[int, str]]] = defaultdict(list)
    for company_id, ticker, country in conn.execute(select(companies.c.id, companies.c.ticker, companies.c.country)):
        normalized = str(ticker or "").strip().upper()
        if normalized:
            candidates[normalized].append((int(company_id), str(country or "").strip().upper()))

    matches: dict[str, int] = {}
    missing: list[str] = []
    for row in universe:
        ticker = row["Ticker"].strip().upper()
        choices = sorted(candidates.get(ticker, []), key=lambda item: (item[1] not in {"US", "USA"}, item[0]))
        if choices:
            matches[ticker] = choices[0][0]
        else:
            missing.append(ticker)
    return matches, missing


def _preflight(conn: Connection) -> tuple[dict[str, int], list[str], list[str]]:
    groups = _group_map(conn)
    recognized = set(LEGACY_BUCKETS) | set(TARGET_BUCKETS)
    unknown_groups = sorted(name for name in groups if name.startswith("Industrials : ") and name not in recognized)
    unknown_betas = sorted(
        {
            str(name)
            for (name,) in conn.execute(
                select(industry_betas.c.user_industry_bucket).where(
                    industry_betas.c.user_industry_bucket.like("Industrials : %")
                )
            )
            if str(name) not in recognized
        }
    )
    if unknown_groups or unknown_betas:
        parts = []
        if unknown_groups:
            parts.append(f"unmapped groups: {unknown_groups}")
        if unknown_betas:
            parts.append(f"unmapped beta buckets: {unknown_betas}")
        raise RuntimeError("Refusing to modify an unrecognized Industrials taxonomy; " + "; ".join(parts))
    return groups, unknown_groups, unknown_betas


def inspect_plan(engine: Engine, universe: list[dict[str, str]]) -> dict[str, Any]:
    required = {"companies", "company_groups", "company_group_members", "industry_betas"}
    missing_tables = sorted(required - _table_names(engine))
    if missing_tables:
        raise RuntimeError(f"Database is missing required tables: {missing_tables}")
    with engine.connect() as conn:
        groups, _, _ = _preflight(conn)
        matches, missing = _company_matches(conn, universe)
        industrial_group_names = sorted(name for name in groups if name.startswith("Industrials : "))
        beta_count = int(
            conn.execute(
                select(func.count()).select_from(industry_betas).where(
                    industry_betas.c.user_industry_bucket.like("Industrials : %")
                )
            ).scalar_one()
        )
        legacy_memberships = int(
            conn.execute(
                select(func.count()).select_from(company_group_members).where(
                    company_group_members.c.group_id.in_([groups[name] for name in LEGACY_BUCKETS if name in groups])
                )
            ).scalar_one()
        )
    return {
        "mode": "dry-run",
        "universe_rows": len(universe),
        "matched_companies": len(matches),
        "missing_companies": len(missing),
        "missing_tickers": missing,
        "existing_industrials_groups": industrial_group_names,
        "existing_industrials_beta_rows": beta_count,
        "legacy_memberships_to_consolidate": legacy_memberships,
        "target_bucket_count": len(TARGET_BUCKETS),
        "target_subcategory_count": len(SUBCATEGORIES),
    }


def _copy_dcf_settings_if_needed(conn: Connection, source_id: int, target_id: int) -> bool:
    if source_id == target_id:
        return False
    target_exists = conn.execute(
        select(dcf_industry_valuation_settings.c.group_id).where(
            dcf_industry_valuation_settings.c.group_id == target_id
        )
    ).first()
    if target_exists:
        return False
    source = conn.execute(
        select(dcf_industry_valuation_settings).where(dcf_industry_valuation_settings.c.group_id == source_id)
    ).mappings().first()
    if not source:
        return False
    payload = dict(source)
    payload["group_id"] = target_id
    conn.execute(insert(dcf_industry_valuation_settings).values(**payload))
    return True


def _insert_missing_memberships(conn: Connection, pairs: set[tuple[int, int]]) -> int:
    if not pairs:
        return 0
    group_ids = {group_id for group_id, _ in pairs}
    company_ids = {company_id for _, company_id in pairs}
    existing: set[tuple[int, int]] = set()
    for company_chunk in _chunks(company_ids):
        existing.update(
            (int(group_id), int(company_id))
            for group_id, company_id in conn.execute(
                select(company_group_members.c.group_id, company_group_members.c.company_id).where(
                    and_(
                        company_group_members.c.group_id.in_(group_ids),
                        company_group_members.c.company_id.in_(company_chunk),
                    )
                )
            )
        )
    payload = [
        {"group_id": group_id, "company_id": company_id}
        for group_id, company_id in sorted(pairs - existing)
    ]
    if payload:
        conn.execute(insert(company_group_members), payload)
    return len(payload)


def _insert_missing_assignments(conn: Connection, pairs: set[tuple[int, int]]) -> int:
    if not pairs:
        return 0
    subcategory_ids = {subcategory_id for company_id, subcategory_id in pairs}
    company_ids = {company_id for company_id, subcategory_id in pairs}
    existing: set[tuple[int, int]] = set()
    for company_chunk in _chunks(company_ids):
        existing.update(
            (int(company_id), int(subcategory_id))
            for company_id, subcategory_id in conn.execute(
                select(
                    relative_valuation_company_assignments.c.company_id,
                    relative_valuation_company_assignments.c.subcategory_id,
                ).where(
                    and_(
                        relative_valuation_company_assignments.c.company_id.in_(company_chunk),
                        relative_valuation_company_assignments.c.subcategory_id.in_(subcategory_ids),
                    )
                )
            )
        )
    payload = [
        {"company_id": company_id, "subcategory_id": subcategory_id}
        for company_id, subcategory_id in sorted(pairs - existing)
    ]
    if payload:
        conn.execute(insert(relative_valuation_company_assignments), payload)
    return len(payload)


def apply_migration(
    engine: Engine,
    universe: list[dict[str, str]],
    beta_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    relevant_tables = [
        company_groups,
        company_group_members,
        relative_valuation_categories,
        relative_valuation_subcategories,
        relative_valuation_company_assignments,
        industry_betas,
        dcf_industry_valuation_settings,
    ]
    metadata.create_all(engine, tables=relevant_tables)
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")

    with engine.begin() as conn:
        groups, _, _ = _preflight(conn)
        initial_groups = dict(groups)
        matches, missing = _company_matches(conn, universe)

        legacy_members_by_id: dict[int, set[int]] = defaultdict(set)
        legacy_ids = {groups[name] for name in LEGACY_BUCKETS if name in groups}
        if legacy_ids:
            for group_id, company_id in conn.execute(
                select(company_group_members.c.group_id, company_group_members.c.company_id).where(
                    company_group_members.c.group_id.in_(legacy_ids)
                )
            ):
                legacy_members_by_id[int(group_id)].add(int(company_id))

        target_ids: dict[str, int] = {}
        dcf_settings_copied = 0
        groups_renamed = 0
        groups_created = 0

        for plan in BUCKET_PLANS:
            if plan.target in groups:
                target_id = groups[plan.target]
                canonical_id = initial_groups.get(plan.canonical_legacy)
                if canonical_id and _copy_dcf_settings_if_needed(conn, canonical_id, target_id):
                    dcf_settings_copied += 1
            elif plan.canonical_legacy in initial_groups:
                target_id = initial_groups[plan.canonical_legacy]
                conn.execute(update(company_groups).where(company_groups.c.id == target_id).values(name=plan.target))
                groups.pop(plan.canonical_legacy, None)
                groups[plan.target] = target_id
                groups_renamed += 1
            else:
                result = conn.execute(insert(company_groups).values(name=plan.target))
                target_id = int(result.inserted_primary_key[0])
                groups[plan.target] = target_id
                groups_created += 1
            target_ids[plan.target] = target_id

        membership_pairs: set[tuple[int, int]] = set()
        for plan in BUCKET_PLANS:
            target_id = target_ids[plan.target]
            for source_name in plan.legacy_sources:
                source_id = initial_groups.get(source_name)
                if source_id is not None:
                    membership_pairs.update((target_id, company_id) for company_id in legacy_members_by_id[source_id])
        memberships_added_from_legacy = _insert_missing_memberships(conn, membership_pairs)

        matched_ids = set(matches.values())
        if matched_ids:
            for company_chunk in _chunks(matched_ids):
                conn.execute(
                    delete(company_group_members).where(
                        and_(
                            company_group_members.c.company_id.in_(company_chunk),
                            company_group_members.c.group_id.in_(set(target_ids.values())),
                        )
                    )
                )
        universe_pairs = {
            (target_ids[row["Industry Bucket"].strip()], matches[row["Ticker"].strip().upper()])
            for row in universe
            if row["Ticker"].strip().upper() in matches
        }
        _insert_missing_memberships(conn, universe_pairs)

        removable_group_ids = legacy_ids - set(target_ids.values())
        legacy_dcf_settings_removed = 0
        if removable_group_ids:
            legacy_dcf_settings_removed = int(
                conn.execute(
                    select(func.count()).select_from(dcf_industry_valuation_settings).where(
                        dcf_industry_valuation_settings.c.group_id.in_(removable_group_ids)
                    )
                ).scalar_one()
            )
            conn.execute(
                delete(dcf_industry_valuation_settings).where(
                    dcf_industry_valuation_settings.c.group_id.in_(removable_group_ids)
                )
            )
            conn.execute(
                delete(company_group_members).where(company_group_members.c.group_id.in_(removable_group_ids))
            )
            conn.execute(delete(company_groups).where(company_groups.c.id.in_(removable_group_ids)))

        beta_names = set(LEGACY_BUCKETS) | set(TARGET_BUCKETS)
        conn.execute(delete(industry_betas).where(industry_betas.c.user_industry_bucket.in_(beta_names)))
        conn.execute(insert(industry_betas), beta_rows)

        category_id = conn.execute(
            select(relative_valuation_categories.c.id).where(relative_valuation_categories.c.name == "Industrials")
        ).scalar_one_or_none()
        if category_id is None:
            category_id = int(
                conn.execute(insert(relative_valuation_categories).values(name="Industrials")).inserted_primary_key[0]
            )

        subcategory_ids = {
            str(name): int(subcategory_id)
            for subcategory_id, name in conn.execute(
                select(relative_valuation_subcategories.c.id, relative_valuation_subcategories.c.name).where(
                    relative_valuation_subcategories.c.category_id == category_id
                )
            )
        }
        for name in SUBCATEGORIES:
            if name not in subcategory_ids:
                subcategory_ids[name] = int(
                    conn.execute(
                        insert(relative_valuation_subcategories).values(category_id=category_id, name=name)
                    ).inserted_primary_key[0]
                )

        stale_subcategory_ids = {
            subcategory_id for name, subcategory_id in subcategory_ids.items() if name not in SUBCATEGORIES
        }
        stale_assignments_removed = 0
        if stale_subcategory_ids:
            stale_assignments_removed = int(
                conn.execute(
                    select(func.count()).select_from(relative_valuation_company_assignments).where(
                        relative_valuation_company_assignments.c.subcategory_id.in_(stale_subcategory_ids)
                    )
                ).scalar_one()
            )
            conn.execute(
                delete(relative_valuation_company_assignments).where(
                    relative_valuation_company_assignments.c.subcategory_id.in_(stale_subcategory_ids)
                )
            )
            conn.execute(
                delete(relative_valuation_subcategories).where(
                    relative_valuation_subcategories.c.id.in_(stale_subcategory_ids)
                )
            )

        target_memberships = list(
            conn.execute(
                select(
                    company_group_members.c.company_id,
                    company_group_members.c.group_id,
                ).where(company_group_members.c.group_id.in_(set(target_ids.values())))
            )
        )
        affected_company_ids = {int(company_id) for company_id, group_id in target_memberships}
        industrial_subcategory_ids = {subcategory_ids[name] for name in SUBCATEGORIES}
        for company_chunk in _chunks(affected_company_ids):
            conn.execute(
                delete(relative_valuation_company_assignments).where(
                    and_(
                        relative_valuation_company_assignments.c.company_id.in_(company_chunk),
                        relative_valuation_company_assignments.c.subcategory_id.in_(industrial_subcategory_ids),
                    )
                )
            )
        group_id_to_target = {group_id: target for target, group_id in target_ids.items()}
        assignment_pairs = {
            (
                int(company_id),
                subcategory_ids[BUCKET_TO_SUBCATEGORY[group_id_to_target[int(group_id)]]],
            )
            for company_id, group_id in target_memberships
        }
        _insert_missing_assignments(conn, assignment_pairs)

        validation = _validate(conn, target_ids, subcategory_ids, universe, matches, beta_rows)
        report = {
            "mode": "applied",
            "applied_at": timestamp,
            "universe_rows": len(universe),
            "matched_companies": len(matches),
            "missing_companies": len(missing),
            "missing_tickers": missing,
            "groups_renamed": groups_renamed,
            "groups_created": groups_created,
            "legacy_groups_removed": len(removable_group_ids),
            "memberships_added_from_legacy": memberships_added_from_legacy,
            "matched_universe_memberships": len(universe_pairs),
            "dcf_settings_copied": dcf_settings_copied,
            "legacy_dcf_settings_removed_after_consolidation": legacy_dcf_settings_removed,
            "stale_industrials_subcategory_assignments_removed": stale_assignments_removed,
            "industrials_company_subcategory_assignments": len(assignment_pairs),
            "validation": validation,
        }
    return report


def _validate(
    conn: Connection,
    target_ids: dict[str, int],
    subcategory_ids: dict[str, int],
    universe: list[dict[str, str]],
    matches: dict[str, int],
    beta_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    group_names = {
        str(name)
        for (name,) in conn.execute(
            select(company_groups.c.name).where(company_groups.c.name.like("Industrials : %"))
        )
    }
    if group_names != set(TARGET_BUCKETS):
        raise RuntimeError(f"Post-migration group validation failed: {sorted(group_names)}")

    actual_betas = {
        str(name): (str(mapped), float(unlevered), float(cash_adjusted))
        for name, mapped, unlevered, cash_adjusted in conn.execute(
            select(
                industry_betas.c.user_industry_bucket,
                industry_betas.c.mapped_sector,
                industry_betas.c.unlevered_beta,
                industry_betas.c.cash_adjusted_beta,
            ).where(industry_betas.c.user_industry_bucket.like("Industrials : %"))
        )
    }
    expected_betas = {
        row["user_industry_bucket"]: (
            row["mapped_sector"],
            row["unlevered_beta"],
            row["cash_adjusted_beta"],
        )
        for row in beta_rows
    }
    if actual_betas != expected_betas:
        raise RuntimeError("Post-migration beta validation failed")

    actual_subcategories = {
        str(name)
        for (name,) in conn.execute(
            select(relative_valuation_subcategories.c.name)
            .join(
                relative_valuation_categories,
                relative_valuation_categories.c.id == relative_valuation_subcategories.c.category_id,
            )
            .where(relative_valuation_categories.c.name == "Industrials")
        )
    }
    if actual_subcategories != set(SUBCATEGORIES):
        raise RuntimeError(f"Post-migration sub-category validation failed: {sorted(actual_subcategories)}")

    expected_by_company = {
        matches[row["Ticker"].strip().upper()]: (
            target_ids[row["Industry Bucket"].strip()],
            subcategory_ids[row["Sub-Category"].strip()],
        )
        for row in universe
        if row["Ticker"].strip().upper() in matches
    }
    matched_ids = set(expected_by_company)
    memberships: dict[int, set[int]] = defaultdict(set)
    assignments: dict[int, set[int]] = defaultdict(set)
    for company_chunk in _chunks(matched_ids):
        for company_id, group_id in conn.execute(
            select(company_group_members.c.company_id, company_group_members.c.group_id).where(
                and_(
                    company_group_members.c.company_id.in_(company_chunk),
                    company_group_members.c.group_id.in_(set(target_ids.values())),
                )
            )
        ):
            memberships[int(company_id)].add(int(group_id))
        for company_id, subcategory_id in conn.execute(
            select(
                relative_valuation_company_assignments.c.company_id,
                relative_valuation_company_assignments.c.subcategory_id,
            ).where(
                and_(
                    relative_valuation_company_assignments.c.company_id.in_(company_chunk),
                    relative_valuation_company_assignments.c.subcategory_id.in_(
                        {subcategory_ids[name] for name in SUBCATEGORIES}
                    ),
                )
            )
        ):
            assignments[int(company_id)].add(int(subcategory_id))

    for company_id, (expected_group_id, expected_subcategory_id) in expected_by_company.items():
        if memberships[company_id] != {expected_group_id}:
            raise RuntimeError(f"Company {company_id} does not have exactly one expected Industrials bucket")
        if assignments[company_id] != {expected_subcategory_id}:
            raise RuntimeError(f"Company {company_id} does not have exactly one expected Industrials sub-category")

    return {
        "bucket_count": len(group_names),
        "beta_row_count": len(actual_betas),
        "subcategory_count": len(actual_subcategories),
        "matched_companies_with_exact_assignment": len(expected_by_company),
    }


def _backup_sqlite(db_url: str, destination: Path | None) -> Path:
    db_path = Path(make_url(db_url).database or "").resolve()
    if not db_path.exists():
        raise FileNotFoundError(f"SQLite database does not exist: {db_path}")
    if destination is None:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        destination = db_path.with_name(f"{db_path.name}.pre-industrials-{timestamp}.bak")
    destination = destination.resolve()
    if destination == db_path:
        raise ValueError("Backup destination must differ from the database path")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        raise FileExistsError(f"Refusing to overwrite an existing backup: {destination}")
    with sqlite3.connect(db_path) as source, sqlite3.connect(destination) as target:
        source.backup(target)
    return destination


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Commit the migration; default is dry-run")
    parser.add_argument(
        "--confirm-backup",
        action="store_true",
        help="Confirm an external backup exists (required for non-SQLite applies)",
    )
    parser.add_argument("--db-url", default=get_db_url(), help="SQLAlchemy database URL")
    parser.add_argument("--universe", type=Path, default=DEFAULT_UNIVERSE)
    parser.add_argument("--betas", type=Path, default=DEFAULT_BETAS)
    parser.add_argument("--backup-file", type=Path, help="Optional SQLite backup destination")
    parser.add_argument("--audit-file", type=Path, help="Optional JSON audit output")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    universe, beta_rows = load_inputs(args.universe.resolve(), args.betas.resolve())
    if args.apply and not is_sqlite_url(args.db_url) and not args.confirm_backup:
        raise SystemExit("Non-SQLite applies require --confirm-backup after an external database backup")

    backup_path: Path | None = None
    if args.apply and is_sqlite_url(args.db_url):
        backup_path = _backup_sqlite(args.db_url, args.backup_file)

    engine = create_engine(
        args.db_url,
        future=True,
        pool_pre_ping=True,
        connect_args={"check_same_thread": False} if is_sqlite_url(args.db_url) else {},
    )
    try:
        report = apply_migration(engine, universe, beta_rows) if args.apply else inspect_plan(engine, universe)
    finally:
        engine.dispose()
    if backup_path is not None:
        report["sqlite_backup"] = str(backup_path)

    output = json.dumps(report, indent=2, sort_keys=True)
    print(output)
    if args.audit_file:
        args.audit_file.parent.mkdir(parents=True, exist_ok=True)
        args.audit_file.write_text(output + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
