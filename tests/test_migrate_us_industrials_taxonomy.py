from __future__ import annotations

import unittest
from collections import defaultdict

from sqlalchemy import create_engine, insert, select

from db_models import (
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
from scripts.migrate_us_industrials_taxonomy import (
    BUCKET_PLANS,
    DEFAULT_BETAS,
    DEFAULT_UNIVERSE,
    LEGACY_BUCKETS,
    SUBCATEGORIES,
    TARGET_BUCKETS,
    apply_migration,
    load_inputs,
)


class IndustrialsTaxonomyMigrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        metadata.create_all(
            self.engine,
            tables=[
                companies,
                company_groups,
                company_group_members,
                industry_betas,
                dcf_industry_valuation_settings,
                relative_valuation_categories,
                relative_valuation_subcategories,
                relative_valuation_company_assignments,
            ],
        )
        self.universe, self.betas = load_inputs(DEFAULT_UNIVERSE, DEFAULT_BETAS)
        self.sample_by_bucket = {}
        for row in self.universe:
            self.sample_by_bucket.setdefault(row["Industry Bucket"], row)

        with self.engine.begin() as conn:
            conn.execute(
                insert(company_groups),
                [{"name": name} for name in LEGACY_BUCKETS],
            )
            group_ids = {
                name: group_id
                for group_id, name in conn.execute(select(company_groups.c.id, company_groups.c.name))
            }
            conn.execute(
                insert(industry_betas),
                [
                    {
                        "user_industry_bucket": name,
                        "mapped_sector": f"Legacy sector {index}",
                        "unlevered_beta": 0.5,
                        "cash_adjusted_beta": 0.6,
                        "updated_at": "2025-01-01",
                    }
                    for index, name in enumerate(LEGACY_BUCKETS)
                ],
            )

            company_rows = [
                {
                    "name": self.sample_by_bucket[plan.target]["Company"],
                    "ticker": self.sample_by_bucket[plan.target]["Ticker"],
                    "country": "USA",
                }
                for plan in BUCKET_PLANS
            ]
            company_rows.append({"name": "Historical Industrial", "ticker": "ZZZOLD", "country": "USA"})
            conn.execute(insert(companies), company_rows)
            company_ids = {
                ticker: company_id
                for company_id, ticker in conn.execute(select(companies.c.id, companies.c.ticker))
            }

            memberships = [
                {
                    "group_id": group_ids[plan.canonical_legacy],
                    "company_id": company_ids[self.sample_by_bucket[plan.target]["Ticker"]],
                }
                for plan in BUCKET_PLANS
            ]
            memberships.append(
                {
                    "group_id": group_ids["Industrials : Tools & Accessories"],
                    "company_id": company_ids["ZZZOLD"],
                }
            )
            conn.execute(insert(company_group_members), memberships)

            settings = {
                "historical_years": 5,
                "terminal_growth_usa": 2.0,
                "terminal_growth_india": 4.0,
                "terminal_growth_china": 3.0,
                "terminal_growth_japan": 1.0,
                "future_revenue_growth": 7.0,
                "starting_projected_revenue_growth_cap": 25.0,
                "ebidta_margin_growth": 0.0,
                "da_percent_growth": 0.0,
                "capex_percent_growth": 0.0,
                "working_capital_days_growth": 0.0,
                "wacc_direction": 0.0,
                "updated_at": "2025-01-01",
            }
            conn.execute(
                insert(dcf_industry_valuation_settings),
                [
                    {**settings, "group_id": group_ids["Industrials : Electrical Equipment & Parts"]},
                    {**settings, "group_id": group_ids["Industrials : Metal Fabrication"]},
                ],
            )

    def tearDown(self) -> None:
        self.engine.dispose()

    def test_inputs_are_strict_and_complete(self) -> None:
        self.assertEqual(len(self.universe), 709)
        self.assertEqual(len({row["Ticker"] for row in self.universe}), 709)
        self.assertEqual({row["Industry Bucket"] for row in self.universe}, set(TARGET_BUCKETS))
        self.assertEqual(len(self.betas), 12)
        self.assertEqual(len(SUBCATEGORIES), 9)

    def test_migration_consolidates_and_is_idempotent(self) -> None:
        first = apply_migration(self.engine, self.universe, self.betas)
        self.assertEqual(first["matched_companies"], 12)
        self.assertEqual(first["legacy_groups_removed"], 13)
        self.assertEqual(first["groups_renamed"], 11)
        self.assertEqual(first["validation"]["bucket_count"], 12)
        self.assertEqual(first["validation"]["beta_row_count"], 12)
        self.assertEqual(first["validation"]["subcategory_count"], 9)

        with self.engine.connect() as conn:
            groups = {
                name: group_id
                for group_id, name in conn.execute(select(company_groups.c.id, company_groups.c.name))
            }
            self.assertEqual(set(groups), set(TARGET_BUCKETS))

            dcf_group_ids = {
                group_id for (group_id,) in conn.execute(select(dcf_industry_valuation_settings.c.group_id))
            }
            self.assertEqual(
                dcf_group_ids,
                {groups["Industrials : Electrical, Automation & Grid Equipment"]},
            )

            old_company_id = conn.execute(
                select(companies.c.id).where(companies.c.ticker == "ZZZOLD")
            ).scalar_one()
            old_groups = {
                name
                for (name,) in conn.execute(
                    select(company_groups.c.name)
                    .join(company_group_members, company_group_members.c.group_id == company_groups.c.id)
                    .where(company_group_members.c.company_id == old_company_id)
                )
            }
            self.assertEqual(old_groups, {"Industrials : Industrial Machinery & Components"})

            old_subcategories = {
                name
                for (name,) in conn.execute(
                    select(relative_valuation_subcategories.c.name)
                    .join(
                        relative_valuation_company_assignments,
                        relative_valuation_company_assignments.c.subcategory_id
                        == relative_valuation_subcategories.c.id,
                    )
                    .where(relative_valuation_company_assignments.c.company_id == old_company_id)
                )
            }
            self.assertEqual(old_subcategories, {"Machinery & Industrial Components"})

        second = apply_migration(self.engine, self.universe, self.betas)
        self.assertEqual(second["groups_renamed"], 0)
        self.assertEqual(second["groups_created"], 0)
        self.assertEqual(second["legacy_groups_removed"], 0)
        self.assertEqual(second["memberships_added_from_legacy"], 0)
        self.assertEqual(second["validation"]["matched_companies_with_exact_assignment"], 12)


if __name__ == "__main__":
    unittest.main()
