"""expose consumer common-income bridge facts

Revision ID: 20260720_0007
Revises: 20260720_0001
Create Date: 2026-07-20
"""

from alembic import op
from sqlalchemy import text


revision = "20260720_0007"
down_revision = "20260720_0001"
branch_labels = None
depends_on = None


BRIDGE_FACT_KEYS = (
    "minorityInterestInEarnings",
    "earningsFromDiscontinuedOperations",
    "commonDividendsPaid",
    "netIncomeToCommon",
)


def _view_definition(view_name: str) -> str:
    definition = op.get_bind().execute(
        text("SELECT pg_get_viewdef(CAST(:view_name AS regclass), true)"),
        {"view_name": view_name},
    ).scalar_one()
    return definition.rstrip(";\n ")


def _append_facts(view_name: str, unions: str) -> None:
    current = _view_definition(view_name)
    op.execute(f"CREATE OR REPLACE VIEW {view_name} AS\n{current}\n{unions}")


def _remove_bridge_facts(view_name: str) -> None:
    current = _view_definition(view_name)
    quoted_keys = ", ".join(f"'{key}'" for key in BRIDGE_FACT_KEYS)
    op.execute(
        f"""
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT * FROM ({current}) AS current_facts
        WHERE fact_key NOT IN ({quoted_keys})
        """
    )


def upgrade() -> None:
    _append_facts(
        "consumer_financial_facts",
        """
        UNION ALL SELECT company_id, 'income', 'minorityInterestInEarnings', 'Minority interest in earnings', 'amount', fiscal_year, minority_interest_in_earnings::double precision FROM minority_interest_in_earnings_annual
        UNION ALL SELECT company_id, 'income', 'earningsFromDiscontinuedOperations', 'Earnings from discontinued operations', 'amount', fiscal_year, earnings_from_discontinued_operations::double precision FROM earnings_from_discontinued_operations_annual
        UNION ALL SELECT company_id, 'cash', 'commonDividendsPaid', 'Common dividends paid', 'amount', fiscal_year, common_dividends_paid::double precision FROM common_dividends_paid_annual
        UNION ALL SELECT company_id, 'income', 'netIncomeToCommon', 'Net income to common', 'amount', fiscal_year, net_income_to_common::double precision FROM net_income_to_common_annual
        """,
    )
    _append_facts(
        "consumer_industry_income_facts",
        """
        UNION ALL SELECT gm.group_id, g.name, c.id, c.country, 'minorityInterestInEarnings', r.fiscal_year, r.minority_interest_in_earnings::double precision FROM company_group_members gm JOIN company_groups g ON g.id = gm.group_id JOIN companies c ON c.id = gm.company_id JOIN minority_interest_in_earnings_annual r ON r.company_id = c.id
        UNION ALL SELECT gm.group_id, g.name, c.id, c.country, 'earningsFromDiscontinuedOperations', r.fiscal_year, r.earnings_from_discontinued_operations::double precision FROM company_group_members gm JOIN company_groups g ON g.id = gm.group_id JOIN companies c ON c.id = gm.company_id JOIN earnings_from_discontinued_operations_annual r ON r.company_id = c.id
        UNION ALL SELECT gm.group_id, g.name, c.id, c.country, 'commonDividendsPaid', r.fiscal_year, r.common_dividends_paid::double precision FROM company_group_members gm JOIN company_groups g ON g.id = gm.group_id JOIN companies c ON c.id = gm.company_id JOIN common_dividends_paid_annual r ON r.company_id = c.id
        UNION ALL SELECT gm.group_id, g.name, c.id, c.country, 'netIncomeToCommon', r.fiscal_year, r.net_income_to_common::double precision FROM company_group_members gm JOIN company_groups g ON g.id = gm.group_id JOIN companies c ON c.id = gm.company_id JOIN net_income_to_common_annual r ON r.company_id = c.id
        """,
    )
    op.execute(
        "REVOKE ALL ON consumer_financial_facts, consumer_industry_income_facts FROM anon, authenticated"
    )


def downgrade() -> None:
    _remove_bridge_facts("consumer_financial_facts")
    _remove_bridge_facts("consumer_industry_income_facts")
    op.execute(
        "REVOKE ALL ON consumer_financial_facts, consumer_industry_income_facts FROM anon, authenticated"
    )
