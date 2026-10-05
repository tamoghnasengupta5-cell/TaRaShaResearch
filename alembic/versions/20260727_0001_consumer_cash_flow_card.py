"""expose Consumer cash-flow bridge inputs

Revision ID: 20260727_0001
Revises: 20260725_0001
Create Date: 2026-07-27
"""

from alembic import op
from sqlalchemy import text


revision = "20260727_0001"
down_revision = "20260725_0001"
branch_labels = None
depends_on = None


COMPANY_FACT_KEYS = (
    "effectiveTaxRate",
    "nonCashWorkingCapital",
    "shareBasedCompensation",
    "otherAdjustments",
)

INDUSTRY_FACT_KEYS = (
    *COMPANY_FACT_KEYS,
    "capex",
    "netDebtIssuedPaid",
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


def _remove_facts(view_name: str, fact_keys: tuple[str, ...]) -> None:
    current = _view_definition(view_name)
    quoted_keys = ", ".join(f"'{key}'" for key in fact_keys)
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
        UNION ALL SELECT company_id, 'income', 'effectiveTaxRate', 'Effective tax rate', 'ratio', fiscal_year, eff_tax_rate::double precision FROM eff_tax_rate_annual
        UNION ALL SELECT company_id, 'balance', 'nonCashWorkingCapital', 'Non-cash working capital', 'amount', fiscal_year, non_cash_working_capital::double precision FROM non_cash_working_capital_annual
        UNION ALL SELECT company_id, 'cash', 'shareBasedCompensation', 'Share-based compensation', 'amount', fiscal_year, share_based_compensation::double precision FROM share_based_compensation_annual
        UNION ALL SELECT company_id, 'cash', 'otherAdjustments', 'Other adjustments', 'amount', fiscal_year, other_adjustments::double precision FROM other_adjustments_annual
        """,
    )
    _append_facts(
        "consumer_industry_income_facts",
        """
        UNION ALL SELECT gm.group_id, g.name, c.id, c.country, 'effectiveTaxRate', r.fiscal_year, r.eff_tax_rate::double precision FROM company_group_members gm JOIN company_groups g ON g.id = gm.group_id JOIN companies c ON c.id = gm.company_id JOIN eff_tax_rate_annual r ON r.company_id = c.id
        UNION ALL SELECT gm.group_id, g.name, c.id, c.country, 'nonCashWorkingCapital', r.fiscal_year, r.non_cash_working_capital::double precision FROM company_group_members gm JOIN company_groups g ON g.id = gm.group_id JOIN companies c ON c.id = gm.company_id JOIN non_cash_working_capital_annual r ON r.company_id = c.id
        UNION ALL SELECT gm.group_id, g.name, c.id, c.country, 'shareBasedCompensation', r.fiscal_year, r.share_based_compensation::double precision FROM company_group_members gm JOIN company_groups g ON g.id = gm.group_id JOIN companies c ON c.id = gm.company_id JOIN share_based_compensation_annual r ON r.company_id = c.id
        UNION ALL SELECT gm.group_id, g.name, c.id, c.country, 'otherAdjustments', r.fiscal_year, r.other_adjustments::double precision FROM company_group_members gm JOIN company_groups g ON g.id = gm.group_id JOIN companies c ON c.id = gm.company_id JOIN other_adjustments_annual r ON r.company_id = c.id
        UNION ALL SELECT gm.group_id, g.name, c.id, c.country, 'capex', r.fiscal_year, r.capital_expenditures::double precision FROM company_group_members gm JOIN company_groups g ON g.id = gm.group_id JOIN companies c ON c.id = gm.company_id JOIN capital_expenditures_annual r ON r.company_id = c.id
        UNION ALL SELECT gm.group_id, g.name, c.id, c.country, 'netDebtIssuedPaid', r.fiscal_year, r.net_debt_issued_paid::double precision FROM company_group_members gm JOIN company_groups g ON g.id = gm.group_id JOIN companies c ON c.id = gm.company_id JOIN net_debt_issued_paid_annual r ON r.company_id = c.id
        """,
    )
    op.execute(
        "REVOKE ALL ON consumer_financial_facts, consumer_industry_income_facts FROM anon, authenticated"
    )


def downgrade() -> None:
    _remove_facts("consumer_financial_facts", COMPANY_FACT_KEYS)
    _remove_facts("consumer_industry_income_facts", INDUSTRY_FACT_KEYS)
    op.execute(
        "REVOKE ALL ON consumer_financial_facts, consumer_industry_income_facts FROM anon, authenticated"
    )
