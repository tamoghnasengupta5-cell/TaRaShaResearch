"""capture and expose current-debt components for Consumer working capital

Revision ID: 20260727_0002
Revises: 20260727_0001
Create Date: 2026-07-27
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text


revision = "20260727_0002"
down_revision = "20260727_0001"
branch_labels = None
depends_on = None


FACT_KEYS = ("shortTermBorrowings", "currentPortionLongTermDebt")


def _view_definition(view_name: str) -> str:
    definition = op.get_bind().execute(
        text("SELECT pg_get_viewdef(CAST(:view_name AS regclass), true)"),
        {"view_name": view_name},
    ).scalar_one()
    return definition.rstrip(";\n ")


def _remove_facts(view_name: str) -> None:
    current = _view_definition(view_name)
    quoted_keys = ", ".join(f"'{key}'" for key in FACT_KEYS)
    op.execute(
        f"""
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT * FROM ({current}) AS current_facts
        WHERE fact_key NOT IN ({quoted_keys})
        """
    )


def upgrade() -> None:
    op.create_table(
        "short_term_borrowings_annual",
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("fiscal_year", sa.Integer(), nullable=False),
        sa.Column("short_term_borrowings", sa.Float(), nullable=False),
        sa.ForeignKeyConstraint(["company_id"], ["companies.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("company_id", "fiscal_year"),
    )
    op.create_table(
        "current_portion_long_term_debt_annual",
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("fiscal_year", sa.Integer(), nullable=False),
        sa.Column("current_portion_long_term_debt", sa.Float(), nullable=False),
        sa.ForeignKeyConstraint(["company_id"], ["companies.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("company_id", "fiscal_year"),
    )

    current = _view_definition("consumer_financial_facts")
    op.execute(
        f"""
        CREATE OR REPLACE VIEW consumer_financial_facts AS
        {current}
        UNION ALL SELECT company_id, 'balance', 'shortTermBorrowings', 'Short-term borrowings', 'amount', fiscal_year, short_term_borrowings::double precision FROM short_term_borrowings_annual
        UNION ALL SELECT company_id, 'balance', 'currentPortionLongTermDebt', 'Current portion of long-term debt', 'amount', fiscal_year, current_portion_long_term_debt::double precision FROM current_portion_long_term_debt_annual
        """
    )
    op.execute("REVOKE ALL ON consumer_financial_facts FROM anon, authenticated")


def downgrade() -> None:
    _remove_facts("consumer_financial_facts")
    op.execute("REVOKE ALL ON consumer_financial_facts FROM anon, authenticated")
    op.drop_table("current_portion_long_term_debt_annual")
    op.drop_table("short_term_borrowings_annual")
