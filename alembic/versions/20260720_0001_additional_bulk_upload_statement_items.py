"""add annual and quarterly stores for additional bulk-upload statement items

Revision ID: 20260720_0001
Revises: 20260712_0001
Create Date: 2026-07-20
"""

from alembic import op
import sqlalchemy as sa


revision = "20260720_0001"
down_revision = "20260712_0001"
branch_labels = None
depends_on = None


def _create_annual_table(table_name: str, value_column: str) -> None:
    op.create_table(
        table_name,
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("fiscal_year", sa.Integer(), nullable=False),
        sa.Column(value_column, sa.Float(), nullable=False),
        sa.ForeignKeyConstraint(["company_id"], ["companies.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("company_id", "fiscal_year"),
    )


def _create_quarterly_table(table_name: str, value_column: str) -> None:
    op.create_table(
        table_name,
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("quarter_end", sa.Text(), nullable=False),
        sa.Column(value_column, sa.Float(), nullable=False),
        sa.ForeignKeyConstraint(["company_id"], ["companies.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("company_id", "quarter_end"),
    )


def upgrade() -> None:
    _create_annual_table("net_income_to_common_annual", "net_income_to_common")
    _create_quarterly_table("net_income_to_common_quarterly", "net_income_to_common")
    _create_annual_table(
        "earnings_from_discontinued_operations_annual",
        "earnings_from_discontinued_operations",
    )
    _create_quarterly_table(
        "earnings_from_discontinued_operations_quarterly",
        "earnings_from_discontinued_operations",
    )
    _create_annual_table(
        "minority_interest_in_earnings_annual",
        "minority_interest_in_earnings",
    )
    _create_quarterly_table(
        "minority_interest_in_earnings_quarterly",
        "minority_interest_in_earnings",
    )
    _create_annual_table("common_dividends_paid_annual", "common_dividends_paid")
    _create_quarterly_table("common_dividends_paid_quarterly", "common_dividends_paid")


def downgrade() -> None:
    op.drop_table("common_dividends_paid_quarterly")
    op.drop_table("common_dividends_paid_annual")
    op.drop_table("minority_interest_in_earnings_quarterly")
    op.drop_table("minority_interest_in_earnings_annual")
    op.drop_table("earnings_from_discontinued_operations_quarterly")
    op.drop_table("earnings_from_discontinued_operations_annual")
    op.drop_table("net_income_to_common_quarterly")
    op.drop_table("net_income_to_common_annual")
