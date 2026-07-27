"""add annual and quarterly FCFE cash-flow inputs

Revision ID: 20260725_0001
Revises: 20260720_0007
Create Date: 2026-07-25
"""

from alembic import op
import sqlalchemy as sa


revision = "20260725_0001"
down_revision = "20260720_0007"
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
    _create_annual_table("share_based_compensation_annual", "share_based_compensation")
    _create_quarterly_table("share_based_compensation_quarterly", "share_based_compensation")
    _create_annual_table("other_adjustments_annual", "other_adjustments")
    _create_quarterly_table("other_adjustments_quarterly", "other_adjustments")


def downgrade() -> None:
    op.drop_table("other_adjustments_quarterly")
    op.drop_table("other_adjustments_annual")
    op.drop_table("share_based_compensation_quarterly")
    op.drop_table("share_based_compensation_annual")
