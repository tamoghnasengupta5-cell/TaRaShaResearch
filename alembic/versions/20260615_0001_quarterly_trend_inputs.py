"""add quarterly trend input tables

Revision ID: 20260615_0001
Revises: 3373347030cf
Create Date: 2026-06-15
"""

from alembic import op
import sqlalchemy as sa


revision = "20260615_0001"
down_revision = "3373347030cf"
branch_labels = None
depends_on = None


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
    _create_quarterly_table("revenues_quarterly", "revenue")
    _create_quarterly_table("operating_income_quarterly", "operating_income")
    _create_quarterly_table("deferred_revenue_quarterly", "deferred_revenue")
    _create_quarterly_table("accounts_receivable_quarterly", "accounts_receivable")
    _create_quarterly_table("capital_expenditures_quarterly", "capital_expenditures")
    _create_quarterly_table("operating_cash_flow_quarterly", "operating_cash_flow")
    op.create_table(
        "business_quarter_trend_weights",
        sa.Column("parameter_key", sa.Text(), nullable=False),
        sa.Column("parameter", sa.Text(), nullable=False),
        sa.Column("weight", sa.Float(), nullable=False),
        sa.Column("sort_order", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("parameter_key"),
    )
    op.bulk_insert(
        sa.table(
            "business_quarter_trend_weights",
            sa.column("parameter_key", sa.Text()),
            sa.column("parameter", sa.Text()),
            sa.column("weight", sa.Float()),
            sa.column("sort_order", sa.Integer()),
        ),
        [
            {"parameter_key": "revenue_growth", "parameter": "Revenue Growth", "weight": 20.0, "sort_order": 1},
            {"parameter_key": "operating_margin", "parameter": "Operating Margin", "weight": 20.0, "sort_order": 2},
            {"parameter_key": "operating_margin_change", "parameter": "Operating Margin Change", "weight": 15.0, "sort_order": 3},
            {"parameter_key": "incremental_operating_margin", "parameter": "Incremental Operating Margin", "weight": 15.0, "sort_order": 4},
            {"parameter_key": "bill_to_revenue", "parameter": "Bill to Revenue", "weight": 10.0, "sort_order": 5},
            {"parameter_key": "days_sales_outstanding", "parameter": "Days Sales Outstanding", "weight": 10.0, "sort_order": 6},
            {"parameter_key": "capex_to_ocf", "parameter": "Capex to OCF", "weight": 10.0, "sort_order": 7},
        ],
    )


def downgrade() -> None:
    op.drop_table("business_quarter_trend_weights")
    op.drop_table("operating_cash_flow_quarterly")
    op.drop_table("capital_expenditures_quarterly")
    op.drop_table("accounts_receivable_quarterly")
    op.drop_table("deferred_revenue_quarterly")
    op.drop_table("operating_income_quarterly")
    op.drop_table("revenues_quarterly")
