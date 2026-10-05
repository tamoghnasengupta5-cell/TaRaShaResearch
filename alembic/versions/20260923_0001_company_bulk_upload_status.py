"""record latest successful bulk upload per ticker, company, and country

Revision ID: 20260923_0001
Revises: 20260727_0002
"""

from alembic import op
import sqlalchemy as sa


revision = "20260923_0001"
down_revision = "20260727_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "company_bulk_upload_status",
        sa.Column("ticker", sa.Text(), nullable=False),
        sa.Column("company_name", sa.Text(), nullable=False),
        sa.Column("country", sa.Text(), nullable=False),
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("latest_uploaded_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["company_id"], ["companies.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("ticker", "company_name", "country"),
    )
    op.create_index(
        "ix_company_bulk_upload_status_company_id",
        "company_bulk_upload_status",
        ["company_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_company_bulk_upload_status_company_id", table_name="company_bulk_upload_status")
    op.drop_table("company_bulk_upload_status")
