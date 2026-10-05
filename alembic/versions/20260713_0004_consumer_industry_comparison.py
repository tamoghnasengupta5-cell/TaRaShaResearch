"""add consumer industry comparison read contract

Revision ID: 20260713_0004
Revises: 20260713_0003
Create Date: 2026-07-13
"""

from alembic import op


revision = "20260713_0004"
down_revision = "20260713_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE VIEW consumer_industry_bucket_memberships AS
        SELECT
            gm.company_id,
            g.id AS bucket_id,
            g.name AS bucket_name
        FROM company_group_members gm
        JOIN company_groups g ON g.id = gm.group_id
        """
    )
    op.execute(
        """
        CREATE VIEW consumer_industry_income_facts AS
        SELECT gm.group_id AS bucket_id, g.name AS bucket_name, c.id AS company_id,
               c.country, 'revenue'::text AS fact_key, r.fiscal_year,
               r.revenue::double precision AS value
        FROM company_group_members gm
        JOIN company_groups g ON g.id = gm.group_id
        JOIN companies c ON c.id = gm.company_id
        JOIN revenues_annual r ON r.company_id = c.id
        UNION ALL
        SELECT gm.group_id, g.name, c.id, c.country, 'costOfRevenue', r.fiscal_year,
               r.cost_of_revenue::double precision
        FROM company_group_members gm
        JOIN company_groups g ON g.id = gm.group_id
        JOIN companies c ON c.id = gm.company_id
        JOIN cost_of_revenue_annual r ON r.company_id = c.id
        UNION ALL
        SELECT gm.group_id, g.name, c.id, c.country, 'operatingIncome', r.fiscal_year,
               r.operating_income::double precision
        FROM company_group_members gm
        JOIN company_groups g ON g.id = gm.group_id
        JOIN companies c ON c.id = gm.company_id
        JOIN operating_income_annual r ON r.company_id = c.id
        """
    )
    op.execute(
        "REVOKE ALL ON consumer_industry_bucket_memberships, consumer_industry_income_facts FROM anon, authenticated"
    )


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS consumer_industry_income_facts")
    op.execute("DROP VIEW IF EXISTS consumer_industry_bucket_memberships")
