"""expose industry bucket in the consumer company read contract

Revision ID: 20260713_0003
Revises: 20260713_0002
Create Date: 2026-07-13
"""

from alembic import op


revision = "20260713_0003"
down_revision = "20260713_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE OR REPLACE VIEW consumer_companies AS
        SELECT
            c.id,
            c.name,
            upper(c.ticker) AS ticker,
            c.country,
            coalesce(
                (
                    SELECT string_agg(g.name, ' | ' ORDER BY g.name)
                    FROM company_group_members gm
                    JOIN company_groups g ON g.id = gm.group_id
                    WHERE gm.company_id = c.id
                ),
                'Unclassified'
            ) AS industry_bucket
        FROM companies c
        WHERE trim(coalesce(c.ticker, '')) <> ''
        """
    )
    op.execute("REVOKE ALL ON consumer_companies FROM anon, authenticated")


def downgrade() -> None:
    op.execute(
        """
        CREATE OR REPLACE VIEW consumer_companies AS
        SELECT id, name, upper(ticker) AS ticker, country
        FROM companies
        WHERE trim(coalesce(ticker, '')) <> ''
        """
    )
    op.execute("REVOKE ALL ON consumer_companies FROM anon, authenticated")
