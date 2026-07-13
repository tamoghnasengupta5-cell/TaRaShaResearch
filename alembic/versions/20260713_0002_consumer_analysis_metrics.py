"""add consumer analysis metrics read contract

Revision ID: 20260713_0002
Revises: 20260712_0001
Create Date: 2026-07-13
"""

from alembic import op


revision = "20260713_0002"
down_revision = "20260712_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE VIEW consumer_analysis_metrics AS
        SELECT company_id, 'roic'::text AS metric_key, 'percent'::text AS unit_kind,
               fiscal_year, roic_pct::double precision AS value
        FROM roic_direct_upload_annual
        UNION ALL
        SELECT company_id, 'wacc', 'percent', fiscal_year, wacc::double precision
        FROM wacc_annual
        UNION ALL
        SELECT company_id, 'spread', 'percent', fiscal_year, spread_pct::double precision
        FROM roic_wacc_spread_annual
        UNION ALL
        SELECT company_id, 'fcff', 'amount', fiscal_year, fcff::double precision
        FROM fcff_annual
        """
    )
    op.execute("REVOKE ALL ON consumer_analysis_metrics FROM anon, authenticated")


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS consumer_analysis_metrics")
