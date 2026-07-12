"""add consumer read contract and unmanaged legacy tables

Revision ID: 20260712_0001
Revises: 20260615_0001
Create Date: 2026-07-12
"""

from alembic import op
import sqlalchemy as sa


revision = "20260712_0001"
down_revision = "20260615_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # These four tables existed in SQLite but were previously created outside
    # Alembic/db_models. Bringing them under migration keeps a PostgreSQL cutover
    # complete; only the two views below are exposed to the Consumer adapter.
    op.create_table(
        "price_annual",
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("fiscal_year", sa.Integer(), nullable=False),
        sa.Column("price_change", sa.Float(), nullable=False),
        sa.ForeignKeyConstraint(["company_id"], ["companies.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("company_id", "fiscal_year"),
    )
    op.create_table(
        "relative_valuation_market_metrics",
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("enterprise_value", sa.Float()),
        sa.Column("enterprise_value_source", sa.Text()),
        sa.Column("enterprise_value_as_of", sa.Text()),
        sa.Column("enterprise_value_detail", sa.Text()),
        sa.Column("trailing_pe", sa.Float()),
        sa.Column("trailing_pe_source", sa.Text()),
        sa.Column("trailing_pe_as_of", sa.Text()),
        sa.Column("trailing_pe_detail", sa.Text()),
        sa.Column("forward_pe", sa.Float()),
        sa.Column("forward_pe_source", sa.Text()),
        sa.Column("forward_pe_as_of", sa.Text()),
        sa.Column("forward_pe_detail", sa.Text()),
        sa.Column("updated_at", sa.Text()),
        sa.ForeignKeyConstraint(["company_id"], ["companies.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("company_id"),
    )
    op.create_table(
        "ttc_score_formula",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("score_type", sa.Text(), nullable=False),
        sa.Column("component", sa.Text(), nullable=False),
        sa.Column("weight", sa.Float(), nullable=False),
        sa.Column("threshold", sa.Float(), nullable=False),
        sa.Column("mode", sa.Text(), nullable=False),
        sa.Column("sort_order", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("score_type", "component", name="uq_ttc_score_formula_type_component"),
    )
    op.create_table(
        "valuation_saved_dashboards",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("company_ids_json", sa.Text(), nullable=False),
        sa.Column("result_json", sa.Text()),
        sa.Column("score_year_range", sa.Text()),
        sa.Column("terminal_year", sa.Integer()),
        sa.Column("created_at", sa.Text(), nullable=False),
        sa.Column("updated_at", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )

    op.execute(
        """
        CREATE VIEW consumer_companies AS
        SELECT id, name, upper(ticker) AS ticker, country
        FROM companies
        WHERE trim(coalesce(ticker, '')) <> ''
        """
    )
    op.execute(
        """
        CREATE VIEW consumer_financial_facts AS
        SELECT company_id, 'income'::text AS statement_key, 'revenue'::text AS fact_key, 'Revenue'::text AS label, 'amount'::text AS unit_kind, fiscal_year, revenue::double precision AS value FROM revenues_annual
        UNION ALL SELECT company_id, 'income', 'costOfRevenue', 'Cost of revenue', 'amount', fiscal_year, cost_of_revenue::double precision FROM cost_of_revenue_annual
        UNION ALL SELECT company_id, 'income', 'sga', 'Selling, general and administrative expense', 'amount', fiscal_year, sga::double precision FROM sga_annual
        UNION ALL SELECT company_id, 'income', 'ebitda', 'EBITDA', 'amount', fiscal_year, ebitda::double precision FROM ebitda_annual
        UNION ALL SELECT company_id, 'income', 'operatingIncome', 'Operating income', 'amount', fiscal_year, operating_income::double precision FROM operating_income_annual
        UNION ALL SELECT company_id, 'income', 'interestExpense', 'Interest expense', 'amount', fiscal_year, interest_expense::double precision FROM interest_expense_annual
        UNION ALL SELECT company_id, 'income', 'pretaxIncome', 'Pre-tax income', 'amount', fiscal_year, pretax_income::double precision FROM pretax_income_annual
        UNION ALL SELECT company_id, 'income', 'netIncome', 'Net income', 'amount', fiscal_year, net_income::double precision FROM net_income_annual
        UNION ALL SELECT company_id, 'balance', 'cash', 'Cash and equivalents', 'amount', fiscal_year, cash_and_cash_equivalents::double precision FROM cash_and_cash_equivalents_annual
        UNION ALL SELECT company_id, 'balance', 'shortTermInvestments', 'Short-term investments', 'amount', fiscal_year, short_term_investments::double precision FROM short_term_investments_annual
        UNION ALL SELECT company_id, 'balance', 'accountsReceivable', 'Accounts receivable', 'amount', fiscal_year, accounts_receivable::double precision FROM accounts_receivable_annual
        UNION ALL SELECT company_id, 'balance', 'inventory', 'Inventory', 'amount', fiscal_year, inventory::double precision FROM inventory_annual
        UNION ALL SELECT company_id, 'balance', 'currentAssets', 'Current assets', 'amount', fiscal_year, total_current_assets::double precision FROM total_current_assets_annual
        UNION ALL SELECT company_id, 'balance', 'assets', 'Total assets', 'amount', fiscal_year, total_assets::double precision FROM total_assets_annual
        UNION ALL SELECT company_id, 'balance', 'accountsPayable', 'Accounts payable', 'amount', fiscal_year, accounts_payable::double precision FROM accounts_payable_annual
        UNION ALL SELECT company_id, 'balance', 'currentDebt', 'Current debt', 'amount', fiscal_year, current_debt::double precision FROM current_debt_annual
        UNION ALL SELECT company_id, 'balance', 'currentLiabilities', 'Current liabilities', 'amount', fiscal_year, total_current_liabilities::double precision FROM total_current_liabilities_annual
        UNION ALL SELECT company_id, 'balance', 'longTermLiabilities', 'Long-term liabilities', 'amount', fiscal_year, total_long_term_liabilities::double precision FROM total_long_term_liabilities_annual
        UNION ALL SELECT company_id, 'balance', 'totalDebt', 'Total debt', 'amount', fiscal_year, total_debt::double precision FROM total_debt_annual
        UNION ALL SELECT company_id, 'balance', 'equity', 'Shareholders’ equity', 'amount', fiscal_year, shareholders_equity::double precision FROM shareholders_equity_annual
        UNION ALL SELECT company_id, 'cash', 'operatingCash', 'Operating cash flow', 'amount', fiscal_year, operating_cash_flow::double precision FROM operating_cash_flow_annual
        UNION ALL SELECT company_id, 'cash', 'capex', 'Capital expenditure', 'amount', fiscal_year, capital_expenditures::double precision FROM capital_expenditures_annual
        UNION ALL SELECT company_id, 'cash', 'depreciation', 'Depreciation and amortization', 'amount', fiscal_year, depreciation_amortization::double precision FROM depreciation_amortization_annual
        UNION ALL SELECT company_id, 'cash', 'netDebtIssuedPaid', 'Net debt issued or repaid', 'amount', fiscal_year, net_debt_issued_paid::double precision FROM net_debt_issued_paid_annual
        UNION ALL SELECT company_id, 'shares', 'sharesOutstanding', 'Basic shares outstanding', 'shares', fiscal_year, shares_outstanding_basic::double precision FROM shares_outstanding_basic_annual
        UNION ALL SELECT company_id, 'shares', 'marketCapitalization', 'Market capitalization', 'amount', fiscal_year, market_capitalization::double precision FROM market_capitalization_annual
        """
    )
    op.execute("REVOKE ALL ON consumer_companies, consumer_financial_facts FROM anon, authenticated")


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS consumer_financial_facts")
    op.execute("DROP VIEW IF EXISTS consumer_companies")
    op.drop_table("valuation_saved_dashboards")
    op.drop_table("ttc_score_formula")
    op.drop_table("relative_valuation_market_metrics")
    op.drop_table("price_annual")
