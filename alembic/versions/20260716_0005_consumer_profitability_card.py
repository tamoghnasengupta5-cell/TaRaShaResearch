"""expose consumer margin and cost-structure facts

Revision ID: 20260716_0005
Revises: 20260713_0004
Create Date: 2026-07-16
"""

from alembic import op


revision = "20260716_0005"
down_revision = "20260713_0004"
branch_labels = None
depends_on = None


def _financial_facts_view(include_research_and_development: bool) -> str:
    research_and_development = """
        UNION ALL SELECT company_id, 'income', 'researchAndDevelopment', 'Research and development expense', 'amount', fiscal_year, research_and_development_expense::double precision FROM research_and_development_expense_annual
    """ if include_research_and_development else ""
    return f"""
        CREATE OR REPLACE VIEW consumer_financial_facts AS
        SELECT company_id, 'income'::text AS statement_key, 'revenue'::text AS fact_key, 'Revenue'::text AS label, 'amount'::text AS unit_kind, fiscal_year, revenue::double precision AS value FROM revenues_annual
        UNION ALL SELECT company_id, 'income', 'costOfRevenue', 'Cost of revenue', 'amount', fiscal_year, cost_of_revenue::double precision FROM cost_of_revenue_annual
        UNION ALL SELECT company_id, 'income', 'sga', 'Selling, general and administrative expense', 'amount', fiscal_year, sga::double precision FROM sga_annual
        {research_and_development}
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


def _industry_facts_view(include_cost_structure: bool) -> str:
    extra_facts = """
        UNION ALL
        SELECT gm.group_id, g.name, c.id, c.country, 'sga', r.fiscal_year, r.sga::double precision
        FROM company_group_members gm JOIN company_groups g ON g.id = gm.group_id JOIN companies c ON c.id = gm.company_id JOIN sga_annual r ON r.company_id = c.id
        UNION ALL
        SELECT gm.group_id, g.name, c.id, c.country, 'depreciation', r.fiscal_year, r.depreciation_amortization::double precision
        FROM company_group_members gm JOIN company_groups g ON g.id = gm.group_id JOIN companies c ON c.id = gm.company_id JOIN depreciation_amortization_annual r ON r.company_id = c.id
        UNION ALL
        SELECT gm.group_id, g.name, c.id, c.country, 'researchAndDevelopment', r.fiscal_year, r.research_and_development_expense::double precision
        FROM company_group_members gm JOIN company_groups g ON g.id = gm.group_id JOIN companies c ON c.id = gm.company_id JOIN research_and_development_expense_annual r ON r.company_id = c.id
    """ if include_cost_structure else ""
    return f"""
        CREATE OR REPLACE VIEW consumer_industry_income_facts AS
        SELECT gm.group_id AS bucket_id, g.name AS bucket_name, c.id AS company_id,
               c.country, 'revenue'::text AS fact_key, r.fiscal_year, r.revenue::double precision AS value
        FROM company_group_members gm JOIN company_groups g ON g.id = gm.group_id JOIN companies c ON c.id = gm.company_id JOIN revenues_annual r ON r.company_id = c.id
        UNION ALL
        SELECT gm.group_id, g.name, c.id, c.country, 'costOfRevenue', r.fiscal_year, r.cost_of_revenue::double precision
        FROM company_group_members gm JOIN company_groups g ON g.id = gm.group_id JOIN companies c ON c.id = gm.company_id JOIN cost_of_revenue_annual r ON r.company_id = c.id
        UNION ALL
        SELECT gm.group_id, g.name, c.id, c.country, 'operatingIncome', r.fiscal_year, r.operating_income::double precision
        FROM company_group_members gm JOIN company_groups g ON g.id = gm.group_id JOIN companies c ON c.id = gm.company_id JOIN operating_income_annual r ON r.company_id = c.id
        {extra_facts}
    """


def upgrade() -> None:
    op.execute(_financial_facts_view(include_research_and_development=True))
    op.execute(_industry_facts_view(include_cost_structure=True))
    op.execute("REVOKE ALL ON consumer_financial_facts, consumer_industry_income_facts FROM anon, authenticated")


def downgrade() -> None:
    op.execute(_financial_facts_view(include_research_and_development=False))
    op.execute(_industry_facts_view(include_cost_structure=False))
    op.execute("REVOKE ALL ON consumer_financial_facts, consumer_industry_income_facts FROM anon, authenticated")
