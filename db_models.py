from sqlalchemy import Column, Float, ForeignKey, Integer, MetaData, Table, Text, UniqueConstraint

# Keep names lowercase to avoid quoted identifiers in Postgres.
metadata = MetaData()


companies = Table(
    "companies",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", Text, nullable=False),
    Column("ticker", Text, nullable=False),
    Column("country", Text, nullable=False, server_default="USA"),
    UniqueConstraint("name", "ticker", name="uq_companies_name_ticker"),
)


revenues_annual = Table(
    "revenues_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("revenue", Float, nullable=False),
)

revenues_ttm = Table(
    "revenues_ttm",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("as_of", Text, nullable=False),
    Column("revenue", Float, nullable=False),
)

cost_of_revenue_annual = Table(
    "cost_of_revenue_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("cost_of_revenue", Float, nullable=False),
)

cost_of_revenue_ttm = Table(
    "cost_of_revenue_ttm",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("as_of", Text, nullable=False),
    Column("cost_of_revenue", Float, nullable=False),
)

sga_annual = Table(
    "sga_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("sga", Float, nullable=False),
)

sga_ttm = Table(
    "sga_ttm",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("as_of", Text, nullable=False),
    Column("sga", Float, nullable=False),
)

op_margin_annual = Table(
    "op_margin_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("margin", Float, nullable=False),
)

op_margin_ttm = Table(
    "op_margin_ttm",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("as_of", Text, nullable=False),
    Column("margin", Float, nullable=False),
)

pretax_income_annual = Table(
    "pretax_income_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("pretax_income", Float, nullable=False),
)

pretax_income_ttm = Table(
    "pretax_income_ttm",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("as_of", Text, nullable=False),
    Column("pretax_income", Float, nullable=False),
)

net_income_annual = Table(
    "net_income_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("net_income", Float, nullable=False),
)

net_income_ttm = Table(
    "net_income_ttm",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("as_of", Text, nullable=False),
    Column("net_income", Float, nullable=False),
)

eff_tax_rate_annual = Table(
    "eff_tax_rate_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("eff_tax_rate", Float, nullable=False),
)

eff_tax_rate_ttm = Table(
    "eff_tax_rate_ttm",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("as_of", Text, nullable=False),
    Column("eff_tax_rate", Float, nullable=False),
)

ebit_annual = Table(
    "ebit_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("ebit", Float, nullable=False),
)

ebit_ttm = Table(
    "ebit_ttm",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("as_of", Text, nullable=False),
    Column("ebit", Float, nullable=False),
)

ebitda_annual = Table(
    "ebitda_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("ebitda", Float, nullable=False),
)

ebitda_ttm = Table(
    "ebitda_ttm",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("as_of", Text, nullable=False),
    Column("ebitda", Float, nullable=False),
)

interest_expense_annual = Table(
    "interest_expense_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("interest_expense", Float, nullable=False),
)

interest_expense_ttm = Table(
    "interest_expense_ttm",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("as_of", Text, nullable=False),
    Column("interest_expense", Float, nullable=False),
)

operating_income_annual = Table(
    "operating_income_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("operating_income", Float, nullable=False),
)

operating_income_ttm = Table(
    "operating_income_ttm",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("as_of", Text, nullable=False),
    Column("operating_income", Float, nullable=False),
)

nopat_annual = Table(
    "nopat_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("nopat", Float, nullable=False),
)

total_assets_annual = Table(
    "total_assets_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("total_assets", Float, nullable=False),
)

total_assets_ttm = Table(
    "total_assets_ttm",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("as_of", Text, nullable=False),
    Column("total_assets", Float, nullable=False),
)

short_term_investments_annual = Table(
    "short_term_investments_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("short_term_investments", Float, nullable=False),
)

short_term_investments_ttm = Table(
    "short_term_investments_ttm",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("as_of", Text, nullable=False),
    Column("short_term_investments", Float, nullable=False),
)

accounts_receivable_annual = Table(
    "accounts_receivable_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("accounts_receivable", Float, nullable=False),
)

accounts_receivable_ttm = Table(
    "accounts_receivable_ttm",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("as_of", Text, nullable=False),
    Column("accounts_receivable", Float, nullable=False),
)

total_current_liabilities_annual = Table(
    "total_current_liabilities_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("total_current_liabilities", Float, nullable=False),
)

total_current_liabilities_ttm = Table(
    "total_current_liabilities_ttm",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("as_of", Text, nullable=False),
    Column("total_current_liabilities", Float, nullable=False),
)

total_long_term_liabilities_annual = Table(
    "total_long_term_liabilities_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("total_long_term_liabilities", Float, nullable=False),
)

total_long_term_liabilities_ttm = Table(
    "total_long_term_liabilities_ttm",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("as_of", Text, nullable=False),
    Column("total_long_term_liabilities", Float, nullable=False),
)

total_debt_annual = Table(
    "total_debt_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("total_debt", Float, nullable=False),
)

total_debt_ttm = Table(
    "total_debt_ttm",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("as_of", Text, nullable=False),
    Column("total_debt", Float, nullable=False),
)

market_capitalization_annual = Table(
    "market_capitalization_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("market_capitalization", Float, nullable=False),
)

roic_direct_upload_annual = Table(
    "roic_direct_upload_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("roic_pct", Float, nullable=False),
)

debt_equity_annual = Table(
    "debt_equity_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("debt_equity", Float, nullable=False),
)

levered_beta_annual = Table(
    "levered_beta_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("levered_beta", Float, nullable=False),
)

cost_of_equity_annual = Table(
    "cost_of_equity_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("cost_of_equity", Float, nullable=False),
)

wacc_annual = Table(
    "wacc_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("wacc", Float, nullable=False),
)

roic_wacc_spread_annual = Table(
    "roic_wacc_spread_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("spread_pct", Float, nullable=False),
)

cash_and_cash_equivalents_annual = Table(
    "cash_and_cash_equivalents_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("cash_and_cash_equivalents", Float, nullable=False),
)

cash_and_cash_equivalents_ttm = Table(
    "cash_and_cash_equivalents_ttm",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("as_of", Text, nullable=False),
    Column("cash_and_cash_equivalents", Float, nullable=False),
)

total_current_assets_annual = Table(
    "total_current_assets_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("total_current_assets", Float, nullable=False),
)

total_current_assets_ttm = Table(
    "total_current_assets_ttm",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("as_of", Text, nullable=False),
    Column("total_current_assets", Float, nullable=False),
)

current_debt_annual = Table(
    "current_debt_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("current_debt", Float, nullable=False),
)

current_debt_ttm = Table(
    "current_debt_ttm",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("as_of", Text, nullable=False),
    Column("current_debt", Float, nullable=False),
)

non_cash_working_capital_annual = Table(
    "non_cash_working_capital_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("non_cash_working_capital", Float, nullable=False),
)

revenue_yield_non_cash_working_capital_annual = Table(
    "revenue_yield_non_cash_working_capital_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("revenue_yield_ncwc", Float, nullable=False),
)

research_and_development_expense_annual = Table(
    "research_and_development_expense_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("research_and_development_expense", Float, nullable=False),
)

capital_expenditures_annual = Table(
    "capital_expenditures_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("capital_expenditures", Float, nullable=False),
)

depreciation_amortization_annual = Table(
    "depreciation_amortization_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("depreciation_amortization", Float, nullable=False),
)

net_debt_issued_paid_annual = Table(
    "net_debt_issued_paid_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("net_debt_issued_paid", Float, nullable=False),
)

fcfe_annual = Table(
    "fcfe_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("fcfe", Float),
)

fcff_annual = Table(
    "fcff_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("fcff", Float),
)

reinvestment_rate_annual = Table(
    "reinvestment_rate_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("reinvestment_rate", Float),
)

rd_spend_rate_annual = Table(
    "rd_spend_rate_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("rd_spend_rate", Float),
)

long_term_investments_annual = Table(
    "long_term_investments_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("long_term_investments", Float, nullable=False),
)

long_term_investments_ttm = Table(
    "long_term_investments_ttm",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("as_of", Text, nullable=False),
    Column("long_term_investments", Float, nullable=False),
)

capital_employed_annual = Table(
    "capital_employed_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("capital_employed", Float, nullable=False),
)

invested_capital_annual = Table(
    "invested_capital_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("invested_capital", Float, nullable=False),
)

shareholders_equity_annual = Table(
    "shareholders_equity_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("shareholders_equity", Float, nullable=False),
)

shareholders_equity_ttm = Table(
    "shareholders_equity_ttm",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("as_of", Text, nullable=False),
    Column("shareholders_equity", Float, nullable=False),
)

retained_earnings_annual = Table(
    "retained_earnings_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("retained_earnings", Float, nullable=False),
)

retained_earnings_ttm = Table(
    "retained_earnings_ttm",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("as_of", Text, nullable=False),
    Column("retained_earnings", Float, nullable=False),
)

comprehensive_income_annual = Table(
    "comprehensive_income_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("comprehensive_income", Float, nullable=False),
)

comprehensive_income_ttm = Table(
    "comprehensive_income_ttm",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("as_of", Text, nullable=False),
    Column("comprehensive_income", Float, nullable=False),
)

accumulated_profit_annual = Table(
    "accumulated_profit_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("accumulated_profit", Float, nullable=False),
)

total_equity_annual = Table(
    "total_equity_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("total_equity", Float, nullable=False),
)

average_equity_annual = Table(
    "average_equity_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("average_equity", Float, nullable=False),
)

roe_annual = Table(
    "roe_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("roe", Float, nullable=False),
)

roce_annual = Table(
    "roce_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("roce", Float, nullable=False),
)

interest_coverage_annual = Table(
    "interest_coverage_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("interest_coverage_ratio", Float, nullable=False),
)

interest_load_annual = Table(
    "interest_load_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("interest_load_pct", Float, nullable=False),
)

default_spread_annual = Table(
    "default_spread_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("default_spread", Float, nullable=False),
)

pre_tax_cost_of_debt_annual = Table(
    "pre_tax_cost_of_debt_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("pre_tax_cost_of_debt", Float, nullable=False),
)

price_change_annual = Table(
    "price_change_annual",
    metadata,
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
    Column("fiscal_year", Integer, primary_key=True),
    Column("price_change", Float, nullable=False),
)

company_groups = Table(
    "company_groups",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", Text, nullable=False, unique=True),
)

company_group_members = Table(
    "company_group_members",
    metadata,
    Column("group_id", Integer, ForeignKey("company_groups.id", ondelete="CASCADE"), primary_key=True),
    Column("company_id", Integer, ForeignKey("companies.id", ondelete="CASCADE"), primary_key=True),
)

risk_free_rates = Table(
    "risk_free_rates",
    metadata,
    Column("year", Integer, primary_key=True),
    Column("usa_rf", Float, nullable=False),
    Column("india_rf", Float, nullable=False),
    Column("china_rf", Float, nullable=False),
    Column("japan_rf", Float, nullable=False),
    Column("updated_at", Text),
)

index_annual_price_movement = Table(
    "index_annual_price_movement",
    metadata,
    Column("year", Integer, primary_key=True),
    Column("nasdaq_composite", Float, nullable=False),
    Column("sp500", Float, nullable=False),
    Column("updated_at", Text),
)

implied_equity_risk_premium_usa = Table(
    "implied_equity_risk_premium_usa",
    metadata,
    Column("year", Integer, primary_key=True),
    Column("implied_erp", Float, nullable=False),
    Column("notes", Text),
    Column("updated_at", Text),
)

country_risk_premium = Table(
    "country_risk_premium",
    metadata,
    Column("year", Integer, primary_key=True),
    Column("india", Float, nullable=False),
    Column("china", Float, nullable=False),
    Column("japan", Float, nullable=False),
    Column("us", Float, nullable=False),
    Column("uk", Float, nullable=False),
    Column("uae", Float, nullable=False),
    Column("updated_at", Text),
)

marginal_corporate_tax_rates = Table(
    "marginal_corporate_tax_rates",
    metadata,
    Column("country", Text, primary_key=True),
    Column("effective_rate", Float, nullable=False),
    Column("notes", Text),
    Column("updated_at", Text),
)

industry_betas = Table(
    "industry_betas",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_industry_bucket", Text, nullable=False),
    Column("mapped_sector", Text, nullable=False),
    Column("unlevered_beta", Float, nullable=False),
    Column("cash_adjusted_beta", Float, nullable=False),
    Column("updated_at", Text),
    UniqueConstraint("user_industry_bucket", "mapped_sector", name="uq_industry_betas_bucket_sector"),
)

growth_weight_factors = Table(
    "growth_weight_factors",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("factor", Text, nullable=False, unique=True),
    Column("weight", Float, nullable=False),
)

stddev_weight_factors = Table(
    "stddev_weight_factors",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("factor", Text, nullable=False, unique=True),
    Column("weight", Float, nullable=False),
)

ttc_assumptions = Table(
    "ttc_assumptions",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("section", Text, nullable=False),
    Column("metric", Text, nullable=False),
    Column("weight", Float, nullable=False),
    Column("threshold", Float, nullable=False),
    Column("units", Text, nullable=False, server_default=""),
    Column("sort_order", Integer, nullable=False),
    UniqueConstraint("section", "sort_order", name="uq_ttc_assumptions_section_order"),
)
