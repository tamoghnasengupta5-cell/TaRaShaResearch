from sqlalchemy.orm import declarative_base

import db_models

Base = declarative_base(metadata=db_models.metadata)

class Companies(Base):
    __table__ = db_models.companies

class CompanyGroups(Base):
    __table__ = db_models.company_groups

class CountryRiskPremium(Base):
    __table__ = db_models.country_risk_premium

class GrowthWeightFactors(Base):
    __table__ = db_models.growth_weight_factors

class ImpliedEquityRiskPremiumUsa(Base):
    __table__ = db_models.implied_equity_risk_premium_usa

class IndexAnnualPriceMovement(Base):
    __table__ = db_models.index_annual_price_movement

class IndustryBetas(Base):
    __table__ = db_models.industry_betas

class DcfValuationSettings(Base):
    __table__ = db_models.dcf_valuation_settings

class DcfIndustryValuationSettings(Base):
    __table__ = db_models.dcf_industry_valuation_settings

class DcfCompanyValuationSettings(Base):
    __table__ = db_models.dcf_company_valuation_settings

class MarginalCorporateTaxRates(Base):
    __table__ = db_models.marginal_corporate_tax_rates

class RiskFreeRates(Base):
    __table__ = db_models.risk_free_rates

class StddevWeightFactors(Base):
    __table__ = db_models.stddev_weight_factors

class TtcAssumptions(Base):
    __table__ = db_models.ttc_assumptions


class AccumulatedProfitAnnual(Base):
    __table__ = db_models.accumulated_profit_annual

class AverageEquityAnnual(Base):
    __table__ = db_models.average_equity_annual

class CapitalEmployedAnnual(Base):
    __table__ = db_models.capital_employed_annual

class CapitalExpendituresAnnual(Base):
    __table__ = db_models.capital_expenditures_annual

class CashAndCashEquivalentsAnnual(Base):
    __table__ = db_models.cash_and_cash_equivalents_annual

class CashAndCashEquivalentsTtm(Base):
    __table__ = db_models.cash_and_cash_equivalents_ttm

class SharesOutstandingBasicAnnual(Base):
    __table__ = db_models.shares_outstanding_basic_annual

class SharesOutstandingBasicTtm(Base):
    __table__ = db_models.shares_outstanding_basic_ttm

class CompanyGroupMembers(Base):
    __table__ = db_models.company_group_members

class ComprehensiveIncomeAnnual(Base):
    __table__ = db_models.comprehensive_income_annual

class ComprehensiveIncomeTtm(Base):
    __table__ = db_models.comprehensive_income_ttm

class CostOfEquityAnnual(Base):
    __table__ = db_models.cost_of_equity_annual

class CostOfRevenueAnnual(Base):
    __table__ = db_models.cost_of_revenue_annual

class CostOfRevenueTtm(Base):
    __table__ = db_models.cost_of_revenue_ttm

class CurrentDebtAnnual(Base):
    __table__ = db_models.current_debt_annual

class CurrentDebtTtm(Base):
    __table__ = db_models.current_debt_ttm

class DebtEquityAnnual(Base):
    __table__ = db_models.debt_equity_annual

class DefaultSpreadAnnual(Base):
    __table__ = db_models.default_spread_annual

class DepreciationAmortizationAnnual(Base):
    __table__ = db_models.depreciation_amortization_annual

class EbitdaAnnual(Base):
    __table__ = db_models.ebitda_annual

class EbitdaTtm(Base):
    __table__ = db_models.ebitda_ttm

class EbitAnnual(Base):
    __table__ = db_models.ebit_annual

class EbitTtm(Base):
    __table__ = db_models.ebit_ttm

class EffTaxRateAnnual(Base):
    __table__ = db_models.eff_tax_rate_annual

class EffTaxRateTtm(Base):
    __table__ = db_models.eff_tax_rate_ttm

class FcfeAnnual(Base):
    __table__ = db_models.fcfe_annual

class FcffAnnual(Base):
    __table__ = db_models.fcff_annual

class InterestCoverageAnnual(Base):
    __table__ = db_models.interest_coverage_annual

class InterestExpenseAnnual(Base):
    __table__ = db_models.interest_expense_annual

class InterestExpenseTtm(Base):
    __table__ = db_models.interest_expense_ttm

class InterestLoadAnnual(Base):
    __table__ = db_models.interest_load_annual

class InvestedCapitalAnnual(Base):
    __table__ = db_models.invested_capital_annual

class LeveredBetaAnnual(Base):
    __table__ = db_models.levered_beta_annual

class LongTermInvestmentsAnnual(Base):
    __table__ = db_models.long_term_investments_annual

class LongTermInvestmentsTtm(Base):
    __table__ = db_models.long_term_investments_ttm

class MarketCapitalizationAnnual(Base):
    __table__ = db_models.market_capitalization_annual

class LastClosePriceAnnual(Base):
    __table__ = db_models.last_close_price_annual

class LastClosePriceTtm(Base):
    __table__ = db_models.last_close_price_ttm

class NetDebtIssuedPaidAnnual(Base):
    __table__ = db_models.net_debt_issued_paid_annual

class NetIncomeAnnual(Base):
    __table__ = db_models.net_income_annual

class NetIncomeTtm(Base):
    __table__ = db_models.net_income_ttm

class NonCashWorkingCapitalAnnual(Base):
    __table__ = db_models.non_cash_working_capital_annual

class NopatAnnual(Base):
    __table__ = db_models.nopat_annual

class OpMarginAnnual(Base):
    __table__ = db_models.op_margin_annual

class OpMarginTtm(Base):
    __table__ = db_models.op_margin_ttm

class OperatingIncomeAnnual(Base):
    __table__ = db_models.operating_income_annual

class OperatingIncomeTtm(Base):
    __table__ = db_models.operating_income_ttm

class OperatingCashFlowAnnual(Base):
    __table__ = db_models.operating_cash_flow_annual

class OperatingCashFlowTtm(Base):
    __table__ = db_models.operating_cash_flow_ttm

class NetPpeAnnual(Base):
    __table__ = db_models.net_ppe_annual

class NetPpeTtm(Base):
    __table__ = db_models.net_ppe_ttm

class GoodwillAndIntangiblesAnnual(Base):
    __table__ = db_models.goodwill_and_intangibles_annual

class GoodwillAndIntangiblesTtm(Base):
    __table__ = db_models.goodwill_and_intangibles_ttm

class OtherLongTermAssetsAnnual(Base):
    __table__ = db_models.other_long_term_assets_annual

class OtherLongTermAssetsTtm(Base):
    __table__ = db_models.other_long_term_assets_ttm

class DeferredRevenueAnnual(Base):
    __table__ = db_models.deferred_revenue_annual

class DeferredRevenueTtm(Base):
    __table__ = db_models.deferred_revenue_ttm

class DeferredTaxLiabilitiesAnnual(Base):
    __table__ = db_models.deferred_tax_liabilities_annual

class DeferredTaxLiabilitiesTtm(Base):
    __table__ = db_models.deferred_tax_liabilities_ttm

class OtherLongTermLiabilitiesAnnual(Base):
    __table__ = db_models.other_long_term_liabilities_annual

class OtherLongTermLiabilitiesTtm(Base):
    __table__ = db_models.other_long_term_liabilities_ttm

class PreTaxCostOfDebtAnnual(Base):
    __table__ = db_models.pre_tax_cost_of_debt_annual

class PretaxIncomeAnnual(Base):
    __table__ = db_models.pretax_income_annual

class PretaxIncomeTtm(Base):
    __table__ = db_models.pretax_income_ttm

class PriceChangeAnnual(Base):
    __table__ = db_models.price_change_annual

class RdSpendRateAnnual(Base):
    __table__ = db_models.rd_spend_rate_annual

class ReinvestmentRateAnnual(Base):
    __table__ = db_models.reinvestment_rate_annual

class ResearchAndDevelopmentExpenseAnnual(Base):
    __table__ = db_models.research_and_development_expense_annual

class RetainedEarningsAnnual(Base):
    __table__ = db_models.retained_earnings_annual

class RetainedEarningsTtm(Base):
    __table__ = db_models.retained_earnings_ttm

class RevenueYieldNonCashWorkingCapitalAnnual(Base):
    __table__ = db_models.revenue_yield_non_cash_working_capital_annual

class RevenuesAnnual(Base):
    __table__ = db_models.revenues_annual

class RevenuesTtm(Base):
    __table__ = db_models.revenues_ttm

class SgaAnnual(Base):
    __table__ = db_models.sga_annual

class SgaTtm(Base):
    __table__ = db_models.sga_ttm

class RoceAnnual(Base):
    __table__ = db_models.roce_annual

class RoeAnnual(Base):
    __table__ = db_models.roe_annual

class RoicDirectUploadAnnual(Base):
    __table__ = db_models.roic_direct_upload_annual

class RoicWaccSpreadAnnual(Base):
    __table__ = db_models.roic_wacc_spread_annual

class ShareholdersEquityAnnual(Base):
    __table__ = db_models.shareholders_equity_annual

class ShareholdersEquityTtm(Base):
    __table__ = db_models.shareholders_equity_ttm

class ShortTermInvestmentsAnnual(Base):
    __table__ = db_models.short_term_investments_annual

class ShortTermInvestmentsTtm(Base):
    __table__ = db_models.short_term_investments_ttm

class AccountsReceivableAnnual(Base):
    __table__ = db_models.accounts_receivable_annual

class AccountsReceivableTtm(Base):
    __table__ = db_models.accounts_receivable_ttm

class InventoryAnnual(Base):
    __table__ = db_models.inventory_annual

class InventoryTtm(Base):
    __table__ = db_models.inventory_ttm

class AccountsPayableAnnual(Base):
    __table__ = db_models.accounts_payable_annual

class AccountsPayableTtm(Base):
    __table__ = db_models.accounts_payable_ttm

class TotalAssetsAnnual(Base):
    __table__ = db_models.total_assets_annual

class TotalAssetsTtm(Base):
    __table__ = db_models.total_assets_ttm

class TotalCurrentAssetsAnnual(Base):
    __table__ = db_models.total_current_assets_annual

class TotalCurrentAssetsTtm(Base):
    __table__ = db_models.total_current_assets_ttm

class TotalCurrentLiabilitiesAnnual(Base):
    __table__ = db_models.total_current_liabilities_annual

class TotalCurrentLiabilitiesTtm(Base):
    __table__ = db_models.total_current_liabilities_ttm

class TotalDebtAnnual(Base):
    __table__ = db_models.total_debt_annual

class TotalDebtTtm(Base):
    __table__ = db_models.total_debt_ttm

class TotalEquityAnnual(Base):
    __table__ = db_models.total_equity_annual

class TotalLongTermLiabilitiesAnnual(Base):
    __table__ = db_models.total_long_term_liabilities_annual

class TotalLongTermLiabilitiesTtm(Base):
    __table__ = db_models.total_long_term_liabilities_ttm

class WaccAnnual(Base):
    __table__ = db_models.wacc_annual

