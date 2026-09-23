export interface DataProviderEnv {
  TARASHA_DATA_API_URL?: string;
  TARASHA_DATA_API_KEY?: string;
}

interface DataCompanyRow {
  id: number;
  cik: string;
  name: string;
  ticker: string;
  exchange: string;
  country: "USA";
  industry_bucket: string;
}

interface DataFactRow {
  company_id: number;
  statement_key: "income" | "balance" | "cash" | "shares";
  fact_key: string;
  label: string;
  unit_kind: "amount" | "shares" | "ratio" | "per_share";
  fiscal_year: number;
  value: number;
}

interface DataIndustryFactRow {
  bucket_id: number;
  bucket_name: string;
  company_id: number;
  country: "USA";
  fact_key:
    | "revenue"
    | "costOfRevenue"
    | "sga"
    | "researchAndDevelopment"
    | "ebitda"
    | "depreciation"
    | "ebit"
    | "operatingIncome"
    | "interestExpense"
    | "pretaxIncome"
    | "netIncome"
    | "minorityInterestInEarnings"
    | "earningsFromDiscontinuedOperations"
    | "commonDividendsPaid"
    | "netIncomeToCommon"
    | "dilutedShares"
    | "eps"
    | "effectiveTaxRate"
    | "nonCashWorkingCapital"
    | "shareBasedCompensation"
    | "otherAdjustments"
    | "capex"
    | "netDebtIssuedPaid";
  fiscal_year: number;
  value: number;
}

interface DataMarketMetricRow {
  company_id: number;
  enterprise_value: number | null;
  enterprise_value_source: string | null;
  enterprise_value_as_of: string | null;
  enterprise_value_detail: string | null;
  trailing_pe: number | null;
  trailing_pe_source: string | null;
  trailing_pe_as_of: string | null;
  trailing_pe_detail: string | null;
  updated_at: string | null;
}

interface DataApiIndustryBucket {
  id: number | null;
  name: string;
}

interface DataApiNormalizedCoverage {
  available: boolean;
  data_access: "normalized";
  source: string;
  first_fiscal_year: number | null;
  latest_fiscal_year: number | null;
  annual_observation_count: number;
  metric_count: number;
  statements: Array<{
    statement: "income" | "balance" | "cash_flow";
    observation_count: number;
    metric_count: number;
    first_fiscal_year: number | null;
    latest_fiscal_year: number | null;
  }>;
}

interface DataApiSearchCompany {
  id?: number;
  cik: string;
  name: string;
  ticker: string | null;
  exchange: string | null;
  country?: string;
  industry_buckets?: DataApiIndustryBucket[];
  publication_status: string;
  normalized_coverage?: DataApiNormalizedCoverage;
}

interface DataApiCompany {
  id: number;
  cik: string;
  name: string;
  sic_description: string | null;
  reporting_currency: string;
  aliases: Array<{ ticker: string; exchange: string; is_current: boolean }>;
  updated_at: string;
}

interface DataApiFinancialItem {
  metric: string;
  metric_label: string;
  display: { value: string; unit: string };
  period: { type: string; end: string };
  provenance?: { source_url?: string | null };
}

interface DataApiFinancials {
  statement: "income" | "balance" | "cash_flow";
  metrics: Array<{ key: string; label: string; description?: string }>;
  items: DataApiFinancialItem[];
}

interface DataApiDataset {
  company: DataApiCompany;
  income: DataApiFinancials;
  balance: DataApiFinancials;
  cash_flow: DataApiFinancials;
}

interface DataApiConstituent {
  identifier: string;
  cik: string;
  name: string;
  ticker: string;
  exchange: string;
  country: "USA";
  industry_buckets: DataApiIndustryBucket[];
}

interface DataApiFiling {
  accession: string;
  form: string;
  filing_date: string | null;
  report_date: string | null;
  primary_document: string | null;
  source_url: string;
}

interface DataApiDiscoverDataset {
  company: DataApiCompany;
  business_segments?: DataApiBusinessSegments;
  revenue_offerings?: DataApiRevenueOfferings;
  operating_cost_structure?: DataApiCostStructure;
  cash_conversion?: DataApiCashConversion;
  stock_risk?: DataApiStockRisk;
  market_pricing?: DataApiMarketPricing;
  stock_history?: DataApiStockHistory;
  normalized_coverage: DataApiNormalizedCoverage;
  country: "USA";
  industry_buckets: DataApiIndustryBucket[];
  constituents_customized: boolean;
  constituents: DataApiConstituent[];
  datasets: DataApiDataset[];
  filings: DataApiFiling[];
}

type DataApiStockRiskRange = "1Y" | "3Y" | "5Y" | "7Y" | "10Y";

interface DataApiStockRiskSeries {
  key: "company" | "market" | "sector";
  name: string;
  symbol: string;
  role: string;
  source_url: string;
  start_date: string;
  end_date: string;
  trading_days: number;
  monthly_observations: number;
  annualized_volatility_percent: number | null;
  beta_to_market: number | null;
  maximum_drawdown_percent: number | null;
  worst_monthly_return_percent: number | null;
  best_monthly_return_percent: number | null;
  positive_months_percent: number | null;
  negative_months_percent: number | null;
  flat_months_percent: number | null;
  drawdown: Array<{ date: string; value: number }>;
  rolling_volatility: Array<{ date: string; value: number }>;
  monthly_distribution: Array<{
    key: string;
    label: string;
    months: number;
    percentage: number | null;
  }>;
}

interface DataApiStockRiskPeriod {
  range: DataApiStockRiskRange;
  start_date: string;
  end_date: string;
  series: DataApiStockRiskSeries[];
  risk_summary: string;
  drawdown_summary: string;
  positive_months_summary: string;
}

interface DataApiStockRisk {
  status: "available" | "unavailable";
  reason: string | null;
  symbol?: string | null;
  as_of?: string | null;
  available_ranges: DataApiStockRiskRange[];
  default_range?: DataApiStockRiskRange | null;
  sector_benchmark?: {
    name: string;
    symbol: string;
    selection_basis: string;
  } | null;
  periods: Partial<Record<DataApiStockRiskRange, DataApiStockRiskPeriod>>;
  source: {
    name: string;
    source_role: string;
    delayed: boolean;
    persisted: boolean;
    cost: string;
    price_basis: string;
  };
  methodology?: string;
  quality?: {
    company_observations?: number | null;
    market_observations?: number | null;
    sector_observations?: number | null;
    alignment?: string | null;
    warnings?: string[];
  };
}

type DataApiMarketPricingMetricKey = "pe" | "forward_pe" | "ev_ebitda" | "ev_ebit" | "price_sales" | "price_book" | "fcf_yield" | "peg";

interface DataApiMarketPricing {
  status: "available" | "unavailable";
  reason: string | null;
  symbol?: string | null;
  currency?: string | null;
  as_of?: string | null;
  market_state?: string | null;
  current_price?: number | null;
  current_metrics?: Array<{
    key: DataApiMarketPricingMetricKey;
    label: string;
    short_label: string;
    current: number;
    unit: "multiple" | "percent";
    basis: string;
  }>;
  windows?: Partial<Record<"5Y" | "10Y", {
    key: "5Y" | "10Y";
    years: number;
    start_date: string | null;
    end_date: string | null;
    observations: number;
    metrics: Array<{
      key: Exclude<DataApiMarketPricingMetricKey, "forward_pe" | "peg">;
      label: string;
      current: number;
      median: number;
      low: number;
      high: number;
      percentile: number;
      unit: "multiple" | "percent";
      observations: number;
    }>;
  }>>;
  valuation_read?: string | null;
  implied_expectations?: Array<{
    key: "revenue_growth" | "fcf_growth" | "operating_margin" | "discount_rate";
    label: string;
    value: number;
    detail: string;
  }>;
  peer_framework?: string;
  peers?: Array<{
    cik: string | null;
    name: string;
    symbol: string;
    is_company: boolean;
    price: number | null;
    pe: number | null;
    ev_ebitda: number | null;
    fcf_yield: number | null;
    basis: string;
  }>;
  treasury_comparison?: {
    fcf_yield: number | null;
    treasury_yield: number | null;
    spread: number | null;
    treasury_as_of: string | null;
    source_name: string;
    source_url: string;
    cost: string;
  };
  analyst_estimates?: {
    status: "available" | "unavailable";
    reason: string | null;
    base_case: number | null;
    bull_case: number | null;
    bear_case: number | null;
    analyst_count: number | null;
    current_price: number | null;
    current_vs_base_percent: number | null;
    source_name: string;
    source_url: string | null;
    cost: string;
  };
  takeaway?: string | null;
  source?: {
    financials: string;
    peer_framework: string;
    market_name: string;
    market_role: string;
    market_url: string;
    cost: string;
    persisted: boolean;
    price_basis: string;
  };
  methodology?: string;
  quality?: {
    warnings?: string[];
    history_observations?: number | null;
    peer_quotes?: number | null;
    analyst_consensus_available?: boolean;
  };
}

type DataApiStockHistoryRange = "1Y" | "3Y" | "5Y" | "10Y" | "Max";

interface DataApiStockHistory {
  status: "available" | "unavailable";
  reason: string | null;
  symbol?: string | null;
  currency?: string | null;
  as_of?: string | null;
  market_state?: string | null;
  available_ranges?: DataApiStockHistoryRange[];
  default_range?: DataApiStockHistoryRange | null;
  prices?: Array<{
    date: string;
    close: number;
    adjusted_close: number;
    volume: number | null;
  }>;
  fundamentals?: {
    revenue?: DataApiStockHistoryFundamental;
    eps?: DataApiStockHistoryFundamental;
    free_cash_flow?: DataApiStockHistoryFundamental;
  };
  events?: Array<{
    date: string;
    type: "earnings" | "filing" | "corporate_action" | "strategy";
    title: string;
    detail: string;
    source_url: string | null;
  }>;
  performance?: Partial<Record<DataApiStockHistoryRange, {
    range: DataApiStockHistoryRange;
    start_date: string;
    end_date: string;
    price_change_percent: number | null;
    total_return_percent: number | null;
    cagr_percent: number | null;
    sp500_total_return_percent: number | null;
    high: number | null;
    low: number | null;
  }>>;
  glance?: {
    current_price: number | null;
    market_cap: number | null;
    diluted_shares: number | null;
    average_daily_volume_3m: number | null;
  };
  strategy?: {
    status: "available" | "unavailable";
    reason: string | null;
    points: string[];
    source_name: string;
    source_date: string | null;
    source_url: string | null;
    cost: string;
    methodology: string;
  };
  source?: {
    financials: string;
    filings: string;
    market_name: string;
    market_role: string;
    market_url: string;
    benchmark_url: string;
    cost: string;
    persisted: boolean;
    price_basis: string;
  };
  methodology?: string;
  quality?: {
    warnings?: string[];
    raw_price_observations?: number | null;
    display_price_observations?: number | null;
    fundamental_years?: number[];
  };
}

interface DataApiStockHistoryFundamental {
  label: string;
  points: Array<{
    date: string;
    fiscal_year: number;
    value: number;
    source_url: string | null;
    unit: string | null;
  }>;
}

interface DataApiOfferingPoint {
  revenue_display: { value: string; unit: string };
  source_url?: string;
  period_end?: string;
}
interface DataApiRevenueSource {
  source_url?: string;
  accession?: string | null;
  concept?: string;
  role?: string;
}
interface DataApiOffering {
  id: string;
  label: string;
  history: Record<string, DataApiOfferingPoint>;
  ttm?: (DataApiOfferingPoint & { sources: Array<{ source_url: string }> }) | null;
  children?: DataApiOffering[];
}
interface DataApiAdjustmentPoint {
  value_display: { value: string; unit: string };
  coverage?: string;
  derivation?: string;
  sources?: DataApiRevenueSource[];
}
interface DataApiAdjustmentEntry {
  id: string;
  label: string;
  definition?: string;
  history: Record<string, DataApiAdjustmentPoint>;
  ttm?: DataApiAdjustmentPoint | null;
}
interface DataApiAdjustmentImpactPoint extends DataApiAdjustmentPoint {
  direction: "positive" | "negative" | "neutral";
  opening_balance_display?: { value: string; unit: string };
  closing_balance_display?: { value: string; unit: string };
}
interface DataApiAdjustmentImpact {
  id: string;
  label: string;
  definition?: string;
  history: Record<string, DataApiAdjustmentImpactPoint>;
  ttm?: DataApiAdjustmentImpactPoint | null;
}
interface DataApiAdjustedRevenuePoint {
  fiscal_year: number;
  period_type?: "ttm";
  period_end: string;
  reported_revenue_display: { value: string; unit: string };
  total_adjustment_display?: { value: string; unit: string } | null;
  adjusted_revenue_display?: { value: string; unit: string } | null;
  reported_growth_percent?: string | null;
  adjusted_growth_percent?: string | null;
  growth_difference_pp?: string | null;
  applied_adjustment_ids?: string[];
  sources?: DataApiRevenueSource[];
}
interface DataApiAdjustedRevenue {
  status: "available" | "unavailable";
  reason?: string | null;
  points?: DataApiAdjustedRevenuePoint[];
  methodology?: string;
}
interface DataApiRevenueOfferings {
  status?: "available" | "unavailable";
  axis?: string | null;
  offerings: DataApiOffering[];
  annual_totals: Record<string, DataApiOfferingPoint>;
  methodology?: string;
  adjustment_methodology?: string;
  adjustment_entries?: DataApiAdjustmentEntry[];
  revenue_adjustment_impacts?: DataApiAdjustmentImpact[];
  adjusted_revenue?: DataApiAdjustedRevenue;
  quality_warnings?: string[];
  reason?: string | null;
  ttm?: { period_end: string; revenue_display: { value: string; unit: string }; prior_year_revenue_display: { value: string; unit: string } | null; sources?: DataApiRevenueSource[]; prior_year_sources?: DataApiRevenueSource[] } | null;
}

interface Offering { id: string; label: string; revenueByPeriod: Record<string, number | null>; sourceByPeriod: Record<string, string>; sourcesByPeriod: Record<string, string[]>; children?: Offering[] }
export function revenueOfferingStory(payload: DataApiRevenueOfferings | undefined) {
  if (!payload) return undefined;
  const sourceUrls = (sources?: DataApiRevenueSource[]) => [
    ...new Set((sources ?? []).flatMap(source => source.source_url ? [source.source_url] : [])),
  ];
  const firstSource = (sources?: DataApiRevenueSource[]) => sourceUrls(sources)[0];
  function offering(item: DataApiOffering): Offering {
    const revenueByPeriod: Record<string, number | null> = {};
    const sourceByPeriod: Record<string, string> = {};
    const sourcesByPeriod: Record<string, string[]> = {};
    for (const [year, point] of Object.entries(item.history)) {
      revenueByPeriod[year] = finiteNumber(point.revenue_display.value);
      if (point.source_url) {
        sourceByPeriod[year] = point.source_url;
        sourcesByPeriod[year] = [point.source_url];
      }
    }
    if (item.ttm) {
      revenueByPeriod.TTM = finiteNumber(item.ttm.revenue_display.value);
      const sources = sourceUrls(item.ttm.sources);
      const source = sources[0];
      if (source) sourceByPeriod.TTM = source;
      if (sources.length) sourcesByPeriod.TTM = sources;
    }
    return { id: item.id, label: item.label, revenueByPeriod, sourceByPeriod, sourcesByPeriod, children: item.children?.map(offering) };
  }
  const revenue = finiteNumber(payload.ttm?.revenue_display.value);
  const totalSourceByPeriod = Object.fromEntries(Object.entries(payload.annual_totals).flatMap(([year, point]) => point.source_url ? [[year, point.source_url]] : []));
  const totalSourcesByPeriod: Record<string, string[]> = Object.fromEntries(Object.entries(payload.annual_totals).flatMap(([year, point]) => point.source_url ? [[year, [point.source_url]]] : []));
  const ttmSource = firstSource(payload.ttm?.sources);
  if (ttmSource) totalSourceByPeriod.TTM = ttmSource;
  const ttmSources = [
    ...new Set([
      ...sourceUrls(payload.ttm?.sources),
      ...sourceUrls(payload.ttm?.prior_year_sources),
    ]),
  ];
  if (ttmSources.length) totalSourcesByPeriod.TTM = ttmSources;
  return {
    status: payload.status,
    reason: payload.reason,
    offerings: payload.offerings.map(offering),
    offeringAxis: payload.axis,
    annualTotals: Object.fromEntries(Object.entries(payload.annual_totals).map(([year, point]) => [year, finiteNumber(point.revenue_display.value)])),
    annualPeriodEnds: Object.fromEntries(Object.entries(payload.annual_totals).flatMap(([year, point]) => point.period_end ? [[year, point.period_end]] : [])),
    totalSourceByPeriod,
    totalSourcesByPeriod,
    methodology: payload.methodology,
    adjustmentMethodology: payload.adjustment_methodology,
    adjustmentEntries: (payload.adjustment_entries ?? []).map(item => {
      const valueByPeriod: Record<string, number | null> = {};
      const sourceByPeriod: Record<string, string> = {};
      const sourcesByPeriod: Record<string, string[]> = {};
      const derivationByPeriod: Record<string, string> = {};
      const coverageByPeriod: Record<string, string> = {};
      for (const [period, point] of Object.entries(item.history)) {
        valueByPeriod[period] = finiteNumber(point.value_display.value);
        const sources = sourceUrls(point.sources);
        const source = sources[0];
        if (source) sourceByPeriod[period] = source;
        if (sources.length) sourcesByPeriod[period] = sources;
        if (point.derivation) derivationByPeriod[period] = point.derivation;
        if (point.coverage) coverageByPeriod[period] = point.coverage;
      }
      if (item.ttm) {
        valueByPeriod.TTM = finiteNumber(item.ttm.value_display.value);
        const sources = sourceUrls(item.ttm.sources);
        const source = sources[0];
        if (source) sourceByPeriod.TTM = source;
        if (sources.length) sourcesByPeriod.TTM = sources;
        if (item.ttm.derivation) derivationByPeriod.TTM = item.ttm.derivation;
        if (item.ttm.coverage) coverageByPeriod.TTM = item.ttm.coverage;
      }
      return { id: item.id, label: item.label, definition: item.definition ?? "Issuer-reported revenue disclosure.", valueByPeriod, sourceByPeriod, sourcesByPeriod, derivationByPeriod, coverageByPeriod };
    }),
    adjustmentImpacts: (payload.revenue_adjustment_impacts ?? []).map(item => {
      const valueByPeriod: Record<string, number | null> = {};
      const directionByPeriod: Record<string, "positive" | "negative" | "neutral"> = {};
      const openingBalanceByPeriod: Record<string, number | null> = {};
      const closingBalanceByPeriod: Record<string, number | null> = {};
      const sourcesByPeriod: Record<string, string[]> = {};
      const derivationByPeriod: Record<string, string> = {};
      const coverageByPeriod: Record<string, string> = {};
      for (const [period, point] of Object.entries(item.history)) {
        valueByPeriod[period] = finiteNumber(point.value_display.value);
        directionByPeriod[period] = point.direction;
        openingBalanceByPeriod[period] = finiteNumber(point.opening_balance_display?.value);
        closingBalanceByPeriod[period] = finiteNumber(point.closing_balance_display?.value);
        const sources = sourceUrls(point.sources);
        if (sources.length) sourcesByPeriod[period] = sources;
        if (point.derivation) derivationByPeriod[period] = point.derivation;
        if (point.coverage) coverageByPeriod[period] = point.coverage;
      }
      if (item.ttm) {
        const point = item.ttm;
        valueByPeriod.TTM = finiteNumber(point.value_display.value);
        directionByPeriod.TTM = point.direction;
        openingBalanceByPeriod.TTM = finiteNumber(point.opening_balance_display?.value);
        closingBalanceByPeriod.TTM = finiteNumber(point.closing_balance_display?.value);
        const sources = sourceUrls(point.sources);
        if (sources.length) sourcesByPeriod.TTM = sources;
        if (point.derivation) derivationByPeriod.TTM = point.derivation;
        if (point.coverage) coverageByPeriod.TTM = point.coverage;
      }
      return {
        id: item.id,
        label: item.label,
        definition: item.definition ?? "Source-backed annual movement in a revenue timing balance.",
        valueByPeriod,
        directionByPeriod,
        openingBalanceByPeriod,
        closingBalanceByPeriod,
        sourcesByPeriod,
        derivationByPeriod,
        coverageByPeriod,
      };
    }),
    adjustedRevenue: payload.adjusted_revenue ? {
      status: payload.adjusted_revenue.status,
      reason: payload.adjusted_revenue.reason ?? null,
      points: (payload.adjusted_revenue.points ?? []).flatMap(point => {
        const reportedRevenue = finiteNumber(point.reported_revenue_display.value);
        if (reportedRevenue === null) return [];
        return [{
          fiscalYear: point.fiscal_year,
          periodType: (point.period_type ?? "annual") as "annual" | "ttm",
          periodEnd: point.period_end,
          reportedRevenue,
          totalAdjustment: finiteNumber(point.total_adjustment_display?.value),
          adjustedRevenue: finiteNumber(point.adjusted_revenue_display?.value),
          reportedGrowthPercent: finiteNumber(point.reported_growth_percent),
          adjustedGrowthPercent: finiteNumber(point.adjusted_growth_percent),
          growthDifferencePp: finiteNumber(point.growth_difference_pp),
          appliedAdjustmentIds: point.applied_adjustment_ids ?? [],
          sourceUrls: sourceUrls(point.sources),
        }];
      }),
      methodology: payload.adjusted_revenue.methodology ?? "Only source-backed TaRaShaData adjustments are applied; missing balances are not estimated.",
    } : undefined,
    qualityWarnings: payload.quality_warnings ?? [],
    ttm: payload.ttm && revenue !== null ? { periodEnd: payload.ttm.period_end, revenue, priorYearRevenue: finiteNumber(payload.ttm.prior_year_revenue_display?.value) } : undefined,
  };
}

interface DataApiBusinessSegmentHistory {
  fiscal_year: number;
  period_start: string | null;
  period_end: string;
  revenue_display: { value: string; unit: string };
  accession: string | null;
  filed_date: string | null;
  source_url: string | null;
}

interface DataApiBusinessSegment {
  member: string;
  name: string;
  latest_revenue_display: { value: string; unit: string };
  percentage_of_total: string | null;
  yoy_growth_percent: string | null;
  three_year_cagr_percent: string | null;
  history: DataApiBusinessSegmentHistory[];
  accession: string | null;
  filed_date: string | null;
  source_url: string | null;
}

interface DataApiBusinessSegments {
  status: "available" | "undetermined" | "unavailable";
  reason: string | null;
  latest_fiscal_year: number | null;
  latest_period_end: string | null;
  reporting_currency: string;
  total_revenue_display: { value: string; unit: string } | null;
  segments: DataApiBusinessSegment[];
  summary: string | null;
  methodology: string;
}

interface DataApiCostStructureHistory {
  fiscal_year: number;
  period_end: string | null;
  value_per_hundred: string;
}

interface DataApiCostStructureLine {
  key: string;
  label: string;
  role: "revenue" | "expense" | "subtotal";
  latest_value_per_hundred: string | null;
  history: DataApiCostStructureHistory[];
  lineage: {
    kind?: string | null;
    concept?: string | null;
    derivation_method?: string | null;
    formula?: string | null;
    source_url?: string | null;
  };
}

interface DataApiCostStructure {
  status: "available" | "unavailable";
  reason: string | null;
  latest_fiscal_year: number | null;
  latest_period_end: string | null;
  reporting_currency: string;
  years: number[];
  lines: DataApiCostStructureLine[];
  summary: string | null;
  methodology: string;
  source_url: string | null;
}

interface DataApiCashConversionLineage {
  kind?: string | null;
  concept?: string | null;
  derivation_method?: string | null;
  formula?: string | null;
  source_url?: string | null;
}

interface DataApiCashConversionComponent {
  key: string;
  label: string;
  value: string;
  lineage: DataApiCashConversionLineage;
}

interface DataApiCashConversionBridgeLine {
  key: string;
  label: string;
  operation: "base" | "add" | "subtract" | "subtotal" | "total";
  value: string;
  formula: string | null;
  lineage: DataApiCashConversionLineage;
  components: DataApiCashConversionComponent[];
}

interface DataApiCashConversionTrendPoint {
  fiscal_year: number;
  period_end: string;
  ebit: string;
  tax_rate_percent: string;
  taxes_on_operating_profit: string;
  nopat: string;
  depreciation_and_amortization: string;
  working_capital_impact: string;
  working_capital_components: DataApiCashConversionComponent[];
  capital_expenditure: string;
  fcff: string;
  conversion_percent: string | null;
  revenue: string;
  fcff_revenue_percent: string | null;
}

interface DataApiCashConversion {
  status: "available" | "unavailable";
  reason: string | null;
  latest_fiscal_year: number | null;
  latest_period_end: string | null;
  reporting_currency: string;
  display_unit: string;
  years: number[];
  bridge: DataApiCashConversionBridgeLine[] | null;
  trend: DataApiCashConversionTrendPoint[];
  metrics: {
    fcff: string | null;
    prior_fiscal_year: number | null;
    prior_fcff: string | null;
    conversion_percent: string | null;
    prior_conversion_percent: string | null;
    growth_percent: string | null;
    prior_growth_percent: string | null;
    revenue_percent: string | null;
    prior_revenue_percent: string | null;
    cagr_percent: string | null;
  } | null;
  summary: string | null;
  takeaway: string | null;
  methodology: string;
  source_url: string | null;
}

interface YearValue {
  year: number;
  value: number;
}

interface DistributionObservation {
  label: string;
  value: number;
}

export interface GrowthStatistics {
  median: number | null;
  standardDeviation: number | null;
  observations: number;
  distribution: DistributionObservation[];
  startYear: number | null;
  endYear: number | null;
  startValue: number | null;
  endValue: number | null;
  totalChange: number | null;
}

const factDescriptions: Record<string, string> = {
  revenue: "Sales or operating revenue standardized by TaRaShaData.ai from issuer filings.",
  costOfRevenue: "Direct cost associated with reported revenue.",
  sga: "Selling, general and administrative expense.",
  ebitda: "Earnings before interest, tax, depreciation and amortization.",
  operatingIncome: "Profit from operations before interest and tax.",
  interestExpense: "Reported financing cost.",
  pretaxIncome: "Reported income before tax.",
  netIncome: "Reported profit after expenses and tax.",
  minorityInterestInEarnings: "Earnings attributable to non-controlling interests.",
  earningsFromDiscontinuedOperations: "Reported gain or loss from discontinued operations.",
  commonDividendsPaid: "Cash dividends paid to common shareholders.",
  netIncomeToCommon: "Reported net income attributable to common shareholders.",
  effectiveTaxRate: "Reported effective tax rate used to derive NOPAT.",
  nonCashWorkingCapital: "Current operating assets less current operating liabilities, excluding cash and current debt.",
  shareBasedCompensation: "Reported non-cash share-based compensation expense.",
  otherAdjustments: "Other reported cash-flow adjustments, retaining their source sign.",
  cash: "Cash and cash-equivalent balance.",
  shortTermInvestments: "Reported short-term investments.",
  accountsReceivable: "Amounts due from customers and other debtors.",
  inventory: "Reported inventory balance.",
  prepaidExpenses: "Reported prepaid expenses and other prepaid operating assets.",
  currentAssets: "Assets expected to turn into cash or be used in the operating cycle.",
  propertyPlantEquipment: "Reported property, plant and equipment, net of accumulated depreciation.",
  goodwill: "Reported goodwill from acquired businesses.",
  otherIntangibleAssets: "Reported intangible assets other than goodwill.",
  assets: "Total reported assets.",
  accountsPayable: "Amounts owed to suppliers and other creditors.",
  unearnedRevenueCurrent: "Revenue collected or billed before delivery and classified as current.",
  unearnedRevenueLongTerm: "Revenue collected or billed before delivery and classified as non-current.",
  currentDebt: "Borrowings classified as current.",
  longTermDebt: "Borrowings classified as non-current.",
  shortTermLeaseLiabilities: "Lease obligations classified as current.",
  longTermLeaseLiabilities: "Lease obligations classified as non-current.",
  shortTermBorrowings: "Short-term borrowings reported separately from the current portion of long-term debt.",
  currentPortionLongTermDebt: "Long-term borrowings due within the current period.",
  currentLiabilities: "Obligations classified as current.",
  longTermLiabilities: "Reported longer-term obligations.",
  totalDebt: "Total reported borrowings.",
  liabilities: "Total reported liabilities.",
  equity: "Reported shareholders’ equity.",
  commonStock: "Reported par or stated value of issued common stock.",
  additionalPaidInCapital: "Capital contributed by shareholders above common-stock par value.",
  retainedEarnings: "Cumulative earnings retained in the business after dividends.",
  comprehensiveIncome: "Accumulated other comprehensive income or loss reported in equity.",
  operatingCash: "Net cash generated or used by operations.",
  capex: "Reported capital expenditure.",
  depreciation: "Reported depreciation and amortization.",
  researchAndDevelopment: "Reported research and development expense.",
  ebit: "Earnings before interest and tax.",
  netDebtIssuedPaid: "Net debt issued or repaid during the year.",
  sharesOutstanding: "Reported basic shares outstanding.",
  marketCapitalization: "Reported year-end market capitalization.",
};

const statementLabels = {
  income: "Income statement",
  balance: "Balance sheet",
  cash: "Cash-flow statement",
  shares: "Share information",
} as const;

function requireConfiguration(env: DataProviderEnv): { base: string; key: string } {
  const base = String(env.TARASHA_DATA_API_URL ?? "").replace(/\/$/, "");
  const key = String(env.TARASHA_DATA_API_KEY ?? "").trim();
  if (!base) throw new Error("TaRaShaData.ai API provider is not configured.");
  return { base, key };
}

async function dataFetch<T>(env: DataProviderEnv, path: string, init?: RequestInit): Promise<T> {
  const { base, key } = requireConfiguration(env);
  const headers = new Headers(init?.headers);
  headers.set("Accept", "application/json");
  if (key) headers.set("Authorization", `Bearer ${key}`);
  let response: Response;
  try {
    response = await fetch(`${base}${path}`, {
      ...init,
      headers,
    });
  } catch {
    throw new Error("TaRaShaData.ai is not reachable. Verify that the configured TaRaShaData service is running, then try again.");
  }
  if (!response.ok) {
    const payload = await response.json().catch(() => ({})) as { detail?: string; error?: string };
    throw new Error(payload.detail || payload.error || `TaRaShaData.ai API returned ${response.status}.`);
  }
  return await response.json() as T;
}

export async function fetchDataCompanyLogo(env: DataProviderEnv, companyId: string): Promise<Response> {
  const identifier = companyId.replace(/^data-/, "").trim();
  if (!/^[0-9]{10}$/.test(identifier)) {
    throw new Error("Invalid TaRaShaData.ai company identifier for logo retrieval.");
  }
  const { base, key } = requireConfiguration(env);
  const headers = new Headers({ Accept: "image/svg+xml,image/png,image/webp,image/jpeg,image/*;q=0.8" });
  if (key) headers.set("Authorization", `Bearer ${key}`);
  let response: Response;
  try {
    response = await fetch(`${base}/v1/companies/${identifier}/logo`, { headers });
  } catch {
    throw new Error("TaRaShaData.ai is not reachable for company-logo retrieval.");
  }
  if (!response.ok) {
    const payload = await response.json().catch(() => ({})) as { detail?: string; error?: string };
    throw new Error(payload.detail || payload.error || `TaRaShaData.ai logo API returned ${response.status}.`);
  }
  const contentType = String(response.headers.get("content-type") || "").split(";", 1)[0];
  if (!contentType.startsWith("image/")) {
    throw new Error("TaRaShaData.ai returned an invalid company-logo content type.");
  }
  const content = await response.arrayBuffer();
  if (!content.byteLength || content.byteLength > 2_000_000) {
    throw new Error("TaRaShaData.ai returned an invalid company-logo asset size.");
  }
  const outputHeaders = new Headers({
    "cache-control": "public, max-age=86400, stale-while-revalidate=604800",
    "content-type": contentType,
    "x-content-type-options": "nosniff",
  });
  for (const name of ["etag", "last-modified", "x-tarashadata-logo-source"]) {
    const value = response.headers.get(name);
    if (value) outputHeaders.set(name, value);
  }
  return new Response(content, { status: 200, headers: outputHeaders });
}

export function dataProviderEnabled(env: { TARASHA_DATA_API_URL?: string }): boolean {
  return Boolean(String(env.TARASHA_DATA_API_URL ?? "").trim());
}

export async function searchDataCompanies(env: DataProviderEnv, query: string, country: "USA" | "India") {
  const safeQuery = query.replace(/[^a-zA-Z0-9 .&-]/g, "").trim().slice(0, 60);
  if (safeQuery.length < 2) return [];
  const params = new URLSearchParams({
    q: safeQuery,
    country,
    limit: "30",
  });
  const rows = await dataFetch<DataApiSearchCompany[]>(env, `/v1/companies/search?${params}`);
  if (rows.some((row) => row.ticker && !row.normalized_coverage)) {
    throw new Error(
      "TaRaShaData.ai is running an older Discover API contract. Restart the TaRaShaData service, then search again.",
    );
  }
  return rows
    .filter(
      (row): row is DataApiSearchCompany & {
        ticker: string;
        normalized_coverage: DataApiNormalizedCoverage;
      } => Boolean(row.ticker && row.normalized_coverage?.available),
    )
    .sort((left, right) => Number(left.ticker!.toLowerCase() !== safeQuery.toLowerCase()) - Number(right.ticker!.toLowerCase() !== safeQuery.toLowerCase()) || left.name.localeCompare(right.name))
    .map((row) => ({
      id: `data-${row.cik}`,
      cik: row.cik,
      name: row.name,
      ticker: row.ticker!,
      exchange: row.exchange || "",
      country: "USA" as const,
      provider: "TaRaShaData.ai API",
      industryBucket: row.industry_buckets?.[0]?.name,
      data_available: Number(row.normalized_coverage.available),
      data_access: "normalized" as const,
      firstFiscalYear: row.normalized_coverage.first_fiscal_year ?? undefined,
      latestFiscalYear: row.normalized_coverage.latest_fiscal_year ?? undefined,
    }));
}

function finiteNumber(value: string | number | null | undefined): number | null {
  if (value === null || value === undefined) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function revenueSegmentStory(payload: DataApiBusinessSegments | undefined, reportingCurrency: string) {
  const unavailable = {
    status: "unavailable" as const,
    reason: "TaRaShaData did not return a reported business-segment revenue breakdown for the selected years.",
    latestFiscalYear: null,
    latestPeriodEnd: null,
    reportingCurrency,
    displayUnit: `${reportingCurrency} millions`,
    totalRevenue: null,
    segments: [],
    summary: null,
    methodology: "Issuer-filed annual revenue facts carrying a business-segment XBRL dimension.",
  };
  if (!payload) return unavailable;
  const displayUnit = payload.total_revenue_display?.unit
    ?? payload.segments[0]?.latest_revenue_display.unit
    ?? `${payload.reporting_currency || reportingCurrency} millions`;
  return {
    status: payload.status,
    reason: payload.reason,
    latestFiscalYear: payload.latest_fiscal_year,
    latestPeriodEnd: payload.latest_period_end,
    reportingCurrency: payload.reporting_currency || reportingCurrency,
    displayUnit,
    totalRevenue: finiteNumber(payload.total_revenue_display?.value),
    segments: payload.segments.flatMap((segment) => {
      const latestRevenue = finiteNumber(segment.latest_revenue_display.value);
      if (latestRevenue === null) return [];
      return [{
        member: segment.member,
        name: segment.name,
        latestRevenue,
        percentageOfTotal: finiteNumber(segment.percentage_of_total),
        yoyGrowthPercent: finiteNumber(segment.yoy_growth_percent),
        threeYearCagrPercent: finiteNumber(segment.three_year_cagr_percent),
        history: segment.history.flatMap((point) => {
          const revenue = finiteNumber(point.revenue_display.value);
          return revenue === null ? [] : [{
            fiscalYear: point.fiscal_year,
            periodStart: point.period_start,
            periodEnd: point.period_end,
            revenue,
            accession: point.accession,
            filedDate: point.filed_date,
            sourceUrl: point.source_url,
          }];
        }),
        accession: segment.accession,
        filedDate: segment.filed_date,
        sourceUrl: segment.source_url,
      }];
    }),
    summary: payload.summary,
    methodology: payload.methodology,
  };
}

function costStructureStory(payload: DataApiCostStructure | undefined, reportingCurrency: string) {
  const unavailable = {
    status: "unavailable" as const,
    reason: "TaRaShaData did not return an annual operating cost structure for the selected years.",
    latestFiscalYear: null,
    latestPeriodEnd: null,
    reportingCurrency,
    years: [],
    lines: [],
    summary: null,
    methodology: "Normalized annual income-statement lines divided by reported revenue.",
    sourceUrl: null,
  };
  if (!payload) return unavailable;
  return {
    status: payload.status,
    reason: payload.reason,
    latestFiscalYear: payload.latest_fiscal_year,
    latestPeriodEnd: payload.latest_period_end,
    reportingCurrency: payload.reporting_currency || reportingCurrency,
    years: payload.years.filter(Number.isInteger),
    lines: payload.lines.map((line) => ({
      key: line.key,
      label: line.label,
      role: line.role,
      latestValuePerHundred: finiteNumber(line.latest_value_per_hundred),
      history: line.history.flatMap((point) => {
        const valuePerHundred = finiteNumber(point.value_per_hundred);
        return valuePerHundred === null ? [] : [{
          fiscalYear: point.fiscal_year,
          periodEnd: point.period_end,
          valuePerHundred,
        }];
      }),
      lineage: {
        kind: line.lineage.kind ?? null,
        concept: line.lineage.concept ?? null,
        derivationMethod: line.lineage.derivation_method ?? null,
        formula: line.lineage.formula ?? null,
        sourceUrl: line.lineage.source_url ?? null,
      },
    })),
    summary: payload.summary,
    methodology: payload.methodology,
    sourceUrl: payload.source_url,
  };
}

function cashConversionLineage(lineage: DataApiCashConversionLineage | undefined) {
  return {
    kind: lineage?.kind ?? null,
    concept: lineage?.concept ?? null,
    derivationMethod: lineage?.derivation_method ?? null,
    formula: lineage?.formula ?? null,
    sourceUrl: lineage?.source_url ?? null,
  };
}

function cashConversionComponents(components: DataApiCashConversionComponent[] | undefined) {
  return (components ?? []).flatMap((component) => {
    const value = finiteNumber(component.value);
    return value === null ? [] : [{
      key: component.key,
      label: component.label,
      value,
      lineage: cashConversionLineage(component.lineage),
    }];
  });
}

function cashConversionStory(payload: DataApiCashConversion | undefined, reportingCurrency: string) {
  const unavailable = {
    status: "unavailable" as const,
    reason: "TaRaShaData did not return a complete annual operating-profit-to-FCFF bridge for the selected years.",
    latestFiscalYear: null,
    latestPeriodEnd: null,
    reportingCurrency,
    displayUnit: `${reportingCurrency} millions`,
    years: [],
    bridge: [],
    trend: [],
    metrics: null,
    summary: null,
    takeaway: null,
    methodology: "Normalized annual TaRaShaData EBIT, tax, depreciation and amortization, working-capital cash effects, and capital expenditure; missing inputs are never estimated.",
    sourceUrl: null,
  };
  if (!payload) return unavailable;
  return {
    status: payload.status,
    reason: payload.reason,
    latestFiscalYear: payload.latest_fiscal_year,
    latestPeriodEnd: payload.latest_period_end,
    reportingCurrency: payload.reporting_currency || reportingCurrency,
    displayUnit: payload.display_unit || `${reportingCurrency} millions`,
    years: payload.years.filter(Number.isInteger),
    bridge: (payload.bridge ?? []).flatMap((line) => {
      const value = finiteNumber(line.value);
      return value === null ? [] : [{
        key: line.key,
        label: line.label,
        operation: line.operation,
        value,
        formula: line.formula,
        lineage: cashConversionLineage(line.lineage),
        components: cashConversionComponents(line.components),
      }];
    }),
    trend: payload.trend.flatMap((point) => {
      const ebit = finiteNumber(point.ebit);
      const taxRatePercent = finiteNumber(point.tax_rate_percent);
      const taxesOnOperatingProfit = finiteNumber(point.taxes_on_operating_profit);
      const nopat = finiteNumber(point.nopat);
      const depreciationAndAmortization = finiteNumber(point.depreciation_and_amortization);
      const workingCapitalImpact = finiteNumber(point.working_capital_impact);
      const capitalExpenditure = finiteNumber(point.capital_expenditure);
      const fcff = finiteNumber(point.fcff);
      const revenue = finiteNumber(point.revenue);
      if ([ebit, taxRatePercent, taxesOnOperatingProfit, nopat, depreciationAndAmortization, workingCapitalImpact, capitalExpenditure, fcff, revenue].some((value) => value === null)) return [];
      return [{
        fiscalYear: point.fiscal_year,
        periodEnd: point.period_end,
        ebit: ebit!,
        taxRatePercent: taxRatePercent!,
        taxesOnOperatingProfit: taxesOnOperatingProfit!,
        nopat: nopat!,
        depreciationAndAmortization: depreciationAndAmortization!,
        workingCapitalImpact: workingCapitalImpact!,
        workingCapitalComponents: cashConversionComponents(point.working_capital_components),
        capitalExpenditure: capitalExpenditure!,
        fcff: fcff!,
        conversionPercent: finiteNumber(point.conversion_percent),
        revenue: revenue!,
        fcffRevenuePercent: finiteNumber(point.fcff_revenue_percent),
      }];
    }),
    metrics: payload.metrics ? {
      fcff: finiteNumber(payload.metrics.fcff),
      priorFiscalYear: payload.metrics.prior_fiscal_year,
      priorFcff: finiteNumber(payload.metrics.prior_fcff),
      conversionPercent: finiteNumber(payload.metrics.conversion_percent),
      priorConversionPercent: finiteNumber(payload.metrics.prior_conversion_percent),
      growthPercent: finiteNumber(payload.metrics.growth_percent),
      priorGrowthPercent: finiteNumber(payload.metrics.prior_growth_percent),
      revenuePercent: finiteNumber(payload.metrics.revenue_percent),
      priorRevenuePercent: finiteNumber(payload.metrics.prior_revenue_percent),
      cagrPercent: finiteNumber(payload.metrics.cagr_percent),
    } : null,
    summary: payload.summary,
    takeaway: payload.takeaway,
    methodology: payload.methodology,
    sourceUrl: payload.source_url,
  };
}

const stockRiskDisplayRanges: DataApiStockRiskRange[] = ["1Y", "3Y", "5Y", "10Y"];
const stockRiskPeriodRanges: DataApiStockRiskRange[] = ["1Y", "3Y", "5Y", "7Y", "10Y"];

function stockRiskStory(payload: DataApiStockRisk | undefined) {
  const source = payload?.source ?? {
    name: "TaRaShaData market-history pipeline",
    source_role: "Zero-cost adjusted market-price history",
    delayed: true,
    persisted: false,
    cost: "$0",
    price_basis: "Daily adjusted close",
  };
  const periods = Object.fromEntries(stockRiskPeriodRanges.flatMap((range) => {
    const period = payload?.periods?.[range];
    if (!period) return [];
    return [[range, {
      range: period.range,
      startDate: period.start_date,
      endDate: period.end_date,
      series: period.series.map((series) => ({
        key: series.key,
        name: series.name,
        symbol: series.symbol,
        role: series.role,
        sourceUrl: series.source_url,
        startDate: series.start_date,
        endDate: series.end_date,
        tradingDays: series.trading_days,
        monthlyObservations: series.monthly_observations,
        annualizedVolatilityPercent: series.annualized_volatility_percent,
        betaToMarket: series.beta_to_market,
        maximumDrawdownPercent: series.maximum_drawdown_percent,
        worstMonthlyReturnPercent: series.worst_monthly_return_percent,
        bestMonthlyReturnPercent: series.best_monthly_return_percent,
        positiveMonthsPercent: series.positive_months_percent,
        negativeMonthsPercent: series.negative_months_percent,
        flatMonthsPercent: series.flat_months_percent,
        drawdown: series.drawdown.filter((point) => Number.isFinite(point.value)),
        rollingVolatility: series.rolling_volatility.filter((point) => Number.isFinite(point.value)),
        monthlyDistribution: series.monthly_distribution.map((bin) => ({
          key: bin.key,
          label: bin.label,
          months: bin.months,
          percentage: bin.percentage,
        })),
      })),
      riskSummary: period.risk_summary,
      drawdownSummary: period.drawdown_summary,
      positiveMonthsSummary: period.positive_months_summary,
    }]];
  }));
  return {
    status: payload?.status ?? "unavailable" as const,
    reason: payload?.reason ?? "TaRaShaData did not return sufficient aligned company and benchmark price history.",
    symbol: payload?.symbol ?? null,
    asOf: payload?.as_of ?? null,
    availableRanges: payload?.available_ranges?.filter((range) => stockRiskDisplayRanges.includes(range)) ?? [],
    defaultRange: payload?.default_range && stockRiskDisplayRanges.includes(payload.default_range) ? payload.default_range : null,
    sectorBenchmark: payload?.sector_benchmark ? {
      name: payload.sector_benchmark.name,
      symbol: payload.sector_benchmark.symbol,
      selectionBasis: payload.sector_benchmark.selection_basis,
    } : null,
    periods,
    source: {
      name: source.name,
      sourceRole: source.source_role,
      delayed: source.delayed,
      persisted: source.persisted,
      cost: source.cost,
      priceBasis: source.price_basis,
    },
    methodology: payload?.methodology ?? "No risk metric is estimated when aligned adjusted-price history is insufficient.",
    quality: payload?.quality ? {
      companyObservations: payload.quality.company_observations ?? null,
      marketObservations: payload.quality.market_observations ?? null,
      sectorObservations: payload.quality.sector_observations ?? null,
      alignment: payload.quality.alignment ?? null,
      warnings: payload.quality.warnings ?? [],
    } : null,
  };
}

function marketPricingStory(payload: DataApiMarketPricing | undefined) {
  const windows = Object.fromEntries((["5Y", "10Y"] as const).flatMap((key) => {
    const window = payload?.windows?.[key];
    if (!window) return [];
    return [[key, {
      key: window.key,
      years: window.years,
      startDate: window.start_date,
      endDate: window.end_date,
      observations: window.observations,
      metrics: window.metrics.filter((metric) => [metric.current, metric.median, metric.low, metric.high, metric.percentile].every(Number.isFinite)),
    }]];
  }));
  return {
    status: payload?.status ?? "unavailable" as const,
    reason: payload?.reason ?? "TaRaShaData did not return a valid delayed market-pricing snapshot.",
    symbol: payload?.symbol ?? null,
    currency: payload?.currency ?? null,
    asOf: payload?.as_of ?? null,
    marketState: payload?.market_state ?? null,
    currentPrice: finiteNumber(payload?.current_price),
    currentMetrics: (payload?.current_metrics ?? []).filter((metric) => Number.isFinite(metric.current)).map((metric) => ({
      key: metric.key,
      label: metric.label,
      shortLabel: metric.short_label,
      current: metric.current,
      unit: metric.unit,
      basis: metric.basis,
    })),
    windows,
    valuationRead: payload?.valuation_read ?? null,
    impliedExpectations: (payload?.implied_expectations ?? []).filter((item) => Number.isFinite(item.value)),
    peerFramework: payload?.peer_framework ?? "TaRaShaData SEC-SIC peer framework",
    peers: (payload?.peers ?? []).map((peer) => ({
      cik: peer.cik,
      name: peer.name,
      symbol: peer.symbol,
      isCompany: peer.is_company,
      price: finiteNumber(peer.price),
      pe: finiteNumber(peer.pe),
      evEbitda: finiteNumber(peer.ev_ebitda),
      fcfYield: finiteNumber(peer.fcf_yield),
      basis: peer.basis,
    })),
    treasuryComparison: payload?.treasury_comparison ? {
      fcfYield: finiteNumber(payload.treasury_comparison.fcf_yield),
      treasuryYield: finiteNumber(payload.treasury_comparison.treasury_yield),
      spread: finiteNumber(payload.treasury_comparison.spread),
      treasuryAsOf: payload.treasury_comparison.treasury_as_of,
      sourceName: payload.treasury_comparison.source_name,
      sourceUrl: payload.treasury_comparison.source_url,
      cost: payload.treasury_comparison.cost,
    } : null,
    analystEstimates: payload?.analyst_estimates ? {
      status: payload.analyst_estimates.status,
      reason: payload.analyst_estimates.reason,
      baseCase: finiteNumber(payload.analyst_estimates.base_case),
      bullCase: finiteNumber(payload.analyst_estimates.bull_case),
      bearCase: finiteNumber(payload.analyst_estimates.bear_case),
      analystCount: payload.analyst_estimates.analyst_count,
      currentPrice: finiteNumber(payload.analyst_estimates.current_price),
      currentVsBasePercent: finiteNumber(payload.analyst_estimates.current_vs_base_percent),
      sourceName: payload.analyst_estimates.source_name,
      sourceUrl: payload.analyst_estimates.source_url,
      cost: payload.analyst_estimates.cost,
    } : null,
    takeaway: payload?.takeaway ?? null,
    source: payload?.source ? {
      financials: payload.source.financials,
      peerFramework: payload.source.peer_framework,
      marketName: payload.source.market_name,
      marketRole: payload.source.market_role,
      marketUrl: payload.source.market_url,
      cost: payload.source.cost,
      persisted: payload.source.persisted,
      priceBasis: payload.source.price_basis,
    } : null,
    methodology: payload?.methodology ?? "No valuation metric is estimated when a required TaRaShaData financial denominator or delayed market price is unavailable.",
    quality: payload?.quality ? {
      warnings: payload.quality.warnings ?? [],
      historyObservations: payload.quality.history_observations ?? null,
      peerQuotes: payload.quality.peer_quotes ?? null,
      analystConsensusAvailable: payload.quality.analyst_consensus_available ?? false,
    } : null,
  };
}

const stockHistoryRanges: DataApiStockHistoryRange[] = ["1Y", "3Y", "5Y", "10Y", "Max"];

function stockHistoryFundamental(payload: DataApiStockHistoryFundamental | undefined, label: string) {
  return {
    label: payload?.label ?? label,
    points: (payload?.points ?? []).flatMap((point) => {
      const value = finiteNumber(point.value);
      if (value === null || !Number.isInteger(point.fiscal_year)) return [];
      return [{
        date: point.date,
        fiscalYear: point.fiscal_year,
        value,
        sourceUrl: point.source_url,
        unit: point.unit,
      }];
    }),
  };
}

function stockHistoryStory(payload: DataApiStockHistory | undefined) {
  const performance = Object.fromEntries(stockHistoryRanges.flatMap((range) => {
    const item = payload?.performance?.[range];
    if (!item) return [];
    return [[range, {
      range: item.range,
      startDate: item.start_date,
      endDate: item.end_date,
      priceChangePercent: finiteNumber(item.price_change_percent),
      totalReturnPercent: finiteNumber(item.total_return_percent),
      cagrPercent: finiteNumber(item.cagr_percent),
      sp500TotalReturnPercent: finiteNumber(item.sp500_total_return_percent),
      high: finiteNumber(item.high),
      low: finiteNumber(item.low),
    }]];
  }));
  const source = payload?.source;
  const strategy = payload?.strategy;
  return {
    status: payload?.status ?? "unavailable" as const,
    reason: payload?.reason ?? "TaRaShaData did not return usable historical price coverage.",
    symbol: payload?.symbol ?? null,
    currency: payload?.currency ?? null,
    asOf: payload?.as_of ?? null,
    marketState: payload?.market_state ?? null,
    availableRanges: payload?.available_ranges?.filter((range) => stockHistoryRanges.includes(range)) ?? [],
    defaultRange: payload?.default_range ?? null,
    prices: (payload?.prices ?? []).flatMap((point) => {
      const close = finiteNumber(point.close);
      const adjustedClose = finiteNumber(point.adjusted_close);
      if (close === null || adjustedClose === null) return [];
      return [{ date: point.date, close, adjustedClose, volume: finiteNumber(point.volume) }];
    }),
    fundamentals: {
      revenue: stockHistoryFundamental(payload?.fundamentals?.revenue, "Revenue"),
      eps: stockHistoryFundamental(payload?.fundamentals?.eps, "Earnings Per Share"),
      freeCashFlow: stockHistoryFundamental(payload?.fundamentals?.free_cash_flow, "Free Cash Flow"),
    },
    events: (payload?.events ?? []).map((event) => ({
      date: event.date,
      type: event.type,
      title: event.title,
      detail: event.detail,
      sourceUrl: event.source_url,
    })),
    performance,
    glance: {
      currentPrice: finiteNumber(payload?.glance?.current_price),
      marketCap: finiteNumber(payload?.glance?.market_cap),
      dilutedShares: finiteNumber(payload?.glance?.diluted_shares),
      averageDailyVolume3m: finiteNumber(payload?.glance?.average_daily_volume_3m),
    },
    strategy: {
      status: strategy?.status ?? "unavailable" as const,
      reason: strategy?.reason ?? "No source-specific management strategy was available.",
      points: strategy?.points ?? [],
      sourceName: strategy?.source_name ?? "TaRaShaData SEC filing pipeline",
      sourceDate: strategy?.source_date ?? null,
      sourceUrl: strategy?.source_url ?? null,
      cost: strategy?.cost ?? "$0",
      methodology: strategy?.methodology ?? "No strategy statement is generated without management source language.",
    },
    source: source ? {
      financials: source.financials,
      filings: source.filings,
      marketName: source.market_name,
      marketRole: source.market_role,
      marketUrl: source.market_url,
      benchmarkUrl: source.benchmark_url,
      cost: source.cost,
      persisted: source.persisted,
      priceBasis: source.price_basis,
    } : null,
    methodology: payload?.methodology ?? "No historical performance is estimated when source observations are unavailable.",
    quality: payload?.quality ? {
      warnings: payload.quality.warnings ?? [],
      rawPriceObservations: payload.quality.raw_price_observations ?? null,
      displayPriceObservations: payload.quality.display_price_observations ?? null,
      fundamentalYears: payload.quality.fundamental_years ?? [],
    } : null,
  };
}

function seriesFor(facts: DataFactRow[], key: string): YearValue[] {
  const byYear = new Map<number, number>();
  for (const fact of facts) {
    if (fact.fact_key === key && !byYear.has(fact.fiscal_year)) byYear.set(fact.fiscal_year, fact.value);
  }
  return [...byYear.entries()]
    .map(([year, value]) => ({ year, value }))
    .sort((left, right) => left.year - right.year);
}

function median(values: number[]): number | null {
  if (!values.length) return null;
  const sorted = [...values].sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
}

function sampleStandardDeviation(values: number[]): number | null {
  if (!values.length) return null;
  if (values.length === 1) return 0;
  const average = values.reduce((total, value) => total + value, 0) / values.length;
  return Math.sqrt(values.reduce((total, value) => total + ((value - average) ** 2), 0) / (values.length - 1));
}

export function calculateGrossOperatingLeverage(
  currentGrossProfit: number,
  priorGrossProfit: number,
  currentRevenue: number,
  priorRevenue: number,
): number | null {
  const revenueChange = currentRevenue - priorRevenue;
  return revenueChange === 0 ? null : ((currentGrossProfit - priorGrossProfit) / revenueChange) * 100;
}

export function growthStatistics(series: YearValue[]): GrowthStatistics {
  const sorted = [...series].sort((left, right) => left.year - right.year);
  const growthRates = growthRateObservations(sorted);
  const first = sorted[0];
  const last = sorted[sorted.length - 1];
  return growthStatisticsForRates(growthRates, first, last);
}

function growthRateObservations(series: YearValue[], owner = "Company"): DistributionObservation[] {
  const sorted = [...series].sort((left, right) => left.year - right.year);
  const growthRates: DistributionObservation[] = [];
  for (let index = 1; index < sorted.length; index += 1) {
    const previous = sorted[index - 1].value;
    if (sorted[index].year === sorted[index - 1].year + 1 && previous !== 0) {
      growthRates.push({
        label: `${owner} · FY ${sorted[index - 1].year}–${sorted[index].year}`,
        value: ((sorted[index].value - previous) / Math.abs(previous)) * 100,
      });
    }
  }
  return growthRates;
}

function grossOperatingLeverageObservations(
  revenue: YearValue[],
  grossProfit: YearValue[],
  owner = "Company",
): DistributionObservation[] {
  const revenueByYear = new Map(revenue.map((item) => [item.year, item.value]));
  const grossProfitByYear = new Map(grossProfit.map((item) => [item.year, item.value]));
  return [...revenueByYear.keys()].sort((left, right) => left - right).flatMap((year) => {
    const currentRevenue = revenueByYear.get(year);
    const priorRevenue = revenueByYear.get(year - 1);
    const currentGrossProfit = grossProfitByYear.get(year);
    const priorGrossProfit = grossProfitByYear.get(year - 1);
    if (currentRevenue === undefined || priorRevenue === undefined || currentGrossProfit === undefined || priorGrossProfit === undefined) return [];
    const value = calculateGrossOperatingLeverage(currentGrossProfit, priorGrossProfit, currentRevenue, priorRevenue);
    if (value === null) return [];
    return [{
      label: `${owner} · FY ${year - 1}–${year}`,
      value,
    }];
  });
}

function growthRatesForSeries(series: YearValue[]): number[] {
  return growthRateObservations(series).map((observation) => observation.value);
}

function growthStatisticsForRates(distribution: DistributionObservation[], first?: YearValue, last?: YearValue): GrowthStatistics {
  const growthRates = distribution.map((observation) => observation.value);
  return {
    median: median(growthRates),
    standardDeviation: sampleStandardDeviation(growthRates),
    observations: growthRates.length,
    distribution,
    startYear: first?.year ?? null,
    endYear: last?.year ?? null,
    startValue: first?.value ?? null,
    endValue: last?.value ?? null,
    totalChange: first && last && first.value !== 0 ? ((last.value - first.value) / Math.abs(first.value)) * 100 : null,
  };
}

function pooledIndustryGrowthStatistics(seriesByCompany: Map<number, YearValue[]>, companyLabels: Map<number, string>): GrowthStatistics {
  const growthRates = [...seriesByCompany.entries()].flatMap(([companyId, series]) => growthRateObservations(series, companyLabels.get(companyId) ?? `Bucket company ${companyId}`));
  return growthStatisticsForRates(growthRates);
}

function grossOperatingLeverageStatistics(revenue: YearValue[], grossProfit: YearValue[], owner = "Company"): GrowthStatistics {
  return growthStatisticsForRates(grossOperatingLeverageObservations(revenue, grossProfit, owner));
}

function pooledIndustryGrossOperatingLeverageStatistics(
  revenueByCompany: Map<number, YearValue[]>,
  grossProfitByCompany: Map<number, YearValue[]>,
  companyLabels: Map<number, string>,
): GrowthStatistics {
  const distribution = [...revenueByCompany.entries()].flatMap(([companyId, revenue]) => {
    const grossProfit = grossProfitByCompany.get(companyId);
    return grossProfit
      ? grossOperatingLeverageObservations(revenue, grossProfit, companyLabels.get(companyId) ?? `Bucket company ${companyId}`)
      : [];
  });
  return growthStatisticsForRates(distribution);
}

function derivedSeries(left: YearValue[], right: YearValue[], operation: (a: number, b: number) => number): YearValue[] {
  const rightByYear = new Map(right.map((item) => [item.year, item.value]));
  return left.filter((item) => rightByYear.has(item.year) && rightByYear.get(item.year) !== 0)
    .map((item) => ({ year: item.year, value: operation(item.value, rightByYear.get(item.year)!) }));
}

function deltaSeries(series: YearValue[]): YearValue[] {
  const sorted = [...series].sort((left, right) => left.year - right.year);
  const values: YearValue[] = [];
  for (let index = 1; index < sorted.length; index += 1) {
    if (sorted[index].year === sorted[index - 1].year + 1) {
      values.push({ year: sorted[index].year, value: sorted[index].value - sorted[index - 1].value });
    }
  }
  return values;
}

function percentChangeSeries(series: YearValue[]): YearValue[] {
  const sorted = [...series].sort((left, right) => left.year - right.year);
  const values: YearValue[] = [];
  for (let index = 1; index < sorted.length; index += 1) {
    const previous = sorted[index - 1];
    const current = sorted[index];
    if (current.year === previous.year + 1 && previous.value !== 0) {
      values.push({ year: current.year, value: ((current.value - previous.value) / Math.abs(previous.value)) * 100 });
    }
  }
  return values;
}

function nullableValueByYear(series: YearValue[], year: number): number | null {
  return series.find((item) => item.year === year)?.value ?? null;
}

function companyDeltaPoints(
  revenue: YearValue[],
  grossProfit: YearValue[],
  operatingIncome: YearValue[],
  fromYear: number,
  toYear: number,
) {
  const revenueDeltas = deltaSeries(revenue);
  const revenueChanges = percentChangeSeries(revenue);
  const grossProfitDeltas = deltaSeries(grossProfit);
  const grossProfitChanges = percentChangeSeries(grossProfit);
  const operatingIncomeDeltas = deltaSeries(operatingIncome);
  const operatingIncomeChanges = percentChangeSeries(operatingIncome);
  return Array.from({ length: Math.max(0, toYear - fromYear) }, (_, index) => {
    const year = fromYear + index + 1;
    const revenueDelta = nullableValueByYear(revenueDeltas, year);
    const grossProfitDelta = nullableValueByYear(grossProfitDeltas, year);
    return {
      fromYear: year - 1,
      toYear: year,
      revenue: revenueDelta,
      revenueChangePercent: nullableValueByYear(revenueChanges, year),
      grossProfit: grossProfitDelta,
      grossProfitChangePercent: nullableValueByYear(grossProfitChanges, year),
      grossOperatingLeverage: revenueDelta === null || revenueDelta === 0 || grossProfitDelta === null
        ? null
        : (grossProfitDelta / revenueDelta) * 100,
      operatingIncome: nullableValueByYear(operatingIncomeDeltas, year),
      operatingIncomeChangePercent: nullableValueByYear(operatingIncomeChanges, year),
    };
  });
}

function industryDeltaPoints(
  companySeries: YearValue[],
  industrySeriesByCompany: Map<number, YearValue[]>,
  fromYear: number,
  toYear: number,
) {
  const companyDeltas = deltaSeries(companySeries);
  const companyChanges = percentChangeSeries(companySeries);
  const peerDeltas = [...industrySeriesByCompany.values()].map(deltaSeries);
  const peerChanges = [...industrySeriesByCompany.values()].map(percentChangeSeries);
  return Array.from({ length: Math.max(0, toYear - fromYear) }, (_, index) => {
    const year = fromYear + index + 1;
    const industryValues = peerDeltas
      .map((series) => nullableValueByYear(series, year))
      .filter((value): value is number => value !== null);
    const industryChanges = peerChanges
      .map((series) => nullableValueByYear(series, year))
      .filter((value): value is number => value !== null);
    return {
      fromYear: year - 1,
      toYear: year,
      company: nullableValueByYear(companyDeltas, year),
      companyChangePercent: nullableValueByYear(companyChanges, year),
      industryMedian: median(industryValues),
      industryMedianChangePercent: median(industryChanges),
    };
  });
}

function rawIncomePoints(
  revenue: YearValue[],
  grossProfit: YearValue[],
  operatingIncome: YearValue[],
  fromYear: number,
  toYear: number,
) {
  const revenueByYear = new Map(revenue.map((item) => [item.year, item.value]));
  const grossProfitByYear = new Map(grossProfit.map((item) => [item.year, item.value]));
  const operatingIncomeByYear = new Map(operatingIncome.map((item) => [item.year, item.value]));
  const growth = (values: Map<number, number>, year: number): number | null => {
    const current = values.get(year);
    const previous = values.get(year - 1);
    return current === undefined || previous === undefined || previous === 0
      ? null
      : ((current - previous) / Math.abs(previous)) * 100;
  };
  const grossOperatingLeverage = (year: number): number | null => {
    const currentRevenue = revenueByYear.get(year);
    const priorRevenue = revenueByYear.get(year - 1);
    const currentGrossProfit = grossProfitByYear.get(year);
    const priorGrossProfit = grossProfitByYear.get(year - 1);
    if (currentRevenue === undefined || priorRevenue === undefined || currentGrossProfit === undefined || priorGrossProfit === undefined) return null;
    return calculateGrossOperatingLeverage(currentGrossProfit, priorGrossProfit, currentRevenue, priorRevenue);
  };
  return Array.from({ length: toYear - fromYear + 1 }, (_, index) => {
    const year = fromYear + index;
    return {
      year,
      revenue: revenueByYear.get(year) ?? null,
      revenueChangePercent: growth(revenueByYear, year),
      grossProfit: grossProfitByYear.get(year) ?? null,
      grossProfitChangePercent: growth(grossProfitByYear, year),
      grossOperatingLeverage: grossOperatingLeverage(year),
      operatingIncome: operatingIncomeByYear.get(year) ?? null,
      operatingIncomeChangePercent: growth(operatingIncomeByYear, year),
    };
  });
}

function groupIndustrySeries(facts: DataIndustryFactRow[], factKey: DataIndustryFactRow["fact_key"]): Map<number, YearValue[]> {
  const grouped = new Map<number, YearValue[]>();
  const seen = new Set<string>();
  for (const fact of facts) {
    if (fact.fact_key !== factKey) continue;
    const identity = `${fact.company_id}:${fact.fiscal_year}:${fact.fact_key}`;
    if (seen.has(identity)) continue;
    seen.add(identity);
    grouped.set(fact.company_id, [...(grouped.get(fact.company_id) ?? []), { year: fact.fiscal_year, value: fact.value }]);
  }
  return grouped;
}

function derivedIndustrySeries(
  left: Map<number, YearValue[]>,
  right: Map<number, YearValue[]>,
  operation: (a: number, b: number) => number,
): Map<number, YearValue[]> {
  const output = new Map<number, YearValue[]>();
  for (const [companyId, leftSeries] of left) {
    const rightSeries = right.get(companyId);
    if (rightSeries) output.set(companyId, derivedSeries(leftSeries, rightSeries, operation));
  }
  return output;
}

type ProfitabilityMetricKey = "grossMargin" | "operatingMargin" | "cogsRatio" | "sgaRatio" | "daRatio" | "rdRatio";
type Direction = "higher" | "lower";
type ProfitabilitySeries = Record<ProfitabilityMetricKey, YearValue[]>;
type IndustryProfitabilitySeries = Record<ProfitabilityMetricKey, Map<number, YearValue[]>>;
type ProfitabilityAbsoluteSeries = Record<ProfitabilityMetricKey, YearValue[]>;
type IndustryProfitabilityAbsoluteSeries = Record<ProfitabilityMetricKey, Map<number, YearValue[]>>;

const profitabilityMetricKeys: ProfitabilityMetricKey[] = ["grossMargin", "operatingMargin", "cogsRatio", "sgaRatio", "daRatio", "rdRatio"];
const profitabilityDirections: Record<ProfitabilityMetricKey, Direction> = {
  grossMargin: "higher",
  operatingMargin: "higher",
  cogsRatio: "lower",
  sgaRatio: "lower",
  daRatio: "lower",
  rdRatio: "lower",
};

function levelStatistics(series: YearValue[], owner = "Company") {
  const distribution = series
    .filter((item) => Number.isFinite(item.value))
    .map((item) => ({ label: `${owner} · FY ${item.year}`, value: item.value }));
  const values = distribution.map((item) => item.value);
  return { median: median(values), standardDeviation: sampleStandardDeviation(values), observations: values.length, distribution };
}

function percentile(values: number[], quantile: number): number | null {
  if (!values.length) return null;
  const sorted = [...values].sort((left, right) => left - right);
  const position = (sorted.length - 1) * quantile;
  const lower = Math.floor(position);
  const upper = Math.ceil(position);
  if (lower === upper) return sorted[lower];
  return sorted[lower] + ((sorted[upper] - sorted[lower]) * (position - lower));
}

function plausibleProfitabilityValue(metric: ProfitabilityMetricKey, value: number): boolean {
  if (!Number.isFinite(value)) return false;
  if (metric === "grossMargin" || metric === "operatingMargin") return value >= -100 && value <= 100;
  if (metric === "cogsRatio" || metric === "sgaRatio") return value >= 0 && value <= 200;
  return value >= 0 && value <= 100;
}

function performanceThresholds(values: number[], direction: Direction) {
  const finite = values.filter(Number.isFinite);
  return {
    lowerQuartile: percentile(finite, 0.25),
    median: percentile(finite, 0.5),
    upperQuartile: percentile(finite, 0.75),
    observations: finite.length,
    direction,
  };
}

function buildProfitabilitySeries(
  revenue: YearValue[],
  costOfRevenue: YearValue[],
  operatingIncome: YearValue[],
  sga: YearValue[],
  depreciation: YearValue[],
  researchAndDevelopment: YearValue[],
): ProfitabilitySeries {
  return {
    grossMargin: derivedSeries(costOfRevenue, revenue, (cost, sales) => ((sales - cost) / sales) * 100),
    operatingMargin: derivedSeries(operatingIncome, revenue, (income, sales) => (income / sales) * 100),
    cogsRatio: derivedSeries(costOfRevenue, revenue, (cost, sales) => (cost / sales) * 100),
    sgaRatio: derivedSeries(sga, revenue, (expense, sales) => (expense / sales) * 100),
    daRatio: derivedSeries(depreciation, revenue, (expense, sales) => (expense / sales) * 100),
    rdRatio: derivedSeries(researchAndDevelopment, revenue, (expense, sales) => (expense / sales) * 100),
  };
}

function buildIndustryProfitabilitySeries(
  revenue: Map<number, YearValue[]>,
  costOfRevenue: Map<number, YearValue[]>,
  operatingIncome: Map<number, YearValue[]>,
  sga: Map<number, YearValue[]>,
  depreciation: Map<number, YearValue[]>,
  researchAndDevelopment: Map<number, YearValue[]>,
): IndustryProfitabilitySeries {
  return {
    grossMargin: derivedIndustrySeries(costOfRevenue, revenue, (cost, sales) => ((sales - cost) / sales) * 100),
    operatingMargin: derivedIndustrySeries(operatingIncome, revenue, (income, sales) => (income / sales) * 100),
    cogsRatio: derivedIndustrySeries(costOfRevenue, revenue, (cost, sales) => (cost / sales) * 100),
    sgaRatio: derivedIndustrySeries(sga, revenue, (expense, sales) => (expense / sales) * 100),
    daRatio: derivedIndustrySeries(depreciation, revenue, (expense, sales) => (expense / sales) * 100),
    rdRatio: derivedIndustrySeries(researchAndDevelopment, revenue, (expense, sales) => (expense / sales) * 100),
  };
}

function profitabilityYearPoints(series: ProfitabilitySeries, absoluteSeries: ProfitabilityAbsoluteSeries, fromYear: number, toYear: number) {
  return Array.from({ length: toYear - fromYear + 1 }, (_, index) => {
    const year = fromYear + index;
    return {
      year,
      grossMargin: nullableValueByYear(series.grossMargin, year),
      grossProfit: nullableValueByYear(absoluteSeries.grossMargin, year),
      operatingMargin: nullableValueByYear(series.operatingMargin, year),
      operatingIncome: nullableValueByYear(absoluteSeries.operatingMargin, year),
      cogsRatio: nullableValueByYear(series.cogsRatio, year),
      cogs: nullableValueByYear(absoluteSeries.cogsRatio, year),
      sgaRatio: nullableValueByYear(series.sgaRatio, year),
      sga: nullableValueByYear(absoluteSeries.sgaRatio, year),
      daRatio: nullableValueByYear(series.daRatio, year),
      da: nullableValueByYear(absoluteSeries.daRatio, year),
      rdRatio: nullableValueByYear(series.rdRatio, year),
      rd: nullableValueByYear(absoluteSeries.rdRatio, year),
    };
  });
}

function industryLevelPoints(
  companySeries: YearValue[],
  companyAbsoluteSeries: YearValue[],
  industrySeries: Map<number, YearValue[]>,
  industryAbsoluteSeries: Map<number, YearValue[]>,
  fromYear: number,
  toYear: number,
) {
  return Array.from({ length: toYear - fromYear + 1 }, (_, index) => {
    const year = fromYear + index;
    const observed = [...industrySeries.values()]
      .map((series) => nullableValueByYear(series, year))
      .filter((value): value is number => value !== null && Number.isFinite(value));
    const observedAbsoluteValues = [...industryAbsoluteSeries.values()]
      .map((series) => nullableValueByYear(series, year))
      .filter((value): value is number => value !== null && Number.isFinite(value));
    return {
      year,
      companyValue: nullableValueByYear(companySeries, year),
      companyAbsoluteValue: nullableValueByYear(companyAbsoluteSeries, year),
      industryMedian: median(observed),
      industryMedianAbsoluteValue: median(observedAbsoluteValues),
    };
  });
}

function profitabilityAnalysis(
  companySeries: ProfitabilitySeries,
  companyAbsoluteSeries: ProfitabilityAbsoluteSeries,
  industrySeries: IndustryProfitabilitySeries,
  industryAbsoluteSeries: IndustryProfitabilityAbsoluteSeries,
  fromYear: number,
  toYear: number,
  companyLabels: Map<number, string>,
) {
  const statistics = Object.fromEntries(profitabilityMetricKeys.map((metric) => [metric, levelStatistics(companySeries[metric])])) as Record<ProfitabilityMetricKey, ReturnType<typeof levelStatistics>>;
  const industryStatistics = Object.fromEntries(profitabilityMetricKeys.map((metric) => {
    const distribution = [...industrySeries[metric].entries()].flatMap(([companyId, series]) => levelStatistics(series, companyLabels.get(companyId) ?? `Bucket company ${companyId}`).distribution);
    const values = distribution.map((observation) => observation.value);
    return [metric, { median: median(values), standardDeviation: sampleStandardDeviation(values), observations: values.length, distribution }];
  })) as Record<ProfitabilityMetricKey, ReturnType<typeof levelStatistics>>;
  const performanceBands = Object.fromEntries(profitabilityMetricKeys.map((metric) => {
    const peerSeries = [...industrySeries[metric].values()];
    const levelValues = peerSeries.flatMap((series) => series.map((item) => item.value)).filter((value) => plausibleProfitabilityValue(metric, value));
    const deviationValues = peerSeries
      .map((series) => levelStatistics(series).standardDeviation)
      .filter((value): value is number => value !== null && Number.isFinite(value));
    return [metric, {
      level: performanceThresholds(levelValues, profitabilityDirections[metric]),
      standardDeviation: performanceThresholds(deviationValues, "lower"),
    }];
  })) as Record<ProfitabilityMetricKey, { level: ReturnType<typeof performanceThresholds>; standardDeviation: ReturnType<typeof performanceThresholds> }>;
  return {
    statistics,
    industryStatistics,
    yearly: profitabilityYearPoints(companySeries, companyAbsoluteSeries, fromYear, toYear),
    industryComparisons: {
      grossMargin: industryLevelPoints(companySeries.grossMargin, companyAbsoluteSeries.grossMargin, industrySeries.grossMargin, industryAbsoluteSeries.grossMargin, fromYear, toYear),
      operatingMargin: industryLevelPoints(companySeries.operatingMargin, companyAbsoluteSeries.operatingMargin, industrySeries.operatingMargin, industryAbsoluteSeries.operatingMargin, fromYear, toYear),
      daRatio: industryLevelPoints(companySeries.daRatio, companyAbsoluteSeries.daRatio, industrySeries.daRatio, industryAbsoluteSeries.daRatio, fromYear, toYear),
      rdRatio: industryLevelPoints(companySeries.rdRatio, companyAbsoluteSeries.rdRatio, industrySeries.rdRatio, industryAbsoluteSeries.rdRatio, fromYear, toYear),
    },
    performanceBands,
  };
}

type EarningsFlowMetricKey =
  | "revenue"
  | "cogs"
  | "grossProfit"
  | "sga"
  | "researchAndDevelopment"
  | "otherOperatingExpense"
  | "ebitda"
  | "depreciationAndAmortization"
  | "ebit"
  | "interestExpense"
  | "ebt"
  | "taxes"
  | "netProfit"
  | "minorityInterestInEarnings"
  | "earningsFromDiscontinuedOperations"
  | "commonDividendsPaid"
  | "other"
  | "netIncomeToCommon"
  | "currentYearEarningsRetained"
  | "dilutedShares"
  | "eps";

type EarningsSeries = {
  revenue: YearValue[];
  cogs: YearValue[];
  sga: YearValue[];
  researchAndDevelopment: YearValue[];
  ebitda: YearValue[];
  depreciationAndAmortization: YearValue[];
  ebit: YearValue[];
  interestExpense: YearValue[];
  ebt: YearValue[];
  netProfit: YearValue[];
  minorityInterestInEarnings: YearValue[];
  earningsFromDiscontinuedOperations: YearValue[];
  commonDividendsPaid: YearValue[];
  netIncomeToCommon: YearValue[];
  dilutedShares: YearValue[];
  eps: YearValue[];
};

type ValuationMetricKey = "evRevenue" | "evGrossProfit" | "evEbitda" | "evEbit" | "pe";

const earningsFlowMetricKeys: EarningsFlowMetricKey[] = [
  "revenue",
  "cogs",
  "grossProfit",
  "sga",
  "researchAndDevelopment",
  "otherOperatingExpense",
  "ebitda",
  "depreciationAndAmortization",
  "ebit",
  "interestExpense",
  "ebt",
  "taxes",
  "netProfit",
  "minorityInterestInEarnings",
  "earningsFromDiscontinuedOperations",
  "commonDividendsPaid",
  "other",
  "netIncomeToCommon",
  "currentYearEarningsRetained",
  "dilutedShares",
  "eps",
];

function finiteValueForYear(series: YearValue[], year: number): number | null {
  const value = nullableValueByYear(series, year);
  return value !== null && Number.isFinite(value) ? value : null;
}

function zeroIfFloatingPointNoise(value: number): number {
  return Math.abs(value) < 1e-9 ? 0 : value;
}

function earningsFlowValues(series: EarningsSeries, year: number): Record<EarningsFlowMetricKey, number | null> {
  const revenue = finiteValueForYear(series.revenue, year);
  const cogs = finiteValueForYear(series.cogs, year);
  const sga = finiteValueForYear(series.sga, year);
  const rawResearchAndDevelopment = finiteValueForYear(series.researchAndDevelopment, year);
  // Treat tiny source artifacts as missing rather than displaying invented precision.
  const researchAndDevelopment = rawResearchAndDevelopment !== null && Math.abs(rawResearchAndDevelopment) > 0.0001
    ? rawResearchAndDevelopment
    : null;
  const ebitda = finiteValueForYear(series.ebitda, year);
  const depreciationAndAmortization = finiteValueForYear(series.depreciationAndAmortization, year);
  const ebit = finiteValueForYear(series.ebit, year);
  const interestExpense = finiteValueForYear(series.interestExpense, year);
  const ebt = finiteValueForYear(series.ebt, year);
  const netProfit = finiteValueForYear(series.netProfit, year);
  const rawMinorityInterest = finiteValueForYear(series.minorityInterestInEarnings, year);
  const earningsFromDiscontinuedOperations = finiteValueForYear(series.earningsFromDiscontinuedOperations, year);
  const rawCommonDividendsPaid = finiteValueForYear(series.commonDividendsPaid, year);
  const netIncomeToCommon = finiteValueForYear(series.netIncomeToCommon, year);
  const grossProfit = revenue !== null && cogs !== null ? revenue - cogs : null;
  const otherOperatingExpense = grossProfit !== null && sga !== null && ebitda !== null
    ? grossProfit - sga - (researchAndDevelopment ?? 0) - ebitda
    : null;
  const taxes = ebt !== null && netProfit !== null ? ebt - netProfit : null;
  // Fixed-deduction rows are positive amounts paired with a minus operator in the
  // UI. Discontinued operations and Other are signed contributions.
  const minorityInterestInEarnings = rawMinorityInterest === null ? null : Math.abs(rawMinorityInterest);
  const commonDividendsPaid = rawCommonDividendsPaid === null ? null : Math.abs(rawCommonDividendsPaid);
  const other = netProfit !== null && netIncomeToCommon !== null
    ? zeroIfFloatingPointNoise(netIncomeToCommon - netProfit
      + (minorityInterestInEarnings ?? 0)
      - (earningsFromDiscontinuedOperations ?? 0))
    : null;
  const currentYearEarningsRetained = netIncomeToCommon !== null
    ? netIncomeToCommon - (commonDividendsPaid ?? 0)
    : null;
  return {
    revenue,
    cogs,
    grossProfit,
    sga,
    researchAndDevelopment,
    otherOperatingExpense,
    ebitda,
    depreciationAndAmortization,
    ebit,
    interestExpense,
    ebt,
    taxes,
    netProfit,
    minorityInterestInEarnings,
    earningsFromDiscontinuedOperations,
    commonDividendsPaid,
    other,
    netIncomeToCommon,
    currentYearEarningsRetained,
    dilutedShares: finiteValueForYear(series.dilutedShares, year),
    eps: finiteValueForYear(series.eps, year),
  };
}

function industryEarningsSeries(
  industryFacts: DataIndustryFactRow[],
  grouped: Record<EarningsSeriesKey, Map<number, YearValue[]>>,
): Map<number, EarningsSeries> {
  const companyIds = [...new Set(industryFacts.map((fact) => fact.company_id))];
  return new Map(companyIds.map((companyId) => [companyId, {
    revenue: grouped.revenue.get(companyId) ?? [],
    cogs: grouped.cogs.get(companyId) ?? [],
    sga: grouped.sga.get(companyId) ?? [],
    researchAndDevelopment: grouped.researchAndDevelopment.get(companyId) ?? [],
    ebitda: grouped.ebitda.get(companyId) ?? [],
    depreciationAndAmortization: grouped.depreciationAndAmortization.get(companyId) ?? [],
    ebit: grouped.ebit.get(companyId) ?? [],
    interestExpense: grouped.interestExpense.get(companyId) ?? [],
    ebt: grouped.ebt.get(companyId) ?? [],
    netProfit: grouped.netProfit.get(companyId) ?? [],
    minorityInterestInEarnings: grouped.minorityInterestInEarnings.get(companyId) ?? [],
    earningsFromDiscontinuedOperations: grouped.earningsFromDiscontinuedOperations.get(companyId) ?? [],
    commonDividendsPaid: grouped.commonDividendsPaid.get(companyId) ?? [],
    netIncomeToCommon: grouped.netIncomeToCommon.get(companyId) ?? [],
    dilutedShares: grouped.dilutedShares.get(companyId) ?? [],
    eps: grouped.eps.get(companyId) ?? [],
  }]));
}

type EarningsSeriesKey = keyof EarningsSeries;

function earningsFlowAnalysis(
  companySeries: EarningsSeries,
  peers: Map<number, EarningsSeries>,
  fromYear: number,
  toYear: number,
) {
  return Array.from({ length: toYear - fromYear + 1 }, (_, index) => {
    const year = fromYear + index;
    const companyValues = earningsFlowValues(companySeries, year);
    const peerValues = [...peers.values()].map((series) => earningsFlowValues(series, year));
    const marginMetrics = new Set<EarningsFlowMetricKey>(["grossProfit", "ebitda", "ebit", "ebt", "netProfit"]);
    const metrics = Object.fromEntries(earningsFlowMetricKeys.map((metric) => {
      const observations = peerValues
        .map((values) => values[metric])
        .filter((value): value is number => value !== null && Number.isFinite(value));
      const companyRevenue = companyValues.revenue;
      const companyMetric = companyValues[metric];
      const companyMarginPercent = marginMetrics.has(metric)
        && companyMetric !== null && companyRevenue !== null && companyRevenue !== 0
        ? (companyMetric / companyRevenue) * 100
        : null;
      const peerMargins = marginMetrics.has(metric)
        ? peerValues.map((values) => {
          const value = values[metric];
          return value !== null && values.revenue !== null && values.revenue !== 0
            ? (value / values.revenue) * 100
            : null;
        }).filter((value): value is number => value !== null && Number.isFinite(value))
        : [];
      return [metric, {
        companyValue: companyValues[metric],
        industryMedian: median(observations),
        industryObservations: observations.length,
        companyMarginPercent,
        industryMedianMarginPercent: median(peerMargins),
      }];
    })) as Record<EarningsFlowMetricKey, {
      companyValue: number | null;
      industryMedian: number | null;
      industryObservations: number;
      companyMarginPercent: number | null;
      industryMedianMarginPercent: number | null;
    }>;
    // Independent medians do not necessarily add up. Make the displayed industry
    // "Other" the residual of the displayed medians so its bridge ties exactly.
    const medianNetProfit = metrics.netProfit.industryMedian;
    const medianNetIncomeToCommon = metrics.netIncomeToCommon.industryMedian;
    const medianMinorityInterest = metrics.minorityInterestInEarnings.industryMedian;
    const medianDiscontinuedOperations = metrics.earningsFromDiscontinuedOperations.industryMedian;
    const medianCommonDividends = metrics.commonDividendsPaid.industryMedian;
    if (medianNetProfit !== null && medianNetIncomeToCommon !== null) {
      metrics.other.industryMedian = zeroIfFloatingPointNoise(medianNetIncomeToCommon - medianNetProfit
        + (medianMinorityInterest ?? 0)
        - (medianDiscontinuedOperations ?? 0));
      metrics.currentYearEarningsRetained.industryMedian = medianNetIncomeToCommon - (medianCommonDividends ?? 0);
    }
    return { year, metrics };
  });
}

function positiveRatio(numerator: number | null, denominator: number | null): number | null {
  return numerator !== null && denominator !== null && numerator > 0 && denominator > 0
    ? numerator / denominator
    : null;
}

function valuationAnalysis(
  companySeries: EarningsSeries,
  peers: Map<number, EarningsSeries>,
  marketByCompany: Map<number, DataMarketMetricRow>,
  companyId: number,
  fromYear: number,
  toYear: number,
) {
  const denominatorYear = [...companySeries.revenue]
    .map((item) => item.year)
    .filter((year) => year >= fromYear && year <= toYear)
    .sort((left, right) => right - left)[0] ?? null;
  const companyMarket = marketByCompany.get(companyId);
  const enterpriseValue = companyMarket?.enterprise_value ?? null;
  const companyFlow = denominatorYear === null ? null : earningsFlowValues(companySeries, denominatorYear);
  const companyComparisons: Record<ValuationMetricKey, number | null> = {
    evRevenue: positiveRatio(enterpriseValue, companyFlow?.revenue ?? null),
    evGrossProfit: positiveRatio(enterpriseValue, companyFlow?.grossProfit ?? null),
    evEbitda: positiveRatio(enterpriseValue, companyFlow?.ebitda ?? null),
    evEbit: positiveRatio(enterpriseValue, companyFlow?.ebit ?? null),
    pe: companyMarket?.trailing_pe !== null && companyMarket?.trailing_pe !== undefined && companyMarket.trailing_pe > 0
      ? companyMarket.trailing_pe
      : null,
  };
  const peerComparisons = [...peers.entries()].map(([peerId, peerSeries]) => {
    const peerMarket = marketByCompany.get(peerId);
    const peerFlow = denominatorYear === null ? null : earningsFlowValues(peerSeries, denominatorYear);
    return {
      evRevenue: positiveRatio(peerMarket?.enterprise_value ?? null, peerFlow?.revenue ?? null),
      evGrossProfit: positiveRatio(peerMarket?.enterprise_value ?? null, peerFlow?.grossProfit ?? null),
      evEbitda: positiveRatio(peerMarket?.enterprise_value ?? null, peerFlow?.ebitda ?? null),
      evEbit: positiveRatio(peerMarket?.enterprise_value ?? null, peerFlow?.ebit ?? null),
      pe: peerMarket?.trailing_pe !== null && peerMarket?.trailing_pe !== undefined && peerMarket.trailing_pe > 0
        ? peerMarket.trailing_pe
        : null,
    };
  });
  const valuationMetricKeys: ValuationMetricKey[] = ["evRevenue", "evGrossProfit", "evEbitda", "evEbit", "pe"];
  const comparisons = Object.fromEntries(valuationMetricKeys.map((metric) => {
    const observations = peerComparisons
      .map((values) => values[metric])
      .filter((value): value is number => value !== null && Number.isFinite(value));
    return [metric, {
      companyValue: companyComparisons[metric],
      industryMedian: median(observations),
      industryObservations: observations.length,
    }];
  })) as Record<ValuationMetricKey, { companyValue: number | null; industryMedian: number | null; industryObservations: number }>;
  return {
    denominatorYear,
    enterpriseValue,
    enterpriseValueAsOf: companyMarket?.enterprise_value_as_of ?? null,
    enterpriseValueSource: companyMarket?.enterprise_value_source ?? "unavailable",
    enterpriseValueDetail: companyMarket?.enterprise_value_detail ?? "TaRaShaData.ai does not currently publish a governed company-level Enterprise Value snapshot.",
    comparisons,
  };
}

export interface CashFlowSeriesInput {
  revenue: YearValue[];
  ebit: YearValue[];
  effectiveTaxRate: YearValue[];
  depreciationAndAmortization: YearValue[];
  capitalExpenditure: YearValue[];
  nonCashWorkingCapital: YearValue[];
  netIncomeToCommon: YearValue[];
  shareBasedCompensation: YearValue[];
  otherAdjustments: YearValue[];
  netBorrowing: YearValue[];
  currentAssets?: YearValue[];
  cashAndCashEquivalents?: YearValue[];
  currentLiabilities?: YearValue[];
  currentDebt?: YearValue[];
  shortTermBorrowings?: YearValue[];
  currentPortionLongTermDebt?: YearValue[];
}

type FcffBridgeMetricKey = "ebit" | "nopat" | "depreciationAndAmortization" | "capitalExpenditure" | "workingCapitalImpact" | "fcff";
type FcfeBridgeMetricKey = "netIncomeToCommon" | "depreciationAndAmortization" | "shareBasedCompensation" | "otherAdjustments" | "capitalExpenditure" | "workingCapitalImpact" | "netBorrowing" | "fcfe";
type CashFlowMetricSnapshot = { value: number | null; percent: number | null };
type WorkingCapitalPeriodSnapshot = {
  year: number;
  currentAssets: number | null;
  cashAndCashEquivalents: number | null;
  netCurrentAssets: number | null;
  currentLiabilities: number | null;
  shortTermBorrowings: number | null;
  currentPortionLongTermDebt: number | null;
  otherInterestBearingCurrentDebt: number | null;
  totalInterestBearingCurrentDebt: number | null;
  netCurrentLiabilities: number | null;
  netOperatingWorkingCapital: number | null;
  debtBreakdownStatus: "reported-components" | "partially-reported" | "aggregate-only" | "components-only" | "unavailable";
};
type CashFlowYearSnapshot = {
  effectiveTaxRatePercent: number | null;
  workingCapital: {
    previousYear: WorkingCapitalPeriodSnapshot;
    currentYear: WorkingCapitalPeriodSnapshot;
    netChangeInWorkingCapital: number | null;
    cashImpact: number | null;
    cashEffect: "inflow" | "outflow" | "neutral" | "unavailable";
  };
  fcff: Record<FcffBridgeMetricKey, CashFlowMetricSnapshot>;
  fcfe: Record<FcfeBridgeMetricKey, CashFlowMetricSnapshot>;
};

function normalizedTaxRate(value: number | null): number | null {
  if (value === null || !Number.isFinite(value)) return null;
  return Math.abs(value) > 1 ? value / 100 : value;
}

function percentageOf(value: number | null, denominator: number | null): number | null {
  return value !== null && denominator !== null && denominator !== 0 && Number.isFinite(value) && Number.isFinite(denominator)
    ? (value / Math.abs(denominator)) * 100
    : null;
}

function workingCapitalPeriodSnapshot(series: CashFlowSeriesInput, year: number): WorkingCapitalPeriodSnapshot {
  const currentAssets = finiteValueForYear(series.currentAssets ?? [], year);
  const cashAndCashEquivalents = finiteValueForYear(series.cashAndCashEquivalents ?? [], year);
  const currentLiabilities = finiteValueForYear(series.currentLiabilities ?? [], year);
  const aggregateCurrentDebt = finiteValueForYear(series.currentDebt ?? [], year);
  const shortTermBorrowings = finiteValueForYear(series.shortTermBorrowings ?? [], year);
  const currentPortionLongTermDebt = finiteValueForYear(series.currentPortionLongTermDebt ?? [], year);
  const reportedComponents = [shortTermBorrowings, currentPortionLongTermDebt].filter((value) => value !== null).length;
  const componentTotal = (shortTermBorrowings ?? 0) + (currentPortionLongTermDebt ?? 0);
  const totalInterestBearingCurrentDebt = aggregateCurrentDebt ?? (reportedComponents ? componentTotal : null);
  const otherInterestBearingCurrentDebt = aggregateCurrentDebt !== null
    ? aggregateCurrentDebt - componentTotal
    : reportedComponents === 2
      ? 0
      : null;
  const debtBreakdownStatus: WorkingCapitalPeriodSnapshot["debtBreakdownStatus"] = aggregateCurrentDebt !== null
    ? reportedComponents === 2
      ? "reported-components"
      : reportedComponents === 1
        ? "partially-reported"
        : "aggregate-only"
    : reportedComponents
      ? "components-only"
      : "unavailable";
  const netCurrentAssets = currentAssets !== null && cashAndCashEquivalents !== null
    ? currentAssets - cashAndCashEquivalents
    : null;
  const netCurrentLiabilities = currentLiabilities !== null && totalInterestBearingCurrentDebt !== null
    ? currentLiabilities - totalInterestBearingCurrentDebt
    : null;
  const calculatedWorkingCapital = netCurrentAssets !== null && netCurrentLiabilities !== null
    ? netCurrentAssets - netCurrentLiabilities
    : null;
  const storedWorkingCapital = finiteValueForYear(series.nonCashWorkingCapital, year);

  return {
    year,
    currentAssets,
    cashAndCashEquivalents,
    netCurrentAssets,
    currentLiabilities,
    shortTermBorrowings,
    currentPortionLongTermDebt,
    otherInterestBearingCurrentDebt,
    totalInterestBearingCurrentDebt,
    netCurrentLiabilities,
    netOperatingWorkingCapital: calculatedWorkingCapital ?? storedWorkingCapital,
    debtBreakdownStatus,
  };
}

function cashFlowSnapshot(series: CashFlowSeriesInput, year: number): CashFlowYearSnapshot {
  const revenue = finiteValueForYear(series.revenue, year);
  const ebit = finiteValueForYear(series.ebit, year);
  const effectiveTaxRate = normalizedTaxRate(finiteValueForYear(series.effectiveTaxRate, year));
  const da = finiteValueForYear(series.depreciationAndAmortization, year);
  const rawCapex = finiteValueForYear(series.capitalExpenditure, year);
  const capex = rawCapex === null ? null : Math.abs(rawCapex);
  const currentNcwc = finiteValueForYear(series.nonCashWorkingCapital, year);
  const priorNcwc = finiteValueForYear(series.nonCashWorkingCapital, year - 1);
  const workingCapitalImpact = currentNcwc !== null && priorNcwc !== null ? priorNcwc - currentNcwc : null;
  const netChangeInWorkingCapital = workingCapitalImpact === null ? null : -workingCapitalImpact;
  const previousWorkingCapital = workingCapitalPeriodSnapshot(series, year - 1);
  const currentWorkingCapital = workingCapitalPeriodSnapshot(series, year);
  const nopat = ebit !== null && effectiveTaxRate !== null ? ebit * (1 - effectiveTaxRate) : null;
  const fcff = nopat !== null && da !== null && capex !== null && workingCapitalImpact !== null
    ? nopat + da - capex + workingCapitalImpact
    : null;

  const netIncomeToCommon = finiteValueForYear(series.netIncomeToCommon, year);
  const shareBasedCompensation = finiteValueForYear(series.shareBasedCompensation, year);
  const otherAdjustments = finiteValueForYear(series.otherAdjustments, year);
  // The Discover FCFE bridge treats a missing net borrowing row as zero.
  const netBorrowing = finiteValueForYear(series.netBorrowing, year) ?? 0;
  const fcfe = netIncomeToCommon !== null && da !== null && shareBasedCompensation !== null && otherAdjustments !== null && capex !== null && workingCapitalImpact !== null
    ? netIncomeToCommon + da + shareBasedCompensation + otherAdjustments - capex + workingCapitalImpact + netBorrowing
    : null;

  return {
    effectiveTaxRatePercent: effectiveTaxRate === null ? null : effectiveTaxRate * 100,
    workingCapital: {
      previousYear: previousWorkingCapital,
      currentYear: currentWorkingCapital,
      netChangeInWorkingCapital,
      cashImpact: workingCapitalImpact,
      cashEffect: workingCapitalImpact === null ? "unavailable" : workingCapitalImpact > 0 ? "inflow" : workingCapitalImpact < 0 ? "outflow" : "neutral",
    },
    fcff: {
      ebit: { value: ebit, percent: percentageOf(ebit, revenue) },
      nopat: { value: nopat, percent: null },
      depreciationAndAmortization: { value: da, percent: percentageOf(da, ebit) },
      capitalExpenditure: { value: capex, percent: percentageOf(capex, ebit) },
      workingCapitalImpact: { value: workingCapitalImpact, percent: percentageOf(workingCapitalImpact, fcff) },
      fcff: { value: fcff, percent: null },
    },
    fcfe: {
      netIncomeToCommon: { value: netIncomeToCommon, percent: null },
      depreciationAndAmortization: { value: da, percent: percentageOf(da, netIncomeToCommon) },
      shareBasedCompensation: { value: shareBasedCompensation, percent: percentageOf(shareBasedCompensation, netIncomeToCommon) },
      otherAdjustments: { value: otherAdjustments, percent: percentageOf(otherAdjustments, netIncomeToCommon) },
      capitalExpenditure: { value: capex, percent: percentageOf(capex, netIncomeToCommon) },
      workingCapitalImpact: { value: workingCapitalImpact, percent: percentageOf(workingCapitalImpact, fcfe) },
      netBorrowing: { value: netBorrowing, percent: percentageOf(netBorrowing, netIncomeToCommon) },
      fcfe: { value: fcfe, percent: null },
    },
  };
}

function cashFlowMetric(
  company: CashFlowMetricSnapshot,
  peers: CashFlowMetricSnapshot[],
) {
  const peerValues = peers.map((item) => item.value).filter((value): value is number => value !== null && Number.isFinite(value));
  const peerPercentages = peers.map((item) => item.percent).filter((value): value is number => value !== null && Number.isFinite(value));
  return {
    companyValue: company.value,
    industryMedian: median(peerValues),
    industryObservations: peerValues.length,
    companyPercent: company.percent,
    industryMedianPercent: median(peerPercentages),
  };
}

export function buildCashFlowAnalysis(
  companySeries: CashFlowSeriesInput,
  peerSeries: Map<number, CashFlowSeriesInput>,
  fromYear: number,
  toYear: number,
) {
  const fcffKeys: FcffBridgeMetricKey[] = ["ebit", "nopat", "depreciationAndAmortization", "capitalExpenditure", "workingCapitalImpact", "fcff"];
  const fcfeKeys: FcfeBridgeMetricKey[] = ["netIncomeToCommon", "depreciationAndAmortization", "shareBasedCompensation", "otherAdjustments", "capitalExpenditure", "workingCapitalImpact", "netBorrowing", "fcfe"];
  return {
    yearly: Array.from({ length: toYear - fromYear + 1 }, (_, index) => {
      const year = fromYear + index;
      const company = cashFlowSnapshot(companySeries, year);
      const peers = [...peerSeries.values()].map((series) => cashFlowSnapshot(series, year));
      const peerTaxRates = peers.map((item) => item.effectiveTaxRatePercent).filter((value): value is number => value !== null && Number.isFinite(value));
      return {
        year,
        effectiveTaxRatePercent: company.effectiveTaxRatePercent,
        industryMedianEffectiveTaxRatePercent: median(peerTaxRates),
        workingCapital: company.workingCapital,
        fcff: Object.fromEntries(fcffKeys.map((key) => [key, cashFlowMetric(company.fcff[key], peers.map((item) => item.fcff[key]))])) as Record<FcffBridgeMetricKey, ReturnType<typeof cashFlowMetric>>,
        fcfe: Object.fromEntries(fcfeKeys.map((key) => [key, cashFlowMetric(company.fcfe[key], peers.map((item) => item.fcfe[key]))])) as Record<FcfeBridgeMetricKey, ReturnType<typeof cashFlowMetric>>,
      };
    }),
  };
}

function netDebtSeries(debt: YearValue[], cash: YearValue[], investments: YearValue[]): YearValue[] {
  const cashByYear = new Map(cash.map((item) => [item.year, item.value]));
  const investmentsByYear = new Map(investments.map((item) => [item.year, item.value]));
  return debt.filter((item) => cashByYear.has(item.year)).map((item) => ({
    year: item.year,
    value: item.value - cashByYear.get(item.year)! - (investmentsByYear.get(item.year) ?? 0),
  }));
}

function firstAvailableSeries(facts: DataFactRow[], keys: string[]): YearValue[] {
  for (const key of keys) {
    const series = seriesFor(facts, key);
    if (series.length) return series;
  }
  return [];
}

function combineAvailableSeries(series: YearValue[][]): YearValue[] {
  const years = [...new Set(series.flatMap((values) => values.map((point) => point.year)))].sort((left, right) => left - right);
  return years.flatMap((year) => {
    const reported = series.flatMap((values) => {
      const point = values.find((candidate) => candidate.year === year);
      return point ? [point.value] : [];
    });
    return reported.length ? [{ year, value: reported.reduce((total, value) => total + value, 0) }] : [];
  });
}

function coalesceSeries(primary: YearValue[], fallback: YearValue[]): YearValue[] {
  const primaryByYear = new Map(primary.map((point) => [point.year, point.value]));
  const fallbackByYear = new Map(fallback.map((point) => [point.year, point.value]));
  return [...new Set([...primaryByYear.keys(), ...fallbackByYear.keys()])]
    .sort((left, right) => left - right)
    .map((year) => ({ year, value: primaryByYear.get(year) ?? fallbackByYear.get(year)! }));
}

function valueForYear(series: YearValue[], year: number | null): number | null {
  if (year === null) return null;
  return series.find((point) => point.year === year)?.value ?? null;
}

function percentOfBase(value: number | null, base: number | null): number | null {
  return value !== null && base !== null && base !== 0 ? (value / Math.abs(base)) * 100 : null;
}

function normalizedPercentValue(value: number | null): number | null {
  if (value === null || !Number.isFinite(value)) return null;
  return Math.abs(value) <= 1 ? value * 100 : value;
}

function latestPointAtOrBefore(series: YearValue[], year: number | null): YearValue | null {
  if (year === null) return null;
  return [...series]
    .filter((point) => point.year <= year && Number.isFinite(point.value))
    .sort((left, right) => right.year - left.year)[0] ?? null;
}

function averageBalanceForYear(series: YearValue[], year: number): number | null {
  const current = valueForYear(series, year);
  if (current === null || current === 0) return null;
  const prior = [...series]
    .filter((point) => point.year < year && Number.isFinite(point.value))
    .sort((left, right) => right.year - left.year)[0]?.value;
  return prior === undefined ? Math.abs(current) : (Math.abs(current) + Math.abs(prior)) / 2;
}

function buildBalanceSheetStory(
  facts: DataFactRow[],
  dataset: DataApiDataset,
  reportingCurrency: string,
  displayUnit: string,
) {
  const assets = firstAvailableSeries(facts, ["assets"]);
  const equity = firstAvailableSeries(facts, ["equity", "shareholders_equity"]);
  const derivedLiabilities = derivedSeries(assets, equity, (totalAssets, shareholdersEquity) => totalAssets - shareholdersEquity);
  const liabilities = coalesceSeries(firstAvailableSeries(facts, ["liabilities", "total_liabilities"]), derivedLiabilities);
  const equationYears = assets
    .map((point) => point.year)
    .filter((year) => valueForYear(liabilities, year) !== null && valueForYear(equity, year) !== null)
    .sort((left, right) => left - right)
    .slice(-5);
  const latestFiscalYear = equationYears.at(-1) ?? null;

  const reportedCashCushion = firstAvailableSeries(facts, ["totalCash", "cashAndShortTermInvestments", "total_cash", "cash_and_short_term_investments"]);
  const cash = reportedCashCushion.length
    ? reportedCashCushion
    : combineAvailableSeries([
      firstAvailableSeries(facts, ["cash"]),
      firstAvailableSeries(facts, ["shortTermInvestments", "short_term_investments"]),
    ]);
  const accountsReceivable = firstAvailableSeries(facts, ["accountsReceivable", "accounts_receivable"]);
  const inventory = firstAvailableSeries(facts, ["inventory"]);
  const prepaidExpenses = firstAvailableSeries(facts, ["prepaidExpenses", "prepaid_expenses"]);
  const prepaidExpensesAndOtherCurrentAssets = coalesceSeries(
    prepaidExpenses,
    firstAvailableSeries(facts, ["prepaid_expenses_and_other_current_assets", "other_current_assets"]),
  );
  const operatingCurrentAssets = combineAvailableSeries([accountsReceivable, inventory, prepaidExpensesAndOtherCurrentAssets]);
  const propertyPlantEquipment = firstAvailableSeries(facts, ["propertyPlantEquipment", "property_plant_equipment_net", "property_plant_and_equipment_net"]);
  const goodwill = firstAvailableSeries(facts, ["goodwill"]);
  const otherIntangibleAssets = firstAvailableSeries(facts, ["otherIntangibleAssets", "intangible_assets_net_excluding_goodwill", "finite_lived_intangible_assets_net"]);

  const unearnedRevenueCurrent = firstAvailableSeries(facts, ["unearnedRevenueCurrent", "unearned_revenue", "deferred_revenue_current", "contract_liabilities_current"]);
  const unearnedRevenueLongTerm = firstAvailableSeries(facts, ["unearnedRevenueLongTerm", "unearned_revenue_noncurrent", "deferred_revenue_noncurrent", "contract_liabilities_noncurrent"]);
  const unearnedRevenue = combineAvailableSeries([unearnedRevenueCurrent, unearnedRevenueLongTerm]);
  const accountsPayable = firstAvailableSeries(facts, ["accountsPayable", "accounts_payable"]);
  const totalDebtReported = firstAvailableSeries(facts, ["totalDebt", "total_debt"]);
  const shortTermDebt = firstAvailableSeries(facts, ["currentDebt", "shortTermBorrowings", "short_term_debt"]);
  const longTermDebt = firstAvailableSeries(facts, ["longTermDebt", "long_term_debt"]);
  const shortTermLeases = firstAvailableSeries(facts, ["shortTermLeaseLiabilities", "current_lease_liabilities", "lease_liabilities_current", "operating_lease_liabilities_current", "finance_lease_liabilities_current"]);
  const longTermLeases = firstAvailableSeries(facts, ["longTermLeaseLiabilities", "long_term_leases", "lease_liabilities_noncurrent", "operating_lease_liabilities_noncurrent", "finance_lease_liabilities_noncurrent"]);
  const totalDebt = totalDebtReported.length ? totalDebtReported : combineAvailableSeries([shortTermDebt, longTermDebt, shortTermLeases, longTermLeases]);
  const shortTermDebtRate = firstAvailableSeries(facts, ["shortTermDebtWeightedAverageInterestRate", "short_term_debt_weighted_average_interest_rate", "short_term_debt_interest_rate", "short_term_borrowing_rate"]);
  const reportedDebtRate = firstAvailableSeries(facts, ["debtWeightedAverageInterestRate", "debt_weighted_average_interest_rate", "long_term_debt_interest_rate", "long_term_borrowing_rate"]);
  const operatingLeaseRate = firstAvailableSeries(facts, ["operatingLeaseWeightedAverageDiscountRate", "operating_lease_weighted_average_discount_rate", "lease_discount_rate"]);
  const financeLeaseRate = firstAvailableSeries(facts, ["financeLeaseWeightedAverageDiscountRate", "finance_lease_weighted_average_discount_rate"]);
  const shortTermLeaseRate = operatingLeaseRate.length ? operatingLeaseRate : financeLeaseRate;
  const longTermLeaseRate = operatingLeaseRate.length ? operatingLeaseRate : financeLeaseRate;

  const commonStock = firstAvailableSeries(facts, ["commonStock", "common_stock", "common_stock_value"]);
  const additionalPaidInCapital = firstAvailableSeries(facts, ["additionalPaidInCapital", "additional_paid_in_capital"]);
  const combinedCommonStockAndPaidInCapital = firstAvailableSeries(facts, ["commonStockAndPaidInCapital", "common_stock_and_additional_paid_in_capital"]);
  const retainedEarnings = firstAvailableSeries(facts, ["retainedEarnings", "retained_earnings", "retained_earnings_accumulated_deficit"]);
  const comprehensiveIncome = firstAvailableSeries(facts, ["comprehensiveIncome", "accumulated_other_comprehensive_income", "accumulated_other_comprehensive_income_loss"]);

  const ebitda = firstAvailableSeries(facts, ["ebitda"]);
  const ebit = firstAvailableSeries(facts, ["ebit", "operatingIncome"]);
  const interestExpense = firstAvailableSeries(facts, ["interestExpense", "interest_expense"]);
  const weightedAverageCost = firstAvailableSeries(facts, ["weighted_average_cost_of_debt", "weighted_average_interest_rate_debt"]);

  const latestAssets = valueForYear(assets, latestFiscalYear);
  const latestLiabilities = valueForYear(liabilities, latestFiscalYear);
  const latestEquity = valueForYear(equity, latestFiscalYear);
  const component = (
    key: string,
    label: string,
    series: YearValue[],
    base: YearValue[],
    children: Array<{ key: string; label: string; series: YearValue[]; rate?: YearValue[] }> = [],
  ) => ({
    key,
    label,
    value: valueForYear(series, latestFiscalYear),
    percentageOfBase: percentOfBase(valueForYear(series, latestFiscalYear), valueForYear(base, latestFiscalYear)),
    trend: equationYears.flatMap((fiscalYear) => {
      const value = valueForYear(series, fiscalYear);
      return value === null ? [] : [{
        fiscalYear,
        value,
        percentageOfBase: percentOfBase(value, valueForYear(base, fiscalYear)),
      }];
    }),
    children: children.map((child) => ({
      key: child.key,
      label: child.label,
      value: valueForYear(child.series, latestFiscalYear),
      percentageOfBase: percentOfBase(valueForYear(child.series, latestFiscalYear), valueForYear(base, latestFiscalYear)),
      interestRatePercent: normalizedPercentValue(valueForYear(child.rate ?? [], latestFiscalYear)),
    })),
  });

  const latestCash = valueForYear(cash, latestFiscalYear);
  const latestDebt = valueForYear(totalDebt, latestFiscalYear);
  const latestEbitda = valueForYear(ebitda, latestFiscalYear);
  const latestEbit = valueForYear(ebit, latestFiscalYear);
  const latestInterestExpense = valueForYear(interestExpense, latestFiscalYear);
  const netCashDebt = latestCash !== null && latestDebt !== null ? latestCash - latestDebt : null;
  const netDebtToEbitda = latestDebt !== null && latestCash !== null && latestEbitda !== null && latestEbitda !== 0
    ? (latestDebt - latestCash) / latestEbitda
    : null;
  const interestCoverage = latestEbit !== null && latestInterestExpense !== null && latestInterestExpense !== 0
    ? latestEbit / Math.abs(latestInterestExpense)
    : null;
  const interestBearingDebt = combineAvailableSeries([shortTermDebt, longTermDebt]);
  let weightedAverageCostOfDebt = normalizedPercentValue(valueForYear(weightedAverageCost, latestFiscalYear));
  let weightedAverageCostOfDebtFiscalYear = weightedAverageCostOfDebt === null ? null : latestFiscalYear;
  let weightedAverageCostOfDebtNote = weightedAverageCostOfDebt === null
    ? "Not calculable from reported inputs"
    : "Issuer-reported borrowing cost";
  if (weightedAverageCostOfDebt === null && latestFiscalYear !== null) {
    const currentReportedRate = valueForYear(reportedDebtRate, latestFiscalYear);
    if (currentReportedRate !== null) {
      weightedAverageCostOfDebt = normalizedPercentValue(currentReportedRate);
      weightedAverageCostOfDebtFiscalYear = latestFiscalYear;
      weightedAverageCostOfDebtNote = "Issuer-reported weighted borrowing rate";
    }
  }
  if (weightedAverageCostOfDebt === null && latestFiscalYear !== null) {
    const averageDebt = averageBalanceForYear(interestBearingDebt, latestFiscalYear);
    if (averageDebt !== null && latestInterestExpense !== null) {
      weightedAverageCostOfDebt = Math.abs(latestInterestExpense) / averageDebt * 100;
      weightedAverageCostOfDebtFiscalYear = latestFiscalYear;
      weightedAverageCostOfDebtNote = `FY${latestFiscalYear} interest expense ÷ average borrowings`;
    }
  }
  if (weightedAverageCostOfDebt === null) {
    const latestLeaseRate = [
      latestPointAtOrBefore(operatingLeaseRate, latestFiscalYear),
      latestPointAtOrBefore(financeLeaseRate, latestFiscalYear),
    ].filter((point): point is YearValue => point !== null).sort((left, right) => right.year - left.year)[0] ?? null;
    const latestBorrowings = valueForYear(interestBearingDebt, latestFiscalYear);
    if (
      latestLeaseRate
      && latestDebt !== null
      && latestDebt > 0
      && (latestBorrowings === null || latestBorrowings === 0)
    ) {
      weightedAverageCostOfDebt = normalizedPercentValue(latestLeaseRate.value);
      weightedAverageCostOfDebtFiscalYear = latestLeaseRate.year;
      weightedAverageCostOfDebtNote = `Lease-only funding · FY${latestLeaseRate.year} disclosed discount rate`;
    } else if (latestDebt === null || latestDebt === 0) {
      weightedAverageCostOfDebtNote = "Not applicable · no reported debt";
    }
  }
  if (weightedAverageCostOfDebt === null) {
    const latestReportedRate = latestPointAtOrBefore(reportedDebtRate, latestFiscalYear);
    if (latestReportedRate) {
      weightedAverageCostOfDebt = normalizedPercentValue(latestReportedRate.value);
      weightedAverageCostOfDebtFiscalYear = latestReportedRate.year;
      weightedAverageCostOfDebtNote = `Latest disclosed borrowing rate · FY${latestReportedRate.year}`;
    }
  }
  if (weightedAverageCostOfDebt === null && latestFiscalYear !== null) {
    const candidateYears = interestExpense
      .map((point) => point.year)
      .filter((year) => year < latestFiscalYear && averageBalanceForYear(interestBearingDebt, year) !== null)
      .sort((left, right) => right - left);
    const costYear = candidateYears[0] ?? null;
    const averageDebt = costYear === null ? null : averageBalanceForYear(interestBearingDebt, costYear);
    const interest = costYear === null ? null : valueForYear(interestExpense, costYear);
    if (costYear !== null && averageDebt !== null && interest !== null) {
      weightedAverageCostOfDebt = Math.abs(interest) / averageDebt * 100;
      weightedAverageCostOfDebtFiscalYear = costYear;
      weightedAverageCostOfDebtNote = `Latest calculable cost · FY${costYear} interest expense ÷ average borrowings`;
    }
  }
  const cashPercent = percentOfBase(latestCash, latestAssets);
  const summary = latestFiscalYear === null
    ? null
    : cashPercent === null
      ? `FY${latestFiscalYear} balances reconcile through the accounting equation; the cash cushion was not separately reported.`
      : `${cashPercent.toFixed(1)}% of assets is held in cash and cash equivalents. ${netCashDebt === null ? "Net cash or debt could not be calculated from the reported facts." : netCashDebt >= 0 ? "Cash exceeds total debt, adding balance-sheet flexibility." : "Debt exceeds cash, so repayment capacity and interest cover matter."}`;
  const latestBalanceItem = [...dataset.balance.items]
    .filter((item) => item.period.type === "annual" && Number(item.period.end.slice(0, 4)) === latestFiscalYear)
    .sort((left, right) => right.period.end.localeCompare(left.period.end))[0];

  return {
    status: latestFiscalYear === null ? "unavailable" as const : "available" as const,
    reason: latestFiscalYear === null ? "TaRaShaData did not return total assets and shareholders’ equity for a common annual period." : null,
    latestFiscalYear,
    latestPeriodEnd: latestBalanceItem?.period.end ?? null,
    reportingCurrency,
    displayUnit,
    years: equationYears,
    equation: { assets: latestAssets, liabilities: latestLiabilities, shareholdersEquity: latestEquity },
    assets: [
      component("cash", "Cash & short-term investments cushion", cash, assets),
      component("operating_current_assets", "Receivables, inventory & other current assets", operatingCurrentAssets, assets, [
        { key: "accounts_receivable", label: "Receivables", series: accountsReceivable },
        { key: "inventory", label: "Inventory", series: inventory },
        { key: "prepaid_expenses", label: "Prepaids & other current assets", series: prepaidExpensesAndOtherCurrentAssets },
      ]),
      component("property_plant_equipment", "Property, plant & equipment", propertyPlantEquipment, assets),
      component("goodwill", "Goodwill", goodwill, assets),
      component("other_intangible_assets", "Other intangible assets", otherIntangibleAssets, assets),
    ],
    liabilities: [
      component("unearned_revenue", "Unearned revenue", unearnedRevenue, liabilities, [
        { key: "unearned_revenue_current", label: "Current", series: unearnedRevenueCurrent },
        { key: "unearned_revenue_long_term", label: "Long term", series: unearnedRevenueLongTerm },
      ]),
      component("borrowings", "Borrowings", totalDebt, liabilities, [
        { key: "short_term_debt", label: "Short-term borrowings", series: shortTermDebt, rate: shortTermDebtRate },
        { key: "long_term_debt", label: "Long-term borrowings", series: longTermDebt, rate: reportedDebtRate },
        { key: "short_term_leases", label: "Short-term leases", series: shortTermLeases, rate: shortTermLeaseRate },
        { key: "long_term_leases", label: "Long-term leases", series: longTermLeases, rate: longTermLeaseRate },
      ]),
      component("accounts_payable", "Accounts payable", accountsPayable, liabilities),
    ],
    shareholdersEquity: [
      component(
        "common_stock",
        valueForYear(combinedCommonStockAndPaidInCapital, latestFiscalYear) !== null && valueForYear(additionalPaidInCapital, latestFiscalYear) === null
          ? "Common stock & APIC (reported together)"
          : "Common stock",
        valueForYear(combinedCommonStockAndPaidInCapital, latestFiscalYear) !== null && valueForYear(additionalPaidInCapital, latestFiscalYear) === null
          ? combinedCommonStockAndPaidInCapital
          : commonStock,
        equity,
      ),
      component(
        "additional_paid_in_capital",
        valueForYear(combinedCommonStockAndPaidInCapital, latestFiscalYear) !== null && valueForYear(additionalPaidInCapital, latestFiscalYear) === null
          ? "APIC (not separately reported)"
          : "Additional paid-in capital",
        additionalPaidInCapital,
        equity,
      ),
      component("retained_earnings", "Retained earnings", retainedEarnings, equity),
      component("comprehensive_income", "Comprehensive income", comprehensiveIncome, equity),
    ],
    health: {
      totalCash: latestCash,
      totalDebt: latestDebt,
      netCashDebt,
      netDebtToEbitda,
      interestCoverage,
      weightedAverageCostOfDebt,
      weightedAverageCostOfDebtFiscalYear,
      weightedAverageCostOfDebtNote,
    },
    summary,
    methodology: "Annual normalized balance-sheet facts are divided by the relevant reported total for each fiscal year. Total liabilities is derived as assets less shareholders’ equity only when the issuer total is absent. The cash cushion and net cash (debt) include cash equivalents and short-term investments. Weighted average cost of debt uses an issuer-reported rate when available; otherwise it is absolute annual interest expense divided by average short- and long-term borrowings, or the disclosed lease discount rate for lease-only funding. The metric’s displayed note identifies the fiscal year and method. The plain-language health label screens net cash or debt, Net Debt / EBITDA (≤1x and ≤2.5x), and interest coverage (≥5x and ≥2x); it is context, not a recommendation. Missing components are never estimated.",
    sourceUrl: latestBalanceItem?.provenance?.source_url ?? null,
  };
}

const dataMetricToDiscoverFact: Record<string, string> = {
  revenue: "revenue",
  cost_of_revenue: "costOfRevenue",
  selling_general_admin: "sga",
  research_development: "researchAndDevelopment",
  operating_income: "operatingIncome",
  interest_expense: "interestExpense",
  pretax_income: "pretaxIncome",
  effective_tax_rate: "effectiveTaxRate",
  net_income: "netIncome",
  cash_flow_net_income: "netIncome",
  minority_interest_earnings: "minorityInterestInEarnings",
  net_income_common: "netIncomeToCommon",
  shares_outstanding_basic: "sharesOutstanding",
  shares_outstanding_diluted: "dilutedShares",
  eps_basic: "epsBasic",
  eps_diluted: "eps",
  ebitda: "ebitda",
  ebit: "ebit",
  depreciation_amortization: "depreciation",
  cash_flow_depreciation_amortization: "depreciation",
  cash: "cash",
  short_term_investments: "shortTermInvestments",
  cash_and_short_term_investments: "cashAndShortTermInvestments",
  total_cash: "totalCash",
  accounts_receivable: "accountsReceivable",
  inventory: "inventory",
  prepaid_expenses: "prepaidExpenses",
  current_assets: "currentAssets",
  property_plant_equipment_net: "propertyPlantEquipment",
  property_plant_and_equipment_net: "propertyPlantEquipment",
  goodwill: "goodwill",
  intangible_assets_net_excluding_goodwill: "otherIntangibleAssets",
  other_intangible_assets: "otherIntangibleAssets",
  assets: "assets",
  accounts_payable: "accountsPayable",
  unearned_revenue: "unearnedRevenueCurrent",
  deferred_revenue_current: "unearnedRevenueCurrent",
  contract_liabilities_current: "unearnedRevenueCurrent",
  deferred_revenue_noncurrent: "unearnedRevenueLongTerm",
  contract_liabilities_noncurrent: "unearnedRevenueLongTerm",
  unearned_revenue_noncurrent: "unearnedRevenueLongTerm",
  short_term_debt: "currentDebt",
  long_term_debt: "longTermDebt",
  lease_liabilities_current: "shortTermLeaseLiabilities",
  operating_lease_liabilities_current: "shortTermLeaseLiabilities",
  current_lease_liabilities: "shortTermLeaseLiabilities",
  lease_liabilities_noncurrent: "longTermLeaseLiabilities",
  operating_lease_liabilities_noncurrent: "longTermLeaseLiabilities",
  long_term_leases: "longTermLeaseLiabilities",
  short_term_debt_weighted_average_interest_rate: "shortTermDebtWeightedAverageInterestRate",
  debt_weighted_average_interest_rate: "debtWeightedAverageInterestRate",
  operating_lease_weighted_average_discount_rate: "operatingLeaseWeightedAverageDiscountRate",
  finance_lease_weighted_average_discount_rate: "financeLeaseWeightedAverageDiscountRate",
  current_liabilities: "currentLiabilities",
  long_term_liabilities: "longTermLiabilities",
  total_debt: "totalDebt",
  liabilities: "liabilities",
  shareholders_equity: "equity",
  common_stock_value: "commonStock",
  common_stock: "commonStock",
  common_stock_and_additional_paid_in_capital: "commonStockAndPaidInCapital",
  additional_paid_in_capital: "additionalPaidInCapital",
  retained_earnings: "retainedEarnings",
  accumulated_other_comprehensive_income: "comprehensiveIncome",
  operating_cash_flow: "operatingCash",
  capital_expenditures: "capex",
  share_based_compensation: "shareBasedCompensation",
  other_adjustments: "otherAdjustments",
  net_long_term_debt_issued_repaid: "netDebtIssuedPaid",
  common_dividends_paid: "commonDividendsPaid",
};

function unitKind(item: DataApiFinancialItem): DataFactRow["unit_kind"] {
  if (item.display.unit === "ratio") return "ratio";
  if (item.display.unit.includes("shares millions")) return "shares";
  if (item.display.unit.includes("per share")) return "per_share";
  return "amount";
}

function statementKey(statement: DataApiFinancials["statement"], metric: string): DataFactRow["statement_key"] {
  if (metric.startsWith("shares_outstanding") || metric.startsWith("eps_")) return "shares";
  if (statement === "cash_flow") return "cash";
  return statement;
}

function financialRows(dataset: DataApiDataset): DataFactRow[] {
  const rows: DataFactRow[] = [];
  for (const financials of [dataset.income, dataset.balance, dataset.cash_flow]) {
    for (const item of financials.items) {
      if (item.period.type !== "annual") continue;
      const year = Number(String(item.period.end).slice(0, 4));
      const value = Number(item.display.value);
      if (!Number.isInteger(year) || !Number.isFinite(value)) continue;
      rows.push({
        company_id: dataset.company.id,
        statement_key: statementKey(financials.statement, item.metric),
        fact_key: dataMetricToDiscoverFact[item.metric] ?? item.metric,
        label: item.metric_label,
        unit_kind: unitKind(item),
        fiscal_year: year,
        value,
      });
    }
  }
  const currentAssets = new Map(seriesFor(rows, "currentAssets").map((item) => [item.year, item.value]));
  const cash = new Map(seriesFor(rows, "cash").map((item) => [item.year, item.value]));
  const currentLiabilities = new Map(seriesFor(rows, "currentLiabilities").map((item) => [item.year, item.value]));
  const currentDebt = new Map(seriesFor(rows, "currentDebt").map((item) => [item.year, item.value]));
  for (const [year, assets] of currentAssets) {
    const cashValue = cash.get(year);
    const liabilities = currentLiabilities.get(year);
    if (cashValue === undefined || liabilities === undefined) continue;
    rows.push({
      company_id: dataset.company.id,
      statement_key: "balance",
      fact_key: "nonCashWorkingCapital",
      label: "Net operating working capital",
      unit_kind: "amount",
      fiscal_year: year,
      value: assets - cashValue - (liabilities - (currentDebt.get(year) ?? 0)),
    });
  }
  return rows;
}

function companyRow(dataset: DataApiDataset, context?: DataApiConstituent, fallbackBucket = "Unclassified"): DataCompanyRow {
  const currentAlias = dataset.company.aliases.find((alias) => alias.is_current) ?? dataset.company.aliases[0];
  return {
    id: dataset.company.id,
    cik: dataset.company.cik,
    name: dataset.company.name,
    ticker: context?.ticker || currentAlias?.ticker || dataset.company.cik,
    exchange: context?.exchange || currentAlias?.exchange || "",
    country: "USA",
    industry_bucket: context?.industry_buckets[0]?.name || dataset.company.sic_description || fallbackBucket,
  };
}

export async function pullDataCompany(env: DataProviderEnv, companyId: string, fromYear: number, toYear: number, customConstituentIds?: string[]) {
  const identifier = companyId.replace(/^data-/, "").trim();
  if (!identifier) throw new Error("Invalid TaRaShaData.ai company identifier.");
  const constituentIdentifiers = customConstituentIds === undefined
    ? undefined
    : [...new Set(customConstituentIds.map((id) => id.replace(/^data-/, "").trim()).filter(Boolean))];
  if (constituentIdentifiers && (!constituentIdentifiers.length || constituentIdentifiers.length > 100)) {
    throw new Error("Select between 1 and 100 industry constituents.");
  }
  const payload = await dataFetch<DataApiDiscoverDataset>(env, "/v1/discover/company-dataset", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      identifier,
      from_year: fromYear,
      to_year: toYear,
      constituent_identifiers: constituentIdentifiers,
    }),
  });
  if (!payload.normalized_coverage.available) {
    throw new Error("TaRaShaData.ai does not currently publish normalized annual coverage for this company.");
  }
  const targetDataset = payload.datasets.find((item) => item.company.cik === payload.company.cik);
  if (!targetDataset) throw new Error("TaRaShaData.ai returned no normalized dataset for the selected company.");
  const contextByCik = new Map(payload.constituents.map((item) => [item.cik, item]));
  const defaultBucket = payload.industry_buckets[0]?.name || payload.company.sic_description || "Unclassified";
  const company = companyRow(targetDataset, contextByCik.get(payload.company.cik), defaultBucket);
  const numericId = company.id;
  const rawFacts = financialRows(targetDataset);
  const constituentCompanies = payload.constituents.flatMap((item) => {
    const dataset = payload.datasets.find((candidate) => candidate.company.cik === item.cik);
    return dataset ? [companyRow(dataset, item, defaultBucket)] : [];
  });
  const constituentCompanyIds = new Set(constituentCompanies.map((item) => item.id));
  const bucketId = payload.industry_buckets[0]?.id ?? 0;
  const rawIndustryFacts: DataIndustryFactRow[] = payload.datasets
    .filter((dataset) => constituentCompanyIds.has(dataset.company.id))
    .flatMap((dataset) => financialRows(dataset).map((fact) => ({
      bucket_id: bucketId,
      bucket_name: defaultBucket,
      company_id: fact.company_id,
      country: "USA" as const,
      fact_key: fact.fact_key as DataIndustryFactRow["fact_key"],
      fiscal_year: fact.fiscal_year,
      value: fact.value,
    })));
  const amountUnit = payload.company.reporting_currency === "USD"
    ? "US$ million"
    : `${payload.company.reporting_currency} million`;
  const analysisFacts = rawFacts.map((fact) => ({ ...fact, value: Number(fact.value) }));
  const facts = analysisFacts.filter((fact) => fact.fiscal_year >= fromYear);
  const cashFlowIndustryFacts = rawIndustryFacts.map((fact) => ({
    ...fact,
    value: Number(fact.value),
  }));
  const industryFacts = cashFlowIndustryFacts.filter((fact) => fact.fiscal_year >= fromYear);
  const statements = (Object.keys(statementLabels) as Array<keyof typeof statementLabels>).map((statementKey) => {
    const statementFacts = facts.filter((fact) => fact.statement_key === statementKey);
    const grouped = new Map<string, DataFactRow[]>();
    for (const fact of statementFacts) grouped.set(fact.fact_key, [...(grouped.get(fact.fact_key) ?? []), fact]);
    return {
      key: statementKey,
      label: statementLabels[statementKey],
      facts: [...grouped.entries()].map(([key, values]) => ({
        key,
        label: values[0].label,
        description: factDescriptions[key] ?? "Normalized historical financial fact from TaRaShaData.ai.",
        unit: values[0].unit_kind === "shares" ? "million shares" : values[0].unit_kind === "ratio" ? "%" : values[0].unit_kind === "per_share" ? `${payload.company.reporting_currency} per share` : amountUnit,
        values: values.map((value) => ({ year: value.fiscal_year, value: value.value })),
      })),
    };
  });
  const revenue = seriesFor(facts, "revenue");
  const operatingCost = seriesFor(facts, "costOfRevenue");
  const sga = seriesFor(facts, "sga");
  const depreciation = seriesFor(facts, "depreciation");
  const researchAndDevelopment = seriesFor(facts, "researchAndDevelopment");
  const operatingIncome = seriesFor(facts, "operatingIncome");
  const ebitda = seriesFor(facts, "ebitda");
  const ebit = seriesFor(facts, "ebit");
  const interestExpense = seriesFor(facts, "interestExpense").map((item) => ({ ...item, value: Math.abs(item.value) }));
  const ebt = seriesFor(facts, "pretaxIncome");
  const netProfit = seriesFor(facts, "netIncome");
  const minorityInterestInEarnings = seriesFor(facts, "minorityInterestInEarnings");
  const earningsFromDiscontinuedOperations = seriesFor(facts, "earningsFromDiscontinuedOperations");
  const commonDividendsPaid = seriesFor(facts, "commonDividendsPaid");
  const netIncomeToCommon = seriesFor(facts, "netIncomeToCommon");
  const dilutedShares = seriesFor(facts, "dilutedShares");
  const eps = seriesFor(facts, "eps");
  const effectiveTaxRate = seriesFor(facts, "effectiveTaxRate");
  const cashFlowNonCashWorkingCapital = seriesFor(analysisFacts, "nonCashWorkingCapital");
  const workingCapitalCurrentAssets = seriesFor(analysisFacts, "currentAssets");
  const workingCapitalCash = seriesFor(analysisFacts, "cash");
  const workingCapitalCurrentLiabilities = seriesFor(analysisFacts, "currentLiabilities");
  const workingCapitalCurrentDebt = seriesFor(analysisFacts, "currentDebt");
  const shortTermBorrowings = seriesFor(analysisFacts, "shortTermBorrowings");
  const currentPortionLongTermDebt = seriesFor(analysisFacts, "currentPortionLongTermDebt");
  const shareBasedCompensation = seriesFor(facts, "shareBasedCompensation");
  const otherAdjustments = seriesFor(facts, "otherAdjustments");
  const netBorrowing = seriesFor(facts, "netDebtIssuedPaid");
  const grossProfit = derivedSeries(revenue, operatingCost, (a, b) => a - b);
  const operatingCash = seriesFor(facts, "operatingCash");
  const capex = seriesFor(facts, "capex");
  const debt = seriesFor(facts, "totalDebt");
  const cash = seriesFor(facts, "cash");
  const investments = seriesFor(facts, "shortTermInvestments");
  const netDebt = netDebtSeries(debt, cash, investments);
  const operatingMargin = derivedSeries(operatingIncome, revenue, (a, b) => (a / b) * 100);
  const companyProfitability = buildProfitabilitySeries(revenue, operatingCost, operatingIncome, sga, depreciation, researchAndDevelopment);
  const companyProfitabilityAbsolute: ProfitabilityAbsoluteSeries = {
    grossMargin: grossProfit,
    operatingMargin: operatingIncome,
    cogsRatio: operatingCost,
    sgaRatio: sga,
    daRatio: depreciation,
    rdRatio: researchAndDevelopment,
  };
  const industryRevenue = groupIndustrySeries(industryFacts, "revenue");
  const industryOperatingCost = groupIndustrySeries(industryFacts, "costOfRevenue");
  const industryOperatingIncome = groupIndustrySeries(industryFacts, "operatingIncome");
  const industrySga = groupIndustrySeries(industryFacts, "sga");
  const industryDepreciation = groupIndustrySeries(industryFacts, "depreciation");
  const industryResearchAndDevelopment = groupIndustrySeries(industryFacts, "researchAndDevelopment");
  const industryEbitda = groupIndustrySeries(industryFacts, "ebitda");
  const industryEbit = groupIndustrySeries(industryFacts, "ebit");
  const industryInterestExpense = new Map(
    [...groupIndustrySeries(industryFacts, "interestExpense")].map(([companyId, series]) => [
      companyId,
      series.map((item) => ({ ...item, value: Math.abs(item.value) })),
    ]),
  );
  const industryEbt = groupIndustrySeries(industryFacts, "pretaxIncome");
  const industryNetProfit = groupIndustrySeries(industryFacts, "netIncome");
  const industryMinorityInterestInEarnings = groupIndustrySeries(industryFacts, "minorityInterestInEarnings");
  const industryEarningsFromDiscontinuedOperations = groupIndustrySeries(industryFacts, "earningsFromDiscontinuedOperations");
  const industryCommonDividendsPaid = groupIndustrySeries(industryFacts, "commonDividendsPaid");
  const industryNetIncomeToCommon = groupIndustrySeries(industryFacts, "netIncomeToCommon");
  const industryDilutedShares = groupIndustrySeries(industryFacts, "dilutedShares");
  const industryEps = groupIndustrySeries(industryFacts, "eps");
  const industryEffectiveTaxRate = groupIndustrySeries(industryFacts, "effectiveTaxRate");
  const industryNonCashWorkingCapital = groupIndustrySeries(cashFlowIndustryFacts, "nonCashWorkingCapital");
  const industryShareBasedCompensation = groupIndustrySeries(industryFacts, "shareBasedCompensation");
  const industryOtherAdjustments = groupIndustrySeries(industryFacts, "otherAdjustments");
  const industryCapex = groupIndustrySeries(industryFacts, "capex");
  const industryNetBorrowing = groupIndustrySeries(industryFacts, "netDebtIssuedPaid");
  const industryGrossProfit = derivedIndustrySeries(industryRevenue, industryOperatingCost, (a, b) => a - b);
  const industryProfitability = buildIndustryProfitabilitySeries(industryRevenue, industryOperatingCost, industryOperatingIncome, industrySga, industryDepreciation, industryResearchAndDevelopment);
  const industryProfitabilityAbsolute: IndustryProfitabilityAbsoluteSeries = {
    grossMargin: industryGrossProfit,
    operatingMargin: industryOperatingIncome,
    cogsRatio: industryOperatingCost,
    sgaRatio: industrySga,
    daRatio: industryDepreciation,
    rdRatio: industryResearchAndDevelopment,
  };
  const industryConstituents = constituentCompanies.map((item) => ({
    id: `data-${item.cik}`,
    name: item.name,
    ticker: item.ticker,
    industryBucket: item.industry_bucket || "Unclassified",
    revenueObservations: industryRevenue.get(item.id)?.length ?? 0,
    grossProfitObservations: industryGrossProfit.get(item.id)?.length ?? 0,
    operatingIncomeObservations: industryOperatingIncome.get(item.id)?.length ?? 0,
  }));
  const industryCompanyLabels = new Map(constituentCompanies.map((item) => [item.id, `${item.name} (${item.ticker})`]));
  const industryCompanyCount = industryConstituents.length;
  const companyEarningsSeries: EarningsSeries = {
    revenue,
    cogs: operatingCost,
    sga,
    researchAndDevelopment,
    ebitda,
    depreciationAndAmortization: depreciation,
    ebit,
    interestExpense,
    ebt,
    netProfit,
    minorityInterestInEarnings,
    earningsFromDiscontinuedOperations,
    commonDividendsPaid,
    netIncomeToCommon,
    dilutedShares,
    eps,
  };
  const peerEarningsSeries = industryEarningsSeries(industryFacts, {
    revenue: industryRevenue,
    cogs: industryOperatingCost,
    sga: industrySga,
    researchAndDevelopment: industryResearchAndDevelopment,
    ebitda: industryEbitda,
    depreciationAndAmortization: industryDepreciation,
    ebit: industryEbit,
    interestExpense: industryInterestExpense,
    ebt: industryEbt,
    netProfit: industryNetProfit,
    minorityInterestInEarnings: industryMinorityInterestInEarnings,
    earningsFromDiscontinuedOperations: industryEarningsFromDiscontinuedOperations,
    commonDividendsPaid: industryCommonDividendsPaid,
    netIncomeToCommon: industryNetIncomeToCommon,
    dilutedShares: industryDilutedShares,
    eps: industryEps,
  });
  const companyCashFlowSeries: CashFlowSeriesInput = {
    revenue,
    ebit,
    effectiveTaxRate,
    depreciationAndAmortization: depreciation,
    capitalExpenditure: capex,
    nonCashWorkingCapital: cashFlowNonCashWorkingCapital,
    netIncomeToCommon,
    shareBasedCompensation,
    otherAdjustments,
    netBorrowing,
    currentAssets: workingCapitalCurrentAssets,
    cashAndCashEquivalents: workingCapitalCash,
    currentLiabilities: workingCapitalCurrentLiabilities,
    currentDebt: workingCapitalCurrentDebt,
    shortTermBorrowings,
    currentPortionLongTermDebt,
  };
  const cashFlowPeerIds = [...new Set(industryFacts.map((fact) => fact.company_id))];
  const peerCashFlowSeries = new Map<number, CashFlowSeriesInput>(cashFlowPeerIds.map((peerId) => [peerId, {
    revenue: industryRevenue.get(peerId) ?? [],
    ebit: industryEbit.get(peerId) ?? [],
    effectiveTaxRate: industryEffectiveTaxRate.get(peerId) ?? [],
    depreciationAndAmortization: industryDepreciation.get(peerId) ?? [],
    capitalExpenditure: industryCapex.get(peerId) ?? [],
    nonCashWorkingCapital: industryNonCashWorkingCapital.get(peerId) ?? [],
    netIncomeToCommon: industryNetIncomeToCommon.get(peerId) ?? [],
    shareBasedCompensation: industryShareBasedCompensation.get(peerId) ?? [],
    otherAdjustments: industryOtherAdjustments.get(peerId) ?? [],
    netBorrowing: industryNetBorrowing.get(peerId) ?? [],
  }]));
  const marketByCompany = new Map<number, DataMarketMetricRow>();
  const latestYear = Math.max(...facts.map((fact) => fact.fiscal_year), toYear);
  return {
    id: `data-${company.cik}`,
    logoUrl: `/api/data/company-logo?companyId=${encodeURIComponent(`data-${company.cik}`)}&contract=1`,
    name: company.name,
    symbol: company.ticker,
    exchange: targetDataset.company.aliases.find((alias) => alias.is_current)?.exchange
      ?? targetDataset.company.aliases[0]?.exchange
      ?? "",
    sector: company.industry_bucket || `${company.country} · TaRaShaData.ai coverage`,
    description: "Source-linked historical financial statements standardized by TaRaShaData.ai from issuer filings.",
    currency: amountUnit,
    reportingPeriod: `FY ${latestYear}`,
    updatedAt: new Date(payload.company.updated_at).toLocaleDateString("en-GB", { day: "numeric", month: "long", year: "numeric", timeZone: "UTC" }),
    metrics: {
      revenue,
      operatingMargin,
      freeCashFlow: derivedSeries(operatingCash, capex, (a, b) => a - Math.abs(b)),
      netDebt,
    },
    companyStory: {
      revenueSegments: revenueSegmentStory(payload.business_segments, payload.company.reporting_currency),
      revenueOfferings: revenueOfferingStory(payload.revenue_offerings),
      costStructure: costStructureStory(payload.operating_cost_structure, payload.company.reporting_currency),
      cashConversion: cashConversionStory(payload.cash_conversion, payload.company.reporting_currency),
      balanceSheet: buildBalanceSheetStory(facts, targetDataset, payload.company.reporting_currency, amountUnit),
      stockRisk: stockRiskStory(payload.stock_risk),
      marketPricing: marketPricingStory(payload.market_pricing),
      stockHistory: stockHistoryStory(payload.stock_history),
    },
    researchShelf: {
      fromYear,
      toYear,
      industryBucket: company.industry_bucket || "Unclassified",
      industryCompanyCount,
      industryConstituents,
      industryConstituentsCustomized: payload.constituents_customized,
      growthComparisons: {
        revenue: {
          company: growthStatistics(revenue),
          industryBucket: pooledIndustryGrowthStatistics(industryRevenue, industryCompanyLabels),
        },
        grossProfit: {
          company: growthStatistics(grossProfit),
          industryBucket: pooledIndustryGrowthStatistics(industryGrossProfit, industryCompanyLabels),
        },
        grossOperatingLeverage: {
          company: grossOperatingLeverageStatistics(revenue, grossProfit),
          industryBucket: pooledIndustryGrossOperatingLeverageStatistics(industryRevenue, industryGrossProfit, industryCompanyLabels),
        },
        operatingIncome: {
          company: growthStatistics(operatingIncome),
          industryBucket: pooledIndustryGrowthStatistics(industryOperatingIncome, industryCompanyLabels),
        },
      },
      companyDeltas: companyDeltaPoints(revenue, grossProfit, operatingIncome, fromYear, toYear),
      industryDeltas: {
        revenue: industryDeltaPoints(revenue, industryRevenue, fromYear, toYear),
        grossProfit: industryDeltaPoints(grossProfit, industryGrossProfit, fromYear, toYear),
        operatingIncome: industryDeltaPoints(operatingIncome, industryOperatingIncome, fromYear, toYear),
      },
      rawIncome: rawIncomePoints(revenue, grossProfit, operatingIncome, fromYear, toYear),
      profitability: profitabilityAnalysis(companyProfitability, companyProfitabilityAbsolute, industryProfitability, industryProfitabilityAbsolute, fromYear, toYear, industryCompanyLabels),
      earningsAndValuation: {
        earningsFlow: earningsFlowAnalysis(companyEarningsSeries, peerEarningsSeries, fromYear, toYear),
        valuation: valuationAnalysis(companyEarningsSeries, peerEarningsSeries, marketByCompany, numericId, fromYear, toYear),
      },
      cashFlow: buildCashFlowAnalysis(companyCashFlowSeries, peerCashFlowSeries, fromYear, toYear),
    },
    notes: {
      growth: "Review the source-linked multi-year revenue record and filing lineage before drawing conclusions.",
      profitability: "Operating margin is calculated from TaRaShaData.ai operating income divided by revenue.",
      cash: "Free cash flow is calculated as TaRaShaData.ai operating cash flow less the absolute value of capital expenditure.",
      debt: "Net debt is calculated from TaRaShaData.ai total debt less cash and short-term investments when available.",
    },
    statements,
    filings: payload.filings.map((filing) => ({
      accession: filing.accession,
      form: filing.form,
      filed: filing.filing_date || "",
      period: filing.report_date || "",
      title: filing.primary_document || `${filing.form} filing`,
      url: filing.source_url,
    })),
    limitations: [
      "TaRaShaData.ai coverage is currently limited to companies and periods with standardized SEC filing facts.",
      "Enterprise Value and trailing P/E remain unavailable until TaRaShaData.ai publishes a governed company-level market-data contract.",
      "Derived values can differ from issuer presentation because of standardized definitions, restatements, units, or filing context.",
      "TaRaSha Discover stores no separate financial copy; the API response is held only in browser session memory.",
    ],
    dataMode: "tarasha-data" as const,
    source: {
      dataset: "TaRaShaData.ai normalized financials API",
      upstream: "Issuer filings and SEC XBRL",
      usage: "Educational research",
      persistence: "Browser session memory only",
    },
  };
}
