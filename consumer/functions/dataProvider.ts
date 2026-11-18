
export interface DataProviderEnv {
  TARASHA_DATA_API_URL?: string;
  TARASHA_DATA_API_KEY?: string;
}

interface DataCompanyRow {
  identifier?: string;
  supported_story_chapters?: number[];
  id: number;
  cik: string | null;
  name: string;
  ticker: string;
  exchange: string;
  country: "USA" | "India";
  industry_bucket: string;
}

interface DataFactRow {
  reporting_currency?: string;
  reporting_basis?: "standalone" | "consolidated";
  period_end?: string;
  period_start?: string | null;
  display_scale?: number;
  canonical_amount?: number;
  pure_da_calculation_unavailable?: boolean;
  company_id: number;
  statement_key: "income" | "balance" | "cash" | "shares" | "ratios" | "kpis";
  fact_key: string;
  label: string;
  unit_kind: "amount" | "shares" | "ratio" | "percent" | "per_share";
  ratio_display?: "percentage" | "number";
  fiscal_year: number;
  value: number;
}

interface DataIndustryFactRow {
  unit_kind?: DataFactRow["unit_kind"];
  bucket_id: number;
  bucket_name: string;
  company_id: number;
  country: "USA" | "India";
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
  identifier?: string;
  id?: number;
  cik: string | null;
  name: string;
  ticker: string | null;
  exchange: string | null;
  country?: string;
  industry_buckets?: DataApiIndustryBucket[];
  publication_status: string;
  normalized_coverage?: DataApiNormalizedCoverage;
  prepared_default_request?: PreparedDefaultRequest | null;
}

export interface PreparedDefaultRequest {
  identifier: string;
  from_year: number;
  to_year: number;
  include_market_context: boolean;
  include_industry_context: true;
  defer_market_context: true;
  constituent_identifiers: string[] | null;
  provenance: "references" | "full";
}

export function readPreparedDefaultRequest(value: unknown, identifier: string): PreparedDefaultRequest | undefined {
  if (value === undefined || value === null) return undefined;
  if (!value || typeof value !== "object") throw new Error("Invalid saved default financial request.");
  const request = value as PreparedDefaultRequest;
  const fields = ["identifier", "from_year", "to_year", "include_market_context", "include_industry_context",
    "defer_market_context", "constituent_identifiers", "provenance"];
  if (Object.keys(value).length !== fields.length || fields.some(field => !Object.hasOwn(value, field))
    || request.identifier !== identifier || !Number.isInteger(request.from_year) || !Number.isInteger(request.to_year)
    || request.from_year < (/^(?:IN|NSE|BSE):/i.test(identifier) ? 1900 : 1995)
    || request.from_year > request.to_year || request.to_year - request.from_year + 1 > 30
    || request.to_year > new Date().getUTCFullYear() || typeof request.include_market_context !== "boolean"
    || request.include_industry_context !== true || request.defer_market_context !== true
    || (request.constituent_identifiers !== null && (!Array.isArray(request.constituent_identifiers)
      || request.constituent_identifiers.length < 1 || request.constituent_identifiers.length > 100
      || request.constituent_identifiers.some(id => typeof id !== "string" || id.length < 1 || id.length > 100)))
    || !["references", "full"].includes(request.provenance)) {
    throw new Error("Invalid saved default financial request.");
  }
  return { ...request };
}

interface DataApiCompany {
  identifier?: string;
  reporting_basis?: "standalone" | "consolidated";
  reporting_basis_selection?: {
    periods?: Record<string, { reporting_basis?: "standalone" | "consolidated"; reporting_currency?: string | null }>;
  };
  debt_cost_policy?: { allow_interest_to_average_borrowings: boolean; reason: string } | null;
  supported_story_chapters?: number[];
  id: number;
  cik: string | null;
  name: string;
  sic_description: string | null;
  reporting_currency: string;
  country?: "USA" | "India";
  aliases: Array<{ ticker: string; exchange: string; is_current: boolean }>;
  updated_at: string;
}

interface DataApiFinancialItem {
  fact_id?: number | null;
  observation_id?: number | null;
  value?: string | null;
  metric: string;
  metric_label: string;
  display: { value: string | null; unit: string | null; format?: string | null; scale?: number | string | null };
  period: { type: string; start?: string | null; end: string; reporting_basis?: "standalone" | "consolidated"; reporting_currency?: string | null };
  unit?: string | null;
  status?: string;
  reason?: string | null;
  provenance?: { source_url?: string | null; derivation_method?: string | null; formula?: string | null;
    sec_cash_da_other_scope?: Record<string, unknown>; presentation_role?: string };
}

interface DataApiFinancials {
  statement: "income" | "balance" | "cash_flow" | "ratios" | "kpis";
  metrics: Array<{ key: string; label: string; description?: string; required_presentation?: boolean; parent_key?: string }>;
  items: DataApiFinancialItem[];
  columns?: Array<{ period_type: string; end: string }>;
  borrowing_cost_scope?: Record<string, { status?: string; reason?: string }>;
  sec_ifrs_debt_scope?: Array<{ period: { type: string; start?: string | null; end: string };
    section: "current" | "noncurrent"; status: string; reason?: string; parent_fact_id?: number }>;
  ppe_lease_scope?: Array<{ period: { type: string; start?: string | null; end: string }; status: string;
    presentation?: string; reason?: string; ppe_fact_id?: number | null; rou_fact_ids?: number[]; evidence?: unknown }>;
}

interface DataApiDataset {
  annual_reporting_currencies?: Record<string, { reporting_currency?: string | null; reporting_basis?: string; status?: string }>;
  company: DataApiCompany;
  income: DataApiFinancials;
  balance: DataApiFinancials;
  cash_flow: DataApiFinancials;
  ratios?: DataApiFinancials;
  kpis?: DataApiFinancials;
}

interface DataApiConstituent {
  identifier: string;
  cik: string | null;
  name: string;
  ticker: string;
  exchange: string;
  country: "USA" | "India";
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

interface PreparedStoryDelivery {
  preparedAtUtc: string;
  snapshotId: string;
  isLatest: boolean;
  latestStatus: "ready" | "preparing" | "failed" | "unknown";
}

interface DataApiDiscoverDataset {
  prepared_financial_snapshot?: { snapshot_id: string; key: string };
  prepared_story_delivery?: PreparedStoryDelivery;
  company: DataApiCompany;
  business_segments?: DataApiBusinessSegments;
  revenue_offerings?: DataApiRevenueOfferings;
  operating_cost_structure?: DataApiCostStructure;
  cash_conversion?: DataApiCashConversion;
  stock_risk?: DataApiStockRisk;
  market_pricing?: DataApiMarketPricing;
  stock_history?: DataApiStockHistory;
  normalized_coverage: DataApiNormalizedCoverage;
  country: "USA" | "India";
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
  status: "available" | "unavailable" | "pending";
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
  status: "available" | "unavailable" | "pending";
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
  status: "available" | "unavailable" | "pending";
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

type DataApiOperationsScope = "whole" | "continuing" | "discontinued" | "undetermined";
interface DataApiRevenueGrowthComparisons {
  current?: { sources?: DataApiRevenueSource[] };
  prior_year_ttm?: { sources?: DataApiRevenueSource[] } | null;
  three_year_prior_ttm?: { sources?: DataApiRevenueSource[] } | null;
  yoy_growth_percent?: { reason_text?: string | null };
  three_year_cagr_percent?: { reason_text?: string | null };
  [key: string]: unknown;
}
interface DataApiOfferingPoint {
  yoy_growth_percent?: string | null;
  three_period_cagr_percent?: string | null;
  three_year_cagr_percent?: string | null;
  growth_comparisons?: DataApiRevenueGrowthComparisons;
  sources?: DataApiRevenueSource[];
  accession?: string | null;
  operations_scope?: DataApiOperationsScope | null;
  operations_scope_comparability_group?: string | null;
  revenue_display: { value: string; unit: string };
  reporting_basis?: "standalone" | "consolidated";
  reporting_currency?: string;
  source_url?: string;
  period_end?: string;
}
interface DataApiRevenueSource {
  source_url?: string;
  accession?: string | null;
  concept?: string;
  role?: string;
  fact_id?: number;
  company_id?: number;
  artifact_id?: number;
  taxonomy?: string;
  dimensions?: Record<string, string>;
  value?: string;
  unit?: string;
  period_start?: string | null;
  period_end?: string;
  period_type?: string;
  fiscal_year?: number | null;
  fiscal_period?: string | null;
  filed_date?: string | null;
}
interface DataApiOffering {
  id: string;
  label: string;
  axis?: string;
  member?: string;
  label_basis?: string;
  history: Record<string, DataApiOfferingPoint>;
  ttm?: (DataApiOfferingPoint & { sources: DataApiRevenueSource[] }) | null;
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
type DataApiRevenueBreakdownSourceType = "reported_segment_reconciliation" | "disclosed_revenue_note" | "reported_revenue_hierarchy";
interface DataApiSegmentReconciliationPoint {
  source_type?: DataApiRevenueBreakdownSourceType;
  basis?: string;
  methodology?: string;
  period_end: string;
  reporting_basis?: "standalone" | "consolidated";
  revenue_display: { value: string; unit: string };
  items: { source_type?: DataApiRevenueBreakdownSourceType; id: string; label: string; role: "segment" | "elimination" | "reconciliation";
    revenue_display: { value: string; unit: string }; sources: DataApiRevenueSource[];
    yoy_growth_percent?: string | null; three_period_cagr_percent?: string | null;
    three_year_cagr_percent?: string | null; growth_comparisons?: DataApiRevenueGrowthComparisons }[];
}
interface DataApiSegmentReconciliation extends DataApiSegmentReconciliationPoint {
  period_type?: "ttm" | "annual";
  fiscal_year?: number;
  basis: string;
  methodology: string;
  history?: Record<string, DataApiSegmentReconciliationPoint>;
}
interface DataApiRevenueOfferings {
  reported_segment_reconciliation?: DataApiSegmentReconciliation | null;
  status?: "available" | "unavailable";
  axis?: string | null;
  offerings: DataApiOffering[];
  reported_offerings?: DataApiOffering[];
  reported_offerings_methodology?: string;
  annual_totals: Record<string, DataApiOfferingPoint>;
  methodology?: string;
  adjustment_methodology?: string;
  adjustment_entries?: DataApiAdjustmentEntry[];
  revenue_adjustment_impacts?: DataApiAdjustmentImpact[];
  adjusted_revenue?: DataApiAdjustedRevenue;
  quality_warnings?: string[];
  reason?: string | null;
  ttm?: { period_end: string; reporting_currency?: string; operations_scope?: DataApiOperationsScope | null; operations_scope_comparability_group?: string | null; revenue_display: { value: string; unit: string }; prior_year_revenue_display: { value: string; unit: string } | null; sources?: DataApiRevenueSource[]; prior_year_sources?: DataApiRevenueSource[] } | null;
}

interface Offering { ttmGrowthSourceUrls?: string[]; ttmThreeYearCagrPercent?: number | null; ttmYoyGrowthReason?: string | null; ttmThreeYearCagrReason?: string | null; ttmGrowthComparisons?: DataApiRevenueGrowthComparisons; ttmYoyGrowthPercent?: number | null; ttmThreePeriodCagrPercent?: number | null; id: string; label: string; axis?: string; member?: string; labelBasis?: string; revenueByPeriod: Record<string, number | null>; sourceByPeriod: Record<string, string>; sourcesByPeriod: Record<string, string[]>; provenanceByPeriod: Record<string, DataApiRevenueSource[]>; periodEnds: Record<string, string>; children?: Offering[] }
export function revenueOfferingStory(payload: DataApiRevenueOfferings | undefined) {
  if (!payload) return undefined;
  const sourceUrls = (sources?: DataApiRevenueSource[]) => [
    ...new Set((sources ?? []).flatMap(source => source.source_url ? [source.source_url] : [])),
  ];
  const firstSource = (sources?: DataApiRevenueSource[]) => sourceUrls(sources)[0];
  const growthSourceUrls = (comparison?: DataApiRevenueGrowthComparisons) => [...new Set(
    [comparison?.current, comparison?.prior_year_ttm, comparison?.three_year_prior_ttm]
      .flatMap(point => sourceUrls(point?.sources)),
  )];
  function offering(item: DataApiOffering): Offering {
    const revenueByPeriod: Record<string, number | null> = {};
    const sourceByPeriod: Record<string, string> = {};
    const sourcesByPeriod: Record<string, string[]> = {};
    const provenanceByPeriod: Record<string, DataApiRevenueSource[]> = {};
    const periodEnds: Record<string, string> = {};
    for (const [year, point] of Object.entries(item.history)) {
      revenueByPeriod[year] = finiteNumber(point.revenue_display.value);
      const sources = [...(point.sources ?? [])];
      if (!sources.length && point.source_url) sources.push({ source_url: point.source_url, accession: point.accession });
      provenanceByPeriod[year] = sources;
      const urls = sourceUrls(sources);
      if (urls[0]) sourceByPeriod[year] = urls[0];
      if (urls.length) sourcesByPeriod[year] = urls;
      if (point.period_end) periodEnds[year] = point.period_end;
    }
    if (item.ttm) {
      revenueByPeriod.TTM = finiteNumber(item.ttm.revenue_display.value);
      provenanceByPeriod.TTM = [...item.ttm.sources];
      if (item.ttm.period_end) periodEnds.TTM = item.ttm.period_end;
      const sources = sourceUrls(item.ttm.sources);
      const source = sources[0];
      if (source) sourceByPeriod.TTM = source;
      if (sources.length) sourcesByPeriod.TTM = sources;
    }
    return { ttmThreeYearCagrPercent: finiteNumber(item.ttm?.three_year_cagr_percent),
      ttmYoyGrowthReason: item.ttm?.growth_comparisons?.yoy_growth_percent?.reason_text,
      ttmThreeYearCagrReason: item.ttm?.growth_comparisons?.three_year_cagr_percent?.reason_text,
      ttmGrowthComparisons: item.ttm?.growth_comparisons,
      ttmGrowthSourceUrls: growthSourceUrls(item.ttm?.growth_comparisons),
      ttmYoyGrowthPercent: finiteNumber(item.ttm?.yoy_growth_percent), ttmThreePeriodCagrPercent: finiteNumber(item.ttm?.three_period_cagr_percent), id: item.id, label: item.label, axis: item.axis, member: item.member, labelBasis: item.label_basis, revenueByPeriod, sourceByPeriod, sourcesByPeriod, provenanceByPeriod, periodEnds, children: item.children?.map(offering) };
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
    reportedOfferings: payload.reported_offerings?.map(offering),
    reportedOfferingsMethodology: payload.reported_offerings_methodology,
    segmentReconciliation: payload.reported_segment_reconciliation ? {
      sourceType: payload.reported_segment_reconciliation.source_type,
      reportingBasis: payload.reported_segment_reconciliation.reporting_basis,
      periodType: payload.reported_segment_reconciliation.period_type,
      fiscalYear: payload.reported_segment_reconciliation.fiscal_year,
      history: Object.fromEntries(Object.entries(payload.reported_segment_reconciliation.history ?? {}).map(([year, point]) => [year, {
        sourceType: point.source_type,
        basis: point.basis,
        methodology: point.methodology,
        periodEnd: point.period_end,
        reportingBasis: point.reporting_basis,
        totalRevenue: finiteNumber(point.revenue_display.value),
        items: point.items.map(item => ({
          sourceType: item.source_type, id: item.id, label: item.label, role: item.role,
          revenue: finiteNumber(item.revenue_display.value), sourceUrls: sourceUrls(item.sources),
        yoyGrowthPercent: finiteNumber(item.yoy_growth_percent),
        threePeriodCagrPercent: finiteNumber(item.three_period_cagr_percent),
        threeYearCagrPercent: finiteNumber(item.three_year_cagr_percent),
        yoyGrowthReason: item.growth_comparisons?.yoy_growth_percent?.reason_text,
        threeYearCagrReason: item.growth_comparisons?.three_year_cagr_percent?.reason_text,
        growthComparisons: item.growth_comparisons,
        growthSourceUrls: growthSourceUrls(item.growth_comparisons),
        })),
      }])),
      periodEnd: payload.reported_segment_reconciliation.period_end,
      basis: payload.reported_segment_reconciliation.basis,
      methodology: payload.reported_segment_reconciliation.methodology,
      totalRevenue: finiteNumber(payload.reported_segment_reconciliation.revenue_display.value),
      items: payload.reported_segment_reconciliation.items.map(item => ({
        sourceType: item.source_type, id: item.id, label: item.label, role: item.role,
        revenue: finiteNumber(item.revenue_display.value), sourceUrls: sourceUrls(item.sources),
        yoyGrowthPercent: finiteNumber(item.yoy_growth_percent),
        threePeriodCagrPercent: finiteNumber(item.three_period_cagr_percent),
        threeYearCagrPercent: finiteNumber(item.three_year_cagr_percent),
        yoyGrowthReason: item.growth_comparisons?.yoy_growth_percent?.reason_text,
        threeYearCagrReason: item.growth_comparisons?.three_year_cagr_percent?.reason_text,
        growthComparisons: item.growth_comparisons,
        growthSourceUrls: growthSourceUrls(item.growth_comparisons),
      })),
    } : undefined,
    offeringAxis: payload.axis,
    annualTotals: Object.fromEntries(Object.entries(payload.annual_totals).map(([year, point]) => [year, finiteNumber(point.revenue_display.value)])),
    annualPeriodEnds: Object.fromEntries(Object.entries(payload.annual_totals).flatMap(([year, point]) => point.period_end ? [[year, point.period_end]] : [])),
    annualReportingBases: Object.fromEntries(Object.entries(payload.annual_totals).flatMap(([year, point]) => point.reporting_basis ? [[year, point.reporting_basis]] : [])),
    annualReportingCurrencies: Object.fromEntries(Object.entries(payload.annual_totals).flatMap(([year, point]) => {
      const currency = point.reporting_currency ?? currencyFromUnit(point.revenue_display.unit);
      return currency ? [[year, currency]] : [];
    })),
    ...(Object.values(payload.annual_totals).some(point => Object.hasOwn(point, "operations_scope")) ? {
      annualOperationsScopes: Object.fromEntries(Object.entries(payload.annual_totals)
        .map(([year, point]) => [year, point.operations_scope ?? null])),
      annualOperationsScopeGroups: Object.fromEntries(Object.entries(payload.annual_totals)
        .flatMap(([year, point]) => point.operations_scope_comparability_group ? [[year, point.operations_scope_comparability_group]] : [])),
    } : {}),
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
    ttm: payload.ttm && revenue !== null ? { periodEnd: payload.ttm.period_end, revenue, priorYearRevenue: finiteNumber(payload.ttm.prior_year_revenue_display?.value),
      reportingCurrency: payload.ttm.reporting_currency ?? currencyFromUnit(payload.ttm.revenue_display.unit) ?? undefined,
      ...(Object.hasOwn(payload.ttm, "operations_scope") ? {
        operationsScope: payload.ttm.operations_scope,
        operationsScopeGroup: payload.ttm.operations_scope_comparability_group,
      } : {}) } : undefined,
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

interface DataApiCostStoryPeriod {
  reporting_currency?: string | null;
  key: string;
  label: string;
  type: "annual" | "ttm";
  fiscal_year: number | null;
  period_end: string | null;
}

interface DataApiCostStoryValue {
  value: string | null;
  inputs: unknown[];
}

interface DataApiCostInterestValue extends DataApiCostStoryValue {
  status: "reported" | "not_reported";
  display_value: string | null;
  display_policy: string;
  percent_of_operating_income?: string | null;
}


interface DataApiCostStructure {
  schema_version?: number;
  annual_summary?: string | null;
  interest_summary?: string | null;
  interest_comparison_note?: string | null;
  median?: {
    status: "available" | "unavailable";
    reason: string | null;
    periods: DataApiCostStoryPeriod[];
    lines: { key: string; label: string; role: "revenue" | "expense" | "subtotal";
      value_per_hundred: string | null; observation_count: number; reason: string | null; inputs: unknown[] }[];
  };
  annual_breakdown?: {
    display_unit: string;
    scale: string;
    periods: DataApiCostStoryPeriod[];
    lines: { key: string; label: string; role: "revenue" | "expense" | "subtotal"; values: Record<string, DataApiCostStoryValue> }[];
    operating_margin_percent: Record<string, string | null>;
  };
  interest_comparison?: { key?: string; label?: string; type?: "annual" | "ttm";
    fiscal_year: number | null; period_end: string | null; reporting_currency?: string | null;
    operating_income: DataApiCostInterestValue; interest_expense: DataApiCostInterestValue;
    interest_income: DataApiCostInterestValue; net_interest_other_income_expense?: DataApiCostInterestValue }[];
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
  disclosure?: string;
  working_capital_calculation?: DataApiNcwcCalculation;
}

interface DataApiNcwcCalculation {
  period_type?: string;
  prior_period_label?: string;
  current_period_label?: string;
  cash_input_policy?: string;
  status: "available" | "unavailable";
  reason: string | null;
  formula: string;
  prior_fiscal_year: number;
  current_fiscal_year: number;
  prior_period_end: string | null;
  current_period_end: string | null;
  prior_ncwc: string | null;
  current_ncwc: string | null;
  prior_operating_current_assets: string | null;
  current_operating_current_assets: string | null;
  cash_effect: string | null;
  inputs: Array<{
    key: "current_assets" | "cash_input" | "current_liabilities" | "current_debt";
    label: string;
    operator: "" | "−" | "+";
    prior_value: string | null;
    current_value: string | null;
    prior_detail_status: string;
    current_detail_status: string;
    prior_note: string | null;
    current_note: string | null;
    components: Array<{
      key: string; label: string; prior_value: string | null; current_value: string | null;
      prior_source_url: string | null; current_source_url: string | null;
    }>;
  }>;
}

interface DataApiCashConversionTrendPoint {
  reporting_currency?: string;
  fiscal_year: number;
  period_end: string;
  ebit: string;
  tax_rate_percent: string;
  taxes_on_operating_profit: string;
  nopat: string;
  depreciation_and_amortization: string;
  working_capital_impact: string;
  working_capital_components: DataApiCashConversionComponent[];
  working_capital_calculation?: DataApiNcwcCalculation;
  capital_expenditure: string;
  fcff: string;
  conversion_percent: string | null;
  revenue: string;
  fcff_revenue_percent: string | null;
}

interface DataApiCashConversionPeriodGap {
  fiscal_year: number;
  period_end: string | null;
  reporting_basis: "standalone" | "consolidated" | null;
  reporting_currency: string | null;
  status: "unavailable";
  reason_code: string;
  reason: string;
  missing_inputs: string[];
  source_urls: string[];
}

interface DataApiCashConversion {
  latest_statement_fiscal_year?: number;
  display_window?: { annual_years: number[] };
  ttm?: {
    status: "available" | "unavailable"; reason: string | null;
    period_start?: string; period_end?: string; display_unit?: string;
    earnings_basis?: string; working_capital_calculation?: DataApiNcwcCalculation;
    partial_bridge?: DataApiCashConversion["partial_bridge"];
    snapshot?: DataApiCashConversionTrendPoint;
    bridge?: DataApiCashConversionBridgeLine[];
    metrics?: Partial<NonNullable<DataApiCashConversion["metrics"]>>;
    missing_inputs: string[]; methodology?: string;
  };
  partial_bridge?: {
    fiscal_year: number;
    lines: (Omit<DataApiCashConversionBridgeLine, "value"> & { value: string | null })[];
  };
  earnings_basis?: string;
  working_capital_calculation?: DataApiNcwcCalculation;
  period_gaps?: DataApiCashConversionPeriodGap[];
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
  ratios: "Ratios",
  kpis: "KPIs",
} as const;

function requireConfiguration(env: DataProviderEnv): { base: string; key: string } {
  const base = String(env.TARASHA_DATA_API_URL ?? "").replace(/\/$/, "");
  const key = String(env.TARASHA_DATA_API_KEY ?? "").trim();
  if (!base) throw new Error("TaRaShaData.ai API provider is not configured.");
  return { base, key };
}

function dataErrorReason(value: unknown): string | undefined {
  if (typeof value === "string" && value.trim()) return value;
  if (value && typeof value === "object" && "reason" in value
    && typeof value.reason === "string" && value.reason.trim()) return value.reason;
  return undefined;
}

class DataApiResponseError extends Error {
  constructor(message: string, readonly status: number, readonly detail: unknown) {
    super(message);
  }
}

function dataRequestTimedOut(value: unknown, depth = 0): boolean {
  if (!value || typeof value !== "object" || depth > 2) return false;
  const error = value as { name?: string; code?: string; cause?: unknown };
  return error.name === "TimeoutError"
    || ["ETIMEDOUT", "UND_ERR_CONNECT_TIMEOUT", "UND_ERR_HEADERS_TIMEOUT", "UND_ERR_BODY_TIMEOUT"].includes(error.code ?? "")
    || dataRequestTimedOut(error.cause, depth + 1);
}

function preparedStoryDelivery(response: Response, payload: unknown): PreparedStoryDelivery | undefined {
  const preparedAtUtc = response.headers.get("X-TaRaShaData-Prepared-At");
  const latest = response.headers.get("X-TaRaShaData-Is-Latest");
  const status = response.headers.get("X-TaRaShaData-Latest-Status");
  if (preparedAtUtc === null && latest === null && status === null) return undefined;
  const snapshotId = response.headers.get("X-TaRaShaData-Snapshot");
  const bodySnapshot = payload && typeof payload === "object"
    ? (payload as DataApiDiscoverDataset).prepared_financial_snapshot?.snapshot_id : undefined;
  if (!preparedAtUtc || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)$/.test(preparedAtUtc)
    || !Number.isFinite(Date.parse(preparedAtUtc)) || !snapshotId || snapshotId !== bodySnapshot
    || !["true", "false"].includes(latest ?? "")
    || !["ready", "preparing", "failed", "unknown"].includes(status ?? "")
    || (latest === "true" && status !== "ready")) {
    throw new Error("TaRaShaData.ai returned invalid prepared story delivery details.");
  }
  return { preparedAtUtc, snapshotId, isLatest: latest === "true",
    latestStatus: status as PreparedStoryDelivery["latestStatus"] };
}

async function dataFetch<T>(env: DataProviderEnv, path: string, init?: RequestInit, timeoutMs?: number): Promise<T> {
  const { base, key } = requireConfiguration(env);
  const headers = new Headers(init?.headers);
  headers.set("Accept", "application/json");
  if (key) headers.set("Authorization", `Bearer ${key}`);
  const callerSignal = init?.signal;
  const deadline = timeoutMs === undefined ? null : new AbortController();
  let timedOut = false;
  let callerCancelled = false;
  const cancelFromCaller = () => {
    if (deadline?.signal.aborted) return;
    callerCancelled = true;
    deadline?.abort(callerSignal?.reason);
  };
  if (deadline && callerSignal) {
    if (callerSignal.aborted) cancelFromCaller();
    else callerSignal.addEventListener("abort", cancelFromCaller, { once: true });
  }
  const timer = deadline ? setTimeout(() => {
    if (deadline.signal.aborted) return;
    timedOut = true;
    deadline.abort();
  }, timeoutMs) : undefined;
  const requestFailure = (cause: unknown) => {
    if (callerCancelled || (!deadline && callerSignal?.aborted)) {
      return callerSignal?.reason ?? cause;
    }
    if (timedOut || dataRequestTimedOut(cause)) return new Error("TaRaShaData.ai request timed out. Please try again.");
    return new Error("TaRaShaData.ai is not reachable. Verify that the configured TaRaShaData service is running, then try again.");
  };
  try {
    let response: Response;
    try {
      response = await fetch(`${base}${path}`, {
        ...init,
        headers,
        ...(deadline ? { signal: deadline.signal } : {}),
      });
    } catch (cause) { throw requestFailure(cause); }
    if (!response.ok) {
      const payload = await response.json().catch((cause: unknown) => {
        if (timedOut || callerCancelled || callerSignal?.aborted) throw requestFailure(cause);
        return {};
      }) as { detail?: unknown; error?: unknown };
      throw new DataApiResponseError(dataErrorReason(payload.detail) || dataErrorReason(payload.error)
        || `TaRaShaData.ai API returned ${response.status}.`, response.status, payload.detail);
    }
    try {
      const payload = await response.json() as T;
      if (path === "/v1/discover/company-dataset") {
        const delivery = preparedStoryDelivery(response, payload);
        if (delivery) return { ...payload, prepared_story_delivery: delivery };
      }
      return payload;
    }
    catch (cause) {
      if (timedOut || callerCancelled || callerSignal?.aborted) throw requestFailure(cause);
      throw cause;
    }
  } finally {
    if (timer !== undefined) clearTimeout(timer);
    callerSignal?.removeEventListener("abort", cancelFromCaller);
  }
}

function waitForSavedSelection(signal?: AbortSignal | null): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) { reject(signal.reason); return; }
    const cancel = () => { clearTimeout(timer); reject(signal?.reason); };
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", cancel);
      resolve();
    }, 1000);
    signal?.addEventListener("abort", cancel, { once: true });
  });
}

// Large prepared views retain their complete SEC calculation provenance. Allow
// their transfer within the one-minute story-opening target, reserving time for
// presentation mapping. Compact status and retry requests remain bounded at 4s.
const PREPARED_BODY_TIMEOUT_MS = 45_000;
// Bound the complete opening attempt, including preparation and snapshot races.
// Per-request deadlines alone do not stop an indefinitely preparing selection.
const PREPARED_SELECTION_TIMEOUT_MS = 90_000;

async function preparedDatasetFetch(env: DataProviderEnv, init: RequestInit): Promise<DataApiDiscoverDataset> {
  const deadline = new AbortController();
  const cancelFromCaller = () => deadline.abort(init.signal?.reason);
  if (init.signal?.aborted) cancelFromCaller();
  else init.signal?.addEventListener("abort", cancelFromCaller, { once: true });
  const timer = setTimeout(() => deadline.abort(new Error(
    "This company view is taking longer than expected to load. Preparation may still be running. Please try again shortly.",
  )), PREPARED_SELECTION_TIMEOUT_MS);
  try {
    return await preparedDatasetFetchBeforeDeadline(env, { ...init, signal: deadline.signal });
  } finally {
    clearTimeout(timer);
    init.signal?.removeEventListener("abort", cancelFromCaller);
  }
}

async function preparedDatasetFetchBeforeDeadline(env: DataProviderEnv, init: RequestInit): Promise<DataApiDiscoverDataset> {
  let pending: { code?: string; status_url?: string; retry_url?: string };
  try { return await dataFetch<DataApiDiscoverDataset>(env, "/v1/discover/company-dataset", init, PREPARED_BODY_TIMEOUT_MS); }
  catch (error) {
    if (!(error instanceof DataApiResponseError) || error.status !== 503
      || !error.detail || typeof error.detail !== "object") throw error;
    pending = error.detail as typeof pending;
    if (!["selection_preparing", "selection_failed"].includes(pending.code ?? "")
      || typeof pending.status_url !== "string"
      || !/^\/v1\/discover\/selections\/[a-f0-9]{64}$/.test(pending.status_url)) throw error;
    if (pending.code === "selection_failed") {
      // A new user-initiated pull can retry saved-source preparation once.
      // Failure during the following polling cycle remains an explicit error.
      if (pending.retry_url !== `${pending.status_url}/retry`) throw error;
      await dataFetch(env, pending.retry_url, { method: "POST", signal: init.signal }, 4000);
    }
  }
  const key = pending.status_url!.split("/").at(-1)!;
  let preparingPolls = 0;
  for (;;) {
    await waitForSavedSelection(init.signal);
    const state = await dataFetch<{
      state: string; request_key?: string; reader_armed_ready?: boolean;
      snapshot_id?: string; error_message?: string;
    }>(env, pending.status_url!, { signal: init.signal }, 4000);
    if (state.request_key !== key) throw new Error("Saved financial selection does not match this request.");
    if (["failed", "unavailable"].includes(state.state)) {
      throw new Error(state.error_message || "The selected saved financial view could not be prepared. Please try again.");
    }
    const readRestoredStory = state.state === "preparing";
    // Startup can restore a verified previous page while a newer generation is
    // still preparing. Recheck the exact body periodically without retrying the
    // preparation job or treating the new generation as ready.
    if (readRestoredStory && ++preparingPolls % 3 !== 0) continue;
    if (!readRestoredStory && (state.state !== "ready" || state.reader_armed_ready !== true)) {
      throw new Error("The selected saved financial view returned an invalid readiness state.");
    }
    try {
      const body = await dataFetch<DataApiDiscoverDataset>(env, "/v1/discover/company-dataset", init, PREPARED_BODY_TIMEOUT_MS);
      if (body.prepared_financial_snapshot?.key === key
        && typeof body.prepared_financial_snapshot.snapshot_id === "string"
        && body.prepared_financial_snapshot.snapshot_id.trim()
        && (readRestoredStory || body.prepared_financial_snapshot.snapshot_id === state.snapshot_id)) return body;
      if (readRestoredStory) throw new Error("Saved financial selection does not match this request.");
      // A newer source can arrive between readiness and the exact body read.
      // Keep waiting for a matched revision; never display a different snapshot.
    } catch (error) {
      if (!(error instanceof DataApiResponseError) || error.status !== 503
        || !error.detail || typeof error.detail !== "object"
        || (error.detail as { code?: string }).code !== "selection_preparing") throw error;
    }
  }
}

export async function fetchDataCompanyLogo(env: DataProviderEnv, companyId: string): Promise<Response> {
  const identifier = companyId.replace(/^data-/, "").trim();
  if (!/^(?:[0-9]{10}|IN:(?:[A-Z0-9]+|ISIN:IN[A-Z0-9]{10}))$/.test(identifier)) {
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
  const safeQuery = query.replace(/[^a-zA-Z0-9 .:&-]/g, "").trim().slice(0, 60);
  if (safeQuery.length < 2) return [];
  const params = new URLSearchParams({
    q: safeQuery,
    country,
    limit: "30",
    audience: "discover",
  });
  const rows = await dataFetch<DataApiSearchCompany[]>(env, `/v1/companies/search?${params}`, { cache: "no-store" });
  if (rows.some((row) => row.ticker && !row.normalized_coverage)) {
    throw new Error(
      "TaRaShaData.ai is running an older Discover API contract. Restart the TaRaShaData service, then search again.",
    );
  }
  return rows.map(row => ({ ...row, preparedDefaultRequest: readPreparedDefaultRequest(
    row.prepared_default_request, row.identifier ?? row.cik ?? "",
  ) }))
    .filter(
      (row): row is DataApiSearchCompany & {
        ticker: string;
        normalized_coverage: DataApiNormalizedCoverage;
        preparedDefaultRequest: PreparedDefaultRequest | undefined;
      } => Boolean(row.ticker && row.normalized_coverage
        && (row.normalized_coverage.available || row.preparedDefaultRequest)),
    )
    .sort((left, right) => Number(left.ticker!.toLowerCase() !== safeQuery.toLowerCase()) - Number(right.ticker!.toLowerCase() !== safeQuery.toLowerCase()) || left.name.localeCompare(right.name))
    .map((row) => ({
      id: `data-${row.identifier ?? row.cik}`,
      cik: row.cik,
      name: row.name,
      ticker: row.ticker!,
      exchange: row.exchange || "",
      country: (row.country === "India" ? "India" : "USA") as "USA" | "India",
      provider: "TaRaShaData.ai API",
      industryBucket: row.industry_buckets?.[0]?.name,
      data_available: Number(row.normalized_coverage.available),
      data_access: "normalized" as const,
      firstFiscalYear: row.normalized_coverage.first_fiscal_year ?? undefined,
      latestFiscalYear: row.normalized_coverage.latest_fiscal_year ?? undefined,
      ...(row.preparedDefaultRequest ? { preparedDefaultRequest: row.preparedDefaultRequest } : {}),
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

function costStoryPeriod(period: DataApiCostStoryPeriod) {
  return { key: period.key, label: period.label, type: period.type,
    fiscalYear: period.fiscal_year, periodEnd: period.period_end, reportingCurrency: period.reporting_currency };
}

function costActualValue(cell: DataApiCostStoryValue) {
  const raw = finiteNumber(cell.value);
  return { value: raw === null ? null : raw / 1_000_000, inputs: cell.inputs };
}

function costInterestValue(cell: DataApiCostInterestValue) {
  const display = cell.status === "not_reported" ? null : finiteNumber(cell.display_value);
  return { ...costActualValue(cell), status: cell.status,
    displayValue: display === null ? null : display / 1_000_000,
    displayPolicy: cell.display_policy,
    percentOfOperatingIncome: cell.status === "not_reported" ? null : finiteNumber(cell.percent_of_operating_income) };
}


export function costStructureStory(payload: DataApiCostStructure | undefined, reportingCurrency: string) {
  const unavailable = {
    status: "unavailable" as const,
    schemaVersion: undefined,
    annualSummary: undefined,
    interestSummary: undefined,
    median: undefined,
    annualBreakdown: undefined,
    interestComparison: undefined,
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
    schemaVersion: payload.schema_version,
    annualSummary: payload.annual_summary,
    interestSummary: payload.interest_summary,
    interestComparisonNote: payload.interest_comparison_note,
    median: payload.median ? {
      status: payload.median.status, reason: payload.median.reason,
      periods: payload.median.periods.map(costStoryPeriod),
      lines: payload.median.lines.map(line => ({
        key: line.key, label: line.label, role: line.role,
        valuePerHundred: finiteNumber(line.value_per_hundred),
        observationCount: line.observation_count, reason: line.reason, inputs: line.inputs,
      })),
    } : undefined,
    annualBreakdown: payload.annual_breakdown ? {
      displayUnit: payload.annual_breakdown.display_unit,
      periods: payload.annual_breakdown.periods.map(costStoryPeriod),
      lines: payload.annual_breakdown.lines.map(line => ({ ...line,
        values: Object.fromEntries(Object.entries(line.values).map(([year, cell]) => [year, costActualValue(cell)])),
      })),
      operatingMarginPercent: Object.fromEntries(Object.entries(payload.annual_breakdown.operating_margin_percent).map(([year, value]) => [year, finiteNumber(value)])),
    } : undefined,
    interestComparison: payload.interest_comparison?.map(point => ({
      key: point.key, label: point.label, type: point.type,
      fiscalYear: point.fiscal_year, periodEnd: point.period_end, reportingCurrency: point.reporting_currency,
      operatingIncome: costInterestValue(point.operating_income),
      interestExpense: costInterestValue(point.interest_expense),
      interestIncome: costInterestValue(point.interest_income),
      netInterestOtherIncomeExpense: point.net_interest_other_income_expense
        ? costInterestValue(point.net_interest_other_income_expense) : undefined,
    })),
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

function ncwcCalculation(payload: DataApiNcwcCalculation | undefined) {
  if (!payload) return undefined;
  return {
    status: payload.status, reason: payload.reason, formula: payload.formula,
    cashInputPolicy: payload.cash_input_policy,
    periodType: payload.period_type, priorPeriodLabel: payload.prior_period_label, currentPeriodLabel: payload.current_period_label,
    priorFiscalYear: payload.prior_fiscal_year, currentFiscalYear: payload.current_fiscal_year,
    priorPeriodEnd: payload.prior_period_end, currentPeriodEnd: payload.current_period_end,
    priorNcwc: finiteNumber(payload.prior_ncwc), currentNcwc: finiteNumber(payload.current_ncwc),
    priorOperatingCurrentAssets: finiteNumber(payload.prior_operating_current_assets),
    currentOperatingCurrentAssets: finiteNumber(payload.current_operating_current_assets),
    cashEffect: finiteNumber(payload.cash_effect),
    inputs: payload.inputs.map(input => ({
      key: input.key, label: input.label, operator: input.operator,
      priorValue: finiteNumber(input.prior_value), currentValue: finiteNumber(input.current_value),
      priorDetailStatus: input.prior_detail_status, currentDetailStatus: input.current_detail_status,
      priorNote: input.prior_note, currentNote: input.current_note,
      components: input.components.map(component => ({
        key: component.key, label: component.label,
        priorValue: finiteNumber(component.prior_value), currentValue: finiteNumber(component.current_value),
        priorSourceUrl: component.prior_source_url, currentSourceUrl: component.current_source_url,
      })),
    })),
  };
}

function partialCashBridge(payload: DataApiCashConversion["partial_bridge"]) {
  return payload ? {
      fiscalYear: payload.fiscal_year,
      lines: payload.lines.map(line => ({
        key: line.key, label: line.label, operation: line.operation,
        value: finiteNumber(line.value), formula: line.formula,
        lineage: cashConversionLineage(line.lineage),
        components: cashConversionComponents(line.components), disclosure: line.disclosure,
        workingCapitalCalculation: ncwcCalculation(line.working_capital_calculation),
      })),
  } : undefined;
}

function annualCashConversionStory(payload: DataApiCashConversion | undefined, reportingCurrency: string) {
  const unavailable = {
    status: "unavailable" as const,
    earningsBasis: undefined as string | undefined,
    partialBridge: undefined as ReturnType<typeof partialCashBridge>,
    reason: "TaRaShaData did not return a complete annual operating-profit-to-FCFF bridge for the selected years.",
    workingCapitalCalculation: undefined as ReturnType<typeof ncwcCalculation>,
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
    workingCapitalCalculation: ncwcCalculation(payload.working_capital_calculation),
    earningsBasis: payload.earnings_basis,
    partialBridge: partialCashBridge(payload.partial_bridge),
    ...(payload.period_gaps ? { periodGaps: payload.period_gaps.flatMap(gap =>
      Number.isInteger(gap.fiscal_year) && gap.status === "unavailable" ? [{
        fiscalYear: gap.fiscal_year,
        periodEnd: gap.period_end,
        reportingBasis: gap.reporting_basis,
        reportingCurrency: gap.reporting_currency,
        status: gap.status,
        reasonCode: gap.reason_code,
        reason: gap.reason,
        missingInputs: gap.missing_inputs,
        sourceUrls: gap.source_urls,
      }] : []) } : {}),
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
        disclosure: line.disclosure,
        workingCapitalCalculation: ncwcCalculation(line.working_capital_calculation),
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
        reportingCurrency: point.reporting_currency,
        ebit: ebit!,
        taxRatePercent: taxRatePercent!,
        taxesOnOperatingProfit: taxesOnOperatingProfit!,
        nopat: nopat!,
        depreciationAndAmortization: depreciationAndAmortization!,
        workingCapitalImpact: workingCapitalImpact!,
        workingCapitalComponents: cashConversionComponents(point.working_capital_components),
        workingCapitalCalculation: ncwcCalculation(point.working_capital_calculation),
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

function cashConversionStory(payload: DataApiCashConversion | undefined, reportingCurrency: string) {
  const annual = annualCashConversionStory(payload, reportingCurrency);
  const ttm = payload?.ttm;
  return {
    ...annual,
    latestStatementFiscalYear: payload?.latest_statement_fiscal_year,
    displayAnnualYears: payload?.display_window?.annual_years.filter(Number.isInteger),
    ttm: ttm && payload ? {
      ...annualCashConversionStory({
        ...payload, status: ttm.status, reason: ttm.reason,
        earnings_basis: ttm.earnings_basis,
        latest_fiscal_year: ttm.period_end ? Number(ttm.period_end.slice(0, 4)) : null,
        latest_period_end: ttm.period_end ?? null,
        display_unit: ttm.display_unit ?? payload.display_unit,
        years: [], bridge: ttm.bridge ?? [], trend: ttm.snapshot ? [ttm.snapshot] : [],
        partial_bridge: ttm.partial_bridge,
        working_capital_calculation: ttm.working_capital_calculation,
        period_gaps: [], summary: null, takeaway: null,
        methodology: ttm.methodology ?? payload.methodology,
        source_url: null,
        metrics: ttm.metrics ? {
          fcff: null, prior_fiscal_year: null, prior_fcff: null,
          conversion_percent: null, prior_conversion_percent: null,
          growth_percent: null, prior_growth_percent: null,
          revenue_percent: null, prior_revenue_percent: null, cagr_percent: null,
          ...ttm.metrics,
        } : null,
      }, reportingCurrency),
      periodStart: ttm.period_start ?? null,
      missingInputs: ttm.missing_inputs ?? [],
    } : undefined,
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
    status: payload?.status === "pending" ? "unavailable" as const : payload?.status ?? "unavailable" as const,
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
    status: payload?.status === "pending" ? "unavailable" as const : payload?.status ?? "unavailable" as const,
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
    status: payload?.status === "pending" ? "unavailable" as const : payload?.status ?? "unavailable" as const,
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
  return left.flatMap((item) => {
    if (!rightByYear.has(item.year)) return [];
    const value = operation(item.value, rightByYear.get(item.year)!);
    return Number.isFinite(value) ? [{ year: item.year, value }] : [];
  });
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
  /** Pure decimal ratio: 0.005 means 0.5%, and 2 means 200%. */
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
  return value;
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
  const componentTotal = shortTermBorrowings !== null && currentPortionLongTermDebt !== null
    ? shortTermBorrowings + currentPortionLongTermDebt : null;
  // Reported components do not prove a complete debt total. The canonical API
  // aggregate is required, including an explicitly reported aggregate of zero.
  const totalInterestBearingCurrentDebt = aggregateCurrentDebt;
  const otherInterestBearingCurrentDebt = aggregateCurrentDebt !== null && componentTotal !== null
    ? aggregateCurrentDebt - componentTotal : null;
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
  const netBorrowing = finiteValueForYear(series.netBorrowing, year);
  const fcfe = netIncomeToCommon !== null && da !== null && shareBasedCompensation !== null && otherAdjustments !== null && capex !== null && workingCapitalImpact !== null && netBorrowing !== null
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
  return debt.filter((item) => cashByYear.has(item.year) && investmentsByYear.has(item.year)).map((item) => ({
    year: item.year,
    value: item.value - cashByYear.get(item.year)! - investmentsByYear.get(item.year)!,
  }));
}

function firstAvailableSeries(facts: DataFactRow[], keys: string[]): YearValue[] {
  for (const key of keys) {
    const series = seriesFor(facts, key);
    if (series.length) return series;
  }
  return [];
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
  return value * 100;
}

function latestPointAtOrBefore(series: YearValue[], year: number | null): YearValue | null {
  if (year === null) return null;
  return [...series]
    .filter((point) => point.year <= year && Number.isFinite(point.value))
    .sort((left, right) => right.year - left.year)[0] ?? null;
}

function buildBalanceSheetStory(
  facts: DataFactRow[],
  dataset: DataApiDataset,
  reportingCurrency: string,
  displayUnit: string,
) {
  const indiaListed = dataset.company.country === "India";
  const proofs = new Map<YearValue[], Map<number, DataFactRow[]>>();
  const reported = (keys: string[]) => {
    // Alias precedence is period-specific: a newer aggregate must not erase
    // separately reported historical periods from an older supported alias.
    const years = [...new Set(facts.filter(row => keys.includes(row.fact_key)).map(row => row.fiscal_year))].sort((a, b) => a - b);
    const sources = new Map<number, DataFactRow[]>();
    const values = years.flatMap(year => {
      const row = keys.map(key => facts.find(fact => fact.fact_key === key && fact.fiscal_year === year)).find(Boolean);
      if (!row) return [];
      sources.set(year, [row]);
      return [{ year, value: row.value }];
    });
    proofs.set(values, sources);
    return values;
  };
  // Rate calculations require an explicit source unit, never a size-based guess.
  const reportedRate = (keys: string[]) => {
    const rows = facts.flatMap(row => row.unit_kind === "ratio" ? [row]
      : row.unit_kind === "percent" ? [{ ...row, value: row.value / 100 }] : []);
    return firstAvailableSeries(rows, keys);
  };
  const compatible = (series: YearValue[][], year: number) => {
    const sources = series.map(values => proofs.get(values)?.get(year) ?? []);
    if (sources.some(values => !values.length)) return false;
    const rows = sources.flat();
    return ["period_end", "reporting_basis", "reporting_currency", "display_scale"].every(key => {
      const values = rows.map(row => row[key as "period_end" | "reporting_basis" | "reporting_currency" | "display_scale"]);
      return values.every(Boolean) && new Set(values).size === 1;
    });
  };
  const combine = (series: YearValue[][], operation = (values: number[]) => values.reduce((total, value) => total + value, 0)) => {
    const years = [...new Set(series.flatMap(values => values.map(point => point.year)))].sort((left, right) => left - right);
    const output = years.flatMap(year => series.every(values => valueForYear(values, year) !== null) && compatible(series, year)
      ? [{ year, value: operation(series.map(values => valueForYear(values, year)!)) }] : []);
    proofs.set(output, new Map(output.map(point => [point.year, series.flatMap(values => proofs.get(values)?.get(point.year) ?? [])])));
    return output;
  };
  const coalesce = (primary: YearValue[], fallback: YearValue[]) => {
    const output = coalesceSeries(primary, fallback);
    proofs.set(output, new Map(output.map(point => [point.year,
      proofs.get(valueForYear(primary, point.year) !== null ? primary : fallback)?.get(point.year) ?? []])));
    return output;
  };
  const assets = reported(["assets"]);
  const equity = reported(["equity", "shareholders_equity"]);
  const derivedLiabilities = combine([assets, equity], ([totalAssets, shareholdersEquity]) => totalAssets - shareholdersEquity);
  const liabilities = coalesce(reported(["liabilities", "total_liabilities"]), derivedLiabilities);
  const availableEquationYears = assets
    .map((point) => point.year)
    .filter((year) => valueForYear(liabilities, year) !== null && valueForYear(equity, year) !== null && compatible([assets, liabilities, equity], year))
    .sort((left, right) => left - right);
  const sourceAnnualCurrencies = indiaListed ? annualReportingCurrenciesByYear(dataset) : {};
  const equationYears = indiaListed
    ? [...new Set(facts.filter(fact => fact.statement_key === "balance" && fact.period_end && sourceAnnualCurrencies[String(fact.fiscal_year)]).map(fact => fact.fiscal_year))].sort((left, right) => left - right)
    : [...new Set(facts.filter(fact => fact.statement_key === "balance" && fact.period_end).map(fact => fact.fiscal_year))]
      .sort((left, right) => left - right).slice(-5);
  const latestFiscalYear = equationYears.at(-1) ?? null;
  const accountingEquationAvailable = latestFiscalYear !== null && availableEquationYears.includes(latestFiscalYear);

  const reportedCashCushion = reported(["totalCash", "cashAndShortTermInvestments", "total_cash", "cash_and_short_term_investments"]);
  const cashAndEquivalents = reported(["cash"]);
  const shortTermInvestments = reported(["shortTermInvestments", "short_term_investments"]);
  const shortTermInvestmentLabel = facts.find(row => ["shortTermInvestments", "short_term_investments"].includes(row.fact_key)
    && row.fiscal_year === latestFiscalYear)?.label ?? "Short-term investments";
  const cash = indiaListed ? reportedCashCushion
    : coalesce(reportedCashCushion, combine([cashAndEquivalents, shortTermInvestments]));
  const accountsReceivable = reported(["accountsReceivable", "accounts_receivable"]);
  const inventory = reported(["inventory"]);
  const prepaidExpenses = reported(["prepaidExpenses", "prepaid_expenses"]);
  const otherCurrentAssets = reported(["other_current_assets"]);
  const reportedPrepaidsAndOther = reported(["prepaid_expenses_and_other_current_assets"]);
  const otherReceivables = reported(["other_receivables"]);
  const currentAssets = reported(["currentAssets", "current_assets"]);
  // These two authoritative totals establish the full operating-current-asset
  // group, including categories absent from the separate reported detail rows.
  // Details retain their own scopes and are never blindly added together.
  const operatingCurrentAssets = combine([currentAssets, cash], ([total, cushion]) => total - cushion);
  const propertyPlantEquipment = reported(["propertyPlantEquipment", "property_plant_equipment_net", "property_plant_and_equipment_net"]);
  const goodwill = reported(["goodwill"]);
  const otherIntangibleAssets = reported(["otherIntangibleAssets", "intangible_assets_net_excluding_goodwill", "finite_lived_intangible_assets_net"]);

  const unearnedRevenueCurrent = reported(["unearnedRevenueCurrent", "unearned_revenue", "deferred_revenue_current", "contract_liabilities_current"]);
  const unearnedRevenueLongTerm = reported(["unearnedRevenueLongTerm", "unearned_revenue_noncurrent", "deferred_revenue_noncurrent", "contract_liabilities_noncurrent"]);
  const unearnedRevenue = combine([unearnedRevenueCurrent, unearnedRevenueLongTerm]);
  const accountsPayable = reported(["accountsPayable", "accounts_payable"]);
  const totalDebtReported = reported(["totalDebt", "total_debt"]);
  const shortTermDebt = reported(["currentDebt", "shortTermBorrowings", "short_term_debt"]);
  const longTermDebt = reported(["longTermDebt", "long_term_debt"]);
  const shortTermLeases = reported(["shortTermLeaseLiabilities", "current_lease_liabilities", "lease_liabilities_current", "operating_lease_liabilities_current", "finance_lease_liabilities_current"]);
  const longTermLeases = reported(["longTermLeaseLiabilities", "long_term_leases", "lease_liabilities_noncurrent", "operating_lease_liabilities_noncurrent", "finance_lease_liabilities_noncurrent"]);
  // Lease and borrowing detail scopes can overlap. Only the API's complete,
  // source-verified debt aggregate establishes the total.
  const totalDebt = totalDebtReported;
  const shortTermDebtRate = reportedRate(["shortTermDebtWeightedAverageInterestRate", "short_term_debt_weighted_average_interest_rate", "short_term_debt_interest_rate", "short_term_borrowing_rate"]);
  const reportedDebtRate = reportedRate(["debtWeightedAverageInterestRate", "debt_weighted_average_interest_rate", "long_term_debt_interest_rate", "long_term_borrowing_rate"]);
  const operatingLeaseRate = reportedRate(["operatingLeaseWeightedAverageDiscountRate", "operating_lease_weighted_average_discount_rate", "lease_discount_rate"]);
  const financeLeaseRate = reportedRate(["financeLeaseWeightedAverageDiscountRate", "finance_lease_weighted_average_discount_rate"]);
  const shortTermLeaseRate = operatingLeaseRate.length ? operatingLeaseRate : financeLeaseRate;
  const longTermLeaseRate = operatingLeaseRate.length ? operatingLeaseRate : financeLeaseRate;

  const commonStock = reported(["commonStock", "common_stock", "common_stock_value"]);
  const additionalPaidInCapital = reported(["additionalPaidInCapital", "additional_paid_in_capital"]);
  const combinedCommonStockAndPaidInCapital = reported(["commonStockAndPaidInCapital", "common_stock_and_additional_paid_in_capital"]);
  const retainedEarnings = reported(["retainedEarnings", "retained_earnings", "retained_earnings_accumulated_deficit"]);
  const comprehensiveIncome = reported(["comprehensiveIncome", "accumulated_other_comprehensive_income", "accumulated_other_comprehensive_income_loss"]);

  const ebitda = reported(["ebitda"]);
  const ebit = reported(["ebit", "operatingIncome"]);
  const interestExpense = reported(["interestExpense", "interest_expense"]);
  const weightedAverageCost = reportedRate(["weighted_average_cost_of_debt", "weighted_average_interest_rate_debt"]);

  const latestAssets = valueForYear(assets, latestFiscalYear);
  const latestLiabilities = valueForYear(liabilities, latestFiscalYear);
  const latestEquity = valueForYear(equity, latestFiscalYear);
  const component = (
    key: string,
    label: string,
    series: YearValue[],
    base: YearValue[],
    children: Array<{ key: string; label: string; series: YearValue[]; rate?: YearValue[] }> = [],
    unavailableReason?: string,
  ) => ({
    key,
    label,
    value: valueForYear(series, latestFiscalYear),
    ...(valueForYear(series, latestFiscalYear) === null && unavailableReason ? { unavailableReason } : {}),
    percentageOfBase: latestFiscalYear !== null && compatible([series, base], latestFiscalYear)
      ? percentOfBase(valueForYear(series, latestFiscalYear), valueForYear(base, latestFiscalYear)) : null,
    trend: equationYears.flatMap((fiscalYear) => {
      const value = valueForYear(series, fiscalYear);
      return value === null ? [] : [{
        fiscalYear,
        value,
        percentageOfBase: compatible([series, base], fiscalYear) ? percentOfBase(value, valueForYear(base, fiscalYear)) : null,
      }];
    }),
    children: children.map((child) => ({
      key: child.key,
      label: child.label,
      value: valueForYear(child.series, latestFiscalYear),
      percentageOfBase: latestFiscalYear !== null && compatible([child.series, base], latestFiscalYear)
        ? percentOfBase(valueForYear(child.series, latestFiscalYear), valueForYear(base, latestFiscalYear)) : null,
      interestRatePercent: normalizedPercentValue(valueForYear(child.rate ?? [], latestFiscalYear)),
    })),
  });

  const latestCash = valueForYear(cash, latestFiscalYear);
  const latestDebt = valueForYear(totalDebt, latestFiscalYear);
  const latestEbitda = valueForYear(ebitda, latestFiscalYear);
  const latestEbit = valueForYear(ebit, latestFiscalYear);
  const latestInterestExpense = valueForYear(interestExpense, latestFiscalYear);
  const netCashDebt = latestCash !== null && latestDebt !== null && latestFiscalYear !== null && compatible([cash, totalDebt], latestFiscalYear) ? latestCash - latestDebt : null;
  const netDebtToEbitda = latestDebt !== null && latestCash !== null && latestEbitda !== null && latestEbitda !== 0
    && latestFiscalYear !== null && compatible([totalDebt, cash, ebitda], latestFiscalYear)
    ? (latestDebt - latestCash) / latestEbitda
    : null;
  const interestCoverage = latestEbit !== null && latestInterestExpense !== null && latestInterestExpense !== 0
    && latestFiscalYear !== null && compatible([ebit, interestExpense], latestFiscalYear)
    ? latestEbit / Math.abs(latestInterestExpense)
    : null;
  const interestBearingDebt = combine([shortTermDebt, longTermDebt]);
  const averageBorrowingsForYear = (year: number): number | null => {
    const closing = valueForYear(interestBearingDebt, year);
    const opening = valueForYear(interestBearingDebt, year - 1);
    if (closing === null || opening === null) return null;
    const currentRows = proofs.get(interestBearingDebt)?.get(year) ?? [];
    const priorRows = proofs.get(interestBearingDebt)?.get(year - 1) ?? [];
    const interestRows = proofs.get(interestExpense)?.get(year) ?? [];
    if (!currentRows.length || !priorRows.length || !interestRows.length) return null;
    const allRows = [...currentRows, ...priorRows, ...interestRows];
    if (!["reporting_basis", "reporting_currency", "display_scale"].every(key => {
      const values = allRows.map(row => row[key as "reporting_basis" | "reporting_currency" | "display_scale"]);
      return values.every(Boolean) && new Set(values).size === 1;
    })) return null;
    const closingEnd = currentRows[0].period_end;
    const openingEnd = priorRows[0].period_end;
    if (!closingEnd || !openingEnd || !currentRows.every(row => row.period_end === closingEnd)
        || !priorRows.every(row => row.period_end === openingEnd)
        || !interestRows.every(row => row.period_end === closingEnd && row.period_start)) return null;
    const nextDay = new Date(`${openingEnd}T00:00:00Z`);
    if (!Number.isFinite(nextDay.getTime())) return null;
    nextDay.setUTCDate(nextDay.getUTCDate() + 1);
    if (!interestRows.every(row => row.period_start === nextDay.toISOString().slice(0, 10))) return null;
    const average = (Math.abs(closing) + Math.abs(opening)) / 2;
    return average > 0 ? average : null;
  };
  const indiaBorrowingCostItem = indiaListed ? [...dataset.balance.items]
    .filter(item => item.metric === "weighted_average_cost_of_debt" && item.period.type === "annual"
      && Number(item.period.end.slice(0, 4)) === latestFiscalYear && item.display.unit === "ratio"
      && Number.isFinite(Number(item.display.value)))
    .sort((left, right) => right.period.end.localeCompare(left.period.end))[0] : undefined;
  // The India API publishes a pure decimal ratio. Its magnitude never decides
  // the unit: 0.005 means 0.5%, and 2 means 200%.
  let weightedAverageCostOfDebt = indiaListed
    ? indiaBorrowingCostItem ? Number(indiaBorrowingCostItem.display.value) * 100 : null
    : normalizedPercentValue(valueForYear(weightedAverageCost, latestFiscalYear));
  let weightedAverageCostOfDebtFiscalYear = weightedAverageCostOfDebt === null ? null : latestFiscalYear;
  let weightedAverageCostOfDebtNote = weightedAverageCostOfDebt === null
    ? "Not calculable from reported inputs"
    : indiaListed ? indiaBorrowingCostItem?.provenance?.derivation_method === "india_scoped_interest_to_average_borrowings_v1"
      ? `FY${latestFiscalYear} verified borrowing interest ÷ average borrowings`
      : indiaBorrowingCostItem?.provenance?.derivation_method === "india_reported_overall_borrowing_rate_v1"
        ? `FY${latestFiscalYear} issuer-reported overall borrowing rate`
        : `FY${latestFiscalYear} TaRaShaData borrowing cost`
      : "Issuer-reported borrowing cost";
  if (!indiaListed && weightedAverageCostOfDebt === null && latestFiscalYear !== null) {
    const currentReportedRate = valueForYear(reportedDebtRate, latestFiscalYear);
    if (currentReportedRate !== null) {
      weightedAverageCostOfDebt = normalizedPercentValue(currentReportedRate);
      weightedAverageCostOfDebtFiscalYear = latestFiscalYear;
      weightedAverageCostOfDebtNote = "Issuer-reported weighted borrowing rate";
    }
  }
  if (!indiaListed && weightedAverageCostOfDebt === null && latestFiscalYear !== null) {
    const averageDebt = averageBorrowingsForYear(latestFiscalYear);
    if (averageDebt !== null && latestInterestExpense !== null) {
      weightedAverageCostOfDebt = Math.abs(latestInterestExpense) / averageDebt * 100;
      weightedAverageCostOfDebtFiscalYear = latestFiscalYear;
      weightedAverageCostOfDebtNote = `FY${latestFiscalYear} interest expense ÷ average borrowings`;
    }
  }
  if (!indiaListed && weightedAverageCostOfDebt === null) {
    const latestLeaseRate = [
      latestPointAtOrBefore(operatingLeaseRate, latestFiscalYear),
      latestPointAtOrBefore(financeLeaseRate, latestFiscalYear),
    ].filter((point): point is YearValue => point !== null).sort((left, right) => right.year - left.year)[0] ?? null;
    const latestBorrowings = valueForYear(interestBearingDebt, latestFiscalYear);
    const leaseOnlyBorrowings = latestBorrowings === 0;
    if (
      latestLeaseRate
      && latestDebt !== null
      && latestDebt > 0
      && leaseOnlyBorrowings
    ) {
      weightedAverageCostOfDebt = normalizedPercentValue(latestLeaseRate.value);
      weightedAverageCostOfDebtFiscalYear = latestLeaseRate.year;
      weightedAverageCostOfDebtNote = `Lease-only funding · FY${latestLeaseRate.year} disclosed discount rate`;
    } else if (latestDebt === 0) {
      weightedAverageCostOfDebtNote = "Not applicable · no reported debt";
    } else if (latestDebt === null || latestBorrowings === null) {
      weightedAverageCostOfDebtNote = "Not calculable · complete borrowing or debt inputs unavailable";
    }
  }
  if (!indiaListed && weightedAverageCostOfDebt === null) {
    const latestReportedRate = latestPointAtOrBefore(reportedDebtRate, latestFiscalYear);
    if (latestReportedRate) {
      weightedAverageCostOfDebt = normalizedPercentValue(latestReportedRate.value);
      weightedAverageCostOfDebtFiscalYear = latestReportedRate.year;
      weightedAverageCostOfDebtNote = `Latest disclosed borrowing rate · FY${latestReportedRate.year}`;
    }
  }
  if (!indiaListed && weightedAverageCostOfDebt === null && latestFiscalYear !== null) {
    const candidateYears = interestExpense
      .map((point) => point.year)
      .filter((year) => year < latestFiscalYear && averageBorrowingsForYear(year) !== null)
      .sort((left, right) => right - left);
    const costYear = candidateYears[0] ?? null;
    const averageDebt = costYear === null ? null : averageBorrowingsForYear(costYear);
    const interest = costYear === null ? null : valueForYear(interestExpense, costYear);
    if (costYear !== null && averageDebt !== null && interest !== null) {
      weightedAverageCostOfDebt = Math.abs(interest) / averageDebt * 100;
      weightedAverageCostOfDebtFiscalYear = costYear;
      weightedAverageCostOfDebtNote = `Latest calculable cost · FY${costYear} interest expense ÷ average borrowings`;
    }
  }
  if (indiaListed && weightedAverageCostOfDebt === null && dataset.company.debt_cost_policy) {
    weightedAverageCostOfDebtNote = dataset.company.debt_cost_policy.reason;
  }
  if (!indiaListed && dataset.company.debt_cost_policy?.allow_interest_to_average_borrowings === false
      && !weightedAverageCost.length && !reportedDebtRate.length) {
    weightedAverageCostOfDebt = null;
    weightedAverageCostOfDebtFiscalYear = null;
    weightedAverageCostOfDebtNote = dataset.company.debt_cost_policy.reason;
  }
  const cashPercent = latestFiscalYear !== null && compatible([cash, assets], latestFiscalYear) ? percentOfBase(latestCash, latestAssets) : null;
  const summary = latestFiscalYear === null
    ? null
    : !accountingEquationAvailable
      ? `FY${latestFiscalYear} verified balances are shown individually. The accounting equation and calculations with missing or incompatible inputs are unavailable.`
    : cashPercent === null
      ? indiaListed ? `FY${latestFiscalYear} balances reconcile through the accounting equation; the cash cushion is unavailable from the verified inputs.`
        : `FY${latestFiscalYear} balances reconcile through the accounting equation; the cash cushion was not separately reported.`
      : `${cashPercent.toFixed(1)}% of assets is held in cash and short-term investments. ${netCashDebt === null ? "Net cash or debt could not be calculated from the reported facts." : netCashDebt >= 0 ? "Cash exceeds total debt, adding balance-sheet flexibility." : "Debt exceeds cash, so repayment capacity and interest cover matter."}`;
  const latestBalanceItem = [...dataset.balance.items]
    .filter((item) => item.period.type === "annual" && Number(item.period.end.slice(0, 4)) === latestFiscalYear)
    .sort((left, right) => right.period.end.localeCompare(left.period.end))[0];

  return {
    status: latestFiscalYear === null ? "unavailable" as const : "available" as const,
    accountingEquationAvailable,
    reason: latestFiscalYear === null ? indiaListed ? "TaRaShaData did not return verified annual balance-sheet values."
      : "TaRaShaData did not return total assets and shareholders’ equity for a common annual period." : null,
    latestFiscalYear,
    latestPeriodEnd: latestBalanceItem?.period.end ?? null,
    reportingCurrency,
    displayUnit,
    years: equationYears,
    equation: { assets: latestAssets, liabilities: latestLiabilities, shareholdersEquity: latestEquity },
    assets: [
      component("cash", "Cash & short-term investments cushion", cash, assets, [
        { key: "cash_and_equivalents", label: "Cash and cash equivalents", series: cashAndEquivalents },
        { key: "short_term_investments", label: shortTermInvestmentLabel, series: shortTermInvestments },
      ], "Complete cash and short-term investment inputs unavailable"),
      component("operating_current_assets", "Current assets excluding cash & short-term investments", operatingCurrentAssets, assets, [
        { key: "accounts_receivable", label: "Receivables", series: accountsReceivable },
        { key: "inventory", label: "Inventory", series: inventory },
        { key: "prepaid_expenses", label: "Reported prepaid expenses", series: prepaidExpenses },
        { key: "other_current_assets", label: "Reported other current assets", series: otherCurrentAssets },
        { key: "other_receivables", label: "Other receivables (notes & loans)", series: otherReceivables },
        { key: "reported_prepaids_and_other", label: "Prepaids & other current assets (reported together)", series: reportedPrepaidsAndOther },
      ], "Complete same-period current asset and cash totals unavailable"),
      component("property_plant_equipment", "Property, plant & equipment", propertyPlantEquipment, assets),
      component("goodwill", "Goodwill", goodwill, assets),
      component("other_intangible_assets", "Other intangible assets", otherIntangibleAssets, assets),
    ],
    liabilities: [
      component("unearned_revenue", "Unearned revenue", unearnedRevenue, liabilities, [
        { key: "unearned_revenue_current", label: "Current", series: unearnedRevenueCurrent },
        { key: "unearned_revenue_long_term", label: "Long term", series: unearnedRevenueLongTerm },
      ], "Complete same-period current and long-term unearned revenue inputs unavailable"),
      component("borrowings", "Borrowings", totalDebt, liabilities, [
        { key: "short_term_debt", label: "Short-term borrowings", series: shortTermDebt, rate: shortTermDebtRate },
        { key: "long_term_debt", label: "Long-term borrowings", series: longTermDebt, rate: reportedDebtRate },
        { key: "short_term_leases", label: "Short-term leases", series: shortTermLeases, rate: shortTermLeaseRate },
        { key: "long_term_leases", label: "Long-term leases", series: longTermLeases, rate: longTermLeaseRate },
      ], "Complete source-verified total debt unavailable"),
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
      ...(indiaListed ? { weightedAverageCostOfDebtLineage: indiaBorrowingCostItem ? {
        unit: indiaBorrowingCostItem.display.unit,
        derivationMethod: indiaBorrowingCostItem.provenance?.derivation_method ?? null,
        formula: indiaBorrowingCostItem.provenance?.formula ?? null,
        sourceUrl: indiaBorrowingCostItem.provenance?.source_url ?? null,
      } : null } : {}),
    },
    summary,
    methodology: indiaListed
      ? "Annual source-backed balance-sheet facts retain their reported basis and currency. Total debt uses a verified issuer total or a complete borrowing-and-lease group from TaRaShaData. Borrowing cost uses only the year-specific source-verified TaRaShaData rate, retaining its interest/debt scope and derivation. Missing inputs and unsupported calculations remain unavailable."
      : (dataset.company.debt_cost_policy?.allow_interest_to_average_borrowings === false
      ? "Debt cost is unavailable unless the issuer reports a comparable weighted rate. " : "") + "Annual normalized balance-sheet facts are divided by the relevant reported total for each fiscal year. Total liabilities is derived as assets less shareholders’ equity only when the issuer total is absent and both inputs share a reported period, basis and currency. Complete API cash and debt aggregates take precedence. Cash may alternatively use both compatible cash and short-term investment inputs. Operating current assets is current assets less the complete cash cushion; reported details keep their individual scopes and are not added together. Weighted average cost of debt uses an issuer-reported rate when available; otherwise it is absolute annual interest expense divided by complete opening and closing short- and long-term borrowings whose dates bound the actual annual interest period, or the disclosed lease discount rate for lease-only funding. The metric’s displayed note identifies the fiscal year and method. The plain-language health label screens net cash or debt, Net Debt / EBITDA (≤1x and ≤2.5x), and interest coverage (≥5x and ≥2x); it is context, not a recommendation. Missing components are never estimated.",
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

// These catalog definitions are fractional percentages. Other pure ratios
// (for example current ratio or asset turnover) remain plain numeric ratios.
const percentageRatioMetrics = new Set([
  "gross_margin", "operating_margin", "ebit_margin", "ebitda_margin", "pretax_margin", "profit_margin", "net_margin",
  "effective_tax_rate", "weighted_average_cost_of_debt", "weighted_average_interest_rate_debt",
  "debt_weighted_average_interest_rate", "short_term_debt_weighted_average_interest_rate",
  "operating_lease_weighted_average_discount_rate", "finance_lease_weighted_average_discount_rate",
  "long_term_debt_interest_rate", "short_term_debt_interest_rate", "long_term_borrowing_rate", "short_term_borrowing_rate", "lease_discount_rate",
]);
function ratioDisplay(item: DataApiFinancialItem): DataFactRow["ratio_display"] {
  if (item.display.unit === "%") return "percentage";
  if (item.display.unit !== "ratio") return undefined;
  return item.display.format === "percentage" || item.display.format === "number" ? item.display.format
    : percentageRatioMetrics.has(item.metric) ? "percentage" : "number";
}

function declaredDisplayScale(item: DataApiFinancialItem): number | undefined {
  const raw = item.display.scale;
  if (typeof raw !== "number" && typeof raw !== "string" || typeof raw === "string" && !raw.trim()) return undefined;
  const scale = Number(raw);
  return Number.isFinite(scale) && scale > 0 ? scale : undefined;
}

// Scale is presentation metadata supplied by TaRaShaData. A bare currency
// must retain its scale in captions even when the API does not spell it out.
function financialDisplayUnit(item: DataApiFinancialItem): string | null {
  const unit = item.display.unit;
  if (!unit || !/^(?:US\$|₹|[A-Z]{3})$/.test(unit)) return unit;
  if (item.display.format === "per_share") return `${unit} per share`;
  const scale = declaredDisplayScale(item);
  if (scale === undefined || scale === 1) return unit;
  const suffix = new Map([[1000, "thousands"], [1000000, "millions"], [10000000, "crores"], [1000000000, "billions"]]).get(scale);
  return suffix ? `${unit} ${suffix}` : `${unit} × ${scale}`;
}

function unitKind(item: DataApiFinancialItem): DataFactRow["unit_kind"] | null {
  const unit = item.display.unit;
  if (typeof unit !== "string" || !unit.trim()) return null;
  if (item.display.format === "per_share" || /^(?:US\$|[A-Z]{3})\/shares$/.test(unit)) return "per_share";
  if (unit === "ratio") return "ratio";
  if (unit === "%") return "percent";
  if (unit.includes("shares millions")) return "shares";
  if (unit.includes("per share")) return "per_share";
  return "amount";
}

function statementKey(statement: DataApiFinancials["statement"], metric: string): DataFactRow["statement_key"] {
  if (statement === "ratios" || statement === "kpis") return statement;
  if (metric.startsWith("shares_outstanding") || metric.startsWith("eps_")) return "shares";
  if (statement === "cash_flow") return "cash";
  return statement;
}

function currencyFromUnit(unit: string | null | undefined): string | null {
  if (!unit) return null;
  if (/US\$/.test(unit)) return "USD";
  const tokens = [...new Set(unit.match(/\b[A-Z]{3}\b/g) ?? [])];
  return tokens.length === 1 ? tokens[0] : null;
}

function annualReportingCurrenciesByYear(dataset: DataApiDataset): Record<string, string | null> {
  const currencies = new Map<string, Set<string>>();
  const ambiguous = new Set<string>();
  const add = (end: string, currency: string | null | undefined) => {
    const year = end.slice(0, 4);
    if (!/^\d{4}$/.test(year)) return;
    if (currency) currencies.set(year, new Set([...(currencies.get(year) ?? []), currency]));
  };
  for (const [end, point] of Object.entries(dataset.annual_reporting_currencies ?? {})) {
    add(end, point.reporting_currency);
    if (point.reporting_currency === null || point.status && !["available", "unique", "verified"].includes(point.status)) ambiguous.add(end.slice(0, 4));
  }
  for (const financials of [dataset.income, dataset.balance, dataset.cash_flow]) {
    for (const item of financials.items) {
      const kind = unitKind(item);
      if (item.period.type !== "annual" || kind === null || !["amount", "per_share"].includes(kind)) continue;
      add(item.period.end, item.period.reporting_currency);
      add(item.period.end, currencyFromUnit(item.unit));
      add(item.period.end, currencyFromUnit(item.display.unit));
    }
  }
  for (const [end, point] of Object.entries(dataset.company.reporting_basis_selection?.periods ?? {})) add(end, point.reporting_currency);
  return Object.fromEntries([...new Set([...currencies.keys(), ...ambiguous])].map(year => {
    const values = [...(currencies.get(year) ?? [])];
    return [year, !ambiguous.has(year) && values.length === 1 ? values[0] : null];
  }));
}

function cashDaScopeStatus(item: DataApiFinancialItem): string | undefined {
  const scope = item.provenance?.sec_cash_da_other_scope;
  if (!scope) return undefined;
  const pureTotal = item.metric === "depreciation_amortization" || item.metric === "cash_flow_depreciation_amortization";
  const subset = scope.pure_da_subset as { status?: string } | undefined;
  if (pureTotal && typeof subset?.status === "string") return subset.status;
  if (pureTotal && Array.isArray(scope.input_scopes) && scope.input_scopes.some(input => {
    const category = input?.category_scope as { pure_da_subset?: { status?: string } } | undefined;
    const directSubset = input?.pure_da_subset as { status?: string } | undefined;
    return directSubset?.status === "unavailable" || category?.pure_da_subset?.status === "unavailable";
  })) return "unavailable";
  return typeof scope.status === "string" ? scope.status : undefined;
}

function financialRows(dataset: DataApiDataset, statements = [dataset.income, dataset.balance, dataset.cash_flow]): DataFactRow[] {
  const rows: DataFactRow[] = [];
  const currencies = dataset.company.country === "India" ? annualReportingCurrenciesByYear(dataset) : {};
  for (const financials of statements) {
    for (const item of financials.items) {
      if (item.period.type !== "annual" || item.display.value === null || item.display.value.trim() === ""
        || item.status && !["available", "reported", "derived"].includes(item.status)) continue;
      const kind = unitKind(item);
      if (kind === null) continue;
      const year = Number(String(item.period.end).slice(0, 4));
      // Canonical pure ratios retain their precision before percent display.
      const value = item.display.unit === "%" ? Number(item.display.value) / 100
        : Number(item.display.unit === "ratio" && item.value != null ? item.value : item.display.value);
      if (!Number.isInteger(year) || !Number.isFinite(value)) continue;
      if (dataset.company.country === "India" && currencies[String(year)] === null && ["amount", "per_share"].includes(kind)) continue;
      const declaredCurrencies = [...new Set([item.period.reporting_currency, currencyFromUnit(item.unit),
        currencyFromUnit(item.display.unit)].filter((currency): currency is string => Boolean(currency)))];
      const rowCurrency = declaredCurrencies.length === 1 ? declaredCurrencies[0]
        : declaredCurrencies.length === 0 ? currencies[String(year)] ?? undefined : undefined;
      const amountDisplay = /^(?:US\$|₹|[A-Z]{3})(?:\s+(million|millions|crore|crores))?$/.exec(item.display.unit ?? "");
      const explicitScale = declaredDisplayScale(item);
      const displayScale = explicitScale ?? (amountDisplay ? amountDisplay[1]?.startsWith("crore") ? 10000000
        : amountDisplay[1] ? 1000000 : 1 : undefined);
      rows.push({
        company_id: dataset.company.id,
        statement_key: statementKey(financials.statement, item.metric),
        fact_key: dataMetricToDiscoverFact[item.metric] ?? item.metric,
        label: item.metric_label,
        unit_kind: kind === "percent" ? "ratio" : kind,
        ratio_display: ratioDisplay(item),
        fiscal_year: year,
        value,
        reporting_currency: rowCurrency,
        reporting_basis: item.period.reporting_basis ?? dataset.company.reporting_basis_selection?.periods?.[item.period.end]?.reporting_basis
          ?? dataset.company.reporting_basis,
        period_end: item.period.end,
        period_start: item.period.start,
        display_scale: displayScale,
        ...(kind === "amount" && rowCurrency && item.unit === rowCurrency && item.value != null
          && item.value.trim() !== "" && Number.isFinite(Number(item.value)) ? { canonical_amount: Number(item.value) } : {}),
        ...((item.metric === "depreciation_amortization" || item.metric === "cash_flow_depreciation_amortization")
          && cashDaScopeStatus(item) === "unavailable" ? { pure_da_calculation_unavailable: true } : {}),

      });
    }
  }
  const balanceRows = rows.filter(row => row.statement_key === "balance");
  const currentAssets = new Map(seriesFor(balanceRows, "currentAssets").map((item) => [item.year, item.value]));
  const cash = new Map(seriesFor(balanceRows, "cash").map((item) => [item.year, item.value]));
  const currentLiabilities = new Map(seriesFor(balanceRows, "currentLiabilities").map((item) => [item.year, item.value]));
  const currentDebt = new Map(seriesFor(balanceRows, "currentDebt").map((item) => [item.year, item.value]));
  for (const [year, assets] of currentAssets) {
    const cashValue = cash.get(year);
    const liabilities = currentLiabilities.get(year);
    const debt = currentDebt.get(year);
    if (cashValue === undefined || liabilities === undefined || debt === undefined) continue;
    rows.push({
      company_id: dataset.company.id,
      statement_key: "balance",
      fact_key: "nonCashWorkingCapital",
      label: "Net operating working capital",
      unit_kind: "amount",
      fiscal_year: year,
      value: assets - cashValue - (liabilities - debt),
    });
  }
  return rows;
}

// The same API metric can appear in supporting disclosures in another
// statement. Analysis inputs must use their financial-statement scope rather
// than whichever item happens to be encountered first in the response.
const analysisStatementScopes: Record<string, DataFactRow["statement_key"][]> = {
  ...Object.fromEntries(["revenue", "costOfRevenue", "sga", "researchAndDevelopment", "operatingIncome", "ebitda", "ebit",
    "interestExpense", "pretaxIncome", "effectiveTaxRate", "netIncome", "minorityInterestInEarnings",
    "earningsFromDiscontinuedOperations", "netIncomeToCommon"].map(key => [key, ["income"]])),
  ...Object.fromEntries(["operatingCash", "capex", "free_cash_flow", "shareBasedCompensation", "otherAdjustments",
    "netDebtIssuedPaid", "commonDividendsPaid"].map(key => [key, ["cash"]])),
  ...Object.fromEntries(["sharesOutstanding", "dilutedShares", "epsBasic", "eps"].map(key => [key, ["shares"]])),
  ...Object.fromEntries(["cash", "shortTermInvestments", "cashAndShortTermInvestments", "totalCash", "accountsReceivable",
    "inventory", "prepaidExpenses", "currentAssets", "propertyPlantEquipment", "goodwill", "otherIntangibleAssets", "assets",
    "accountsPayable", "unearnedRevenueCurrent", "unearnedRevenueLongTerm", "currentDebt", "longTermDebt",
    "shortTermLeaseLiabilities", "longTermLeaseLiabilities", "currentLiabilities", "longTermLiabilities", "totalDebt", "liabilities",
    "equity", "commonStock", "commonStockAndPaidInCapital", "additionalPaidInCapital", "retainedEarnings", "comprehensiveIncome",
    "nonCashWorkingCapital", "shortTermBorrowings", "currentPortionLongTermDebt", "net_debt"].map(key => [key, ["balance"]])),
};

function financialAnalysisRows(dataset: DataApiDataset): DataFactRow[] {
  return financialRows(dataset).filter(row => !analysisStatementScopes[row.fact_key]
    || analysisStatementScopes[row.fact_key].includes(row.statement_key)).map(row => {
    // Company story amounts use millions. Convert only a scale declared by the
    // API metadata or its unit label. Tables keep original displayed values;
    // scale provenance remains available to the compatibility guards.
    if (row.unit_kind !== "amount" || !row.reporting_currency) return row;
    if (row.canonical_amount !== undefined) return { ...row, value: row.canonical_amount / 1000000 };
    if (row.display_scale === undefined) return row;
    return { ...row, value: row.value * row.display_scale / 1000000 };
  });
}

// The SEC canonical extension does not currently cover India. Preserve that
// existing path only when the two cash-statement operands have an identical,
// explicitly declared annual source basis and display scale.
function legacyIndiaFreeCashFlow(facts: DataFactRow[]): YearValue[] {
  return facts.filter(row => row.statement_key === "cash" && row.fact_key === "operatingCash").flatMap(operating => {
    const capex = facts.find(row => row.statement_key === "cash" && row.fact_key === "capex" && row.fiscal_year === operating.fiscal_year);
    if (!capex || !["period_start", "period_end", "reporting_basis", "reporting_currency", "display_scale"].every(key => {
      const a = operating[key as keyof DataFactRow], b = capex[key as keyof DataFactRow];
      return a !== undefined && a !== null && a !== "" && a === b;
    })) return [];
    return [{ year: operating.fiscal_year, value: operating.value - Math.abs(capex.value) }];
  }).sort((a, b) => a.year - b.year);
}

function companyRow(dataset: DataApiDataset, context?: DataApiConstituent, fallbackBucket = "Unclassified"): DataCompanyRow {
  const currentAlias = dataset.company.aliases.find((alias) => alias.is_current) ?? dataset.company.aliases[0];
  return {
    id: dataset.company.id,
    cik: dataset.company.cik,
    identifier: dataset.company.identifier ?? dataset.company.cik ?? String(dataset.company.id),
    name: dataset.company.name,
    ticker: context?.ticker || currentAlias?.ticker || dataset.company.identifier || dataset.company.cik || "",
    exchange: context?.exchange || currentAlias?.exchange || "",
    country: dataset.company.country ?? context?.country ?? "USA",
    industry_bucket: context?.industry_buckets[0]?.name || dataset.company.sic_description || fallbackBucket,
  };
}

function annualReportingBasisByYear(payload: DataApiDiscoverDataset, dataset: DataApiDataset) {
  const totals = payload.revenue_offerings?.annual_totals ?? {};
  const segments = payload.revenue_offerings?.reported_segment_reconciliation?.history ?? {};
  const selections = payload.company.reporting_basis_selection?.periods ?? {};
  const annualItems = [dataset.income, dataset.balance, dataset.cash_flow].flatMap(statement => statement.items)
    .filter(item => item.period.type === "annual");
  const years = new Set([...Object.keys(totals), ...annualItems.map(item => item.period.end.slice(0, 4))]);
  const bases: Record<string, "standalone" | "consolidated"> = {};
  for (const year of years) {
    const periodEnd = totals[year]?.period_end ?? segments[year]?.period_end;
    const items = annualItems.filter(item => periodEnd ? item.period.end === periodEnd : item.period.end.startsWith(`${year}-`));
    const declared = [totals[year]?.reporting_basis, segments[year]?.reporting_basis, ...items.map(item => item.period.reporting_basis)]
      .filter((basis): basis is "standalone" | "consolidated" => basis === "standalone" || basis === "consolidated");
    const known = [...new Set(declared.length ? declared : [periodEnd ? selections[periodEnd]?.reporting_basis : undefined])]
      .filter((basis): basis is "standalone" | "consolidated" => basis === "standalone" || basis === "consolidated");
    // A period label requires one supported basis; conflicting provenance is not relabelled.
    if (known.length === 1) bases[year] = known[0];
  }
  return bases;
}

export async function pullDataCompany(env: DataProviderEnv, companyId: string, fromYear: number, toYear: number, customConstituentIds?: string[], options?: { signal?: AbortSignal; deferMarketContext?: boolean; preparedDefaultRequest?: unknown }) {
  const identifier = companyId.replace(/^data-/, "").trim();
  if (!identifier) throw new Error("Invalid TaRaShaData.ai company identifier.");
  const savedDefault = readPreparedDefaultRequest(options?.preparedDefaultRequest, identifier);
  if (savedDefault && (savedDefault.from_year !== fromYear || savedDefault.to_year !== toYear
    || customConstituentIds !== undefined || options?.deferMarketContext !== true)) {
    throw new Error("Saved default financial request does not match this company selection.");
  }
  const requestFromYear = savedDefault?.from_year ?? (/^(?:IN|NSE|BSE):/i.test(identifier) ? 1900 : fromYear);
  const constituentIdentifiers = customConstituentIds === undefined
    ? undefined
    : [...new Set(customConstituentIds.map((id) => id.replace(/^data-/, "").trim()).filter(Boolean))];
  if (constituentIdentifiers && (!constituentIdentifiers.length || constituentIdentifiers.length > 100)) {
    throw new Error("Select between 1 and 100 industry constituents.");
  }
  const payload = await preparedDatasetFetch(env, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    signal: options?.signal,
    body: JSON.stringify(savedDefault ?? {
      identifier,
      from_year: requestFromYear,
      to_year: toYear,
      constituent_identifiers: constituentIdentifiers,
      ...(options?.deferMarketContext ? { defer_market_context: true } : {}),
    }),
  });
  if (options?.deferMarketContext && (!payload.prepared_financial_snapshot
    || typeof payload.prepared_financial_snapshot.snapshot_id !== "string" || !payload.prepared_financial_snapshot.snapshot_id
    || typeof payload.prepared_financial_snapshot.key !== "string" || !payload.prepared_financial_snapshot.key)) {
    throw new Error("TaRaShaData.ai did not return a prepared financial snapshot for this company.");
  }
  if (options?.deferMarketContext && (payload.company.identifier ?? payload.company.cik) !== identifier) {
    throw new Error("Prepared financial data does not match the selected company.");
  }
  if (!payload.normalized_coverage.available) {
    throw new Error("TaRaShaData.ai does not currently publish normalized annual coverage for this company.");
  }
  const targetDataset = payload.datasets.find((item) => item.company.id === payload.company.id);
  if (!targetDataset) throw new Error("TaRaShaData.ai returned no normalized dataset for the selected company.");
  const contextByIdentifier = new Map(payload.constituents.map((item) => [item.identifier, item]));
  const defaultBucket = payload.industry_buckets[0]?.name || payload.company.sic_description || "Unclassified";
  const company = companyRow(targetDataset, contextByIdentifier.get(payload.company.identifier ?? payload.company.cik ?? ""), defaultBucket);
  const numericId = company.id;
  const rawFacts = financialAnalysisRows(targetDataset);
  const constituentCompanies = payload.constituents.flatMap((item) => {
    const dataset = payload.datasets.find((candidate) => (candidate.company.identifier ?? candidate.company.cik) === item.identifier);
    return dataset ? [companyRow(dataset, item, defaultBucket)] : [];
  });
  const constituentCompanyIds = new Set(constituentCompanies.map((item) => item.id));
  const bucketId = payload.industry_buckets[0]?.id ?? 0;
  const rawIndustryFacts: DataIndustryFactRow[] = payload.datasets
    .filter((dataset) => constituentCompanyIds.has(dataset.company.id))
    .flatMap((dataset) => financialAnalysisRows(dataset).filter(fact => !fact.pure_da_calculation_unavailable).map((fact) => ({
      bucket_id: bucketId,
      bucket_name: defaultBucket,
      company_id: fact.company_id,
      country: dataset.company.country ?? "USA",
      fact_key: fact.fact_key as DataIndustryFactRow["fact_key"],
      fiscal_year: fact.fiscal_year,
      value: fact.value,
      unit_kind: fact.unit_kind,
    })));
  const amountUnit = payload.company.reporting_currency === "USD"
    ? "US$ million"
    : `${payload.company.reporting_currency} million`;
  const analysisFacts = rawFacts.map((fact) => ({ ...fact, value: Number(fact.value) }));
  const facts = analysisFacts.filter((fact) => fact.fiscal_year >= requestFromYear);
  const cashFlowIndustryFacts = rawIndustryFacts.map((fact) => ({
    ...fact,
    value: Number(fact.value),
  }));
  const industryFacts = cashFlowIndustryFacts.filter((fact) => fact.fiscal_year >= fromYear);
  // Keep TTM points out of every annual series and calculation. The financial
  // statement column is supplied by the same TaRaShaData dataset response.
  const financialsByStatement = { income: targetDataset.income, balance: targetDataset.balance, cash: targetDataset.cash_flow, shares: targetDataset.income, ratios: targetDataset.ratios, kpis: targetDataset.kpis };
  const statements = (Object.keys(statementLabels) as Array<keyof typeof statementLabels>).map((groupKey) => {
    const financials = financialsByStatement[groupKey];
    if (!financials) return { key: groupKey, label: statementLabels[groupKey], facts: [], annualPeriods: [], ttmPeriodEnd: null };
    const ttmPeriodEnd = [...(financials.columns ?? []).filter(column => column.period_type === "ttm").map(column => column.end),
      ...financials.items.filter(item => item.period.type === "ttm").map(item => item.period.end)].sort().at(-1) ?? null;
    const ttmItems = new Map(financials.items.filter(item => item.period.type === "ttm" && item.period.end === ttmPeriodEnd
      && groupKey === statementKey(financials.statement, item.metric))
      .map(item => [dataMetricToDiscoverFact[item.metric] ?? item.metric, item]));
    const statementFacts = financialRows(targetDataset, [financials]).filter(fact => fact.statement_key === groupKey
      && fact.fiscal_year >= requestFromYear && fact.fiscal_year <= toYear);
    const grouped = new Map<string, DataFactRow[]>();
    for (const fact of statementFacts) grouped.set(fact.fact_key, [...(grouped.get(fact.fact_key) ?? []), fact]);
    const catalog = new Map(financials.metrics.filter(metric => groupKey === statementKey(financials.statement, metric.key))
      .map(metric => [dataMetricToDiscoverFact[metric.key] ?? metric.key, metric]));
    const required = [...catalog.entries()].filter(([, metric]) => metric.required_presentation).map(([key]) => key);
    const keys = [...new Set([...catalog.keys(), ...grouped.keys(), ...ttmItems.keys(), ...required])];
    return {
      key: groupKey,
      label: statementLabels[groupKey],
      ttmPeriodEnd,
      annualPeriods: [...new Set([...(financials.columns ?? []).filter(column => column.period_type === "annual").map(column => column.end),
        ...financials.items.filter(item => item.period.type === "annual").map(item => item.period.end)])]
        .filter(end => Number(end.slice(0, 4)) >= requestFromYear && Number(end.slice(0, 4)) <= toYear).sort(),
      facts: keys.map(key => {
        const values = grouped.get(key) ?? [], trailing = ttmItems.get(key), definition = catalog.get(key);
        const annualItems = financials.items.filter(item => item.period.type === "annual"
          && (dataMetricToDiscoverFact[item.metric] ?? item.metric) === key
          && Number(item.period.end.slice(0, 4)) >= requestFromYear && Number(item.period.end.slice(0, 4)) <= toYear);
        const firstItem = annualItems.find(item => unitKind(item) !== null)
          ?? (trailing && unitKind(trailing) !== null ? trailing : undefined) ?? annualItems[0] ?? trailing;
        const kind = values[0]?.unit_kind ?? (firstItem ? unitKind(firstItem) : null);
        const factUnit = kind === "shares" ? "million shares" : kind === "ratio" ? "ratio" : kind === "percent" ? "%"
          : firstItem ? financialDisplayUnit(firstItem) : null;
        const trailingValue = trailing?.display.value == null || trailing.display.value.trim() === "" || unitKind(trailing) === null
          || trailing.status && !["available", "reported", "derived"].includes(trailing.status) ? null
          : trailing.display.unit === "%" ? Number(trailing.display.value) / 100
          : Number(trailing.display.unit === "ratio" && trailing.value != null ? trailing.value : trailing.display.value);
        const cashDaScopes = financials.items.filter(item => (dataMetricToDiscoverFact[item.metric] ?? item.metric) === key
          && item.provenance?.sec_cash_da_other_scope && (item === trailing || item.period.type === "annual"
            && values.some(value => value.period_end === item.period.end)))
          .map(item => ({ period: { ...item.period }, status: cashDaScopeStatus(item) ?? "available",
            ...(item.provenance?.presentation_role ? { presentation: item.provenance.presentation_role } : {}),
            evidence: item.provenance!.sec_cash_da_other_scope! }));
        return {
          key,
          label: definition?.label ?? values[0]?.label ?? firstItem?.metric_label ?? key,
          description: definition?.description ?? factDescriptions[key] ?? "Normalized historical financial fact from TaRaShaData.ai.",
          ...(definition?.key || trailing?.metric ? { sourceMetric: definition?.key ?? trailing!.metric } : {}),
          ...(definition?.parent_key ? { parentKey: dataMetricToDiscoverFact[definition.parent_key] ?? definition.parent_key } : {}),
          ...(definition?.required_presentation ? { presentationRequired: true } : {}),
          ...(definition?.key === "property_plant_equipment_net" && financials.ppe_lease_scope
            ? { periodScope: financials.ppe_lease_scope } : {}),
          ...((definition?.key === "short_term_debt" || definition?.key === "long_term_debt")
            && financials.sec_ifrs_debt_scope ? { periodScope: financials.sec_ifrs_debt_scope.filter(scope =>
              scope.section === (definition.key === "short_term_debt" ? "current" : "noncurrent")) } : {}),
          ...(cashDaScopes.length ? { periodScope: cashDaScopes } : {}),
          unit: factUnit,
          displayFormat: values[0]?.ratio_display ?? (firstItem ? ratioDisplay(firstItem) : undefined),
          values: values.map(value => ({ year: value.fiscal_year, value: value.value })),
          annualSources: annualItems.map(item => ({ year: Number(item.period.end.slice(0, 4)), periodEnd: item.period.end,
            periodStart: item.period.start ?? null, value: item.value ?? null, unit: item.unit ?? null,
            displayUnit: financialDisplayUnit(item), sourceDisplayUnit: item.display.unit, displayScale: item.display.scale ?? null,
            displayFormat: ratioDisplay(item), status: item.status, reason: item.reason ?? null,
            sourceUrl: item.provenance?.source_url ?? null, provenance: item.provenance ? { ...item.provenance } : null,
            factId: item.fact_id ?? null, observationId: item.observation_id ?? null })),
          ttm: trailing ? { value: trailingValue !== null && Number.isFinite(trailingValue) ? trailingValue : null,
            periodEnd: trailing.period.end, periodStart: trailing.period.start ?? null,
            unit: trailing.display.unit === "%" ? "ratio" : trailing.display.unit === "shares millions" ? "million shares" : financialDisplayUnit(trailing), displayScale: trailing.display.scale ?? null, sourceDisplayUnit: trailing.display.unit,
            displayFormat: ratioDisplay(trailing), sourceUrl: trailing.provenance?.source_url ?? null,
            factId: trailing.fact_id ?? null, observationId: trailing.observation_id ?? null,
            canonicalValue: trailing.value ?? null, canonicalUnit: trailing.unit ?? null,
            status: trailing.status, reason: trailing.reason ?? null,
            provenance: trailing.provenance ? { ...trailing.provenance } : null } : null,
        };
      }),
    };
  });
  const revenue = seriesFor(facts, "revenue");
  const operatingCost = seriesFor(facts, "costOfRevenue");
  const sga = seriesFor(facts, "sga");
  const depreciation = seriesFor(facts.filter(fact => !fact.pure_da_calculation_unavailable), "depreciation");
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
  const taxRateRows = (rows: DataFactRow[]) => rows.flatMap(row => row.fact_key === "effectiveTaxRate"
    && (row.unit_kind === "ratio" || row.unit_kind === "percent")
    ? [{ ...row, value: row.unit_kind === "percent" ? row.value / 100 : row.value }] : []);
  const effectiveTaxRate = seriesFor(taxRateRows(facts), "effectiveTaxRate");
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
  const canonicalFreeCashFlow = seriesFor(facts, "free_cash_flow");
  const freeCashFlow = company.country === "India" && !canonicalFreeCashFlow.length
    ? legacyIndiaFreeCashFlow(facts) : canonicalFreeCashFlow;
  const capex = seriesFor(facts, "capex");
  const debt = seriesFor(facts, "totalDebt");
  const cash = seriesFor(facts, "cash");
  const investments = seriesFor(facts, "shortTermInvestments");
  // TaRaShaData net_debt is explicitly Total Cash minus Total Debt. The
  // headline uses the opposite sign, and complete API aggregates take priority.
  const reportedNetDebt = seriesFor(facts, "net_debt").map(point => ({ ...point, value: 0 - point.value }));
  const reportedTotalCash = coalesceSeries(seriesFor(facts, "totalCash"), seriesFor(facts, "cashAndShortTermInvestments"));
  const netDebt = coalesceSeries(reportedNetDebt, coalesceSeries(
    derivedSeries(debt, reportedTotalCash, (totalDebt, totalCash) => totalDebt - totalCash),
    netDebtSeries(debt, cash, investments),
  ));
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
  const industryEffectiveTaxRate = groupIndustrySeries(rawIndustryFacts.flatMap(row => row.fact_key === "effectiveTaxRate"
    && (row.unit_kind === "ratio" || row.unit_kind === "percent")
    ? [{ ...row, value: row.unit_kind === "percent" ? Number(row.value) / 100 : Number(row.value) }] : []), "effectiveTaxRate");
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
    id: `data-${item.identifier ?? item.cik}`,
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
  const latestYear = company.country === "India" && facts.length
    ? Math.max(...facts.map((fact) => fact.fiscal_year))
    : Math.max(...facts.map((fact) => fact.fiscal_year), toYear);
  const reportingBasisByYear = company.country === "India" ? annualReportingBasisByYear(payload, targetDataset) : undefined;
  const annualCurrencyByYear = company.country === "India" ? annualReportingCurrenciesByYear(targetDataset) : undefined;
  return {
    id: `data-${company.identifier ?? company.cik}`,
    ...(payload.prepared_story_delivery ? { preparedStoryDelivery: payload.prepared_story_delivery } : {}),
    ...(payload.prepared_financial_snapshot ? {
      preparedFinancialSnapshot: {
        snapshotId: payload.prepared_financial_snapshot.snapshot_id,
        key: payload.prepared_financial_snapshot.key,
        identifier,
        fromYear: requestFromYear,
        toYear,
      },
      ...(options?.deferMarketContext ? { marketContextStatus: { status: "pending" as const } } : {}),
    } : {}),
    logoUrl: `/api/data/company-logo?companyId=${encodeURIComponent(`data-${company.identifier ?? company.cik}`)}&contract=1`,
    name: company.name,
    symbol: company.ticker,
    country: payload.company.country ?? payload.country,
    reportingBasis: payload.company.reporting_basis,
    reportingBasisByYear,
    ...(company.country === "India" ? { annualCurrencyByYear } : {}),
    exchange: targetDataset.company.aliases.find((alias) => alias.is_current)?.exchange
      ?? targetDataset.company.aliases[0]?.exchange
      ?? "",
    sector: company.industry_bucket || `${company.country} · TaRaShaData.ai coverage`,
    description: "Source-linked historical financial statements standardized by TaRaShaData.ai from issuer filings.",
    currency: amountUnit,
    reportingPeriod: `FY ${latestYear}`,
    supportedStoryChapters: payload.company.supported_story_chapters,
    updatedAt: new Date(payload.company.updated_at).toLocaleDateString("en-GB", { day: "numeric", month: "long", year: "numeric", timeZone: "UTC" }),
    metrics: {
      revenue,
      operatingMargin,
      freeCashFlow,
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
      "TaRaShaData.ai coverage is currently limited to companies and periods with standardized issuer filing facts.",
      "Enterprise Value and trailing P/E remain unavailable until TaRaShaData.ai publishes a governed company-level market-data contract.",
      "Derived values can differ from issuer presentation because of standardized definitions, restatements, units, or filing context.",
      "TaRaSha Discover stores no separate financial copy; the API response is held only in browser session memory.",
    ],
    dataMode: "tarasha-data" as const,
    source: {
      dataset: "TaRaShaData.ai normalized financials API",
      upstream: payload.company.country === "India"
        ? "NSE/BSE financial filings and issuer annual reports"
        : "Issuer filings and SEC XBRL",
      usage: "Educational research",
      persistence: "Browser session memory only",
    },
  };
}


/** Quotes and market history use the exact SEC snapshot already served to the story. */
export async function pullDataCompanyMarketContext(env: DataProviderEnv, companyId: string,
  snapshot: { snapshotId: string; key: string; identifier: string; fromYear: number; toYear: number }, signal?: AbortSignal) {
  const identifier = companyId.replace(/^data-/, "").trim();
  if (!identifier || snapshot.identifier !== identifier || !snapshot.snapshotId || !snapshot.key
    || !Number.isInteger(snapshot.fromYear) || !Number.isInteger(snapshot.toYear)
    || snapshot.fromYear > snapshot.toYear) throw new Error("Invalid prepared financial snapshot for market context.");
  const payload = await dataFetch<{
    identifier: string; prepared_snapshot_id: string; prepared_snapshot_key: string;
    stock_risk: DataApiStockRisk; market_pricing: DataApiMarketPricing; stock_history: DataApiStockHistory;
  }>(env, "/v1/discover/company-market-context", {
    method: "POST", headers: { "Content-Type": "application/json" }, signal,
    body: JSON.stringify({ identifier, from_year: snapshot.fromYear, to_year: snapshot.toYear,
      prepared_snapshot_id: snapshot.snapshotId, prepared_snapshot_key: snapshot.key }),
  }, 20000);
  if (payload.identifier !== identifier || payload.prepared_snapshot_id !== snapshot.snapshotId
    || payload.prepared_snapshot_key !== snapshot.key) throw new Error("Market context does not match the prepared financial snapshot.");
  if (![payload.stock_risk, payload.market_pricing, payload.stock_history].every(item => item
    && ["available", "unavailable"].includes(item.status))) throw new Error("TaRaShaData.ai returned incomplete market context.");
  return { companyId: `data-${identifier}`, snapshotId: snapshot.snapshotId, snapshotKey: snapshot.key,
    stockRisk: stockRiskStory(payload.stock_risk), marketPricing: marketPricingStory(payload.market_pricing),
    stockHistory: stockHistoryStory(payload.stock_history) };
}
