export type MetricKey = "revenue" | "operatingMargin" | "freeCashFlow" | "netDebt";

export interface YearValue {
  year: number;
  value: number;
}

export interface DistributionObservation {
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

export interface GrowthComparison {
  company: GrowthStatistics;
  industryBucket: GrowthStatistics;
}

export type ProfitabilityMetricKey = "grossMargin" | "operatingMargin" | "cogsRatio" | "sgaRatio" | "daRatio" | "rdRatio";

export interface LevelStatistics {
  median: number | null;
  standardDeviation: number | null;
  observations: number;
  distribution: DistributionObservation[];
}

export interface ProfitabilityYearPoint {
  year: number;
  grossMargin: number | null;
  grossProfit: number | null;
  operatingMargin: number | null;
  operatingIncome: number | null;
  cogsRatio: number | null;
  cogs: number | null;
  sgaRatio: number | null;
  sga: number | null;
  daRatio: number | null;
  da: number | null;
  rdRatio: number | null;
  rd: number | null;
}

export interface IndustryLevelPoint {
  year: number;
  companyValue: number | null;
  companyAbsoluteValue: number | null;
  industryMedian: number | null;
  industryMedianAbsoluteValue: number | null;
}

export interface PerformanceThresholds {
  lowerQuartile: number | null;
  median: number | null;
  upperQuartile: number | null;
  observations: number;
  direction: "higher" | "lower";
}

export interface ProfitabilityMetricBands {
  level: PerformanceThresholds;
  standardDeviation: PerformanceThresholds;
}

export interface ProfitabilityAnalysis {
  statistics: Record<ProfitabilityMetricKey, LevelStatistics>;
  industryStatistics: Record<ProfitabilityMetricKey, LevelStatistics>;
  yearly: ProfitabilityYearPoint[];
  industryComparisons: {
    grossMargin: IndustryLevelPoint[];
    operatingMargin: IndustryLevelPoint[];
    daRatio: IndustryLevelPoint[];
    rdRatio: IndustryLevelPoint[];
  };
  performanceBands: Record<ProfitabilityMetricKey, ProfitabilityMetricBands>;
}

export type EarningsFlowMetricKey =
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

export interface EarningsFlowMetricValue {
  companyValue: number | null;
  industryMedian: number | null;
  industryObservations: number;
  companyMarginPercent: number | null;
  industryMedianMarginPercent: number | null;
}

export interface EarningsFlowYear {
  year: number;
  metrics: Record<EarningsFlowMetricKey, EarningsFlowMetricValue>;
}

export type ValuationMetricKey = "evRevenue" | "evGrossProfit" | "evEbitda" | "evEbit" | "pe";

export interface ValuationComparisonValue {
  companyValue: number | null;
  industryMedian: number | null;
  industryObservations: number;
}

export interface EarningsAndValuationAnalysis {
  earningsFlow: EarningsFlowYear[];
  valuation: {
    denominatorYear: number | null;
    enterpriseValue: number | null;
    enterpriseValueAsOf: string | null;
    enterpriseValueSource: string;
    enterpriseValueDetail: string;
    comparisons: Record<ValuationMetricKey, ValuationComparisonValue>;
  };
}

export type FcffBridgeMetricKey =
  | "ebit"
  | "nopat"
  | "depreciationAndAmortization"
  | "capitalExpenditure"
  | "workingCapitalImpact"
  | "fcff";

export type FcfeBridgeMetricKey =
  | "netIncomeToCommon"
  | "depreciationAndAmortization"
  | "shareBasedCompensation"
  | "otherAdjustments"
  | "capitalExpenditure"
  | "workingCapitalImpact"
  | "netBorrowing"
  | "fcfe";

export interface CashFlowBridgeMetricValue {
  companyValue: number | null;
  industryMedian: number | null;
  industryObservations: number;
  companyPercent: number | null;
  industryMedianPercent: number | null;
}

export type WorkingCapitalDebtBreakdownStatus =
  | "reported-components"
  | "partially-reported"
  | "aggregate-only"
  | "components-only"
  | "unavailable";

export interface WorkingCapitalPeriodBreakdown {
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
  debtBreakdownStatus: WorkingCapitalDebtBreakdownStatus;
}

export interface WorkingCapitalMovement {
  previousYear: WorkingCapitalPeriodBreakdown;
  currentYear: WorkingCapitalPeriodBreakdown;
  netChangeInWorkingCapital: number | null;
  cashImpact: number | null;
  cashEffect: "inflow" | "outflow" | "neutral" | "unavailable";
}

export interface CashFlowYear {
  year: number;
  effectiveTaxRatePercent: number | null;
  industryMedianEffectiveTaxRatePercent: number | null;
  workingCapital: WorkingCapitalMovement;
  fcff: Record<FcffBridgeMetricKey, CashFlowBridgeMetricValue>;
  fcfe: Record<FcfeBridgeMetricKey, CashFlowBridgeMetricValue>;
}

export interface CashFlowAnalysis {
  yearly: CashFlowYear[];
}

export interface CompanyDeltaPoint {
  fromYear: number;
  toYear: number;
  revenue: number | null;
  revenueChangePercent: number | null;
  grossProfit: number | null;
  grossProfitChangePercent: number | null;
  grossOperatingLeverage: number | null;
  operatingIncome: number | null;
  operatingIncomeChangePercent: number | null;
}

export interface IndustryDeltaPoint {
  fromYear: number;
  toYear: number;
  company: number | null;
  companyChangePercent: number | null;
  industryMedian: number | null;
  industryMedianChangePercent: number | null;
}

export interface RawIncomePoint {
  year: number;
  revenue: number | null;
  revenueChangePercent: number | null;
  grossProfit: number | null;
  grossProfitChangePercent: number | null;
  grossOperatingLeverage: number | null;
  operatingIncome: number | null;
  operatingIncomeChangePercent: number | null;
}

export interface IndustryConstituent {
  id: string;
  name: string;
  ticker: string;
  industryBucket: string;
  revenueObservations: number;
  grossProfitObservations: number;
  operatingIncomeObservations: number;
}

export interface ResearchShelfAnalysis {
  fromYear: number;
  toYear: number;
  industryBucket: string;
  industryCompanyCount: number;
  industryConstituents: IndustryConstituent[];
  industryConstituentsCustomized: boolean;
  growthComparisons: {
    revenue: GrowthComparison;
    grossProfit: GrowthComparison;
    grossOperatingLeverage: GrowthComparison;
    operatingIncome: GrowthComparison;
  };
  companyDeltas: CompanyDeltaPoint[];
  industryDeltas: {
    revenue: IndustryDeltaPoint[];
    grossProfit: IndustryDeltaPoint[];
    operatingIncome: IndustryDeltaPoint[];
  };
  rawIncome: RawIncomePoint[];
  profitability: ProfitabilityAnalysis;
  earningsAndValuation: EarningsAndValuationAnalysis;
  cashFlow: CashFlowAnalysis;
}

export interface BusinessSegmentRevenueHistory {
  fiscalYear: number;
  periodStart: string | null;
  periodEnd: string;
  revenue: number;
  accession: string | null;
  filedDate: string | null;
  sourceUrl: string | null;
}

export interface BusinessSegmentRevenue {
  member: string;
  name: string;
  latestRevenue: number;
  percentageOfTotal: number | null;
  yoyGrowthPercent: number | null;
  threeYearCagrPercent: number | null;
  yoyGrowthReason?: string | null;
  threeYearCagrReason?: string | null;
  history: BusinessSegmentRevenueHistory[];
  accession: string | null;
  filedDate: string | null;
  sourceUrl: string | null;
}

export type RevenueBreakdownSourceType = "reported_segment_reconciliation" | "disclosed_revenue_note" | "reported_revenue_hierarchy";

export interface RevenueOfferingSource {
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

export interface RevenueOffering {
  ttmThreeYearCagrPercent?: number | null;
  ttmYoyGrowthReason?: string | null;
  ttmThreeYearCagrReason?: string | null;
  ttmGrowthComparisons?: unknown;
  ttmGrowthSourceUrls?: string[];
  ttmYoyGrowthPercent?: number | null;
  ttmThreePeriodCagrPercent?: number | null;
  id: string;
  label: string;
  axis?: string;
  member?: string;
  labelBasis?: string;
  periodEnds?: Record<string, string>;
  provenanceByPeriod?: Record<string, RevenueOfferingSource[]>;
  /** Values use the same millions-based reporting currency as company metrics. */
  revenueByPeriod: Record<string, number | null>;
  sourceByPeriod?: Record<string, string>;
  sourcesByPeriod?: Record<string, string[]>;
  children?: RevenueOffering[];
}

export interface RevenueAdjustmentEntry {
  id: string;
  label: string;
  definition: string;
  /** Values use the same millions-based reporting currency as company metrics. */
  valueByPeriod: Record<string, number | null>;
  sourceByPeriod: Record<string, string>;
  sourcesByPeriod: Record<string, string[]>;
  derivationByPeriod: Record<string, string>;
  coverageByPeriod: Record<string, string>;
}

export interface RevenueAdjustmentImpact {
  id: string;
  label: string;
  definition: string;
  /** Signed values use the same millions-based reporting currency as company metrics. */
  valueByPeriod: Record<string, number | null>;
  directionByPeriod: Record<string, "positive" | "negative" | "neutral">;
  openingBalanceByPeriod: Record<string, number | null>;
  closingBalanceByPeriod: Record<string, number | null>;
  sourcesByPeriod: Record<string, string[]>;
  derivationByPeriod: Record<string, string>;
  coverageByPeriod: Record<string, string>;
}

export interface AdjustedRevenuePoint {
  fiscalYear: number;
  periodType?: "annual" | "ttm";
  periodEnd: string;
  reportedRevenue: number;
  totalAdjustment: number | null;
  adjustedRevenue: number | null;
  reportedGrowthPercent: number | null;
  adjustedGrowthPercent: number | null;
  growthDifferencePp: number | null;
  appliedAdjustmentIds: string[];
  sourceUrls: string[];
}

export interface AdjustedRevenueStory {
  status: "available" | "unavailable";
  reason: string | null;
  points: AdjustedRevenuePoint[];
  methodology: string;
}

export interface CompanyRevenueStory {
  status: "available" | "undetermined" | "unavailable";
  reason: string | null;
  latestFiscalYear: number | null;
  latestPeriodEnd: string | null;
  reportingCurrency: string;
  displayUnit: string;
  totalRevenue: number | null;
  segments: BusinessSegmentRevenue[];
  summary: string | null;
  methodology: string;
}

export interface CompanyCostStoryPeriod {
  reportingCurrency?: string | null;
  key: string;
  label: string;
  type: "annual" | "ttm";
  fiscalYear: number | null;
  periodEnd: string | null;
}

export interface CompanyCostStoryValue {
  value: number | null; // Actual reporting-currency millions; no FX or revenue normalization.
  inputs: unknown[]; // Full TaRaShaData canonical provenance references.
}

export interface CompanyCostMedianLine {
  key: string;
  label: string;
  role: "revenue" | "expense" | "subtotal";
  valuePerHundred: number | null;
  observationCount: number;
  reason: string | null;
  inputs: unknown[];
}

export interface CompanyCostInterestValue extends CompanyCostStoryValue {
  displayValue: number | null;
  status: "reported" | "not_reported";
  displayPolicy: string;
  percentOfOperatingIncome: number | null;
}

export interface CompanyCostInterestYear {
  key?: string;
  label?: string;
  type?: "annual" | "ttm";
  reportingCurrency?: string | null;
  fiscalYear: number | null;
  periodEnd: string | null;
  operatingIncome: CompanyCostInterestValue;
  interestExpense: CompanyCostInterestValue;
  interestIncome: CompanyCostInterestValue;
  netInterestOtherIncomeExpense?: CompanyCostInterestValue;
}


export interface CompanyCostStructureHistory {
  fiscalYear: number;
  periodEnd: string | null;
  valuePerHundred: number;
}

export interface CompanyCostStructureLine {
  key: string;
  label: string;
  role: "revenue" | "expense" | "subtotal";
  latestValuePerHundred: number | null;
  history: CompanyCostStructureHistory[];
  lineage: {
    kind: string | null;
    concept: string | null;
    derivationMethod: string | null;
    formula: string | null;
    sourceUrl: string | null;
  };
}

export interface CompanyCostStructureStory {
  schemaVersion?: number;
  annualSummary?: string | null;
  interestSummary?: string | null;
  interestComparisonNote?: string | null;
  median?: {
    status: "available" | "unavailable";
    reason: string | null;
    periods: CompanyCostStoryPeriod[];
    lines: CompanyCostMedianLine[];
  };
  annualBreakdown?: {
    displayUnit: string;
    periods: CompanyCostStoryPeriod[];
    lines: { key: string; label: string; role: "revenue" | "expense" | "subtotal"; values: Record<string, CompanyCostStoryValue> }[];
    operatingMarginPercent: Record<string, number | null>;
  };
  interestComparison?: CompanyCostInterestYear[];
  status: "available" | "unavailable";
  reason: string | null;
  latestFiscalYear: number | null;
  latestPeriodEnd: string | null;
  reportingCurrency: string;
  years: number[];
  lines: CompanyCostStructureLine[];
  summary: string | null;
  methodology: string;
  sourceUrl: string | null;
}

export interface CompanyCashConversionLineage {
  kind: string | null;
  concept: string | null;
  derivationMethod: string | null;
  formula: string | null;
  sourceUrl: string | null;
}

export interface CompanyCashConversionComponent {
  key: string;
  label: string;
  value: number;
  lineage: CompanyCashConversionLineage;
}

export interface CompanyCashConversionBridgeLine {
  key: string;
  label: string;
  operation: "base" | "add" | "subtract" | "subtotal" | "total";
  value: number;
  formula: string | null;
  lineage: CompanyCashConversionLineage;
  components: CompanyCashConversionComponent[];
  disclosure?: string;
  workingCapitalCalculation?: CompanyNcwcCalculation;
}

export interface CompanyNcwcComponent {
  key: string;
  label: string;
  priorValue: number | null;
  currentValue: number | null;
  priorSourceUrl: string | null;
  currentSourceUrl: string | null;
}

export interface CompanyNcwcInput {
  key: "current_assets" | "cash_input" | "current_liabilities" | "current_debt";
  label: string;
  operator: "" | "−" | "+";
  priorValue: number | null;
  currentValue: number | null;
  components: CompanyNcwcComponent[];
  priorDetailStatus: string;
  currentDetailStatus: string;
  priorNote: string | null;
  currentNote: string | null;
}

export interface CompanyNcwcCalculation {
  periodType?: string;
  priorPeriodLabel?: string;
  currentPeriodLabel?: string;
  cashInputPolicy?: string;
  status: "available" | "unavailable";
  reason: string | null;
  formula: string;
  priorFiscalYear: number;
  currentFiscalYear: number;
  priorPeriodEnd: string | null;
  currentPeriodEnd: string | null;
  priorNcwc: number | null;
  currentNcwc: number | null;
  priorOperatingCurrentAssets: number | null;
  currentOperatingCurrentAssets: number | null;
  cashEffect: number | null;
  inputs: CompanyNcwcInput[];
}

export interface CompanyCashConversionTrendPoint {
  reportingCurrency?: string;
  fiscalYear: number;
  periodEnd: string;
  ebit: number;
  taxRatePercent: number;
  taxesOnOperatingProfit: number;
  nopat: number;
  depreciationAndAmortization: number;
  workingCapitalImpact: number;
  workingCapitalComponents: CompanyCashConversionComponent[];
  workingCapitalCalculation?: CompanyNcwcCalculation;
  capitalExpenditure: number;
  fcff: number;
  conversionPercent: number | null;
  revenue: number;
  fcffRevenuePercent: number | null;
}

export interface CompanyCashConversionPeriodGap {
  fiscalYear: number;
  periodEnd: string | null;
  reportingBasis: "standalone" | "consolidated" | null;
  reportingCurrency: string | null;
  status: "unavailable";
  reasonCode: string;
  reason: string;
  missingInputs: string[];
  sourceUrls: string[];
}

export interface CompanyCashConversionStory {
  latestStatementFiscalYear?: number;
  displayAnnualYears?: number[];
  ttm?: Omit<CompanyCashConversionStory, "ttm"> & { periodStart: string | null; missingInputs: string[] };
  partialBridge?: {
    fiscalYear: number;
    lines: (Omit<CompanyCashConversionBridgeLine, "value"> & { value: number | null })[];
  };
  earningsBasis?: string;
  workingCapitalCalculation?: CompanyNcwcCalculation;
  periodGaps?: CompanyCashConversionPeriodGap[];
  status: "available" | "unavailable";
  reason: string | null;
  latestFiscalYear: number | null;
  latestPeriodEnd: string | null;
  reportingCurrency: string;
  displayUnit: string;
  years: number[];
  bridge: CompanyCashConversionBridgeLine[];
  trend: CompanyCashConversionTrendPoint[];
  metrics: {
    fcff: number | null;
    priorFiscalYear: number | null;
    priorFcff: number | null;
    conversionPercent: number | null;
    priorConversionPercent: number | null;
    growthPercent: number | null;
    priorGrowthPercent: number | null;
    revenuePercent: number | null;
    priorRevenuePercent: number | null;
    cagrPercent: number | null;
  } | null;
  summary: string | null;
  takeaway: string | null;
  methodology: string;
  sourceUrl: string | null;
}

export interface CompanyBalanceSheetTrendPoint {
  fiscalYear: number;
  value: number;
  percentageOfBase: number | null;
}

export interface CompanyBalanceSheetComponent {
  key: string;
  label: string;
  value: number | null;
  unavailableReason?: string;
  percentageOfBase: number | null;
  trend: CompanyBalanceSheetTrendPoint[];
  children: Array<{
    key: string;
    label: string;
    value: number | null;
    percentageOfBase: number | null;
    interestRatePercent: number | null;
  }>;
}

export interface CompanyBalanceSheetStory {
  status: "available" | "unavailable";
  accountingEquationAvailable?: boolean;
  reason: string | null;
  latestFiscalYear: number | null;
  latestPeriodEnd: string | null;
  reportingCurrency: string;
  displayUnit: string;
  years: number[];
  equation: {
    assets: number | null;
    liabilities: number | null;
    shareholdersEquity: number | null;
  };
  assets: CompanyBalanceSheetComponent[];
  liabilities: CompanyBalanceSheetComponent[];
  shareholdersEquity: CompanyBalanceSheetComponent[];
  health: {
    totalCash: number | null;
    totalDebt: number | null;
    netCashDebt: number | null;
    netDebtToEbitda: number | null;
    interestCoverage: number | null;
    weightedAverageCostOfDebt: number | null;
    weightedAverageCostOfDebtFiscalYear: number | null;
    weightedAverageCostOfDebtNote: string;
    weightedAverageCostOfDebtLineage?: { unit: string | null; derivationMethod: string | null; formula: string | null; sourceUrl: string | null } | null;
  };
  summary: string | null;
  methodology: string;
  sourceUrl: string | null;
}

export type StockRiskRange = "1Y" | "3Y" | "5Y" | "7Y" | "10Y";

export interface CompanyStockRiskPoint {
  date: string;
  value: number;
}

export interface CompanyStockRiskDistributionBin {
  key: string;
  label: string;
  months: number;
  percentage: number | null;
}

export interface CompanyStockRiskSeries {
  key: "company" | "market" | "sector";
  name: string;
  symbol: string;
  role: string;
  sourceUrl: string;
  startDate: string;
  endDate: string;
  tradingDays: number;
  monthlyObservations: number;
  annualizedVolatilityPercent: number | null;
  betaToMarket: number | null;
  maximumDrawdownPercent: number | null;
  worstMonthlyReturnPercent: number | null;
  bestMonthlyReturnPercent: number | null;
  positiveMonthsPercent: number | null;
  negativeMonthsPercent: number | null;
  flatMonthsPercent: number | null;
  drawdown: CompanyStockRiskPoint[];
  rollingVolatility: CompanyStockRiskPoint[];
  monthlyDistribution: CompanyStockRiskDistributionBin[];
}

export interface CompanyStockRiskPeriod {
  range: StockRiskRange;
  startDate: string;
  endDate: string;
  series: CompanyStockRiskSeries[];
  riskSummary: string;
  drawdownSummary: string;
  positiveMonthsSummary: string;
}

export interface CompanyStockRiskStory {
  status: "available" | "unavailable";
  reason: string | null;
  symbol: string | null;
  asOf: string | null;
  availableRanges: StockRiskRange[];
  defaultRange: StockRiskRange | null;
  sectorBenchmark: {
    name: string;
    symbol: string;
    selectionBasis: string;
  } | null;
  periods: Partial<Record<StockRiskRange, CompanyStockRiskPeriod>>;
  source: {
    name: string;
    sourceRole: string;
    delayed: boolean;
    persisted: boolean;
    cost: string;
    priceBasis: string;
  };
  methodology: string;
  quality: {
    companyObservations: number | null;
    marketObservations: number | null;
    sectorObservations: number | null;
    alignment: string | null;
    warnings: string[];
  } | null;
}

export type MarketPricingMetricKey =
  | "pe"
  | "forward_pe"
  | "ev_ebitda"
  | "ev_ebit"
  | "price_sales"
  | "price_book"
  | "fcf_yield"
  | "peg";

export interface CompanyMarketPricingMetric {
  key: MarketPricingMetricKey;
  label: string;
  shortLabel: string;
  current: number;
  unit: "multiple" | "percent";
  basis: string;
}

export interface CompanyMarketPricingHistoryMetric {
  key: Exclude<MarketPricingMetricKey, "forward_pe" | "peg">;
  label: string;
  current: number;
  median: number;
  low: number;
  high: number;
  percentile: number;
  unit: "multiple" | "percent";
  observations: number;
}

export interface CompanyMarketPricingWindow {
  key: "5Y" | "10Y";
  years: number;
  startDate: string | null;
  endDate: string | null;
  observations: number;
  metrics: CompanyMarketPricingHistoryMetric[];
}

export interface CompanyMarketPricingPeer {
  cik: string | null;
  name: string;
  symbol: string;
  isCompany: boolean;
  price: number | null;
  pe: number | null;
  evEbitda: number | null;
  fcfYield: number | null;
  basis: string;
}

export interface CompanyMarketPricingStory {
  status: "available" | "unavailable";
  reason: string | null;
  symbol: string | null;
  currency: string | null;
  asOf: string | null;
  marketState: string | null;
  currentPrice: number | null;
  currentMetrics: CompanyMarketPricingMetric[];
  windows: Partial<Record<"5Y" | "10Y", CompanyMarketPricingWindow>>;
  valuationRead: string | null;
  impliedExpectations: Array<{
    key: "revenue_growth" | "fcf_growth" | "operating_margin" | "discount_rate";
    label: string;
    value: number;
    detail: string;
  }>;
  peerFramework: string;
  peers: CompanyMarketPricingPeer[];
  treasuryComparison: {
    fcfYield: number | null;
    treasuryYield: number | null;
    spread: number | null;
    treasuryAsOf: string | null;
    sourceName: string;
    sourceUrl: string;
    cost: string;
  } | null;
  analystEstimates: {
    status: "available" | "unavailable";
    reason: string | null;
    baseCase: number | null;
    bullCase: number | null;
    bearCase: number | null;
    analystCount: number | null;
    currentPrice: number | null;
    currentVsBasePercent: number | null;
    sourceName: string;
    sourceUrl: string | null;
    cost: string;
  } | null;
  takeaway: string | null;
  source: {
    financials: string;
    peerFramework: string;
    marketName: string;
    marketRole: string;
    marketUrl: string;
    cost: string;
    persisted: boolean;
    priceBasis: string;
  } | null;
  methodology: string;
  quality: {
    warnings: string[];
    historyObservations: number | null;
    peerQuotes: number | null;
    analystConsensusAvailable: boolean;
  } | null;
}

export type StockHistoryRange = "1Y" | "3Y" | "5Y" | "10Y" | "Max";
export type StockHistoryMode = "price" | "total_return" | "revenue" | "eps" | "free_cash_flow";

export interface CompanyStockHistoryPricePoint {
  date: string;
  close: number;
  adjustedClose: number;
  volume: number | null;
}

export interface CompanyStockHistoryFundamentalPoint {
  date: string;
  fiscalYear: number;
  value: number;
  sourceUrl: string | null;
  unit: string | null;
}

export interface CompanyStockHistoryFundamentalSeries {
  label: string;
  points: CompanyStockHistoryFundamentalPoint[];
}

export interface CompanyStockHistoryPerformance {
  range: StockHistoryRange;
  startDate: string;
  endDate: string;
  priceChangePercent: number | null;
  totalReturnPercent: number | null;
  cagrPercent: number | null;
  sp500TotalReturnPercent: number | null;
  high: number | null;
  low: number | null;
}

export interface CompanyStockHistoryEvent {
  date: string;
  type: "earnings" | "filing" | "corporate_action" | "strategy";
  title: string;
  detail: string;
  sourceUrl: string | null;
}

export interface CompanyStockHistoryStory {
  status: "available" | "unavailable";
  reason: string | null;
  symbol: string | null;
  currency: string | null;
  asOf: string | null;
  marketState: string | null;
  availableRanges: StockHistoryRange[];
  defaultRange: StockHistoryRange | null;
  prices: CompanyStockHistoryPricePoint[];
  fundamentals: {
    revenue: CompanyStockHistoryFundamentalSeries;
    eps: CompanyStockHistoryFundamentalSeries;
    freeCashFlow: CompanyStockHistoryFundamentalSeries;
  };
  events: CompanyStockHistoryEvent[];
  performance: Partial<Record<StockHistoryRange, CompanyStockHistoryPerformance>>;
  glance: {
    currentPrice: number | null;
    marketCap: number | null;
    dilutedShares: number | null;
    averageDailyVolume3m: number | null;
  };
  strategy: {
    status: "available" | "unavailable";
    reason: string | null;
    points: string[];
    sourceName: string;
    sourceDate: string | null;
    sourceUrl: string | null;
    cost: string;
    methodology: string;
  };
  source: {
    financials: string;
    filings: string;
    marketName: string;
    marketRole: string;
    marketUrl: string;
    benchmarkUrl: string;
    cost: string;
    persisted: boolean;
    priceBasis: string;
  } | null;
  methodology: string;
  quality: {
    warnings: string[];
    rawPriceObservations: number | null;
    displayPriceObservations: number | null;
    fundamentalYears: number[];
  } | null;
}

export type OperationsScope = "whole" | "continuing" | "discontinued" | "undetermined";

export interface PreparedFinancialSnapshot {
  snapshotId: string;
  key: string;
  identifier: string;
  fromYear: number;
  toYear: number;
}

export interface PreparedStoryDelivery {
  preparedAtUtc: string;
  snapshotId: string;
  isLatest: boolean;
  latestStatus: "ready" | "preparing" | "failed" | "unknown";
}

export interface CompanyMarketContext {
  companyId: string;
  snapshotId: string;
  snapshotKey: string;
  stockRisk: CompanyStockRiskStory;
  marketPricing: CompanyMarketPricingStory;
  stockHistory: CompanyStockHistoryStory;
}

export interface Company {
  preparedFinancialSnapshot?: PreparedFinancialSnapshot;
  preparedStoryDelivery?: PreparedStoryDelivery;
  marketContextStatus?: { status: "pending" | "loading" | "ready" | "error"; error?: string };
  supportedStoryChapters?: number[];
  country?: "USA" | "India";
  reportingBasis?: "standalone" | "consolidated";
  reportingBasisByYear?: Record<string, "standalone" | "consolidated">;
  annualCurrencyByYear?: Record<string, string | null>;
  id: string;
  logoUrl?: string;
  name: string;
  symbol: string;
  exchange?: string;
  sector: string;
  description: string;
  founded?: number;
  employees?: string;
  currency: string;
  reportingPeriod: string;
  updatedAt: string;
  metrics: Record<MetricKey, YearValue[]>;
  researchShelf?: ResearchShelfAnalysis;
  companyStory?: {
    revenueSegments?: CompanyRevenueStory;
    revenueOfferings?: {
      segmentReconciliation?: {
        sourceType?: RevenueBreakdownSourceType;
        reportingBasis?: "standalone" | "consolidated";
        periodType?: "ttm" | "annual";
        fiscalYear?: number;
        history?: Record<string, {
          sourceType?: RevenueBreakdownSourceType;
          basis?: string;
          methodology?: string;
          reportingBasis?: "standalone" | "consolidated";
          periodEnd: string; totalRevenue: number | null;
          items: { sourceType?: RevenueBreakdownSourceType; id: string; label: string; role: "segment" | "elimination" | "reconciliation";
            revenue: number | null; sourceUrls: string[];
            yoyGrowthPercent?: number | null; threePeriodCagrPercent?: number | null;
            threeYearCagrPercent?: number | null; yoyGrowthReason?: string | null;
            threeYearCagrReason?: string | null; growthComparisons?: unknown; growthSourceUrls?: string[] }[];
        }>;
        periodEnd: string; basis: string; methodology: string; totalRevenue: number | null;
        items: { sourceType?: RevenueBreakdownSourceType; id: string; label: string; role: "segment" | "elimination" | "reconciliation";
          revenue: number | null; sourceUrls: string[];
            yoyGrowthPercent?: number | null; threePeriodCagrPercent?: number | null;
            threeYearCagrPercent?: number | null; yoyGrowthReason?: string | null;
            threeYearCagrReason?: string | null; growthComparisons?: unknown; growthSourceUrls?: string[] }[];
      };
      offerings: RevenueOffering[];
      /** Reported disclosures may overlap; never use them to sum chart shares. */
      reportedOfferings?: RevenueOffering[];
      reportedOfferingsMethodology?: string;
      offeringAxis?: string | null;
      status?: "available" | "unavailable";
      reason?: string | null;
      annualTotals?: Record<string, number | null>;
      annualPeriodEnds?: Record<string, string>;
      annualReportingBases?: Record<string, "standalone" | "consolidated">;
      annualReportingCurrencies?: Record<string, string>;
      annualOperationsScopes?: Record<string, OperationsScope | null>;
      annualOperationsScopeGroups?: Record<string, string>;
      totalSourceByPeriod?: Record<string, string>;
      totalSourcesByPeriod?: Record<string, string[]>;
      methodology?: string;
      adjustmentMethodology?: string;
      adjustmentEntries?: RevenueAdjustmentEntry[];
      adjustmentImpacts?: RevenueAdjustmentImpact[];
      adjustedRevenue?: AdjustedRevenueStory;
      qualityWarnings?: string[];
      ttm?: { periodEnd: string; revenue: number; priorYearRevenue: number | null; reportingCurrency?: string;
        operationsScope?: OperationsScope | null; operationsScopeGroup?: string | null };
    };
    costStructure?: CompanyCostStructureStory;
    cashConversion?: CompanyCashConversionStory;
    balanceSheet?: CompanyBalanceSheetStory;
    stockRisk?: CompanyStockRiskStory;
    marketPricing?: CompanyMarketPricingStory;
    stockHistory?: CompanyStockHistoryStory;
  };
  notes: {
    growth: string;
    profitability: string;
    cash: string;
    debt: string;
  };
  statements?: StatementGroup[];
  filings?: FilingDocument[];
  limitations?: string[];
  dataMode?: "illustrative" | "tarasha-data";
  source?: {
    dataset: string;
    upstream: string;
    usage: string;
    persistence: string;
  };
}

export interface CatalogCompany {
  preparedDefaultRequest?: {
    identifier: string; from_year: number; to_year: number;
    include_market_context: boolean; include_industry_context: true; defer_market_context: true;
    constituent_identifiers: string[] | null; provenance: "references" | "full";
  };
  id: string;
  cik: string | null;
  name: string;
  ticker: string;
  exchange: string;
  country: "USA" | "India";
  provider: string;
  industryBucket?: string;
  data_available: number;
  data_access?: "normalized";
  firstFiscalYear?: number;
  latestFiscalYear?: number;
}

export interface StatementFact {
  /** Explicit presentation semantics for dimensionless ratios. */
  displayFormat?: "percentage" | "number";
  key: string;
  label: string;
  description: string;
  unit: string | null;
  values: YearValue[];
  /** API-defined supporting rows remain distinct from a verified combined row. */
  parentKey?: string;
  sourceMetric?: string;
  presentationRequired?: boolean;
  periodScope?: Array<{ period: { type: string; start?: string | null; end: string }; status: string;
    presentation?: string; reason?: string; ppe_fact_id?: number | null; rou_fact_ids?: number[]; evidence?: unknown }>;
  annualSources?: Array<{ year: number; periodEnd: string; periodStart: string | null; value: string | null; unit: string | null;
    displayUnit: string | null; sourceDisplayUnit?: string | null; displayScale?: number | string | null; displayFormat?: "percentage" | "number"; status?: string; reason?: string | null; sourceUrl: string | null;
    factId?: number | null; observationId?: number | null; provenance: Record<string, unknown> | null }>;
  /** Latest API-supplied TTM stays separate from fiscal-year series. */
  ttm?: { value: number | null; periodEnd: string; periodStart: string | null; unit: string | null;
    displayScale?: number | string | null; sourceDisplayUnit?: string | null;
    displayFormat?: "percentage" | "number"; sourceUrl: string | null; provenance: Record<string, unknown> | null;
    status?: string; reason?: string | null;
    factId?: number | null; observationId?: number | null; canonicalValue?: string | null; canonicalUnit?: string | null } | null;
}

export interface StatementGroup {
  key: "income" | "balance" | "cash" | "shares" | "ratios" | "kpis";
  label: string;
  ttmPeriodEnd?: string | null;
  annualPeriods?: string[];
  facts: StatementFact[];
}

export interface FilingDocument {
  accession: string;
  form: string;
  filed: string;
  period: string;
  title: string;
  url: string;
}

export type Page = "home" | "login" | "discover" | "company" | "compare" | "watchlist" | "learn" | "reconciliation";
