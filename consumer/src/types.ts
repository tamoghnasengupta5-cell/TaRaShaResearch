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
  history: BusinessSegmentRevenueHistory[];
  accession: string | null;
  filedDate: string | null;
  sourceUrl: string | null;
}

export interface CompanyRevenueStory {
  status: "available" | "unavailable";
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
}

export interface CompanyCashConversionTrendPoint {
  fiscalYear: number;
  periodEnd: string;
  ebit: number;
  taxRatePercent: number;
  taxesOnOperatingProfit: number;
  nopat: number;
  depreciationAndAmortization: number;
  workingCapitalImpact: number;
  workingCapitalComponents: CompanyCashConversionComponent[];
  capitalExpenditure: number;
  fcff: number;
  conversionPercent: number | null;
  revenue: number;
  fcffRevenuePercent: number | null;
}

export interface CompanyCashConversionStory {
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

export interface Company {
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
    revenueSegments: CompanyRevenueStory;
    costStructure: CompanyCostStructureStory;
    cashConversion: CompanyCashConversionStory;
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
  key: string;
  label: string;
  description: string;
  unit: string;
  values: YearValue[];
}

export interface StatementGroup {
  key: "income" | "balance" | "cash" | "shares";
  label: string;
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

export type Page = "home" | "login" | "discover" | "company" | "compare" | "watchlist" | "learn";
