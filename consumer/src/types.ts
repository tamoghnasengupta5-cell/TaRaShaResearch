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

export interface CompanyDeltaPoint {
  fromYear: number;
  toYear: number;
  revenue: number | null;
  revenueChangePercent: number | null;
  grossProfit: number | null;
  grossProfitChangePercent: number | null;
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
}

export interface Company {
  id: string;
  name: string;
  symbol: string;
  sector: string;
  description: string;
  founded?: number;
  employees?: string;
  currency: string;
  reportingPeriod: string;
  updatedAt: string;
  metrics: Record<MetricKey, YearValue[]>;
  researchShelf?: ResearchShelfAnalysis;
  notes: {
    growth: string;
    profitability: string;
    cash: string;
    debt: string;
  };
  statements?: StatementGroup[];
  filings?: FilingDocument[];
  limitations?: string[];
  dataMode?: "illustrative" | "sec-live" | "research-db";
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
  research_available: number;
  data_access?: "sec" | "normalized";
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
  form: "10-K" | "10-Q" | "8-K";
  filed: string;
  period: string;
  title: string;
  url: string;
}

export type Page = "home" | "login" | "discover" | "company" | "compare" | "watchlist" | "learn";
