export interface ResearchProviderEnv {
  SHARED_RESEARCH_URL?: string;
  SHARED_RESEARCH_SERVICE_KEY?: string;
}

interface ResearchCompanyRow {
  id: number;
  name: string;
  ticker: string;
  country: "USA" | "India";
  industry_bucket: string;
}

interface ResearchFactRow {
  company_id: number;
  statement_key: "income" | "balance" | "cash" | "shares";
  fact_key: string;
  label: string;
  unit_kind: "amount" | "shares";
  fiscal_year: number;
  value: number;
}

interface ResearchBucketMembershipRow {
  company_id: number;
  bucket_id: number;
  bucket_name: string;
}

interface ResearchIndustryFactRow {
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
    | "netIncomeToCommon";
  fiscal_year: number;
  value: number;
}

interface ResearchMarketMetricRow {
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
  revenue: "Sales or operating revenue imported into TaRaSha Research.",
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
  cash: "Cash and cash-equivalent balance.",
  shortTermInvestments: "Reported short-term investments.",
  accountsReceivable: "Amounts due from customers and other debtors.",
  inventory: "Reported inventory balance.",
  currentAssets: "Assets expected to turn into cash or be used in the operating cycle.",
  assets: "Total reported assets.",
  accountsPayable: "Amounts owed to suppliers and other creditors.",
  currentDebt: "Borrowings classified as current.",
  currentLiabilities: "Obligations classified as current.",
  longTermLiabilities: "Reported longer-term obligations.",
  totalDebt: "Total reported borrowings.",
  equity: "Reported shareholders’ equity.",
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

function requireConfiguration(env: ResearchProviderEnv): { base: string; key: string } {
  const base = String(env.SHARED_RESEARCH_URL ?? "").replace(/\/$/, "");
  const key = String(env.SHARED_RESEARCH_SERVICE_KEY ?? "");
  if (!base || !key) throw new Error("Shared Research database provider is not configured.");
  return { base, key };
}

async function researchFetch<T>(env: ResearchProviderEnv, path: string): Promise<T> {
  const { base, key } = requireConfiguration(env);
  const response = await fetch(`${base}/rest/v1/${path}`, {
    headers: { apikey: key, Authorization: `Bearer ${key}`, Accept: "application/json" },
  });
  if (!response.ok) throw new Error(`Shared Research database returned ${response.status}.`);
  return response.json<T>();
}

async function researchFetchAll<T>(env: ResearchProviderEnv, path: string, pageSize = 1000): Promise<T[]> {
  const rows: T[] = [];
  for (let offset = 0; ; offset += pageSize) {
    const separator = path.includes("?") ? "&" : "?";
    const page = await researchFetch<T[]>(env, `${path}${separator}limit=${pageSize}&offset=${offset}`);
    rows.push(...page);
    if (page.length < pageSize) return rows;
  }
}

export function researchProviderEnabled(env: { DATA_PROVIDER?: string }): boolean {
  return env.DATA_PROVIDER === "research-db";
}

export async function searchResearchCompanies(env: ResearchProviderEnv, query: string, country: "USA" | "India") {
  const safeQuery = query.replace(/[^a-zA-Z0-9 .&-]/g, "").trim().slice(0, 60);
  if (safeQuery.length < 2) return [];
  const params = new URLSearchParams({
    select: "id,name,ticker,country,industry_bucket",
    country: `eq.${country}`,
    or: `(name.ilike.*${safeQuery}*,ticker.ilike.*${safeQuery}*)`,
    limit: "30",
  });
  const rows = await researchFetch<ResearchCompanyRow[]>(env, `consumer_companies?${params}`);
  return rows
    .sort((left, right) => Number(left.ticker.toLowerCase() !== safeQuery.toLowerCase()) - Number(right.ticker.toLowerCase() !== safeQuery.toLowerCase()) || left.name.localeCompare(right.name))
    .map((row) => ({
      id: `research-${row.id}`,
      cik: null,
      name: row.name,
      ticker: row.ticker,
      exchange: row.country,
      country: row.country,
      provider: "TaRaSha Research database",
      industryBucket: row.industry_bucket,
      research_available: 1,
      data_access: "normalized" as const,
    }));
}

function seriesFor(facts: ResearchFactRow[], key: string): YearValue[] {
  return facts.filter((fact) => fact.fact_key === key)
    .map((fact) => ({ year: fact.fiscal_year, value: fact.value }))
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
    return {
      fromYear: year - 1,
      toYear: year,
      revenue: nullableValueByYear(revenueDeltas, year),
      revenueChangePercent: nullableValueByYear(revenueChanges, year),
      grossProfit: nullableValueByYear(grossProfitDeltas, year),
      grossProfitChangePercent: nullableValueByYear(grossProfitChanges, year),
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
  return Array.from({ length: toYear - fromYear + 1 }, (_, index) => {
    const year = fromYear + index;
    return {
      year,
      revenue: revenueByYear.get(year) ?? null,
      revenueChangePercent: growth(revenueByYear, year),
      grossProfit: grossProfitByYear.get(year) ?? null,
      grossProfitChangePercent: growth(grossProfitByYear, year),
      operatingIncome: operatingIncomeByYear.get(year) ?? null,
      operatingIncomeChangePercent: growth(operatingIncomeByYear, year),
    };
  });
}

function groupIndustrySeries(facts: ResearchIndustryFactRow[], factKey: ResearchIndustryFactRow["fact_key"]): Map<number, YearValue[]> {
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
  // Research historically used a tiny sentinel for an undisclosed R&D row.
  // Treat it as missing in Consumer rather than displaying invented precision.
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
    dilutedShares: null,
    eps: null,
  };
}

function industryEarningsSeries(
  industryFacts: ResearchIndustryFactRow[],
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
  marketByCompany: Map<number, ResearchMarketMetricRow>,
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
    enterpriseValueDetail: companyMarket?.enterprise_value_detail ?? "No saved Enterprise Value snapshot is available in TaRaSha Research.",
    comparisons,
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

export async function pullResearchCompany(env: ResearchProviderEnv, companyId: string, fromYear: number, toYear: number, customConstituentIds?: number[]) {
  const numericId = Number(companyId.replace(/^research-/, ""));
  if (!Number.isInteger(numericId) || numericId <= 0) throw new Error("Invalid Research company identifier.");
  const constituentIds = customConstituentIds === undefined
    ? undefined
    : [...new Set(customConstituentIds.filter((id) => Number.isInteger(id) && id > 0))];
  if (constituentIds && (!constituentIds.length || constituentIds.length > 100)) throw new Error("Select between 1 and 100 industry constituents.");
  const companyParams = new URLSearchParams({ select: "id,name,ticker,country,industry_bucket", id: `eq.${numericId}`, limit: "1" });
  const companies = await researchFetch<ResearchCompanyRow[]>(env, `consumer_companies?${companyParams}`);
  const company = companies[0];
  if (!company) return null;
  const factParams = new URLSearchParams({
    select: "company_id,statement_key,fact_key,label,unit_kind,fiscal_year,value",
    company_id: `eq.${numericId}`,
    fiscal_year: `gte.${fromYear}`,
    and: `(fiscal_year.lte.${toYear})`,
    order: "statement_key.asc,fact_key.asc,fiscal_year.asc",
  });
  const membershipParams = new URLSearchParams({
    select: "company_id,bucket_id,bucket_name",
    company_id: `eq.${numericId}`,
    order: "bucket_name.asc",
  });
  const [rawFacts, memberships] = await Promise.all([
    researchFetch<ResearchFactRow[]>(env, `consumer_financial_facts?${factParams}`),
    researchFetch<ResearchBucketMembershipRow[]>(env, `consumer_industry_bucket_memberships?${membershipParams}`),
  ]);
  const bucketIds = [...new Set(memberships.map((membership) => membership.bucket_id))];
  let rawIndustryFacts: ResearchIndustryFactRow[] = [];
  if (constituentIds?.length || bucketIds.length) {
    const industryParams = new URLSearchParams({
      select: "bucket_id,bucket_name,company_id,country,fact_key,fiscal_year,value",
      country: `eq.${company.country}`,
      fact_key: "in.(revenue,costOfRevenue,sga,researchAndDevelopment,ebitda,depreciation,ebit,operatingIncome,interestExpense,pretaxIncome,netIncome,minorityInterestInEarnings,earningsFromDiscontinuedOperations,commonDividendsPaid,netIncomeToCommon)",
      fiscal_year: `gte.${fromYear}`,
      and: `(fiscal_year.lte.${toYear})`,
      order: "company_id.asc,fact_key.asc,fiscal_year.asc",
    });
    if (constituentIds) industryParams.set("company_id", `in.(${constituentIds.join(",")})`);
    else industryParams.set("bucket_id", `in.(${bucketIds.join(",")})`);
    rawIndustryFacts = await researchFetchAll<ResearchIndustryFactRow>(env, `consumer_industry_income_facts?${industryParams}`);
  }
  const amountScale = company.country === "India" ? 10 : 1;
  const amountUnit = company.country === "India" ? "₹ crore" : "US$ million";
  const facts = rawFacts.map((fact) => ({ ...fact, value: fact.unit_kind === "amount" ? Number(fact.value) / amountScale : Number(fact.value) }));
  const industryFacts = rawIndustryFacts.map((fact) => ({ ...fact, value: Number(fact.value) / amountScale }));
  const statements = (Object.keys(statementLabels) as Array<keyof typeof statementLabels>).map((statementKey) => {
    const statementFacts = facts.filter((fact) => fact.statement_key === statementKey);
    const grouped = new Map<string, ResearchFactRow[]>();
    for (const fact of statementFacts) grouped.set(fact.fact_key, [...(grouped.get(fact.fact_key) ?? []), fact]);
    return {
      key: statementKey,
      label: statementLabels[statementKey],
      facts: [...grouped.entries()].map(([key, values]) => ({
        key,
        label: values[0].label,
        description: factDescriptions[key] ?? "Imported historical financial fact.",
        unit: values[0].unit_kind === "shares" ? "million shares" : amountUnit,
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
  const interestExpense = seriesFor(facts, "interestExpense");
  const ebt = seriesFor(facts, "pretaxIncome");
  const netProfit = seriesFor(facts, "netIncome");
  const minorityInterestInEarnings = seriesFor(facts, "minorityInterestInEarnings");
  const earningsFromDiscontinuedOperations = seriesFor(facts, "earningsFromDiscontinuedOperations");
  const commonDividendsPaid = seriesFor(facts, "commonDividendsPaid");
  const netIncomeToCommon = seriesFor(facts, "netIncomeToCommon");
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
  const industryInterestExpense = groupIndustrySeries(industryFacts, "interestExpense");
  const industryEbt = groupIndustrySeries(industryFacts, "pretaxIncome");
  const industryNetProfit = groupIndustrySeries(industryFacts, "netIncome");
  const industryMinorityInterestInEarnings = groupIndustrySeries(industryFacts, "minorityInterestInEarnings");
  const industryEarningsFromDiscontinuedOperations = groupIndustrySeries(industryFacts, "earningsFromDiscontinuedOperations");
  const industryCommonDividendsPaid = groupIndustrySeries(industryFacts, "commonDividendsPaid");
  const industryNetIncomeToCommon = groupIndustrySeries(industryFacts, "netIncomeToCommon");
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
  const constituentMetadataIds = constituentIds ?? [...new Set(industryFacts.map((fact) => fact.company_id))];
  let constituentCompanies: ResearchCompanyRow[] = [];
  if (constituentMetadataIds.length) {
    const constituentParams = new URLSearchParams({
      select: "id,name,ticker,country,industry_bucket",
      id: `in.(${constituentMetadataIds.join(",")})`,
      country: `eq.${company.country}`,
      order: "name.asc",
    });
    constituentCompanies = await researchFetch<ResearchCompanyRow[]>(env, `consumer_companies?${constituentParams}`);
  }
  const industryConstituents = constituentCompanies.map((item) => ({
    id: `research-${item.id}`,
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
  });
  const marketMetricIds = [...new Set([numericId, ...constituentMetadataIds])];
  let rawMarketMetrics: ResearchMarketMetricRow[] = [];
  if (marketMetricIds.length) {
    const marketParams = new URLSearchParams({
      select: "company_id,enterprise_value,enterprise_value_source,enterprise_value_as_of,enterprise_value_detail,trailing_pe,trailing_pe_source,trailing_pe_as_of,trailing_pe_detail,updated_at",
      company_id: `in.(${marketMetricIds.join(",")})`,
      order: "company_id.asc",
    });
    rawMarketMetrics = await researchFetch<ResearchMarketMetricRow[]>(env, `consumer_market_metrics?${marketParams}`);
  }
  const marketByCompany = new Map(rawMarketMetrics.map((item) => [item.company_id, {
    ...item,
    enterprise_value: item.enterprise_value === null ? null : Number(item.enterprise_value) / amountScale,
    trailing_pe: item.trailing_pe === null ? null : Number(item.trailing_pe),
  }]));
  const latestYear = Math.max(...facts.map((fact) => fact.fiscal_year), toYear);
  return {
    id: `research-${company.id}`,
    name: company.name,
    symbol: company.ticker,
    sector: company.industry_bucket || `${company.country} · TaRaSha Research coverage`,
    description: "Historical financial statements imported through the TaRaSha Private Research bulk-upload workflow.",
    currency: amountUnit,
    reportingPeriod: `FY ${latestYear}`,
    updatedAt: new Date().toLocaleDateString("en-GB", { day: "numeric", month: "long", year: "numeric", timeZone: "UTC" }),
    metrics: {
      revenue,
      operatingMargin,
      freeCashFlow: derivedSeries(operatingCash, capex, (a, b) => a - Math.abs(b)),
      netDebt,
    },
    researchShelf: {
      fromYear,
      toYear,
      industryBucket: company.industry_bucket || "Unclassified",
      industryCompanyCount,
      industryConstituents,
      industryConstituentsCustomized: constituentIds !== undefined,
      growthComparisons: {
        revenue: {
          company: growthStatistics(revenue),
          industryBucket: pooledIndustryGrowthStatistics(industryRevenue, industryCompanyLabels),
        },
        grossProfit: {
          company: growthStatistics(grossProfit),
          industryBucket: pooledIndustryGrowthStatistics(industryGrossProfit, industryCompanyLabels),
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
    },
    notes: {
      growth: "Review the imported multi-year revenue record and the underlying spreadsheet source before drawing conclusions.",
      profitability: "Operating margin is calculated from imported operating income divided by imported revenue.",
      cash: "Free cash flow is calculated as imported operating cash flow less the absolute value of imported capital expenditure.",
      debt: "Net debt is calculated as imported total debt less cash and short-term investments when available.",
    },
    statements,
    filings: [],
    limitations: [
      "Private, non-commercial preview. Commercial redistribution is disabled pending source-provider permission.",
      "The figures originate in spreadsheets downloaded through StockAnalysis.com and bulk-uploaded into TaRaSha Research; StockAnalysis.com may use third-party data providers.",
      "Imported values can differ from issuer filings because of provider definitions, restatements, currency units or spreadsheet mapping.",
      "TaRaSha Consumer stores no separate financial copy; this response is held only in browser session memory.",
    ],
    dataMode: "research-db" as const,
    source: {
      dataset: "TaRaSha Research bulk-upload database",
      upstream: "StockAnalysis.com spreadsheet download",
      usage: "Private non-commercial evaluation only",
      persistence: "Browser session memory only",
    },
  };
}
