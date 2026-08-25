import type { Company, CompanyDeltaPoint, ProfitabilityMetricKey, ProfitabilityYearPoint } from "./types";

export const MAX_COMPARISON_COMPANIES = 5;

export type ComparisonYoYMetric = "revenue" | "grossProfit" | "operatingIncome";

export interface ComparisonYoYInterval {
  fromYear: number;
  toYear: number;
  values: Array<number | null>;
}

export interface ComparisonProfitabilityYear {
  year: number;
  values: Array<number | null>;
  absoluteValues: Array<number | null>;
}

const comparisonYoYFields: Record<ComparisonYoYMetric, keyof Pick<CompanyDeltaPoint, "revenueChangePercent" | "grossProfitChangePercent" | "operatingIncomeChangePercent">> = {
  revenue: "revenueChangePercent",
  grossProfit: "grossProfitChangePercent",
  operatingIncome: "operatingIncomeChangePercent",
};

const profitabilityAbsoluteFields: Record<ProfitabilityMetricKey, keyof Pick<ProfitabilityYearPoint, "grossProfit" | "operatingIncome" | "cogs" | "sga" | "da" | "rd">> = {
  grossMargin: "grossProfit",
  operatingMargin: "operatingIncome",
  cogsRatio: "cogs",
  sgaRatio: "sga",
  daRatio: "da",
  rdRatio: "rd",
};

export function researchShelfCompanies(companies: Company[]): Company[] {
  return companies.filter((company) => Boolean(company.researchShelf));
}

export function toggleComparisonCompany(
  selected: string[],
  companyId: string,
  availableCompanyIds: string[],
): string[] {
  const available = new Set(availableCompanyIds);
  const active = selected.filter((id) => available.has(id));
  if (active.includes(companyId)) return active.filter((id) => id !== companyId);
  if (!available.has(companyId) || active.length >= MAX_COMPARISON_COMPANIES) return active;
  return [...active, companyId];
}

export function comparisonYoYIntervals(companies: Company[], metric: ComparisonYoYMetric): ComparisonYoYInterval[] {
  const field = comparisonYoYFields[metric];
  const intervals = new Map<string, { fromYear: number; toYear: number }>();
  for (const company of companies) {
    for (const point of company.researchShelf?.companyDeltas ?? []) {
      intervals.set(`${point.fromYear}-${point.toYear}`, { fromYear: point.fromYear, toYear: point.toYear });
    }
  }
  return [...intervals.values()]
    .sort((left, right) => left.fromYear - right.fromYear || left.toYear - right.toYear)
    .map(({ fromYear, toYear }) => ({
      fromYear,
      toYear,
      values: companies.map((company) => {
        const point = company.researchShelf?.companyDeltas.find((item) => item.fromYear === fromYear && item.toYear === toYear);
        const value = point?.[field];
        return value !== undefined && value !== null && Number.isFinite(value) ? value : null;
      }),
    }));
}

export function comparisonProfitabilityYears(companies: Company[], metric: ProfitabilityMetricKey): ComparisonProfitabilityYear[] {
  const years = new Set(companies.flatMap((company) => company.researchShelf?.profitability.yearly.map((point) => point.year) ?? []));
  const absoluteField = profitabilityAbsoluteFields[metric];
  return [...years].sort((left, right) => left - right).map((year) => {
    const points = companies.map((company) => company.researchShelf?.profitability.yearly.find((point) => point.year === year));
    return {
      year,
      values: points.map((point) => point?.[metric] ?? null),
      absoluteValues: points.map((point) => point?.[absoluteField] ?? null),
    };
  });
}
