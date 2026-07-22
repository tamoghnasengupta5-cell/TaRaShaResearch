import { companies as demoCompanies } from "./data/demo";
import { normalizeSecResearch, type SecCompanyFacts, type SecSubmissions } from "./sec";
import type { CatalogCompany, Company, DistributionObservation, ProfitabilityMetricKey, ResearchShelfAnalysis } from "./types";

export const MAX_SESSION_COMPANIES = 50;
export const MAX_YEAR_RANGE = 7;
export const liveDataEnabled = import.meta.env.VITE_DATA_MODE === "live";
const apiBase = String(import.meta.env.VITE_API_BASE_URL || "").replace(/\/$/, "");
const sessionId = `${crypto.randomUUID().replace(/-/g, "")}${Date.now().toString(36)}`;
const wait = (milliseconds: number) => new Promise((resolve) => setTimeout(resolve, milliseconds));

const requiredEarningsFlowMetrics = [
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
] as const;

const demoCatalog: CatalogCompany[] = demoCompanies.map((company) => ({
  id: company.id,
  cik: null,
  name: company.name,
  ticker: company.symbol,
  exchange: "Illustrative",
  country: "USA",
  provider: "TaRaSha preview",
  research_available: 1,
}));

async function requestJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${apiBase}${path}`, init);
  const payload = await response.json().catch(() => ({})) as { error?: string };
  if (!response.ok) throw new Error(payload.error || `Data request failed with status ${response.status}.`);
  return payload as T;
}

export function backfillResearchDistributions(shelf: ResearchShelfAnalysis): ResearchShelfAnalysis {
  const growthMetrics: Array<{
    comparison: keyof ResearchShelfAnalysis["growthComparisons"];
    delta: "revenueChangePercent" | "grossProfitChangePercent" | "operatingIncomeChangePercent";
  }> = [
    { comparison: "revenue", delta: "revenueChangePercent" },
    { comparison: "grossProfit", delta: "grossProfitChangePercent" },
    { comparison: "operatingIncome", delta: "operatingIncomeChangePercent" },
  ];
  for (const metric of growthMetrics) {
    const comparison = shelf.growthComparisons[metric.comparison];
    if (!Array.isArray(comparison.company.distribution)) {
      comparison.company.distribution = shelf.companyDeltas
        .filter((point) => point[metric.delta] !== null && Number.isFinite(point[metric.delta]))
        .map((point) => ({ label: `Company · FY ${point.fromYear}–${point.toYear}`, value: point[metric.delta]! }));
    }
    if (!Array.isArray(comparison.industryBucket.distribution)) comparison.industryBucket.distribution = [];
  }
  const profitabilityMetrics: ProfitabilityMetricKey[] = ["grossMargin", "operatingMargin", "cogsRatio", "sgaRatio", "daRatio", "rdRatio"];
  for (const metric of profitabilityMetrics) {
    const companyStatistics = shelf.profitability.statistics[metric];
    const industryStatistics = shelf.profitability.industryStatistics[metric];
    if (!Array.isArray(companyStatistics.distribution)) {
      companyStatistics.distribution = shelf.profitability.yearly
        .filter((point) => point[metric] !== null && Number.isFinite(point[metric]))
        .map((point) => ({ label: `Company · FY ${point.year}`, value: point[metric]! } satisfies DistributionObservation));
    }
    if (!Array.isArray(industryStatistics.distribution)) industryStatistics.distribution = [];
  }
  return shelf;
}

function normalizeResearchShelfContract(company: Company): Company {
  const shelf = company.researchShelf;
  const earningsFlowIsCurrent = Array.isArray(shelf?.earningsAndValuation?.earningsFlow)
    && shelf.earningsAndValuation.earningsFlow.every((point) => {
      const metrics = point?.metrics as Record<string, unknown> | undefined;
      return metrics && requiredEarningsFlowMetrics.every((key) => {
        const metric = metrics[key] as Record<string, unknown> | undefined;
        return metric
          && "companyValue" in metric
          && "industryMedian" in metric
          && "industryObservations" in metric
          && "companyMarginPercent" in metric
          && "industryMedianMarginPercent" in metric;
      });
    });
  if (
    !shelf
    || !shelf.growthComparisons
    || !Array.isArray(shelf.companyDeltas)
    || shelf.companyDeltas.some((item) => !("revenueChangePercent" in item) || !("grossProfitChangePercent" in item) || !("operatingIncomeChangePercent" in item))
    || !shelf.industryDeltas
    || !Array.isArray(shelf.industryDeltas.revenue)
    || !Array.isArray(shelf.industryDeltas.grossProfit)
    || !Array.isArray(shelf.industryDeltas.operatingIncome)
    || [...shelf.industryDeltas.revenue, ...shelf.industryDeltas.grossProfit, ...shelf.industryDeltas.operatingIncome].some((item) => !("companyChangePercent" in item) || !("industryMedianChangePercent" in item))
    || !Array.isArray(shelf.rawIncome)
    || !shelf.profitability
    || !shelf.profitability.statistics
    || !shelf.profitability.industryStatistics
    || !Array.isArray(shelf.profitability.yearly)
    || shelf.profitability.yearly.some((item) => !("grossProfit" in item) || !("operatingIncome" in item) || !("cogs" in item) || !("sga" in item) || !("da" in item) || !("rd" in item))
    || !shelf.profitability.industryComparisons
    || !shelf.profitability.performanceBands
    || !shelf.earningsAndValuation
    || !Array.isArray(shelf.earningsAndValuation.earningsFlow)
    || !earningsFlowIsCurrent
    || !shelf.earningsAndValuation.valuation
    || !shelf.earningsAndValuation.valuation.comparisons
  ) {
    throw new Error("TaRaShaConsumer received an older Research API response. Refresh the page after the Consumer deployment finishes, then pull the company again.");
  }
  if (!Array.isArray(shelf.industryConstituents)) shelf.industryConstituents = [];
  if (typeof shelf.industryConstituentsCustomized !== "boolean") shelf.industryConstituentsCustomized = false;
  backfillResearchDistributions(shelf);
  return company;
}

export async function searchCompanyCatalog(query: string, country: "USA" | "India"): Promise<CatalogCompany[]> {
  if (query.trim().length < 2) return [];
  if (!liveDataEnabled) {
    if (country === "India") return [];
    const normalized = query.toLowerCase();
    return demoCatalog.filter((company) => company.name.toLowerCase().includes(normalized) || company.ticker.toLowerCase().includes(normalized));
  }
  const result = await requestJson<{ companies: CatalogCompany[] }>(`/api/companies?query=${encodeURIComponent(query)}&country=${country}`);
  return result.companies;
}

export async function pullCompanyResearch(catalog: CatalogCompany, fromYear: number, toYear: number): Promise<Company> {
  if (toYear - fromYear + 1 > MAX_YEAR_RANGE) throw new Error(`Choose no more than ${MAX_YEAR_RANGE} years.`);
  if (!liveDataEnabled) {
    const demo = demoCompanies.find((company) => company.id === catalog.id);
    if (!demo) throw new Error("Live Indian filing extraction is not available in the preview.");
    await new Promise((resolve) => setTimeout(resolve, 350));
    return { ...demo, dataMode: "illustrative", limitations: ["This is fictional preview data. Enable the SEC/Cloudflare configuration for live US filings."] };
  }
  if (catalog.data_access === "normalized") {
    const params = new URLSearchParams({ companyId: catalog.id, fromYear: String(fromYear), toYear: String(toYear) });
    return normalizeResearchShelfContract(await requestJson<Company>(`/api/research/company?${params}`));
  }
  if (catalog.country !== "USA" || !catalog.research_available) throw new Error("A lawful free structured filing source is not available for this market yet.");
  await requestJson("/api/session/claim", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ companyId: catalog.id, sessionId, fromYear, toYear }),
  });
  const params = `companyId=${encodeURIComponent(catalog.id)}&sessionId=${encodeURIComponent(sessionId)}`;
  // Keep upstream SEC requests sequential. This is slightly slower for one user,
  // but makes aggregate traffic more predictable during the founding-user phase.
  const facts = await requestJson<SecCompanyFacts>(`/api/sec/companyfacts?${params}`);
  await wait(350);
  const submissions = await requestJson<SecSubmissions>(`/api/sec/submissions?${params}`);
  return normalizeSecResearch(catalog, facts, submissions, fromYear, toYear);
}

export async function recalculateIndustryConstituents(company: Company, constituentIds?: string[]): Promise<Company> {
  const shelf = company.researchShelf;
  if (!liveDataEnabled || company.dataMode !== "research-db" || !shelf) throw new Error("Industry constituent editing is available for live Research database companies only.");
  const updated = await requestJson<Company>("/api/research/company", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      companyId: company.id,
      fromYear: shelf.fromYear,
      toYear: shelf.toYear,
      constituentIds: constituentIds ?? null,
    }),
  });
  return normalizeResearchShelfContract(updated);
}
