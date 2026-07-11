import { companies as demoCompanies } from "./data/demo";
import { normalizeSecResearch, type SecCompanyFacts, type SecSubmissions } from "./sec";
import type { CatalogCompany, Company } from "./types";

export const MAX_SESSION_COMPANIES = 3;
export const MAX_YEAR_RANGE = 5;
export const liveDataEnabled = import.meta.env.VITE_DATA_MODE === "live";
const apiBase = String(import.meta.env.VITE_API_BASE_URL || "").replace(/\/$/, "");
const sessionId = `${crypto.randomUUID().replace(/-/g, "")}${Date.now().toString(36)}`;

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
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(payload.error || `Data request failed with status ${response.status}.`);
  return payload as T;
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
  const submissions = await requestJson<SecSubmissions>(`/api/sec/submissions?${params}`);
  return normalizeSecResearch(catalog, facts, submissions, fromYear, toYear);
}
