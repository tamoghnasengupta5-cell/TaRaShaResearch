import { describe, expect, it } from "vitest";
import { normalizeSecResearch, type SecCompanyFacts, type SecSubmissions } from "./sec";
import type { CatalogCompany } from "./types";

const catalog: CatalogCompany = { id: "us-sec-0000000123-demo", cik: "0000000123", name: "Demo Corp", ticker: "DEMO", exchange: "Nasdaq", country: "USA", provider: "SEC EDGAR", research_available: 1 };
const entry = (year: number, value: number, tag = "10-K") => ({ start: `${year}-01-01`, end: `${year}-12-31`, val: value, accn: `0000123-${year}-000001`, form: tag, filed: `${year + 1}-02-01`, fy: year, fp: "FY" });
const facts: SecCompanyFacts = {
  entityName: "Demo Corporation",
  facts: {
    "us-gaap": {
      Revenues: { units: { USD: [entry(2024, 1_000_000_000), entry(2025, 1_200_000_000)] } },
      OperatingIncomeLoss: { units: { USD: [entry(2024, 100_000_000), entry(2025, 144_000_000)] } },
      NetCashProvidedByUsedInOperatingActivities: { units: { USD: [entry(2024, 150_000_000), entry(2025, 180_000_000)] } },
      PaymentsToAcquirePropertyPlantAndEquipment: { units: { USD: [entry(2024, 50_000_000), entry(2025, 60_000_000)] } },
      CashAndCashEquivalentsAtCarryingValue: { units: { USD: [entry(2024, 200_000_000), entry(2025, 240_000_000)] } },
      LongTermDebt: { units: { USD: [entry(2024, 500_000_000), entry(2025, 480_000_000)] } },
    },
    dei: {
      EntityCommonStockSharesOutstanding: { units: { shares: [entry(2024, 80_000_000), entry(2025, 82_000_000)] } },
    },
  },
};
const submissions: SecSubmissions = {
  filings: { recent: {
    form: ["10-K", "10-Q", "8-K"],
    filingDate: ["2026-02-01", "2025-11-01", "2025-08-01"],
    reportDate: ["2025-12-31", "2025-09-30", "2025-06-30"],
    accessionNumber: ["0000123-26-000001", "0000123-25-000002", "0000123-25-000003"],
    primaryDocument: ["annual.htm", "quarter.htm", "current.htm"],
    items: ["", "", "2.02"],
  } },
};

describe("SEC filing normalization", () => {
  it("extracts statements and calculates derived annual metrics", () => {
    const company = normalizeSecResearch(catalog, facts, submissions, 2024, 2026);
    expect(company.name).toBe("Demo Corporation");
    expect(company.metrics.revenue.at(-1)?.value).toBe(1200);
    expect(company.metrics.operatingMargin.at(-1)?.value).toBe(12);
    expect(company.metrics.freeCashFlow.at(-1)?.value).toBe(120);
    expect(company.metrics.netDebt.at(-1)?.value).toBe(240);
    expect(company.statements?.find((group) => group.key === "shares")?.facts[0].values.at(-1)?.value).toBe(82);
  });

  it("links annual, quarterly and earnings-related current reports", () => {
    const company = normalizeSecResearch(catalog, facts, submissions, 2024, 2026);
    expect(company.filings?.map((filing) => filing.form)).toEqual(["10-K", "10-Q", "8-K"]);
    expect(company.filings?.[0].url).toContain("/Archives/edgar/data/123/000012326000001/annual.htm");
  });
});
