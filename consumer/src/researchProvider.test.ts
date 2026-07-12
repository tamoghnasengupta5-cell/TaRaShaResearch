import { afterEach, describe, expect, it, vi } from "vitest";
import { pullResearchCompany, searchResearchCompanies } from "../functions/researchProvider";

const env = { SHARED_RESEARCH_URL: "https://research.example", SHARED_RESEARCH_SERVICE_KEY: "server-secret" };

describe("shared Research provider", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("maps Research search rows into the provider-neutral catalogue", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify([
      { id: 9, name: "Microsoft Corporation", ticker: "MSFT", country: "USA" },
    ]), { status: 200 })));

    const companies = await searchResearchCompanies(env, "MSFT", "USA");

    expect(companies[0]).toMatchObject({ id: "research-9", ticker: "MSFT", data_access: "normalized", provider: "TaRaSha Research database" });
  });

  it("normalizes Indian amounts to crore and derives the four research metrics", async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify([{ id: 44, name: "Example India", ticker: "EXAMPLE", country: "India" }]), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify([
        { company_id: 44, statement_key: "income", fact_key: "revenue", label: "Revenue", unit_kind: "amount", fiscal_year: 2025, value: 10000 },
        { company_id: 44, statement_key: "income", fact_key: "operatingIncome", label: "Operating income", unit_kind: "amount", fiscal_year: 2025, value: 2000 },
        { company_id: 44, statement_key: "cash", fact_key: "operatingCash", label: "Operating cash flow", unit_kind: "amount", fiscal_year: 2025, value: 1500 },
        { company_id: 44, statement_key: "cash", fact_key: "capex", label: "Capital expenditure", unit_kind: "amount", fiscal_year: 2025, value: -500 },
        { company_id: 44, statement_key: "balance", fact_key: "totalDebt", label: "Total debt", unit_kind: "amount", fiscal_year: 2025, value: 3000 },
        { company_id: 44, statement_key: "balance", fact_key: "cash", label: "Cash", unit_kind: "amount", fiscal_year: 2025, value: 1000 },
        { company_id: 44, statement_key: "balance", fact_key: "shortTermInvestments", label: "Short-term investments", unit_kind: "amount", fiscal_year: 2025, value: 200 },
      ]), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    const company = await pullResearchCompany(env, "research-44", 2025, 2025);

    expect(company?.currency).toBe("₹ crore");
    expect(company?.metrics.revenue).toEqual([{ year: 2025, value: 1000 }]);
    expect(company?.metrics.operatingMargin[0].value).toBe(20);
    expect(company?.metrics.freeCashFlow[0].value).toBe(100);
    expect(company?.metrics.netDebt[0].value).toBe(180);
    expect(company?.dataMode).toBe("research-db");
  });
});
