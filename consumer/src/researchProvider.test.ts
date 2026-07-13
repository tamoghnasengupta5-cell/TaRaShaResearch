import { afterEach, describe, expect, it, vi } from "vitest";
import { growthStatistics, pullResearchCompany, searchResearchCompanies } from "../functions/researchProvider";

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

  it("normalizes Indian amounts and derives the consumer research-shelf story", async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify([{ id: 44, name: "Example India", ticker: "EXAMPLE", country: "India" }]), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify([
        { company_id: 44, statement_key: "income", fact_key: "revenue", label: "Revenue", unit_kind: "amount", fiscal_year: 2024, value: 8000 },
        { company_id: 44, statement_key: "income", fact_key: "revenue", label: "Revenue", unit_kind: "amount", fiscal_year: 2025, value: 10000 },
        { company_id: 44, statement_key: "income", fact_key: "costOfRevenue", label: "Cost of revenue", unit_kind: "amount", fiscal_year: 2024, value: 5000 },
        { company_id: 44, statement_key: "income", fact_key: "costOfRevenue", label: "Cost of revenue", unit_kind: "amount", fiscal_year: 2025, value: 6000 },
        { company_id: 44, statement_key: "income", fact_key: "sga", label: "SG&A", unit_kind: "amount", fiscal_year: 2024, value: 1000 },
        { company_id: 44, statement_key: "income", fact_key: "sga", label: "SG&A", unit_kind: "amount", fiscal_year: 2025, value: 1100 },
        { company_id: 44, statement_key: "income", fact_key: "ebitda", label: "EBITDA", unit_kind: "amount", fiscal_year: 2024, value: 1600 },
        { company_id: 44, statement_key: "income", fact_key: "ebitda", label: "EBITDA", unit_kind: "amount", fiscal_year: 2025, value: 2000 },
        { company_id: 44, statement_key: "income", fact_key: "operatingIncome", label: "Operating income", unit_kind: "amount", fiscal_year: 2024, value: 1200 },
        { company_id: 44, statement_key: "income", fact_key: "operatingIncome", label: "Operating income", unit_kind: "amount", fiscal_year: 2025, value: 2000 },
        { company_id: 44, statement_key: "cash", fact_key: "operatingCash", label: "Operating cash flow", unit_kind: "amount", fiscal_year: 2025, value: 1500 },
        { company_id: 44, statement_key: "cash", fact_key: "capex", label: "Capital expenditure", unit_kind: "amount", fiscal_year: 2025, value: -500 },
        { company_id: 44, statement_key: "balance", fact_key: "totalDebt", label: "Total debt", unit_kind: "amount", fiscal_year: 2024, value: 2800 },
        { company_id: 44, statement_key: "balance", fact_key: "totalDebt", label: "Total debt", unit_kind: "amount", fiscal_year: 2025, value: 3000 },
        { company_id: 44, statement_key: "balance", fact_key: "cash", label: "Cash", unit_kind: "amount", fiscal_year: 2024, value: 800 },
        { company_id: 44, statement_key: "balance", fact_key: "cash", label: "Cash", unit_kind: "amount", fiscal_year: 2025, value: 1000 },
        { company_id: 44, statement_key: "balance", fact_key: "shortTermInvestments", label: "Short-term investments", unit_kind: "amount", fiscal_year: 2024, value: 200 },
        { company_id: 44, statement_key: "balance", fact_key: "shortTermInvestments", label: "Short-term investments", unit_kind: "amount", fiscal_year: 2025, value: 200 },
      ]), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify([
        { company_id: 44, metric_key: "spread", unit_kind: "percent", fiscal_year: 2024, value: 4 },
        { company_id: 44, metric_key: "spread", unit_kind: "percent", fiscal_year: 2025, value: 6 },
        { company_id: 44, metric_key: "fcff", unit_kind: "amount", fiscal_year: 2024, value: 900 },
        { company_id: 44, metric_key: "fcff", unit_kind: "amount", fiscal_year: 2025, value: 1200 },
      ]), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    const company = await pullResearchCompany(env, "research-44", 2024, 2025);

    expect(company?.currency).toBe("₹ crore");
    expect(company?.metrics.revenue).toEqual([{ year: 2024, value: 800 }, { year: 2025, value: 1000 }]);
    expect(company?.metrics.operatingMargin[1].value).toBe(20);
    expect(company?.metrics.freeCashFlow[0].value).toBe(100);
    expect(company?.metrics.netDebt[1].value).toBe(180);
    expect(company?.researchShelf?.revenueGrowth.median).toBe(25);
    expect(company?.researchShelf?.operatingCostGrowth.median).toBe(20);
    expect(company?.researchShelf?.sgaGrowth.median).toBe(10);
    expect(company?.researchShelf?.netDebtToEbitda).toEqual([{ year: 2024, value: 1.125 }, { year: 2025, value: 0.9 }]);
    expect(company?.researchShelf?.spread).toMatchObject({ median: 5, standardDeviation: Math.SQRT2 });
    expect(company?.researchShelf?.fcff).toEqual([{ year: 2024, value: 90 }, { year: 2025, value: 120 }]);
    expect(company?.dataMode).toBe("research-db");
  });

  it("uses sample deviation for year-over-year growth", () => {
    const stats = growthStatistics([{ year: 2022, value: 100 }, { year: 2023, value: 110 }, { year: 2024, value: 132 }]);
    expect(stats.median).toBeCloseTo(15);
    expect(stats.standardDeviation).toBeCloseTo(Math.sqrt(50));
    expect(stats.totalChange).toBeCloseTo(32);
  });
});
