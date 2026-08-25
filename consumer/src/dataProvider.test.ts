import { afterEach, describe, expect, it, vi } from "vitest";
import {
  buildCashFlowAnalysis,
  calculateGrossOperatingLeverage,
  fetchDataCompanyLogo,
  growthStatistics,
  pullDataCompany,
  searchDataCompanies,
} from "../functions/dataProvider";

const env = { TARASHA_DATA_API_URL: "https://data.example" };

function item(metric: string, label: string, year: number, value: number, unit = "USD millions") {
  return {
    metric,
    metric_label: label,
    display: { value: String(value), unit },
    period: { type: "annual", end: `${year}-12-31` },
  };
}

function dataset(
  id: number,
  cik: string,
  name: string,
  ticker: string,
  revenue: [number, number],
  cost: [number, number],
  operatingIncome: [number, number],
) {
  const years = [2024, 2025] as const;
  const incomeItems = years.flatMap((year, index) => [
    item("revenue", "Revenue", year, revenue[index]),
    item("cost_of_revenue", "Cost of Revenue", year, cost[index]),
    item("selling_general_admin", "Selling, General & Admin", year, 100 + index * 10),
    item("research_development", "Research & Development", year, 20 + index * 5),
    item("operating_income", "Operating Income", year, operatingIncome[index]),
    item("ebitda", "EBITDA", year, operatingIncome[index] + 40),
    item("ebit", "EBIT", year, operatingIncome[index]),
    item("depreciation_amortization", "Depreciation & Amortization", year, 40),
    item("interest_expense", "Interest Expense", year, -10),
    item("pretax_income", "Pretax Income", year, operatingIncome[index] - 10),
    item("effective_tax_rate", "Effective Tax Rate", year, 0.25, "ratio"),
    item("net_income", "Net Income", year, operatingIncome[index] - 40),
    item("net_income_common", "Net Income to Common", year, operatingIncome[index] - 45),
    item("shares_outstanding_diluted", "Shares Outstanding (Diluted)", year, 50, "shares millions"),
    item("eps_diluted", "EPS (Diluted)", year, 3, "USD per share"),
  ]);
  const balanceItems = [2023, 2024, 2025].flatMap((year, index) => [
    item("current_assets", "Current Assets", year, 480 + index * 20),
    item("cash", "Cash & Cash Equivalents", year, 90 + index * 10),
    item("short_term_investments", "Short-Term Investments", year, 20),
    item("current_liabilities", "Current Liabilities", year, 330 + index * 20),
    item("short_term_debt", "Short-Term Debt", year, 40),
    item("total_debt", "Total Debt", year, 300 + index * 20),
  ]);
  const cashItems = years.flatMap((year, index) => [
    item("operating_cash_flow", "Operating Cash Flow", year, 180 + index * 20),
    item("capital_expenditures", "Capital Expenditures", year, -50),
    item("share_based_compensation", "Share-Based Compensation", year, 10),
    item("other_adjustments", "Other Adjustments", year, 5),
    item("net_long_term_debt_issued_repaid", "Net Long-Term Debt Issued (Repaid)", year, 0),
    item("common_dividends_paid", "Common Dividends Paid", year, -20),
  ]);
  return {
    company: {
      id,
      cik,
      name,
      sic_description: "Computer Systems",
      reporting_currency: "USD",
      aliases: [{ ticker, exchange: "Nasdaq", is_current: true }],
      updated_at: "2026-08-20T12:00:00Z",
    },
    income: { statement: "income", metrics: [], items: incomeItems },
    balance: { statement: "balance", metrics: [], items: balanceItems },
    cash_flow: { statement: "cash_flow", metrics: [], items: cashItems },
  };
}

function discoverPayload(customized = false) {
  const target = dataset(1, "0000000001", "Alpha Systems", "ALPHA", [800, 1000], [500, 600], [120, 200]);
  const peer = dataset(2, "0000000002", "Beta Systems", "BETA", [1200, 1320], [700, 750], [180, 198]);
  const constituents = [
    {
      identifier: "0000000001",
      cik: "0000000001",
      name: "Alpha Systems",
      ticker: "ALPHA",
      exchange: "Nasdaq",
      country: "USA",
      industry_buckets: [{ id: 7, name: "Computer Systems" }],
    },
    {
      identifier: "0000000002",
      cik: "0000000002",
      name: "Beta Systems",
      ticker: "BETA",
      exchange: "Nasdaq",
      country: "USA",
      industry_buckets: [{ id: 7, name: "Computer Systems" }],
    },
  ];
  return {
    company: target.company,
    business_segments: {
      status: "available",
      reason: null,
      latest_fiscal_year: 2025,
      latest_period_end: "2025-12-31",
      reporting_currency: "USD",
      total_revenue_display: { value: "1000.00", unit: "USD millions" },
      summary: "Cloud is the largest reported revenue segment.",
      methodology: "Issuer-filed annual segment revenue facts.",
      segments: [{
        member: "test:CloudMember",
        name: "Cloud",
        latest_revenue_display: { value: "700.00", unit: "USD millions" },
        percentage_of_total: "70.00",
        yoy_growth_percent: "16.67",
        three_year_cagr_percent: null,
        accession: "0000000001-25-000001",
        filed_date: "2026-02-15",
        source_url: "https://www.sec.gov/Archives/alpha-20251231.xml",
        history: [{
          fiscal_year: 2025,
          period_start: "2025-01-01",
          period_end: "2025-12-31",
          revenue_display: { value: "700.00", unit: "USD millions" },
          accession: "0000000001-25-000001",
          filed_date: "2026-02-15",
          source_url: "https://www.sec.gov/Archives/alpha-20251231.xml",
        }],
      }],
    },
    operating_cost_structure: {
      status: "available",
      reason: null,
      latest_fiscal_year: 2025,
      latest_period_end: "2025-12-31",
      reporting_currency: "USD",
      years: [2024, 2025],
      summary: "FY2025 retained $40.00 as gross profit; $20.00 reached operating profit per $100 of revenue.",
      methodology: "Normalized annual income-statement lines divided by reported revenue.",
      source_url: "https://www.sec.gov/Archives/alpha-20251231.xml",
      lines: [
        {
          key: "revenue",
          label: "Revenue",
          role: "revenue",
          latest_value_per_hundred: "100.00",
          history: [
            { fiscal_year: 2024, period_end: "2024-12-31", value_per_hundred: "100.00" },
            { fiscal_year: 2025, period_end: "2025-12-31", value_per_hundred: "100.00" },
          ],
          lineage: { kind: "mapped", concept: "Revenue", source_url: "https://www.sec.gov/Archives/alpha-20251231.xml" },
        },
        {
          key: "cost_of_revenue",
          label: "Cost of Revenue",
          role: "expense",
          latest_value_per_hundred: "-60.00",
          history: [
            { fiscal_year: 2024, period_end: "2024-12-31", value_per_hundred: "-62.50" },
            { fiscal_year: 2025, period_end: "2025-12-31", value_per_hundred: "-60.00" },
          ],
          lineage: { kind: "mapped", concept: "CostOfRevenue", source_url: "https://www.sec.gov/Archives/alpha-20251231.xml" },
        },
        {
          key: "gross_profit",
          label: "Gross Profit",
          role: "subtotal",
          latest_value_per_hundred: "40.00",
          history: [
            { fiscal_year: 2024, period_end: "2024-12-31", value_per_hundred: "37.50" },
            { fiscal_year: 2025, period_end: "2025-12-31", value_per_hundred: "40.00" },
          ],
          lineage: { kind: "derived", derivation_method: "revenue_minus_cost", formula: "Revenue − Cost", source_url: "https://www.sec.gov/Archives/alpha-20251231.xml" },
        },
        {
          key: "selling_general_admin",
          label: "Selling, General & Administrative",
          role: "expense",
          latest_value_per_hundred: "-11.00",
          history: [{ fiscal_year: 2025, period_end: "2025-12-31", value_per_hundred: "-11.00" }],
          lineage: { kind: "mapped", concept: "SellingGeneralAndAdministrativeExpense", source_url: "https://www.sec.gov/Archives/alpha-20251231.xml" },
        },
        {
          key: "research_development",
          label: "Research & Development",
          role: "expense",
          latest_value_per_hundred: "-2.50",
          history: [{ fiscal_year: 2025, period_end: "2025-12-31", value_per_hundred: "-2.50" }],
          lineage: { kind: "mapped", concept: "ResearchAndDevelopmentExpense", source_url: "https://www.sec.gov/Archives/alpha-20251231.xml" },
        },
        {
          key: "operating_income",
          label: "Operating Profit",
          role: "subtotal",
          latest_value_per_hundred: "20.00",
          history: [
            { fiscal_year: 2024, period_end: "2024-12-31", value_per_hundred: "15.00" },
            { fiscal_year: 2025, period_end: "2025-12-31", value_per_hundred: "20.00" },
          ],
          lineage: { kind: "mapped", concept: "OperatingIncomeLoss", source_url: "https://www.sec.gov/Archives/alpha-20251231.xml" },
        },
      ],
    },
    cash_conversion: {
      status: "available",
      reason: null,
      latest_fiscal_year: 2025,
      latest_period_end: "2025-12-31",
      reporting_currency: "USD",
      display_unit: "USD millions",
      years: [2024, 2025],
      bridge: [
        {
          key: "ebit",
          label: "Operating Profit (EBIT)",
          operation: "base",
          value: "200.00",
          formula: null,
          lineage: { kind: "mapped", concept: "OperatingIncomeLoss", source_url: "https://www.sec.gov/Archives/alpha-20251231.xml" },
          components: [],
        },
        {
          key: "working_capital_impact",
          label: "Operating Working Capital Cash Effect",
          operation: "subtract",
          value: "-5.00",
          formula: "Sum of normalized working-capital cash-flow components",
          lineage: {},
          components: [{
            key: "change_receivables",
            label: "Change in Receivables",
            value: "-5.00",
            lineage: { kind: "mapped", concept: "IncreaseDecreaseInAccountsReceivable", source_url: "https://www.sec.gov/Archives/alpha-20251231.xml" },
          }],
        },
        {
          key: "fcff",
          label: "Free Cash Flow to the Firm (FCFF)",
          operation: "total",
          value: "151.00",
          formula: "NOPAT + D&A + working-capital cash effect − Capex",
          lineage: {},
          components: [],
        },
      ],
      trend: [{
        fiscal_year: 2025,
        period_end: "2025-12-31",
        ebit: "200.00",
        tax_rate_percent: "25.00",
        taxes_on_operating_profit: "50.00",
        nopat: "150.00",
        depreciation_and_amortization: "36.00",
        working_capital_impact: "-5.00",
        working_capital_components: [],
        capital_expenditure: "30.00",
        fcff: "151.00",
        conversion_percent: "75.50",
        revenue: "1000.00",
        fcff_revenue_percent: "15.10",
      }],
      metrics: {
        fcff: "151.00",
        prior_fiscal_year: 2024,
        prior_fcff: "125.00",
        conversion_percent: "75.50",
        prior_conversion_percent: "62.50",
        growth_percent: "20.80",
        prior_growth_percent: null,
        revenue_percent: "15.10",
        prior_revenue_percent: "12.50",
        cagr_percent: "20.80",
      },
      summary: "FCFF grew and conversion improved.",
      takeaway: "Working capital absorbed cash while investment remained disciplined.",
      methodology: "FCFF uses normalized annual TaRaShaData inputs only.",
      source_url: "https://www.sec.gov/Archives/alpha-20251231.xml",
    },
    normalized_coverage: {
      available: true,
      data_access: "normalized",
      source: "SEC filings",
      first_fiscal_year: 2024,
      latest_fiscal_year: 2025,
      annual_observation_count: 120,
      metric_count: 38,
      statements: [],
    },
    country: "USA",
    industry_buckets: [{ id: 7, name: "Computer Systems" }],
    constituents_customized: customized,
    constituents: customized ? constituents.slice(1) : constituents,
    datasets: [target, peer],
    filings: [{
      accession: "0000000001-25-000001",
      form: "10-K",
      filing_date: "2026-02-15",
      report_date: "2025-12-31",
      primary_document: "alpha-20251231.htm",
      source_url: "https://www.sec.gov/Archives/alpha-20251231.htm",
    }],
  };
}

describe("TaRaShaData.ai provider", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("proxies a validated company logo only from the TaRaShaData.ai logo endpoint", async () => {
    const logo = new Uint8Array([0x89, 0x50, 0x4e, 0x47]);
    const fetchMock = vi.fn().mockResolvedValue(new Response(logo, {
      status: 200,
      headers: {
        "content-type": "image/png",
        etag: '"logo-sha"',
        "x-tarashadata-logo-source": "wikimedia_logo",
      },
    }));
    vi.stubGlobal("fetch", fetchMock);

    const response = await fetchDataCompanyLogo(env, "data-0000320193");

    expect(String(fetchMock.mock.calls[0][0])).toBe(
      "https://data.example/v1/companies/0000320193/logo",
    );
    expect(response.headers.get("content-type")).toBe("image/png");
    expect(response.headers.get("etag")).toBe('"logo-sha"');
    expect(response.headers.get("x-tarashadata-logo-source")).toBe("wikimedia_logo");
    expect(Array.from(new Uint8Array(await response.arrayBuffer()))).toEqual(Array.from(logo));
  });

  it("maps the TaRaShaData.ai company search API into the Discover catalogue", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify([{
      id: 1,
      cik: "0000000001",
      name: "Alpha Systems",
      ticker: "ALPHA",
      exchange: "Nasdaq",
      country: "USA",
      industry_buckets: [{ id: 7, name: "Computer Systems" }],
      publication_status: "ingested",
      normalized_coverage: {
        available: true,
        data_access: "normalized",
        source: "SEC filings",
        first_fiscal_year: 2020,
        latest_fiscal_year: 2025,
        annual_observation_count: 120,
        metric_count: 38,
        statements: [],
      },
    }]), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    const companies = await searchDataCompanies(env, "ALPHA", "USA");

    expect(companies[0]).toMatchObject({
      id: "data-0000000001",
      cik: "0000000001",
      ticker: "ALPHA",
      industryBucket: "Computer Systems",
      data_available: 1,
      provider: "TaRaShaData.ai API",
    });
    expect(String(fetchMock.mock.calls[0][0])).toContain("/v1/companies/search?");
  });

  it("omits catalogue rows without normalized annual coverage", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify([{
      id: 1,
      cik: "0000000001",
      name: "Alpha Systems",
      ticker: "ALPHA",
      exchange: "Nasdaq",
      country: "USA",
      industry_buckets: [],
      publication_status: "ingested",
      normalized_coverage: { available: false },
    }]), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    expect(await searchDataCompanies(env, "ALPHA", "USA")).toEqual([]);
  });

  it("reports a stale TaRaShaData.ai search contract instead of silently returning no matches", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify([{
      cik: "0000000001",
      name: "Alpha Systems",
      ticker: "ALPHA",
      exchange: "Nasdaq",
      publication_status: "validated",
    }]), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    await expect(searchDataCompanies(env, "ALPHA", "USA")).rejects.toThrow(
      "TaRaShaData.ai is running an older Discover API contract",
    );
  });

  it("builds the Discover research contract only from the batched TaRaShaData.ai API", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify(discoverPayload()), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    const company = await pullDataCompany(env, "data-0000000001", 2024, 2025);

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(String(fetchMock.mock.calls[0][0])).toBe("https://data.example/v1/discover/company-dataset");
    expect(JSON.parse(String((fetchMock.mock.calls[0][1] as RequestInit).body))).toMatchObject({
      identifier: "0000000001",
      from_year: 2024,
      to_year: 2025,
    });
    expect(company.dataMode).toBe("tarasha-data");
    expect(company.metrics.revenue).toEqual([{ year: 2024, value: 800 }, { year: 2025, value: 1000 }]);
    expect(company.metrics.operatingMargin[1].value).toBe(20);
    expect(company.metrics.freeCashFlow[1].value).toBe(150);
    expect(company.metrics.netDebt[1].value).toBe(210);
    expect(company.exchange).toBe("Nasdaq");
    expect(company.logoUrl).toBe("/api/data/company-logo?companyId=data-0000000001&contract=1");
    expect(company.companyStory?.revenueSegments).toMatchObject({
      status: "available",
      latestFiscalYear: 2025,
      totalRevenue: 1000,
    });
    expect(company.companyStory?.revenueSegments.segments[0]).toMatchObject({ name: "Cloud", latestRevenue: 700, percentageOfTotal: 70 });
    expect(company.companyStory?.costStructure).toMatchObject({
      status: "available",
      latestFiscalYear: 2025,
      years: [2024, 2025],
    });
    expect(company.companyStory?.costStructure.lines.find((line) => line.key === "gross_profit")?.latestValuePerHundred).toBe(40);
    expect(company.companyStory?.cashConversion).toMatchObject({
      status: "available",
      latestFiscalYear: 2025,
      years: [2024, 2025],
    });
    expect(company.companyStory?.cashConversion.bridge.find((line) => line.key === "working_capital_impact")).toMatchObject({
      value: -5,
      components: [{ key: "change_receivables", value: -5 }],
    });
    expect(company.companyStory?.cashConversion.metrics).toMatchObject({ fcff: 151, conversionPercent: 75.5 });
    expect(company.researchShelf?.industryBucket).toBe("Computer Systems");
    expect(company.researchShelf?.industryCompanyCount).toBe(2);
    expect(company.researchShelf?.growthComparisons.revenue.industryBucket.median).toBe(17.5);
    expect(company.researchShelf?.profitability.yearly[1]).toMatchObject({ grossMargin: 40, operatingMargin: 20 });
    expect(company.researchShelf?.earningsAndValuation.earningsFlow[1].metrics.interestExpense.companyValue).toBe(10);
    expect(company.researchShelf?.earningsAndValuation.earningsFlow[1].metrics.dilutedShares.companyValue).toBe(50);
    expect(company.researchShelf?.earningsAndValuation.valuation.enterpriseValue).toBeNull();
    expect(company.filings?.[0]).toMatchObject({ form: "10-K", filed: "2026-02-15" });
    expect(company.source?.dataset).toBe("TaRaShaData.ai normalized financials API");
  });

  it("passes custom constituent identifiers to TaRaShaData.ai without the Discover prefix", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify(discoverPayload(true)), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    const company = await pullDataCompany(env, "data-0000000001", 2024, 2025, ["data-0000000002"]);
    const body = JSON.parse(String((fetchMock.mock.calls[0][1] as RequestInit).body));

    expect(body.constituent_identifiers).toEqual(["0000000002"]);
    expect(company.researchShelf?.industryConstituentsCustomized).toBe(true);
    expect(company.researchShelf?.industryConstituents.map((item) => item.id)).toEqual(["data-0000000002"]);
  });

  it("uses sample deviation for year-over-year growth", () => {
    const stats = growthStatistics([{ year: 2022, value: 100 }, { year: 2023, value: 110 }, { year: 2024, value: 132 }]);
    expect(stats.median).toBeCloseTo(15);
    expect(stats.standardDeviation).toBeCloseTo(Math.sqrt(50));
    expect(stats.totalChange).toBeCloseTo(32);
  });

  it("builds annual FCFF and FCFE bridges from normalized API inputs", () => {
    const companySeries = {
      revenue: [{ year: 2024, value: 1000 }, { year: 2025, value: 1200 }],
      ebit: [{ year: 2024, value: 200 }, { year: 2025, value: 240 }],
      effectiveTaxRate: [{ year: 2024, value: 0.25 }, { year: 2025, value: 0.25 }],
      depreciationAndAmortization: [{ year: 2024, value: 30 }, { year: 2025, value: 36 }],
      capitalExpenditure: [{ year: 2024, value: -50 }, { year: 2025, value: -60 }],
      nonCashWorkingCapital: [{ year: 2024, value: 100 }, { year: 2025, value: 85 }],
      netIncomeToCommon: [{ year: 2024, value: 130 }, { year: 2025, value: 160 }],
      shareBasedCompensation: [{ year: 2024, value: 10 }, { year: 2025, value: 12 }],
      otherAdjustments: [{ year: 2024, value: 5 }, { year: 2025, value: -4 }],
      netBorrowing: [{ year: 2024, value: -2 }, { year: 2025, value: 20 }],
      currentAssets: [{ year: 2024, value: 500 }, { year: 2025, value: 520 }],
      cashAndCashEquivalents: [{ year: 2024, value: 100 }, { year: 2025, value: 100 }],
      currentLiabilities: [{ year: 2024, value: 350 }, { year: 2025, value: 385 }],
      currentDebt: [{ year: 2024, value: 50 }, { year: 2025, value: 50 }],
    };

    const current = buildCashFlowAnalysis(companySeries, new Map(), 2024, 2025).yearly[1];

    expect(current.effectiveTaxRatePercent).toBe(25);
    expect(current.fcff.nopat.companyValue).toBe(180);
    expect(current.fcff.fcff.companyValue).toBe(171);
    expect(current.fcfe.fcfe.companyValue).toBe(179);
    expect(current.workingCapital.cashEffect).toBe("inflow");
  });

  it("calculates gross operating leverage from paired period changes", () => {
    expect(calculateGrossOperatingLeverage(130, 100, 250, 200)).toBe(60);
    expect(calculateGrossOperatingLeverage(80, 100, 160, 200)).toBe(50);
    expect(calculateGrossOperatingLeverage(120, 100, 200, 200)).toBeNull();
  });
});
