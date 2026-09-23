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
    item("accounts_receivable", "Accounts Receivable", year, 140 + index * 10),
    item("inventory", "Inventory", year, 70 + index * 5),
    item("prepaid_expenses", "Prepaid Expenses", year, 20 + index * 2),
    item("property_plant_equipment_net", "Property, Plant & Equipment", year, 250 + index * 20),
    item("goodwill", "Goodwill", year, 100),
    item("intangible_assets_net_excluding_goodwill", "Other Intangible Assets", year, 60 - index * 5),
    item("assets", "Total Assets", year, [900, 980, 1070][index]),
    item("current_liabilities", "Current Liabilities", year, 330 + index * 20),
    item("short_term_debt", "Short-Term Debt", year, 40),
    item("long_term_debt", "Long-Term Debt", year, 260 + index * 20),
    item("lease_liabilities_current", "Current Lease Liabilities", year, 10 + index),
    item("lease_liabilities_noncurrent", "Long-Term Lease Liabilities", year, 20 + index * 2),
    item("deferred_revenue_current", "Current Unearned Revenue", year, 60 + index * 5),
    item("deferred_revenue_noncurrent", "Long-Term Unearned Revenue", year, 30),
    item("accounts_payable", "Accounts Payable", year, 80 + index * 5),
    item("total_debt", "Total Debt", year, 300 + index * 20),
    item("liabilities", "Total Liabilities", year, [500, 540, 590][index]),
    item("shareholders_equity", "Shareholders' Equity", year, [400, 440, 480][index]),
    item("common_stock_value", "Common Stock", year, 5),
    item("additional_paid_in_capital", "Additional Paid-In Capital", year, 200 + index * 10),
    item("retained_earnings", "Retained Earnings", year, 205 + index * 22),
    item("accumulated_other_comprehensive_income", "Accumulated Other Comprehensive Income", year, -10 + index),
    item("short_term_debt_interest_rate", "Short-Term Debt Rate", year, .045, "ratio"),
    item("long_term_debt_interest_rate", "Long-Term Debt Rate", year, .04, "ratio"),
    item("weighted_average_cost_of_debt", "Weighted Average Cost of Debt", year, .041, "ratio"),
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

function stockRiskSeries(key: "company" | "sector" | "market", symbol: string) {
  const offset = key === "company" ? 0 : key === "sector" ? 4 : -2;
  return {
    key,
    name: key === "company" ? "Alpha Systems" : key === "sector" ? "Technology sector (XLK)" : "S&P 500",
    symbol,
    role: key === "company" ? "Company" : "Benchmark",
    source_url: `https://finance.yahoo.com/quote/${symbol}/history/`,
    start_date: "2021-08-20",
    end_date: "2026-08-20",
    trading_days: 1258,
    monthly_observations: 60,
    annualized_volatility_percent: 19.2 + offset,
    beta_to_market: key === "market" ? 1 : .87 + offset / 100,
    maximum_drawdown_percent: -27.1 - offset,
    worst_monthly_return_percent: -16.4 - offset,
    best_monthly_return_percent: 15.3 + offset,
    positive_months_percent: 60,
    negative_months_percent: 40,
    flat_months_percent: 0,
    drawdown: [{ date: "2021-08-20", value: 0 }, { date: "2022-06-20", value: -27.1 - offset }, { date: "2026-08-20", value: -2 }],
    rolling_volatility: [{ date: "2021-08-20", value: 18 + offset }, { date: "2022-06-20", value: 30 + offset }, { date: "2026-08-20", value: 19.2 + offset }],
    monthly_distribution: [
      { key: "lt_15", label: "< −15%", months: 1, percentage: 1.67 },
      { key: "neg_15_10", label: "−15% to −10%", months: 4, percentage: 6.67 },
      { key: "neg_10_5", label: "−10% to −5%", months: 9, percentage: 15 },
      { key: "neg_5_0", label: "−5% to 0%", months: 10, percentage: 16.67 },
      { key: "pos_0_5", label: "0% to 5%", months: 24, percentage: 40 },
      { key: "pos_5_10", label: "5% to 10%", months: 9, percentage: 15 },
      { key: "gt_10", label: "> 10%", months: 3, percentage: 5 },
    ],
  };
}

function stockRiskPeriod(range: "1Y" | "3Y" | "5Y" | "7Y" | "10Y") {
  return {
    range,
    start_date: "2021-08-20",
    end_date: "2026-08-20",
    series: [
      stockRiskSeries("company", "ALPHA"),
      stockRiskSeries("sector", "XLK"),
      stockRiskSeries("market", "^GSPC"),
    ],
    risk_summary: "Alpha was less volatile than its sector benchmark.",
    drawdown_summary: "Alpha's drawdown was shallower than both benchmarks.",
    positive_months_summary: "Alpha had positive returns in 60% of months.",
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
    stock_risk: {
      status: "available",
      reason: null,
      symbol: "ALPHA",
      as_of: "2026-08-20",
      available_ranges: ["1Y", "3Y", "5Y", "7Y", "10Y"],
      default_range: "5Y",
      market_benchmark: { key: "market", name: "S&P 500", symbol: "^GSPC", role: "Broad-market benchmark" },
      sector_benchmark: { key: "sector", name: "Technology sector (XLK)", symbol: "XLK", role: "Sector benchmark", selection_basis: "Mapped from SEC SIC 3571" },
      periods: {
        "1Y": stockRiskPeriod("1Y"),
        "3Y": stockRiskPeriod("3Y"),
        "5Y": stockRiskPeriod("5Y"),
        "7Y": stockRiskPeriod("7Y"),
        "10Y": stockRiskPeriod("10Y"),
      },
      source: { name: "Yahoo Finance chart service", source_role: "Zero-cost market history", delayed: true, persisted: false, cost: "$0", price_basis: "Daily adjusted close" },
      methodology: "Daily returns; volatility uses sample standard deviation × √252.",
      quality: { company_observations: 2780, market_observations: 2780, sector_observations: 2780, alignment: "Exact shared trading dates", warnings: [] },
    },
    stock_history: {
      status: "available",
      reason: null,
      symbol: "ALPHA",
      currency: "USD",
      as_of: "2026-08-20T20:00:00Z",
      market_state: "CLOSED",
      available_ranges: ["5Y", "Max"],
      default_range: "5Y",
      prices: [
        { date: "2021-08-20", close: 50, adjusted_close: 45, volume: 1_000_000 },
        { date: "2026-08-20", close: 100, adjusted_close: 100, volume: 1_500_000 },
      ],
      fundamentals: {
        revenue: { label: "Revenue", points: [{ date: "2025-12-31", fiscal_year: 2025, value: 1000, source_url: "https://www.sec.gov/example", unit: "USD millions" }] },
        eps: { label: "Earnings Per Share", points: [{ date: "2025-12-31", fiscal_year: 2025, value: 3, source_url: "https://www.sec.gov/example", unit: "USD per share" }] },
        free_cash_flow: { label: "Free Cash Flow", points: [{ date: "2025-12-31", fiscal_year: 2025, value: 150, source_url: "https://www.sec.gov/example", unit: "USD millions" }] },
      },
      events: [{ date: "2026-02-15", type: "earnings", title: "Annual results filed", detail: "10-K", source_url: "https://www.sec.gov/example" }],
      performance: {
        "5Y": { range: "5Y", start_date: "2021-08-20", end_date: "2026-08-20", price_change_percent: 100, total_return_percent: 122.22, cagr_percent: 14.87, sp500_total_return_percent: 70, high: 100, low: 50 },
      },
      glance: { current_price: 100, market_cap: 5000, diluted_shares: 50, average_daily_volume_3m: 1_400_000 },
      strategy: { status: "available", reason: null, points: ["We will continue to invest in product development."], source_name: "Alpha management commentary", source_date: "2026-02-15", source_url: "https://www.sec.gov/example", cost: "$0", methodology: "Source-verbatim management statements." },
      source: { financials: "TaRaShaData normalized issuer filings", filings: "TaRaShaData stored SEC filing metadata", market_name: "Yahoo Finance chart service", market_role: "Delayed display", market_url: "https://finance.yahoo.com/quote/ALPHA/history/", benchmark_url: "https://finance.yahoo.com/quote/%5EGSPC/history/", cost: "$0", persisted: false, price_basis: "Price uses close; total return uses adjusted close." },
      methodology: "FCF equals operating cash flow less capital expenditure.",
      quality: { warnings: [], raw_price_observations: 1258, display_price_observations: 700, fundamental_years: [2025] },
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
    expect(company.companyStory?.balanceSheet).toMatchObject({
      status: "available",
      latestFiscalYear: 2025,
      equation: { assets: 1070, liabilities: 590, shareholdersEquity: 480 },
      health: { totalCash: 130, totalDebt: 340, netCashDebt: -210 },
    });
    expect(company.companyStory?.balanceSheet.health.weightedAverageCostOfDebt).toBeCloseTo(4.1);
    expect(company.companyStory?.balanceSheet.assets.find((component) => component.key === "cash")?.trend).toHaveLength(2);
    expect(company.companyStory?.balanceSheet.liabilities.find((component) => component.key === "borrowings")?.children).toEqual(expect.arrayContaining([
      expect.objectContaining({ key: "short_term_debt", value: 40, interestRatePercent: 4.5 }),
      expect.objectContaining({ key: "long_term_debt", value: 300, interestRatePercent: 4 }),
    ]));
    expect(company.companyStory?.stockRisk).toMatchObject({
      status: "available",
      symbol: "ALPHA",
      defaultRange: "5Y",
      availableRanges: ["1Y", "3Y", "5Y", "10Y"],
      source: { name: "Yahoo Finance chart service", cost: "$0", persisted: false },
      sectorBenchmark: { symbol: "XLK", selectionBasis: "Mapped from SEC SIC 3571" },
    });
    expect(company.companyStory?.stockRisk?.periods["5Y"]?.series[0]).toMatchObject({
      symbol: "ALPHA",
      annualizedVolatilityPercent: 19.2,
      maximumDrawdownPercent: -27.1,
      positiveMonthsPercent: 60,
    });
    expect(company.companyStory?.stockRisk?.periods["7Y"]?.series[0]).toMatchObject({
      symbol: "ALPHA",
      monthlyObservations: 60,
    });
    expect(company.companyStory?.stockHistory).toMatchObject({
      status: "available",
      symbol: "ALPHA",
      defaultRange: "5Y",
      availableRanges: ["5Y", "Max"],
      glance: { currentPrice: 100, marketCap: 5000, dilutedShares: 50 },
      strategy: { status: "available", points: ["We will continue to invest in product development."] },
    });
    expect(company.companyStory?.stockHistory?.fundamentals.freeCashFlow.points[0]).toMatchObject({ value: 150, fiscalYear: 2025 });
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

  it("calculates weighted average cost of debt from interest expense and average borrowings", async () => {
    const payload = discoverPayload(true);
    payload.datasets[0].balance.items = payload.datasets[0].balance.items.filter(
      (fact) => !["weighted_average_cost_of_debt", "long_term_debt_interest_rate"].includes(fact.metric),
    );
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify(payload), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    const company = await pullDataCompany(env, "data-0000000001", 2024, 2025);

    expect(company.companyStory?.balanceSheet.health.weightedAverageCostOfDebt).toBeCloseTo(10 / 330 * 100);
    expect(company.companyStory?.balanceSheet.health.weightedAverageCostOfDebtFiscalYear).toBe(2025);
    expect(company.companyStory?.balanceSheet.health.weightedAverageCostOfDebtNote).toBe(
      "FY2025 interest expense ÷ average borrowings",
    );
  });

  it("uses the disclosed lease discount rate when funding is lease-only", async () => {
    const payload = discoverPayload(true);
    payload.datasets[0].income.items = payload.datasets[0].income.items.filter(
      (fact) => fact.metric !== "interest_expense",
    );
    payload.datasets[0].balance.items = payload.datasets[0].balance.items.filter(
      (fact) => ![
        "weighted_average_cost_of_debt",
        "short_term_debt_interest_rate",
        "long_term_debt_interest_rate",
        "short_term_debt",
        "long_term_debt",
        "total_debt",
      ].includes(fact.metric),
    );
    payload.datasets[0].balance.items.push(
      item("operating_lease_weighted_average_discount_rate", "Operating Lease Discount Rate", 2025, .052, "ratio"),
    );
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify(payload), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    const company = await pullDataCompany(env, "data-0000000001", 2024, 2025);

    expect(company.companyStory?.balanceSheet.health.weightedAverageCostOfDebt).toBeCloseTo(5.2);
    expect(company.companyStory?.balanceSheet.health.weightedAverageCostOfDebtNote).toBe(
      "Lease-only funding · FY2025 disclosed discount rate",
    );
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
