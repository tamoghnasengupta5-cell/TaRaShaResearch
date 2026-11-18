import { afterEach, describe, expect, it, vi } from "vitest";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { CompanyStoryHome } from "./CompanyStoryHome";
import { StatementExplorer } from "./FinancialStatements";
import {
  buildCashFlowAnalysis,
  calculateGrossOperatingLeverage,
  fetchDataCompanyLogo,
  growthStatistics,
  pullDataCompany,
  searchDataCompanies,
} from "../functions/dataProvider";

const env = { TARASHA_DATA_API_URL: "https://data.example" };

function item(metric: string, label: string, year: number, value: number, unit = "USD millions"): {
  metric: string; metric_label: string; display: { value: string; unit: string };
  period: { type: string; start?: string; end: string; reporting_currency?: string };
  provenance?: { source_url?: string; derivation_method?: string; formula?: string;
    sec_cash_da_other_scope?: Record<string, unknown>; presentation_role?: string };
} {
  return {
    metric,
    metric_label: label,
    display: { value: String(value), unit },
    period: { type: "annual", start: `${year}-01-01`, end: `${year}-12-31` },
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
    item("free_cash_flow", "Free Cash Flow", year, 130 + index * 20),
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
      reporting_basis: "consolidated" as const,
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

  it("preserves the API annual display window, TTM values and exact worksheet dates", async () => {
    const base = discoverPayload();
    const cash = { ...base.cash_conversion, latest_statement_fiscal_year: 2025,
      display_window: { annual_years: [2019,2020,2021,2022,2023,2024,2025] },
      ttm: { status: "available", reason: null, period_start: "2025-07-01", period_end: "2026-06-30", missing_inputs: [],
        bridge: base.cash_conversion.bridge.map(line => ({ ...line, value: "456.78", disclosure: "Filed broader add-back" })),
        metrics: { fcff: "456.78", conversion_percent: "44.5", revenue_percent: "12.3" },
        working_capital_calculation: { status: "available", reason: null, formula: "Server formula", period_type: "ttm",
          prior_period_label: "As of 2025-06-30", current_period_label: "As of 2026-06-30", prior_fiscal_year: 2025, current_fiscal_year: 2026,
          prior_period_end: "2025-06-30", current_period_end: "2026-06-30", prior_ncwc: "100", current_ncwc: "90", cash_effect: "10",
          prior_operating_current_assets: "200", current_operating_current_assets: "210", inputs: [] } } };
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify({ ...base, cash_conversion: cash }), { status: 200 })));
    const company = await pullDataCompany(env, "data-0000000001", 2024, 2025);
    const story = company.companyStory!.cashConversion!;
    expect(story.displayAnnualYears).toEqual([2019,2020,2021,2022,2023,2024,2025]);
    expect(story.metrics!.fcff).toBe(151);
    expect(story.ttm!.metrics!.fcff).toBe(456.78);
    expect(story.ttm!.bridge[0].disclosure).toBe("Filed broader add-back");
    expect(story.ttm!.workingCapitalCalculation).toMatchObject({ periodType: "ttm", priorPeriodLabel: "As of 2025-06-30", cashEffect: 10 });
    expect(story.ttm!.metrics!.priorFcff).toBeNull();
  });

  it("preserves null partial-bridge values without converting gaps to zero or calculating FCFF", async () => {
    const base = discoverPayload();
    const source = base.cash_conversion.bridge[0];
    const cash = { ...base.cash_conversion, status: "unavailable", bridge: null, trend: [], metrics: null,
      partial_bridge: { fiscal_year: 2025, lines: [
        { ...source, key: "ebit", value: "123.45" },
        { ...source, key: "capital_expenditure", value: null },
        { ...source, key: "fcff", value: null },
      ] } };
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify({ ...base, cash_conversion: cash }), { status: 200 })));
    const company = await pullDataCompany(env, "data-0000000001", 2024, 2025);
    const story = company.companyStory!.cashConversion!;
    expect(story.partialBridge?.lines.map(line => line.value)).toEqual([123.45, null, null]);
    expect(story.metrics).toBeNull();
    expect(story.bridge).toEqual([]);
  });

  it("preserves the API NCWC worksheet and both-year constituent values without recalculation", async () => {
    const base = discoverPayload();
    const worksheet = {
      status: "available", cash_input_policy: "cash_and_cash_equivalents_plus_current_investments", reason: null, formula: "Approved API formula",
      prior_fiscal_year: 2024, current_fiscal_year: 2025,
      prior_period_end: "2024-12-31", current_period_end: "2025-12-31",
      prior_ncwc: "220.25", current_ncwc: "210.50", cash_effect: "9.75",
      prior_operating_current_assets: "400.00", current_operating_current_assets: "480.00",
      inputs: [{ key: "cash_input", label: "Cash Input", operator: "−", prior_value: "100.00", current_value: "120.00",
        prior_detail_status: "complete", current_detail_status: "complete", prior_note: null, current_note: null,
        components: [{ key: "cash", label: "Cash and equivalents", prior_value: "100.00", current_value: "120.00",
          prior_source_url: "https://www.sec.gov/2024.xml", current_source_url: "https://www.sec.gov/2025.xml" }] }],
    };
    const cash = { ...base.cash_conversion, earnings_basis: "operating_income_plus_disclosed_interest_investment_income", working_capital_calculation: worksheet,
      bridge: base.cash_conversion.bridge.map(line => ({ ...line, disclosure: "SEC operating income plus disclosed interest/investment income.", working_capital_calculation: worksheet })),
      trend: base.cash_conversion.trend.map(point => ({ ...point, working_capital_calculation: worksheet })) };
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify({ ...base, cash_conversion: cash }), { status: 200 })));
    const company = await pullDataCompany(env, "data-0000000001", 2024, 2025);
    const story = company.companyStory!.cashConversion!;
    expect(story.earningsBasis).toBe("operating_income_plus_disclosed_interest_investment_income");
    expect(story.bridge[0].disclosure).toBe("SEC operating income plus disclosed interest/investment income.");
    expect(story.workingCapitalCalculation).toMatchObject({ cashInputPolicy: "cash_and_cash_equivalents_plus_current_investments", cashEffect: 9.75, priorNcwc: 220.25, currentNcwc: 210.5,
      inputs: [{ key: "cash_input", priorValue: 100, currentValue: 120,
        components: [{ priorValue: 100, currentValue: 120, currentSourceUrl: "https://www.sec.gov/2025.xml" }] }] });
    expect(story.bridge[0].workingCapitalCalculation).toEqual(story.workingCapitalCalculation);
    expect(story.trend[0].workingCapitalCalculation).toEqual(story.workingCapitalCalculation);
    expect(story.metrics!.fcff).toBe(151); // Preserve the server's FCFF; never recompute it from browser balances.
  });

  it("preserves an Indian issuer ID, INR and API chapter/comparability limits", async () => {
    const payload = discoverPayload();
    const company = {
      ...payload.company, cik: null, identifier: "IN:L31300GJ1996PLC114183",
      country: "India", reporting_currency: "INR", supported_story_chapters: [1, 2, 3, 4],
      reporting_basis: "standalone",
      aliases: [{ ticker: "POLYCAB", exchange: "NSE", is_current: true }],
      debt_cost_policy: { allow_interest_to_average_borrowings: false, reason: "Incompatible interest and borrowing scope" },
    };
    const target = payload.datasets.find((row) => row.company.id === payload.company.id)!;
    const indianTarget = { ...target, company, balance: { ...target.balance,
      items: target.balance.items.filter((row) => !row.metric.includes("interest_rate") && row.metric !== "weighted_average_cost_of_debt"),
    }};
    const body = { ...payload, company, country: "India", datasets: [indianTarget] };
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify(body), {status: 200}));
    vi.stubGlobal("fetch", fetchMock);
    const result = await pullDataCompany(env, "data-" + company.identifier, 2024, 2025);
    expect(result.id).toBe("data-IN:L31300GJ1996PLC114183");
    expect(result.country).toBe("India");
    expect(result.reportingBasis).toBe("standalone");
    expect(result.companyStory?.balanceSheet.reportingCurrency).toBe("INR");
    expect(result.supportedStoryChapters).toEqual([1, 2, 3, 4]);
    expect(result.companyStory?.balanceSheet.health.weightedAverageCostOfDebt).toBeNull();
    expect(result.companyStory?.balanceSheet.health.weightedAverageCostOfDebtNote).toBe("Incompatible interest and borrowing scope");
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(String(fetchMock.mock.calls[0][0])).toBe("https://data.example/v1/discover/company-dataset");
    expect(JSON.parse(fetchMock.mock.calls[0][1].body).from_year).toBe(1900);
  });

  it("keeps all available Indian issuer history and resolves annual basis labels from actual December period ends", async () => {
    const payload = discoverPayload();
    const company = { ...payload.company, cik: null, identifier: "IN:ISIN:INE000000001", country: "India", reporting_currency: "INR",
      reporting_basis: "standalone", aliases: [{ ticker: "EXAMPLE", exchange: "NSE", is_current: true }],
      reporting_basis_selection: { periods: { "2024-12-31": { reporting_basis: "consolidated" }, "2025-12-31": { reporting_basis: "standalone" } } },
    };
    const target = payload.datasets.find(row => row.company.id === payload.company.id)!;
    const dataset = { ...target, company, income: { ...target.income, items: [item("revenue", "Revenue", 2001, 300, "INR millions"), ...target.income.items] } };
    const body = { ...payload, company, country: "India", datasets: [dataset],
      revenue_offerings: { annual_totals: { "2024": { period_end: "2024-12-31", revenue_display: { value: "800", unit: "INR millions" } },
        "2025": { period_end: "2025-12-31", revenue_display: { value: "1000", unit: "INR millions" } } }, offerings: [], ttm: null },
    };
    const fetchMock = vi.fn().mockResolvedValue(Response.json(body));
    vi.stubGlobal("fetch", fetchMock);
    const result = await pullDataCompany(env, "data-" + company.identifier, 2024, 2025);
    expect(result.metrics.revenue).toContainEqual({ year: 2001, value: 300 });
    expect(result.reportingBasisByYear).toEqual({ "2024": "consolidated", "2025": "standalone" });
    expect(result.researchShelf.fromYear).toBe(2024);
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toMatchObject({ identifier: company.identifier, from_year: 1900, to_year: 2025 });
  });

  it.each(["NSE:EXAMPLE", "BSE:500000"])("requests all annual history for an explicit Indian exchange alias %s", async identifier => {
    const payload = discoverPayload();
    const fetchMock = vi.fn().mockResolvedValue(Response.json(payload));
    vi.stubGlobal("fetch", fetchMock);
    await pullDataCompany(env, "data-" + identifier, 2020, 2025);
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toMatchObject({ identifier, from_year: 1900, to_year: 2025 });
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

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
    expect(new URL(String(fetchMock.mock.calls[0][0])).searchParams.get("audience")).toBe("discover");
    expect(fetchMock.mock.calls[0][1].cache).toBe("no-store");
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

  it("declares percentage margins separately from plain financial ratios", async () => {
    const payload = discoverPayload();
    const margin = { ...item("gross_margin", "Gross Margin", 2025, -3.94, "ratio"), value: "-3.93891", unit: "pure" };
    const currentRatio = { ...item("current_ratio", "Current Ratio", 2025, 3.91, "ratio"), value: "3.91", unit: "pure" };
    payload.datasets[0].income.items.push(margin);
    payload.datasets[0].balance.items.push(currentRatio);
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(payload)));
    const result = await pullDataCompany(env, "data-0000000001", 2024, 2025);
    const facts = result.statements!.flatMap(group => group.facts);
    expect(facts.find(fact => fact.key === "gross_margin")).toMatchObject({ unit: "ratio", displayFormat: "percentage", values: [{ year: 2025, value: -3.93891 }] });
    expect(facts.find(fact => fact.key === "current_ratio")).toMatchObject({ unit: "ratio", displayFormat: "number", values: [{ year: 2025, value: 3.91 }] });
  });

  it("retains declared pure ratio precision above and below one in statements and tax calculations", async () => {
    const payload = discoverPayload();
    payload.datasets[0].income.items = payload.datasets[0].income.items.map(fact => fact.metric === "effective_tax_rate"
      ? { ...fact, value: fact.period.end.startsWith("2024") ? "2.571" : "-3.93891", unit: "pure",
          display: { value: fact.period.end.startsWith("2024") ? "2.57" : "-3.94", unit: "ratio" } } : fact);
    payload.datasets[0].balance.items = payload.datasets[0].balance.items.map(fact => fact.metric === "weighted_average_cost_of_debt"
      ? { ...fact, display: { value: "2", unit: "ratio" } } : fact);
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(payload)));
    const result = await pullDataCompany(env, "data-0000000001", 2024, 2025);
    const tax = result.statements!.flatMap(group => group.facts).find(fact => fact.key === "effectiveTaxRate")!;
    expect(tax.unit).toBe("ratio");
    expect(tax.values).toEqual([{ year: 2024, value: 2.571 }, { year: 2025, value: -3.93891 }]);
    const current = result.researchShelf!.cashFlow.yearly.find(point => point.year === 2025)!;
    expect(current.effectiveTaxRatePercent).toBeCloseTo(-393.891);
    expect(current.fcff.nopat.companyValue).toBeCloseTo(200 * (1 + 3.93891));
    expect(result.companyStory!.balanceSheet.health.weightedAverageCostOfDebt).toBe(200);
  });

  it("converts an explicitly published small percentage once for tax calculation", async () => {
    const payload = discoverPayload();
    payload.datasets[0].income.items = payload.datasets[0].income.items.map(fact => fact.metric === "effective_tax_rate"
      ? { ...fact, display: { value: "0.5", unit: "%" } } : fact);
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(payload)));
    const result = await pullDataCompany(env, "data-0000000001", 2024, 2025);
    const tax = result.statements!.flatMap(group => group.facts).find(fact => fact.key === "effectiveTaxRate")!;
    expect(tax.unit).toBe("ratio");
    expect(tax.values[1].value).toBe(.005);
    const current = result.researchShelf!.cashFlow.yearly.find(point => point.year === 2025)!;
    expect(current.effectiveTaxRatePercent).toBe(.5);
    expect(current.fcff.nopat.companyValue).toBe(199);
  });

  it("preserves reported zero costs, capital spending and equity in subtraction results", async () => {
    const payload = discoverPayload();
    const target = payload.datasets[0];
    target.income.items = target.income.items.map((fact) =>
      fact.period.end === "2025-12-31" && fact.metric === "cost_of_revenue"
        ? { ...fact, display: { ...fact.display, value: "0" } }
        : fact,
    );
    target.cash_flow.items = target.cash_flow.items.map((fact) =>
      fact.period.end === "2025-12-31" && ["capital_expenditures", "free_cash_flow"].includes(fact.metric)
        ? { ...fact, display: { ...fact.display, value: fact.metric === "free_cash_flow" ? "200" : "0" } }
        : fact,
    );
    target.balance.items = target.balance.items
      .filter((fact) => fact.period.end !== "2025-12-31" || fact.metric !== "liabilities")
      .map((fact) => fact.period.end === "2025-12-31" && ["assets", "shareholders_equity"].includes(fact.metric)
        ? { ...fact, display: { ...fact.display, value: fact.metric === "assets" ? "100" : "0" } }
        : fact);
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(payload)));

    const company = await pullDataCompany(env, "data-0000000001", 2024, 2025);

    expect.soft(company.researchShelf?.rawIncome.find((point) => point.year === 2025)?.grossProfit).toBe(1000);
    expect.soft(company.metrics.freeCashFlow).toContainEqual({ year: 2025, value: 200 });
    expect.soft(company.companyStory?.balanceSheet).toMatchObject({
      status: "available",
      latestFiscalYear: 2025,
      equation: { assets: 100, liabilities: 100, shareholdersEquity: 0 },
    });
  });

  it("withholds division results when reported revenue is zero", async () => {
    const payload = discoverPayload();
    for (const target of payload.datasets) {
      target.income.items = target.income.items.map((fact) =>
        fact.period.end === "2025-12-31" && fact.metric === "revenue"
          ? { ...fact, display: { ...fact.display, value: "0" } }
          : fact,
      );
    }
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(payload)));

    const company = await pullDataCompany(env, "data-0000000001", 2024, 2025);

    expect(company.metrics.revenue).toContainEqual({ year: 2025, value: 0 });
    expect(company.metrics.operatingMargin.some((point) => point.year === 2025)).toBe(false);
    const profitability = company.researchShelf?.profitability.yearly.find((point) => point.year === 2025);
    expect(profitability).toMatchObject({ grossMargin: null, operatingMargin: null, cogsRatio: null });
    const comparison = company.researchShelf?.profitability.industryComparisons.operatingMargin
      .find((point) => point.year === 2025);
    expect(comparison).toMatchObject({ companyValue: null, industryMedian: null });
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
      item("short_term_debt", "Reported current borrowings", 2025, 0),
      item("long_term_debt", "Reported long-term borrowings", 2025, 0),
      item("total_debt", "Verified complete lease-only debt", 2025, 36),
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

  it("does not derive an Indian cost rate from frontend lease or borrowing components", async () => {
    for (const borrowingCategories of [[], ["short_term_debt"], ["short_term_debt", "long_term_debt"]]) {
      const payload = discoverPayload(true);
      const company = { ...payload.company, cik: null, identifier: "IN:ISIN:INE000000001", country: "India", reporting_currency: "INR",
        aliases: [{ ticker: "EXAMPLE", exchange: "NSE", is_current: true }] };
      const target = payload.datasets[0];
      const income = { ...target.income, items: target.income.items.filter((fact) => fact.metric !== "interest_expense") };
      const balance = { ...target.balance, items: target.balance.items.filter((fact) => ![
        "weighted_average_cost_of_debt", "short_term_debt_interest_rate", "long_term_debt_interest_rate",
        "short_term_debt", "long_term_debt", "total_debt",
      ].includes(fact.metric)) };
      balance.items.push(item("total_debt", "Issuer-reported total debt", 2025, 36),
        item("operating_lease_weighted_average_discount_rate", "Operating Lease Discount Rate", 2025, .052, "ratio"),
        ...borrowingCategories.map((metric) => item(metric, "Reported borrowing category", 2025, 0)));
      const body = { ...payload, company, country: "India", datasets: [{ ...target, company, income, balance }] };
      vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(body)));
      const result = await pullDataCompany(env, "data-" + company.identifier, 2024, 2025);
      expect(result.companyStory?.balanceSheet.health.weightedAverageCostOfDebt).toBeNull();
      expect(result.companyStory?.balanceSheet.health.weightedAverageCostOfDebtNote).not.toContain("Lease-only funding");
    }
  });

  it.each([.005, 2])("retains the India API decimal cost-rate unit at %s", async (rate) => {
    const payload = discoverPayload(true);
    const company = { ...payload.company, cik: null, identifier: "IN:ISIN:INE000000001", country: "India", reporting_currency: "INR",
      aliases: [{ ticker: "EXAMPLE", exchange: "NSE", is_current: true }] };
    const target = payload.datasets[0];
    const balance = { ...target.balance, items: target.balance.items.filter(fact => fact.metric !== "weighted_average_cost_of_debt") };
    balance.items.push({ ...item("weighted_average_cost_of_debt", "Weighted Average Cost of Debt", 2025, rate, "ratio"),
      provenance: { source_url: "https://issuer.example/annual.pdf", derivation_method: "india_scoped_interest_to_average_borrowings_v1",
        formula: "Verified annual borrowing interest / average verified borrowings" } });
    const body = { ...payload, company, country: "India", datasets: [{ ...target, company, balance }] };
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(body)));
    const result = await pullDataCompany(env, "data-" + company.identifier, 2024, 2025);
    expect(result.companyStory?.balanceSheet.health.weightedAverageCostOfDebt).toBeCloseTo(rate * 100);
    expect(result.companyStory?.balanceSheet.health.weightedAverageCostOfDebtNote).toBe("FY2025 verified borrowing interest ÷ average borrowings");
    expect(result.companyStory?.balanceSheet.health.weightedAverageCostOfDebtLineage).toEqual({ unit: "ratio",
      derivationMethod: "india_scoped_interest_to_average_borrowings_v1", formula: "Verified annual borrowing interest / average verified borrowings",
      sourceUrl: "https://issuer.example/annual.pdf" });
  });

  it("retains each annual API currency and withholds an ambiguous same-year currency", async () => {
    for (const ambiguous of [false, true]) {
      const payload = discoverPayload(true);
      const company = { ...payload.company, cik: null, identifier: "IN:ISIN:INE000000001", country: "India", reporting_currency: "INR",
        aliases: [{ ticker: "EXAMPLE", exchange: "NSE", is_current: true }] };
      const target = payload.datasets[0];
      for (const financials of [target.income, target.balance, target.cash_flow]) {
        financials.items = financials.items.map(fact => {
          const currency = fact.period.end.startsWith("2024") ? "USD" : "INR";
          return fact.display.unit.includes("USD") ? { ...fact,
            display: { ...fact.display, unit: fact.display.unit.replace("USD", currency) },
            period: { ...fact.period, reporting_currency: currency } } : fact;
        });
      }
      if (ambiguous) target.income.items.push(item("revenue", "Conflicting statement currency", 2025, 12, "USD millions"));
      const body = { ...payload, company, country: "India", datasets: [{ ...target, company,
        annual_reporting_currencies: { "2024-12-31": { reporting_currency: "USD", status: "available" },
          "2025-12-31": { reporting_currency: "INR", status: "available" } } }] };
      vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(body)));
      const result = await pullDataCompany(env, "data-" + company.identifier, 2024, 2025);
      expect(result.annualCurrencyByYear).toMatchObject({ "2024": "USD", "2025": ambiguous ? null : "INR" });
      expect(result.metrics.revenue.some(point => point.year === 2025)).toBe(!ambiguous);
      expect(result.companyStory?.balanceSheet.latestFiscalYear).toBe(ambiguous ? 2024 : 2025);
    }
  });

  it("shows canonical Indian intangible assets other than goodwill without merging goodwill", async () => {
    const payload = discoverPayload(true);
    const company = { ...payload.company, cik: null, identifier: "IN:ISIN:INE000000001", country: "India", reporting_currency: "INR",
      aliases: [{ ticker: "EXAMPLE", exchange: "NSE", is_current: true }] };
    const target = payload.datasets[0];
    const balance = { ...target.balance, items: target.balance.items.map(fact => fact.metric === "intangible_assets_net_excluding_goodwill"
      ? { ...fact, metric: "other_intangible_assets" } : fact) };
    const body = { ...payload, company, country: "India", datasets: [{ ...target, company, balance }] };
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(body)));
    const result = await pullDataCompany(env, "data-" + company.identifier, 2024, 2025);
    const assets = result.companyStory?.balanceSheet.assets;
    expect(assets?.find(component => component.key === "other_intangible_assets")?.value).toBe(50);
    expect(assets?.find(component => component.key === "other_intangible_assets")?.trend.map(point => ({ fiscalYear: point.fiscalYear, value: point.value })))
      .toEqual([{ fiscalYear: 2023, value: 60 }, { fiscalYear: 2024, value: 55 }, { fiscalYear: 2025, value: 50 }]);
    expect(assets?.find(component => component.key === "goodwill")?.value).toBe(100);
  });

  it.each(["equity", "assets", "all"])("retains verified partial Indian balance values when %s is unavailable", async (missing) => {
    const payload = discoverPayload(true);
    const company = { ...payload.company, cik: null, identifier: "IN:ISIN:INE000000001", country: "India", reporting_currency: "INR", reporting_basis: "consolidated",
      aliases: [{ ticker: "EXAMPLE", exchange: "NSE", is_current: true }] };
    const target = payload.datasets[0];
    const supported = new Set(["assets", "cash", "short_term_investments", "intangible_assets_net_excluding_goodwill"]);
    for (const financials of [target.income, target.cash_flow]) financials.items = financials.items.map(fact => ({ ...fact,
      display: { ...fact.display, unit: fact.display.unit.replace("USD", "INR") },
      period: { ...fact.period, reporting_basis: "consolidated", reporting_currency: "INR" } }));
    const balance = { ...target.balance, items: target.balance.items.filter(fact => missing !== "all" && supported.has(fact.metric) && !(missing === "assets" && fact.metric === "assets"))
      .map(fact => ({ ...fact, metric: fact.metric === "intangible_assets_net_excluding_goodwill" ? "other_intangible_assets" : fact.metric,
        display: { ...fact.display, unit: "INR millions" }, period: { ...fact.period, reporting_basis: "consolidated", reporting_currency: "INR" } })) };
    const body = { ...payload, company, country: "India", datasets: [{ ...target, company, balance }] };
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(body)));
    const result = await pullDataCompany(env, "data-" + company.identifier, 2024, 2025);
    const story = result.companyStory?.balanceSheet;
    expect(story?.accountingEquationAvailable).toBe(false);
    expect(story?.status).toBe(missing === "all" ? "unavailable" : "available");
    const intangible = story?.assets.find(component => component.key === "other_intangible_assets");
    expect(intangible?.value).toBe(missing === "all" ? null : 50);
    expect(intangible?.percentageOfBase).toBe(missing === "equity" ? 50 / 1070 * 100 : null);
    const cash = story?.assets.find(component => component.key === "cash");
    expect(cash?.value).toBeNull();
    expect(cash?.children.find(child => child.key === "cash_and_equivalents")?.value).toBe(missing === "all" ? null : 110);
    expect(story?.equation.shareholdersEquity).toBeNull();
    expect(story?.health.totalDebt).toBeNull();
    expect(story?.health.netCashDebt).toBeNull();
  });

  it("does not combine partial or incompatible Indian cash components into a total", async () => {
    for (const caseName of ["missing_investments", "different_basis", "different_period", "explicit_zero"] as const) {
      const payload = discoverPayload(true);
      const company = { ...payload.company, cik: null, identifier: "IN:ISIN:INE000000001", country: "India", reporting_currency: "INR", reporting_basis: "consolidated",
        aliases: [{ ticker: "EXAMPLE", exchange: "NSE", is_current: true }] };
      const target = payload.datasets[0];
      for (const financials of [target.income, target.cash_flow]) financials.items = financials.items.map(fact => ({ ...fact,
        display: { ...fact.display, unit: fact.display.unit.replace("USD", "INR") },
        period: { ...fact.period, reporting_basis: "consolidated", reporting_currency: "INR" } }));
      const balance = { ...target.balance, items: target.balance.items.filter(fact => ["assets", "cash", "short_term_investments"].includes(fact.metric) && !(caseName === "missing_investments" && fact.metric === "short_term_investments"))
        .map(fact => ({ ...fact, display: { ...fact.display, value: caseName === "explicit_zero" && fact.metric === "short_term_investments" ? "0" : fact.display.value, unit: "INR millions" },
          period: { ...fact.period, end: caseName === "different_period" && fact.metric === "short_term_investments" ? fact.period.end.replace("12-31", "03-31") : fact.period.end,
            reporting_basis: caseName === "different_basis" && fact.metric === "short_term_investments" ? "standalone" : "consolidated", reporting_currency: "INR" } })) };
      const body = { ...payload, company, country: "India", datasets: [{ ...target, company, balance }] };
      vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(body)));
      const result = await pullDataCompany(env, "data-" + company.identifier, 2024, 2025);
      const cash = result.companyStory?.balanceSheet.assets.find(component => component.key === "cash");
      expect(cash?.value).toBeNull();
      expect(cash?.children.find(child => child.key === "cash_and_equivalents")?.value).toBe(110);
      if (caseName === "explicit_zero") expect(cash?.children.find(child => child.key === "short_term_investments")?.value).toBe(0);
    }
  });

  it("uses the canonical India API cash aggregate without requiring separately published children", async () => {
    const payload = discoverPayload(true);
    const company = { ...payload.company, cik: null, identifier: "IN:ISIN:INE000000001", country: "India", reporting_currency: "INR", reporting_basis: "consolidated",
      aliases: [{ ticker: "EXAMPLE", exchange: "NSE", is_current: true }] };
    const target = payload.datasets[0];
    for (const financials of [target.income, target.cash_flow]) financials.items = financials.items.map(fact => ({ ...fact,
      display: { ...fact.display, unit: fact.display.unit.replace("USD", "INR") },
      period: { ...fact.period, reporting_basis: "consolidated", reporting_currency: "INR" } }));
    const balance = { ...target.balance, items: [item("assets", "Assets", 2025, 1000, "INR millions"),
      item("cash_and_short_term_investments", "Verified complete cash aggregate", 2025, 130, "INR millions")]
      .map(fact => ({ ...fact, period: { ...fact.period, reporting_basis: "consolidated", reporting_currency: "INR" } })) };
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json({ ...payload, company, country: "India", datasets: [{ ...target, company, balance }] })));
    const result = await pullDataCompany(env, "data-" + company.identifier, 2024, 2025);
    const cash = result.companyStory?.balanceSheet.assets.find(component => component.key === "cash");
    expect(cash?.value).toBe(130);
    expect(cash?.percentageOfBase).toBe(13);
    expect(cash?.children.every(child => child.value === null)).toBe(true);
  });

  it("leaves Indian total debt and debt-dependent ratios unavailable without a canonical API total", async () => {
    const payload = discoverPayload(true);
    const company = { ...payload.company, cik: null, identifier: "IN:ISIN:INE000000001", country: "India", reporting_currency: "INR",
      aliases: [{ ticker: "EXAMPLE", exchange: "NSE", is_current: true }] };
    const target = payload.datasets[0];
    const balance = { ...target.balance, items: target.balance.items.filter((fact) => fact.metric !== "total_debt") };
    const body = { ...payload, company, country: "India", datasets: [{ ...target, company, balance }] };
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(body)));
    const result = await pullDataCompany(env, "data-" + company.identifier, 2024, 2025);
    const health = result.companyStory?.balanceSheet.health;
    expect(health?.totalDebt).toBeNull();
    expect(health?.netCashDebt).toBeNull();
    expect(health?.netDebtToEbitda).toBeNull();
    // Verified child disclosures remain available without inventing a total.
    const borrowing = result.companyStory?.balanceSheet.liabilities.find((component) => component.key === "borrowings");
    expect(borrowing?.value).toBeNull();
    expect(borrowing?.children.some((child) => child.value !== null)).toBe(true);
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


it("preserves Data-only omitted-year reasons and source links beside a supported cash bridge", async () => {
  const base = discoverPayload();
  const gap = { fiscal_year: 2026, period_end: "2026-03-31", reporting_basis: "consolidated",
    reporting_currency: "INR", status: "unavailable", reason_code: "incomplete_fcff_inputs",
    reason: "Required annual FCFF inputs are unavailable.", missing_inputs: ["change_operating_working_capital"],
    source_urls: ["https://issuer.test/2026.pdf"] };
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify({ ...base,
    cash_conversion: { ...base.cash_conversion, period_gaps: [gap] } }), { status: 200 })));
  const company = await pullDataCompany(env, "data-0000000001", 2024, 2026);
  const cashStory = company.companyStory?.cashConversion;
  expect(cashStory && "periodGaps" in cashStory ? cashStory.periodGaps : undefined).toEqual([{ fiscalYear: 2026,
    periodEnd: "2026-03-31", reportingBasis: "consolidated", reportingCurrency: "INR",
    status: "unavailable", reasonCode: "incomplete_fcff_inputs", reason: gap.reason,
    missingInputs: ["change_operating_working_capital"], sourceUrls: gap.source_urls }]);
  expect(company.companyStory?.cashConversion.latestFiscalYear).toBe(2025);
  expect(company.companyStory?.cashConversion.metrics?.fcff).toBe(151);
});


describe("India authoritative current-asset category coverage", () => {
  afterEach(() => vi.unstubAllGlobals());
  async function story(balanceRows: [string, number][], overrides: Record<string, { basis?: string; currency?: string; end?: string }> = {}, country = "India") {
    const payload = discoverPayload();
    const company = { ...payload.company, country, reporting_currency: country === "India" ? "INR" : "USD", reporting_basis: "consolidated", aliases: [{ticker:"EXAMPLE",exchange:country === "India" ? "NSE" : "Nasdaq",is_current:true}] };
    const target = payload.datasets[0];
    const rows = balanceRows.map(([metric,value]) => ({ ...item(metric,metric,2025,value,country === "India" ? "INR millions" : "USD millions"),
      period:{type:"annual",end:overrides[metric]?.end ?? "2025-12-31",reporting_currency:overrides[metric]?.currency ?? (country === "India" ? "INR" : "USD"),reporting_basis:overrides[metric]?.basis ?? "consolidated"} }));
    const body = { ...payload, company, country, datasets:[{...target,company,
      income:{...target.income,items:target.income.items.map(row=>({...row,period:{...row.period,reporting_currency:company.reporting_currency},display:{...row.display,unit:row.display.unit.replace("USD",company.reporting_currency)}}))},
      cash_flow:{...target.cash_flow,items:target.cash_flow.items.map(row=>({...row,period:{...row.period,reporting_currency:company.reporting_currency},display:{...row.display,unit:row.display.unit.replace("USD",company.reporting_currency)}}))},
      balance:{...target.balance,items:rows}}] };
    vi.stubGlobal("fetch",vi.fn().mockResolvedValue(Response.json(body)));
    return (await pullDataCompany(env,"EXAMPLE",2024,2025)).companyStory!.balanceSheet;
  }
  const core: [string,number][] = [["assets",1000],["shareholders_equity",500],["liabilities",500],["cash_and_short_term_investments",100],["accounts_receivable",150],["inventory",100]];
  it("uses authoritative current assets minus complete cash while preserving narrow subcomponents", async () => {
    const result=await story([...core,["other_current_assets",30],["current_assets",800]]);
    const group=result.assets.find(x=>x.key==="operating_current_assets")!;
    expect(group.value).toBe(700);expect(group.trend).toHaveLength(1);
    expect(group.children.find(x=>x.key==="accounts_receivable")?.value).toBe(150);
  });
  it("requires the authoritative total even when the narrow categories happen to sum", async () => {
    const result=await story([...core,["other_current_assets",30]]);
    expect(result.assets.find(x=>x.key==="operating_current_assets")?.value).toBeNull();
  });
  it("keeps separately reported prepaids and other-current assets as distinct details", async () => {
    const result=await story([...core,["prepaid_expenses",10],["other_current_assets",440],["current_assets",800]]);
    const group=result.assets.find(x=>x.key==="operating_current_assets")!;
    expect(group.value).toBe(700);
    expect(group.children.find(x=>x.key==="prepaid_expenses")?.value).toBe(10);
    expect(group.children.find(x=>x.key==="other_current_assets")?.value).toBe(440);
  });
  it("uses the residual including prepaids when prepaids were not separately reported", async () => {
    const result=await story([...core,["other_current_assets",450],["current_assets",800]]);
    expect(result.assets.find(x=>x.key==="operating_current_assets")?.value).toBe(700);
  });
  it.each([{current_assets:{basis:"standalone"}},{current_assets:{currency:"USD"}},{current_assets:{end:"2025-03-31"}}])("rejects incompatible authoritative total %j", async overrides => {
    const result=await story([...core,["other_current_assets",450],["current_assets",800]],overrides);
    expect(result.assets.find(x=>x.key==="operating_current_assets")?.value).toBeNull();
  });
  it("withholds ordinary US grouping when no authoritative current total is available", async () => {
    const result=await story([...core,["prepaid_expenses",10],["other_current_assets",440]],{},"USA");
    expect(result.assets.find(x=>x.key==="operating_current_assets")?.value).toBeNull();
  });
});


describe("API-supplied TTM statement columns", () => {
  it("retains latest TTM for all statements and shares without altering annual series", async () => {
    const payload = discoverPayload();
    const target = payload.datasets[0];
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(payload)));
    const annualOnly = await pullDataCompany(env, "data-0000000001", 2024, 2026);
    const trailing = (metric: string, label: string, value: number, unit = "USD millions") => ({
      ...item(metric, label, 2026, value, unit),
      period: { type: "ttm", start: "2025-07-01", end: "2026-06-30" },
      provenance: { source_url: "https://www.sec.gov/Archives/alpha-20260630.xml", derivation_method: "trailing_twelve_months" },
    });
    target.income.items.push(trailing("revenue", "Revenue", 1300), trailing("income_tax_provision", "Income Tax Provision", 71),
      trailing("gross_margin", "Gross Margin", -3.93891, "ratio"), trailing("eps_diluted", "EPS (Diluted)", 3.5, "USD per share"));
    target.balance.items.push(trailing("assets", "Total Assets", 1600));
    target.cash_flow.items.push(trailing("operating_cash_flow", "Operating Cash Flow", 340), trailing("capital_expenditures", "Capital Expenditures", 0));
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(payload)));
    const company = await pullDataCompany(env, "data-0000000001", 2024, 2026);
    const income = company.statements.find(group => group.key === "income")!;
    expect(income.ttmPeriodEnd).toBe("2026-06-30");
    expect(income.facts.find(fact => fact.key === "revenue")!.ttm).toMatchObject({ value: 1300, periodEnd: "2026-06-30", periodStart: "2025-07-01",
      provenance: { derivation_method: "trailing_twelve_months" } });
    expect(income.facts.find(fact => fact.key === "income_tax_provision")!.values).toEqual([]);
    expect(income.facts.find(fact => fact.key === "income_tax_provision")!.ttm!.value).toBe(71);
    expect(income.facts.find(fact => fact.key === "gross_margin")!.ttm).toMatchObject({ value: -3.93891, displayFormat: "percentage" });
    expect(company.statements.find(group => group.key === "balance")!.facts.find(fact => fact.key === "assets")!.ttm!.value).toBe(1600);
    expect(company.statements.find(group => group.key === "cash")!.facts.find(fact => fact.key === "capex")!.ttm!.value).toBe(0);
    expect(company.statements.find(group => group.key === "shares")!.facts.find(fact => fact.key === "eps")!.ttm!.value).toBe(3.5);
    expect(company.metrics.revenue).toEqual([{ year: 2024, value: 800 }, { year: 2025, value: 1000 }]);
    expect(income.facts.find(fact => fact.key === "revenue")!.values).toEqual([{ year: 2024, value: 800 }, { year: 2025, value: 1000 }]);
    expect(company.researchShelf.cashFlow).toEqual(annualOnly.researchShelf.cashFlow);
  });
  it("does not fill a missing latest TTM from stale TTM or an annual observation", async () => {
    const payload = discoverPayload();
    payload.datasets[0].income.items.push({ ...item("revenue", "Revenue", 2026, 1250), period: { type: "ttm", end: "2026-03-31" } },
      { ...item("gross_profit", "Gross Profit", 2026, 500), period: { type: "ttm", end: "2026-06-30" } });
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(payload)));
    const company = await pullDataCompany(env, "data-0000000001", 2024, 2026);
    const income = company.statements.find(group => group.key === "income")!;
    expect(income.ttmPeriodEnd).toBe("2026-06-30");
    expect(income.facts.find(fact => fact.key === "revenue")!.ttm).toBeNull();
    expect(company.statements.find(group => group.key === "balance")!.ttmPeriodEnd).toBeNull();
    expect(company.metrics.revenue.at(-1)).toEqual({ year: 2025, value: 1000 });
  });
});

describe("API-defined combined asset presentation", () => {
  it("retains an unavailable required combined parent and exact reported components", async () => {
    const payload = discoverPayload();
    const balance = payload.datasets[0].balance;
    balance.items = balance.items.filter(fact => fact.metric !== "property_plant_equipment_net");
    Object.assign(balance, {
      metrics: [
        { key: "property_plant_equipment_net", label: "Net PP&E including lease assets", required_presentation: true },
        { key: "reported_property_plant_equipment_net", label: "Reported net PP&E", parent_key: "property_plant_equipment_net" },
        { key: "operating_lease_right_of_use_asset", label: "Reported lease right-of-use assets", parent_key: "property_plant_equipment_net" },
      ],
      ppe_lease_scope: [{ period: { type: "annual", end: "2025-12-31" }, status: "unavailable", reason: "SEC inclusion scope unknown", ppe_fact_id: 101 }],
    });
    balance.items.push(item("reported_property_plant_equipment_net", "Reported net PP&E", 2025, 80),
      item("operating_lease_right_of_use_asset", "Reported lease right-of-use assets", 2025, 20));
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(payload)));
    const company = await pullDataCompany(env, "data-0000000001", 2024, 2026);
    const rows = company.statements.find(group => group.key === "balance")!.facts;
    expect(rows.find(fact => fact.key === "propertyPlantEquipment")).toMatchObject({
      label: "Net PP&E including lease assets", presentationRequired: true, values: [], ttm: null,
      periodScope: [{ status: "unavailable", reason: "SEC inclusion scope unknown", ppe_fact_id: 101 }],
    });
    expect(rows.find(fact => fact.key === "reported_property_plant_equipment_net")).toMatchObject({
      label: "Reported net PP&E", parentKey: "propertyPlantEquipment", values: [{ year: 2025, value: 80 }],
    });
    expect(rows.find(fact => fact.key === "operating_lease_right_of_use_asset")!.values).toEqual([{ year: 2025, value: 20 }]);
  });
});

describe("complete source inputs for debt and working-capital calculations", () => {
  it.each([null, 0, 40])("keeps missing current debt unavailable and preserves reported %s", async (debt) => {
    const payload = discoverPayload();
    const balance = payload.datasets[0].balance;
    balance.items = balance.items.filter(row => row.metric !== "short_term_debt" || row.period.end !== "2025-12-31");
    if (debt !== null) balance.items.push(item("short_term_debt", "Short-Term Debt", 2025, debt));
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(payload)));
    const company = await pullDataCompany(env, "data-0000000001", 2024, 2025);
    const current = company.researchShelf.cashFlow.yearly.find(point => point.year === 2025)!;
    expect(current.workingCapital.currentYear.totalInterestBearingCurrentDebt).toBe(debt);
    if (debt === null) {
      expect(current.workingCapital.currentYear.netOperatingWorkingCapital).toBeNull();
      expect(current.workingCapital.cashImpact).toBeNull();
      expect(current.fcff.fcff.companyValue).toBeNull();
      expect(current.fcfe.fcfe.companyValue).toBeNull();
      expect(company.statements.find(group => group.key === "balance")!.facts
        .find(fact => fact.key === "nonCashWorkingCapital")!.values.some(value => value.year === 2025)).toBe(false);
    } else {
      expect(current.workingCapital.currentYear.netOperatingWorkingCapital).toBe(520 - 110 - (370 - debt));
      expect(current.workingCapital.cashImpact).not.toBeNull();
    }
  });

  it.each([null, 0, 20])("requires the short-term investment input for net debt (%s)", async (investments) => {
    const payload = discoverPayload();
    const balance = payload.datasets[0].balance;
    balance.items = balance.items.filter(row => row.metric !== "short_term_investments" || row.period.end !== "2025-12-31");
    if (investments !== null) balance.items.push(item("short_term_investments", "Short-Term Investments", 2025, investments));
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(payload)));
    const company = await pullDataCompany(env, "data-0000000001", 2024, 2025);
    const netDebt = company.metrics.netDebt.find(value => value.year === 2025);
    if (investments === null) expect(netDebt).toBeUndefined();
    else expect(netDebt?.value).toBe(340 - 110 - investments);
  });

  it.each([0, -250])("uses the canonical net-cash aggregate with the explicit headline sign (%s)", async (netCash) => {
    const payload = discoverPayload();
    const balance = payload.datasets[0].balance;
    balance.items = balance.items.filter(row => row.metric !== "short_term_investments");
    balance.items.push(item("net_debt", "Net Cash (Debt)", 2025, netCash));
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(payload)));
    const company = await pullDataCompany(env, "data-0000000001", 2024, 2025);
    expect(company.metrics.netDebt.find(value => value.year === 2025)?.value).toBe(0 - netCash);
  });

  it("uses a supplied complete cash total when its individual investment row is unavailable", async () => {
    const payload = discoverPayload();
    const balance = payload.datasets[0].balance;
    balance.items = balance.items.filter(row => row.metric !== "short_term_investments");
    balance.items.push(item("total_cash", "Total Cash", 2025, 130));
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(payload)));
    const company = await pullDataCompany(env, "data-0000000001", 2024, 2025);
    expect(company.metrics.netDebt.find(value => value.year === 2025)?.value).toBe(210);
  });

  const sourceSeries = () => ({
    revenue: [{ year: 2025, value: 1000 }], ebit: [{ year: 2025, value: 200 }],
    effectiveTaxRate: [{ year: 2025, value: .25 }],
    depreciationAndAmortization: [{ year: 2025, value: 30 }],
    capitalExpenditure: [{ year: 2025, value: -50 }],
    nonCashWorkingCapital: [{ year: 2024, value: 100 }, { year: 2025, value: 85 }],
    netIncomeToCommon: [{ year: 2025, value: 130 }],
    shareBasedCompensation: [{ year: 2025, value: 10 }],
    otherAdjustments: [{ year: 2025, value: 5 }],
    netBorrowing: [] as Array<{ year: number; value: number }>,
  });

  it.each([null, 0, 20])("requires actual net borrowing for FCFE (%s)", (borrowing) => {
    const series = sourceSeries();
    if (borrowing !== null) series.netBorrowing.push({ year: 2025, value: borrowing });
    const current = buildCashFlowAnalysis(series, new Map(), 2025, 2025).yearly[0];
    expect(current.fcfe.netBorrowing.companyValue).toBe(borrowing);
    expect(current.fcfe.fcfe.companyValue).toBe(borrowing === null ? null : 140 + borrowing);
    expect(current.fcff.fcff.companyValue).toBe(145);
  });

  it.each([0, 1, 2])("does not treat %s disclosed debt components as an exhaustive total", (count) => {
    const series = {
      ...sourceSeries(), nonCashWorkingCapital: [],
      currentAssets: [{ year: 2025, value: 500 }],
      cashAndCashEquivalents: [{ year: 2025, value: 100 }],
      currentLiabilities: [{ year: 2025, value: 300 }],
      shortTermBorrowings: count > 0 ? [{ year: 2025, value: 20 }] : [],
      currentPortionLongTermDebt: count > 1 ? [{ year: 2025, value: 30 }] : [],
    };
    const current = buildCashFlowAnalysis(series, new Map(), 2025, 2025).yearly[0].workingCapital.currentYear;
    expect(current.totalInterestBearingCurrentDebt).toBeNull();
    expect(current.otherInterestBearingCurrentDebt).toBeNull();
    expect(current.netCurrentLiabilities).toBeNull();
    expect(current.netOperatingWorkingCapital).toBeNull();
    expect(current.shortTermBorrowings).toBe(count > 0 ? 20 : null);
    expect(current.currentPortionLongTermDebt).toBe(count > 1 ? 30 : null);
  });

  it.each([0, 1, 2])("does not invent the undisclosed residual of a known debt total (%s components)", (count) => {
    const series = {
      ...sourceSeries(), currentDebt: [{ year: 2025, value: 60 }],
      shortTermBorrowings: count > 0 ? [{ year: 2025, value: 20 }] : [],
      currentPortionLongTermDebt: count > 1 ? [{ year: 2025, value: 30 }] : [],
    };
    const current = buildCashFlowAnalysis(series, new Map(), 2025, 2025).yearly[0].workingCapital.currentYear;
    expect(current.totalInterestBearingCurrentDebt).toBe(60);
    expect(current.otherInterestBearingCurrentDebt).toBe(count === 2 ? 10 : null);
  });
});

describe("complete balance-story totals and exact annual averages", () => {
  afterEach(() => vi.unstubAllGlobals());
  async function balanceStory(configure: (payload: ReturnType<typeof discoverPayload>) => void) {
    const payload = discoverPayload();
    configure(payload);
    const fetchMock = vi.fn().mockResolvedValue(Response.json(payload));
    vi.stubGlobal("fetch", fetchMock);
    const story = (await pullDataCompany(env, "data-0000000001", 2024, 2025)).companyStory!.balanceSheet;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    return story;
  }
  const noReportedRates = (payload: ReturnType<typeof discoverPayload>) => {
    payload.datasets[0].balance.items = payload.datasets[0].balance.items.filter(row => ![
      "weighted_average_cost_of_debt", "long_term_debt_interest_rate", "short_term_debt_interest_rate",
    ].includes(row.metric));
    payload.datasets[0].income.items = payload.datasets[0].income.items.filter(row => row.metric !== "interest_expense" || row.period.end.startsWith("2025"));
  };
  it.each([null, 0, 20])("distinguishes absent short-term investments from reported %s in the cash total", async investments => {
    const story = await balanceStory(payload => {
      const balance = payload.datasets[0].balance;
      balance.items = balance.items.filter(row => row.metric !== "short_term_investments" || row.period.end !== "2025-12-31");
      if (investments !== null) balance.items.push(item("short_term_investments", "Short-term investments", 2025, investments));
    });
    const cash = story.assets.find(row => row.key === "cash")!;
    expect(cash.value).toBe(investments === null ? null : 110 + investments);
    expect(cash.children.find(child => child.key === "cash_and_equivalents")?.value).toBe(110);
    expect(cash.children.find(child => child.key === "short_term_investments")?.value).toBe(investments);
    if (investments === null) expect(cash.unavailableReason).toContain("Complete cash");
  });
  it.each(["end", "currency", "basis", "unknown_basis", "conflicting_units"])("refuses incompatible cash components: %s", async difference => {
    const story = await balanceStory(payload => {
      payload.datasets[0].balance.items = payload.datasets[0].balance.items.map(row => row.metric === "short_term_investments" && row.period.end.startsWith("2025")
        ? { ...row, period: { ...row.period, ...(difference === "end" ? { end: "2025-09-30" }
          : difference === "currency" ? { reporting_currency: "EUR" } : { reporting_basis: "standalone" as const }) } } : row);
      if (difference === "conflicting_units") payload.datasets[0].balance.items = payload.datasets[0].balance.items.map(row => row.metric === "short_term_investments"
        ? { ...row, display: { ...row.display, unit: "EUR millions" }, period: { ...row.period, reporting_currency: "USD" } } : row);
      if (difference === "unknown_basis") {
        Object.assign(payload.datasets[0].company, { reporting_basis: undefined });
        payload.datasets[0].balance.items = payload.datasets[0].balance.items.map(row => ({ ...row, period: { type: "annual", end: row.period.end } }));
      }
    });
    expect(story.assets.find(row => row.key === "cash")?.value).toBeNull();
  });
  it("prefers period-specific canonical cash totals and retains historical complete component totals", async () => {
    const story = await balanceStory(payload => {
      const balance = payload.datasets[0].balance;
      balance.items = balance.items.filter(row => row.metric !== "short_term_investments" || row.period.end !== "2025-12-31");
      balance.items.push(item("total_cash", "Complete cash aggregate", 2025, 175));
    });
    const cash = story.assets.find(row => row.key === "cash")!;
    expect(cash.value).toBe(175);
    expect(cash.trend.map(row => [row.fiscalYear, row.value])).toEqual([[2024, 120], [2025, 175]]);
    expect(cash.children.find(row => row.key === "short_term_investments")?.value).toBeNull();
  });
  it.each([null, 0, 30])("requires both unearned-revenue inputs and preserves noncurrent %s", async noncurrent => {
    const story = await balanceStory(payload => {
      const balance = payload.datasets[0].balance;
      balance.items = balance.items.filter(row => row.metric !== "deferred_revenue_noncurrent" || row.period.end !== "2025-12-31");
      if (noncurrent !== null) balance.items.push(item("deferred_revenue_noncurrent", "Noncurrent unearned revenue", 2025, noncurrent));
    });
    const group = story.liabilities.find(row => row.key === "unearned_revenue")!;
    expect(group.value).toBe(noncurrent === null ? null : 70 + noncurrent);
    expect(group.children.find(row => row.key === "unearned_revenue_current")?.value).toBe(70);
    if (noncurrent === null) expect(group.unavailableReason).toContain("Complete same-period");
  });
  it("keeps canonical debt scope without adding overlapping lease details", async () => {
    const story = await balanceStory(() => {});
    const group = story.liabilities.find(row => row.key === "borrowings")!;
    expect(group.value).toBe(340);
    expect(group.children.find(row => row.key === "short_term_leases")?.value).toBe(12);
    expect(group.children.find(row => row.key === "long_term_leases")?.value).toBe(24);
  });
  it.each([null, 0, 400])("requires a canonical debt total, distinguishing absent from %s", async total => {
    const story = await balanceStory(payload => {
      const balance = payload.datasets[0].balance;
      balance.items = balance.items.filter(row => row.metric !== "total_debt" || row.period.end !== "2025-12-31");
      if (total !== null) balance.items.push(item("total_debt", "Verified debt total", 2025, total));
    });
    expect(story.health.totalDebt).toBe(total);
    expect(story.health.netCashDebt).toBe(total === null ? null : 130 - total);
    expect(story.liabilities.find(row => row.key === "borrowings")?.children.find(row => row.key === "long_term_debt")?.value).toBe(300);
  });
  it.each(["missing_prior", "stale_prior", "missing_component", "wrong_start", "prior_currency", "prior_basis"])("refuses incomplete or nonadjacent annual borrowing balances: %s", async problem => {
    const story = await balanceStory(payload => {
      noReportedRates(payload);
      const balance = payload.datasets[0].balance;
      if (["missing_prior", "stale_prior"].includes(problem)) balance.items = balance.items.filter(row => !["short_term_debt", "long_term_debt"].includes(row.metric)
        || row.period.end !== "2024-12-31" && (problem === "stale_prior" || row.period.end !== "2023-12-31"));
      if (problem === "missing_component") balance.items = balance.items.filter(row => row.metric !== "long_term_debt" || row.period.end !== "2025-12-31");
      if (problem === "wrong_start") payload.datasets[0].income.items = payload.datasets[0].income.items.map(row => row.metric === "interest_expense"
        ? { ...row, period: { ...row.period, start: "2025-01-02" } } : row);
      if (problem === "prior_currency" || problem === "prior_basis") balance.items = balance.items.map(row => row.period.end === "2024-12-31" && ["short_term_debt", "long_term_debt"].includes(row.metric)
        ? { ...row, period: { ...row.period, ...(problem === "prior_currency" ? { reporting_currency: "EUR" } : { reporting_basis: "standalone" as const }) } } : row);
    });
    expect(story.health.weightedAverageCostOfDebt).toBeNull();
    expect(story.health.weightedAverageCostOfDebtNote).not.toContain("no reported debt");
  });
  it("allows an explicitly reported zero closing balance in a complete annual average", async () => {
    const story = await balanceStory(payload => {
      noReportedRates(payload);
      payload.datasets[0].balance.items = payload.datasets[0].balance.items.map(row => row.period.end === "2025-12-31" && ["short_term_debt", "long_term_debt"].includes(row.metric)
        ? { ...row, display: { ...row.display, value: "0" } } : row);
    });
    expect(story.health.weightedAverageCostOfDebt).toBe(10 / 160 * 100);
  });
  it("uses actual 53-week fiscal boundaries without calendar-month substitution", async () => {
    const story = await balanceStory(payload => {
      noReportedRates(payload);
      payload.datasets[0].income.items = payload.datasets[0].income.items.map(row => row.period.end.startsWith("2025")
        ? { ...row, period: { ...row.period, start: "2024-12-29", end: "2025-12-27" } } : row);
      payload.datasets[0].balance.items = payload.datasets[0].balance.items.map(row => ({ ...row,
        period: { ...row.period, end: row.period.end.startsWith("2024") ? "2024-12-28" : row.period.end.startsWith("2025") ? "2025-12-27" : row.period.end } }));
    });
    expect(story.health.weightedAverageCostOfDebt).toBeCloseTo(10 / 330 * 100);
  });
  it.each(["missing_current", "missing_longterm", "complete_zero"])("requires known zero borrowings before calling funding lease-only: %s", async coverage => {
    const story = await balanceStory(payload => {
      noReportedRates(payload);
      payload.datasets[0].income.items = payload.datasets[0].income.items.filter(row => row.metric !== "interest_expense");
      const balance = payload.datasets[0].balance;
      balance.items = balance.items.filter(row => !["short_term_debt", "long_term_debt"].includes(row.metric));
      balance.items.push(item("operating_lease_weighted_average_discount_rate", "Lease discount rate", 2025, .052, "ratio"));
      if (coverage !== "missing_current") balance.items.push(item("short_term_debt", "Current borrowings", 2025, 0));
      if (coverage !== "missing_longterm") balance.items.push(item("long_term_debt", "Long-term borrowings", 2025, 0));
    });
    expect(story.health.weightedAverageCostOfDebt).toBe(coverage === "complete_zero" ? 5.2 : null);
    expect(story.health.weightedAverageCostOfDebtNote.includes("Lease-only")).toBe(coverage === "complete_zero");
  });
  it("does not classify missing total debt as no reported debt", async () => {
    const story = await balanceStory(payload => {
      noReportedRates(payload);
      payload.datasets[0].income.items = payload.datasets[0].income.items.filter(row => row.metric !== "interest_expense");
      payload.datasets[0].balance.items = payload.datasets[0].balance.items.filter(row => row.metric !== "total_debt");
    });
    expect(story.health.weightedAverageCostOfDebtNote).toContain("unavailable");
    expect(story.health.weightedAverageCostOfDebtNote).not.toContain("no reported debt");
  });
  it("does not double-count overlapping prepaid details and retains other receivables", async () => {
    const story = await balanceStory(payload => {
      payload.datasets[0].balance.items.push(item("other_current_assets", "Other current assets including prepaid expenses", 2025, 160),
        item("prepaid_expenses_and_other_current_assets", "Reported combined detail", 2025, 160),
        item("other_receivables", "Notes and loans receivable", 2025, 30));
    });
    const group = story.assets.find(row => row.key === "operating_current_assets")!;
    expect(group.value).toBe(520 - 130);
    expect(group.children.find(row => row.key === "prepaid_expenses")?.value).toBe(24);
    expect(group.children.find(row => row.key === "other_current_assets")?.value).toBe(160);
    expect(group.children.find(row => row.key === "reported_prepaids_and_other")?.value).toBe(160);
    expect(group.children.find(row => row.key === "other_receivables")?.value).toBe(30);
  });
});


describe("API-defined current investment scope labels", () => {
  it("uses the approved upstream label for the statement and reported investment detail", async () => {
    const payload = discoverPayload();
    const label = "Current Investments & Other Financial Assets";
    payload.datasets[0].balance.items = payload.datasets[0].balance.items.map(row => row.metric === "short_term_investments"
      ? { ...row, metric_label: label } : row);
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(payload)));
    const company = await pullDataCompany(env, "data-0000000001", 2024, 2025);
    expect(company.statements.find(row => row.key === "balance")!.facts.find(row => row.key === "shortTermInvestments")!.label).toBe(label);
    expect(company.companyStory!.balanceSheet!.assets.find(row => row.key === "cash")!.children
      .find(row => row.key === "short_term_investments")).toMatchObject({ label, value: 20 });
    expect(company.companyStory!.balanceSheet!.health.totalCash).toBe(130);
  });
});


describe("compatible balance display scales", () => {
  it("does not combine differently scaled monetary inputs despite matching dates and currency", async () => {
    const payload = discoverPayload();
    payload.datasets[0].balance.items = payload.datasets[0].balance.items.map(row => row.metric === "short_term_investments"
      ? { ...row, display: { value: "20000000", unit: "USD" } } : row);
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(payload)));
    const company = await pullDataCompany(env, "data-0000000001", 2024, 2025);
    const cash = company.companyStory!.balanceSheet!.assets.find(row => row.key === "cash")!;
    expect(cash.value).toBeNull();
    expect(cash.children.find(row => row.key === "cash_and_equivalents")?.value).toBe(110);
  });
});



describe("API-proved borrowing scope presentation", () => {
  it("retains unavailable combined parents, original children and exact period reasons", async () => {
    const payload = discoverPayload(), balance = payload.datasets[0].balance;
    balance.items = balance.items.filter(row => !["short_term_debt", "long_term_debt"].includes(row.metric));
    Object.assign(balance, {
      metrics: [
        { key: "short_term_debt", label: "Current Borrowings & Current Debt Maturities", required_presentation: true },
        { key: "long_term_debt", label: "Long-Term Borrowings & Bonds", required_presentation: true },
        { key: "reported_noncurrent_borrowing_component", label: "Reported Noncurrent Borrowing Component", parent_key: "long_term_debt" },
      ],
      sec_ifrs_debt_scope: [
        { section: "current", period: { type: "annual", end: "2025-12-31" }, status: "unavailable", reason: "Current maturity scope unresolved", parent_fact_id: 51 },
        { section: "noncurrent", period: { type: "annual", end: "2025-12-31" }, status: "unavailable", reason: "Original bond operand is not bound", parent_fact_id: 52 },
        { section: "noncurrent", period: { type: "ttm", end: "2026-06-30" }, status: "unavailable", reason: "Competing filed definitions", parent_fact_id: 53 },
      ],
    });
    balance.items.push(item("reported_noncurrent_borrowing_component", "Reported Noncurrent Borrowing Component", 2025, 20));
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(payload)));
    const company = await pullDataCompany(env, "data-0000000001", 2024, 2026);
    const rows = company.statements.find(group => group.key === "balance")!.facts;
    const current = rows.find(row => row.key === "currentDebt")!, noncurrent = rows.find(row => row.key === "longTermDebt")!;
    expect(current).toMatchObject({ values: [], ttm: null, presentationRequired: true,
      label: "Current Borrowings & Current Debt Maturities", periodScope: [{ reason: "Current maturity scope unresolved" }] });
    expect(noncurrent).toMatchObject({ values: [], ttm: null, presentationRequired: true,
      label: "Long-Term Borrowings & Bonds", periodScope: [{ parent_fact_id: 52 }, { parent_fact_id: 53 }] });
    expect(rows.find(row => row.key === "reported_noncurrent_borrowing_component")).toMatchObject({
      parentKey: "longTermDebt", values: [{ year: 2025, value: 20 }],
    });
  });
  it("preserves an explicit API combined zero without substituting a missing component", async () => {
    const payload = discoverPayload(), balance = payload.datasets[0].balance;
    balance.items = balance.items.filter(row => row.metric !== "short_term_debt");
    balance.items.push(item("short_term_debt", "Current Borrowings & Current Debt Maturities", 2025, 0));
    Object.assign(balance, { metrics: [{ key: "short_term_debt", label: "Current Borrowings & Current Debt Maturities" }],
      sec_ifrs_debt_scope: [{ section: "current", period: { type: "annual", end: "2025-12-31" }, status: "available", parent_fact_id: 51 }] });
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(payload)));
    const company = await pullDataCompany(env, "data-0000000001", 2024, 2026);
    expect(company.statements.find(group => group.key === "balance")!.facts.find(row => row.key === "currentDebt"))
      .toMatchObject({ values: [{ year: 2025, value: 0 }], periodScope: [{ status: "available" }] });
  });
});


describe("API-defined broad cash adjustment and pure D&A scope", () => {
  it("retains exact annual and latest TTM scope with source-labelled nonadditive details", async () => {
    const payload = discoverPayload(), cash = payload.datasets[0].cash_flow;
    payload.datasets[0].income.items = payload.datasets[0].income.items.filter(row => row.metric !== "depreciation_amortization");
    const annualScope = { schema: "sec-cash-da-other-scope-v1", status: "available", cik: "0000000001", accession: "0000000001-26-000001",
      period: { type: "annual", start: "2025-01-01", end: "2025-12-31" }, unit: "USD",
      reported_detail_components: [{ fact_id: 31, concept: "DepreciationDepletionAndAmortization", value: "30000000" }],
      detail_decomposition: { status: "available" }, pure_da_subset: { status: "available", value: "30000000", detail_fact_bindings: [31] }, source_artifacts: [{ id: 11, sha256: "a".repeat(64) }] };
    const ttmScope = { schema: "sec-cash-da-other-ttm-scope-v1", status: "available", input_scopes: [{ input_role: "annual", category_scope: annualScope }],
      ttm_method: "annual_plus_current_ytd_minus_prior_ytd" };
    const scoped = (metric: string, label: string, year: number, value: number, scope: Record<string, unknown>, role?: string) => ({
      ...item(metric, label, year, value), provenance: { sec_cash_da_other_scope: scope, ...(role ? { presentation_role: role } : {}) },
    });
    Object.assign(cash, { metrics: [
      { key: "cash_flow_depreciation_amortization_and_other", label: "Depreciation, Amortization and Other" },
      { key: "cash_flow_depreciation_amortization", label: "Pure Depreciation & Amortization", parent_key: "cash_flow_depreciation_amortization_and_other" },
    ] });
    cash.items.push(scoped("cash_flow_depreciation_amortization_and_other", "Item label", 2023, 90, { schema: "outside-selected-history" }),
      scoped("cash_flow_depreciation_amortization_and_other", "Item label", 2025, 100, annualScope),
      scoped("cash_flow_depreciation_amortization", "Pure D&A", 2025, 30, annualScope, "non_additive_reported_detail"),
      { ...scoped("cash_flow_depreciation_amortization_and_other", "Item label", 2026, 110, ttmScope),
        period: { type: "ttm", start: "2025-07-01", end: "2026-06-30" } },
      { ...scoped("cash_flow_depreciation_amortization_and_other", "Item label", 2026, 105, { schema: "stale-ttm" }),
        period: { type: "ttm", start: "2025-04-01", end: "2026-03-31" } });
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(payload)));
    const company = await pullDataCompany(env, "data-0000000001", 2024, 2026);
    const rows = company.statements.find(group => group.key === "cash")!.facts;
    const broad = rows.find(row => row.key === "cash_flow_depreciation_amortization_and_other")!;
    expect(broad.label).toBe("Depreciation, Amortization and Other");
    expect(broad.values).toEqual([{ year: 2025, value: 100 }]);
    expect(broad.ttm).toMatchObject({ value: 110, periodEnd: "2026-06-30", provenance: { sec_cash_da_other_scope: ttmScope } });
    expect(broad.periodScope).toEqual([
      { period: { type: "annual", start: "2025-01-01", end: "2025-12-31" }, status: "available", evidence: annualScope },
      { period: { type: "ttm", start: "2025-07-01", end: "2026-06-30" }, status: "available", evidence: ttmScope },
    ]);
    expect(rows.find(row => row.key === "depreciation")).toMatchObject({ label: "Pure Depreciation & Amortization",
      parentKey: broad.key, values: [{ year: 2025, value: 30 }],
      periodScope: [{ presentation: "non_additive_reported_detail", evidence: annualScope }] });
    expect(company.researchShelf.cashFlow.yearly.find(row => row.year === 2025)!.fcff.depreciationAndAmortization.companyValue).toBe(30);
    expect(company.researchShelf.cashFlow.yearly.find(row => row.year === 2025)!.fcfe.depreciationAndAmortization.companyValue).toBe(30);
  });
  it.each(["direct", "category_scope"])("preserves available parent but refuses incomplete pure total from %s TTM leg", async legShape => {
    const payload = discoverPayload(), target = payload.datasets[0], cash = target.cash_flow;
    target.income.items = target.income.items.filter(row => row.metric !== "depreciation_amortization");
    const scope = { schema: "sec-cash-da-other-scope-v1", status: "available",
      detail_decomposition: { status: "unavailable", reason: "Original note equation incomplete" },
      pure_da_subset: { status: "unavailable", reason: "A pure amortization component is missing" },
      source_artifacts: [{ id: 11, sha256: "a".repeat(64) }] };
    Object.assign(cash, { metrics: [
      { key: "cash_flow_depreciation_amortization_and_other", label: "Depreciation, Amortization and Other" },
      { key: "cash_flow_depreciation_amortization", label: "Reported partial D&A", parent_key: "cash_flow_depreciation_amortization_and_other" },
      { key: "cash_flow_depreciation", label: "Reported Depreciation", parent_key: "cash_flow_depreciation_amortization_and_other" },
    ] });
    cash.items.push(...["cash_flow_depreciation_amortization_and_other", "cash_flow_depreciation_amortization", "cash_flow_depreciation"]
      .map((metric, index) => ({ ...item(metric, "Reported amount", 2025, [100, 30, 12][index]),
        provenance: { sec_cash_da_other_scope: scope, presentation_role: "non_additive_reported_detail" } })));
    cash.items.push({ ...item("cash_flow_depreciation_amortization", "Reported partial D&A", 2026, 32),
      period: { type: "ttm", start: "2025-07-01", end: "2026-06-30" }, provenance: {
        sec_cash_da_other_scope: { schema: "sec-cash-da-other-ttm-scope-v1", status: "available",
          input_scopes: [legShape === "direct" ? { input_role: "annual", ...scope }
            : { input_role: "annual", category_scope: scope }] }, presentation_role: "non_additive_reported_detail" } });
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(payload)));
    const company = await pullDataCompany(env, "data-0000000001", 2024, 2026);
    const rows = company.statements.find(group => group.key === "cash")!.facts;
    expect(rows.find(row => row.key === "cash_flow_depreciation_amortization_and_other")!.periodScope![0].status).toBe("available");
    const partial = rows.find(row => row.key === "depreciation")!;
    expect(partial).toMatchObject({ label: "Reported partial D&A", parentKey: "cash_flow_depreciation_amortization_and_other",
      values: [{ year: 2025, value: 30 }], periodScope: [{ status: "unavailable", evidence: scope }, { status: "unavailable" }] });
    expect(rows.find(row => row.key === "cash_flow_depreciation")!.periodScope![0].status).toBe("available");
    const current = company.researchShelf.cashFlow.yearly.find(row => row.year === 2025)!;
    expect(current.fcff.depreciationAndAmortization.companyValue).toBeNull();
    expect(current.fcfe.depreciationAndAmortization.companyValue).toBeNull();
  });
  it("does not substitute a broad adjustment for an unavailable pure D&A input", async () => {
    const payload = discoverPayload(), target = payload.datasets[0];
    target.income.items = target.income.items.filter(row => row.metric !== "depreciation_amortization");
    target.cash_flow.items.push(item("cash_flow_depreciation_amortization_and_other", "Depreciation, Amortization and Other", 2025, 100));
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(payload)));
    const company = await pullDataCompany(env, "data-0000000001", 2024, 2026);
    const current = company.researchShelf.cashFlow.yearly.find(row => row.year === 2025)!;
    expect(current.fcff.depreciationAndAmortization.companyValue).toBeNull();
    expect(current.fcfe.depreciationAndAmortization.companyValue).toBeNull();
  });
});


describe("complete source-backed supplementary financial tables", () => {
  it("retains unavailable annual and TTM cells with the published PCAR null-unit shape", async () => {
    const payload = discoverPayload(), target = payload.datasets[0];
    const reason = "Four current quarters explicitly classified as regular cash dividends are not supported by retained SEC sources.";
    // This is the nullable item shape returned by the normal prepared API,
    // including its per-share format despite the unavailable source unit.
    const trailing = { fact_id: null, observation_id: null, metric: "dividend_per_share", metric_label: "Regular Dividend Per Share",
      value: null, unit: null, display: { decimals: 2, format: "per_share", scale: 1, unit: null, value: null },
      period: { end: "2026-06-30", start: null, type: "ttm" }, status: "unavailable", reason,
      provenance: { source_gap: { status: "unavailable", reason }, source_url: null } };
    const annual = { ...trailing, metric: "book_value_per_share", metric_label: "Book Value Per Share (As Filed)",
      period: { type: "annual", start: null, end: "2005-12-31" }, reason: "Required SEC calculation input is unavailable: book_value" };
    Object.assign(target.income, { metrics: [{ key: trailing.metric, label: trailing.metric_label, required_presentation: true }],
      items: [...target.income.items, trailing] });
    Object.assign(target.balance, { metrics: [{ key: annual.metric, label: annual.metric_label, required_presentation: true },
      { key: "required_without_observation", label: "Required source metric", description: null, required_presentation: true }],
      items: [...target.balance.items, annual] });
    const fetcher = vi.fn().mockResolvedValue(Response.json(payload));
    vi.stubGlobal("fetch", fetcher);
    const company = await pullDataCompany(env, "data-0000000001", 2005, 2025);
    expect(company.statements.find(group => group.key === "income")!.facts.find(fact => fact.key === trailing.metric))
      .toMatchObject({ unit: null, values: [], ttm: { value: null, unit: null, canonicalValue: null, canonicalUnit: null,
        status: "unavailable", reason, provenance: trailing.provenance } });
    const balance = company.statements.find(group => group.key === "balance")!.facts;
    expect(balance.find(fact => fact.key === annual.metric)).toMatchObject({ unit: null, values: [], annualSources: [
      { year: 2005, value: null, unit: null, displayUnit: null, status: "unavailable", reason: annual.reason, provenance: annual.provenance }], ttm: null });
    expect(balance.find(fact => fact.key === "required_without_observation")).toMatchObject({ unit: null, values: [], ttm: null });
    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(fetcher.mock.calls[0][0]).toBe("https://data.example/v1/discover/company-dataset");
  });
  it("uses an explicitly supplied later unit and never invents a unit for an otherwise numeric cell", async () => {
    const payload = discoverPayload(), target = payload.datasets[0];
    const missing = { ...item("dividend_per_share", "Regular Dividend Per Share", 2024, 12), value: "12", unit: null,
      display: { value: "12", unit: null, format: "per_share" }, status: "reported", provenance: null };
    const available = { ...item("dividend_per_share", "Regular Dividend Per Share", 2025, 1.32, "CAD per share"), value: "1.32", unit: "CAD/shares" };
    Object.assign(target.income, { metrics: [{ key: missing.metric, label: missing.metric_label }],
      items: [...target.income.items, missing, available] });
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(payload)));
    const company = await pullDataCompany(env, "data-0000000001", 2024, 2025);
    const dividend = company.statements.find(group => group.key === "income")!.facts.find(fact => fact.key === missing.metric)!;
    expect(dividend).toMatchObject({ unit: "CAD per share", values: [{ year: 2025, value: 1.32 }] });
    expect(dividend.annualSources![0]).toMatchObject({ year: 2024, value: "12", unit: null, displayUnit: null, provenance: null });
    expect(dividend.unit).not.toBe("USD per share");
  });
  it("maps API ratios and KPIs with their own periods, units, missing reasons and provenance", async () => {
    const payload = discoverPayload();
    const annual = Object.assign(item("return_on_assets", "Return on Assets", 2025, 0.12, "ratio"), {
      value: "0.123456789", display: { value: "0.12", unit: "ratio", format: "percentage" },
      provenance: { source_url: "https://www.sec.gov/Archives/test/filing.xml", accession: "test-accession",
        concept: "NetIncomeLoss", filed_date: "2026-02-01", calculation_inputs: [{ fact_id: 11 }] },
    });
    const unavailable = Object.assign(item("debt_ratio", "Debt Ratio", 2025, 0, "ratio"), {
      value: null, display: { value: null, unit: "ratio", format: "number" },
      status: "unavailable", reason: "Comparable SEC debt scope is unavailable.",
    });
    Object.assign(payload.datasets[0], {
      ratios: { statement: "ratios", columns: [{ period_type: "annual", end: "2025-12-31" }, { period_type: "ttm", end: "2026-06-30" }],
        metrics: [{ key: "return_on_assets", label: "Return on Assets" }, { key: "debt_ratio", label: "Debt Ratio" }],
        items: [annual, unavailable, { ...annual, period: { type: "ttm", start: "2025-07-01", end: "2026-06-30" } }] },
      kpis: { statement: "kpis", metrics: [{ key: "truck_deliveries", label: "Truck Deliveries" }],
        columns: [{ period_type: "annual", end: "2005-12-31" }, { period_type: "annual", end: "2025-12-31" }],
        items: [item("truck_deliveries", "Truck Deliveries", 2005, 1523, "trucks"), item("truck_deliveries", "Truck Deliveries", 2025, 2417, "trucks")] },
    });
    const fetcher = vi.fn().mockResolvedValue(Response.json(payload));
    vi.stubGlobal("fetch", fetcher);
    const company = await pullDataCompany(env, "data-0000000001", 2005, 2025);
    const ratios = company.statements.find(group => group.key === "ratios")!;
    expect(ratios.annualPeriods).toEqual(["2025-12-31"]);
    expect(ratios.facts[0]).toMatchObject({ displayFormat: "percentage", values: [{ year: 2025, value: .123456789 }],
      annualSources: [{ provenance: annual.provenance, sourceUrl: annual.provenance.source_url }],
      ttm: { value: .123456789, periodEnd: "2026-06-30" } });
    expect(ratios.facts[1]).toMatchObject({ values: [], annualSources: [{ status: "unavailable", reason: unavailable.reason }] });
    const kpis = company.statements.find(group => group.key === "kpis")!;
    expect(kpis.facts[0]).toMatchObject({ unit: "trucks", values: [{ year: 2005, value: 1523 }, { year: 2025, value: 2417 }] });
    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(fetcher.mock.calls[0][0]).toBe("https://data.example/v1/discover/company-dataset");
  });
});


describe("canonical cash-flow headline and declared display scales", () => {
  it.each([1, 1000000, "1", "1000000"])("uses canonical FCF and statement-scoped inputs with API scale %s", async scale => {
    const payload = discoverPayload(), target = payload.datasets[0];
    const source = "https://www.sec.gov/Archives/edgar/data/75362/000119312526057025/pcar-20251231.htm";
    const canonical = { metric: "free_cash_flow", metric_label: "Free Cash Flow", value: "3029200000", unit: "USD",
      display: { value: String(3029200000 / Number(scale)), unit: "USD", scale, decimals: 2, format: "number" },
      period: { type: "annual", start: "2025-01-01", end: "2025-12-31", reporting_currency: "USD" }, status: "derived",
      provenance: { source_url: source, formula: "operating_cash_flow - abs(capital_expenditures)" } };
    target.balance.items.push(item("capital_expenditures", "Supporting Capital Expenditures", 2025, 743),
      item("share_based_compensation", "Supporting Share-Based Compensation", 2025, 999));
    target.income.items.push(item("free_cash_flow", "Supporting Free Cash Flow", 2025, 999));
    target.cash_flow.items = target.cash_flow.items.map(row => row.period.end !== "2025-12-31" ? row
      : row.metric === "free_cash_flow" ? canonical
      : row.metric === "operating_cash_flow" ? { ...row, display: { ...row.display, value: "4415.80" } }
      : row.metric === "capital_expenditures" ? { ...row, display: { ...row.display, value: "-1386.60" } } : row);
    target.cash_flow.items.push({ ...canonical, period: { type: "ttm", start: "2025-07-01", end: "2026-06-30" } });
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(payload)));
    const company = await pullDataCompany(env, "data-0000000001", 2024, 2025);
    expect(company.metrics.freeCashFlow).toContainEqual({ year: 2025, value: 3029.2 });
    const cash = company.statements.find(group => group.key === "cash")!.facts.find(row => row.key === "free_cash_flow")!;
    expect(cash.values).toContainEqual({ year: 2025, value: 3029200000 / Number(scale) });
    expect(cash.annualSources!.find(row => row.year === 2025)).toMatchObject({ value: "3029200000", unit: "USD",
      sourceDisplayUnit: "USD", displayScale: scale, sourceUrl: source });
    expect(cash.ttm).toMatchObject({ sourceDisplayUnit: "USD", displayScale: scale, canonicalValue: "3029200000" });
    const bridge = company.researchShelf!.cashFlow.yearly.find(row => row.year === 2025)!;
    expect(bridge.fcff.capitalExpenditure.companyValue).toBe(1386.6);
    expect(bridge.fcfe.shareBasedCompensation.companyValue).toBe(10);
    const html = renderToStaticMarkup(createElement(CompanyStoryHome, { company, watched: false, toggleWatch() {}, backToDiscover() {} }));
    expect(html).toContain("$3.03B");
    expect(html).not.toContain("$3.67B");
    const table = renderToStaticMarkup(createElement(StatementExplorer, { company, initialGroupKey: "cash" }));
    if (Number(scale) === 1000000) {
      expect(table).toContain("USD millions");
      expect(table).toContain("$3,029.2m");
    } else {
      expect(cash.annualSources!.find(row => row.year === 2025)!.displayUnit).toBe("USD");
      expect(table).toContain("3,029,200,000");
    }
  });

  it("keeps canonical FCF missing even when cash inputs and another statement alias exist", async () => {
    const payload = discoverPayload(), target = payload.datasets[0];
    target.cash_flow.items = target.cash_flow.items.filter(row => row.metric !== "free_cash_flow");
    target.income.items.push(item("free_cash_flow", "Supporting Free Cash Flow", 2025, 999));
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(payload)));
    const company = await pullDataCompany(env, "data-0000000001", 2024, 2025);
    expect(company.metrics.freeCashFlow).toEqual([]);
  });

  it("retains ratios, shares and per-share precision while labelling scaled derived amounts", async () => {
    const payload = discoverPayload(), cash = payload.datasets[0].cash_flow;
    cash.items.push({ ...item("unlevered_free_cash_flow", "Unlevered Free Cash Flow", 2025, 3252.5967058045, "USD"),
      display: { value: "3252.5967058045", unit: "USD", scale: 1000000, format: "number" },
      value: "3252596705.8045", unit: "USD" } as ReturnType<typeof item>);
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(payload)));
    const company = await pullDataCompany(env, "data-0000000001", 2024, 2025);
    const facts = company.statements.flatMap(group => group.facts);
    expect(facts.find(row => row.key === "unlevered_free_cash_flow")!.unit).toBe("USD millions");
    expect(facts.find(row => row.key === "effectiveTaxRate")).toMatchObject({ unit: "ratio", values: [{ year: 2024, value: .25 }, { year: 2025, value: .25 }] });
    expect(facts.find(row => row.key === "dilutedShares")).toMatchObject({ unit: "million shares", values: [{ year: 2024, value: 50 }, { year: 2025, value: 50 }] });
    expect(facts.find(row => row.key === "eps")).toMatchObject({ unit: "USD per share", values: [{ year: 2024, value: 3 }, { year: 2025, value: 3 }] });
  });
});


describe("India legacy cash headline compatibility", () => {
  it.each([false, true])("preserves the India-only fallback and rejects incompatible metadata (%s)", async incompatible => {
    const payload = discoverPayload();
    Object.assign(payload.company, { country: "India", reporting_currency: "INR" });
    Object.assign(payload.datasets[0].company, { country: "India", reporting_currency: "INR" });
    for (const statement of [payload.datasets[0].income, payload.datasets[0].balance, payload.datasets[0].cash_flow]) {
      statement.items = statement.items.filter(row => row.metric !== "free_cash_flow").map(row => ({ ...row,
        display: { ...row.display, unit: row.display.unit.replace("USD", "INR") } }));
    }
    if (incompatible) payload.datasets[0].cash_flow.items = payload.datasets[0].cash_flow.items.map(row => row.metric === "capital_expenditures"
      ? { ...row, display: { ...row.display, unit: "INR" } } : row);
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(payload)));
    const company = await pullDataCompany(env, "data-0000000001", 2024, 2025);
    expect(company.metrics.freeCashFlow).toEqual(incompatible ? [] : [{ year: 2024, value: 130 }, { year: 2025, value: 150 }]);
  });
});


describe("exact API scale and per-share metadata", () => {
  it("keeps canonical USD/shares per-share values and format-labelled per-share amounts unscaled", async () => {
    const payload = discoverPayload(), income = payload.datasets[0].income;
    const perShare = { metric: "free_cash_flow_per_share", metric_label: "Free Cash Flow Per Share", value: "5.75", unit: "USD/shares",
      display: { value: "5.75", unit: "USD/shares", format: "per_share", scale: 1 },
      period: { type: "annual", start: "2025-01-01", end: "2025-12-31" } };
    income.items.push(perShare);
    // Exercise the classification through an existing analysis key: the amount
    // normalizer must not scale source-explicit per-share observations.
    income.items = income.items.map(row => row.metric === "eps_diluted" ? { ...row,
      display: { value: "5.75", unit: "USD", format: "per_share", scale: "1" }, value: "5.75", unit: "USD/shares" } : row);
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(payload)));
    const company = await pullDataCompany(env, "data-0000000001", 2024, 2025);
    const facts = company.statements.flatMap(group => group.facts);
    expect(facts.find(row => row.key === "free_cash_flow_per_share")).toMatchObject({ unit: "USD/shares", values: [{ year: 2025, value: 5.75 }] });
    expect(company.researchShelf!.earningsAndValuation.earningsFlow.find(row => row.year === 2025)!.metrics.eps.companyValue).toBe(5.75);
    const html = renderToStaticMarkup(createElement(StatementExplorer, { company, initialGroupKey: "income" }));
    expect(html).toContain("$5.75");
  });
});


describe("invalid API display scales", () => {
  it.each(["", "0", "-1000000", "Infinity", "not-a-scale", 0, -1, Infinity])("does not label a bare currency as millions for %s", async scale => {
    const payload = discoverPayload();
    payload.datasets[0].cash_flow.items = payload.datasets[0].cash_flow.items.map(row => row.metric === "free_cash_flow" ? { ...row,
      display: { ...row.display, unit: "USD", scale } } : row);
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(payload)));
    const company = await pullDataCompany(env, "data-0000000001", 2024, 2025);
    expect(company.statements.find(group => group.key === "cash")!.facts.find(row => row.key === "free_cash_flow")!.unit).toBe("USD");
  });
});
