import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { BalanceSheetChapter, CompanyStoryHome, HowItMakesMoney, MarketPricingChapter, StockRiskChapter, WaterfallChart } from "./CompanyStoryHome";
import { StockHistoryChapter } from "./StockHistoryChapter";
import type {
  Company,
  CompanyBalanceSheetComponent,
  CompanyBalanceSheetStory,
  CompanyCostStructureLine,
  CompanyMarketPricingHistoryMetric,
  CompanyMarketPricingStory,
  CompanyStockRiskSeries,
  CompanyStockRiskStory,
  CompanyStockHistoryStory,
  StockRiskRange,
} from "./types";

function costLine(
  key: string,
  label: string,
  role: CompanyCostStructureLine["role"],
  value: number,
): CompanyCostStructureLine {
  return {
    key,
    label,
    role,
    latestValuePerHundred: value,
    history: [],
    lineage: {
      kind: null,
      concept: null,
      derivationMethod: null,
      formula: null,
      sourceUrl: null,
    },
  };
}

function barRect(markup: string, key: string, region: "positive" | "negative"): string {
  return markup.match(new RegExp(`<rect(?=[^>]*data-line-key="${key}")(?=[^>]*data-region="${region}")[^>]*>`))?.[0] ?? "";
}

describe("how-it-makes-money story", () => {
  it("starts on page 1 of 3 and retains an undetermined 100% revenue view", () => {
    const company: Company = {
      id: "undetermined",
      name: "Undetermined Co",
      symbol: "UNDT",
      sector: "Restaurants",
      description: "Example",
      currency: "US$ millions",
      reportingPeriod: "FY 2025",
      updatedAt: "1 January 2026",
      metrics: {
        revenue: [{ year: 2024, value: 237.9 }, { year: 2025, value: 282.8 }],
        operatingMargin: [],
        freeCashFlow: [],
        netDebt: [],
      },
      companyStory: {
        revenueSegments: {
          status: "undetermined",
          reason: "Segment detail is not separately disclosed.",
          latestFiscalYear: 2025,
          latestPeriodEnd: "2025-08-31",
          reportingCurrency: "USD",
          displayUnit: "USD millions",
          totalRevenue: 282.8,
          segments: [],
          summary: null,
          methodology: "Consolidated issuer-filed revenue; no segment split is estimated.",
        },
      },
      notes: { growth: "", profitability: "", cash: "", debt: "" },
    };

    const markup = renderToStaticMarkup(<HowItMakesMoney company={company} />);
    expect(markup).toContain("1 of 3");
    expect(markup).toContain('type="button" disabled="" aria-label="Previous page"');
    expect(markup).toContain("Undetermined business segment");
    expect(markup).toContain("100.0%");
    expect(markup).toContain("$282.8M");
    expect(markup).toContain("Sources &amp; derivations");
    expect(markup).not.toContain("Revenue-related adjustment entries");
    expect(markup).not.toContain("Reported vs Adjusted Revenue");
  });

  it("shows reconciled recent product/service TTM on page 1 without calling it an operating segment", () => {
    const company: Company = {
      id: "anet", name: "Arista Networks", symbol: "ANET", sector: "Technology",
      description: "Example", currency: "US$ millions", reportingPeriod: "FY 2025", updatedAt: "1 September 2026",
      metrics: { revenue: [{ year: 2025, value: 9005.7 }], operatingMargin: [], freeCashFlow: [], netDebt: [] },
      companyStory: {
        revenueSegments: { status: "undetermined", reason: "One reportable segment", latestFiscalYear: 2025,
          latestPeriodEnd: "2025-12-31", reportingCurrency: "USD", displayUnit: "USD millions",
          totalRevenue: 9005.7, segments: [], summary: null, methodology: "Issuer filing" },
        revenueOfferings: { offerings: [
          { id: "srt:ProductOrServiceAxis/Product", label: "Product", revenueByPeriod: { "2024": 5711.296, "2025": 7139.12, TTM: 8923.9 }, sourceByPeriod: { TTM: "https://example.test/10q" } },
          { id: "srt:ProductOrServiceAxis/Service", label: "Service", revenueByPeriod: { "2024": 1034.816, "2025": 1293.52, TTM: 1616.9 }, sourceByPeriod: { TTM: "https://example.test/10q" } },
        ], offeringAxis: "srt:ProductOrServiceAxis", ttm: { periodEnd: "2026-06-30", revenue: 10540.8, priorYearRevenue: 7950.9 },
        methodology: "Filed categories" },
      },
      notes: { growth: "", profitability: "", cash: "", debt: "" },
    };
    const markup = renderToStaticMarkup(<HowItMakesMoney company={company} />);
    expect(markup).toContain("Revenue breakdown for TTM");
    expect(markup).toContain("$10.54B");
    expect(markup).toContain("$8.92B");
    expect(markup).toContain("$1.62B");
    expect(markup.match(/25\.0%/g)).toHaveLength(4);
    expect(markup).toContain("Product / Service");
    expect(markup).not.toContain("Undetermined business segment");
    expect(markup).toContain("Growth uses TTM, the prior comparable, and the prior-to-prior comparable values");
    expect(markup).toContain("not necessarily reportable operating segments");
  });

  it("shows any reconciled issuer revenue categories for a newer TTM", () => {
    const company: Company = {
      id: "generic-categories", name: "Generic Categories Co", symbol: "GCC", sector: "Technology",
      description: "Example", currency: "US$ millions", reportingPeriod: "FY 2026", updatedAt: "1 November 2026",
      metrics: { revenue: [{ year: 2026, value: 500 }], operatingMargin: [], freeCashFlow: [], netDebt: [] },
      companyStory: {
        revenueSegments: { status: "undetermined", reason: "No safe annual segment split", latestFiscalYear: 2026,
          latestPeriodEnd: "2026-01-31", reportingCurrency: "USD", displayUnit: "USD millions",
          totalRevenue: 500, segments: [], summary: null, methodology: "Issuer filing" },
        revenueOfferings: { offerings: [
          { id: "business:x:OperatingSegmentsAxis/Cloud", label: "Cloud", revenueByPeriod: { TTM: 420 }, sourceByPeriod: { TTM: "https://example.test/10q" } },
          { id: "business:x:OperatingSegmentsAxis/Support", label: "Support", revenueByPeriod: { TTM: 180 }, sourceByPeriod: { TTM: "https://example.test/10q" } },
        ], offeringAxis: "business:x:OperatingSegmentsAxis", annualTotals: { "2026": 500 },
        annualPeriodEnds: { "2026": "2026-01-31" },
        ttm: { periodEnd: "2026-10-31", revenue: 600, priorYearRevenue: 480 }, methodology: "Filed categories" },
      },
      notes: { growth: "", profitability: "", cash: "", debt: "" },
    };

    const markup = renderToStaticMarkup(<CompanyStoryHome company={company} watched={false} toggleWatch={() => {}} backToDiscover={() => {}} />);

    expect(markup).toContain("Revenue (TTM to 2026-10-31)");
    expect(markup).toContain("$600M");
    expect(markup).toContain("Revenue breakdown for TTM");
    expect(markup).toContain("Reported Revenue Category");
    expect(markup).toContain("$420M");
    expect(markup).toContain("$180M");
    expect(markup).toContain("issuer-reported revenue categories reconcile");
    expect(markup).not.toContain("Undetermined business segment");
  });

  it("does not silently show a prior-year headline value for the current reporting year", () => {
    const company: Company = {
      id: "period-aligned",
      name: "Period Aligned Co",
      symbol: "PAC",
      sector: "Networking",
      description: "Example",
      currency: "US$ million",
      reportingPeriod: "FY 2025",
      updatedAt: "1 January 2026",
      metrics: {
        revenue: [{ year: 2024, value: 7000 }, { year: 2025, value: 9000 }],
        operatingMargin: [{ year: 2025, value: 42.8 }],
        freeCashFlow: [{ year: 2025, value: 4250 }],
        netDebt: [{ year: 2024, value: -8266 }],
      },
      notes: { growth: "", profitability: "", cash: "", debt: "" },
    };

    const markup = renderToStaticMarkup(<CompanyStoryHome company={company} watched={false} toggleWatch={() => {}} backToDiscover={() => {}} />);

    expect(markup).toContain("Net Debt (FY2025)");
    expect(markup).toContain("Not available");
    expect(markup).not.toContain("8.27B");
    expect(markup).toContain("Operating Margin (FY2025)");
    expect(markup).toContain("Free Cash Flow (FY2025)");
  });
});

describe("cost-structure waterfall", () => {
  it("keeps profitable economics above an always-visible breakeven line", () => {
    const markup = renderToStaticMarkup(<WaterfallChart fiscalYear={2026} lines={[
      costLine("revenue", "Revenue", "revenue", 100),
      costLine("cost_of_revenue", "Cost of Revenue", "expense", -32.06),
      costLine("gross_profit", "Gross Profit", "subtotal", 67.94),
      costLine("research_development", "Research & Development", "expense", -10.72),
      costLine("selling_and_marketing", "Selling & Marketing", "expense", -8.05),
      costLine("general_and_admin", "General & Administrative", "expense", -2.4),
      costLine("operating_income", "Operating Profit", "subtotal", 46.78),
    ]} />);

    expect(markup).toContain('class="waterfall-zero"');
    expect(markup).not.toContain("$0 · BREAKEVEN");
    expect(markup).toContain('class="waterfall-negative-zone"');
    expect(markup).not.toContain('data-region="negative"');
    expect(markup).not.toContain('fill="url(#waterfall-slate)"');
    expect(markup).not.toContain('fill="url(#waterfall-bronze)"');
  });

  it("splits a crossing expense and keeps two distinct colors below breakeven", () => {
    const markup = renderToStaticMarkup(<WaterfallChart fiscalYear={2026} lines={[
      costLine("revenue", "Revenue", "revenue", 100),
      costLine("cost_of_revenue", "Cost of Revenue", "expense", -32.83),
      costLine("gross_profit", "Gross Profit", "subtotal", 67.17),
      costLine("research_development", "Research & Development", "expense", -42.05),
      costLine("selling_and_marketing", "Selling & Marketing", "expense", -44.03),
      costLine("general_and_admin", "General & Administrative", "expense", -11.74),
      costLine("operating_income", "Operating Profit", "subtotal", -30.64),
    ]} />);

    expect(barRect(markup, "selling_and_marketing", "positive")).toContain('fill="url(#waterfall-red)"');
    expect(barRect(markup, "selling_and_marketing", "negative")).toContain('fill="url(#waterfall-slate)"');
    expect(barRect(markup, "general_and_admin", "negative")).toContain('fill="url(#waterfall-slate)"');
    expect(barRect(markup, "operating_income", "negative")).toContain('fill="url(#waterfall-bronze)"');
    expect(barRect(markup, "general_and_admin", "negative")).not.toContain("waterfall-bronze");
  });
});

function balanceComponent(key: string, label: string, value: number | null, percentageOfBase: number | null, children: CompanyBalanceSheetComponent["children"] = []): CompanyBalanceSheetComponent {
  return {
    key,
    label,
    value,
    percentageOfBase,
    trend: value === null ? [] : [
      { fiscalYear: 2024, value: value * .9, percentageOfBase: percentageOfBase === null ? null : percentageOfBase - 1 },
      { fiscalYear: 2025, value, percentageOfBase },
    ],
    children,
  };
}

describe("balance-sheet story", () => {
  it("starts with the accounting equation and keeps every requested story branch visible", () => {
    const story: CompanyBalanceSheetStory = {
      status: "available",
      reason: null,
      latestFiscalYear: 2025,
      latestPeriodEnd: "2025-12-31",
      reportingCurrency: "USD",
      displayUnit: "USD millions",
      years: [2024, 2025],
      equation: { assets: 1000, liabilities: 600, shareholdersEquity: 400 },
      assets: [
        balanceComponent("cash", "Cash & cash equivalents", 150, 15),
        balanceComponent("operating_current_assets", "Receivables, inventory & prepaids", 200, 20),
        balanceComponent("property_plant_equipment", "Property, plant & equipment", 300, 30),
        balanceComponent("goodwill", "Goodwill", 100, 10),
        balanceComponent("other_intangible_assets", "Other intangible assets", null, null),
      ],
      liabilities: [
        balanceComponent("unearned_revenue", "Unearned revenue", 90, 15, [
          { key: "current", label: "Current", value: 60, percentageOfBase: 10, interestRatePercent: null },
          { key: "long", label: "Long term", value: 30, percentageOfBase: 5, interestRatePercent: null },
        ]),
        balanceComponent("borrowings", "Borrowings", 250, 41.7, [
          { key: "short", label: "Short-term borrowings", value: 50, percentageOfBase: 8.3, interestRatePercent: 5.1 },
          { key: "long", label: "Long-term borrowings", value: 180, percentageOfBase: 30, interestRatePercent: 4.2 },
          { key: "lease", label: "Long-term leases", value: 20, percentageOfBase: 3.3, interestRatePercent: 3.9 },
        ]),
        balanceComponent("accounts_payable", "Accounts payable", 80, 13.3),
      ],
      shareholdersEquity: [
        balanceComponent("common_stock", "Common stock", 10, 2.5),
        balanceComponent("additional_paid_in_capital", "Additional paid-in capital", 150, 37.5),
        balanceComponent("retained_earnings", "Retained earnings", 250, 62.5),
        balanceComponent("comprehensive_income", "Comprehensive income", -10, -2.5),
      ],
      health: { totalCash: 150, totalDebt: 250, netCashDebt: -100, netDebtToEbitda: .7, interestCoverage: 8.2, weightedAverageCostOfDebt: 4.3, weightedAverageCostOfDebtFiscalYear: 2025, weightedAverageCostOfDebtNote: "FY2025 interest expense ÷ average borrowings" },
      summary: "Debt is covered comfortably by earnings.",
      methodology: "Reported annual facts only.",
      sourceUrl: "https://www.sec.gov/example",
    };
    const company: Company = {
      id: "alpha",
      name: "Alpha Systems",
      symbol: "ALPHA",
      sector: "Software",
      description: "Example",
      currency: "US$ million",
      reportingPeriod: "FY 2025",
      updatedAt: "1 January 2026",
      metrics: { revenue: [], operatingMargin: [], freeCashFlow: [], netDebt: [] },
      companyStory: { balanceSheet: story } as Company["companyStory"],
      notes: { growth: "", profitability: "", cash: "", debt: "" },
    };

    const markup = renderToStaticMarkup(<BalanceSheetChapter company={company} />);

    expect(markup).toContain("Assets = Liabilities + Shareholders’ Equity");
    expect(markup).toContain("Assets are funded by liabilities and shareholders’ equity.");
    expect(markup).not.toContain("Plain-English read");
    expect(markup).toContain("Net cash (debt)");
    expect(markup).toContain("Weighted avg. cost of debt");
    expect(markup.indexOf("balance-health-metrics")).toBeLessThan(markup.indexOf("balance-equation-card"));
    expect(markup).toContain('class="balance-equation-funding-item liabilities"');
    expect(markup).toContain('class="balance-equation-funding-item equity"');
    expect(markup).toContain('role="tablist"');
    expect(markup).toContain('id="balance-assets-tab"');
    expect(markup).toContain('aria-selected="true"');
    expect(markup).toContain("Unearned revenue");
    expect(markup).toContain("Short-term borrowings");
    expect(markup).toContain("5.10% rate");
    expect(markup).toContain("Other intangible assets");
    expect(markup).toContain("Not separately reported");
    expect(markup).toContain("What this means");
    expect(markup).toContain("It is not the company’s stock-market value.");
    expect(markup).toContain("FY24");
    expect(markup).toContain("$135M");
    expect(markup).toContain('class="balance-bar latest"');
    expect(markup).not.toContain('<details class="balance-component-details" open');
    expect(markup).toContain("TaRaSha balance-sheet read");
  });
});

function riskSeries(key: CompanyStockRiskSeries["key"], symbol: string): CompanyStockRiskSeries {
  const company = key === "company";
  return {
    key,
    name: company ? "Alpha Systems" : key === "sector" ? "Technology sector (XLK)" : "S&P 500",
    symbol,
    role: company ? "Company" : "Benchmark",
    sourceUrl: `https://finance.yahoo.com/quote/${symbol}/history/`,
    startDate: "2021-08-20",
    endDate: "2026-08-20",
    tradingDays: 1258,
    monthlyObservations: 60,
    annualizedVolatilityPercent: company ? 19.2 : key === "sector" ? 22.6 : 16.6,
    betaToMarket: company ? .87 : key === "sector" ? 1.18 : 1,
    maximumDrawdownPercent: company ? -27.1 : key === "sector" ? -38.9 : -33.9,
    worstMonthlyReturnPercent: company ? -16.4 : key === "sector" ? -23.7 : -20,
    bestMonthlyReturnPercent: company ? 15.3 : key === "sector" ? 19.8 : 13.1,
    positiveMonthsPercent: company ? 60 : 55,
    negativeMonthsPercent: company ? 40 : 45,
    flatMonthsPercent: 0,
    drawdown: [{ date: "2021-08-20", value: 0 }, { date: "2022-10-20", value: company ? -27.1 : -35 }, { date: "2026-08-20", value: -4 }],
    rollingVolatility: [{ date: "2021-08-20", value: 20 }, { date: "2022-10-20", value: company ? 31 : 38 }, { date: "2026-08-20", value: company ? 19.2 : 22.6 }],
    monthlyDistribution: [
      { key: "lt_15", label: "< −15%", months: 1, percentage: 1.7 },
      { key: "neg_15_10", label: "−15% to −10%", months: 4, percentage: 6.7 },
      { key: "neg_10_5", label: "−10% to −5%", months: 9, percentage: 15 },
      { key: "neg_5_0", label: "−5% to 0%", months: 10, percentage: 16.6 },
      { key: "pos_0_5", label: "0% to 5%", months: 24, percentage: 40 },
      { key: "pos_5_10", label: "5% to 10%", months: 9, percentage: 15 },
      { key: "gt_10", label: "> 10%", months: 3, percentage: 5 },
    ],
  };
}

function riskStory(): CompanyStockRiskStory {
  const ranges: StockRiskRange[] = ["1Y", "3Y", "5Y", "7Y", "10Y"];
  const series = [riskSeries("company", "ALPHA"), riskSeries("sector", "XLK"), riskSeries("market", "^GSPC")];
  return {
    status: "available",
    reason: null,
    symbol: "ALPHA",
    asOf: "2026-08-20",
    availableRanges: ranges,
    defaultRange: "5Y",
    sectorBenchmark: { name: "Technology sector (XLK)", symbol: "XLK", selectionBasis: "Mapped from SEC SIC 3571" },
    periods: Object.fromEntries(ranges.map((range) => [range, {
      range,
      startDate: "2021-08-20",
      endDate: "2026-08-20",
      series,
      riskSummary: "Alpha was less volatile than the technology sector.",
      drawdownSummary: "Alpha's drawdown was shallower than both benchmarks.",
      positiveMonthsSummary: "Alpha had positive returns in 60% of months.",
    }])),
    source: { name: "Yahoo Finance chart service", sourceRole: "Zero-cost market history", delayed: true, persisted: false, cost: "$0", priceBasis: "Daily adjusted close" },
    methodology: "Daily simple returns; annualized volatility is sample standard deviation × √252.",
    quality: { companyObservations: 2780, marketObservations: 2780, sectorObservations: 2780, alignment: "Exact shared trading dates", warnings: [] },
  };
}

describe("stock-risk story", () => {
  it("matches the approved chapter structure and exposes sources and derivations", () => {
    const company: Company = {
      id: "alpha",
      name: "Alpha Systems",
      symbol: "ALPHA",
      sector: "Software",
      description: "Example",
      currency: "US$ million",
      reportingPeriod: "FY 2025",
      updatedAt: "1 January 2026",
      metrics: { revenue: [], operatingMargin: [], freeCashFlow: [], netDebt: [] },
      companyStory: { stockRisk: riskStory() },
      notes: { growth: "", profitability: "", cash: "", debt: "" },
    };

    const markup = renderToStaticMarkup(<StockRiskChapter company={company} />);

    expect(markup).toContain("How the stock behaves");
    expect(markup).toContain("Risk Characteristics");
    expect(markup).toContain("1Y Volatility (Annualized)");
    expect(markup).toContain("Maximum Drawdown");
    expect(markup).toContain("Drawdown Analysis");
    expect(markup).toContain("Rolling Volatility (Annualized)");
    expect(markup).toContain("Monthly Returns Distribution (7Y)");
    expect(markup).toContain("Up vs Down Months (5Y)");
    expect(markup).toContain("Past performance is not indicative of future results.");
    expect(markup).toContain("Source:</b> TaRaShaData");
    expect(markup).toContain("Derivation:</b>");
    expect(markup).toContain("Mapped from SEC SIC 3571");
    expect(markup).toContain('aria-label="Maximize Risk Characteristics"');
    expect(markup.match(/aria-label="Maximize /g)).toHaveLength(5);
    expect(markup.match(/data-chart-hit="risk-line"/g)).toHaveLength(2);
    expect(markup).toContain('data-chart-hit="monthly-distribution"');
    expect(markup).toContain('data-chart-hit="up-months"');
    expect(markup).not.toContain("$0");
    expect(markup).not.toContain("Historical stock price");
  });

  it("does not relabel another range as a seven-year monthly distribution", () => {
    const story = riskStory();
    delete story.periods["7Y"];
    const company: Company = {
      id: "alpha",
      name: "Alpha Systems",
      symbol: "ALPHA",
      sector: "Software",
      description: "Example",
      currency: "US$ million",
      reportingPeriod: "FY 2025",
      updatedAt: "1 January 2026",
      metrics: { revenue: [], operatingMargin: [], freeCashFlow: [], netDebt: [] },
      companyStory: { stockRisk: story },
      notes: { growth: "", profitability: "", cash: "", debt: "" },
    };

    const markup = renderToStaticMarkup(<StockRiskChapter company={company} />);

    expect(markup).toContain("Monthly Returns Distribution (7Y)");
    expect(markup).toContain("7-year history required");
    expect(markup).toContain("Seven years of aligned company and benchmark monthly returns are required.");
  });
});

function marketPricingStory(): CompanyMarketPricingStory {
  const historyMetrics: CompanyMarketPricingHistoryMetric[] = [
    { key: "pe", label: "P/E", current: 25, median: 22, low: 18, high: 30, percentile: 70, unit: "multiple", observations: 5 },
    { key: "ev_ebitda", label: "EV / EBITDA", current: 18, median: 16, low: 12, high: 21, percentile: 68, unit: "multiple", observations: 5 },
    { key: "fcf_yield", label: "FCF Yield", current: 2.8, median: 3.4, low: 2.2, high: 4.5, percentile: 32, unit: "percent", observations: 5 },
  ];
  return {
    status: "available",
    reason: null,
    symbol: "ALPHA",
    currency: "USD",
    asOf: "2026-08-25T20:00:00+00:00",
    marketState: "CLOSED",
    currentPrice: 100,
    currentMetrics: [
      { key: "pe", label: "P/E", shortLabel: "P/E", current: 25, unit: "multiple", basis: "TTM" },
      { key: "forward_pe", label: "Forward P/E", shortLabel: "Forward P/E", current: 21, unit: "multiple", basis: "Third-party forward estimate" },
      { key: "ev_ebitda", label: "EV / EBITDA", shortLabel: "EV / EBITDA", current: 18, unit: "multiple", basis: "TTM" },
      { key: "fcf_yield", label: "FCF Yield", shortLabel: "FCF Yield", current: 2.8, unit: "percent", basis: "TTM" },
    ],
    windows: {
      "5Y": { key: "5Y", years: 5, startDate: "2022-06-30", endDate: "2026-06-30", observations: 5, metrics: historyMetrics },
      "10Y": { key: "10Y", years: 10, startDate: "2017-06-30", endDate: "2026-06-30", observations: 10, metrics: historyMetrics.map((metric) => ({ ...metric, observations: 10 })) },
    },
    valuationRead: "Alpha trades above its historical midpoint across two of three covered multiples.",
    impliedExpectations: [
      { key: "revenue_growth", label: "Implied Revenue Growth", value: 8.2, detail: "5Y normalization shorthand." },
      { key: "fcf_growth", label: "Implied FCF Growth", value: 4.0, detail: "5Y normalization shorthand." },
      { key: "operating_margin", label: "Implied Operating Margin", value: 32, detail: "Historical multiple shorthand." },
      { key: "discount_rate", label: "Implied Discount Rate", value: 4.8, detail: "FCF yield plus terminal growth." },
    ],
    peerFramework: "SEC SIC 3571 · Electronic Computers",
    peers: [
      { cik: "1", name: "Alpha Systems", symbol: "ALPHA", isCompany: true, price: 100, pe: 25, evEbitda: 18, fcfYield: 2.8, basis: "TTM" },
      { cik: "2", name: "Beta Systems", symbol: "BETA", isCompany: false, price: 80, pe: 20, evEbitda: 15, fcfYield: 3.5, basis: "TTM" },
    ],
    treasuryComparison: { fcfYield: 2.8, treasuryYield: 4.2, spread: -1.4, treasuryAsOf: "2026-08-25", sourceName: "FRED DGS10", sourceUrl: "https://fred.stlouisfed.org/series/DGS10", cost: "$0" },
    analystEstimates: { status: "available", reason: null, baseCase: 120, bullCase: 150, bearCase: 80, analystCount: 24, currentPrice: 100, currentVsBasePercent: -16.67, sourceName: "Yahoo Finance analyst consensus", sourceUrl: "https://finance.yahoo.com/quote/ALPHA/analyst-insights/", cost: "$0" },
    takeaway: "The valuation premium leaves less room for execution misses.",
    source: { financials: "TaRaShaData normalized issuer filings", peerFramework: "TaRaShaData SEC-SIC company universe", marketName: "Yahoo Finance chart service", marketRole: "Delayed display", marketUrl: "https://finance.yahoo.com/quote/ALPHA/history/", cost: "$0", persisted: false, priceBasis: "Daily close" },
    methodology: "Current price × diluted shares; enterprise value adds debt and subtracts cash.",
    quality: { warnings: [], historyObservations: 10, peerQuotes: 2, analystConsensusAvailable: true },
  };
}

describe("market-pricing story", () => {
  it("matches the approved Section 06 hierarchy and replaces DCF with analyst scenarios", () => {
    const company: Company = {
      id: "alpha",
      name: "Alpha Systems",
      symbol: "ALPHA",
      sector: "Software",
      description: "Example",
      currency: "US$ million",
      reportingPeriod: "FY 2026",
      updatedAt: "25 August 2026",
      metrics: { revenue: [], operatingMargin: [], freeCashFlow: [], netDebt: [] },
      companyStory: { marketPricing: marketPricingStory() },
      notes: { growth: "", profitability: "", cash: "", debt: "" },
    };

    const markup = renderToStaticMarkup(<MarketPricingChapter company={company} />);

    expect(markup).toContain("What the market is pricing");
    expect(markup).toContain("Valuation Summary");
    expect(markup).toContain("Current vs 5-Year History");
    expect(markup).toContain("Implied Expectations");
    expect(markup).toContain("Peer Comparison");
    expect(markup).toContain("FCF Yield vs 10Y U.S. Treasury");
    expect(markup).toContain("Analysts Estimate");
    expect(markup).toContain("Base Case Value");
    expect(markup).toContain("Bull Case Value");
    expect(markup).toContain("Bear Case Value");
    expect(markup).toContain("Current vs. Base Case");
    expect(markup).toContain("TaRaSha Takeaway");
    expect(markup).toContain("Sources &amp; derivations");
    expect(markup).toContain("SEC SIC 3571");
    expect(markup).toContain('aria-label="Maximize Valuation Summary"');
    expect(markup.match(/aria-label="Maximize /g)).toHaveLength(6);
    expect(markup).toContain('data-chart-hit="valuation-range"');
    expect(markup).toContain('data-chart-hit="fcf-yield"');
    expect(markup).toContain('data-chart-hit="treasury-yield"');
    expect(markup).not.toContain("$0");
    expect(markup).not.toContain("DCF Value Estimate");
  });
});

function stockHistoryStory(): CompanyStockHistoryStory {
  const prices = [100, 130, 170, 220, 260, 300].map((close, index) => ({
    date: `${2021 + index}-08-20`,
    close,
    adjustedClose: close,
    volume: 1_000_000 + index * 100_000,
  }));
  const fundamental = (values: number[], unit = "USD millions") => ({
    label: "Metric",
    points: values.map((value, index) => ({
      date: `${2021 + index}-06-30`,
      fiscalYear: 2021 + index,
      value,
      sourceUrl: "https://www.sec.gov/example",
      unit,
    })),
  });
  return {
    status: "available",
    reason: null,
    symbol: "ALPHA",
    currency: "USD",
    asOf: "2026-08-20T20:00:00Z",
    marketState: "CLOSED",
    availableRanges: ["1Y", "3Y", "5Y", "Max"],
    defaultRange: "5Y",
    prices,
    fundamentals: {
      revenue: fundamental([1000, 1100, 1250, 1500, 1800, 2100]),
      eps: fundamental([2, 2.2, 2.8, 3.4, 4.1, 5], "USD per share"),
      freeCashFlow: fundamental([150, 165, 210, 260, 330, 400]),
    },
    events: [{
      date: "2025-08-01",
      type: "earnings",
      title: "Annual results filed",
      detail: "10-K for period ended 2025-06-30",
      sourceUrl: "https://www.sec.gov/example",
    }],
    performance: {
      "5Y": {
        range: "5Y",
        startDate: "2021-08-20",
        endDate: "2026-08-20",
        priceChangePercent: 200,
        totalReturnPercent: 215.8,
        cagrPercent: 24.6,
        sp500TotalReturnPercent: 80,
        high: 300,
        low: 100,
      },
    },
    glance: {
      currentPrice: 300,
      marketCap: 300_000_000_000,
      dilutedShares: 1_000_000_000,
      averageDailyVolume3m: 1_500_000,
    },
    strategy: {
      status: "available",
      reason: null,
      points: [
        "We will continue to invest in artificial intelligence infrastructure.",
        "Our strategic priority is to expand distribution and customer value.",
      ],
      sourceName: "Alpha management commentary in SEC-filed 10-K",
      sourceDate: "2026-07-29",
      sourceUrl: "https://www.sec.gov/example",
      cost: "$0",
      methodology: "Source-verbatim management statements only.",
    },
    source: {
      financials: "TaRaShaData normalized issuer filings",
      filings: "TaRaShaData stored SEC filing metadata",
      marketName: "Yahoo Finance chart service",
      marketRole: "Delayed display",
      marketUrl: "https://finance.yahoo.com/quote/ALPHA/history/",
      benchmarkUrl: "https://finance.yahoo.com/quote/%5EGSPC/history/",
      cost: "$0",
      persisted: false,
      priceBasis: "Price uses close; total return uses adjusted close.",
    },
    methodology: "Revenue and EPS are reported; FCF equals operating cash flow less capex.",
    quality: {
      warnings: [],
      rawPriceObservations: 1258,
      displayPriceObservations: 800,
      fundamentalYears: [2021, 2022, 2023, 2024, 2025, 2026],
    },
  };
}

describe("stock-history story", () => {
  it("matches Section 07 and replaces the multiple-expansion label with sourced strategy", () => {
    const company: Company = {
      id: "alpha",
      name: "Alpha Systems",
      symbol: "ALPHA",
      sector: "Software",
      description: "Example",
      currency: "US$ million",
      reportingPeriod: "FY 2026",
      updatedAt: "25 August 2026",
      metrics: { revenue: [], operatingMargin: [], freeCashFlow: [], netDebt: [] },
      companyStory: { stockHistory: stockHistoryStory() },
      notes: { growth: "", profitability: "", cash: "", debt: "" },
    };

    const markup = renderToStaticMarkup(<StockHistoryChapter company={company} />);

    expect(markup).toContain("How the stock got here");
    expect(markup).toContain("Total Return");
    expect(markup).toContain("Performance summary");
    expect(markup).toContain("Price performance vs key fundamentals");
    expect(markup).toContain("Management’s projected strategy");
    expect(markup).toContain("continue to invest in artificial intelligence");
    expect(markup).toContain("$300.0B");
    expect(markup).toContain("1.00B");
    expect(markup).toContain("1.5M");
    expect(markup).toContain("Sources &amp; derivations");
    expect(markup).toContain("Past performance is not indicative of future results.");
    expect(markup).toContain('aria-label="Maximize Historical performance chart"');
    expect(markup).toContain('aria-label="Maximize Price performance vs key fundamentals chart"');
    expect(markup.match(/aria-label="Maximize /g)).toHaveLength(2);
    expect(markup).toContain('data-chart-hit="stock-history"');
    expect(markup).toContain('data-chart-hit="indexed-fundamentals"');
    expect(markup).not.toContain("$0");
    expect(markup).not.toContain("Stock Performance has outpaced");
  });
});
