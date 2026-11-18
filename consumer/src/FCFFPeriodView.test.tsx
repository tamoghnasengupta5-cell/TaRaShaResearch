import { renderToStaticMarkup } from "react-dom/server";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { Company, CompanyCashConversionStory } from "./types";
const view = vi.hoisted(() => ({ mode: "annual" }));
vi.mock("react", async original => {
  const react = await original<typeof import("react")>();
  return { ...react, useState: <T,>(initial: T | (() => T)) => react.useState(
    initial === "makes-money" ? "becomes-cash" as T : initial === "annual" ? view.mode as T : initial) };
});
import { CompanyStoryHome } from "./CompanyStoryHome";
const lineage = { kind: "mapped", concept: null, derivationMethod: null, formula: null, sourceUrl: "https://www.sec.gov/filing.xml" };
function fixture(): Company {
  const years = Array.from({ length: 10 }, (_, i) => 2017 + i);
  const story: CompanyCashConversionStory = {
    status: "available", reason: null, latestFiscalYear: 2026, latestPeriodEnd: "2026-06-30",
    latestStatementFiscalYear: 2026, displayAnnualYears: [2020,2021,2022,2023,2024,2025,2026],
    reportingCurrency: "USD", displayUnit: "USD millions", years,
    bridge: [{ key: "fcff", label: "Free Cash Flow to the Firm", operation: "total", value: 123, formula: "API formula", lineage, components: [] }],
    trend: years.filter(year => year !== 2022).map(year => ({ fiscalYear: year, periodEnd: `${year}-06-30`, ebit: 200, taxRatePercent: 25, taxesOnOperatingProfit: 50, nopat: 150,
      depreciationAndAmortization: 20, workingCapitalImpact: -5, workingCapitalComponents: [], capitalExpenditure: 42,
      fcff: 123, conversionPercent: 61.5, revenue: 500, fcffRevenuePercent: 24.6 })),
    metrics: { fcff: 123, priorFiscalYear: null, priorFcff: null, conversionPercent: 61.5, priorConversionPercent: null,
      growthPercent: null, priorGrowthPercent: null, revenuePercent: 24.6, priorRevenuePercent: null, cagrPercent: null },
    summary: null, takeaway: null, methodology: "SEC API calculations", sourceUrl: null,
  };
  story.ttm = { ...story, latestFiscalYear: 2026, latestPeriodEnd: "2026-09-30", periodStart: "2025-10-01", missingInputs: [],
    bridge: [{ ...story.bridge[0], value: 456 }], metrics: { ...story.metrics!, fcff: 456 },
    workingCapitalCalculation: { status: "available", reason: null, formula: "NCWC formula", periodType: "ttm",
      priorPeriodLabel: "As of 2025-09-30", currentPeriodLabel: "As of 2026-09-30", priorFiscalYear: 2025, currentFiscalYear: 2026,
      priorPeriodEnd: "2025-09-30", currentPeriodEnd: "2026-09-30", priorNcwc: 60, currentNcwc: 55,
      priorOperatingCurrentAssets: 100, currentOperatingCurrentAssets: 110, cashEffect: 5, inputs: [] } };
  return { id: "fixture", name: "SEC Issuer", symbol: "SEC", country: "USA", exchange: "NASDAQ", currency: "USD millions", sector: "Tech", description: "Test", reportingPeriod: "FY2026", updatedAt: "2026-10-23",
    metrics: { revenue: [{ year: 2026, value: 500 }], operatingMargin: [], freeCashFlow: [], netDebt: [] }, notes: { growth: "", profitability: "", cash: "", debt: "" },
    companyStory: { cashConversion: story } };
}
const render = (company: Company) => renderToStaticMarkup(<CompanyStoryHome company={company} watched={false} toggleWatch={() => {}} backToDiscover={() => {}} />);
afterEach(() => { view.mode = "annual"; });
describe("SEC cash chapter annual and trailing periods", () => {
  it("fits seven actual annual slots plus TTM, retaining a gap without a zero bar or a connecting line", () => {
    const html = render(fixture());
    expect(html.match(/data-cash-period=/g)).toHaveLength(8);
    expect(html).toContain('data-cash-period="FY2020"');
    expect(html).toContain('data-cash-period="FY2026"');
    expect(html).toContain('data-cash-period="TTM"');
    expect(html).not.toContain('data-cash-period="FY2019"');
    expect(html).toContain('class="cash-trend-fit"');
    expect(html).not.toContain('data-horizontal-scroll');
    expect(html).toContain('FY2022: FCFF unavailable');
    expect(html.match(/class="cash-conversion-line"/g)).toHaveLength(5);
    expect(html).toContain('aria-pressed="false" title="View the trailing twelve-month cash bridge">TTM');
  });
  it("uses API TTM amounts in the selected bridge and dates in its expandable worksheet", () => {
    view.mode = "ttm";
    const c = fixture(), ttm = c.companyStory!.cashConversion!.ttm!;
    ttm.bridge[0].workingCapitalCalculation = ttm.workingCapitalCalculation;
    const html = render(c);
    expect(html).toContain('TTM through 2026-09-30');
    expect(html).toContain('456.00');
    expect(html).toContain('Opening balance');
    expect(html).toContain('As of 2025-09-30');
    expect(html).toContain('Closing balance');
    expect(html).toContain('Prior TTM comparison unavailable');
  });
  it("keeps all periods but shows no inferred numeric scales when every FCFF value is missing", () => {
    const c=fixture(), story=c.companyStory!.cashConversion!;
    story.status="unavailable"; story.trend=[]; story.metrics=null; story.bridge=[];
    story.ttm!.metrics=null;
    const html=render(c);
    const chart=html.match(/<section class="cash-trend-panel"[\s\S]*?<\/section>/)![0];
    expect(chart.match(/data-cash-period=/g)).toHaveLength(8);
    expect(chart).toContain('FCFF amounts unavailable for these periods');
    expect(chart).toContain('2026-09-30');
    expect(chart).not.toContain('$0M');
    expect(chart).not.toContain('12.0%');
    expect(chart).not.toContain('cash-chart-bar');
  });
  it("preserves a real zero FCFF value instead of treating it as missing", () => {
    const c=fixture(), story=c.companyStory!.cashConversion!;
    story.trend=[{...story.trend[0],fiscalYear:2020,periodEnd:"2020-06-30",fcff:0,conversionPercent:0}];
    story.ttm!.metrics=null;
    const html=render(c);
    const chart=html.match(/<section class="cash-trend-panel"[\s\S]*?<\/section>/)![0];
    expect(chart).toContain('class="cash-chart-bar ');
    expect(chart).toContain('FY2020 through 2020-06-30:');
    expect(chart).not.toContain('FY2020 through 2020-06-30: FCFF unavailable');
    expect(chart).not.toContain('FCFF amounts unavailable for these periods');
  });
  it("discloses broader add-backs and separates the CapEx components without calling them working capital", () => {
    const c=fixture(), story=c.companyStory!.cashConversion!;
    story.bridge.push({ ...story.bridge[0], key: "depreciation_and_amortization", label: "D&A and other filed non-cash adjustments", operation: "add", disclosure: "Includes financing amortization; pure operating D&A is unproved." });
    story.bridge.push({ ...story.bridge[0], key: "capital_expenditure", label: "Net capital investment (CapEx)", operation: "subtract",
      components: [{ key: "rebate", label: "Separate rebate proceeds", value: 7, lineage }] });
    const html=render(c);
    expect(html).toContain("Includes financing amortization; pure operating D&amp;A is unproved.");
    expect(html).toContain("Separate rebate proceeds");
    expect(html).toContain("Net capital investment</span>");
    expect(html).not.toContain("Net working-capital cash effect");
  });
});
