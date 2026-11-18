import { describe, expect, it, vi, afterEach } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import { BalanceSheetChapter, CompanyStoryHome, HowItMakesMoney } from "./CompanyStoryHome";
import { revenueBreakdownCategories } from "./revenueBreakdown";
import { RevenueOfferingHistory } from "./RevenueOfferingHistory";
import { fetchDataCompanyLogo, revenueOfferingStory } from "../functions/dataProvider";
import type { Company } from "./types";

const display = (value: string) => ({ value, unit: "INR millions" });
function fixture(withBridge: boolean): Company {
  return { id: "data-IN:TEST", country: "India", exchange: "NSE", currency: "INR millions", metrics: { revenue: [{ year: 2026, value: 1000 }] }, companyStory: {
    revenueOfferings: revenueOfferingStory({
      annual_totals: { "2026": { period_end: "2026-03-31", revenue_display: display("1000") } },
      offerings: [], ttm: { period_end: "2026-06-30", revenue_display: display("1200"), prior_year_revenue_display: display("950") },
      reported_segment_reconciliation: withBridge ? {
        history: { "2026": {
          period_end: "2026-03-31", revenue_display: display("1000"),
          items: [{ id: "wires", label: "Wires and cables", role: "segment", revenue_display: display("1100"), sources: [{ source_url: "https://nsearchives.nseindia.com/annual.xml" }] },
            { id: "eliminations", label: "Intersegment eliminations", role: "elimination", revenue_display: display("-100"), sources: [] }],
        } },
        period_end: "2026-06-30", basis: "Reported revenue with intersegment sales", methodology: "FY + YTD − prior YTD", revenue_display: display("1200"),
        items: [{ id: "wires", label: "Wires and cables", role: "segment", revenue_display: display("1300"), sources: [{ source_url: "https://nsearchives.nseindia.com/test.xml" }] },
          { id: "eliminations", label: "Intersegment eliminations", role: "elimination", revenue_display: display("-100"), sources: [] }],
      } : null,
    }),
  } } as Company;
}
afterEach(() => vi.unstubAllGlobals());
describe("Indian TTM revenue", () => {
  it("shows newer TTM even when a product allocation is unavailable", () => {
    const html = renderToStaticMarkup(<HowItMakesMoney company={fixture(false)} />);
    expect(html).toContain("Revenue breakdown for TTM");
    expect(html).toContain("1.20B");
    expect(html).not.toContain("Revenue breakdown for FY2026");
    expect(html).toContain("comparable TTM breakdown is unavailable from the verified source data");
    expect(html).not.toContain("not disclosed");
  });
  it("shows signed, source-linked TTM reconciliation on both revenue pages", () => {
    for (const View of [HowItMakesMoney, RevenueOfferingHistory]) {
      const html = renderToStaticMarkup(<View company={fixture(true)} />);
      expect(html).toContain("Wires and cables");
      expect(html).toContain("Intersegment eliminations");
      expect(html).toContain("−₹100M");
      expect(html).toContain("₹1.20B");
      expect(html).toContain("https://nsearchives.nseindia.com/test.xml");
      expect(html).not.toContain("segment-reconciliation");
    }
  });
  it("uses the shared donut and five-column growth table for signed Indian revenue", () => {
    const html = renderToStaticMarkup(<HowItMakesMoney company={fixture(true)} />);
    expect(html).toContain('class="revenue-story-grid ');
    expect(html).toContain('class="segment-donut-wrap"');
    for (const heading of ["Business Segment", "% of Total", "YoY Growth", "3Y CAGR"]) expect(html).toContain(heading);
    expect(html).toContain("before deductions");
    expect(html).not.toMatch(/stroke-dasharray="-/);
    expect(html).not.toContain("NaN");
  });
  it("uses the shared historical chart and cards, with signed deductions and one source basis", () => {
    const company = fixture(true);
    company.companyStory!.revenueOfferings!.offerings = [{ id: "external-product", label: "External product only", revenueByPeriod: { "2026": 1000 } }];
    const categories = revenueBreakdownCategories(company);
    expect(categories[0].revenueByPeriod).toEqual({ TTM: 1300, "2026": 1100 });
    expect(categories[1].revenueByPeriod).toEqual({ TTM: -100, "2026": -100 });
    const html = renderToStaticMarkup(<RevenueOfferingHistory company={company} />);
    expect(html).toContain('class="offering-chart-scroll"');
    expect(html).toContain('class="revenue-adjustment-grid"');
    expect(html).toContain("https://nsearchives.nseindia.com/annual.xml");
    expect(html).not.toContain("External product only");
    expect(html).not.toContain("unsafe-composition");
    expect(html).not.toMatch(/height="-/);
    expect(html).not.toContain("NaN");
  });
  it("does not substitute annual revenue for a missing prior TTM comparison", () => {
    const html = renderToStaticMarkup(<HowItMakesMoney company={fixture(true)} />);
    expect(html).not.toContain("18.2%");
    const company = fixture(true);
    company.companyStory!.revenueOfferings!.segmentReconciliation!.items[0].yoyGrowthPercent = 7.25;
    expect(renderToStaticMarkup(<HowItMakesMoney company={company} />)).toContain("7.3%");
  });
  it("uses the latest annual segment partition and its annual denominator when TTM cannot be partitioned", () => {
    const company = fixture(true);
    const reconciliation = company.companyStory!.revenueOfferings!.segmentReconciliation!;
    reconciliation.periodType = "annual";
    reconciliation.fiscalYear = 2026;
    reconciliation.periodEnd = "2026-03-31";
    reconciliation.totalRevenue = 1000;
    reconciliation.items = reconciliation.history!["2026"].items;
    const categories = revenueBreakdownCategories(company);
    expect(categories[0].revenueByPeriod).toEqual({ "2026": 1100 });
    expect(categories[0].revenueByPeriod).not.toHaveProperty("TTM");
    const html = renderToStaticMarkup(<HowItMakesMoney company={company} />);
    expect(html).toContain("Revenue breakdown for FY2026");
    expect(html).toContain("₹1.00B");
    expect(html).toContain("110.0%");
    expect(html).toContain("Wires and cables");
    expect(html).toContain("https://nsearchives.nseindia.com/annual.xml");
    expect(html).not.toContain("₹1.20B");
    expect(html).not.toContain("Revenue breakdown for TTM");
    expect(html).toContain('class="segment-donut-wrap"');
    const historyHtml = renderToStaticMarkup(<RevenueOfferingHistory company={company} />);
    expect(historyHtml).toContain("Composition unavailable");
    expect(historyHtml).not.toContain("Composition withheld");
  });
  it("preserves backend-comparable annual segment growth and does not derive across reporting changes", () => {
    const company = fixture(true);
    company.companyStory!.revenueOfferings!.ttm = undefined;
    const reconciliation = company.companyStory!.revenueOfferings!.segmentReconciliation!;
    reconciliation.periodType = "annual";
    reconciliation.fiscalYear = 2026;
    reconciliation.items = reconciliation.history!["2026"].items;
    reconciliation.history!["2025"] = { periodEnd: "2025-03-31", totalRevenue: 600,
      items: [{ id: "wires", label: "Wires and cables", role: "segment", revenue: 600, sourceUrls: [] }],
    };
    expect(renderToStaticMarkup(<HowItMakesMoney company={company} />)).not.toContain("83.3%");
    reconciliation.history!["2026"].items[0].yoyGrowthPercent = 12.5;
    expect(renderToStaticMarkup(<HowItMakesMoney company={company} />)).toContain("12.5%");
  });
  it("does not relabel an older annual partition as the latest revenue year", () => {
    const company = fixture(true);
    company.companyStory!.revenueOfferings!.ttm = undefined;
    const reconciliation = company.companyStory!.revenueOfferings!.segmentReconciliation!;
    reconciliation.periodType = "annual";
    reconciliation.fiscalYear = 2025;
    reconciliation.history!["2025"] = reconciliation.history!["2026"];
    delete reconciliation.history!["2026"];
    company.companyStory!.revenueSegments = { status: "available", latestFiscalYear: 2025, totalRevenue: 900,
      segments: [{ member: "old", name: "Old partition", latestRevenue: 900, percentageOfTotal: 100 }],
    } as Company["companyStory"] extends { revenueSegments?: infer R } ? R : never;
    const html = renderToStaticMarkup(<HowItMakesMoney company={company} />);
    expect(html).toContain("Revenue breakdown for FY2026");
    expect(html).toContain("₹1.00B");
    expect(html).toContain("Undetermined business segment");
    expect(html).not.toContain("Old partition");
    expect(html).not.toContain("Wires and cables");
    expect(html).not.toContain("₹900M");
  });
  it("retains the shared annual chart when no TTM is available and includes every real Indian revenue year", () => {
    const company = fixture(false);
    company.metrics.revenue = [{ year: 2015, value: 200 }, { year: 2020, value: 600 }, { year: 2026, value: 1000 }];
    company.companyStory!.revenueOfferings = revenueOfferingStory({
      annual_totals: { "2015": { period_end: "2015-03-31", revenue_display: display("200") },
        "2020": { period_end: "2020-03-31", revenue_display: display("600") },
        "2026": { period_end: "2026-03-31", revenue_display: display("1000") } },
      offerings: [], ttm: null,
      reported_segment_reconciliation: { period_type: "annual", fiscal_year: 2026,
        period_end: "2026-03-31", revenue_display: display("1000"), basis: "Reported annual revenue", methodology: "Audited annual segment note",
        items: [{ id: "business", label: "Reported business", role: "segment", revenue_display: display("1000"), sources: [] }],
        history: Object.fromEntries([["2015", "200"], ["2020", "600"], ["2026", "1000"]].map(([year, value]) => [year, {
          period_end: `${year}-03-31`, revenue_display: display(value),
          items: [{ id: "business", label: "Reported business", role: "segment", revenue_display: display(value), sources: [] }],
        }])),
      },
    });
    const html = renderToStaticMarkup(<RevenueOfferingHistory company={company} />);
    for (const year of [2015, 2020, 2026]) expect(html).toContain(`FY${year}`);
    expect(html).not.toContain("FY2016");
    expect(html).not.toContain("TTM through");
    expect(html).toContain("Reported business");
    expect(html).toContain('class="offering-chart-scroll"');
    expect(html).not.toContain("unsafe-composition");
    expect(html).not.toContain("NaN");
    expect(renderToStaticMarkup(<HowItMakesMoney company={company} />)).toContain("Revenue breakdown for FY2026");
  });
  it("shows the API reporting basis beside the existing period metadata only for Indian NSE/BSE listings", () => {
    const company = { ...fixture(false), name: "Example India Limited", symbol: "EXAMPLE", sector: "Industrial",
      reportingPeriod: "FY 2026", updatedAt: "6 October 2026", reportingBasis: "standalone" as const,
      metrics: { revenue: [{ year: 2026, value: 1000 }], operatingMargin: [], freeCashFlow: [], netDebt: [] },
    } as Company;
    const render = (value: Company) => renderToStaticMarkup(<CompanyStoryHome company={value} watched={false} toggleWatch={() => {}} backToDiscover={() => {}} />);
    const html = render(company);
    expect(html).toContain('<div class="story-data-date"><span>Latest reported period</span><strong>FY 2026</strong><span class="story-reporting-basis">Standalone</span><small>Updated 6 October 2026</small></div>');
    expect(render({ ...company, reportingBasis: "consolidated", exchange: "BSE" })).toContain('class="story-reporting-basis">Consolidated');
    expect(render({ ...company, id: "data-0000789019", country: "USA", exchange: "NASDAQ" })).not.toContain('class="story-reporting-basis"');
    expect(render({ ...company, reportingBasis: undefined })).not.toContain('class="story-reporting-basis"');
    expect(render({ ...company, exchange: "OTHER" })).not.toContain('class="story-reporting-basis"');
  });
  it("labels differing historical bases within the shared revenue chart without adding labels to US companies", () => {
    const company = fixture(true);
    company.reportingBasis = "standalone";
    company.reportingBasisByYear = { "2025": "consolidated", "2026": "standalone" };
    company.metrics.revenue.unshift({ year: 2025, value: 800 });
    const html = renderToStaticMarkup(<RevenueOfferingHistory company={company} />);
    expect(html).toMatch(/class="story-period-basis"[^>]*>Consolidated/);
    expect(html).toMatch(/class="story-period-basis"[^>]*>Standalone/);
    expect(html).toContain('class="offering-chart-scroll"');
    const us = { ...company, id: "data-0000789019", country: "USA", exchange: "NASDAQ" } as Company;
    expect(renderToStaticMarkup(<RevenueOfferingHistory company={us} />)).not.toContain('class="story-period-basis"');
  });
  it("serves an Indian logo through TaRaShaData rather than rejecting its identifier", async () => {
    const fetch = vi.fn().mockResolvedValue(new Response(new Uint8Array([1, 2, 3]), { headers: { "Content-Type": "image/png" } }));
    vi.stubGlobal("fetch", fetch);
    const response = await fetchDataCompanyLogo({ TARASHA_DATA_API_URL: "http://localhost:8000" }, "data-IN:ISIN:INE176B01034");
    expect(response.status).toBe(200);
    expect(fetch.mock.calls[0][0]).toBe("http://localhost:8000/v1/companies/IN:ISIN:INE176B01034/logo");
    await expect(fetchDataCompanyLogo({ TARASHA_DATA_API_URL: "http://localhost:8000" }, "data-IN:../../secret")).rejects.toThrow("Invalid");
  });
  it("labels each stated historical currency and withholds cross-currency revenue comparisons", () => {
    const company = fixture(false);
    company.companyStory!.revenueOfferings!.ttm = undefined;
    company.metrics.revenue.unshift({ year: 2025, value: 800 });
    company.annualCurrencyByYear = { "2025": "USD", "2026": "INR" };
    const html = renderToStaticMarkup(<RevenueOfferingHistory company={company} />);
    expect(html).toContain("FY2025");
    expect(html).toContain("FY2026");
    expect(html).toContain("$800M");
    expect(html).toContain("₹1.00B");
    expect(html).toContain("Amount trend unavailable across different reported currencies");
    expect(html).not.toContain("25.0%");
    expect(html).not.toContain('class="offering-growth-arrow');
  });
  it("preserves an Indian company’s explicitly reported foreign currency on the latest revenue page", () => {
    const company = fixture(false);
    company.companyStory!.revenueOfferings!.ttm = undefined;
    company.annualCurrencyByYear = { "2026": "USD" };
    const html = renderToStaticMarkup(<HowItMakesMoney company={company} />);
    expect(html).toContain("$1.00B");
    expect(html).not.toContain("₹1.00B");
  });
  it("retains historical balance amounts with their own currencies without a shared monetary axis", () => {
    const company = fixture(false);
    company.annualCurrencyByYear = { "2025": "USD", "2026": "INR" };
    company.companyStory!.balanceSheet = {
      status: "available", reason: null, latestFiscalYear: 2026, latestPeriodEnd: "2026-03-31",
      reportingCurrency: "INR", displayUnit: "INR millions", years: [2025, 2026],
      equation: { assets: 1000, liabilities: 400, shareholdersEquity: 600 },
      assets: [{ key: "cash", label: "Cash cushion", value: 300, percentageOfBase: 30,
        trend: [{ fiscalYear: 2025, value: 200, percentageOfBase: 20 }, { fiscalYear: 2026, value: 300, percentageOfBase: 30 }], children: [] }],
      liabilities: [], shareholdersEquity: [], health: { totalCash: 300, totalDebt: null, netCashDebt: null,
        netDebtToEbitda: null, interestCoverage: null, weightedAverageCostOfDebt: null,
        weightedAverageCostOfDebtFiscalYear: null, weightedAverageCostOfDebtNote: "Unavailable" },
      summary: null, methodology: "Source-backed annual facts", sourceUrl: null,
    };
    const html = renderToStaticMarkup(<BalanceSheetChapter company={company} />);
    expect(html).toContain("$200M");
    expect(html).toContain("₹300M");
    expect(html).toContain("Amount comparison unavailable across currencies");
    expect(html).not.toContain('class="balance-bar-chart');
  });
  it("shows verified partial Indian balances in the shared cards without claiming an accounting equation", () => {
    const company = fixture(false);
    company.annualCurrencyByYear = { "2026": "INR" };
    company.companyStory!.balanceSheet = {
      status: "available", accountingEquationAvailable: false, reason: null, latestFiscalYear: 2026, latestPeriodEnd: "2026-03-31",
      reportingCurrency: "INR", displayUnit: "INR millions", years: [2026],
      equation: { assets: null, liabilities: null, shareholdersEquity: null },
      assets: [{ key: "other_intangible_assets", label: "Other intangible assets", value: 40, percentageOfBase: null,
        trend: [{ fiscalYear: 2026, value: 40, percentageOfBase: null }], children: [] }],
      liabilities: [], shareholdersEquity: [], health: { totalCash: null, totalDebt: null, netCashDebt: null,
        netDebtToEbitda: null, interestCoverage: null, weightedAverageCostOfDebt: null,
        weightedAverageCostOfDebtFiscalYear: null, weightedAverageCostOfDebtNote: "Unavailable" },
      summary: null, methodology: "Source-backed annual facts", sourceUrl: null,
    };
    const html = renderToStaticMarkup(<BalanceSheetChapter company={company} />);
    expect(html).toContain("Verified annual balances · FY2026");
    expect(html).toContain('class="balance-component-card assets ');
    expect(html).toContain("₹40M");
    expect(html).toContain("Share unavailable");
    expect(html).toContain("The accounting equation is unavailable for this period");
    expect(html).not.toContain("The accounting equation reconciles");
    expect(html).not.toContain('class="balance-funding-bar"');
    expect(html).not.toContain('class="story-unavailable"');
    expect(html).toContain("Comparable annual history unavailable from verified source data.");
    expect(html).not.toContain("not separately reported");
    expect(html).not.toContain('class="balance-bar-chart');
  });
  it("preserves verified timing amounts and explicit zero while withholding their shares without a positive revenue denominator", () => {
    const company = fixture(false);
    const story = company.companyStory!.revenueOfferings!;
    story.ttm!.revenue = 0;
    story.adjustmentImpacts = [{ id: "timing", label: "Contract liability timing", definition: "Closing minus opening.",
      valueByPeriod: { "2026": null, TTM: 20 }, coverageByPeriod: {}, derivationByPeriod: {}, sourcesByPeriod: {}, directionByPeriod: {}, openingBalanceByPeriod: {}, closingBalanceByPeriod: {},
    }, { id: "zero", label: "Explicit zero movement", definition: "Reported zero movement.",
      valueByPeriod: { "2026": null, TTM: 0 }, coverageByPeriod: {}, derivationByPeriod: {}, sourcesByPeriod: {}, directionByPeriod: {}, openingBalanceByPeriod: {}, closingBalanceByPeriod: {},
    }];
    const html = renderToStaticMarkup(<RevenueOfferingHistory company={company} />);
    const cards = html.match(/<article class="revenue-adjustment-card">.*?<\/article>/g)!;
    const ttm = cards.find(card => card.includes("<h5>TTM"))!;
    expect(ttm).toContain("+₹20M");
    expect(ttm).toContain("₹0M");
    expect(ttm.match(/<small>Share unavailable<\/small>/g)).toHaveLength(2);
    expect(ttm).not.toContain("of revenue");
    expect(ttm).not.toContain("Not disclosed");
    expect(cards.find(card => card.includes("<h5>FY2026"))).toContain("Data unavailable");
  });
  it("retains verified annual totals and the shared chart when source-backed composition is unavailable", () => {
    const company = fixture(false);
    const html = renderToStaticMarkup(<RevenueOfferingHistory company={company} />);
    expect(html).toContain("₹1.00B");
    expect(html).toContain("₹1.20B");
    expect(html).toContain("Composition unavailable");
    expect(html).toContain("Unallocated / composition unavailable");
    expect(html).toContain("Data unavailable");
    expect(html).toContain('class="offering-chart-scroll"');
    expect(html).toContain('class="revenue-adjustment-grid"');
    expect(html).not.toContain("not disclosed");
    expect(html).not.toContain("not separately disclosed");
    const us = { ...company, id: "data-0000789019", country: "USA", exchange: "NASDAQ", currency: "US$ millions" } as Company;
    const usHtml = renderToStaticMarkup(<RevenueOfferingHistory company={us} />);
    expect(usHtml).toContain("Composition not disclosed");
    expect(usHtml).toContain("Other / composition not separately disclosed");
    expect(usHtml).toContain("Not disclosed");
    expect(usHtml).not.toContain("Composition unavailable");
  });
});


function balanceAvailabilityFixture(): Company {
  const company = fixture(false);
  company.annualCurrencyByYear = { "2024": "INR", "2025": "INR", "2026": "INR" };
  company.companyStory!.balanceSheet = {
    status: "available", reason: null, latestFiscalYear: 2026, latestPeriodEnd: "2026-03-31",
    reportingCurrency: "INR", displayUnit: "INR millions", years: [2024, 2025, 2026],
    equation: { assets: 1000, liabilities: 400, shareholdersEquity: 600 },
    assets: [{ key: "other_intangible_assets", label: "Other intangible assets", value: null, percentageOfBase: null,
      trend: [{ fiscalYear: 2024, value: 20, percentageOfBase: 2 }, { fiscalYear: 2025, value: 40, percentageOfBase: 4 }], children: [] }],
    liabilities: [], shareholdersEquity: [], health: { totalCash: null, totalDebt: null, netCashDebt: null,
      netDebtToEbitda: null, interestCoverage: null, weightedAverageCostOfDebt: null,
      weightedAverageCostOfDebtFiscalYear: null, weightedAverageCostOfDebtNote: "Unavailable" },
    summary: null, methodology: "Source-backed annual facts", sourceUrl: null,
  };
  return company;
}

describe("Indian balance availability annotations", () => {
  it("retains older verified values without presenting their movement as a current comparison", () => {
    const html = renderToStaticMarkup(<BalanceSheetChapter company={balanceAvailabilityFixture()} />);
    expect(html).toContain("Latest value unavailable");
    expect(html).toContain("FY2024: ₹20M");
    expect(html).toContain("FY2025: ₹40M");
    expect(html).not.toContain("pts vs FY24");
    expect(html).toContain('<div class="neutral"><span>Net cash (debt)</span><strong>Not available</strong>');
  });
  it("requires a latest-year share before annotating a current movement", () => {
    const company = balanceAvailabilityFixture();
    const component = company.companyStory!.balanceSheet!.assets[0];
    component.value = 50;
    component.percentageOfBase = 5;
    expect(renderToStaticMarkup(<BalanceSheetChapter company={company} />)).not.toContain("pts vs FY24");
    component.trend.push({ fiscalYear: 2026, value: 50, percentageOfBase: null });
    expect(renderToStaticMarkup(<BalanceSheetChapter company={company} />)).not.toContain("pts vs FY24");
    component.trend[2].percentageOfBase = 5;
    expect(renderToStaticMarkup(<BalanceSheetChapter company={company} />)).toContain("↑ 3.0 pts vs FY24");
  });
  it("preserves reported zero and its supported comparison", () => {
    const company = balanceAvailabilityFixture();
    const story = company.companyStory!.balanceSheet!;
    const component = story.assets[0];
    component.value = 0;
    component.percentageOfBase = 0;
    component.trend.push({ fiscalYear: 2026, value: 0, percentageOfBase: 0 });
    story.health.netCashDebt = 0;
    const html = renderToStaticMarkup(<BalanceSheetChapter company={company} />);
    expect(html).toContain("↓ 2.0 pts vs FY24");
    expect(html).toContain('<div class="positive"><span>Net cash (debt)</span><strong>₹0M</strong>');
    expect(html).not.toContain("Latest value unavailable");
  });
  it("retains US historical values and a neutral annotation when latest balances are unavailable", () => {
    const company = balanceAvailabilityFixture();
    company.country = "USA";
    company.exchange = "NASDAQ";
    const html = renderToStaticMarkup(<BalanceSheetChapter company={company} />);
    expect(html).not.toContain("pts vs FY24");
    expect(html).toContain('<div class="neutral"><span>Net cash (debt)</span><strong>Not available</strong>');
    expect(html).toContain("Complete debt data unavailable");
    expect(html).toContain("Other intangible assets: FY2024 ₹20M, FY2025 ₹40M");
  });
});


describe("source-complete balance annotations", () => {
  it("explains unavailable groups while retaining reported child values", () => {
    const company = balanceAvailabilityFixture();
    company.country = "USA";
    const group = company.companyStory!.balanceSheet!.assets[0];
    group.unavailableReason = "Complete same-period current asset and cash totals unavailable";
    group.children.push({ key: "prepaid_expenses", label: "Reported prepaid expenses", value: 7,
      percentageOfBase: .7, interestRatePercent: null });
    const html = renderToStaticMarkup(<BalanceSheetChapter company={company} />);
    expect(html).toContain(group.unavailableReason);
    expect(html).toContain("Reported prepaid expenses");
    expect(html).toContain("₹7M");
  });
  it("does not claim a complete US accounting equation when its inputs are incomplete", () => {
    const company = balanceAvailabilityFixture();
    company.country = "USA";
    company.companyStory!.balanceSheet!.accountingEquationAvailable = false;
    company.companyStory!.balanceSheet!.equation.shareholdersEquity = null;
    const html = renderToStaticMarkup(<BalanceSheetChapter company={company} />);
    expect(html).toContain("The accounting equation is unavailable for this period");
    expect(html).not.toContain("The accounting equation reconciles for the latest reported fiscal year");
  });
});
