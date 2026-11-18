import { describe, expect, it } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import { AdjustedRevenueComparison, RevenueOfferingHistory } from "./RevenueOfferingHistory";
import { revenueGrowth, revenuePeriods, threePeriodRevenueCagr } from "./revenueOfferingMath";
import type { AdjustedRevenuePoint, Company } from "./types";
import { revenueOfferingStory } from "../functions/dataProvider";

const company = { currency: "US$ millions", metrics: { revenue: [{ year: 2023, value: 200 }, { year: 2025, value: 400 }] } } as Company;
describe("revenue offering history", () => {
  it("maps backend offering values and uses backend totals as percentage denominators", () => {
    const display = (value: string) => ({ value, unit: "USD millions" });
    const revenueOfferings = revenueOfferingStory({
      status: "available",
      annual_totals: { "2025": { revenue_display: display("500"), period_end: "2025-09-30" } },
      ttm: { period_end: "2026-03-31", revenue_display: display("600"), prior_year_revenue_display: display("480"), sources: [{ source_url: "https://example.test/ttm" }], prior_year_sources: [{ source_url: "https://example.test/ttm-prior" }] },
      offerings: [{ id: "products", label: "Products", history: { "2025": { revenue_display: display("250"), source_url: "https://example.test/filing" } }, ttm: { revenue_display: display("300"), sources: [{ source_url: "https://example.test/interim" }] } }],
      adjustment_entries: [{ id: "contract_liabilities", label: "Contract liabilities", definition: "Reported contract liabilities.", history: { "2025": { value_display: display("100"), coverage: "reported", derivation: "Reported fact.", sources: [{ source_url: "https://example.test/filing" }] } }, ttm: { value_display: display("120"), coverage: "reported", derivation: "Balance at TTM end.", sources: [{ source_url: "https://example.test/ttm" }, { source_url: "https://example.test/adjustment-component" }] } }],
      revenue_adjustment_impacts: [{ id: "contract_liability_timing", label: "Contract liabilities / deferred revenue", definition: "Annual movement.", history: { "2025": { value_display: display("20"), direction: "positive", coverage: "reported to reported", derivation: "+(closing minus opening).", sources: [{ source_url: "https://example.test/filing" }, { source_url: "https://example.test/opening" }] } }, ttm: { value_display: display("30"), direction: "positive", sources: [{ source_url: "https://example.test/ttm-balance" }] } }],
      adjusted_revenue: { status: "available", reason: null, methodology: "Reported revenue plus source-backed timing movements.", points: [
        { fiscal_year: 2024, period_end: "2024-12-31", reported_revenue_display: display("400"), total_adjustment_display: display("10"), adjusted_revenue_display: display("410"), reported_growth_percent: "10", adjusted_growth_percent: "11", growth_difference_pp: "1", applied_adjustment_ids: ["contract_liability_timing"], sources: [{ source_url: "https://example.test/2024" }] },
        { fiscal_year: 2025, period_end: "2025-12-31", reported_revenue_display: display("500"), total_adjustment_display: display("20"), adjusted_revenue_display: display("520"), reported_growth_percent: "25", adjusted_growth_percent: "26.83", growth_difference_pp: "1.83", applied_adjustment_ids: ["contract_liability_timing"], sources: [{ source_url: "https://example.test/filing" }] },
        { fiscal_year: 2026, period_type: "ttm", period_end: "2026-03-31", reported_revenue_display: display("600"), total_adjustment_display: display("30"), adjusted_revenue_display: display("630"), reported_growth_percent: "25", adjusted_growth_percent: "28", growth_difference_pp: "3", applied_adjustment_ids: ["contract_liability_timing"], sources: [{ source_url: "https://example.test/ttm" }] },
      ] },
      quality_warnings: [],
    });
    const html = renderToStaticMarkup(<RevenueOfferingHistory company={{ ...company, companyStory: { revenueOfferings } }} />);
    expect(revenueOfferings?.annualPeriodEnds).toEqual({ "2025": "2025-09-30" });
    expect(html).toContain("$500M");
    expect(html).toContain("50%");
    expect(html).toContain("Adds to revenue");
    expect(html).toContain("+$20M");
    expect(html).toContain("+$30M");
    expect(html).toContain("TTM through 2026-03-31");
    expect(html).toContain("https://example.test/filing");
    expect(html).toContain("https://example.test/opening");
    expect(html).toContain("Contract liabilities / deferred revenue");
    expect(html).toContain("+4.0% of revenue");
    expect(html).toContain("Sources &amp; derivations");
    expect(html).toContain("<svg");
    expect(html).not.toContain("<table");

    const adjustedHtml = renderToStaticMarkup(<AdjustedRevenueComparison company={{ ...company, companyStory: { revenueOfferings } }} />);
    expect(adjustedHtml).toContain("Reported vs Adjusted Revenue");
    expect(adjustedHtml).toContain("$630M");
    expect(adjustedHtml).toContain("+3.0 pp vs reported");
    expect(adjustedHtml).toContain("Revenue and growth comparison");
  });
  it("preserves reported parent histories and sources without displaying their table", () => {
    const display = (value: string) => ({ value, unit: "USD millions" });
    const source = { source_url: "https://www.sec.gov/Archives/example/filing.xml", accession: "0000000001-26-000001",
      fact_id: 11, artifact_id: 7, company_id: 1, taxonomy: "us-gaap", concept: "RevenueFromContractWithCustomerExcludingAssessedTax",
      dimensions: { "us-gaap:ProductOrServiceAxis": "x:CloudMember" }, value: "80000000", unit: "USD",
      period_start: "2025-01-01", period_end: "2025-12-31", period_type: "annual", fiscal_year: 2025,
      fiscal_period: "FY", filed_date: "2026-02-15", role: "reported" };
    const revenueOfferings = revenueOfferingStory({
      status: "available", annual_totals: { "2025": { revenue_display: display("100"), period_end: "2025-12-31" } },
      ttm: { period_end: "2026-03-31", revenue_display: display("130"), prior_year_revenue_display: display("100") },
      offerings: [
        { id: "compute", label: "Compute", history: {}, ttm: { revenue_display: display("100"), sources: [source] } },
        { id: "graphics", label: "Graphics", history: {}, ttm: { revenue_display: display("30"), sources: [source] } },
      ],
      reported_offerings: [{ id: "cloud", label: "Cloud parent", axis: "us-gaap:ProductOrServiceAxis", member: "x:CloudMember", label_basis: "xbrl_member_name",
        history: { "2025": { revenue_display: display("80"), period_end: "2025-12-31", sources: [source] } },
        ttm: { revenue_display: display("110"), period_end: "2026-03-31", sources: [{ ...source, role: "prior_fiscal_year" }, { ...source, fact_id: 12, role: "current_ytd" }] },
        children: [{ id: "applications", label: "Applications", history: { "2025": { revenue_display: display("50"), sources: [source] } }, ttm: null },
          { id: "infrastructure", label: "Infrastructure", history: { "2025": { revenue_display: display("30"), sources: [source] } }, ttm: null }],
      }], reported_offerings_methodology: "Reported overlapping parent and child disclosures retain their SEC sources.",
    });
    const parent = revenueOfferings!.reportedOfferings![0];
    expect(parent.revenueByPeriod).toEqual({ "2025": 80, TTM: 110 });
    expect(parent.provenanceByPeriod["2025"]).toEqual([source]);
    expect(parent.provenanceByPeriod.TTM).toHaveLength(2);
    expect(parent.periodEnds).toEqual({ "2025": "2025-12-31", TTM: "2026-03-31" });
    expect(parent.member).toBe("x:CloudMember");
    expect(parent.children!.map(item => item.label)).toEqual(["Applications", "Infrastructure"]);
    const html = renderToStaticMarkup(<RevenueOfferingHistory company={{ ...company, companyStory: { revenueOfferings } }} />);
    const chart = html.slice(html.indexOf("<svg"), html.indexOf("</svg>") + 6);
    expect(chart).toContain("Compute: $100M (76.9% of total revenue)");
    expect(chart).toContain("Graphics: $30M (23.1% of total revenue)");
    expect(chart).not.toContain("Cloud parent");
    expect(html).not.toContain("Reported category history");
    expect(html).not.toContain("Cloud parent");
    expect(html).not.toContain("Applications");
    expect(html).not.toContain("Infrastructure");
    expect(html).not.toContain("<table");
    expect(html).toContain(source.source_url);
    expect(html).toContain("Reported overlapping parent and child disclosures retain their SEC sources.");
  });

  it.each([false, true])("shows seven fiscal years with newer TTM=%s while retaining full history", includeTtm => {
    const annualTotals = Object.fromEntries(Array.from({ length: 25 }, (_, index) => [String(2001 + index), 100 + index]));
    const fullHistory = {
      ...company,
      companyStory: { revenueOfferings: {
        offerings: [], annualTotals,
        ttm: includeTtm ? { periodEnd: "2026-06-30", revenue: 150, priorYearRevenue: 130 } : undefined,
      } },
    } as Company;
    const before = structuredClone(fullHistory);
    const html = renderToStaticMarkup(<RevenueOfferingHistory company={fullHistory} />);
    const expected = ["FY2019", "FY2020", "FY2021", "FY2022", "FY2023", "FY2024", "FY2025", ...(includeTtm ? ["TTM"] : [])];
    expect([...html.matchAll(/class="offering-period-label"[^>]*>([^<]+)</g)].map(match => match[1])).toEqual(expected);
    expect([...html.matchAll(/<h5>([^<]+)<\/h5>/g)].map(match => match[1])).toEqual(expected);
    expect(html).not.toContain("FY2018");
    expect(html).not.toContain("FY2001");
    expect(fullHistory).toEqual(before);
    expect(revenuePeriods(fullHistory, false)).toHaveLength(25 + Number(includeTtm));
    expect(revenuePeriods(fullHistory, false)[0]).toBe("2001");
  });

  it("keeps actual annual history and adds a newer TTM without inventing missing years", () => {
    expect(revenuePeriods(company, false)).toEqual(["2023", "2025"]);
    const withTtm = { ...company, companyStory: { revenueOfferings: { offerings: [], ttm: { periodEnd: "2026-06-30", revenue: 600, priorYearRevenue: 480 } } } } as Company;
    expect(revenuePeriods(withTtm, false)).toEqual(["2023", "2025", "TTM"]);
    expect(revenuePeriods(withTtm, true)).toEqual(revenuePeriods(withTtm, false).reverse());
    const yearEnd = { ...company, companyStory: { revenueOfferings: { offerings: [], ttm: { periodEnd: "2025-12-31", revenue: 400, priorYearRevenue: 350 } } } } as Company;
    expect(revenuePeriods(yearEnd, false)).not.toContain("TTM");

    const nonCalendarFiscalYear = {
      ...company,
      metrics: { revenue: [{ year: 2026, value: 500 }], operatingMargin: [], freeCashFlow: [], netDebt: [] },
      companyStory: { revenueOfferings: {
        offerings: [],
        annualTotals: { "2026": 500 },
        annualPeriodEnds: { "2026": "2026-01-31" },
        ttm: { periodEnd: "2026-10-31", revenue: 650, priorYearRevenue: 520 },
      } },
    } as Company;
    expect(revenuePeriods(nonCalendarFiscalYear, false)).toContain("TTM");
  });
  it("does not calculate growth across missing or zero denominators", () => {
    expect(revenueGrowth(400, null)).toBeNull();
    expect(revenueGrowth(400, 0)).toBeNull();
    expect(revenueGrowth(100, 200)).toBe(-50);
    expect(threePeriodRevenueCagr(225, 144)).toBe(25);
    expect(threePeriodRevenueCagr(225, null)).toBeNull();
    expect(threePeriodRevenueCagr(225, 0)).toBeNull();
  });
  it("retains revenue while explaining unavailable offering composition", () => {
    const html = renderToStaticMarkup(<RevenueOfferingHistory company={company} />);
    expect(html).toContain("$400M");
    expect(html).toContain("Composition not disclosed");
    expect(html).toContain("No source-backed timing movement");
    expect(html).not.toContain(">TTM</text>");
  });
  it.each([false, true])("limits page 3 chart and table to seven fiscal years with newer TTM=%s, preserving history", includeTtm => {
    const annualPoints = Array.from({ length: 25 }, (_, index): AdjustedRevenuePoint => ({
      fiscalYear: 2001 + index, periodEnd: `${2001 + index}-12-31`,
      reportedRevenue: 100 + index, adjustedRevenue: 101 + index, totalAdjustment: 1,
      reportedGrowthPercent: 5, adjustedGrowthPercent: 4, growthDifferencePp: -1,
      appliedAdjustmentIds: [], sourceUrls: [],
    })).reverse();
    const historical = {
      ...company, companyStory: { revenueOfferings: { offerings: [], adjustedRevenue: {
        status: "available", reason: null, methodology: "Source-backed timing movements.", points: [...annualPoints, ...(includeTtm ? [{
          ...annualPoints[0], fiscalYear: 2026, periodType: "ttm" as const, periodEnd: "2026-06-30",
        }] : [])],
      } } },
    } as Company;
    const before = structuredClone(historical);
    const html = renderToStaticMarkup(<AdjustedRevenueComparison company={historical} />);
    const expected = ["FY2019", "FY2020", "FY2021", "FY2022", "FY2023", "FY2024", "FY2025", ...(includeTtm ? ["TTM"] : [])];
    expect([...html.matchAll(/class="adjusted-year-label"[^>]*>([^<]+)</g)].map(match => match[1])).toEqual(expected);
    const tableHeader = html.match(/<thead>(.*?)<\/thead>/)?.[1] ?? "";
    expect([...tableHeader.matchAll(/<th>([^<]+)<\/th>/g)].map(match => match[1])).toEqual(expected);
    expect(html).not.toContain("FY2018");
    expect(html).not.toContain("FY2001");
    expect(historical).toEqual(before);
    expect(historical.companyStory!.revenueOfferings!.adjustedRevenue!.points).toHaveLength(25 + Number(includeTtm));
  });

  it("does not connect the adjusted line across a missing adjusted year", () => {
    const display = (value: string) => ({ value, unit: "USD millions" });
    const revenueOfferings = revenueOfferingStory({
      status: "available",
      annual_totals: {},
      offerings: [],
      adjusted_revenue: {
        status: "available",
        points: [
          { fiscal_year: 2023, period_end: "2023-12-31", reported_revenue_display: display("100"), adjusted_revenue_display: display("102") },
          { fiscal_year: 2024, period_end: "2024-12-31", reported_revenue_display: display("110"), adjusted_revenue_display: null },
          { fiscal_year: 2025, period_end: "2025-12-31", reported_revenue_display: display("120"), adjusted_revenue_display: display("123") },
        ],
      },
    });
    const html = renderToStaticMarkup(<AdjustedRevenueComparison company={{ ...company, companyStory: { revenueOfferings } }} />);
    expect(html.match(/class="adjusted-line adjusted"/g)).toHaveLength(2);
  });
});


describe("adjusted revenue summary availability", () => {
  function fixture(india: boolean, overrides: Partial<AdjustedRevenuePoint> = {}): Company {
    const point: AdjustedRevenuePoint = {
      fiscalYear: 2025, periodEnd: "2025-03-31", reportedRevenue: 500,
      totalAdjustment: null, adjustedRevenue: null, reportedGrowthPercent: 25,
      adjustedGrowthPercent: null, growthDifferencePp: null,
      appliedAdjustmentIds: [], sourceUrls: [], ...overrides,
    };
    return {
      ...company, id: india ? "data-IN:TEST" : "data-0000789019",
      country: india ? "India" : "USA", exchange: india ? "NSE" : "NASDAQ",
      currency: india ? "INR millions" : "US$ millions",
      metrics: { ...company.metrics, revenue: [{ year: 2024, value: 400 }, { year: 2025, value: 500 }] },
      companyStory: { revenueOfferings: { offerings: [], adjustedRevenue: {
        status: "available", reason: null, methodology: "Verified partial adjustments.", points: [point],
      } } },
    };
  }

  function summary(html: string) {
    return html.match(/<div class="adjusted-summary-grid">(.*?)<\/div>/)?.[1] ?? "";
  }

  it.each([false, true])("uses one latest fiscal year for all cards when TTM adjusted revenue is missing (India=%s)", india => {
    const data = fixture(india, {
      totalAdjustment: -10, adjustedRevenue: 490, adjustedGrowthPercent: 22.5, growthDifferencePp: -2.5,
    });
    const story = data.companyStory!.revenueOfferings!.adjustedRevenue!;
    story.points.push({ ...story.points[0], periodType: "ttm", fiscalYear: 2026, periodEnd: "2026-06-30",
      reportedRevenue: 600, adjustedRevenue: null, totalAdjustment: null,
      reportedGrowthPercent: 20, adjustedGrowthPercent: null, growthDifferencePp: null,
    });
    const before = structuredClone(data);
    const html = renderToStaticMarkup(<AdjustedRevenueComparison company={data} />);
    const cards = summary(html);
    expect(cards.match(/FY2025/g)).toHaveLength(4);
    expect(cards).not.toContain("TTM");
    expect(cards).toContain(`${india ? "₹" : "$"}500M`);
    expect(cards).toContain(`${india ? "₹" : "$"}490M`);
    expect(cards).toContain("−2.0% vs reported");
    expect(cards).toContain("22.5%");
    expect(html).toContain("TTM adjusted revenue is unavailable. Showing FY2025 for the comparison.");
    expect(html).toMatch(/class="adjusted-year-label"[^>]*>TTM<\/text>/);
    expect(html).toMatch(/<th>TTM(?:<small[^>]*>[^<]*<\/small>)?<\/th>/);
    expect(html).toContain(`${india ? "₹" : "$"}600M`);
    expect(data).toEqual(before);
  });

  it.each([0, 630])("keeps the TTM comparison when its adjusted revenue is supported, including zero (%s)", adjustedRevenue => {
    const data = fixture(false, { totalAdjustment: -10, adjustedRevenue: 490 });
    const story = data.companyStory!.revenueOfferings!.adjustedRevenue!;
    story.points.push({ ...story.points[0], periodType: "ttm", fiscalYear: 2026, periodEnd: "2026-06-30",
      reportedRevenue: 600, adjustedRevenue, totalAdjustment: adjustedRevenue - 600,
    });
    const html = renderToStaticMarkup(<AdjustedRevenueComparison company={data} />);
    const cards = summary(html);
    expect(cards.match(/TTM/g)).toHaveLength(4);
    expect(cards).not.toContain("FY2025");
    expect(cards).toContain(`<strong>$${adjustedRevenue}M</strong>`);
    expect(html).not.toContain("Showing FY2025 for the comparison.");
  });

  it("uses the latest fiscal year even if only an older year has adjusted revenue", () => {
    const data = fixture(false);
    const story = data.companyStory!.revenueOfferings!.adjustedRevenue!;
    story.points.unshift({ ...story.points[0], fiscalYear: 2024, periodEnd: "2024-03-31", adjustedRevenue: 490 });
    story.points.push({ ...story.points[1], periodType: "ttm", fiscalYear: 2026, periodEnd: "2026-06-30" });
    const cards = summary(renderToStaticMarkup(<AdjustedRevenueComparison company={data} />));
    expect(cards.match(/FY2025/g)).toHaveLength(4);
    expect(cards).not.toContain("FY2024");
    expect(cards).not.toContain("$490M");
    expect(cards.match(/<strong>—<\/strong>/g)).toHaveLength(2);
  });

  it("labels missing Indian adjustments in the existing neutral comparison slots", () => {
    const html = renderToStaticMarkup(<AdjustedRevenueComparison company={fixture(true)} />);
    const cards = summary(html);
    expect(cards.match(/<article>/g)).toHaveLength(4);
    expect(cards).toContain("<strong>₹500M</strong>");
    expect(cards.match(/<strong>—<\/strong>/g)).toHaveLength(2);
    expect(cards.match(/<b class="neutral">Data unavailable<\/b>/g)).toHaveLength(2);
    expect(cards).not.toContain('class="positive"');
    expect(html).toContain('class="adjusted-revenue-main"');
    expect(html).toContain('class="adjusted-chart-panel"');
    expect(html).toContain('class="adjusted-summary-panel"');
    expect(html).toContain("Revenue and growth comparison");
  });

  it("preserves supported zero adjustments and zero growth as actual values", () => {
    const cards = summary(renderToStaticMarkup(<AdjustedRevenueComparison company={fixture(true, {
      totalAdjustment: 0, adjustedRevenue: 500, reportedGrowthPercent: 0,
      adjustedGrowthPercent: 0, growthDifferencePp: 0,
    })} />));
    expect(cards).toContain('<b class="positive">0.0% vs reported</b>');
    expect(cards).toContain('<b class="positive">0.0 pp vs reported</b>');
    expect(cards.match(/<strong>0.0%<\/strong>/g)).toHaveLength(2);
    expect(cards).not.toContain("Data unavailable");
  });

  it("retains reported and adjusted zero revenue while labeling an undefined percentage", () => {
    const cards = summary(renderToStaticMarkup(<AdjustedRevenueComparison company={fixture(true, {
      reportedRevenue: 0, totalAdjustment: 0, adjustedRevenue: 0,
      reportedGrowthPercent: 0, adjustedGrowthPercent: 0, growthDifferencePp: 0,
    })} />));
    expect(cards.match(/<strong>₹0M<\/strong>/g)).toHaveLength(2);
    expect(cards).toContain('<b class="neutral">Data unavailable</b>');
    expect(cards).toContain('<b class="positive">0.0 pp vs reported</b>');
  });

  it("does not present a comparison when an adjusted input is missing", () => {
    for (const overrides of [
      { adjustedRevenue: null },
      { adjustedGrowthPercent: null },
      { reportedGrowthPercent: null },
      { growthDifferencePp: null },
    ]) {
      const cards = summary(renderToStaticMarkup(<AdjustedRevenueComparison company={fixture(true, {
        totalAdjustment: 0, adjustedRevenue: 500, adjustedGrowthPercent: 25,
        growthDifferencePp: 0, ...overrides,
      })} />));
      expect(cards).toMatch(/<span>Adjusted Growth<\/span>.*?<b class="neutral">Data unavailable<\/b>/);
    }
    const missingAmount = summary(renderToStaticMarkup(<AdjustedRevenueComparison company={fixture(true, {
      totalAdjustment: 0, adjustedRevenue: null, adjustedGrowthPercent: 25, growthDifferencePp: 0,
    })} />));
    expect(missingAmount).toMatch(/<span>Adjusted Revenue<\/span>.*?<b class="neutral">Data unavailable<\/b>/);
  });

  it("labels incomparable Indian currency growth neutrally while retaining supported amounts", () => {
    const data = fixture(true, {
      totalAdjustment: -10, adjustedRevenue: 490, adjustedGrowthPercent: 22.5, growthDifferencePp: -2.5,
    });
    data.annualCurrencyByYear = { "2024": "USD", "2025": "INR" };
    const cards = summary(renderToStaticMarkup(<AdjustedRevenueComparison company={data} />));
    expect(cards).toContain("<strong>₹490M</strong>");
    expect(cards).toContain('<b class="negative">−2.0% vs reported</b>');
    expect(cards).toMatch(/<span>Adjusted Growth<\/span>.*?<strong>—<\/strong><b class="neutral">Data unavailable<\/b>/);
  });

  it("preserves the existing US missing-value card markup exactly", () => {
    const cards = summary(renderToStaticMarkup(<AdjustedRevenueComparison company={fixture(false)} />));
    expect(cards).toBe('<article><span>Reported Revenue</span><small>FY2025</small><strong>$500M</strong></article>'
      + '<article><span>Adjusted Revenue</span><small>FY2025</small><strong>—</strong><b class="positive">—</b></article>'
      + '<article><span>Reported Growth</span><small>FY2025 (YoY)</small><strong>25.0%</strong></article>'
      + '<article><span>Adjusted Growth</span><small>FY2025 (YoY)</small><strong>—</strong><b class="positive">—</b></article>');
    expect(cards).not.toContain("Data unavailable");
  });
});
