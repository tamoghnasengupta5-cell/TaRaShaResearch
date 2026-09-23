import { describe, expect, it } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import { AdjustedRevenueComparison, RevenueOfferingHistory } from "./RevenueOfferingHistory";
import { revenueGrowth, revenuePeriods, threePeriodRevenueCagr } from "./revenueOfferingMath";
import type { Company } from "./types";
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
  it("adds a real interim TTM after six fiscal years but does not duplicate FY as TTM", () => {
    expect(revenuePeriods(company, false)).toEqual(["2020", "2021", "2022", "2023", "2024", "2025"]);
    const withTtm = { ...company, companyStory: { revenueOfferings: { offerings: [], ttm: { periodEnd: "2026-06-30", revenue: 600, priorYearRevenue: 480 } } } } as Company;
    expect(revenuePeriods(withTtm, false)).toEqual(["2020", "2021", "2022", "2023", "2024", "2025", "TTM"]);
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
