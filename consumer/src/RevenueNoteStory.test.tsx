import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { revenueOfferingStory } from "../functions/dataProvider";
import { HowItMakesMoney } from "./CompanyStoryHome";
import { RevenueOfferingHistory } from "./RevenueOfferingHistory";
import { categoryPeriodsAreComparable, revenueBreakdownCategories, revenueBreakdownPoint } from "./revenueBreakdown";
import type { Company, RevenueBreakdownSourceType } from "./types";

const display = (value: string) => ({ value, unit: "INR millions" });
function fixture() {
  const point = (year: number, sourceType: RevenueBreakdownSourceType = "disclosed_revenue_note", basis: "standalone" | "consolidated" = "consolidated") => ({
    period_end: `${year}-03-31`, source_type: sourceType, reporting_basis: basis,
    basis: `${year} ${sourceType === "disclosed_revenue_note" ? "net operating revenue note" : "gross segment disclosure"}`,
    methodology: `${year} primary filing derivation`, revenue_display: display(String((year - 2023) * 100)),
    items: [
      { id: "products", label: "Sale of products", role: "segment" as const, source_type: sourceType,
        revenue_display: display(String((year - 2023) * 90)), yoy_growth_percent: "19.7", three_period_cagr_percent: "15.9",
        sources: [{ source_url: `https://issuer.test/${year}/note` }] },
      { id: "incentive", label: "Export incentives (other operating revenue)", role: "segment" as const, source_type: sourceType,
        revenue_display: display(String((year - 2023) * 10)), sources: [{ source_url: `https://issuer.test/${year}/operating` }] },
    ],
  });
  const payload = {
    annual_totals: Object.fromEntries([2024, 2025, 2026].map(year => [String(year), {
      period_end: `${year}-03-31`, reporting_basis: "consolidated" as "consolidated" | "standalone",
      revenue_display: display(String((year - 2023) * 100)),
    }])),
    offerings: [],
    reported_segment_reconciliation: {
      ...point(2026), period_type: "annual" as "annual" | "ttm", fiscal_year: 2026,
      history: { "2024": point(2024), "2025": point(2025), "2026": point(2026) },
    },
    ttm: { period_end: "2026-06-30", reporting_currency: "INR", revenue_display: display("400"), prior_year_revenue_display: display("320") },
  };
  const company = (): Company => ({ id: "data-IN:TEST", country: "India", exchange: "NSE", currency: "INR millions", reportingBasis: "consolidated",
    metrics: { revenue: [2024, 2025, 2026].map(year => ({ year, value: (year - 2023) * 100 })) },
    companyStory: { revenueOfferings: revenueOfferingStory(payload) },
  } as Company);
  return { payload, company };
}
function firstRow(html: string) { return html.match(/<tbody><tr>(.*?)<\/tr>/)?.[1] ?? ""; }

describe("verified annual revenue-note presentation", () => {
  it("preserves API source types, basis and per-year captions through the adapter and category history", () => {
    const company = fixture().company();
    const data = company.companyStory!.revenueOfferings!.segmentReconciliation!;
    expect(data.sourceType).toBe("disclosed_revenue_note");
    expect(data.reportingBasis).toBe("consolidated");
    expect(data.history!["2025"]).toMatchObject({ sourceType: "disclosed_revenue_note", reportingBasis: "consolidated",
      basis: "2025 net operating revenue note", methodology: "2025 primary filing derivation" });
    expect(data.history!["2025"].items[0].sourceType).toBe("disclosed_revenue_note");
    const category = revenueBreakdownCategories(company)[0];
    expect(category.sourceTypeByPeriod).toEqual({ "2024": "disclosed_revenue_note", "2025": "disclosed_revenue_note", "2026": "disclosed_revenue_note" });
    expect(category.reportingBasisByPeriod).toEqual({ "2024": "consolidated", "2025": "consolidated", "2026": "consolidated" });
    expect(category.sourcesByPeriod!["2025"]).toEqual(["https://issuer.test/2025/note"]);
    expect(category.revenueByPeriod).not.toHaveProperty("TTM");
  });
  it("uses the shared donut and five-column table with an annual net-revenue caption", () => {
    const html = renderToStaticMarkup(<HowItMakesMoney company={fixture().company()} />);
    expect(html).toContain('class="segment-donut-wrap"');
    expect(html).toContain('class="segment-table"');
    expect(html).toContain("Revenue breakdown for FY2026");
    expect(html).toContain("Reported Revenue Category");
    expect(html).toContain("₹300M");
    expect(html).toContain("Export incentives (other operating revenue)");
    expect(html).toContain("Issuer-disclosed revenue-note category");
    expect(html).toContain("categories reconcile to net operating revenue");
    expect(html).toContain("not necessarily reportable operating segments");
    expect(firstRow(html)).toContain("19.7%");
    expect(firstRow(html)).toContain("15.9%");
    expect(html).not.toContain("before deductions");
    expect(html).not.toContain("Reported segments include intersegment sales");
    expect(html).not.toContain("Revenue breakdown for TTM");
  });
  it("retains all verified years, the shared chart/cards, each source caption and unavailable TTM composition", () => {
    const html = renderToStaticMarkup(<RevenueOfferingHistory company={fixture().company()} />);
    for (const year of [2024, 2025, 2026]) {
      expect(html).toContain(`FY${year}`);
      expect(html).toContain(`${year} net operating revenue note`);
      expect(html).toContain(`${year} primary filing derivation`);
    }
    expect(html).toContain('class="offering-chart-scroll"');
    expect(html).toContain('class="revenue-adjustment-grid"');
    expect(html).toContain("disclosed revenue-note categories");
    expect(html).toContain("Composition unavailable");
    expect(html).toContain("TTM through 2026-06-30: ₹400M");
    expect(html).toContain("https://issuer.test/2024/note");
    expect(html).not.toContain("The chart retains reported segment sales for every period");
    expect(html).not.toContain("Composition withheld");
  });
  it("labels mixed disclosure history and withholds category growth across the source-type change", () => {
    const { payload, company } = fixture();
    const old = payload.reported_segment_reconciliation.history["2025"];
    old.source_type = "reported_segment_reconciliation";
    old.basis = "2025 gross segment disclosure";
    old.items.forEach(item => { item.source_type = "reported_segment_reconciliation"; });
    const value = company();
    const first = firstRow(renderToStaticMarkup(<HowItMakesMoney company={value} />));
    expect(first).not.toContain("19.7%");
    expect(first).not.toContain("15.9%");
    const history = renderToStaticMarkup(<RevenueOfferingHistory company={value} />);
    expect(history).toContain("each period’s verified disclosure");
    expect(history).toContain("2025 gross segment disclosure");
    expect(history).toContain("FY2026: 2026 net operating revenue note");
    // Total revenue remains comparable even though the category disclosure changed.
    expect(history).toContain('class="offering-growth-arrow positive"');
    expect(history).toContain("+50.0%");
  });
  it("withholds category and total growth across a change of reporting basis while retaining amounts", () => {
    const { payload, company } = fixture();
    payload.reported_segment_reconciliation.history["2025"].reporting_basis = "standalone";
    payload.annual_totals["2025"].reporting_basis = "standalone";
    const value = company();
    const first = firstRow(renderToStaticMarkup(<HowItMakesMoney company={value} />));
    expect(first).not.toContain("19.7%");
    expect(first).not.toContain("15.9%");
    const history = renderToStaticMarkup(<RevenueOfferingHistory company={value} />);
    expect(history).toContain("Standalone · INR");
    expect(history).toContain("Consolidated · INR");
    expect(history).toContain("₹200M");
    expect(history).toContain("₹300M");
    expect(history).not.toContain('class="offering-growth-arrow');
  });
  it("does not invent comparison metadata when a required annual source or basis is missing", () => {
    for (const key of ["source_type", "reporting_basis"] as const) {
      const { payload, company } = fixture();
      delete (payload.reported_segment_reconciliation.history["2025"] as Partial<typeof payload.reported_segment_reconciliation.history["2025"]>)[key];
      if (key === "source_type") payload.reported_segment_reconciliation.history["2025"].items.forEach(item => { delete (item as Partial<typeof item>).source_type; });
      const first = firstRow(renderToStaticMarkup(<HowItMakesMoney company={company()} />));
      expect(first).not.toContain("19.7%");
      expect(first).not.toContain("15.9%");
    }
  });
  it("checks item-level disclosure semantics even if the containing point has the latest note source", () => {
    const { payload, company } = fixture();
    payload.reported_segment_reconciliation.history["2025"].items[0].source_type = "reported_segment_reconciliation";
    expect(categoryPeriodsAreComparable(revenueBreakdownCategories(company())[0], ["2026", "2025"])).toBe(false);
    expect(firstRow(renderToStaticMarkup(<HowItMakesMoney company={company()} />))).not.toContain("19.7%");
  });
  it("preserves the authoritative latest annual API growth and sources over unenriched same-year history", () => {
    const { payload, company } = fixture();
    const latest = payload.reported_segment_reconciliation;
    const duplicate = latest.history["2026"];
    duplicate.items.forEach(item => {
      delete (item as Partial<typeof item>).yoy_growth_percent;
      delete (item as Partial<typeof item>).three_period_cagr_percent;
      item.sources = [{ source_url: "https://issuer.test/history-only" }];
    });
    duplicate.basis = "Unenriched historical record";
    latest.items[0].sources = [{ source_url: "https://issuer.test/current-calculation" }];
    const value = company();
    const story = value.companyStory!.revenueOfferings!.segmentReconciliation!;
    expect(revenueBreakdownPoint(value, "2026")).toBe(story);
    const category = revenueBreakdownCategories(value)[0];
    expect(category.yoyGrowthPercentByPeriod!["2026"]).toBe(19.7);
    expect(category.threePeriodCagrPercentByPeriod!["2026"]).toBe(15.9);
    expect(category.sourcesByPeriod!["2026"]).toEqual(["https://issuer.test/current-calculation"]);
    expect(category.sourcesByPeriod!["2025"]).toEqual(["https://issuer.test/2025/note"]);
    const first = firstRow(renderToStaticMarkup(<HowItMakesMoney company={value} />));
    expect(first).toContain("19.7%");
    expect(first).toContain("15.9%");
  });
  it("does not resurrect stale same-year categories omitted from the authoritative annual partition", () => {
    const { payload, company } = fixture();
    const history = payload.reported_segment_reconciliation.history["2026"];
    history.items.push({ ...history.items[0], id: "stale", label: "Stale category" });
    const categories = revenueBreakdownCategories(company());
    expect(categories.map(item => item.id)).toEqual(["products", "incentive"]);
    expect(renderToStaticMarkup(<HowItMakesMoney company={company()} />)).not.toContain("Stale category");
  });
  it("keeps unsupported prior-year comparisons unavailable even when current annual growth is supplied", () => {
    const { payload, company } = fixture();
    delete (payload.reported_segment_reconciliation.history as Partial<typeof payload.reported_segment_reconciliation.history>)["2025"];
    const value = company();
    const category = revenueBreakdownCategories(value)[0];
    expect(category.yoyGrowthPercentByPeriod!["2026"]).toBe(19.7);
    expect(categoryPeriodsAreComparable(category, ["2026", "2025"])).toBe(false);
    const first = firstRow(renderToStaticMarkup(<HowItMakesMoney company={value} />));
    expect(first).not.toContain("19.7%");
    expect(first).not.toContain("15.9%");
  });
  it("keeps the existing US segment captions and source disclosure even with optional API metadata", () => {
    const { payload, company } = fixture();
    payload.reported_segment_reconciliation.period_type = "ttm";
    payload.reported_segment_reconciliation.revenue_display = display("400");
    payload.reported_segment_reconciliation.items[0].revenue_display = display("360");
    payload.reported_segment_reconciliation.items[1].revenue_display = display("40");
    const value = { ...company(), id: "data-0000789019", country: "USA", exchange: "NASDAQ", currency: "US$ millions" } as Company;
    const first = renderToStaticMarkup(<HowItMakesMoney company={value} />);
    expect(first).toContain("Reported segments include intersegment sales");
    expect(first).toContain("Business Segment");
    expect(first).not.toContain("Issuer-disclosed revenue-note category");
    const history = renderToStaticMarkup(<RevenueOfferingHistory company={value} />);
    expect(history).toContain("The chart retains reported segment sales for every period");
    expect(history).not.toContain("FY2025: 2025 net operating revenue note");
    const legacyPayload = structuredClone(payload);
    for (const point of [legacyPayload.reported_segment_reconciliation, ...Object.values(legacyPayload.reported_segment_reconciliation.history)]) {
      delete (point as Partial<typeof point>).source_type;
      delete (point as Partial<typeof point>).reporting_basis;
      if (point !== legacyPayload.reported_segment_reconciliation) {
        delete (point as Partial<typeof point>).basis;
        delete (point as Partial<typeof point>).methodology;
      }
      point.items.forEach(item => { delete (item as Partial<typeof item>).source_type; });
    }
    const legacy = { ...value, companyStory: { revenueOfferings: revenueOfferingStory(legacyPayload) } };
    expect(first).toBe(renderToStaticMarkup(<HowItMakesMoney company={legacy} />));
    expect(history).toBe(renderToStaticMarkup(<RevenueOfferingHistory company={legacy} />));
  });
});
