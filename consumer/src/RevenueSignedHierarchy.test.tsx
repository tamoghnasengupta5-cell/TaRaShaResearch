import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { revenueOfferingStory } from "../functions/dataProvider";
import { HowItMakesMoney } from "./CompanyStoryHome";
import { RevenueOfferingHistory } from "./RevenueOfferingHistory";
import type { Company } from "./types";

const display = (value: number) => ({ value: String(value), unit: "USD millions" });
function fixture(positive: number[], deduction: number, total: number): Company {
  const items = [...positive.map((value, index) => ({ id: `category-${index}`, label: `Category ${index + 1}`, role: "segment" as const, revenue_display: display(value), sources: [] })),
    { id: "other", label: "Other", role: "reconciliation" as const, revenue_display: display(deduction), sources: [] }];
  return { id: "source-hierarchy", name: "Source issuer", symbol: "ISSUER", sector: "Manufacturing", description: "Filed revenue hierarchy",
    reportingPeriod: "FY2025", updatedAt: "2026-10-17", notes: { growth: "", profitability: "", cash: "", debt: "" },
    country: "USA", exchange: "NASDAQ", currency: "US$ millions",
    metrics: { revenue: [{ year: 2025, value: total }], operatingMargin: [], freeCashFlow: [], netDebt: [] },
    companyStory: { revenueOfferings: revenueOfferingStory({ offerings: [], annual_totals: {
      "2025": { period_end: "2025-12-31", revenue_display: display(total) },
    }, ttm: { period_end: "2026-06-30", revenue_display: display(total), prior_year_revenue_display: null }, reported_segment_reconciliation: {
      basis: "Source-verified complete signed SEC hierarchy", methodology: "Exact complete child-to-parent reconciliation",
      source_type: "reported_revenue_hierarchy", period_type: "ttm", period_end: "2026-06-30", revenue_display: display(total), items,
      history: { "2025": { source_type: "reported_revenue_hierarchy", period_end: "2025-12-31", revenue_display: display(total), items } },
    } }) } } as Company;
}

describe("signed SEC revenue hierarchy presentation", () => {
  it("labels the positive-only donut denominator and retains a separate signed reconciliation", () => {
    const html = renderToStaticMarkup(<HowItMakesMoney company={fixture([60, 40], -10, 90)} />);
    const chart = html.slice(html.indexOf('<svg'), html.indexOf('</svg>'));
    expect(chart).toContain("Category 1: 60.0% of positive disclosures");
    expect(chart).toContain("Category 2: 40.0% of positive disclosures");
    expect(chart).not.toContain("Other:");
    expect(chart).not.toMatch(/stroke-dasharray="-/);
    expect(html).toContain("TTM Positive disclosures");
    expect(html).toContain("Positive disclosures $100M");
    expect(html).toContain("Other (−$10M)");
    expect(html).toContain("company revenue $90M");
    expect(html).toContain("66.7%"); // Table share still uses net company revenue.
    expect(html).not.toContain("Reported segments include intersegment sales");
  });
  it("keeps PACCAR annual and current TTM signed Other quantities visible", () => {
    const company = fixture([18676, 6919.9, 2225.9], -6, 27815.8);
    const history = company.companyStory!.revenueOfferings!.segmentReconciliation!.history!["2025"];
    history.items = [...[19365.3, 6873.7, 2209.7].map((revenue, index) => ({ id: `category-${index}`, label: `Category ${index + 1}`, role: "segment" as const, revenue, sourceUrls: [] })),
      { id: "other", label: "Other", role: "reconciliation", revenue: -3.9, sourceUrls: [] }];
    company.companyStory!.revenueOfferings!.annualTotals!["2025"] = 28444.8;
    company.metrics.revenue[0].value = 28444.8;
    const current = renderToStaticMarkup(<HowItMakesMoney company={company} />);
    expect(current).toContain("Other (−$6M)");
    expect(current).toContain("Positive disclosures $27,821.8M");
    expect(current).toContain("company revenue $27,815.8M");
    const past = renderToStaticMarkup(<RevenueOfferingHistory company={company} />);
    expect(past).toContain("−$3.9M");
    expect(past).toContain("−$6M");
  });
});
