import { describe, expect, it } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import { revenueOfferingStory } from "../functions/dataProvider";
import { HowItMakesMoney } from "./CompanyStoryHome";
import { RevenueOfferingHistory } from "./RevenueOfferingHistory";
import { periodRevenueReportingLabel, periodsHaveComparableOperationsScope } from "./revenueOfferingMath";
import type { Company, OperationsScope } from "./types";

function company(priorScope: OperationsScope | null = "whole", priorGroup = "other"): Company {
  return { id: "data-IN:EXAMPLE", country: "India", exchange: "NSE", name: "Example", symbol: "EXAMPLE", sector: "Manufacturing",
    description: "Source-backed fixture", reportingPeriod: "FY2025", updatedAt: "2026-10-07",
    notes: { growth: "", profitability: "", cash: "", debt: "" },
    currency: "INR millions", reportingBasis: "consolidated",
    metrics: { revenue: [{ year: 2024, value: 1000 }, { year: 2025, value: 1200 }], operatingMargin: [], freeCashFlow: [], netDebt: [] },
    companyStory: { revenueOfferings: {
      annualTotals: { "2024": 1000, "2025": 1200 }, annualPeriodEnds: { "2024": "2024-03-31", "2025": "2025-03-31" },
      annualReportingBases: { "2024": "consolidated", "2025": "consolidated" },
      annualOperationsScopes: { "2024": priorScope, "2025": "continuing" },
      annualOperationsScopeGroups: { "2024": priorGroup, "2025": "proven-comparatives" },
      offerings: [{ id: "product", label: "Products", revenueByPeriod: { "2024": 1000, "2025": 1200 } }],
    } } } as Company;
}

describe("source-backed Indian operating scope", () => {
  it.each(["whole", "undetermined", null] as const)("keeps %s history and withholds an unproved comparison", prior => {
    const c = company(prior);
    expect(periodsHaveComparableOperationsScope(c, [2024, 2025])).toBe(false);
    const html = renderToStaticMarkup(<RevenueOfferingHistory company={c} />);
    expect(html).toContain("₹1.00B");
    expect(html).toContain("₹1.20B");
    expect(html).toContain("Continuing operations");
    expect(html).not.toContain("+20.0%");
    expect(html).toContain('class="offering-chart-scroll"');
    expect(html).toContain('class="revenue-adjustment-grid"');
  });
  it("requires the same primary comparative proof group even for two continuing labels", () => {
    expect(periodsHaveComparableOperationsScope(company("continuing"), [2024, 2025])).toBe(false);
    const c = company("continuing", "proven-comparatives");
    expect(periodsHaveComparableOperationsScope(c, [2024, 2025])).toBe(true);
    expect(renderToStaticMarkup(<RevenueOfferingHistory company={c} />)).toContain("+20.0%");
  });
  it("labels the current revenue scope in the existing page subtitle and shared layout", () => {
    const html = renderToStaticMarkup(<HowItMakesMoney company={company()} />);
    expect(html).toContain("Revenue breakdown for FY2025 · Continuing operations");
    expect(html).toContain('class="segment-donut-wrap"');
    expect(html).toContain('class="segment-table"');
    expect(html).not.toContain("20.0%");
  });
  it("passes Data scope/group declarations through without changing financial values", () => {
    const display = { value: "1200", unit: "INR millions" };
    const result = revenueOfferingStory({ offerings: [], annual_totals: {
      "2025": { revenue_display: display, operations_scope: "continuing", operations_scope_comparability_group: "proof" },
    }, ttm: { period_end: "2025-06-30", revenue_display: display, prior_year_revenue_display: null,
      operations_scope: "continuing", operations_scope_comparability_group: "proof" } });
    expect(result?.annualTotals?.["2025"]).toBe(1200);
    expect(result?.annualOperationsScopes).toEqual({ "2025": "continuing" });
    expect(result?.annualOperationsScopeGroups).toEqual({ "2025": "proof" });
    expect(result?.ttm?.operationsScope).toBe("continuing");
    expect(result?.ttm?.priorYearRevenue).toBeNull();
  });
  it("keeps ordinary Indian and US company layout and comparisons unchanged", () => {
    const ordinary = company();
    delete ordinary.companyStory!.revenueOfferings!.annualOperationsScopes;
    delete ordinary.companyStory!.revenueOfferings!.annualOperationsScopeGroups;
    expect(periodsHaveComparableOperationsScope(ordinary, [2024, 2025])).toBe(true);
    expect(periodRevenueReportingLabel(ordinary, 2025)).toBe("INR");
    const us = { ...company(), id: "data-US", country: "USA", exchange: "NASDAQ", currency: "US$ millions" } as Company;
    expect(periodsHaveComparableOperationsScope(us, [2024, 2025])).toBe(true);
    expect(periodRevenueReportingLabel(us, 2025)).toBeNull();
    expect(renderToStaticMarkup(<RevenueOfferingHistory company={us} />)).not.toContain("Continuing operations");
    const payload = revenueOfferingStory({ offerings: [], annual_totals: { "2025": { revenue_display: { value: "1200", unit: "USD millions" } } } });
    expect(payload).not.toHaveProperty("annualOperationsScopes");
    expect(payload).not.toHaveProperty("annualOperationsScopeGroups");
  });
});
