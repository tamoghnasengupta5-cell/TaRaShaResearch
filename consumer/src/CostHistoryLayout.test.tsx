import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";
import type { Company } from "./types";
vi.mock("react", async original => {
  const react = await original<typeof import("react")>();
  return { ...react, useState: <T,>(initial: T | (() => T)) => react.useState(
    initial === "makes-money" ? "money-goes" as T : initial) };
});
import { CompanyStoryHome } from "./CompanyStoryHome";
export function fixture(yearCount: number, country = "India"): Company {
  const years = Array.from({ length: yearCount }, (_, i) => 2017 + i);
  const india = country === "India";
  const lineage = { kind: null, concept: null, derivationMethod: null, formula: null, sourceUrl: null };
  return { id: "layout-fixture", name: "Annual history fixture", symbol: "LAYOUT", country,
    exchange: india ? "NSE" : "NASDAQ", currency: india ? "INR millions" : "US$ millions", sector: "Industrial", description: "Layout fixture", reportingPeriod: "FY 2026", updatedAt: "7 October 2026", notes: { growth: "", profitability: "", cash: "", debt: "" },
    metrics: { revenue: years.map(year => ({ year, value: 1000 })), operatingMargin: [], freeCashFlow: [], netDebt: [] },
    companyStory: { costStructure: {
      status: "available", reason: null, latestPeriodEnd: years.at(-1) + "-03-31", reportingCurrency: india ? "INR" : "USD", sourceUrl: null, latestFiscalYear: years.at(-1)!, years, summary: "Source-backed cost history", methodology: "Existing API cost calculations",
      lines: [
        { key: "materials", label: "Materials consumed", role: "expense", latestValuePerHundred: -84.29, lineage,
          history: years.filter((_, i) => i !== 1).map(fiscalYear => ({ fiscalYear, periodEnd: fiscalYear + "-03-31", valuePerHundred: -84.29 })) },
        { key: "operating_income", label: "Operating Profit", role: "subtotal", latestValuePerHundred: 8.86, lineage,
          history: years.map(fiscalYear => ({ fiscalYear, periodEnd: fiscalYear + "-03-31", valuePerHundred: 8.86 })) },
      ],
    } },
  } as Company;
}
export function render(company: Company) {
  return renderToStaticMarkup(<CompanyStoryHome company={company} watched={false} toggleWatch={() => {}} backToDiscover={() => {}} />);
}
describe("India annual cost history and US contract upgrade", () => {
  it("retains all ten annual columns, financial text, unavailable values and existing scroll container", () => {
    const html = render(fixture(10));
    expect(html).toContain('class="cost-history-wrap" data-horizontal-scroll="true"');
    expect(html).toContain('class="cost-history-table long-history"');
    const table = html.match(/<table class="cost-history-table.*?<\/table>/)?.[0] ?? "";
    expect(table.match(/<thead>.*?<\/thead>/)?.[0].match(/<th(?:>| )/g)).toHaveLength(11);
    for (let year = 17; year <= 26; year++) expect(table).toContain("FY" + year);
    expect(table.match(/<td[^>]*>\(₹84\.29\)<\/td>/g)).toHaveLength(9);
    expect(table).toContain("<td class=\"\">—</td>");
    expect(table.match(/<td[^>]*>₹8\.86<\/td>/g)).toHaveLength(10);
  });
  it("retains the India table attributes for five or fewer periods", () => {
    for (const count of [1, 5]) for (const country of ["India"]) {
      const html = render(fixture(count, country));
      expect(html).toContain('<table class="cost-history-table">');
      expect(html).not.toContain("long-history");
      expect(html).not.toContain("min-width:");
    }
  });
  it("retains the India historical table when six or more periods are present", () => {
    for (const country of ["India"]) {
      const html = render(fixture(6, country));
      expect(html).toContain('class="cost-history-table long-history"');
      expect(html).toContain("Materials consumed");
      expect(html).toContain("Operating Profit");
    }
  });
});

describe("US cost story API upgrade", () => {
  it("shows three pages and explicit unavailability for the old per-100 contract", () => {
    const html = render(fixture(6, "USA"));
    expect(html).toContain("1 of 3");
    expect(html).toContain("TaRaShaData has not returned the updated SEC cost story");
    expect(html).not.toContain("cost-history-table");
    expect(html).not.toContain("Economics per $100 Revenue");
  });
});
