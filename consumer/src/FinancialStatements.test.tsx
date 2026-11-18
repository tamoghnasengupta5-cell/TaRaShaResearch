import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { formatStatementValue, StatementExplorer } from "./FinancialStatements";
import { CompanyStoryHome } from "./CompanyStoryHome";
import type { Company } from "./types";

const company = { id: "pcar", name: "PACCAR", symbol: "PCAR", sector: "Trucks", description: "Test issuer", reportingPeriod: "FY2025", updatedAt: "17 October 2026",
  notes: { growth: "", profitability: "", cash: "", debt: "" }, currency: "US$ millions", dataMode: "tarasha-data",
  metrics: { revenue: [], operatingMargin: [], freeCashFlow: [], netDebt: [] },
  supportedStoryChapters: [1, 2, 3, 4], statements: [{ key: "kpis", label: "KPIs", annualPeriods: ["2005-12-31", "2018-12-31", "2025-12-31"],
    ttmPeriodEnd: "2026-06-30", facts: [{ key: "truck_deliveries", label: "Truck Deliveries", description: "Issuer filed units", unit: "trucks",
      values: [{ year: 2005, value: 1523 }, { year: 2025, value: 0 }],
      annualSources: [{ year: 2018, periodEnd: "2018-12-31", periodStart: "2018-01-01", displayUnit: "trucks", value: null, unit: "trucks",
        sourceUrl: null, provenance: null, status: "unavailable", reason: "SEC unit scope unavailable" }], ttm: null }] }] } as Company;

describe("accessible complete financial history", () => {
  it("renders source-unavailable null-unit annual and TTM cells without adding a currency or zero", () => {
    const missing = structuredClone(company), reason = "Four current regular quarters are unavailable.";
    const fact = { key: "dividend_per_share", label: "Regular Dividend Per Share", description: "SEC regular quarter sum", unit: null,
      values: [], annualSources: [{ year: 2025, periodEnd: "2025-12-31", periodStart: null, displayUnit: null, value: null, unit: null,
        sourceUrl: null, provenance: null, status: "unavailable", reason }],
      ttm: { value: null, periodEnd: "2026-06-30", periodStart: null, unit: null, sourceUrl: null, provenance: null, status: "unavailable", reason } };
    missing.statements = [{ key: "income", label: "Income statement", annualPeriods: ["2025-12-31"], ttmPeriodEnd: "2026-06-30", facts: [fact] }];
    const html = renderToStaticMarkup(<StatementExplorer company={missing} initialGroupKey="income" />);
    expect(html).toContain("Regular Dividend Per Share");
    expect(html.match(new RegExp(`title="${reason}">—`, "g"))).toHaveLength(2);
    expect(html).not.toContain("$0");
    expect(formatStatementValue(missing, fact, 0)).toBe("—");
  });
  it("exposes Financials alongside supported company story chapters", () => {
    const html = renderToStaticMarkup(<CompanyStoryHome company={company} watched={false} toggleWatch={() => {}} backToDiscover={() => {}} />);
    expect(html).toContain("Financials");
    expect(html).toContain("Statements, ratios and KPIs");
  });
  it("retains old periods, API-declared empty columns, zero units and a genuinely missing TTM", () => {
    const html = renderToStaticMarkup(<StatementExplorer company={company} initialGroupKey="kpis" />);
    expect(html.match(/role="tab"/g)).toHaveLength(6);
    expect(html).toContain('aria-label="Financial tables"');
    expect(html).toContain('role="tabpanel"');
    for (const year of [2005, 2018, 2025]) expect(html).toContain(`FY ${year}`);
    expect(html).toContain('title="SEC unit scope unavailable">—');
    expect(html).toContain("1,523");
    expect(html).toContain(">0</td>");
    expect(html).not.toContain("$1,523");
    expect(html).toContain("Ended 2026-06-30");
    expect(html).not.toContain("$0m");
  });
  it("keeps a supplementary table unavailable when its API payload is absent", () => {
    const html = renderToStaticMarkup(<StatementExplorer company={company} initialGroupKey="ratios" />);
    expect(html).toContain("has not returned source-backed ratios");
    expect(html).not.toContain("Truck Deliveries");
  });
});
