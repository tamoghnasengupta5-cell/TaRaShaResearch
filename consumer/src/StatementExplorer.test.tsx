import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { StatementExplorer } from "./App";
import type { Company, StatementGroup } from "./types";

describe("TTM financial statement column", () => {
  it.each(["income", "balance", "cash", "shares"] as const)("shows the exact latest TTM period and retains zeros for %s", key => {
    const company = { currency: "US$ millions", dataMode: "tarasha-data", statements: [{ key, label: key, ttmPeriodEnd: "2026-06-30", facts: [{
      key: "value", label: "Value", description: "SEC input", unit: "US$ million", values: [{ year: 2025, value: 100 }],
      ttm: { value: 0, periodEnd: "2026-06-30", periodStart: "2025-07-01", unit: "USD millions", sourceUrl: null, provenance: null },
    }] }] } as Company;
    const markup = renderToStaticMarkup(<StatementExplorer company={company} />);
    expect(markup).toContain("FY 2025");
    expect(markup).toContain("Ended 2026-06-30");
    expect(markup).toContain("$0m");
    expect(markup).not.toContain("FY 2026");
  });
  it("shows an unavailable TTM and never repeats an annual amount", () => {
    const company = { currency: "US$ millions", statements: [{ key: "income", label: "Income", ttmPeriodEnd: null, facts: [{
      key: "revenue", label: "Revenue", description: "SEC input", unit: "US$ million", values: [{ year: 2025, value: 100 }], ttm: null,
    }] }] } as Company;
    const markup = renderToStaticMarkup(<StatementExplorer company={company} />);
    expect(markup).toContain("TTM<small>Unavailable</small>");
    expect(markup.match(/\$100m/g)).toHaveLength(1);
    expect(markup).toContain("<td>—</td>");
  });
  it("uses explicit TTM units for fractional percentages and plain financial ratios", () => {
    const facts: StatementGroup["facts"] = [
      { key: "gross_margin", label: "Margin", description: "", unit: "ratio", values: [], ttm: { value: -3.93891, periodEnd: "2026-06-30", periodStart: null, unit: "ratio", displayFormat: "percentage", sourceUrl: null, provenance: null } },
      { key: "current_ratio", label: "Current Ratio", description: "", unit: "ratio", values: [], ttm: { value: 3.91, periodEnd: "2026-06-30", periodStart: null, unit: "ratio", displayFormat: "number", sourceUrl: null, provenance: null } },
      { key: "foreign", label: "Foreign currency", description: "", unit: "US$ million", values: [], ttm: { value: 42, periodEnd: "2026-06-30", periodStart: null, unit: "EUR millions", sourceUrl: null, provenance: null } },
    ];
    const company = { currency: "US$ millions", statements: [{ key: "income", label: "Income", ttmPeriodEnd: "2026-06-30", facts }] } as Company;
    const markup = renderToStaticMarkup(<StatementExplorer company={company} />);
    expect(markup).toContain("-393.89%");
    expect(markup).toContain("<span>3.91</span>");
    expect(markup).not.toContain("391.0%");
    expect(markup).toContain("<span>42</span><small>EUR millions");
    expect(markup).not.toContain("$42m");
  });
});

describe("Reported components of combined assets", () => {
  it("shows an unavailable combined row and keeps reported amounts in a disclosure", () => {
    const company = { currency: "US$ millions", statements: [{ key: "balance", label: "Balance", ttmPeriodEnd: null, facts: [
      { key: "combined", label: "Net PP&E including lease assets", description: "Combined assets", unit: "US$ million", values: [], presentationRequired: true,
        periodScope: [{ period: { type: "annual", end: "2025-12-31" }, status: "unavailable", reason: "SEC inclusion scope unknown" }] },
      { key: "reported", label: "Reported net PP&E", description: "Filed amount", unit: "US$ million", values: [{ year: 2025, value: 80 }], parentKey: "combined" },
      { key: "lease", label: "Reported lease right-of-use assets", description: "Filed amount", unit: "US$ million", values: [{ year: 2025, value: 20 }], parentKey: "combined" },
    ] }] } as Company;
    const markup = renderToStaticMarkup(<StatementExplorer company={company} />);
    expect(markup).toContain("Net PP&amp;E including lease assets");
    expect(markup).toContain('<td title="SEC inclusion scope unknown">—</td>');
    const detail = markup.slice(markup.indexOf("<details>"), markup.indexOf("</details>"));
    expect(detail).toContain("<summary>Reported components</summary>");
    expect(detail).toContain("Reported net PP&amp;E");
    expect(detail).toContain("$80m");
    expect(detail).toContain("$20m");
    expect(markup).not.toContain("$100m");
  });
});
