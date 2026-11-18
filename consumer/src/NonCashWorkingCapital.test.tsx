import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { NonCashWorkingCapital } from "./NonCashWorkingCapital";
import type { CompanyNcwcCalculation } from "./types";

const worksheet: CompanyNcwcCalculation = {
  status: "available", cashInputPolicy: "cash_and_cash_equivalents_plus_current_investments", reason: null, formula: "(Current assets − cash input) − (Current liabilities − current debt)",
  priorFiscalYear: 2024, currentFiscalYear: 2025, priorPeriodEnd: "2024-12-31", currentPeriodEnd: "2025-12-31",
  priorNcwc: 220, currentNcwc: 210, priorOperatingCurrentAssets: 400, currentOperatingCurrentAssets: 480, cashEffect: 10,
  inputs: [
    { key: "current_assets", label: "Current assets", operator: "", priorValue: 500, currentValue: 600 },
    { key: "cash_input", label: "Cash Input", operator: "−", priorValue: 100, currentValue: 120 },
    { key: "current_liabilities", label: "Current Liabilities", operator: "−", priorValue: 200, currentValue: 300 },
    { key: "current_debt", label: "Current Debt", operator: "+", priorValue: 0, currentValue: 0 },
  ].map(input => ({ ...input, priorDetailStatus: input.key === "current_debt" ? "assumed_zero" : "complete",
    currentDetailStatus: input.key === "current_debt" ? "assumed_zero" : "complete",
    priorNote: null, currentNote: null,
    components: input.key === "current_debt" ? [] : [{ key: "example", label: "Filed constituent", priorValue: input.priorValue,
      currentValue: input.currentValue, priorSourceUrl: "https://www.sec.gov/2024.xml", currentSourceUrl: "https://www.sec.gov/2025.xml" }],
  })) as CompanyNcwcCalculation["inputs"],
};

describe("NCWC disclosure worksheet", () => {
  it("discloses an assumed classification even while its component rows are collapsed", () => {
    const calculation = { ...worksheet, inputs: worksheet.inputs.map(input => input.key !== "current_assets" ? input : {
      ...input, label: "Current assets (NCWC proxy)", currentDetailStatus: "classification_proxy",
      currentNote: "Gross one-year receipts replace the multi-year financing portfolio.",
    }) };
    const html = renderToStaticMarkup(<NonCashWorkingCapital calculation={calculation} displayUnit="USD millions" />);
    expect(html).toContain("separate from reported current balances");
    expect(html).toContain("Current assets (NCWC proxy)");
    expect(html).toContain("Gross one-year receipts replace the multi-year financing portfolio");
    expect(html.match(/aria-expanded="false"/g)).toHaveLength(4);
  });
  it("provides four independent accessible disclosures and both annual columns", () => {
    const html = renderToStaticMarkup(<NonCashWorkingCapital calculation={worksheet} displayUnit="USD millions" />);
    expect(html.match(/aria-expanded="false"/g)).toHaveLength(4);
    expect(html.match(/aria-controls=/g)).toHaveLength(4);
    expect(html.match(/class="cash-ncwc-components"[^>]*hidden=""/g)).toHaveLength(4);
    expect(html).toContain("FY2024"); expect(html).toContain("FY2025");
    expect(html).toContain("220.00 − 210.00 = +10.00");
    expect(html).toContain("Cash released");
    expect(html).toContain("not an SEC-reported zero");
    expect(html).toContain("cash and cash equivalents plus current investments");
    expect(html).toContain('href="https://www.sec.gov/2025.xml"');
  });
  it("shows both cash and current investments with their SEC sources under the expandable input", () => {
    const calculation = { ...worksheet, inputs: worksheet.inputs.map(input => input.key !== "cash_input" ? input : {
      ...input, label: "Cash and current investments", priorValue: 8303.5, currentValue: 10743,
      components: [
        { key: "cash", label: "Cash and cash equivalents", priorValue: 2762.4, currentValue: 1963.9,
          priorSourceUrl: "https://www.sec.gov/prior.xml", currentSourceUrl: "https://www.sec.gov/current.xml" },
        { key: "short_term_investments", label: "Current investments", priorValue: 5541.1, currentValue: 8779.1,
          priorSourceUrl: "https://www.sec.gov/prior.xml", currentSourceUrl: "https://www.sec.gov/current.xml" },
      ],
    }) };
    const html = renderToStaticMarkup(<NonCashWorkingCapital calculation={calculation} displayUnit="USD millions" />);
    expect(html).toContain("Cash and current investments");
    expect(html).toContain("Current investments");
    expect(html).toContain("8,779.10");
    expect(html).toContain("10,743.00");
    expect(html).toContain("Current investments, FY2025, SEC source");
  });
  it("distinguishes investment and unavailable values without inventing zero components", () => {
    const outflow = renderToStaticMarkup(<NonCashWorkingCapital calculation={{ ...worksheet, cashEffect: -40 }} displayUnit="USD millions" />);
    expect(outflow).toContain("Cash invested");
    const html = renderToStaticMarkup(<NonCashWorkingCapital calculation={{ ...worksheet, status: "unavailable", cashEffect: null,
      currentNcwc: null, inputs: worksheet.inputs.map(input => ({ ...input, currentValue: null })) }} displayUnit="USD millions" />);
    expect(html).toContain("Unavailable");
    expect(html).toContain("—");
    expect(html).toContain("No separate component detail reported");
  });
});
