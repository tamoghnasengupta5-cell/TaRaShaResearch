import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";
import type { Company } from "./types";
vi.mock("react", async original => {
  const react = await original<typeof import("react")>();
  return { ...react, useState: <T,>(initial: T | (() => T)) => react.useState(
    initial === "makes-money" ? "becomes-cash" as T : initial) };
});
import { CompanyStoryHome } from "./CompanyStoryHome";

function fixture(country: "India" | "USA" = "India", available = true): Company {
  return { id: "fixture", name: "Cash availability fixture", symbol: "CASH", country,
    exchange: country === "India" ? "NSE" : "NASDAQ", currency: "INR millions",
    sector: "Industrial", description: "Test fixture", reportingPeriod: "FY 2026", updatedAt: "7 October 2026",
    annualCurrencyByYear: { "2024": "INR", "2025": "INR", "2026": "USD" },
    metrics: { revenue: [{ year: 2024, value: 1000 }, { year: 2025, value: 1100 }, { year: 2026, value: 1200 }], operatingMargin: [], freeCashFlow: [], netDebt: [] },
    notes: { growth: "", profitability: "", cash: "", debt: "" },
    companyStory: { cashConversion: {
      status: available ? "available" : "unavailable", reason: available ? null : "Required inputs are unavailable.",
      latestFiscalYear: available ? 2024 : null, latestPeriodEnd: available ? "2024-03-31" : null,
      reportingCurrency: "INR", displayUnit: "INR millions", years: available ? [2024] : [],
      bridge: available ? [{ key: "fcff", label: "Free Cash Flow to the Firm", operation: "total", value: 50,
        formula: "Existing formula", lineage: { kind: null, concept: null, derivationMethod: null, formula: null, sourceUrl: null }, components: [] }] : [],
      trend: available ? [{ fiscalYear: 2024, periodEnd: "2024-03-31", reportingCurrency: "INR", ebit: 100,
        taxRatePercent: 25, taxesOnOperatingProfit: 25, nopat: 75, depreciationAndAmortization: 20,
        workingCapitalImpact: -5, workingCapitalComponents: [], capitalExpenditure: 40, fcff: 50,
        conversionPercent: 50, revenue: 1000, fcffRevenuePercent: 5 }] : [],
      metrics: available ? { fcff: 50, priorFiscalYear: null, priorFcff: null, conversionPercent: 50,
        priorConversionPercent: null, growthPercent: null, priorGrowthPercent: null, revenuePercent: 5,
        priorRevenuePercent: null, cagrPercent: null } : null,
      summary: null, takeaway: null, methodology: "Existing source-backed formula", sourceUrl: null,
      periodGaps: [
        { fiscalYear: 2025, periodEnd: "2025-03-31", reportingBasis: "consolidated", reportingCurrency: "INR",
          status: "unavailable", reasonCode: "incomplete_fcff_inputs", reason: "Required annual FCFF inputs are unavailable.",
          missingInputs: ["effective_tax_rate", "depreciation_amortization", "capital_expenditures"], sourceUrls: ["https://issuer.test/2025.pdf"] },
        { fiscalYear: 2026, periodEnd: "2026-03-31", reportingBasis: "standalone", reportingCurrency: "USD",
          status: "unavailable", reasonCode: "incomplete_fcff_inputs", reason: "Required annual FCFF inputs are unavailable.",
          missingInputs: ["change_operating_working_capital"], sourceUrls: ["https://issuer.test/2026.pdf"] },
      ],
    } },
  };
}
function render(company: Company) {
  return renderToStaticMarkup(<CompanyStoryHome company={company} watched={false} toggleWatch={() => {}} backToDiscover={() => {}} />);
}
describe("source-backed cash calculation year availability", () => {
  it("limits SEC availability diagnostics to the API display window without removing historical gaps", () => {
    const company = fixture("USA", false);
    const story = company.companyStory!.cashConversion!;
    story.displayAnnualYears = [2020,2021,2022,2023,2024,2025,2026];
    story.workingCapitalCalculation = { status: "unavailable", reason: "Missing total",
      formula: "SEC NCWC formula", priorFiscalYear: 2025, currentFiscalYear: 2026,
      priorPeriodEnd: "2025-12-31", currentPeriodEnd: "2026-12-31", priorNcwc: null,
      currentNcwc: null, priorOperatingCurrentAssets: null, currentOperatingCurrentAssets: null,
      cashEffect: null, inputs: [] };
    const oldGap = { ...story.periodGaps![0], fiscalYear: 2015, periodEnd: "2015-12-31" };
    story.periodGaps!.unshift(oldGap);
    const html = render(company);
    expect(html).toContain("2 incomplete years");
    expect(html).toContain("FY2025");
    expect(html).toContain("FY2026");
    expect(html).not.toContain("FY2015");
    expect(story.periodGaps).toHaveLength(3);
    expect(story.periodGaps![0]).toEqual(oldGap);
  });
  it("omits SEC availability diagnostics when only retained older years have gaps", () => {
    const company = fixture("USA", false);
    const story = company.companyStory!.cashConversion!;
    story.displayAnnualYears = [2020,2021,2022,2023,2024,2025,2026];
    story.workingCapitalCalculation = { status: "available", reason: null,
      formula: "SEC NCWC formula", priorFiscalYear: 2025, currentFiscalYear: 2026,
      priorPeriodEnd: "2025-12-31", currentPeriodEnd: "2026-12-31", priorNcwc: 100,
      currentNcwc: 90, priorOperatingCurrentAssets: 200, currentOperatingCurrentAssets: 210,
      cashEffect: 10, inputs: [] };
    story.periodGaps = [{ ...story.periodGaps![0], fiscalYear: 2015, periodEnd: "2015-12-31" }];
    const html = render(company);
    expect(html).not.toContain("Data availability by fiscal year");
    expect(story.periodGaps).toHaveLength(1);
  });
  it("keeps a partially supported SEC bridge, explicit missing badges and collapsed year diagnostics", () => {
    const company = fixture("USA", false);
    const story = company.companyStory!.cashConversion!;
    story.earningsBasis = "operating_income_plus_disclosed_interest_investment_income";
    story.workingCapitalCalculation = { status: "available", reason: null, formula: "SEC NCWC formula",
      priorFiscalYear: 2024, currentFiscalYear: 2025, priorPeriodEnd: "2024-12-31", currentPeriodEnd: "2025-12-31",
      priorNcwc: 100, currentNcwc: 90, priorOperatingCurrentAssets: 200, currentOperatingCurrentAssets: 210, cashEffect: 10, inputs: [] };
    const lineage = { kind: "mapped", concept: null, derivationMethod: null, formula: null, sourceUrl: "https://www.sec.gov/filing.xml" };
    story.partialBridge = { fiscalYear: 2025, lines: [
      { key: "ebit", label: "EBIT including interest and investment income", operation: "base", value: 200, formula: "API formula", lineage, components: [] },
      { key: "capital_expenditure", label: "Capital Expenditure (Capex)", operation: "subtract", value: null, formula: null, lineage, components: [] },
      { key: "fcff", label: "Free Cash Flow to the Firm (FCFF)", operation: "total", value: null, formula: null, lineage, components: [] },
    ] };
    // A complete older result remains in API history, but is not the
    // current annual worksheet or a substitute FCFF result.
    const historical = fixture("USA").companyStory!.cashConversion!.trend[0];
    story.trend = [{ ...historical, fiscalYear: 2019, periodEnd: "2019-12-31", fcff: 9876 }];
    story.latestFiscalYear = story.latestStatementFiscalYear = 2025;
    story.displayAnnualYears = [2019, 2020, 2021, 2022, 2023, 2024, 2025];
    const html = render(company);
    expect(html).toContain("Still needed for FY2025");
    expect(html).toContain("cash-story-grid cash-story-incomplete");
    expect(html).toContain("From Interest-inclusive EBIT to FCFF");
    expect(html).toContain("200.00");
    expect(html).toContain("Input unavailable");
    expect(html).toContain("Awaiting inputs");
    expect(html).toContain('<details class="cash-availability-details">');
    expect(html).toContain("Data availability by fiscal year");
    expect(html).not.toContain("Operating profit to FCFF is not available");
    expect(html).not.toContain("effective_tax_rate");
  });
  it("shows the API interest-inclusive earnings basis and SEC component sources without relabelling operating-profit ratios", () => {
    const company = fixture("USA");
    const story = company.companyStory!.cashConversion!;
    story.earningsBasis = "operating_income_plus_disclosed_interest_investment_income";
    const lineage = { kind: "mapped", concept: null, derivationMethod: null, formula: null,
      sourceUrl: "https://www.sec.gov/Archives/edgar/data/1/filing.xml" };
    story.bridge.unshift({ key: "ebit", label: "EBIT including interest and investment income", operation: "base", value: 120,
      formula: "Operating income + separately disclosed interest and investment income",
      disclosure: "Other non-operating totals are excluded to avoid double counting.", lineage,
      components: [{ key: "operating_income", label: "Operating income", value: 100, lineage },
        { key: "interest_income", label: "Interest and investment income", value: 20, lineage }] });
    const html = render(company);
    expect(html).toContain("From Interest-inclusive EBIT to FCFF");
    expect(html).toContain("Operating income + separately disclosed interest and investment income");
    expect(html).toContain("Interest and investment income");
    expect(html).toContain("Other non-operating totals are excluded to avoid double counting.");
    expect(html).toContain('href="https://www.sec.gov/Archives/edgar/data/1/filing.xml"');
    expect(html).toContain("FCFF / Operating Profit");
    expect(html).not.toContain("From Operating Profit to FCFF");
    expect(html).not.toContain("Net working-capital cash effect");
  });
  it("retains the supported older bridge and explains later missing inputs in existing methodology", () => {
    const html = render(fixture());
    expect(html).toContain("Free Cash Flow to the Firm · FY2024");
    expect(html).toContain('class="story-methodology"');
    expect(html).toContain("FY2025 · Consolidated · INR: FCFF unavailable.");
    expect(html).toContain("effective tax rate, depreciation and amortization, capital expenditure");
    expect(html).toContain("FY2026 · Standalone · USD: FCFF unavailable.");
    expect(html).toContain("operating working-capital cash effects");
    expect(html).not.toContain("Free Cash Flow to the Firm · FY2026");
  });
  it("explains missing years inside the existing wholly-unavailable story", () => {
    const html = render(fixture("India", false));
    expect(html).toContain('class="story-unavailable"');
    expect(html).toContain("FY2025 · Consolidated · INR: FCFF unavailable.");
    expect(html).not.toContain('class="cash-story-grid"');
  });
  it("keeps the existing US layout without India-only availability text", () => {
    const company = fixture("USA");
    const withIndiaFields = render(company);
    delete company.companyStory!.cashConversion!.periodGaps;
    expect(withIndiaFields).toBe(render(company));
    expect(withIndiaFields).not.toContain("FCFF unavailable.");
  });
});
