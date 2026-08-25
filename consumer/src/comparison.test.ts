import { describe, expect, it } from "vitest";
import { comparisonProfitabilityYears, comparisonYoYIntervals, MAX_COMPARISON_COMPANIES, researchShelfCompanies, toggleComparisonCompany } from "./comparison";
import type { Company, CompanyDeltaPoint, ProfitabilityYearPoint, ResearchShelfAnalysis } from "./types";

function company(id: string, onResearchShelf = true, companyDeltas: CompanyDeltaPoint[] = [], profitabilityYearly: ProfitabilityYearPoint[] = []): Company {
  return {
    id,
    name: id,
    symbol: id.toUpperCase(),
    sector: "Test",
    description: "Test company",
    currency: "US$ millions",
    reportingPeriod: "FY 2026",
    updatedAt: "28 July 2026",
    metrics: { revenue: [], operatingMargin: [], freeCashFlow: [], netDebt: [] },
    researchShelf: onResearchShelf ? ({ companyDeltas, profitability: { yearly: profitabilityYearly } } as ResearchShelfAnalysis) : undefined,
    notes: { growth: "", profitability: "", cash: "", debt: "" },
  };
}

describe("Compare research-shelf selection", () => {
  it("offers only companies pulled onto the Discover research shelf", () => {
    expect(researchShelfCompanies([company("a"), company("b", false)]).map((item) => item.id)).toEqual(["a"]);
  });

  it("allows no more than five available companies", () => {
    const ids = ["a", "b", "c", "d", "e", "f"];
    let selected: string[] = [];
    for (const id of ids) selected = toggleComparisonCompany(selected, id, ids);

    expect(selected).toHaveLength(MAX_COMPARISON_COMPANIES);
    expect(selected).toEqual(ids.slice(0, MAX_COMPARISON_COMPANIES));
  });

  it("removes unavailable and deselected companies", () => {
    expect(toggleComparisonCompany(["a", "removed"], "a", ["a", "b"])).toEqual([]);
  });

  it("aligns YoY values by fiscal interval without filling missing company periods", () => {
    const point = (fromYear: number, toYear: number, revenueChangePercent: number): CompanyDeltaPoint => ({
      fromYear,
      toYear,
      revenue: null,
      revenueChangePercent,
      grossProfit: null,
      grossProfitChangePercent: revenueChangePercent + 1,
      grossOperatingLeverage: null,
      operatingIncome: null,
      operatingIncomeChangePercent: revenueChangePercent + 2,
    });
    const result = comparisonYoYIntervals([
      company("a", true, [point(2022, 2023, 10), point(2023, 2024, 20)]),
      company("b", true, [point(2023, 2024, 30)]),
    ], "revenue");

    expect(result).toEqual([
      { fromYear: 2022, toYear: 2023, values: [10, null] },
      { fromYear: 2023, toYear: 2024, values: [20, 30] },
    ]);
  });

  it("aligns profitability percentages and their absolute values by fiscal year", () => {
    const point = (year: number, grossMargin: number, grossProfit: number): ProfitabilityYearPoint => ({
      year,
      grossMargin,
      grossProfit,
      operatingMargin: null,
      operatingIncome: null,
      cogsRatio: null,
      cogs: null,
      sgaRatio: null,
      sga: null,
      daRatio: null,
      da: null,
      rdRatio: null,
      rd: null,
    });
    const result = comparisonProfitabilityYears([
      company("a", true, [], [point(2023, 40, 400), point(2024, 42, 450)]),
      company("b", true, [], [point(2024, 35, 300)]),
    ], "grossMargin");

    expect(result).toEqual([
      { year: 2023, values: [40, null], absoluteValues: [400, null] },
      { year: 2024, values: [42, 35], absoluteValues: [450, 300] },
    ]);
  });
});
