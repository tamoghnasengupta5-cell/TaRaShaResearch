import { describe, expect, it } from "vitest";
import { backfillResearchDistributions } from "./liveData";
import type { ProfitabilityMetricKey, ResearchShelfAnalysis } from "./types";

const profitabilityMetrics: ProfitabilityMetricKey[] = ["grossMargin", "operatingMargin", "cogsRatio", "sgaRatio", "daRatio", "rdRatio"];

function legacyStatistics() {
  return { median: 10, standardDeviation: 2, observations: 2 };
}

describe("Research API compatibility", () => {
  it("accepts legacy statistics and backfills only defensible company distributions", () => {
    const statistics = Object.fromEntries(profitabilityMetrics.map((metric) => [metric, legacyStatistics()]));
    const industryStatistics = Object.fromEntries(profitabilityMetrics.map((metric) => [metric, legacyStatistics()]));
    const shelf = {
      growthComparisons: {
        revenue: { company: legacyStatistics(), industryBucket: legacyStatistics() },
        grossProfit: { company: legacyStatistics(), industryBucket: legacyStatistics() },
        operatingIncome: { company: legacyStatistics(), industryBucket: legacyStatistics() },
      },
      companyDeltas: [{
        fromYear: 2024,
        toYear: 2025,
        revenue: 20,
        revenueChangePercent: 10,
        grossProfit: 12,
        grossProfitChangePercent: 15,
        operatingIncome: 8,
        operatingIncomeChangePercent: 25,
      }],
      profitability: {
        statistics,
        industryStatistics,
        yearly: [{
          year: 2025,
          grossMargin: 42,
          grossProfit: 42,
          operatingMargin: 18,
          operatingIncome: 18,
          cogsRatio: 58,
          cogs: 58,
          sgaRatio: 12,
          sga: 12,
          daRatio: 4,
          da: 4,
          rdRatio: 6,
          rd: 6,
        }],
      },
    } as unknown as ResearchShelfAnalysis;

    backfillResearchDistributions(shelf);

    expect(shelf.growthComparisons.revenue.company.distribution).toEqual([
      { label: "Company · FY 2024–2025", value: 10 },
    ]);
    expect(shelf.growthComparisons.revenue.industryBucket.distribution).toEqual([]);
    expect(shelf.profitability.statistics.grossMargin.distribution).toEqual([
      { label: "Company · FY 2025", value: 42 },
    ]);
    expect(shelf.profitability.industryStatistics.grossMargin.distribution).toEqual([]);
  });
});
