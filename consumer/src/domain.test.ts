import { describe, expect, it } from "vitest";
import { companySearch, formatMetric, percentChange } from "./domain";
import { companies } from "./data/demo";

describe("consumer metric helpers", () => {
  it("calculates the latest annual change", () => {
    expect(percentChange([{ year: 2025, value: 100 }, { year: 2026, value: 110 }])).toBe(10);
  });

  it("formats consumer-friendly values", () => {
    expect(formatMetric("operatingMargin", 14.04)).toBe("14.0%");
    expect(formatMetric("netDebt", -520)).toBe("−₹520 cr");
  });

  it("searches by ticker and sector", () => {
    expect(companySearch(companies[0], "aarohan", "Consumer staples")).toBe(true);
    expect(companySearch(companies[0], "aarohan", "Healthcare")).toBe(false);
  });
});
