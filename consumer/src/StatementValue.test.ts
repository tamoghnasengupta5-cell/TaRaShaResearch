import { describe, expect, it } from "vitest";
import { formatStatementValue } from "./App";
import type { Company, StatementFact } from "./types";

const company = { currency: "US$ millions" } as Company;
describe("statement units", () => {
  it.each([[2.571, "257.10%"], [-3.93891, "-393.89%"], [.005, "0.50%"], [0, "0.00%"]])("formats declared fractional ratio %s independently of magnitude", (value, expected) => {
    expect(formatStatementValue(company, { unit: "ratio", displayFormat: "percentage" } as StatementFact, value)).toBe(expected);
  });
  it("preserves plain dimensionless ratios independently of size", () => {
    expect(formatStatementValue(company, { key: "current_ratio", unit: "ratio", displayFormat: "number" } as StatementFact, 3.91)).toBe("3.91");
    expect(formatStatementValue(company, { key: "debt_to_equity", unit: "ratio" } as StatementFact, .5)).toBe("0.50");
  });
  it.each([[.5, "0.50%"], [-.5, "-0.50%"], [0, "0.00%"], [25, "25.00%"]])("preserves explicitly declared percentage points %s", (value, expected) => {
    expect(formatStatementValue(company, { unit: "%" } as StatementFact, value)).toBe(expected);
  });
});
