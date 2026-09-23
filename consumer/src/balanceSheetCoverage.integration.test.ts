import { describe, expect, it } from "vitest";
import { pullDataCompany } from "../functions/dataProvider";

const apiUrl = (
  globalThis as typeof globalThis & {
    process?: { env?: Record<string, string | undefined> };
  }
).process?.env?.TARASHA_BALANCE_AUDIT_API_URL;
const normalizedTickers = [
  "NVDA", "AAPL", "MSFT", "TSM", "AVGO", "MU", "AMD", "ASML", "INTC", "CSCO",
  "PLTR", "ORCL", "LRCX", "AMAT", "PANW", "DELL", "ARM", "SAP", "TXN", "KLAC",
  "ANET", "SNDK", "IBM", "MRVL", "CRWD", "APH", "STX", "SHOP", "ADI", "QCOM",
];

describe.skipIf(!apiUrl)("live balance-sheet story coverage", () => {
  it("builds the accounting-equation story and a sourced cost of debt", async () => {
    const env = { TARASHA_DATA_API_URL: apiUrl! };
    const rows: Array<Record<string, string | number | null>> = [];

    for (const ticker of normalizedTickers) {
      const company = await pullDataCompany(env, ticker, 2020, 2026, [ticker]);
      const story = company.companyStory?.balanceSheet;
      expect(story?.status, ticker).toBe("available");
      expect(story?.equation.assets, ticker).not.toBeNull();
      expect(story?.equation.liabilities, ticker).not.toBeNull();
      expect(story?.equation.shareholdersEquity, ticker).not.toBeNull();
      if (ticker === "ANET") {
        expect(story?.health.weightedAverageCostOfDebt, ticker).toBeNull();
        expect(story?.health.weightedAverageCostOfDebtNote, ticker).toBe(
          "Not applicable · no reported debt",
        );
      } else {
        expect(story?.health.weightedAverageCostOfDebt, ticker).not.toBeNull();
      }

      rows.push({
        ticker,
        fiscalYear: story?.latestFiscalYear ?? null,
        totalCash: story?.health.totalCash ?? null,
        totalDebt: story?.health.totalDebt ?? null,
        weightedAverageCostOfDebt: story?.health.weightedAverageCostOfDebt ?? null,
        costMethod: story?.health.weightedAverageCostOfDebtNote ?? null,
      });
    }

    console.log("BALANCE_SHEET_COVERAGE", JSON.stringify(rows));
  }, 180_000);

  it("keeps SK hynix unavailable because the SEC source has no normalized financial facts", async () => {
    const response = await fetch(`${apiUrl}/v1/discover/company-dataset`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        identifier: "SKHY",
        from_year: 2020,
        to_year: 2026,
        constituent_identifiers: ["SKHY"],
      }),
    });
    const payload = await response.json() as {
      company: { name: string };
      normalized_coverage: { available: boolean; annual_observation_count: number };
    };

    expect(response.ok).toBe(true);
    expect(payload.company.name).toBe("SK hynix Inc.");
    expect(payload.normalized_coverage.available).toBe(false);
    expect(payload.normalized_coverage.annual_observation_count).toBe(0);
  });
});
