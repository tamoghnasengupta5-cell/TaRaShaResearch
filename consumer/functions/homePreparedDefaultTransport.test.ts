import { afterEach, describe, expect, it, vi } from "vitest";
import { loadHistoricalCompanyDataset } from "../test-fixtures/loadHistoricalCompanyDataset";
import { pullDataCompany, readPreparedDefaultRequest, searchDataCompanies } from "./dataProvider";

const env = { TARASHA_DATA_API_URL: "http://127.0.0.1:1" };
const identifier = "0001045810";
const saved = { identifier, from_year: 2019, to_year: 2025,
  include_market_context: true as const, include_industry_context: true as const,
  defer_market_context: true as const, constituent_identifiers: null, provenance: "references" as const };
afterEach(() => vi.unstubAllGlobals());

describe("saved Home default transport", () => {
  it("carries the full saved canonical request while keeping current coverage metadata truthful", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => Response.json([{
      identifier, cik: identifier, name: "Nvidia", ticker: "NVDA", country: "USA",
      prepared_default_request: saved, normalized_coverage: { available: false,
        first_fiscal_year: null, latest_fiscal_year: 2026, statements: [] },
    }])));
    const rows = await searchDataCompanies(env, "NVDA", "USA");
    expect(rows).toHaveLength(1);
    expect(rows[0].preparedDefaultRequest).toEqual(saved);
    expect(rows[0].latestFiscalYear).toBe(2026);
    expect(rows[0].data_available).toBe(0);
  });

  it("posts the exact complete default without altering its old-year range or deferral selectors", async () => {
    const payload = loadHistoricalCompanyDataset();
    payload.prepared_financial_snapshot = { snapshot_id: "a".repeat(64), key: "b".repeat(64) };
    const fetcher = vi.fn(async (_url: string, init?: RequestInit) => {
      expect(JSON.parse(String(init?.body))).toEqual(saved);
      return Response.json(payload);
    });
    vi.stubGlobal("fetch", fetcher);
    const result = await pullDataCompany(env, `data-${identifier}`, 2019, 2025, undefined,
      { deferMarketContext: true, preparedDefaultRequest: saved });
    expect(result.preparedFinancialSnapshot).toMatchObject({ identifier, fromYear: 2019, toYear: 2025 });
    expect(result.marketContextStatus).toEqual({ status: "pending" });
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it.each([
    { ...saved, identifier: "0000320193" },
    { ...saved, constituent_identifiers: [""] },
    { ...saved, defer_market_context: false },
    { ...saved, from_year: 1995 },
    { identifier },
  ])("rejects unrelated/partial/custom metadata rather than silently using another view", request => {
    expect(() => readPreparedDefaultRequest(request, identifier)).toThrow("Invalid saved default financial request");
  });

  it("preserves the backend-designated default's complete supported selectors rather than guessing its role from shape", () => {
    const designated = { ...saved, include_market_context: false,
      constituent_identifiers: ["0000320193"], provenance: "full" };
    expect(readPreparedDefaultRequest(designated, identifier)).toEqual(designated);
  });

  it("does not apply a Home default to explicit year or peer choices", async () => {
    const fetcher = vi.fn(); vi.stubGlobal("fetch", fetcher);
    await expect(pullDataCompany(env, `data-${identifier}`, 2022, 2026, undefined,
      { deferMarketContext: true, preparedDefaultRequest: saved })).rejects.toThrow("does not match this company selection");
    await expect(pullDataCompany(env, `data-${identifier}`, 2019, 2025, ["data-0000320193"],
      { deferMarketContext: true, preparedDefaultRequest: saved })).rejects.toThrow("does not match this company selection");
    expect(fetcher).not.toHaveBeenCalled();
  });
});
