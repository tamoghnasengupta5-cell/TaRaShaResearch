import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { loadHistoricalCompanyDataset } from "../test-fixtures/loadHistoricalCompanyDataset";
import { pullDataCompany } from "./dataProvider";
import type { CatalogCompany } from "../src/types";

const saved = { identifier: "0001045810", from_year: 2019, to_year: 2025,
  include_market_context: true as const, include_industry_context: true as const,
  defer_market_context: true as const, constituent_identifiers: null, provenance: "references" as const };
const catalog: CatalogCompany = { id: "data-0001045810", cik: "0001045810", name: "Nvidia", ticker: "NVDA",
  exchange: "NASDAQ", country: "USA", provider: "TaRaShaData.ai API", data_available: 1,
  firstFiscalYear: 2000, latestFiscalYear: 2026, preparedDefaultRequest: saved } as CatalogCompany;
let preparedCompany: Awaited<ReturnType<typeof pullDataCompany>>;
beforeAll(async () => {
  const payload = loadHistoricalCompanyDataset();
  payload.prepared_financial_snapshot = { snapshot_id: "a".repeat(64), key: "b".repeat(64) };
  vi.stubGlobal("fetch", vi.fn(async () => Response.json(payload)));
  preparedCompany = await pullDataCompany({ TARASHA_DATA_API_URL: "http://127.0.0.1:1" },
    catalog.id, 2020, 2026, undefined, { deferMarketContext: true });
  vi.unstubAllGlobals();
});
beforeEach(() => { vi.resetModules(); vi.stubEnv("VITE_DATA_MODE", "live"); });
afterEach(() => { vi.unstubAllGlobals(); vi.unstubAllEnvs(); });

describe("Home last successful default request", () => {
  it("opens the exact old-year prepared default and forwards ownership without substituting the current coverage range", async () => {
    const controller = new AbortController();
    const fetcher = vi.fn(async (input: string, init?: RequestInit) => {
      const url = new URL(input, "http://localhost");
      expect(url.searchParams.get("fromYear")).toBe("2019");
      expect(url.searchParams.get("toYear")).toBe("2025");
      expect(JSON.parse(url.searchParams.get("preparedDefaultRequest")!)).toEqual(saved);
      expect(init?.signal).toBe(controller.signal);
      return Response.json(preparedCompany);
    });
    vi.stubGlobal("fetch", fetcher);
    const { pullHomeCompanyResearch } = await import("../src/liveData");
    await pullHomeCompanyResearch(catalog, controller.signal);
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it("leaves an explicit year selection exact and does not send saved-default metadata", async () => {
    vi.stubGlobal("fetch", vi.fn(async (input: string) => {
      const url = new URL(input, "http://localhost");
      expect(url.searchParams.get("fromYear")).toBe("2022");
      expect(url.searchParams.get("toYear")).toBe("2026");
      expect(url.searchParams.has("preparedDefaultRequest")).toBe(false);
      return Response.json(preparedCompany);
    }));
    const { pullCompanyResearch } = await import("../src/liveData");
    await pullCompanyResearch(catalog, 2022, 2026);
  });

  it("uses the existing current coverage range when no saved default exists", async () => {
    vi.stubGlobal("fetch", vi.fn(async (input: string) => {
      const url = new URL(input, "http://localhost");
      expect(url.searchParams.get("fromYear")).toBe("2000");
      expect(url.searchParams.get("toYear")).toBe("2026");
      expect(url.searchParams.has("preparedDefaultRequest")).toBe(false);
      return Response.json(preparedCompany);
    }));
    const { pullHomeCompanyResearch } = await import("../src/liveData");
    await pullHomeCompanyResearch({ ...catalog, preparedDefaultRequest: undefined });
  });
});
