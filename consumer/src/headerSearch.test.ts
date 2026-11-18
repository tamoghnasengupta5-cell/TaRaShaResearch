import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { CatalogCompany } from "./types";

const issuer = (ticker: string, country: "USA" | "India"): CatalogCompany => ({
  id: `data-${country}-${ticker}`,
  cik: country === "USA" ? "0001596532" : null,
  name: ticker === "POLYCAB" ? "Polycab India Limited" : ticker === "FINCABLES" ? "Finolex Cables Limited" : "Arista Networks, Inc.",
  ticker,
  exchange: country === "India" ? "NSE" : "NYSE",
  country,
  provider: "TaRaShaData.ai API",
  data_available: 1,
});

beforeEach(() => {
  vi.resetModules();
  vi.stubEnv("VITE_DATA_MODE", "live");
  vi.stubEnv("VITE_API_BASE_URL", "");
});
afterEach(() => { vi.unstubAllGlobals(); vi.unstubAllEnvs(); });

describe("header search across supported markets", () => {
  it.each(["POLYCAB", "FINCABLES"])("finds %s when the USA catalogue is empty", async (ticker) => {
    const company = issuer(ticker, "India");
    const fetchMock = vi.fn(async (input: string) => {
      const url = new URL(input, "http://localhost");
      expect(url.pathname).toBe("/api/companies");
      expect(url.searchParams.get("query")).toBe(ticker);
      return Response.json({ companies: url.searchParams.get("country") === "India" ? [company] : [] });
    });
    vi.stubGlobal("fetch", fetchMock);
    const { searchGlobalCompanyCatalog } = await import("./liveData");
    expect(await searchGlobalCompanyCatalog(ticker)).toEqual([company]);
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("preserves US results and prioritizes an exact ticker across markets", async () => {
    const us = issuer("ANET", "USA");
    const india = { ...issuer("FINCABLES", "India"), name: "A matching company" };
    vi.stubGlobal("fetch", vi.fn(async (input: string) => Response.json({
      companies: new URL(input, "http://localhost").searchParams.get("country") === "USA" ? [us] : [india],
    })));
    const { searchGlobalCompanyCatalog } = await import("./liveData");
    expect(await searchGlobalCompanyCatalog("ANET")).toEqual([us, india]);
  });

  it("does not call the API before the minimum search length", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    const { searchGlobalCompanyCatalog } = await import("./liveData");
    expect(await searchGlobalCompanyCatalog(" P ")).toEqual([]);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("surfaces an API failure instead of reporting no matches", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({ error: "Data service unavailable" }, { status: 502 })));
    const { searchGlobalCompanyCatalog } = await import("./liveData");
    await expect(searchGlobalCompanyCatalog("POLYCAB")).rejects.toThrow("Data service unavailable");
  });
});
