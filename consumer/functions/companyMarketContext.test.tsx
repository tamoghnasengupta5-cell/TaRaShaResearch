import { loadHistoricalCompanyDataset } from "../test-fixtures/loadHistoricalCompanyDataset";
import { renderToStaticMarkup } from "react-dom/server";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { pullDataCompany, pullDataCompanyMarketContext } from "./dataProvider";
import { createCompanyOpeningCoordinator, createMarketContextCoordinator, mergeCompanyMarketContext } from "../src/companyMarketContext";
import { CompanyStoryHome, MarketContextStatus } from "../src/CompanyStoryHome";
import type { Company, CompanyMarketContext } from "../src/types";

const env = { TARASHA_DATA_API_URL: "http://127.0.0.1:1" };
const payload = loadHistoricalCompanyDataset();
const prepared = { ...payload, prepared_financial_snapshot: { snapshot_id: "snapshot-a", key: "key-a" } };
let company: Company;
const marketResponse = () => ({ identifier: "0001045810", prepared_snapshot_id: "snapshot-a", prepared_snapshot_key: "key-a",
  stock_risk: { status: "unavailable", reason: "No aligned market history." },
  market_pricing: { status: "unavailable", reason: "No market prices." },
  stock_history: { status: "unavailable", reason: "No market history." } });
beforeEach(async () => {
  vi.stubGlobal("fetch", vi.fn(async () => Response.json(prepared)));
  company = await pullDataCompany(env, "data-0001045810", 2020, 2026, undefined, { deferMarketContext: true }) as Company;
});
afterEach(() => { vi.useRealTimers(); vi.unstubAllGlobals(); });

async function context(): Promise<CompanyMarketContext> {
  vi.stubGlobal("fetch", vi.fn(async () => Response.json(marketResponse())));
  return await pullDataCompanyMarketContext(env, company.id, company.preparedFinancialSnapshot!) as CompanyMarketContext;
}
function financialBytes(item: Company) {
  const copy = JSON.parse(JSON.stringify(item));
  delete copy.marketContextStatus;
  for (const key of ["stockRisk", "marketPricing", "stockHistory"]) delete copy.companyStory[key];
  return JSON.stringify(copy);
}
const flush = async () => { for (let index = 0; index < 5; index += 1) await Promise.resolve(); };
function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}

describe("market requests are independent from prepared financial content", () => {
  it("requests the deferred full financial scope without changing the industry basket", async () => {
    const fetcher = vi.fn(async (_url: string, init?: RequestInit) => {
      expect(JSON.parse(String(init?.body))).toEqual({ identifier: "0001045810", from_year: 2020, to_year: 2026, defer_market_context: true });
      return Response.json(prepared);
    });
    vi.stubGlobal("fetch", fetcher);
    const result = await pullDataCompany(env, company.id, 2020, 2026, undefined, { deferMarketContext: true });
    expect(result.preparedFinancialSnapshot).toEqual({ identifier: "0001045810", snapshotId: "snapshot-a", key: "key-a", fromYear: 2020, toYear: 2026 });
    expect(result.marketContextStatus?.status).toBe("pending");
  });
  it("rejects a deferred financial response without prepared snapshot proof", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => Response.json(payload)));
    await expect(pullDataCompany(env, company.id, 2020, 2026, undefined, { deferMarketContext: true }))
      .rejects.toThrow("did not return a prepared financial snapshot");
  });
  it("refuses a prepared financial response for a different company", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({ ...prepared, company: { ...prepared.company, identifier: "0000320193" } })));
    await expect(pullDataCompany(env, company.id, 2020, 2026, undefined, { deferMarketContext: true }))
      .rejects.toThrow("does not match the selected company");
  });
  it("uses only the dedicated market endpoint and binds its exact source snapshot", async () => {
    const fetcher = vi.fn(async (url: string, init?: RequestInit) => {
      expect(url).toBe("http://127.0.0.1:1/v1/discover/company-market-context");
      expect(JSON.parse(String(init?.body))).toEqual({ identifier: "0001045810", from_year: 2020, to_year: 2026,
        prepared_snapshot_id: "snapshot-a", prepared_snapshot_key: "key-a" });
      return Response.json(marketResponse());
    });
    vi.stubGlobal("fetch", fetcher);
    const loaded = await pullDataCompanyMarketContext(env, company.id, company.preparedFinancialSnapshot!);
    expect(loaded.companyId).toBe(company.id);
    expect(fetcher).toHaveBeenCalledTimes(1);
  });
  it.each(["identifier", "prepared_snapshot_id", "prepared_snapshot_key"])("refuses a market response with a different %s", async key => {
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({ ...marketResponse(), [key]: "different" })));
    await expect(pullDataCompanyMarketContext(env, company.id, company.preparedFinancialSnapshot!)).rejects.toThrow("does not match");
  });
  it("preserves every financial byte and original financial object while merging market fields", async () => {
    const before = financialBytes(company);
    const merged = mergeCompanyMarketContext(company, await context())!;
    expect(financialBytes(merged)).toBe(before);
    for (const key of ["metrics", "statements", "filings", "researchShelf", "preparedFinancialSnapshot", "source"] as const) expect(merged[key]).toBe(company[key]);
    for (const key of ["revenueSegments", "revenueOfferings", "costStructure", "cashConversion", "balanceSheet"] as const) expect(merged.companyStory?.[key]).toBe(company.companyStory?.[key]);
    expect(merged.marketContextStatus?.status).toBe("ready");
  });
  it.each(["companyId", "snapshotId", "snapshotKey"])("does not merge a context with a different %s", async key => {
    expect(mergeCompanyMarketContext(company, { ...await context(), [key]: "different" })).toBeNull();
  });
  it("keeps the actual financial story available while market context is pending", () => {
    const markup = renderToStaticMarkup(<CompanyStoryHome company={company} watched={false} toggleWatch={() => undefined} backToDiscover={() => undefined} />);
    expect(markup).toContain("How it makes money");
    expect(markup).toContain("Revenue");
    expect(markup).not.toContain("Loading market data");
    const market = renderToStaticMarkup(<MarketContextStatus company={company} chapter="market-pricing" />);
    expect(market).toContain("Loading market data");
    expect(market).toContain("Your financial story is ready");
  });
  it("shows a clear market error and retry without replacing the financial story", () => {
    const failed = { ...company, marketContextStatus: { status: "error" as const, error: "Market request timed out." } };
    const markup = renderToStaticMarkup(<MarketContextStatus company={failed} chapter="stock-behaves" retry={() => undefined} />);
    expect(markup).toContain('role="alert"');
    expect(markup).toContain("Market request timed out.");
    expect(markup).toContain("Retry market data");
    expect(financialBytes(failed)).toBe(financialBytes(company));
  });
  it("bounds the dedicated market request separately from the four-second financial deadline", async () => {
    vi.useFakeTimers();
    vi.stubGlobal("fetch", vi.fn((_url: string, init?: RequestInit) => new Promise<Response>((_resolve, reject) => {
      init?.signal?.addEventListener("abort", () => reject(init.signal?.reason));
    })));
    const result = pullDataCompanyMarketContext(env, company.id, company.preparedFinancialSnapshot!).catch(error => (error as Error).message);
    await vi.advanceTimersByTimeAsync(20000);
    expect(await result).toContain("request timed out");
    expect(vi.getTimerCount()).toBe(0);
  });
});

describe("company selection and snapshot races", () => {
  it("does not open a financial response that finishes after logout cancellation", async () => {
    const coordinator = createCompanyOpeningCoordinator();
    const waiting = deferred<Company>();
    let signal!: AbortSignal;
    const result = coordinator.load(value => { signal = value; return waiting.promise; });
    coordinator.cancel();
    expect(signal.aborted).toBe(true);
    waiting.resolve(company);
    expect(await result).toBeNull();
  });
  it("preserves the latest financial selection when an older response arrives late", async () => {
    const coordinator = createCompanyOpeningCoordinator();
    const old = deferred<Company>(), next = deferred<Company>();
    const oldResult = coordinator.load(() => old.promise);
    const nextResult = coordinator.load(() => next.promise);
    old.resolve(company);
    next.resolve(company);
    expect(await oldResult).toBeNull();
    expect(await nextResult).toBe(company);
  });
  it("surfaces an active financial readiness failure while ignoring cancelled failures", async () => {
    const coordinator = createCompanyOpeningCoordinator();
    const old = deferred<Company>();
    const oldResult = coordinator.load(() => old.promise);
    coordinator.cancel();
    old.reject(new Error("Late failure"));
    expect(await oldResult).toBeNull();
    await expect(coordinator.load(async () => { throw new Error("Prepared data is not ready."); })).rejects.toThrow("Prepared data is not ready.");
  });
  it("starts a background request and commits market context without modifying financial objects", async () => {
    const loaded = await context();
    const waiting = deferred<CompanyMarketContext>();
    const coordinator = createMarketContextCoordinator();
    const pending = vi.fn(), ready = vi.fn(), failed = vi.fn();
    coordinator.start(company, () => waiting.promise, pending, ready, failed);
    expect(pending).toHaveBeenCalledTimes(1);
    expect(ready).not.toHaveBeenCalled();
    waiting.resolve(loaded);
    await flush();
    expect(ready).toHaveBeenCalledWith(loaded);
    expect(failed).not.toHaveBeenCalled();
  });
  it("rejects a wrong snapshot from a completed background request", async () => {
    const coordinator = createMarketContextCoordinator();
    const ready = vi.fn(), failed = vi.fn();
    const loaded = { ...await context(), snapshotId: "old-snapshot" };
    coordinator.start(company, async () => loaded, () => undefined, ready, failed);
    await flush();
    expect(ready).not.toHaveBeenCalled();
    expect(failed).toHaveBeenCalledWith("Market context does not match the selected financial snapshot.");
  });
  it("aborts an old company request and ignores its late completion after selection changes", async () => {
    const loaded = await context();
    const old = deferred<CompanyMarketContext>(), next = deferred<CompanyMarketContext>();
    const coordinator = createMarketContextCoordinator();
    let oldSignal!: AbortSignal;
    const oldReady = vi.fn(), nextReady = vi.fn(), oldFailure = vi.fn();
    coordinator.start(company, signal => { oldSignal = signal; return old.promise; }, () => undefined, oldReady, oldFailure);
    await flush();
    const nextCompany = { ...company, id: "data-0000320193", preparedFinancialSnapshot: { ...company.preparedFinancialSnapshot!, identifier: "0000320193", snapshotId: "snapshot-b", key: "key-b" } };
    coordinator.start(nextCompany, () => next.promise, () => undefined, nextReady, () => undefined);
    expect(oldSignal.aborted).toBe(true);
    old.resolve(loaded);
    next.resolve({ ...loaded, companyId: nextCompany.id, snapshotId: "snapshot-b", snapshotKey: "key-b" });
    await flush();
    expect(oldReady).not.toHaveBeenCalled();
    expect(oldFailure).toHaveBeenCalledTimes(1);
    expect(nextReady).toHaveBeenCalledTimes(1);
  });
  it("cancels logout work and ignores its late failure", async () => {
    const old = deferred<CompanyMarketContext>();
    const coordinator = createMarketContextCoordinator();
    const ready = vi.fn(), failed = vi.fn();
    coordinator.start(company, () => old.promise, () => undefined, ready, failed);
    await flush();
    coordinator.cancel();
    old.reject(new Error("Late network failure"));
    await flush();
    expect(ready).not.toHaveBeenCalled();
    expect(failed).toHaveBeenCalledTimes(1);
    expect(failed).not.toHaveBeenCalledWith("Late network failure");
  });
});
