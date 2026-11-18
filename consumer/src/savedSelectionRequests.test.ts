import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createSavedSelectionRequest } from "./savedSelectionRequests";
import type { CatalogCompany, Company } from "./types";

afterEach(() => { vi.unstubAllGlobals(); vi.unstubAllEnvs(); });
function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
function callbacks<T>() {
  return { ready: vi.fn<(value: T) => void>(), failed: vi.fn<(error: unknown) => void>(), settled: vi.fn() };
}

describe("saved selection caller ownership", () => {
  it("applies only the current reporting range when an older response arrives later", async () => {
    const requests = createSavedSelectionRequest<string>();
    const old = deferred<string>(), current = deferred<string>();
    const oldCallbacks = callbacks<string>(), currentCallbacks = callbacks<string>();
    let oldSignal!: AbortSignal;
    const first = requests.run(signal => { oldSignal = signal; return old.promise; }, oldCallbacks);
    const next = requests.run(() => current.promise, currentCallbacks);
    expect(oldSignal.aborted).toBe(true);
    current.resolve("FY2023–2025");
    await next;
    old.resolve("FY2020–2022");
    await first;
    expect(currentCallbacks.ready).toHaveBeenCalledWith("FY2023–2025");
    expect(currentCallbacks.settled).toHaveBeenCalledTimes(1);
    expect(oldCallbacks.ready).not.toHaveBeenCalled();
    expect(oldCallbacks.settled).not.toHaveBeenCalled();
  });
  it("cancels range edits and ignores an old failure without clearing the new loading state", async () => {
    const requests = createSavedSelectionRequest<string>();
    const old = deferred<string>(), next = deferred<string>();
    const stale = callbacks<string>(), current = callbacks<string>();
    const first = requests.run(() => old.promise, stale);
    requests.cancel();
    const second = requests.run(() => next.promise, current);
    old.reject(new Error("old range failed"));
    await first;
    expect(stale.failed).not.toHaveBeenCalled();
    expect(stale.settled).not.toHaveBeenCalled();
    expect(current.settled).not.toHaveBeenCalled();
    next.resolve("current range");
    await second;
    expect(current.ready).toHaveBeenCalledTimes(1);
  });
  it("cancels company, peer and unmount ownership even if a loader ignores abort", async () => {
    const requests = createSavedSelectionRequest<string>();
    const waiting = deferred<string>(), handlers = callbacks<string>();
    let signal!: AbortSignal;
    const loading = requests.run(value => { signal = value; return waiting.promise; }, handlers);
    requests.cancel();
    expect(signal.aborted).toBe(true);
    waiting.resolve("old company's peers");
    await loading;
    expect(handlers.ready).not.toHaveBeenCalled();
    expect(handlers.failed).not.toHaveBeenCalled();
    expect(handlers.settled).not.toHaveBeenCalled();
  });
  it("rejects a changed company/year/peer scope before the React cleanup runs", async () => {
    const requests = createSavedSelectionRequest<string>();
    const waiting = deferred<string>(), handlers = callbacks<string>();
    let scope = "company-one|2023-2025|peer-A";
    const expected = scope;
    const loading = requests.run(() => waiting.promise, handlers, () => scope === expected);
    scope = "company-two|2024-2026|peer-B";
    waiting.resolve("previous scope");
    await loading;
    expect(handlers.ready).not.toHaveBeenCalled();
    expect(handlers.settled).not.toHaveBeenCalled();
  });
  it("reports an active error and ends only the active operation", async () => {
    const requests = createSavedSelectionRequest<string>();
    const handlers = callbacks<string>();
    const error = new Error("Saved financial verification failed.");
    await requests.run(async () => { throw error; }, handlers);
    expect(handlers.failed).toHaveBeenCalledWith(error);
    expect(handlers.settled).toHaveBeenCalledTimes(1);
    expect(handlers.ready).not.toHaveBeenCalled();
  });
});

describe("actual research request transport cancellation", () => {
  beforeEach(() => {
    vi.stubEnv("VITE_DATA_MODE", "live");
    vi.stubEnv("VITE_API_BASE_URL", "");
    vi.resetModules();
  });
  function abortableFetch() {
    const fetcher = vi.fn((_url: string, init?: RequestInit) => new Promise<Response>((_resolve, reject) => {
      const fail = () => reject(init?.signal?.reason);
      if (init?.signal?.aborted) fail();
      else init?.signal?.addEventListener("abort", fail, { once: true });
    }));
    vi.stubGlobal("fetch", fetcher);
    return fetcher;
  }
  it("sends the selected years and caller cancellation through range research", async () => {
    const { pullCompanyResearch } = await import("./liveData");
    const fetcher = abortableFetch();
    const controller = new AbortController();
    const catalog = { id: "data-0000789019", data_available: 1 } as CatalogCompany;
    const result = pullCompanyResearch(catalog, 2023, 2025, controller.signal).catch(error => error);
    const [url, init] = fetcher.mock.calls[0];
    expect(String(url)).toContain("fromYear=2023&toYear=2025&deferMarket=true");
    expect(init?.signal).toBe(controller.signal);
    const reason = new DOMException("Reporting years changed.", "AbortError");
    controller.abort(reason);
    expect(await result).toBe(reason);
  });
  it("preserves the complete peer request and cancellation signal", async () => {
    const { recalculateIndustryConstituents } = await import("./liveData");
    const fetcher = abortableFetch();
    const controller = new AbortController();
    const company = { id: "data-0000789019", dataMode: "tarasha-data",
      researchShelf: { fromYear: 2023, toYear: 2025 } } as Company;
    const peers = ["data-0000320193", "data-0001045810"];
    const result = recalculateIndustryConstituents(company, peers, controller.signal).catch(error => error);
    const [url, init] = fetcher.mock.calls[0];
    expect(url).toBe("/api/data/company");
    expect(init?.signal).toBe(controller.signal);
    expect(JSON.parse(String(init?.body))).toEqual({ companyId: company.id, fromYear: 2023,
      toYear: 2025, constituentIds: peers, deferMarket: true });
    controller.abort();
    expect((await result).name).toBe("AbortError");
  });
});
