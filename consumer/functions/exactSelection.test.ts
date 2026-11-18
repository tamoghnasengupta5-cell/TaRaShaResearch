import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { pullDataCompany } from "./dataProvider";
import { loadHistoricalCompanyDataset } from "../test-fixtures/loadHistoricalCompanyDataset";

const env = { TARASHA_DATA_API_URL: "http://127.0.0.1:1" };
const key = "a".repeat(64);
const path = `/v1/discover/selections/${key}`;
const payload = loadHistoricalCompanyDataset();
const body = { ...payload, prepared_financial_snapshot: { key, snapshot_id: "snapshot-current" } };
const pending = (code = "selection_preparing", statusUrl = path) => Response.json({ detail: {
  code, reason: "Your selected view is being prepared.", status_url: statusUrl, retry_url: `${statusUrl}/retry`,
} }, { status: 503 });
const ready = () => Response.json({ state: "ready", request_key: key,
  reader_armed_ready: true, snapshot_id: "snapshot-current" });
const preparing = () => Response.json({ state: "preparing", request_key: key });
const pull = (signal?: AbortSignal) => pullDataCompany(env, "data-0001045810", 2020, 2026,
  ["0000320193"], { signal, deferMarketContext: true });
beforeEach(() => vi.useFakeTimers());
afterEach(() => { vi.useRealTimers(); vi.unstubAllGlobals(); });

function sequence(...responses: Array<() => Response | Promise<Response>>) {
  const fetcher = vi.fn(async (_url: string, _init?: RequestInit) => {
    const next = responses.shift();
    if (!next) throw new Error("Unexpected extra HTTP request");
    return next();
  });
  vi.stubGlobal("fetch", fetcher);
  return fetcher;
}

describe("automatic exact saved selection preparation", () => {
  it("keeps the ready default/selected body fast and all financial fields intact", async () => {
    const fetcher = sequence(() => Response.json(body));
    const company = await pull();
    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(company.metrics.revenue.length).toBeGreaterThan(0);
    expect(company.companyStory?.revenueOfferings?.offerings.length).toBeGreaterThan(0);
    expect(company.preparedFinancialSnapshot?.snapshotId).toBe("snapshot-current");
    expect(vi.getTimerCount()).toBe(0);
  });
  it("waits for the exact reader acknowledgment and repeats the complete original request", async () => {
    const fetcher = sequence(() => pending(), () => Response.json({ state: "preparing", request_key: key }),
      ready, () => Response.json(body));
    const result = pull();
    await vi.advanceTimersByTimeAsync(2000);
    const company = await result;
    expect(company.preparedFinancialSnapshot?.key).toBe(key);
    expect(fetcher.mock.calls.map(call => call[0])).toEqual([
      env.TARASHA_DATA_API_URL + "/v1/discover/company-dataset", env.TARASHA_DATA_API_URL + path,
      env.TARASHA_DATA_API_URL + path, env.TARASHA_DATA_API_URL + "/v1/discover/company-dataset",
    ]);
    expect(fetcher.mock.calls[3][1]?.body).toBe(fetcher.mock.calls[0][1]?.body);
    expect(JSON.parse(String(fetcher.mock.calls[3][1]?.body))).toEqual({ identifier: "0001045810",
      from_year: 2020, to_year: 2026, constituent_identifiers: ["0000320193"], defer_market_context: true });
    expect(vi.getTimerCount()).toBe(0);
  });
  it("loads a restored verified page while the latest generation remains preparing", async () => {
    const fetcher = sequence(() => pending(), preparing, preparing, preparing,
      () => Response.json({ ...body, prepared_financial_snapshot: { key, snapshot_id: "restored-snapshot" } }, {
        headers: { "X-TaRaShaData-Prepared-At": "2026-10-25T18:31:00Z", "X-TaRaShaData-Is-Latest": "false",
          "X-TaRaShaData-Latest-Status": "preparing", "X-TaRaShaData-Snapshot": "restored-snapshot" },
      }));
    const result = pull();
    await vi.advanceTimersByTimeAsync(3000);
    const company = await result;
    expect(company.preparedFinancialSnapshot?.snapshotId).toBe("restored-snapshot");
    expect(company.preparedStoryDelivery).toMatchObject({ isLatest: false, latestStatus: "preparing" });
    expect(fetcher.mock.calls[4][1]?.body).toBe(fetcher.mock.calls[0][1]?.body);
    expect(fetcher).toHaveBeenCalledTimes(5);
    expect(vi.getTimerCount()).toBe(0);
  });
  it.each([
    { key: "b".repeat(64), snapshot_id: "restored-snapshot" },
    { key, snapshot_id: "" },
    { key, snapshot_id: " " },
    { key },
  ])("rejects a restored body without the exact key and a nonempty snapshot: %j", async snapshot => {
    sequence(() => pending(), preparing, preparing, preparing,
      () => Response.json({ prepared_financial_snapshot: snapshot }));
    const result = pull().catch(error => (error as Error).message);
    await vi.advanceTimersByTimeAsync(3000);
    expect(await result).toContain("does not match this request");
    expect(vi.getTimerCount()).toBe(0);
  });
  it("surfaces restored-body errors other than selection preparation", async () => {
    sequence(() => pending(), preparing, preparing, preparing,
      () => Response.json({ detail: "Saved page failed verification." }, { status: 500 }));
    const result = pull().catch(error => (error as Error).message);
    await vi.advanceTimersByTimeAsync(3000);
    expect(await result).toBe("Saved page failed verification.");
    expect(vi.getTimerCount()).toBe(0);
  });
  it("bounds a restored-body read at forty-five seconds", async () => {
    const fetcher = vi.fn(async (url: string, init?: RequestInit) => {
      if (fetcher.mock.calls.length === 1) return pending();
      if (url.endsWith(path)) return preparing();
      return new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => reject(init.signal?.reason), { once: true });
      });
    });
    vi.stubGlobal("fetch", fetcher);
    const result = pull().catch(error => (error as Error).message);
    await vi.advanceTimersByTimeAsync(48_000);
    expect(await result).toContain("request timed out");
    expect(fetcher.mock.calls.at(-1)?.[1]?.signal?.aborted).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  });
  it("keeps a slow initial transfer and restored-body read inside the same ninety-second budget", async () => {
    const fetcher = vi.fn(async (url: string, init?: RequestInit) => {
      if (fetcher.mock.calls.length === 1) return new Promise<Response>(resolve => setTimeout(() => resolve(pending()), 44_000));
      if (url.endsWith(path)) return preparing();
      return new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => reject(init.signal?.reason), { once: true });
      });
    });
    vi.stubGlobal("fetch", fetcher);
    const result = pull().catch(error => (error as Error).message);
    await vi.advanceTimersByTimeAsync(90_000);
    expect(await result).toContain("taking longer than expected");
    expect(fetcher.mock.calls.at(-1)?.[1]?.signal?.aborted).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  });
  it("preserves cancellation while retrieving a restored page", async () => {
    const fetcher = vi.fn(async (url: string, init?: RequestInit) => {
      if (fetcher.mock.calls.length === 1) return pending();
      if (url.endsWith(path)) return preparing();
      return new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => reject(init.signal?.reason), { once: true });
      });
    });
    vi.stubGlobal("fetch", fetcher);
    const controller = new AbortController();
    const result = pull(controller.signal).catch(error => error);
    await vi.advanceTimersByTimeAsync(3000);
    const reason = new DOMException("Selection changed.", "AbortError");
    controller.abort(reason);
    expect(await result).toBe(reason);
    expect(fetcher.mock.calls.at(-1)?.[1]?.signal?.aborted).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  });
  it("rejects a status for another exact key", async () => {
    sequence(() => pending(), () => Response.json({ state: "ready", request_key: "b".repeat(64),
      reader_armed_ready: true, snapshot_id: "snapshot-current" }));
    const result = pull().catch(error => (error as Error).message);
    await vi.advanceTimersByTimeAsync(1000);
    expect(await result).toContain("does not match this request");
  });
  it("retries a user-requested failed selection once and reports a following failure", async () => {
    const fetcher = sequence(() => pending("selection_failed"),
      () => Response.json({ state: "preparing", request_key: key }),
      () => Response.json({ state: "failed", request_key: key, error_message: "Saved source failed verification." }));
    const result = pull().catch(error => (error as Error).message);
    await vi.advanceTimersByTimeAsync(1000);
    expect(await result).toBe("Saved source failed verification.");
    expect(fetcher.mock.calls[1][0]).toBe(env.TARASHA_DATA_API_URL + path + "/retry");
    expect(fetcher.mock.calls[1][1]?.method).toBe("POST");
    expect(fetcher).toHaveBeenCalledTimes(3);
  });
  it("cancels the wait immediately when a user changes company or range", async () => {
    const fetcher = sequence(() => pending());
    const controller = new AbortController();
    const result = pull(controller.signal).catch(error => error);
    await vi.advanceTimersByTimeAsync(0);
    const reason = new DOMException("Selection changed.", "AbortError");
    controller.abort(reason);
    expect(await result).toBe(reason);
    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
  });
  it("bounds each status attempt at four seconds even while preparation takes longer", async () => {
    const fetcher = vi.fn(async (_url: string, init?: RequestInit) => {
      if (fetcher.mock.calls.length === 1) return pending();
      return new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => reject(init.signal?.reason), { once: true });
      });
    });
    vi.stubGlobal("fetch", fetcher);
    const result = pull().catch(error => (error as Error).message);
    await vi.advanceTimersByTimeAsync(5000);
    expect(await result).toContain("request timed out");
    expect(fetcher.mock.calls[1][1]?.signal?.aborted).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  });
  it("ends a permanently preparing selection after ninety seconds without retrying it", async () => {
    const fetcher = vi.fn(async (url: string) => url.endsWith(path)
      ? Response.json({ state: "preparing", request_key: key }) : pending());
    vi.stubGlobal("fetch", fetcher);
    const result = pull().catch(error => (error as Error).message);
    await vi.advanceTimersByTimeAsync(89_000);
    expect(fetcher.mock.calls.filter(call => call[0].endsWith(path))).toHaveLength(89);
    expect(fetcher.mock.calls.every(call => !call[0].endsWith("/retry"))).toBe(true);
    await vi.advanceTimersByTimeAsync(1000);
    expect(await result).toBe("This company view is taking longer than expected to load. Preparation may still be running. Please try again shortly.");
    const requestsAtDeadline = fetcher.mock.calls.length;
    await vi.advanceTimersByTimeAsync(10_000);
    expect(fetcher).toHaveBeenCalledTimes(requestsAtDeadline);
    expect(vi.getTimerCount()).toBe(0);
  });
  it("cancels an active status request with the caller's original abort reason", async () => {
    const fetcher = vi.fn(async (_url: string, init?: RequestInit) => {
      if (fetcher.mock.calls.length === 1) return pending();
      return new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => reject(init.signal?.reason), { once: true });
      });
    });
    vi.stubGlobal("fetch", fetcher);
    const controller = new AbortController();
    const result = pull(controller.signal).catch(error => error);
    await vi.advanceTimersByTimeAsync(1000);
    const reason = new DOMException("Selection changed.", "AbortError");
    controller.abort(reason);
    expect(await result).toBe(reason);
    expect(fetcher.mock.calls[1][1]?.signal?.aborted).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  });
  it("aborts a body transfer when preparation consumes the overall opening deadline", async () => {
    let statusRequests = 0;
    const fetcher = vi.fn(async (url: string, init?: RequestInit) => {
      if (fetcher.mock.calls.length === 1) return pending();
      if (url.endsWith(path)) return ++statusRequests < 89
        ? Response.json({ state: "preparing", request_key: key }) : ready();
      if (statusRequests < 89) return pending();
      return new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => reject(init.signal?.reason), { once: true });
      });
    });
    vi.stubGlobal("fetch", fetcher);
    const result = pull().catch(error => (error as Error).message);
    await vi.advanceTimersByTimeAsync(90_000);
    expect(await result).toContain("taking longer than expected");
    expect(fetcher.mock.calls.at(-1)?.[0]).toBe(env.TARASHA_DATA_API_URL + "/v1/discover/company-dataset");
    expect(fetcher.mock.calls.at(-1)?.[1]?.signal?.aborted).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  });
  it("waits through a newer snapshot race and refuses the mismatched body", async () => {
    const fetcher = sequence(() => pending(), ready,
      () => Response.json({ ...body, prepared_financial_snapshot: { key, snapshot_id: "old-snapshot" } }),
      ready, () => Response.json(body));
    const result = pull();
    await vi.advanceTimersByTimeAsync(2000);
    expect((await result).preparedFinancialSnapshot?.snapshotId).toBe("snapshot-current");
    expect(fetcher).toHaveBeenCalledTimes(5);
  });
  it("does not reset the deadline when the ready body repeatedly changes snapshots", async () => {
    const fetcher = vi.fn(async (url: string) => fetcher.mock.calls.length === 1 ? pending()
      : url.endsWith(path) ? ready()
      : Response.json({ prepared_financial_snapshot: { key, snapshot_id: "old-snapshot" } }));
    vi.stubGlobal("fetch", fetcher);
    const result = pull().catch(error => (error as Error).message);
    await vi.advanceTimersByTimeAsync(90_000);
    expect(await result).toContain("taking longer than expected");
    expect(vi.getTimerCount()).toBe(0);
  });
  it.each([
    { state: "ready", reader_armed_ready: false },
    { state: "unknown", reader_armed_ready: true },
  ])("requires a truthful ready acknowledgment: $state/$reader_armed_ready", async state => {
    sequence(() => pending(), () => Response.json({ ...state, request_key: key }));
    const result = pull().catch(error => (error as Error).message);
    await vi.advanceTimersByTimeAsync(1000);
    expect(await result).toContain("invalid readiness state");
  });
  it("does not follow untrusted/external status URLs", async () => {
    const fetcher = sequence(() => pending("selection_preparing", "https://elsewhere.invalid/private"));
    await expect(pull()).rejects.toThrow("Your selected view is being prepared");
    expect(fetcher).toHaveBeenCalledTimes(1);
  });
});
