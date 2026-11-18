import { loadHistoricalCompanyDataset } from "../test-fixtures/loadHistoricalCompanyDataset";
import { afterEach, describe, expect, it, vi } from "vitest";
import { pullDataCompany, searchDataCompanies } from "./dataProvider";

const env = { TARASHA_DATA_API_URL: "http://127.0.0.1:1" };
const pull = (signal?: AbortSignal) => pullDataCompany(env, "data-0001045810", 2020, 2026, undefined, { signal });
afterEach(() => { vi.useRealTimers(); vi.unstubAllGlobals(); });

function pendingFetch() {
  return vi.fn((_input: string, init?: RequestInit) => new Promise<Response>((_resolve, reject) => {
    const abort = () => reject(init?.signal?.reason ?? new DOMException("Aborted", "AbortError"));
    if (init?.signal?.aborted) abort();
    else init?.signal?.addEventListener("abort", abort, { once: true });
  }));
}

describe("prepared financial request errors and deadlines", () => {
  it.each([
    { code: "prepared-scope-not-ready", reason: "Prepared financial data is not available for the requested fiscal years." },
    { code: "prepared-scope-stale", reason: "The prepared financial snapshot must be refreshed upstream." },
  ])("shows the $code structured reason without coercing an object", async (detail) => {
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({ detail }, { status: 503 })));
    await expect(pull()).rejects.toThrow(detail.reason);
  });
  it("preserves a legacy string detail", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({ detail: "Company is not published." }, { status: 404 })));
    await expect(pull()).rejects.toThrow("Company is not published.");
  });
  it("uses a supported error string when an object detail has no reason", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({ detail: { code: "not-ready" }, error: "Requested snapshot is not ready." }, { status: 503 })));
    await expect(pull()).rejects.toThrow("Requested snapshot is not ready.");
  });
  it("uses the status fallback when no supported message exists", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({ detail: { code: "not-ready" } }, { status: 503 })));
    await expect(pull()).rejects.toThrow("TaRaShaData.ai API returned 503.");
  });
  it("distinguishes a refused connection from a request deadline", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => { throw new TypeError("fetch failed", { cause: { code: "ECONNREFUSED" } }); }));
    await expect(pull()).rejects.toThrow("TaRaShaData.ai is not reachable.");
  });
  it.each(["UND_ERR_CONNECT_TIMEOUT", "UND_ERR_HEADERS_TIMEOUT", "UND_ERR_BODY_TIMEOUT"])("recognizes %s rather than claiming the service is unreachable", async (code) => {
    vi.stubGlobal("fetch", vi.fn(async () => { throw new TypeError("fetch failed", { cause: { code } }); }));
    await expect(pull()).rejects.toThrow("TaRaShaData.ai request timed out.");
  });
  it("aborts a financial fetch at 45000ms and removes its deadline", async () => {
    vi.useFakeTimers();
    const fetcher = pendingFetch();
    vi.stubGlobal("fetch", fetcher);
    const result = pull().then(() => "unexpected success", error => (error as Error).message);
    await vi.advanceTimersByTimeAsync(44999);
    expect(fetcher.mock.calls[0][1]?.signal?.aborted).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    expect(await result).toBe("TaRaShaData.ai request timed out. Please try again.");
    expect(fetcher.mock.calls[0][1]?.signal?.aborted).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  });
  it("honors caller cancellation and preserves its reason", async () => {
    vi.useFakeTimers();
    const fetcher = pendingFetch();
    vi.stubGlobal("fetch", fetcher);
    const controller = new AbortController();
    const reason = new DOMException("The company selection changed.", "AbortError");
    const result = pull(controller.signal).catch(error => error);
    controller.abort(reason);
    expect(await result).toBe(reason);
    expect(fetcher.mock.calls[0][1]?.signal?.aborted).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  });
  it("honors a caller signal that was already aborted", async () => {
    vi.useFakeTimers();
    vi.stubGlobal("fetch", pendingFetch());
    const controller = new AbortController();
    const reason = new DOMException("Cancelled before opening.", "AbortError");
    controller.abort(reason);
    await expect(pull(controller.signal)).rejects.toBe(reason);
    expect(vi.getTimerCount()).toBe(0);
  });
  it("keeps the deadline active while the JSON response body is stalled", async () => {
    vi.useFakeTimers();
    vi.stubGlobal("fetch", vi.fn(async (_input: string, init?: RequestInit) => new Response(new ReadableStream({
      start(controller) { init?.signal?.addEventListener("abort", () => controller.error(init.signal?.reason)); },
    }), { headers: { "content-type": "application/json" } })));
    const result = pull().catch(error => (error as Error).message);
    await vi.advanceTimersByTimeAsync(45000);
    expect(await result).toBe("TaRaShaData.ai request timed out. Please try again.");
    expect(vi.getTimerCount()).toBe(0);
  });
  it("accepts a prepared body that finishes after the old four-second limit without another source request", async () => {
    vi.useFakeTimers();
    const payload = loadHistoricalCompanyDataset();
    const fetcher = vi.fn(async (_input: string, init?: RequestInit) => new Response(new ReadableStream({
      start(controller) {
        const timer = setTimeout(() => {
          controller.enqueue(new TextEncoder().encode(JSON.stringify(payload)));
          controller.close();
        }, 6000);
        init?.signal?.addEventListener("abort", () => {
          clearTimeout(timer); controller.error(init.signal?.reason);
        }, { once: true });
      },
    }), { headers: { "content-type": "application/json" } }));
    vi.stubGlobal("fetch", fetcher);
    const result = pull();
    await vi.advanceTimersByTimeAsync(4000);
    expect(fetcher.mock.calls[0][1]?.signal?.aborted).toBe(false);
    await vi.advanceTimersByTimeAsync(2000);
    const company = await result;
    expect(company.metrics.revenue.length).toBeGreaterThan(0);
    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(fetcher.mock.calls[0][0]).toBe(env.TARASHA_DATA_API_URL + "/v1/discover/company-dataset");
    expect(vi.getTimerCount()).toBe(0);
  });
  it("does not impose the financial deadline on catalogue search", async () => {
    vi.useFakeTimers();
    const fetcher = vi.fn((_input: string, init?: RequestInit) => new Promise<Response>(resolve => {
      expect(init?.signal).toBeUndefined();
      setTimeout(() => resolve(Response.json([])), 6000);
    }));
    vi.stubGlobal("fetch", fetcher);
    const result = searchDataCompanies(env, "NVDA", "USA");
    await vi.advanceTimersByTimeAsync(6000);
    expect(await result).toEqual([]);
  });
  it("retains actual saved SEC financial data and the existing default request scope", async () => {
    vi.useFakeTimers();
    const payload = loadHistoricalCompanyDataset();
    const fetcher = vi.fn(async (_input: string, init?: RequestInit) => {
      expect(JSON.parse(String(init?.body))).toEqual({ identifier: "0001045810", from_year: 2020, to_year: 2026 });
      return Response.json(payload);
    });
    vi.stubGlobal("fetch", fetcher);
    const company = await pull();
    expect(company.id).toBe("data-0001045810");
    expect(company.metrics.revenue.length).toBeGreaterThan(0);
    expect(company.companyStory?.revenueOfferings?.offerings.length).toBeGreaterThan(0);
    expect(vi.getTimerCount()).toBe(0);
    await vi.advanceTimersByTimeAsync(45000);
    expect(fetcher.mock.calls[0][1]?.signal?.aborted).toBe(false);
  });
});
