import { loadHistoricalCompanyDataset } from "../test-fixtures/loadHistoricalCompanyDataset";
import { EventEmitter } from "node:events";
import { Readable } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";
import { onRequest } from "./api/[[path]]";
import { localDataApiPlugin } from "../localDataDev";

const env = { TARASHA_DATA_API_URL: "http://127.0.0.1:1" };
const snapshot = { identifier: "0001045810", snapshotId: "snapshot-a", key: "key-a", fromYear: 2020, toYear: 2026 };
const body = { companyId: "data-0001045810", snapshot };
const response = { identifier: snapshot.identifier, prepared_snapshot_id: snapshot.snapshotId, prepared_snapshot_key: snapshot.key,
  stock_risk: { status: "unavailable" }, market_pricing: { status: "unavailable" }, stock_history: { status: "unavailable" } };
afterEach(() => { vi.unstubAllGlobals(); });

describe("both Consumer runtimes serve the separate market API", () => {
  it("routes the Worker market request exclusively to the bound TaRaShaData market endpoint", async () => {
    const fetcher = vi.fn(async (url: string) => {
      expect(url).toBe("http://127.0.0.1:1/v1/discover/company-market-context");
      return Response.json(response);
    });
    vi.stubGlobal("fetch", fetcher);
    const request = new Request("http://localhost/api/data/company-market", { method: "POST", body: JSON.stringify(body) });
    const result = await onRequest({ request, env } as never) as Response;
    expect(result.status).toBe(200);
    expect((await result.json() as { snapshotId: string }).snapshotId).toBe(snapshot.snapshotId);
    expect(result.headers.get("cache-control")).toContain("no-store");
    expect(fetcher).toHaveBeenCalledTimes(1);
  });
  it("shows a structured market miss reason without calling a financial endpoint", async () => {
    vi.stubGlobal("fetch", vi.fn(async (url: string) => {
      expect(url).toBe("http://127.0.0.1:1/v1/discover/company-market-context");
      return Response.json({ detail: { reason: "The prepared market scope is unavailable." } }, { status: 503 });
    }));
    const result = await onRequest({ request: new Request("http://localhost/api/data/company-market", { method: "POST", body: JSON.stringify(body) }), env } as never) as Response;
    expect(result.status).toBe(502);
    expect(await result.json()).toEqual({ error: "The prepared market scope is unavailable." });
  });
  it("rejects a missing snapshot before any upstream request", async () => {
    const fetcher = vi.fn();
    vi.stubGlobal("fetch", fetcher);
    const result = await onRequest({ request: new Request("http://localhost/api/data/company-market", { method: "POST", body: "{}" }), env } as never) as Response;
    expect(result.status).toBe(400);
    expect(fetcher).not.toHaveBeenCalled();
  });
  it("forwards the browser deferred-financial flag with the original industry scope", async () => {
    const saved = loadHistoricalCompanyDataset();
    vi.stubGlobal("fetch", vi.fn(async (url: string, init?: RequestInit) => {
      expect(url).toBe("http://127.0.0.1:1/v1/discover/company-dataset");
      expect(JSON.parse(String(init?.body))).toEqual({ identifier: "0001045810", from_year: 2020, to_year: 2026, defer_market_context: true });
      return Response.json({ ...saved, prepared_financial_snapshot: { snapshot_id: "snapshot-a", key: "key-a" } });
    }));
    const request = new Request("http://localhost/api/data/company?companyId=data-0001045810&fromYear=2020&toYear=2026&deferMarket=true");
    const result = await onRequest({ request, env } as never) as Response;
    expect(result.status).toBe(200);
    expect((await result.json() as { preparedFinancialSnapshot: { key: string } }).preparedFinancialSnapshot.key).toBe("key-a");
  });
  it("uses the same dedicated request and cleans up cancellation in local Vite middleware", async () => {
    const fetcher = vi.fn(async (url: string) => {
      expect(url).toBe("http://127.0.0.1:1/v1/discover/company-market-context");
      return Response.json(response);
    });
    vi.stubGlobal("fetch", fetcher);
    let handler!: (request: unknown, response: unknown, next: () => void) => Promise<void>;
    const plugin = localDataApiPlugin(env.TARASHA_DATA_API_URL)!;
    (plugin.configureServer as (server: unknown) => void)({ middlewares: { use(fn: typeof handler) { handler = fn; } } });
    const request = Object.assign(Readable.from([Buffer.from(JSON.stringify(body))]), { method: "POST", url: "/api/data/company-market" });
    const localResponse = Object.assign(new EventEmitter(), { statusCode: 0, writableEnded: false,
      setHeader: vi.fn(), end: vi.fn((json: string) => { localResponse.writableEnded = true; expect(JSON.parse(json).snapshotKey).toBe(snapshot.key); }) });
    await handler(request, localResponse, () => { throw new Error("Market request fell through to another handler."); });
    expect(localResponse.statusCode).toBe(200);
    expect(localResponse.listenerCount("close")).toBe(0);
    expect(fetcher).toHaveBeenCalledTimes(1);
  });
});
