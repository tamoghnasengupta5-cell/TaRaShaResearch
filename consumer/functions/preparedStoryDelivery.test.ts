import { loadHistoricalCompanyDataset } from "../test-fixtures/loadHistoricalCompanyDataset";
import { afterEach, describe, expect, it, vi } from "vitest";
import { pullDataCompany } from "./dataProvider";

const env = { TARASHA_DATA_API_URL: "http://127.0.0.1:1" };
const snapshotId = "a".repeat(64);
const preparedAt = "2026-10-13T16:30:12.123456+00:00";
const deliveryHeaders = {
  "X-TaRaShaData-Snapshot": snapshotId,
  "X-TaRaShaData-Prepared-At": preparedAt,
  "X-TaRaShaData-Is-Latest": "false",
  "X-TaRaShaData-Latest-Status": "preparing",
};
const pull = () => pullDataCompany(env, "data-0001045810", 2020, 2026,
  undefined, { deferMarketContext: true });
afterEach(() => vi.unstubAllGlobals());

describe("saved prepared story delivery", () => {
  it("maps the delivered snapshot's preparation date while retaining SEC financials and deferred market loading", async () => {
    const payload = loadHistoricalCompanyDataset();
    payload.prepared_financial_snapshot = { snapshot_id: snapshotId, key: "b".repeat(64) };
    const fetcher = vi.fn(async (_url: string, init?: RequestInit) => {
      expect(JSON.parse(String(init?.body))).toEqual({
        identifier: "0001045810", from_year: 2020, to_year: 2026, defer_market_context: true,
      });
      return Response.json(payload, { headers: deliveryHeaders });
    });
    vi.stubGlobal("fetch", fetcher);
    const result = await pull();
    expect(result.preparedStoryDelivery).toEqual({ preparedAtUtc: preparedAt, snapshotId,
      isLatest: false, latestStatus: "preparing" });
    expect(result.preparedFinancialSnapshot?.snapshotId).toBe(snapshotId);
    expect(result.metrics.revenue.length).toBeGreaterThan(0);
    expect(result.companyStory.revenueOfferings.offerings.length).toBeGreaterThan(0);
    expect(result.marketContextStatus).toEqual({ status: "pending" });
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it.each([
    { name: "another snapshot", change: { "X-TaRaShaData-Snapshot": "c".repeat(64) } },
    { name: "missing snapshot identity", remove: "X-TaRaShaData-Snapshot" },
    { name: "missing preparation date", remove: "X-TaRaShaData-Prepared-At" },
    { name: "invalid date", change: { "X-TaRaShaData-Prepared-At": "not-a-date" } },
    { name: "timestamp without timezone", change: { "X-TaRaShaData-Prepared-At": "2026-10-13T16:30:12" } },
    { name: "invalid latest flag", change: { "X-TaRaShaData-Is-Latest": "yes" } },
    { name: "unknown status value", change: { "X-TaRaShaData-Latest-Status": "loading" } },
    { name: "contradictory latest claim", change: { "X-TaRaShaData-Is-Latest": "true" } },
  ])("rejects $name rather than displaying a misleading preparation date", async ({ change, remove }) => {
    const headers = new Headers({ ...deliveryHeaders, ...change });
    if (remove) headers.delete(remove);
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({
      prepared_financial_snapshot: { snapshot_id: snapshotId, key: "b".repeat(64) },
    }, { headers })));
    await expect(pull()).rejects.toThrow("invalid prepared story delivery details");
  });
});
