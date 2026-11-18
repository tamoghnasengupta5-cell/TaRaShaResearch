import { readFileSync } from "node:fs";
import { afterEach, describe, expect, it, vi } from "vitest";
import { loadHistoricalCompanyDataset } from "../test-fixtures/loadHistoricalCompanyDataset";
import { pullDataCompany, readPreparedDefaultRequest } from "./dataProvider";

const native = JSON.parse(readFileSync(new URL("../test-fixtures/prepared-story-delivery-native-contract.json", import.meta.url), "utf8"));
afterEach(() => vi.unstubAllGlobals());

describe("frozen native backend R2 to Consumer delivery protocol", () => {
  it("accepts the actual snapshot-bound native timestamp/state/default metadata without changing SEC financial values", async () => {
    expect(native.backend_manifest_sha256).toBe("c399596612bcf043fea2e3022a4cd5a15777f4def952d4764db079fad28764ec");
    expect(native.body_byte_identity_unchanged).toBe(true);
    expect(readPreparedDefaultRequest(native.prepared_default_request, "ONE")).toEqual(native.prepared_default_request);
    const payload = loadHistoricalCompanyDataset();
    // Protocol test only: authentic native metadata plus an unchanged historical SEC financial test payload.
    payload.prepared_financial_snapshot = native.prepared_financial_snapshot;
    const headers = Object.fromEntries(Object.entries(native.headers).filter(([name]) =>
      ["x-tarashadata-snapshot", "x-tarashadata-prepared-at", "x-tarashadata-is-latest", "x-tarashadata-latest-status"].includes(name)));
    vi.stubGlobal("fetch", vi.fn(async () => Response.json(payload, { headers: headers as Record<string, string> })));
    const result = await pullDataCompany({ TARASHA_DATA_API_URL: "http://127.0.0.1:1" },
      "data-0001045810", 2020, 2026, undefined, { deferMarketContext: true });
    expect(result.preparedStoryDelivery).toEqual({
      preparedAtUtc: native.headers["x-tarashadata-prepared-at"],
      snapshotId: native.prepared_financial_snapshot.snapshot_id, isLatest: false, latestStatus: "preparing",
    });
    expect(result.metrics.revenue.length).toBeGreaterThan(0);
    expect(result.marketContextStatus).toEqual({ status: "pending" });
  });
});
