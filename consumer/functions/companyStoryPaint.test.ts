import { beforeAll, afterAll, describe, expect, it, vi } from "vitest";
import { loadHistoricalCompanyDataset } from "../test-fixtures/loadHistoricalCompanyDataset";
import { pullDataCompany } from "./dataProvider";
import type { Company } from "../src/types";
import { createCompanyOpeningCoordinator } from "../src/companyMarketContext";
import { createCompanyStoryPaintTiming, usableFinancialStory } from "../src/companyStoryPaint";

let company: Company;
beforeAll(async () => {
  const payload = loadHistoricalCompanyDataset();
  payload.prepared_financial_snapshot = { snapshot_id: "a".repeat(64), key: "b".repeat(64) };
  vi.stubGlobal("fetch", vi.fn(async () => Response.json(payload)));
  company = await pullDataCompany({ TARASHA_DATA_API_URL: "http://127.0.0.1:1" },
    "data-0001045810", 2020, 2026, undefined, { deferMarketContext: true }) as Company;
});
afterAll(() => vi.unstubAllGlobals());

function clock() {
  let time = 100;
  const frames: FrameRequestCallback[] = [];
  const cancelled: number[] = [];
  const timing = createCompanyStoryPaintTiming({ now: () => time,
    requestFrame: callback => { frames.push(callback); return frames.length; },
    cancelFrame: id => cancelled.push(id) });
  return { timing, frames, cancelled, at: (value: number) => { time = value; } };
}

describe("search selection to painted financial story timing", () => {
  it("measures from the result handler through a committed full financial story and two frames", () => {
    const test = clock();
    const measurement = test.timing.begin(company.id, 80);
    const painted = vi.fn();
    test.at(300);
    test.timing.afterCommit(measurement, company, painted);
    expect(painted).not.toHaveBeenCalled();
    test.frames[0](316);
    expect(painted).not.toHaveBeenCalled();
    test.at(332); test.frames[1](332);
    expect(painted).toHaveBeenCalledWith({ token: measurement.token, companyId: company.id,
      startedAt: 80, state: "painted", snapshotId: "a".repeat(64), requestKey: "b".repeat(64), elapsedMs: 252 });
    expect(usableFinancialStory(company)).toBe(true);
  });

  it.each([
    (value: Company) => ({ ...value, preparedFinancialSnapshot: undefined }),
    (value: Company) => ({ ...value, statements: [] }),
    (value: Company) => ({ ...value, companyStory: undefined }),
    (value: Company) => ({ ...value, metrics: { ...value.metrics, revenue: [] } }),
  ])("does not count a heading, skeleton or incomplete financial payload as ready", incomplete => {
    const test = clock(); const painted = vi.fn();
    test.timing.afterCommit(test.timing.begin(company.id), incomplete(company), painted);
    expect(test.frames).toHaveLength(0);
    expect(painted).not.toHaveBeenCalled();
  });

  it("rejects another company even if its financial body is otherwise complete", () => {
    const test = clock(); const painted = vi.fn();
    test.timing.afterCommit(test.timing.begin("data-other"), company, painted);
    expect(test.frames).toHaveLength(0);
  });

  it("does not let an obsolete second frame report success for a superseding selection", () => {
    const test = clock(); const oldPainted = vi.fn(); const latestPainted = vi.fn();
    const old = test.timing.begin(company.id);
    test.timing.afterCommit(old, company, oldPainted); test.frames[0](116);
    const current = test.timing.begin(company.id);
    test.timing.afterCommit(current, company, latestPainted);
    test.frames[1](132); // Even a scheduler that runs a canceled callback cannot falsely report ready.
    expect(oldPainted).not.toHaveBeenCalled();
    test.frames[2](148); test.at(164); test.frames[3](164);
    expect(latestPainted.mock.calls[0][0].token).toBe(current.token);
    expect(old.token).not.toBe(current.token);
  });

  it("cancels paint success on navigation/unmount before the second frame", () => {
    const test = clock(); const painted = vi.fn();
    test.timing.afterCommit(test.timing.begin(company.id), company, painted);
    test.frames[0](116); test.timing.cancel(); test.frames[1](132);
    expect(painted).not.toHaveBeenCalled();
    expect(test.cancelled).toContain(2);
  });

  it("keeps a canceled asynchronous company opening from arming a newer selection's timing", async () => {
    const test = clock(); const opening = createCompanyOpeningCoordinator(); const painted = vi.fn();
    let resolve!: (value: Company) => void;
    const old = test.timing.begin(company.id);
    const result = opening.load(() => new Promise<Company>(ready => { resolve = ready; }));
    test.timing.begin("data-new-selection"); opening.cancel(); resolve(company);
    expect(await result).toBeNull();
    test.timing.afterCommit(old, company, painted);
    expect(test.frames).toHaveLength(0);
    expect(painted).not.toHaveBeenCalled();
  });
});
