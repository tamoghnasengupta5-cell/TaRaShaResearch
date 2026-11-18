import type { Company } from "./types";

export interface CompanyStoryPaint {
  token: number;
  companyId: string;
  startedAt: number;
  state: "selected" | "painted";
  snapshotId?: string;
  requestKey?: string;
  elapsedMs?: number;
}

export function usableFinancialStory(company: Company): boolean {
  const snapshot = company.preparedFinancialSnapshot;
  return Boolean(company.dataMode === "tarasha-data" && snapshot
    && company.id === `data-${snapshot.identifier}` && snapshot.snapshotId && snapshot.key
    && company.metrics.revenue.some(point => Number.isFinite(point.value))
    && ["income", "balance", "cash"].every(key => company.statements?.some(
      group => group.key === key && group.facts.length > 0,
    )) && company.companyStory?.revenueOfferings && company.companyStory.costStructure
    && company.companyStory.cashConversion && company.companyStory.balanceSheet);
}

/** Two frames after the committed financial story provide a conservative painted-time bound. */
export function createCompanyStoryPaintTiming(clock = {
  now: () => performance.now(),
  requestFrame: (callback: FrameRequestCallback) => requestAnimationFrame(callback),
  cancelFrame: (id: number) => cancelAnimationFrame(id),
}) {
  let token = 0;
  let active: CompanyStoryPaint | null = null;
  let frame: number | null = null;
  const stopFrame = () => { if (frame !== null) clock.cancelFrame(frame); frame = null; };
  const cancel = () => { token += 1; active = null; stopFrame(); };
  const owns = (measurement: CompanyStoryPaint) => active?.token === measurement.token
    && token === measurement.token && active.companyId === measurement.companyId;
  return {
    cancel, owns,
    begin(companyId: string, startedAt = clock.now()): CompanyStoryPaint {
      cancel();
      active = { token, companyId, startedAt, state: "selected" };
      return active;
    },
    afterCommit(measurement: CompanyStoryPaint, company: Company, painted: (value: CompanyStoryPaint) => void) {
      if (!owns(measurement) || active?.state !== "selected" || company.id !== measurement.companyId
        || !usableFinancialStory(company)) return () => {};
      const snapshot = company.preparedFinancialSnapshot!;
      stopFrame();
      frame = clock.requestFrame(() => {
        if (!owns(measurement)) return;
        frame = clock.requestFrame(() => {
          if (!owns(measurement)) return;
          frame = null;
          active = { ...measurement, state: "painted", snapshotId: snapshot.snapshotId, requestKey: snapshot.key,
            elapsedMs: Math.max(0, clock.now() - measurement.startedAt) };
          painted(active);
        });
      });
      return stopFrame;
    },
  };
}
