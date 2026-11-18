import type { Company, CompanyMarketContext } from "./types";

export function createCompanyOpeningCoordinator() {
  let active: AbortController | null = null;
  const cancel = () => { active?.abort(); active = null; };
  return {
    cancel,
    async load(request: (signal: AbortSignal) => Promise<Company>): Promise<Company | null> {
      cancel();
      const controller = new AbortController();
      active = controller;
      try {
        const company = await request(controller.signal);
        if (active !== controller) return null;
        active = null;
        return company;
      } catch (error) {
        if (active !== controller) return null;
        active = null;
        throw error;
      }
    },
  };
}

export function matchingMarketSnapshot(company: Company, context: CompanyMarketContext): boolean {
  const snapshot = company.preparedFinancialSnapshot;
  return Boolean(snapshot && company.id === context.companyId && snapshot.snapshotId === context.snapshotId
    && snapshot.key === context.snapshotKey);
}

/** Keep every financial value and source object from the prepared response. */
export function mergeCompanyMarketContext(company: Company, context: CompanyMarketContext): Company | null {
  if (!matchingMarketSnapshot(company, context)) return null;
  return { ...company, companyStory: { ...company.companyStory,
    stockRisk: context.stockRisk, marketPricing: context.marketPricing, stockHistory: context.stockHistory },
  marketContextStatus: { status: "ready" } };
}

export function createMarketContextCoordinator() {
  let generation = 0;
  let active: AbortController | null = null;
  let interrupted: (() => void) | null = null;
  const cancel = () => { generation += 1; active?.abort(); active = null; interrupted?.(); interrupted = null; };
  return {
    cancel,
    start(company: Company, load: (signal: AbortSignal) => Promise<CompanyMarketContext>,
      pending: () => void, ready: (context: CompanyMarketContext) => void, failed: (message: string) => void): void {
      cancel();
      if (!company.preparedFinancialSnapshot) return;
      const token = generation;
      const controller = new AbortController();
      active = controller;
      interrupted = () => failed("Market loading was cancelled. You can retry it.");
      pending();
      void Promise.resolve().then(() => load(controller.signal)).then(context => {
        if (token !== generation) return;
        if (!matchingMarketSnapshot(company, context)) throw new Error("Market context does not match the selected financial snapshot.");
        active = null;
        interrupted = null;
        ready(context);
      }).catch((error: unknown) => {
        if (token !== generation) return;
        active = null;
        interrupted = null;
        failed(error instanceof Error ? error.message : "Market data could not be loaded.");
      });
    },
  };
}
