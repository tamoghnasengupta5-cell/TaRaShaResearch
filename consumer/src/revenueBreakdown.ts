import type { Company, RevenueBreakdownSourceType, RevenueOffering } from "./types";
import { isIndiaListedCompany } from "./revenueOfferingMath";

export interface RevenueBreakdownCategory extends RevenueOffering {
  role?: "segment" | "elimination" | "reconciliation";
  sourceTypeByPeriod?: Record<string, RevenueBreakdownSourceType | undefined>;
  reportingBasisByPeriod?: Record<string, "standalone" | "consolidated" | undefined>;
  ttmYoyGrowthPercent?: number | null;
  ttmThreePeriodCagrPercent?: number | null;
  yoyGrowthPercentByPeriod?: Record<string, number | null>;
  threePeriodCagrPercentByPeriod?: Record<string, number | null>;
}

/** Select a source-backed presentation; never combine external and gross sales. */
export function revenueBreakdownCategories(company: Company): RevenueBreakdownCategory[] {
  const story = company.companyStory?.revenueOfferings;
  const reconciliation = story?.segmentReconciliation;
  if (!reconciliation) {
    const leaves = (items: RevenueOffering[]): RevenueOffering[] =>
      items.flatMap(item => item.children?.length ? leaves(item.children) : [item]);
    return leaves(story?.offerings ?? []);
  }
  const categories = new Map<string, RevenueBreakdownCategory>();
  // The top point can be annual when a comparable TTM partition is unavailable.
  // Never relabel an annual breakdown as TTM even when total TTM revenue exists.
  const latestPeriod = reconciliation.periodType === "annual"
    ? reconciliation.fiscalYear === undefined ? null : String(reconciliation.fiscalYear)
    : "TTM";
  // Start with the latest presentation to keep its labels and ordering stable.
  for (const [period, point] of [
    ...(latestPeriod ? [[latestPeriod, reconciliation] as const] : []),
    ...Object.entries(reconciliation.history ?? {}).filter(([period]) => period !== latestPeriod),
  ]) {
    const { items } = point;
    for (const item of items) {
      let category = categories.get(item.id);
      if (!category) {
        category = { id: item.id, label: item.label, role: item.role,
          revenueByPeriod: {}, sourceByPeriod: {}, sourcesByPeriod: {},
          sourceTypeByPeriod: {}, reportingBasisByPeriod: {},
          yoyGrowthPercentByPeriod: {}, threePeriodCagrPercentByPeriod: {} };
        categories.set(item.id, category);
      }
      if (period === "TTM") {
        category.ttmThreeYearCagrPercent = item.threeYearCagrPercent ?? null;
        category.ttmYoyGrowthReason = item.yoyGrowthReason;
        category.ttmThreeYearCagrReason = item.threeYearCagrReason;
        category.ttmGrowthComparisons = item.growthComparisons;
        category.ttmGrowthSourceUrls = item.growthSourceUrls;
        category.ttmYoyGrowthPercent = item.yoyGrowthPercent ?? null;
        category.ttmThreePeriodCagrPercent = item.threePeriodCagrPercent ?? null;
      }
      category.revenueByPeriod[period] = item.revenue;
      category.sourceTypeByPeriod![period] = item.sourceType ?? point.sourceType;
      category.reportingBasisByPeriod![period] = point.reportingBasis;
      category.yoyGrowthPercentByPeriod![period] = item.yoyGrowthPercent ?? null;
      category.threePeriodCagrPercentByPeriod![period] = item.threePeriodCagrPercent ?? null;
      category.sourcesByPeriod![period] = item.sourceUrls;
      if (item.sourceUrls[0]) category.sourceByPeriod![period] = item.sourceUrls[0];
    }
  }
  return [...categories.values()];
}

/** Keep reported parent and child disclosures for detail, without composing them. */
export function reportedRevenueBreakdownCategories(company: Company): Array<RevenueOffering & { depth: number }> {
  const flatten = (items: RevenueOffering[], depth = 0): Array<RevenueOffering & { depth: number }> =>
    items.flatMap(item => [{ ...item, depth }, ...flatten(item.children ?? [], depth + 1)]);
  return flatten(company.companyStory?.revenueOfferings?.reportedOfferings ?? []);
}

/** Category comparisons require the same verified disclosure and reporting basis.
 * TTM category growth is supplied by Data after checking its actual TTM inputs;
 * annual history cannot stand in for those prior trailing periods.
 */
export function categoryPeriodsAreComparable(category: RevenueBreakdownCategory, periods: string[]) {
  if (periods.includes("TTM")) return true;
  const types = periods.map(period => category.sourceTypeByPeriod?.[period]);
  const bases = periods.map(period => category.reportingBasisByPeriod?.[period]);
  if (types.every(type => type === undefined)) return bases.every(basis => basis === undefined)
    || bases.every(Boolean) && new Set(bases).size === 1;
  return types.every(Boolean) && new Set(types).size === 1
    && bases.every(Boolean) && new Set(bases).size === 1;
}

export function revenueBreakdownPoint(company: Company, period: string) {
  const data = company.companyStory?.revenueOfferings?.segmentReconciliation;
  if (!data) return undefined;
  if (period === "TTM" && data.periodType !== "annual"
    || data.periodType === "annual" && String(data.fiscalYear) === period) return data;
  return period !== "TTM" ? data.history?.[period] : undefined;
}

export function revenueBreakdownHistoryCaption(company: Company, periods: string[]) {
  const types = new Set(periods.map(period => revenueBreakdownPoint(company, period)?.sourceType).filter(Boolean));
  if (types.has("reported_revenue_hierarchy")) return "The chart retains the issuer’s complete signed revenue hierarchy. Explicit subsegments replace their reported parent only when every child reconciles exactly; Other and eliminations keep their reported signs and source scopes. Geography is shown separately.";
  if (isIndiaListedCompany(company) && types.has("disclosed_revenue_note")) return types.size > 1
    ? "The chart retains each period’s verified disclosure: revenue-note categories reconcile to net operating revenue; reported segment sales retain their disclosed eliminations and reconciliation. Category growth is unavailable across different disclosure types or reporting bases."
    : "The chart retains the issuer’s disclosed revenue-note categories for each available breakdown. Each complete breakdown reconciles to net operating revenue, including separately disclosed other operating revenue. These categories are not necessarily reportable operating segments; intersegment sales are not inferred.";
  return "The chart retains reported segment sales for every period. Intersegment eliminations and unallocated reconciliation are shown separately; each signed breakdown reconciles to total company revenue. External customer sales are not estimated.";
}

export function revenueBreakdownHistorySources(company: Company, periods: string[]) {
  const data = company.companyStory?.revenueOfferings?.segmentReconciliation;
  if (!isIndiaListedCompany(company)) return data?.basis;
  const captions = periods.flatMap(period => {
    const point = revenueBreakdownPoint(company, period);
    return point?.sourceType && (point.basis || point.methodology)
      ? [`${period === "TTM" ? "TTM" : `FY${period}`}: ${[point.basis, point.methodology].filter(Boolean).join(" ")}`] : [];
  });
  return captions.length ? captions.join(" ") : data?.basis;
}
