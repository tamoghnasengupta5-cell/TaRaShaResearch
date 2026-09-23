import type { Company } from "./types";

export function latestAnnualRevenueEnd(company: Company) {
  const offeringEnds = Object.values(
    company.companyStory?.revenueOfferings?.annualPeriodEnds ?? {},
  ).filter(Boolean);
  const segmentEnd = company.companyStory?.revenueSegments?.latestPeriodEnd;
  const observedEnds = [...offeringEnds, ...(segmentEnd ? [segmentEnd] : [])];
  if (observedEnds.length) return observedEnds.sort().at(-1) ?? null;
  const years = company.metrics.revenue.map(point => point.year).filter(Number.isFinite);
  return years.length ? `${Math.max(...years)}-12-31` : null;
}

export function hasNewerRevenueTtm(company: Company) {
  const trailing = company.companyStory?.revenueOfferings?.ttm;
  const latestAnnualEnd = latestAnnualRevenueEnd(company);
  return Boolean(trailing && (!latestAnnualEnd || trailing.periodEnd > latestAnnualEnd));
}

export function revenuePeriods(company: Company, newestFirst: boolean, includeTtm = true) {
  const years = [
    ...company.metrics.revenue.map(point => point.year),
    ...Object.keys(company.companyStory?.revenueOfferings?.annualTotals ?? {}).map(Number),
  ].filter(Number.isFinite);
  const latest = years.length ? Math.max(...years) : null;
  const periods = latest === null
    ? []
    : Array.from({ length: 6 }, (_, index) => String(latest - 5 + index));
  if (includeTtm && hasNewerRevenueTtm(company)) periods.push("TTM");
  return newestFirst ? periods.reverse() : periods;
}

export function revenueGrowth(current: number | null, previous: number | null) {
  return current === null || previous === null || previous <= 0
    ? null
    : (current / previous - 1) * 100;
}

/** CAGR across three consecutive reported values: current and two prior comparables. */
export function threePeriodRevenueCagr(current: number | null, priorToPrior: number | null) {
  return current === null || priorToPrior === null || current <= 0 || priorToPrior <= 0
    ? null
    : (Math.pow(current / priorToPrior, 1 / 2) - 1) * 100;
}
