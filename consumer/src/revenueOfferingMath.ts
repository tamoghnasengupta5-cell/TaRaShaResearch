import type { Company, OperationsScope } from "./types";

export function isIndiaListedCompany(company: Company) {
  return (company.country === "India" || company.id?.startsWith("data-IN:"))
    && Boolean(company.exchange?.split(/[\s,/]+/).some(exchange => /^(NSE|BSE)$/i.test(exchange)));
}

export function annualRevenueYears(company: Company) {
  return [...new Set([
    ...company.metrics.revenue.filter(point => Number.isFinite(point.value)).map(point => point.year),
    ...Object.entries(company.companyStory?.revenueOfferings?.annualTotals ?? {})
      .filter(([, value]) => value !== null && Number.isFinite(value)).map(([year]) => Number(year)),
  ].filter(Number.isFinite))].sort((a, b) => a - b);
}

export function periodsHaveComparableReportingBasis(company: Company, periods: Array<string | number>) {
  if (!isIndiaListedCompany(company)) return true;
  const data = company.companyStory?.revenueOfferings;
  const bases = periods.map(period => String(period) === "TTM"
    ? data?.segmentReconciliation?.reportingBasis ?? company.reportingBasis
    : data?.annualReportingBases?.[String(period)]
      ?? data?.segmentReconciliation?.history?.[String(period)]?.reportingBasis
      ?? company.reportingBasisByYear?.[String(period)]);
  return bases.every(basis => basis === undefined)
    || bases.every(Boolean) && new Set(bases).size === 1;
}

export function periodReportingBasisLabel(company: Company, period: string | number) {
  if (!isIndiaListedCompany(company)) return null;
  const annualBases = company.companyStory?.revenueOfferings?.annualReportingBases ?? {};
  const segmentBases = Object.fromEntries(Object.entries(company.companyStory?.revenueOfferings?.segmentReconciliation?.history ?? {})
    .flatMap(([year, point]) => point.reportingBasis ? [[year, point.reportingBasis]] : []));
  const bases = { ...company.reportingBasisByYear, ...segmentBases, ...annualBases };
  const distinct = new Set([...Object.values(bases), company.reportingBasis].filter(Boolean));
  if (distinct.size < 2) return null;
  const basis = String(period) === "TTM" ? company.reportingBasis : bases[String(period)];
  return basis === "consolidated" ? "Consolidated" : basis === "standalone" ? "Standalone" : null;
}

export function periodReportingCurrency(company: Company, period: string | number) {
  if (!isIndiaListedCompany(company)) return company.currency.startsWith("US$") ? "USD" : company.currency.split(" ")[0];
  const key = String(period);
  if (key === "TTM") return company.companyStory?.revenueOfferings?.ttm?.reportingCurrency
    ?? (company.currency.startsWith("US$") ? "USD" : company.currency.split(" ")[0]);
  if (Object.hasOwn(company.annualCurrencyByYear ?? {}, key)) return company.annualCurrencyByYear![key];
  return company.companyStory?.revenueOfferings?.annualReportingCurrencies?.[key]
    ?? (company.currency.startsWith("US$") ? "USD" : company.currency.split(" ")[0]);
}

export function companyForPeriod(company: Company, period: string | number) {
  if (!isIndiaListedCompany(company)) return company;
  const currency = periodReportingCurrency(company, period);
  return { ...company, currency: currency ? `${currency} millions` : "Currency unavailable" };
}

export function periodReportingLabel(company: Company, period: string | number) {
  const basis = periodReportingBasisLabel(company, period);
  if (!isIndiaListedCompany(company)) return basis;
  return [basis, periodReportingCurrency(company, period) ?? "Currency unavailable"].filter(Boolean).join(" · ");
}

export function periodsHaveComparableCurrency(company: Company, periods: Array<string | number>) {
  if (!isIndiaListedCompany(company)) return true;
  const currencies = periods.map(period => periodReportingCurrency(company, period));
  return currencies.every(Boolean) && new Set(currencies).size <= 1;
}

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
  const periods = annualRevenueYears(company).map(String);
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


/** Scope declarations and comparative proof groups are passed through Data. */
export function periodOperationsScope(company: Company, period: string | number): OperationsScope | null | undefined {
  const data = company.companyStory?.revenueOfferings;
  return String(period) === "TTM" ? data?.ttm?.operationsScope : data?.annualOperationsScopes?.[String(period)];
}

export function periodsHaveComparableOperationsScope(company: Company, periods: Array<string | number>) {
  if (!isIndiaListedCompany(company)) return true;
  const data = company.companyStory?.revenueOfferings;
  const scopes = periods.map(period => periodOperationsScope(company, period));
  if (!scopes.some(scope => scope && scope !== "whole")) return true;
  if (!scopes.every(scope => scope === "continuing")) return false;
  const groups = periods.map(period => String(period) === "TTM"
    ? data?.ttm?.operationsScopeGroup : data?.annualOperationsScopeGroups?.[String(period)]);
  return groups.every(Boolean) && new Set(groups).size === 1;
}

export function periodOperationsScopeLabel(company: Company, period: string | number) {
  if (!isIndiaListedCompany(company)) return null;
  const data = company.companyStory?.revenueOfferings;
  const scopes = [...Object.values(data?.annualOperationsScopes ?? {}), data?.ttm?.operationsScope];
  if (!scopes.some(scope => scope && scope !== "whole")) return null;
  const scope = periodOperationsScope(company, period);
  return scope === "continuing" ? "Continuing operations" : scope === "whole" ? "Whole company"
    : scope === "discontinued" ? "Discontinued operations" : scope === "undetermined"
      ? "Scope unavailable" : "Scope not stated";
}

export function periodRevenueReportingLabel(company: Company, period: string | number) {
  return [periodReportingLabel(company, period), periodOperationsScopeLabel(company, period)]
    .filter(Boolean).join(" · ") || null;
}
