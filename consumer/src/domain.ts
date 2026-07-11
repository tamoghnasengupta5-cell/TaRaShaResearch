import type { Company, MetricKey, YearValue } from "./types";

export function latest(series: YearValue[]): YearValue {
  if (!series.length) throw new Error("Metric series cannot be empty");
  return series[series.length - 1];
}

export function previous(series: YearValue[]): YearValue | undefined {
  return series.length > 1 ? series[series.length - 2] : undefined;
}

export function percentChange(series: YearValue[]): number | null {
  const current = latest(series).value;
  const prior = previous(series)?.value;
  if (prior === undefined || prior === 0) return null;
  return ((current - prior) / Math.abs(prior)) * 100;
}

export function formatMetric(key: MetricKey, value: number): string {
  if (key === "operatingMargin") return `${value.toFixed(1)}%`;
  const sign = value < 0 ? "−" : "";
  return `${sign}₹${Math.abs(value).toLocaleString("en-IN", { maximumFractionDigits: 0 })} cr`;
}

export function companySearch(company: Company, query: string, sector: string): boolean {
  const normalized = query.trim().toLowerCase();
  const matchesText = !normalized || [company.name, company.symbol, company.description]
    .some((value) => value.toLowerCase().includes(normalized));
  return matchesText && (sector === "All sectors" || company.sector === sector);
}
