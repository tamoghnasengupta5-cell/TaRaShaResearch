export type ChangeKind = "percent" | "percentage-points";

export interface MarketIndexOverview {
  id: string;
  country: string;
  countryCode: string;
  flag: string;
  indexName: string;
  ticker: string;
  asOf: string;
  oneWeekPercent: number | null;
  oneYearPercent: number | null;
  fiveYearCagrPercent: number | null;
  sparkline: number[];
  peRatio: number | null;
  pbRatio: number | null;
  trailingYieldPercent: number | null;
  valuationProxy: string;
  valuationAsOf: string;
  gdpToMarketCapRatio: number | null;
  marketCapAsOfYear: string;
  sourceUrl: string;
  valuationSourceUrl: string;
}

export interface CapitalFlowIndicator {
  id: string;
  label: string;
  unit: string;
  value: number | null;
  change: number | null;
  changeKind: ChangeKind;
  comparison: string;
  asOf: string;
  sparkline: number[];
  source: string;
  sourceUrl: string;
}

export interface MarketOverview {
  refreshedAt: string;
  dataAsOf: string;
  indices: MarketIndexOverview[];
  capitalIndicators: CapitalFlowIndicator[];
  warnings: string[];
  dataPolicy: {
    paidDataProviders: 0;
    credentialsRequired: false;
    description: string;
  };
}

export async function fetchMarketOverview(): Promise<MarketOverview> {
  const response = await fetch("/api/market-overview", { headers: { accept: "application/json" } });
  const payload = await response.json().catch(() => ({})) as MarketOverview & { error?: string };
  if (!response.ok) throw new Error(payload.error || "The global market overview is temporarily unavailable.");
  return payload;
}
