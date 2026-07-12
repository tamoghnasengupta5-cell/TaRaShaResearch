export type MetricKey = "revenue" | "operatingMargin" | "freeCashFlow" | "netDebt";

export interface YearValue {
  year: number;
  value: number;
}

export interface Company {
  id: string;
  name: string;
  symbol: string;
  sector: string;
  description: string;
  founded?: number;
  employees?: string;
  currency: string;
  reportingPeriod: string;
  updatedAt: string;
  metrics: Record<MetricKey, YearValue[]>;
  notes: {
    growth: string;
    profitability: string;
    cash: string;
    debt: string;
  };
  statements?: StatementGroup[];
  filings?: FilingDocument[];
  limitations?: string[];
  dataMode?: "illustrative" | "sec-live" | "research-db";
  source?: {
    dataset: string;
    upstream: string;
    usage: string;
    persistence: string;
  };
}

export interface CatalogCompany {
  id: string;
  cik: string | null;
  name: string;
  ticker: string;
  exchange: string;
  country: "USA" | "India";
  provider: string;
  research_available: number;
  data_access?: "sec" | "normalized";
}

export interface StatementFact {
  key: string;
  label: string;
  description: string;
  unit: string;
  values: YearValue[];
}

export interface StatementGroup {
  key: "income" | "balance" | "cash" | "shares";
  label: string;
  facts: StatementFact[];
}

export interface FilingDocument {
  accession: string;
  form: "10-K" | "10-Q" | "8-K";
  filed: string;
  period: string;
  title: string;
  url: string;
}

export type Page = "home" | "discover" | "company" | "compare" | "watchlist" | "learn";
