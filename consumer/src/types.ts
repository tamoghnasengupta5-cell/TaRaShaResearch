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
  founded: number;
  employees: string;
  currency: "₹ crore";
  reportingPeriod: string;
  updatedAt: string;
  metrics: Record<MetricKey, YearValue[]>;
  notes: {
    growth: string;
    profitability: string;
    cash: string;
    debt: string;
  };
}

export type Page = "home" | "discover" | "company" | "compare" | "watchlist" | "learn";
