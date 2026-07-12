export interface ResearchProviderEnv {
  SHARED_RESEARCH_URL?: string;
  SHARED_RESEARCH_SERVICE_KEY?: string;
}

interface ResearchCompanyRow {
  id: number;
  name: string;
  ticker: string;
  country: "USA" | "India";
}

interface ResearchFactRow {
  company_id: number;
  statement_key: "income" | "balance" | "cash" | "shares";
  fact_key: string;
  label: string;
  unit_kind: "amount" | "shares";
  fiscal_year: number;
  value: number;
}

interface YearValue {
  year: number;
  value: number;
}

const factDescriptions: Record<string, string> = {
  revenue: "Sales or operating revenue imported into TaRaSha Research.",
  costOfRevenue: "Direct cost associated with reported revenue.",
  sga: "Selling, general and administrative expense.",
  ebitda: "Earnings before interest, tax, depreciation and amortization.",
  operatingIncome: "Profit from operations before interest and tax.",
  interestExpense: "Reported financing cost.",
  pretaxIncome: "Reported income before tax.",
  netIncome: "Reported profit after expenses and tax.",
  cash: "Cash and cash-equivalent balance.",
  shortTermInvestments: "Reported short-term investments.",
  accountsReceivable: "Amounts due from customers and other debtors.",
  inventory: "Reported inventory balance.",
  currentAssets: "Assets expected to turn into cash or be used in the operating cycle.",
  assets: "Total reported assets.",
  accountsPayable: "Amounts owed to suppliers and other creditors.",
  currentDebt: "Borrowings classified as current.",
  currentLiabilities: "Obligations classified as current.",
  longTermLiabilities: "Reported longer-term obligations.",
  totalDebt: "Total reported borrowings.",
  equity: "Reported shareholders’ equity.",
  operatingCash: "Net cash generated or used by operations.",
  capex: "Reported capital expenditure.",
  depreciation: "Reported depreciation and amortization.",
  netDebtIssuedPaid: "Net debt issued or repaid during the year.",
  sharesOutstanding: "Reported basic shares outstanding.",
  marketCapitalization: "Reported year-end market capitalization.",
};

const statementLabels = {
  income: "Income statement",
  balance: "Balance sheet",
  cash: "Cash-flow statement",
  shares: "Share information",
} as const;

function requireConfiguration(env: ResearchProviderEnv): { base: string; key: string } {
  const base = String(env.SHARED_RESEARCH_URL ?? "").replace(/\/$/, "");
  const key = String(env.SHARED_RESEARCH_SERVICE_KEY ?? "");
  if (!base || !key) throw new Error("Shared Research database provider is not configured.");
  return { base, key };
}

async function researchFetch<T>(env: ResearchProviderEnv, path: string): Promise<T> {
  const { base, key } = requireConfiguration(env);
  const response = await fetch(`${base}/rest/v1/${path}`, {
    headers: { apikey: key, Authorization: `Bearer ${key}`, Accept: "application/json" },
  });
  if (!response.ok) throw new Error(`Shared Research database returned ${response.status}.`);
  return response.json<T>();
}

export function researchProviderEnabled(env: { DATA_PROVIDER?: string }): boolean {
  return env.DATA_PROVIDER === "research-db";
}

export async function searchResearchCompanies(env: ResearchProviderEnv, query: string, country: "USA" | "India") {
  const safeQuery = query.replace(/[^a-zA-Z0-9 .&-]/g, "").trim().slice(0, 60);
  if (safeQuery.length < 2) return [];
  const params = new URLSearchParams({
    select: "id,name,ticker,country",
    country: `eq.${country}`,
    or: `(name.ilike.*${safeQuery}*,ticker.ilike.*${safeQuery}*)`,
    limit: "30",
  });
  const rows = await researchFetch<ResearchCompanyRow[]>(env, `consumer_companies?${params}`);
  return rows
    .sort((left, right) => Number(left.ticker.toLowerCase() !== safeQuery.toLowerCase()) - Number(right.ticker.toLowerCase() !== safeQuery.toLowerCase()) || left.name.localeCompare(right.name))
    .map((row) => ({
      id: `research-${row.id}`,
      cik: null,
      name: row.name,
      ticker: row.ticker,
      exchange: row.country,
      country: row.country,
      provider: "TaRaSha Research database",
      research_available: 1,
      data_access: "normalized" as const,
    }));
}

function seriesFor(facts: ResearchFactRow[], key: string): YearValue[] {
  return facts.filter((fact) => fact.fact_key === key).map((fact) => ({ year: fact.fiscal_year, value: fact.value }));
}

function derivedSeries(left: YearValue[], right: YearValue[], operation: (a: number, b: number) => number): YearValue[] {
  const rightByYear = new Map(right.map((item) => [item.year, item.value]));
  return left.filter((item) => rightByYear.has(item.year) && rightByYear.get(item.year) !== 0)
    .map((item) => ({ year: item.year, value: operation(item.value, rightByYear.get(item.year)!) }));
}

function netDebtSeries(debt: YearValue[], cash: YearValue[], investments: YearValue[]): YearValue[] {
  const cashByYear = new Map(cash.map((item) => [item.year, item.value]));
  const investmentsByYear = new Map(investments.map((item) => [item.year, item.value]));
  return debt.filter((item) => cashByYear.has(item.year)).map((item) => ({
    year: item.year,
    value: item.value - cashByYear.get(item.year)! - (investmentsByYear.get(item.year) ?? 0),
  }));
}

export async function pullResearchCompany(env: ResearchProviderEnv, companyId: string, fromYear: number, toYear: number) {
  const numericId = Number(companyId.replace(/^research-/, ""));
  if (!Number.isInteger(numericId) || numericId <= 0) throw new Error("Invalid Research company identifier.");
  const companyParams = new URLSearchParams({ select: "id,name,ticker,country", id: `eq.${numericId}`, limit: "1" });
  const companies = await researchFetch<ResearchCompanyRow[]>(env, `consumer_companies?${companyParams}`);
  const company = companies[0];
  if (!company) return null;
  const factParams = new URLSearchParams({
    select: "company_id,statement_key,fact_key,label,unit_kind,fiscal_year,value",
    company_id: `eq.${numericId}`,
    fiscal_year: `gte.${fromYear}`,
    and: `(fiscal_year.lte.${toYear})`,
    order: "statement_key.asc,fact_key.asc,fiscal_year.asc",
  });
  const rawFacts = await researchFetch<ResearchFactRow[]>(env, `consumer_financial_facts?${factParams}`);
  const amountScale = company.country === "India" ? 10 : 1;
  const amountUnit = company.country === "India" ? "₹ crore" : "US$ million";
  const facts = rawFacts.map((fact) => ({ ...fact, value: fact.unit_kind === "amount" ? Number(fact.value) / amountScale : Number(fact.value) }));
  const statements = (Object.keys(statementLabels) as Array<keyof typeof statementLabels>).map((statementKey) => {
    const statementFacts = facts.filter((fact) => fact.statement_key === statementKey);
    const grouped = new Map<string, ResearchFactRow[]>();
    for (const fact of statementFacts) grouped.set(fact.fact_key, [...(grouped.get(fact.fact_key) ?? []), fact]);
    return {
      key: statementKey,
      label: statementLabels[statementKey],
      facts: [...grouped.entries()].map(([key, values]) => ({
        key,
        label: values[0].label,
        description: factDescriptions[key] ?? "Imported historical financial fact.",
        unit: values[0].unit_kind === "shares" ? "million shares" : amountUnit,
        values: values.map((value) => ({ year: value.fiscal_year, value: value.value })),
      })),
    };
  });
  const revenue = seriesFor(facts, "revenue");
  const operatingIncome = seriesFor(facts, "operatingIncome");
  const operatingCash = seriesFor(facts, "operatingCash");
  const capex = seriesFor(facts, "capex");
  const debt = seriesFor(facts, "totalDebt");
  const cash = seriesFor(facts, "cash");
  const investments = seriesFor(facts, "shortTermInvestments");
  const latestYear = Math.max(...facts.map((fact) => fact.fiscal_year), toYear);
  return {
    id: `research-${company.id}`,
    name: company.name,
    symbol: company.ticker,
    sector: `${company.country} · TaRaSha Research coverage`,
    description: "Historical financial statements imported through the TaRaSha Private Research bulk-upload workflow.",
    currency: amountUnit,
    reportingPeriod: `FY ${latestYear}`,
    updatedAt: new Date().toLocaleDateString("en-GB", { day: "numeric", month: "long", year: "numeric", timeZone: "UTC" }),
    metrics: {
      revenue,
      operatingMargin: derivedSeries(operatingIncome, revenue, (a, b) => (a / b) * 100),
      freeCashFlow: derivedSeries(operatingCash, capex, (a, b) => a - Math.abs(b)),
      netDebt: netDebtSeries(debt, cash, investments),
    },
    notes: {
      growth: "Review the imported multi-year revenue record and the underlying spreadsheet source before drawing conclusions.",
      profitability: "Operating margin is calculated from imported operating income divided by imported revenue.",
      cash: "Free cash flow is calculated as imported operating cash flow less the absolute value of imported capital expenditure.",
      debt: "Net debt is calculated as imported total debt less cash and short-term investments when available.",
    },
    statements,
    filings: [],
    limitations: [
      "Private, non-commercial preview. Commercial redistribution is disabled pending source-provider permission.",
      "The figures originate in spreadsheets downloaded through StockAnalysis.com and bulk-uploaded into TaRaSha Research; StockAnalysis.com may use third-party data providers.",
      "Imported values can differ from issuer filings because of provider definitions, restatements, currency units or spreadsheet mapping.",
      "TaRaSha Consumer stores no separate financial copy; this response is held only in browser session memory.",
    ],
    dataMode: "research-db" as const,
    source: {
      dataset: "TaRaSha Research bulk-upload database",
      upstream: "StockAnalysis.com spreadsheet download",
      usage: "Private non-commercial evaluation only",
      persistence: "Browser session memory only",
    },
  };
}
