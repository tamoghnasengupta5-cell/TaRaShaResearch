import type { CatalogCompany, Company, FilingDocument, MetricKey, StatementFact, StatementGroup, YearValue } from "./types";

interface SecFactEntry {
  start?: string;
  end: string;
  val: number;
  accn: string;
  fy?: number;
  fp?: string;
  form: string;
  filed: string;
}

interface SecConcept {
  label?: string;
  description?: string;
  units?: Record<string, SecFactEntry[]>;
}

export interface SecCompanyFacts {
  entityName?: string;
  facts?: Record<string, Record<string, SecConcept>>;
}

export interface SecSubmissions {
  filings?: {
    recent?: Record<string, Array<string>>;
  };
}

interface FactDefinition {
  key: string;
  label: string;
  tags: string[];
  taxonomy?: "us-gaap" | "dei";
  unitPreference: string[];
  scale: number;
  description: string;
}

const incomeDefinitions: FactDefinition[] = [
  { key: "revenue", label: "Revenue", tags: ["RevenueFromContractWithCustomerExcludingAssessedTax", "Revenues", "SalesRevenueNet"], unitPreference: ["USD"], scale: 1_000_000, description: "Reported sales or operating revenue." },
  { key: "grossProfit", label: "Gross profit", tags: ["GrossProfit"], unitPreference: ["USD"], scale: 1_000_000, description: "Revenue after the direct cost of goods or services." },
  { key: "operatingIncome", label: "Operating income", tags: ["OperatingIncomeLoss"], unitPreference: ["USD"], scale: 1_000_000, description: "Profit from operations before interest and tax." },
  { key: "netIncome", label: "Net income", tags: ["NetIncomeLoss", "ProfitLoss"], unitPreference: ["USD"], scale: 1_000_000, description: "Profit after reported expenses and taxes." },
  { key: "dilutedEps", label: "Diluted EPS", tags: ["EarningsPerShareDiluted"], unitPreference: ["USD/shares", "USD-per-shares"], scale: 1, description: "Net income per diluted weighted-average share." },
];

const balanceDefinitions: FactDefinition[] = [
  { key: "cash", label: "Cash and equivalents", tags: ["CashAndCashEquivalentsAtCarryingValue", "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"], unitPreference: ["USD"], scale: 1_000_000, description: "Reported cash and near-cash balances." },
  { key: "currentAssets", label: "Current assets", tags: ["AssetsCurrent"], unitPreference: ["USD"], scale: 1_000_000, description: "Assets expected to turn into cash or be used within the operating cycle." },
  { key: "assets", label: "Total assets", tags: ["Assets"], unitPreference: ["USD"], scale: 1_000_000, description: "All resources reported on the balance sheet." },
  { key: "currentLiabilities", label: "Current liabilities", tags: ["LiabilitiesCurrent"], unitPreference: ["USD"], scale: 1_000_000, description: "Obligations due within the operating cycle." },
  { key: "longTermDebt", label: "Long-term debt", tags: ["LongTermDebtAndFinanceLeaseObligations", "LongTermDebt", "LongTermDebtNoncurrent"], unitPreference: ["USD"], scale: 1_000_000, description: "Reported longer-term borrowings and, where combined, finance leases." },
  { key: "liabilities", label: "Total liabilities", tags: ["Liabilities"], unitPreference: ["USD"], scale: 1_000_000, description: "All reported obligations." },
  { key: "equity", label: "Stockholders’ equity", tags: ["StockholdersEquity", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"], unitPreference: ["USD"], scale: 1_000_000, description: "Reported residual interest after liabilities." },
];

const cashDefinitions: FactDefinition[] = [
  { key: "operatingCash", label: "Operating cash flow", tags: ["NetCashProvidedByUsedInOperatingActivities"], unitPreference: ["USD"], scale: 1_000_000, description: "Net cash generated or used by operations." },
  { key: "capex", label: "Capital expenditure", tags: ["PaymentsToAcquirePropertyPlantAndEquipment", "PaymentsForProceedsFromProductiveAssets"], unitPreference: ["USD"], scale: 1_000_000, description: "Cash paid for property, plant and equipment." },
  { key: "investingCash", label: "Investing cash flow", tags: ["NetCashProvidedByUsedInInvestingActivities"], unitPreference: ["USD"], scale: 1_000_000, description: "Net cash generated or used by investing activities." },
  { key: "financingCash", label: "Financing cash flow", tags: ["NetCashProvidedByUsedInFinancingActivities"], unitPreference: ["USD"], scale: 1_000_000, description: "Net cash generated or used by financing activities." },
  { key: "dividends", label: "Dividends paid", tags: ["PaymentsOfDividends", "PaymentsOfDividendsCommonStock"], unitPreference: ["USD"], scale: 1_000_000, description: "Cash dividends paid to shareholders." },
];

const shareDefinitions: FactDefinition[] = [
  { key: "sharesOutstanding", label: "Shares outstanding", tags: ["EntityCommonStockSharesOutstanding"], taxonomy: "dei", unitPreference: ["shares"], scale: 1_000_000, description: "Common shares outstanding reported in the filing cover information." },
  { key: "weightedAverageDiluted", label: "Weighted-average diluted shares", tags: ["WeightedAverageNumberOfDilutedSharesOutstanding"], unitPreference: ["shares"], scale: 1_000_000, description: "Diluted weighted-average shares used for earnings per share." },
  { key: "sharesRepurchased", label: "Shares repurchased", tags: ["StockRepurchasedAndRetiredDuringPeriodValue", "PaymentsForRepurchaseOfCommonStock"], unitPreference: ["USD"], scale: 1_000_000, description: "Reported value of common-share repurchases when tagged." },
];

function annualSeries(concept: SecConcept | undefined, definition: FactDefinition, fromYear: number, toYear: number): YearValue[] {
  if (!concept?.units) return [];
  const unit = definition.unitPreference.find((candidate) => concept.units?.[candidate]?.length) ?? Object.keys(concept.units)[0];
  const entries = (concept.units[unit] ?? []).filter((entry) => {
    const year = Number(entry.end?.slice(0, 4));
    return entry.form === "10-K" && year >= fromYear && year <= toYear && Number.isFinite(entry.val);
  });
  const byYear = new Map<number, SecFactEntry>();
  for (const entry of entries) {
    const year = Number(entry.end.slice(0, 4));
    const existing = byYear.get(year);
    if (!existing || entry.filed > existing.filed) byYear.set(year, entry);
  }
  return [...byYear.entries()].sort(([a], [b]) => a - b).map(([year, entry]) => ({ year, value: Number(entry.val) / definition.scale }));
}

function extractFact(facts: SecCompanyFacts, definition: FactDefinition, fromYear: number, toYear: number): StatementFact | null {
  const taxonomy = definition.taxonomy ?? "us-gaap";
  const concepts = facts.facts?.[taxonomy] ?? {};
  for (const tag of definition.tags) {
    const values = annualSeries(concepts[tag], definition, fromYear, toYear);
    if (values.length) return { key: definition.key, label: definition.label, description: definition.description, unit: definition.scale === 1_000_000 ? (definition.unitPreference[0] === "shares" ? "million shares" : "US$ million") : "US$ per share", values };
  }
  return null;
}

function group(key: StatementGroup["key"], label: string, definitions: FactDefinition[], facts: SecCompanyFacts, fromYear: number, toYear: number): StatementGroup {
  return { key, label, facts: definitions.map((definition) => extractFact(facts, definition, fromYear, toYear)).filter((fact): fact is StatementFact => Boolean(fact)) };
}

function valueMap(fact: StatementFact | undefined): Map<number, number> {
  return new Map((fact?.values ?? []).map((item) => [item.year, item.value]));
}

function combineYears(...facts: Array<StatementFact | undefined>): number[] {
  return [...new Set(facts.flatMap((fact) => fact?.values.map((value) => value.year) ?? []))].sort((a, b) => a - b);
}

function derivedMetrics(groups: StatementGroup[]): Record<MetricKey, YearValue[]> {
  const all = groups.flatMap((item) => item.facts);
  const find = (key: string) => all.find((fact) => fact.key === key);
  const revenue = find("revenue");
  const operating = find("operatingIncome");
  const operatingCash = find("operatingCash");
  const capex = find("capex");
  const debt = find("longTermDebt");
  const cash = find("cash");
  const ratio = (left: StatementFact | undefined, right: StatementFact | undefined, transform: (a: number, b: number) => number) => {
    const leftMap = valueMap(left); const rightMap = valueMap(right);
    return combineYears(left, right).filter((year) => leftMap.has(year) && rightMap.has(year) && rightMap.get(year) !== 0)
      .map((year) => ({ year, value: transform(leftMap.get(year)!, rightMap.get(year)!) }));
  };
  return {
    revenue: revenue?.values ?? [],
    operatingMargin: ratio(operating, revenue, (a, b) => (a / b) * 100),
    freeCashFlow: ratio(operatingCash, capex, (a, b) => a - Math.abs(b)),
    netDebt: ratio(debt, cash, (a, b) => a - b),
  };
}

function filingDocuments(catalog: CatalogCompany, submissions: SecSubmissions, fromYear: number, toYear: number): FilingDocument[] {
  const recent = submissions.filings?.recent ?? {};
  const forms = recent.form ?? [];
  const result: FilingDocument[] = [];
  for (let index = 0; index < forms.length; index += 1) {
    const form = forms[index];
    const filed = recent.filingDate?.[index] ?? "";
    const year = Number(filed.slice(0, 4));
    const items = recent.items?.[index] ?? "";
    if (year < fromYear || year > toYear || (form !== "10-K" && form !== "10-Q" && !(form === "8-K" && items.includes("2.02")))) continue;
    const accession = recent.accessionNumber?.[index] ?? "";
    const primary = recent.primaryDocument?.[index] ?? "";
    if (!accession || !primary || !catalog.cik) continue;
    const accessionPath = accession.replace(/-/g, "");
    const cikPath = String(Number(catalog.cik));
    result.push({
      accession,
      form: form as FilingDocument["form"],
      filed,
      period: recent.reportDate?.[index] ?? "",
      title: form === "10-K" ? "Annual report (10-K)" : form === "10-Q" ? "Quarterly report (10-Q)" : "Earnings-related current report (8-K)",
      url: `https://www.sec.gov/Archives/edgar/data/${cikPath}/${accessionPath}/${primary}`,
    });
  }
  return result.sort((a, b) => b.filed.localeCompare(a.filed));
}

export function normalizeSecResearch(catalog: CatalogCompany, facts: SecCompanyFacts, submissions: SecSubmissions, fromYear: number, toYear: number): Company {
  const groups = [
    group("income", "Income statement", incomeDefinitions, facts, fromYear, toYear),
    group("balance", "Balance sheet", balanceDefinitions, facts, fromYear, toYear),
    group("cash", "Cash-flow statement", cashDefinitions, facts, fromYear, toYear),
    group("shares", "Share information", shareDefinitions, facts, fromYear, toYear),
  ];
  const metrics = derivedMetrics(groups);
  const availableYears = [...new Set(groups.flatMap((item) => item.facts.flatMap((fact) => fact.values.map((value) => value.year))))].sort();
  const latestYear = availableYears.at(-1) ?? toYear;
  return {
    id: catalog.id,
    name: facts.entityName || catalog.name,
    symbol: catalog.ticker,
    sector: `${catalog.exchange} · SEC filer`,
    description: "Structured facts extracted live from SEC EDGAR filings for this browser session.",
    currency: "US$ million",
    reportingPeriod: `FY ${latestYear}`,
    updatedAt: new Date().toLocaleDateString("en-GB", { day: "numeric", month: "long", year: "numeric" }),
    metrics,
    statements: groups,
    filings: filingDocuments(catalog, submissions, fromYear, toYear),
    limitations: [
      "SEC XBRL tags vary by filer; unavailable or non-standard concepts may not appear.",
      "Share information means reported shares outstanding and related share facts—not a complete shareholder ownership register.",
      "EDGAR does not provide earnings-call transcripts. Earnings-related 8-K filings are linked when identifiable.",
      "All extracted financial data is held only in browser memory and disappears when this page session ends.",
    ],
    dataMode: "sec-live",
    notes: {
      growth: "Review the multi-year reported revenue series and its source filings.",
      profitability: "Operating margin is calculated from tagged operating income divided by tagged revenue when both are available.",
      cash: "Free cash flow is calculated as operating cash flow less tagged capital expenditure when both are available.",
      debt: "Net debt is calculated from tagged long-term debt less cash when both are available; issuer tagging may omit some obligations.",
    },
  };
}
