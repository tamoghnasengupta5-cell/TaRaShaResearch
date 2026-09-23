export type ChangeKind = "percent" | "percentage-points";

export interface MarketSeriesPoint {
  date: string;
  value: number;
}

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

type FetchLike = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;

interface IndexConfig {
  id: string;
  country: string;
  countryCode: string;
  flag: string;
  indexName: string;
  ticker: string;
  yahooSymbol: string;
  sourceUrl: string;
  valuationProxy: string;
  valuationUrl: string;
  worldBankCountry: string;
}

const INDEX_CONFIGS: IndexConfig[] = [
  {
    id: "united-states",
    country: "United States",
    countryCode: "US",
    flag: "🇺🇸",
    indexName: "S&P 500",
    ticker: "^GSPC",
    yahooSymbol: "%5EGSPC",
    sourceUrl: "https://finance.yahoo.com/quote/%5EGSPC/",
    valuationProxy: "IVV · iShares Core S&P 500 ETF",
    valuationUrl: "https://www.ishares.com/us/products/239726/IVV",
    worldBankCountry: "USA",
  },
  {
    id: "united-kingdom",
    country: "United Kingdom",
    countryCode: "GB",
    flag: "🇬🇧",
    indexName: "FTSE 100",
    ticker: "^FTSE",
    yahooSymbol: "%5EFTSE",
    sourceUrl: "https://finance.yahoo.com/quote/%5EFTSE/",
    valuationProxy: "EWU · iShares MSCI United Kingdom ETF",
    valuationUrl: "https://www.ishares.com/us/products/239690/EWU",
    worldBankCountry: "GBR",
  },
  {
    id: "japan",
    country: "Japan",
    countryCode: "JP",
    flag: "🇯🇵",
    indexName: "Nikkei 225",
    ticker: "^N225",
    yahooSymbol: "%5EN225",
    sourceUrl: "https://finance.yahoo.com/quote/%5EN225/",
    valuationProxy: "EWJ · iShares MSCI Japan ETF",
    valuationUrl: "https://www.ishares.com/us/products/239665/EWJ",
    worldBankCountry: "JPN",
  },
  {
    id: "china",
    country: "China",
    countryCode: "CN",
    flag: "🇨🇳",
    indexName: "Shanghai Composite",
    ticker: "000001.SS",
    yahooSymbol: "000001.SS",
    sourceUrl: "https://finance.yahoo.com/quote/000001.SS/",
    valuationProxy: "MCHI · iShares MSCI China ETF",
    valuationUrl: "https://www.ishares.com/us/products/239619/MCHI",
    worldBankCountry: "CHN",
  },
  {
    id: "india",
    country: "India",
    countryCode: "IN",
    flag: "🇮🇳",
    indexName: "NIFTY 50",
    ticker: "^NSEI",
    yahooSymbol: "%5ENSEI",
    sourceUrl: "https://finance.yahoo.com/quote/%5ENSEI/",
    valuationProxy: "INDA · iShares MSCI India ETF",
    valuationUrl: "https://www.ishares.com/us/products/239659/INDA",
    worldBankCountry: "IND",
  },
];

const FALLBACK_INDICES: MarketIndexOverview[] = [
  ["united-states", "United States", "US", "🇺🇸", "S&P 500", "^GSPC", "2026-08-24", -1.19, 18.85, 11.22, [7428.78, 7316.15, 7437.63, 7489.72, 7600.5, 7736.52, 7723.55, 7709.96, 7757.64, 7753.11, 7728.2, 7748.5, 7798.99, 7785.76, 7745.06, 7691.76, 7707.98, 7641.16, 7674.37, 7652.86], 30.15, 5.63, 1.09, "IVV · iShares Core S&P 500 ETF", "2026-08-21", 0.45, "2025", "https://finance.yahoo.com/quote/%5EGSPC/", "https://www.ishares.com/us/products/239726/IVV"],
  ["united-kingdom", "United Kingdom", "GB", "🇬🇧", "FTSE 100", "^FTSE", "2026-08-24", 1.25, 16.45, 8.71, [10871, 10908.4, 10897.3, 10868.1, 10857.7, 10879.4, 10888.3, 10867.9, 10901.1, 10862.5, 10844.2, 10833.2, 10772.7, 10750.1, 10720.3, 10728, 10743.4, 10748.2, 10816.6, 10854.32], 17.99, 2.4, 3.09, "EWU · iShares MSCI United Kingdom ETF", "2026-08-21", 1.03, "2022", "https://finance.yahoo.com/quote/%5EFTSE/", "https://www.ishares.com/us/products/239690/EWU"],
  ["japan", "Japan", "JP", "🇯🇵", "Nikkei 225", "^N225", "2026-08-21", -3.93, 54.93, 18.94, [64611.15, 64931.19, 62364.92, 61434.19, 61867.43, 64362.02, 63754.9, 63957.53, 66300.44, 65683.26, 65606.71, 66970.22, 67524.06, 68308.59, 68713.8, 69220.25, 67460.73, 65326.42, 66216.79, 66016.36], 19.05, 2.03, 3.8, "EWJ · iShares MSCI Japan ETF", "2026-08-21", 0.58, "2025", "https://finance.yahoo.com/quote/%5EN225/", "https://www.ishares.com/us/products/239665/EWJ"],
  ["china", "China", "CN", "🇨🇳", "Shanghai Composite", "000001.SS", "2026-08-21", -0.56, 3.56, 2.13, [3858.25, 3813.32, 3828.47, 3804.69, 3832.26, 3809.66, 3822.29, 3878.43, 3900.35, 3940.04, 3966.59, 3934.09, 3946.68, 3926.97, 3927.18, 3982.65, 3990.3, 3894.42, 3903.72, 3905.2], 13.78, 1.68, 1.97, "MCHI · iShares MSCI China ETF", "2026-08-21", 1.26, "2025", "https://finance.yahoo.com/quote/000001.SS/", "https://www.ishares.com/us/products/239619/MCHI"],
  ["india", "India", "IN", "🇮🇳", "NIFTY 50", "^NSEI", "2026-08-21", -0.47, -3.32, 7.84, [23995.95, 23985.35, 24250.2, 24317.15, 24383.6, 24774.3, 24614.9, 24624.65, 24636, 24570.65, 24583.8, 24471.7, 24435.95, 24395.85, 24366, 24287.65, 24154.9, 24078.3, 24231.85, 24252], 22.69, 3.28, 0, "INDA · iShares MSCI India ETF", "2026-08-21", 0.37, "2025", "https://finance.yahoo.com/quote/%5ENSEI/", "https://www.ishares.com/us/products/239659/INDA"],
].map((row) => ({
  id: String(row[0]), country: String(row[1]), countryCode: String(row[2]), flag: String(row[3]), indexName: String(row[4]), ticker: String(row[5]), asOf: String(row[6]),
  oneWeekPercent: Number(row[7]), oneYearPercent: Number(row[8]), fiveYearCagrPercent: Number(row[9]), sparkline: row[10] as number[], peRatio: Number(row[11]), pbRatio: Number(row[12]), trailingYieldPercent: Number(row[13]),
  valuationProxy: String(row[14]), valuationAsOf: String(row[15]), gdpToMarketCapRatio: Number(row[16]), marketCapAsOfYear: String(row[17]), sourceUrl: String(row[18]), valuationSourceUrl: String(row[19]),
}));

const FALLBACK_CAPITAL: CapitalFlowIndicator[] = [
  { id: "reserves", label: "Global foreign reserves", unit: "USD trillion", value: 14.86, change: 0.81, changeKind: "percent", comparison: "year over year", asOf: "2024", sparkline: [14.35, 15.1, 14.07, 14.74, 14.86], source: "World Bank · IMF IFS", sourceUrl: "https://data.worldbank.org/indicator/FI.RES.TOTL.CD" },
  { id: "equity-flows", label: "Global equity fund flows", unit: "USD billion", value: 196, change: -48.15, changeKind: "percent", comparison: "quarter over quarter", asOf: "2026 Q1", sparkline: [144, 83, -141, 378, 196], source: "ICI · IIFA", sourceUrl: "https://www.ici.org/statistical-report/ww_q1_26" },
  { id: "bond-flows", label: "Global bond fund flows", unit: "USD billion", value: 385, change: 0, changeKind: "percent", comparison: "quarter over quarter", asOf: "2026 Q1", sparkline: [223, 308, 422, 385, 385], source: "ICI · IIFA", sourceUrl: "https://www.ici.org/statistical-report/ww_q1_26" },
  { id: "vix", label: "VIX index", unit: "fear gauge", value: 15.86, change: 4.41, changeKind: "percent", comparison: "one week", asOf: "2026-08-24", sparkline: [18.21, 20.66, 17.09, 15.99, 15.86, 16.5, 15.81, 15.15, 14.9, 15.46, 15.28, 14.55, 14.63, 14.25, 15.19, 15.84, 14.89, 16.01, 15.13, 15.86], source: "Yahoo Finance", sourceUrl: "https://finance.yahoo.com/quote/%5EVIX/" },
  { id: "dxy", label: "US Dollar Index", unit: "DXY", value: 99, change: -0.64, changeKind: "percent", comparison: "one week", asOf: "2026-08-24", sparkline: [101.38, 100.8, 100.01, 99.8, 99.96, 99.89, 99.69, 99.97, 99.6, 99.81, 99.82, 100.01, 99.96, 99.67, 99.64, 99.65, 98.83, 98.9, 98.8, 99], source: "Yahoo Finance", sourceUrl: "https://finance.yahoo.com/quote/DX-Y.NYB/" },
  { id: "treasury", label: "US 10Y yield", unit: "%", value: 4.74, change: 0.06, changeKind: "percentage-points", comparison: "one week", asOf: "2026-08-21", sparkline: [4.65, 4.72, 4.7, 4.68, 4.63, 4.68, 4.72, 4.71, 4.65, 4.69, 4.74], source: "Federal Reserve · FRED", sourceUrl: "https://fred.stlouisfed.org/series/DGS10" },
  { id: "fed-funds", label: "Fed funds target", unit: "upper bound %", value: 3.75, change: 0, changeKind: "percentage-points", comparison: "one week", asOf: "2026-08-24", sparkline: [3.75, 3.75, 3.75, 3.75, 3.75, 3.75], source: "Federal Reserve · FRED", sourceUrl: "https://fred.stlouisfed.org/series/DFEDTARU" },
];

function cloneFallback(now: Date): MarketOverview {
  return {
    refreshedAt: now.toISOString(),
    dataAsOf: "2026-08-24",
    indices: FALLBACK_INDICES.map((item) => ({ ...item, sparkline: [...item.sparkline] })),
    capitalIndicators: FALLBACK_CAPITAL.map((item) => ({ ...item, sparkline: [...item.sparkline] })),
    warnings: [],
    dataPolicy: {
      paidDataProviders: 0,
      credentialsRequired: false,
      description: "Public, no-key sources only. No paid market-data API is called by this endpoint.",
    },
  };
}

async function fetchOk(fetcher: FetchLike, url: string, accept = "application/json"): Promise<Response> {
  const response = await fetcher(url, {
    headers: {
      accept,
      "user-agent": "TaRaShaDiscover/1.0 (private non-commercial preview)",
    },
  });
  if (!response.ok) throw new Error(`${new URL(url).hostname} returned HTTP ${response.status}`);
  return response;
}

function isoDate(unixSeconds: number): string {
  return new Date(unixSeconds * 1000).toISOString().slice(0, 10);
}

function valueAtOrBefore(points: MarketSeriesPoint[], targetSeconds: number): MarketSeriesPoint | null {
  let selected: MarketSeriesPoint | null = null;
  for (const point of points) {
    const seconds = Date.parse(`${point.date}T00:00:00Z`) / 1000;
    if (seconds <= targetSeconds) selected = point;
    else break;
  }
  return selected ?? points[0] ?? null;
}

function percentChange(previous: number, current: number): number | null {
  return previous === 0 ? null : ((current / previous) - 1) * 100;
}

function yearsBefore(date: string, years: number): number {
  const value = new Date(`${date}T00:00:00Z`);
  value.setUTCFullYear(value.getUTCFullYear() - years);
  return value.getTime() / 1000;
}

export function parseYahooChart(payload: unknown): { asOf: string; points: MarketSeriesPoint[]; oneWeekPercent: number | null; oneYearPercent: number | null; fiveYearCagrPercent: number | null; sparkline: number[] } {
  const result = (payload as { chart?: { result?: Array<{ timestamp?: number[]; indicators?: { quote?: Array<{ close?: Array<number | null> }> } }> } })?.chart?.result?.[0];
  const timestamps = result?.timestamp ?? [];
  const closes = result?.indicators?.quote?.[0]?.close ?? [];
  const points = timestamps.map((timestamp, index) => ({ timestamp, close: closes[index] }))
    .filter((point): point is { timestamp: number; close: number } => Number.isFinite(point.timestamp) && typeof point.close === "number" && Number.isFinite(point.close))
    .map((point) => ({ date: isoDate(point.timestamp), value: point.close }));
  const latest = points.at(-1);
  if (!latest) throw new Error("Yahoo Finance returned no closing observations.");
  const latestSeconds = Date.parse(`${latest.date}T00:00:00Z`) / 1000;
  const week = valueAtOrBefore(points, latestSeconds - (7 * 86400));
  const year = valueAtOrBefore(points, yearsBefore(latest.date, 1));
  const fiveYear = valueAtOrBefore(points, yearsBefore(latest.date, 5));
  const actualYears = fiveYear ? (latestSeconds - (Date.parse(`${fiveYear.date}T00:00:00Z`) / 1000)) / (365.2425 * 86400) : 0;
  return {
    asOf: latest.date,
    points,
    oneWeekPercent: week ? percentChange(week.value, latest.value) : null,
    oneYearPercent: year ? percentChange(year.value, latest.value) : null,
    fiveYearCagrPercent: fiveYear && actualYears > 0 ? ((latest.value / fiveYear.value) ** (1 / actualYears) - 1) * 100 : null,
    sparkline: points.slice(-20).map((point) => point.value),
  };
}

async function yahooSeries(fetcher: FetchLike, symbol: string, range: "3mo" | "5y") {
  const response = await fetchOk(fetcher, `https://query2.finance.yahoo.com/v8/finance/chart/${symbol}?range=${range}&interval=1d&events=history`);
  return parseYahooChart(await response.json());
}

export function parseIsharesValuation(html: string): { peRatio: number; pbRatio: number; trailingYieldPercent: number; asOf: string } {
  const field = (key: string) => {
    const pattern = new RegExp(`&quot;${key}&quot;:\\{[\\s\\S]{0,5000}?&quot;formattedAsOfDate&quot;:&quot;([^&]+)&quot;[\\s\\S]{0,800}?&quot;formattedValue&quot;:&quot;([^&]+)&quot;`);
    const match = html.match(pattern);
    const value = Number(String(match?.[2] ?? "").replace(/[%,$]/g, ""));
    if (!match || !Number.isFinite(value)) throw new Error(`iShares omitted ${key}.`);
    return { asOf: match[1], value };
  };
  const pe = field("priceEarnings");
  const pb = field("priceBook");
  const yieldValue = field("twelveMonTrlYld");
  const parsedAsOf = new Date(pe.asOf);
  return {
    peRatio: pe.value,
    pbRatio: pb.value,
    trailingYieldPercent: yieldValue.value,
    asOf: Number.isNaN(parsedAsOf.getTime()) ? pe.asOf : parsedAsOf.toISOString().slice(0, 10),
  };
}

async function isharesValuation(fetcher: FetchLike, url: string) {
  const response = await fetchOk(fetcher, url, "text/html");
  return parseIsharesValuation(await response.text());
}

type WorldBankRow = { date?: string; value?: number | null; country?: { id?: string } };

function worldBankRows(payload: unknown): WorldBankRow[] {
  return Array.isArray(payload) && Array.isArray(payload[1]) ? payload[1] as WorldBankRow[] : [];
}

async function marketCapToGdp(fetcher: FetchLike, country: string) {
  const response = await fetchOk(fetcher, `https://api.worldbank.org/v2/country/${country}/indicator/CM.MKT.LCAP.GD.ZS?format=json&mrnev=5`);
  const latest = worldBankRows(await response.json()).find((row) => typeof row.value === "number" && Number.isFinite(row.value));
  if (!latest?.value || !latest.date) throw new Error(`World Bank omitted market capitalization for ${country}.`);
  return { ratio: 100 / latest.value, year: latest.date };
}

export function aggregateReserveRows(countryPayload: unknown, reservePayload: unknown): { value: number; change: number | null; asOf: string; sparkline: number[] } {
  type Country = { iso2Code?: string; region?: { id?: string } };
  const countries = new Set((Array.isArray(countryPayload) && Array.isArray(countryPayload[1]) ? countryPayload[1] as Country[] : [])
    .filter((country) => country.region?.id && country.region.id !== "NA" && country.iso2Code)
    .map((country) => country.iso2Code!));
  const sums = new Map<string, { value: number; observations: number }>();
  for (const row of worldBankRows(reservePayload)) {
    if (!row.date || typeof row.value !== "number" || !row.country?.id || !countries.has(row.country.id)) continue;
    const aggregate = sums.get(row.date) ?? { value: 0, observations: 0 };
    aggregate.value += row.value;
    aggregate.observations += 1;
    sums.set(row.date, aggregate);
  }
  const maximumObservations = Math.max(0, ...[...sums.values()].map((item) => item.observations));
  const complete = [...sums.entries()]
    .filter(([, item]) => item.observations >= maximumObservations * 0.9)
    .sort(([left], [right]) => left.localeCompare(right));
  const latest = complete.at(-1);
  const prior = complete.at(-2);
  if (!latest) throw new Error("World Bank returned no sufficiently complete reserve year.");
  return {
    value: latest[1].value / 1e12,
    change: prior ? percentChange(prior[1].value, latest[1].value) : null,
    asOf: latest[0],
    sparkline: complete.slice(-5).map(([, item]) => item.value / 1e12),
  };
}

async function globalReserves(fetcher: FetchLike, now: Date) {
  const endYear = now.getUTCFullYear();
  const startYear = endYear - 7;
  const [countries, reserves] = await Promise.all([
    fetchOk(fetcher, "https://api.worldbank.org/v2/country?format=json&per_page=400").then((response) => response.json()),
    fetchOk(fetcher, `https://api.worldbank.org/v2/country/all/indicator/FI.RES.TOTL.CD?format=json&per_page=5000&date=${startYear}:${endYear}`).then((response) => response.json()),
  ]);
  return aggregateReserveRows(countries, reserves);
}

function decodeHtml(text: string): string {
  return text.replace(/<[^>]+>/g, " ").replace(/&nbsp;|&#160;/gi, " ").replace(/&amp;/gi, "&").replace(/\s+/g, " ").trim();
}

function flowRow(section: string, label: "Equity" | "Bond"): number[] {
  const rows = section.match(/<tr[\s\S]*?<\/tr>/gi) ?? [];
  const row = rows.map(decodeHtml).find((text) => text.startsWith(label));
  if (!row) throw new Error(`ICI omitted the ${label.toLowerCase()} flow row.`);
  return (row.slice(label.length).match(/-?[\d,]+|\*/g) ?? []).slice(0, 5).map((value) => value === "*" ? 0 : Number(value.replace(/,/g, "")));
}

function quarterLabels(quarter: number, year: number): string[] {
  const labels: string[] = [];
  let cursor = (year * 4) + (quarter - 1) - 4;
  for (let index = 0; index < 5; index += 1) {
    const value = cursor + index;
    labels.push(`${Math.floor(value / 4)} Q${(value % 4) + 1}`);
  }
  return labels;
}

export function parseIciFlows(html: string, sourceUrl: string): { equity: number[]; bond: number[]; periods: string[] } {
  const start = html.indexOf("Net Sales of Worldwide Regulated Open-End Funds");
  if (start < 0) throw new Error("ICI worldwide flow table was not found.");
  const match = sourceUrl.match(/ww_q([1-4])_(\d{2})/i);
  if (!match) throw new Error("ICI release period could not be identified.");
  const section = html.slice(start, start + 24_000);
  return {
    equity: flowRow(section, "Equity"),
    bond: flowRow(section, "Bond"),
    periods: quarterLabels(Number(match[1]), 2000 + Number(match[2])),
  };
}

async function iciFlows(fetcher: FetchLike) {
  const listingUrl = "https://www.ici.org/research/statistics/mutual-funds/quarterly-worldwide-mutual-fund-market";
  const listing = await fetchOk(fetcher, listingUrl, "text/html").then((response) => response.text());
  const releases = [...listing.matchAll(/href="([^"]*\/statistical-report\/ww_q([1-4])_(\d{2}))[^"]*"/gi)]
    .map((match) => ({ url: new URL(match[1], "https://www.ici.org").toString(), quarter: Number(match[2]), year: 2000 + Number(match[3]) }))
    .sort((left, right) => (right.year * 4 + right.quarter) - (left.year * 4 + left.quarter));
  const release = releases[0];
  if (!release) throw new Error("ICI did not publish a worldwide-flow release link.");
  const html = await fetchOk(fetcher, release.url, "text/html").then((response) => response.text());
  return { ...parseIciFlows(html, release.url), sourceUrl: release.url };
}

export function parseFredCsv(csv: string): MarketSeriesPoint[] {
  return csv.trim().split(/\r?\n/).slice(1).map((line) => {
    const [date, rawValue] = line.split(",");
    return { date, value: rawValue?.trim() ? Number(rawValue) : Number.NaN };
  }).filter((point) => /^\d{4}-\d{2}-\d{2}$/.test(point.date) && Number.isFinite(point.value));
}

async function fredSeries(fetcher: FetchLike, id: "DGS10" | "DFEDTARU", now: Date) {
  const start = new Date(now);
  start.setUTCMonth(start.getUTCMonth() - 4);
  const csv = await fetchOk(fetcher, `https://fred.stlouisfed.org/graph/fredgraph.csv?id=${id}&cosd=${start.toISOString().slice(0, 10)}`, "text/csv").then((response) => response.text());
  const points = parseFredCsv(csv);
  const latest = points.at(-1);
  if (!latest) throw new Error(`FRED returned no ${id} observations.`);
  const target = Date.parse(`${latest.date}T00:00:00Z`) / 1000 - (7 * 86400);
  const prior = valueAtOrBefore(points, target);
  return { value: latest.value, asOf: latest.date, change: prior ? latest.value - prior.value : null, sparkline: points.slice(-20).map((point) => point.value) };
}

function warningMessage(label: string, cause: unknown): string {
  return `${label}: ${cause instanceof Error ? cause.message : "source unavailable"}`;
}

let cachedOverview: { expiresAt: number; value: MarketOverview } | null = null;

export async function getMarketOverview(fetcher: FetchLike = fetch, now = new Date()): Promise<MarketOverview> {
  if (cachedOverview && cachedOverview.expiresAt > now.getTime()) return cachedOverview.value;
  const overview = cloneFallback(now);
  const warnings: string[] = [];

  const indexTasks = INDEX_CONFIGS.map(async (config, index) => {
    const [performance, valuation, marketCap] = await Promise.allSettled([
      yahooSeries(fetcher, config.yahooSymbol, "5y"),
      isharesValuation(fetcher, config.valuationUrl),
      marketCapToGdp(fetcher, config.worldBankCountry),
    ]);
    const current = { ...overview.indices[index] };
    if (performance.status === "fulfilled") {
      current.asOf = performance.value.asOf;
      current.oneWeekPercent = performance.value.oneWeekPercent;
      current.oneYearPercent = performance.value.oneYearPercent;
      current.fiveYearCagrPercent = performance.value.fiveYearCagrPercent;
      current.sparkline = performance.value.sparkline;
    } else warnings.push(warningMessage(`${config.indexName} performance fallback`, performance.reason));
    if (valuation.status === "fulfilled") {
      current.peRatio = valuation.value.peRatio;
      current.pbRatio = valuation.value.pbRatio;
      current.trailingYieldPercent = valuation.value.trailingYieldPercent;
      current.valuationAsOf = valuation.value.asOf;
    } else warnings.push(warningMessage(`${config.country} valuation fallback`, valuation.reason));
    if (marketCap.status === "fulfilled") {
      current.gdpToMarketCapRatio = marketCap.value.ratio;
      current.marketCapAsOfYear = marketCap.value.year;
    } else warnings.push(warningMessage(`${config.country} GDP/market-cap fallback`, marketCap.reason));
    overview.indices[index] = current;
  });

  const [indexResult, reservesResult, flowsResult, vixResult, dxyResult, treasuryResult, fedFundsResult] = await Promise.allSettled([
    Promise.all(indexTasks),
    globalReserves(fetcher, now),
    iciFlows(fetcher),
    yahooSeries(fetcher, "%5EVIX", "3mo"),
    yahooSeries(fetcher, "DX-Y.NYB", "3mo"),
    fredSeries(fetcher, "DGS10", now),
    fredSeries(fetcher, "DFEDTARU", now),
  ]);
  if (indexResult.status === "rejected") warnings.push(warningMessage("Index collection fallback", indexResult.reason));

  const capital = new Map(overview.capitalIndicators.map((item) => [item.id, { ...item }]));
  if (reservesResult.status === "fulfilled") Object.assign(capital.get("reserves")!, reservesResult.value);
  else warnings.push(warningMessage("Global reserves fallback", reservesResult.reason));
  if (flowsResult.status === "fulfilled") {
    const latestPeriod = flowsResult.value.periods.at(-1)!;
    const updateFlow = (id: "equity-flows" | "bond-flows", series: number[]) => {
      const latest = series.at(-1) ?? 0;
      const prior = series.at(-2) ?? 0;
      Object.assign(capital.get(id)!, { value: latest, change: percentChange(prior, latest), asOf: latestPeriod, sparkline: series, sourceUrl: flowsResult.value.sourceUrl });
    };
    updateFlow("equity-flows", flowsResult.value.equity);
    updateFlow("bond-flows", flowsResult.value.bond);
  } else warnings.push(warningMessage("Global fund-flow fallback", flowsResult.reason));
  const updateMarketIndicator = (id: "vix" | "dxy", result: PromiseSettledResult<Awaited<ReturnType<typeof yahooSeries>>>) => {
    if (result.status === "fulfilled") Object.assign(capital.get(id)!, { value: result.value.points.at(-1)?.value ?? null, change: result.value.oneWeekPercent, asOf: result.value.asOf, sparkline: result.value.sparkline });
    else warnings.push(warningMessage(`${id.toUpperCase()} fallback`, result.reason));
  };
  updateMarketIndicator("vix", vixResult);
  updateMarketIndicator("dxy", dxyResult);
  const updateRate = (id: "treasury" | "fed-funds", result: PromiseSettledResult<Awaited<ReturnType<typeof fredSeries>>>) => {
    if (result.status === "fulfilled") Object.assign(capital.get(id)!, result.value);
    else warnings.push(warningMessage(`${id} fallback`, result.reason));
  };
  updateRate("treasury", treasuryResult);
  updateRate("fed-funds", fedFundsResult);

  overview.capitalIndicators = [...capital.values()];
  overview.warnings = warnings;
  overview.dataAsOf = [...overview.indices.map((item) => item.asOf), ...overview.capitalIndicators.map((item) => item.asOf)]
    .filter((value) => /^\d{4}-\d{2}-\d{2}$/.test(value))
    .sort()
    .at(-1) ?? overview.dataAsOf;
  cachedOverview = { expiresAt: now.getTime() + (30 * 60 * 1000), value: overview };
  return overview;
}
