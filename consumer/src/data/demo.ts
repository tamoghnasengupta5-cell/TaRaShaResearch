import type { Company, CompanyBalanceSheetComponent, CompanyBalanceSheetStory, YearValue } from "../types";

const balanceYears = [2022, 2023, 2024, 2025, 2026];
const demoAssets: YearValue[] = [6200, 6550, 6900, 7350, 7800].map((value, index) => ({ year: balanceYears[index], value }));
const demoLiabilities: YearValue[] = [3600, 3700, 3800, 3940, 4050].map((value, index) => ({ year: balanceYears[index], value }));
const demoEquity: YearValue[] = [2600, 2850, 3100, 3410, 3750].map((value, index) => ({ year: balanceYears[index], value }));

function demoBalanceComponent(key: string, label: string, values: number[], base: YearValue[], children: CompanyBalanceSheetComponent["children"] = []): CompanyBalanceSheetComponent {
  const series = values.map((value, index) => ({ year: balanceYears[index], value }));
  const latest = series[series.length - 1];
  const latestBase = base[base.length - 1];
  return {
    key,
    label,
    value: latest.value,
    percentageOfBase: (latest.value / Math.abs(latestBase.value)) * 100,
    trend: series.map((point, index) => ({ fiscalYear: point.year, value: point.value, percentageOfBase: (point.value / Math.abs(base[index].value)) * 100 })),
    children,
  };
}

const demoBalanceSheet: CompanyBalanceSheetStory = {
  status: "available",
  reason: null,
  latestFiscalYear: 2026,
  latestPeriodEnd: "2026-03-31",
  reportingCurrency: "INR",
  displayUnit: "INR crore",
  years: balanceYears,
  equation: { assets: 7800, liabilities: 4050, shareholdersEquity: 3750 },
  assets: [
    demoBalanceComponent("cash", "Cash & cash equivalents", [500, 620, 560, 800, 950], demoAssets),
    demoBalanceComponent("operating_current_assets", "Receivables, inventory & prepaids", [1700, 1760, 1900, 2020, 2140], demoAssets, [
      { key: "receivables", label: "Receivables", value: 980, percentageOfBase: 12.6, interestRatePercent: null },
      { key: "inventory", label: "Inventory", value: 910, percentageOfBase: 11.7, interestRatePercent: null },
      { key: "prepaids", label: "Prepaid expenses", value: 250, percentageOfBase: 3.2, interestRatePercent: null },
    ]),
    demoBalanceComponent("property_plant_equipment", "Property, plant & equipment", [2100, 2210, 2360, 2480, 2570], demoAssets),
    demoBalanceComponent("goodwill", "Goodwill", [420, 420, 470, 470, 470], demoAssets),
    demoBalanceComponent("other_intangible_assets", "Other intangible assets", [310, 290, 275, 250, 225], demoAssets),
  ],
  liabilities: [
    demoBalanceComponent("unearned_revenue", "Unearned revenue", [170, 185, 210, 235, 260], demoLiabilities, [
      { key: "current", label: "Current", value: 220, percentageOfBase: 5.4, interestRatePercent: null },
      { key: "long", label: "Long term", value: 40, percentageOfBase: 1, interestRatePercent: null },
    ]),
    demoBalanceComponent("borrowings", "Borrowings", [1200, 1140, 1060, 970, 880], demoLiabilities, [
      { key: "short", label: "Short-term borrowings", value: 120, percentageOfBase: 3, interestRatePercent: 6.2 },
      { key: "long", label: "Long-term borrowings", value: 650, percentageOfBase: 16, interestRatePercent: 7.1 },
      { key: "short-lease", label: "Short-term leases", value: 35, percentageOfBase: .9, interestRatePercent: 6.8 },
      { key: "long-lease", label: "Long-term leases", value: 75, percentageOfBase: 1.9, interestRatePercent: 6.8 },
    ]),
    demoBalanceComponent("accounts_payable", "Accounts payable", [760, 790, 825, 850, 870], demoLiabilities),
  ],
  shareholdersEquity: [
    demoBalanceComponent("common_stock", "Common stock", [125, 125, 125, 125, 125], demoEquity),
    demoBalanceComponent("additional_paid_in_capital", "Additional paid-in capital", [680, 720, 760, 790, 830], demoEquity),
    demoBalanceComponent("retained_earnings", "Retained earnings", [1760, 2000, 2250, 2530, 2850], demoEquity),
    demoBalanceComponent("comprehensive_income", "Comprehensive income", [35, 5, -35, -35, -55], demoEquity),
  ],
  health: { totalCash: 950, totalDebt: 880, netCashDebt: 70, netDebtToEbitda: -.05, interestCoverage: 12.4, weightedAverageCostOfDebt: 6.9, weightedAverageCostOfDebtFiscalYear: 2025, weightedAverageCostOfDebtNote: "FY2025 interest expense ÷ average borrowings" },
  summary: "Cash now exceeds borrowings, leverage has fallen for five consecutive years, and operating profit covers interest expense comfortably.",
  methodology: "Illustrative annual preview data arranged using the TaRaSha balance-sheet contract. The health label screens net cash or debt, leverage and interest cover and is not a recommendation. Live mode uses normalized issuer filings and never estimates missing facts.",
  sourceUrl: null,
};

export const companies: Company[] = [
  {
    id: "aarohan-consumer",
    name: "Aarohan Consumer",
    symbol: "AAROHAN",
    sector: "Consumer staples",
    description: "Makes packaged foods and household essentials sold through stores across India.",
    founded: 1988,
    employees: "8,400",
    currency: "₹ crore",
    reportingPeriod: "FY 2026",
    updatedAt: "30 June 2026",
    metrics: {
      revenue: [{ year: 2022, value: 4200 }, { year: 2023, value: 4580 }, { year: 2024, value: 4920 }, { year: 2025, value: 5410 }, { year: 2026, value: 5780 }],
      operatingMargin: [{ year: 2022, value: 12.2 }, { year: 2023, value: 12.8 }, { year: 2024, value: 13.1 }, { year: 2025, value: 13.7 }, { year: 2026, value: 14.0 }],
      freeCashFlow: [{ year: 2022, value: 280 }, { year: 2023, value: 330 }, { year: 2024, value: 305 }, { year: 2025, value: 410 }, { year: 2026, value: 438 }],
      netDebt: [{ year: 2022, value: 760 }, { year: 2023, value: 690 }, { year: 2024, value: 610 }, { year: 2025, value: 505 }, { year: 2026, value: 410 }],
    },
    companyStory: { balanceSheet: demoBalanceSheet },
    notes: {
      growth: "Sales have increased in each of the last five reported years.",
      profitability: "The company kept a little more operating profit from every ₹100 of sales this year.",
      cash: "Cash left after day-to-day operations and investments improved, with one softer year in FY 2024.",
      debt: "Borrowings after cash have declined over the period shown.",
    },
  },
  {
    id: "nirmaan-tech",
    name: "Nirmaan Technology",
    symbol: "NIRMAAN",
    sector: "Technology services",
    description: "Builds cloud software and manages digital systems for mid-sized businesses.",
    founded: 2006,
    employees: "5,200",
    currency: "₹ crore",
    reportingPeriod: "FY 2026",
    updatedAt: "25 June 2026",
    metrics: {
      revenue: [{ year: 2022, value: 1850 }, { year: 2023, value: 2210 }, { year: 2024, value: 2680 }, { year: 2025, value: 3150 }, { year: 2026, value: 3590 }],
      operatingMargin: [{ year: 2022, value: 18.1 }, { year: 2023, value: 18.8 }, { year: 2024, value: 19.4 }, { year: 2025, value: 18.9 }, { year: 2026, value: 19.2 }],
      freeCashFlow: [{ year: 2022, value: 245 }, { year: 2023, value: 310 }, { year: 2024, value: 382 }, { year: 2025, value: 398 }, { year: 2026, value: 476 }],
      netDebt: [{ year: 2022, value: -180 }, { year: 2023, value: -260 }, { year: 2024, value: -345 }, { year: 2025, value: -390 }, { year: 2026, value: -520 }],
    },
    notes: {
      growth: "Reported sales grew at a double-digit pace in each year shown.",
      profitability: "Operating margin has stayed in a relatively narrow range despite faster sales growth.",
      cash: "Free cash flow rose alongside the business over the period shown.",
      debt: "A negative net-debt figure means the company reports more cash than borrowings.",
    },
  },
  {
    id: "suryodaya-health",
    name: "Suryodaya Healthcare",
    symbol: "SURYAH",
    sector: "Healthcare",
    description: "Operates hospitals and diagnostic centres in large and emerging Indian cities.",
    founded: 1999,
    employees: "12,600",
    currency: "₹ crore",
    reportingPeriod: "FY 2026",
    updatedAt: "28 June 2026",
    metrics: {
      revenue: [{ year: 2022, value: 3100 }, { year: 2023, value: 3440 }, { year: 2024, value: 3810 }, { year: 2025, value: 4290 }, { year: 2026, value: 4810 }],
      operatingMargin: [{ year: 2022, value: 15.0 }, { year: 2023, value: 15.8 }, { year: 2024, value: 16.1 }, { year: 2025, value: 16.9 }, { year: 2026, value: 17.3 }],
      freeCashFlow: [{ year: 2022, value: 120 }, { year: 2023, value: 155 }, { year: 2024, value: 138 }, { year: 2025, value: 205 }, { year: 2026, value: 240 }],
      netDebt: [{ year: 2022, value: 1420 }, { year: 2023, value: 1360 }, { year: 2024, value: 1490 }, { year: 2025, value: 1380 }, { year: 2026, value: 1210 }],
    },
    notes: {
      growth: "Revenue increased as the hospital and diagnostics network expanded.",
      profitability: "Operating margin improved gradually across the five years shown.",
      cash: "Free cash flow was positive each year, though expansion spending made it uneven.",
      debt: "Net debt remains meaningful, but finished below its level five years ago.",
    },
  },
  {
    id: "jaladhara-logistics",
    name: "Jaladhara Logistics",
    symbol: "JALLOG",
    sector: "Logistics",
    description: "Moves consumer and industrial goods using warehouses, road transport and coastal shipping.",
    founded: 1994,
    employees: "7,100",
    currency: "₹ crore",
    reportingPeriod: "FY 2026",
    updatedAt: "27 June 2026",
    metrics: {
      revenue: [{ year: 2022, value: 6650 }, { year: 2023, value: 7020 }, { year: 2024, value: 6890 }, { year: 2025, value: 7420 }, { year: 2026, value: 7810 }],
      operatingMargin: [{ year: 2022, value: 9.4 }, { year: 2023, value: 9.1 }, { year: 2024, value: 8.5 }, { year: 2025, value: 9.0 }, { year: 2026, value: 9.3 }],
      freeCashFlow: [{ year: 2022, value: 310 }, { year: 2023, value: 275 }, { year: 2024, value: 190 }, { year: 2025, value: 260 }, { year: 2026, value: 295 }],
      netDebt: [{ year: 2022, value: 1880 }, { year: 2023, value: 2010 }, { year: 2024, value: 2150 }, { year: 2025, value: 2080 }, { year: 2026, value: 1960 }],
    },
    notes: {
      growth: "Revenue recovered after a small decline in FY 2024.",
      profitability: "Operating margin has moved within roughly one percentage point over five years.",
      cash: "Free cash flow remained positive but has not yet returned to its FY 2022 level.",
      debt: "Net debt rose during expansion and then declined over the last two years.",
    },
  },
  {
    id: "prithvi-materials",
    name: "Prithvi Materials",
    symbol: "PRITHVI",
    sector: "Building materials",
    description: "Produces cement, ready-mix concrete and construction materials for infrastructure projects.",
    founded: 1978,
    employees: "9,900",
    currency: "₹ crore",
    reportingPeriod: "FY 2026",
    updatedAt: "24 June 2026",
    metrics: {
      revenue: [{ year: 2022, value: 9100 }, { year: 2023, value: 10300 }, { year: 2024, value: 11250 }, { year: 2025, value: 10880 }, { year: 2026, value: 11840 }],
      operatingMargin: [{ year: 2022, value: 16.8 }, { year: 2023, value: 17.4 }, { year: 2024, value: 15.9 }, { year: 2025, value: 14.7 }, { year: 2026, value: 16.2 }],
      freeCashFlow: [{ year: 2022, value: 720 }, { year: 2023, value: 830 }, { year: 2024, value: 540 }, { year: 2025, value: 410 }, { year: 2026, value: 680 }],
      netDebt: [{ year: 2022, value: 3220 }, { year: 2023, value: 3050 }, { year: 2024, value: 3380 }, { year: 2025, value: 3710 }, { year: 2026, value: 3440 }],
    },
    notes: {
      growth: "Revenue has grown overall, with a decline during FY 2025.",
      profitability: "Margins recovered this year but remain below the strongest year shown.",
      cash: "Free cash flow improved after two years affected by heavier investment.",
      debt: "Net debt is above its FY 2022 level after capacity expansion.",
    },
  },
  {
    id: "neerja-finance",
    name: "Neerja Finance",
    symbol: "NEERJA",
    sector: "Financial services",
    description: "Provides secured loans to small businesses and self-employed customers.",
    founded: 2011,
    employees: "4,700",
    currency: "₹ crore",
    reportingPeriod: "FY 2026",
    updatedAt: "29 June 2026",
    metrics: {
      revenue: [{ year: 2022, value: 1520 }, { year: 2023, value: 1810 }, { year: 2024, value: 2180 }, { year: 2025, value: 2520 }, { year: 2026, value: 2910 }],
      operatingMargin: [{ year: 2022, value: 24.1 }, { year: 2023, value: 24.8 }, { year: 2024, value: 25.5 }, { year: 2025, value: 25.0 }, { year: 2026, value: 25.7 }],
      freeCashFlow: [{ year: 2022, value: 110 }, { year: 2023, value: 128 }, { year: 2024, value: 150 }, { year: 2025, value: 172 }, { year: 2026, value: 198 }],
      netDebt: [{ year: 2022, value: 6100 }, { year: 2023, value: 7350 }, { year: 2024, value: 8920 }, { year: 2025, value: 10500 }, { year: 2026, value: 12180 }],
    },
    notes: {
      growth: "Reported income increased as the loan book expanded.",
      profitability: "Operating margin remained broadly stable across the period shown.",
      cash: "The simplified demo cash measure rose in each year shown.",
      debt: "Borrowing is a core input for lenders, so debt should not be compared directly with non-financial companies.",
    },
  },
];

export const metricMeta = {
  revenue: { label: "Revenue", short: "Sales", unit: "₹ cr", explanation: "Money earned from selling goods or services before expenses." },
  operatingMargin: { label: "Operating margin", short: "Profitability", unit: "%", explanation: "Operating profit retained from every ₹100 of revenue." },
  freeCashFlow: { label: "Free cash flow", short: "Cash", unit: "₹ cr", explanation: "Cash left after running the business and investing to maintain or grow it." },
  netDebt: { label: "Net debt", short: "Debt", unit: "₹ cr", explanation: "Borrowings minus cash. A negative number means cash is greater than borrowings." },
} as const;

export const learningCards = [
  { id: "revenue", time: "4 min", title: "Revenue is the starting line", text: "Learn what sales growth can—and cannot—tell you about a business." },
  { id: "margin", time: "5 min", title: "Where did every ₹100 go?", text: "Use operating margin to see how much is left after the costs of running a company." },
  { id: "cash", time: "6 min", title: "Profit is not the same as cash", text: "Understand why a profitable company can still struggle to generate cash." },
  { id: "debt", time: "5 min", title: "Debt needs context", text: "See why debt can fund growth, create pressure, or mean something different for a lender." },
];
