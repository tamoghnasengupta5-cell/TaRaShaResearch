import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { costStructureStory } from "../functions/dataProvider";
import { UsWhereTheMoneyGoes } from "./CompanyStoryHome";
import type { Company } from "./types";

const source = { cik: "0000000001", accession: "2025-10-K", filed_date: "2026-02-01",
  concept: "RevenueFromContractWithCustomerExcludingAssessedTax", unit: "USD",
  json_source_url: "https://data.sec.gov/api/xbrl/companyfacts/CIK0000000001.json" };
const periods = Array.from({ length: 7 }, (_, index) => ({ key: `FY${2019+index}`, label: `FY${2019+index}`,
  type: "annual" as const, fiscal_year: 2019+index, period_end: `${2019+index}-12-31` }));
const reported = (value: string) => ({ value, display_value: value, status: "reported" as const,
  display_policy: "reported_value", percent_of_operating_income: "10.00", inputs: [source] });
const payload = {
  schema_version: 2, status: "available" as const, reason: null,
  latest_fiscal_year: 2025, latest_period_end: "2025-12-31", reporting_currency: "USD",
  years: periods.map(period => period.fiscal_year), lines: [], summary: null,
  methodology: "SEC JSON canonical financial data; separate medians.", source_url: "https://www.sec.gov/Archives/test",
  median: { status: "available" as const, reason: null,
    periods: [{ key: "TTM", label: "TTM to 2026-06-30", type: "ttm" as const, fiscal_year: null, period_end: "2026-06-30" }, ...periods.slice(1,6).reverse()],
    lines: [
      { key: "revenue", label: "Revenue", role: "revenue" as const, value_per_hundred: "100.00", observation_count: 6, reason: null, inputs: [source] },
      { key: "operating_income", label: "Operating Profit", role: "subtotal" as const, value_per_hundred: "40.00", observation_count: 6, reason: null, inputs: [source] },
    ],
  },
  annual_breakdown: { display_unit: "USD millions", scale: "1000000", periods,
    operating_margin_percent: Object.fromEntries(periods.map(period => [String(period.fiscal_year), "40.00"])),
    lines: [{ key: "revenue", label: "Revenue", role: "revenue" as const,
      values: Object.fromEntries(periods.map(period => [String(period.fiscal_year), { value: "2000000000", inputs: [source] }])) }],
  },
  interest_comparison: periods.map(period => ({ fiscal_year: period.fiscal_year, period_end: period.period_end,
    operating_income: reported("800000000"), interest_income: reported("80000000"),
    interest_expense: { value: null, display_value: "0", status: "not_reported" as const,
      display_policy: "owner_requested_zero_for_missing_expense", percent_of_operating_income: "0.00", inputs: [] },
  })),
};
function company(): Company {
  return { id: "json-fixture", name: "US SEC fixture", symbol: "SEC", country: "USA", currency: "US$ millions",
    sector: "Technology", description: "Fixture", reportingPeriod: "FY2025", updatedAt: "9 October 2026",
    metrics: { revenue: [], operatingMargin: [], freeCashFlow: [], netDebt: [] },
    notes: { growth: "", profitability: "", cash: "", debt: "" },
    companyStory: { costStructure: costStructureStory(payload, "USD") } };
}
describe("US Where the money goes story", () => {
  it("transports the upstream ratios and provenance without recalculating financial values", () => {
    const story = costStructureStory(payload, "USD");
    expect(story.median?.lines[1].valuePerHundred).toBe(40);
    expect(story.annualBreakdown?.lines[0].values["2025"].value).toBe(2000);
    expect(story.annualBreakdown?.lines[0].values["2025"].inputs).toEqual([source]);
    expect(story.interestComparison?.[0].interestExpense.value).toBeNull();
    expect(story.interestComparison?.[0].interestExpense.displayValue).toBeNull();
    expect(story.interestComparison?.[0].interestExpense.status).toBe("not_reported");
  });
  it("page 1 contains only the comparison set and median revenue representation", () => {
    const html = renderToStaticMarkup(<UsWhereTheMoneyGoes company={company()} />);
    expect(html).toContain("1 of 3");
    expect(html).toContain("TTM to 2026-06-30");
    expect(html).toContain("FY2020");
    expect(html).toContain("$40.00");
    expect(html).not.toContain("<table");
    expect(html).not.toContain("cost-interest-panel");
    expect(html).toContain('disabled="" aria-label="Previous page"');
  });
  it.each([36, -5])("connects the final independent median to operating profit of %s", (profit) => {
    const value = company();
    const median = value.companyStory!.costStructure!.median!;
    const base = median.lines[0];
    median.lines = [
      { ...base, key: "revenue", role: "revenue", valuePerHundred: 100 },
      { ...base, key: "cost_of_revenue", role: "expense", valuePerHundred: -40 },
      { ...base, key: "gross_profit", role: "subtotal", valuePerHundred: 63 },
      { ...base, key: "research_development", role: "expense", valuePerHundred: -15 },
      { ...base, key: "selling_general_admin", role: "expense", valuePerHundred: -10 },
      { ...base, key: "operating_income", role: "subtotal", valuePerHundred: profit },
    ];
    const html = renderToStaticMarkup(<UsWhereTheMoneyGoes company={value} />);
    expect(html.match(/class="waterfall-connector"/g)).toHaveLength(median.lines.length - 1);
    const connector = html.match(/<polyline[^>]*data-to-line="operating_income"[^>]*points="([^"]+)"/)![1];
    const points = connector.split(" ").map(point => point.split(",").map(Number));
    const attributes = (key: string) => Object.fromEntries(
      [...html.match(new RegExp(`<rect[^>]*data-line-key="${key}"[^>]*`))![0].matchAll(/\s(x|y|width|height)="([^"]+)"/g)]
        .map(([, name, number]) => [name, Number(number)])
    );
    const prior = attributes("selling_general_admin"), final = attributes("operating_income");
    expect(points[0][0]).toBeCloseTo(prior.x + prior.width);
    expect(points[0][1]).toBeCloseTo(prior.y + prior.height);
    expect(points.at(-1)![0]).toBeCloseTo(final.x);
    expect(points.at(-1)![1]).toBeCloseTo(final.y + (profit < 0 ? final.height : 0));
    expect(html).toContain(profit < 0 ? "−$5.00" : "$36.00");
  });
  it("page 2 shows all seven annual columns and actual values", () => {
    const html = renderToStaticMarkup(<UsWhereTheMoneyGoes company={company()} initialPage={2} />);
    expect(html).toContain("2 of 3");
    for (const period of periods) expect(html).toContain(period.label);
    expect(html).toContain("Figures in $B");
    expect(html).toContain("$2.00");
    expect(html).toContain("Operating margin");
    expect(html).not.toContain("cost-waterfall");
  });
  it("page 3 shows exactly three independent bars per year with supplied interest percentages", () => {
    const html = renderToStaticMarkup(<UsWhereTheMoneyGoes company={company()} initialPage={3} />);
    expect(html.match(/data-series="operatingIncome"/g)).toHaveLength(7);
    expect(html.match(/data-series="interestExpense"/g)).toHaveLength(7);
    expect(html.match(/data-series="interestIncome"/g)).toHaveLength(7);
    expect(html.match(/\(10\.0%\)/g)).toHaveLength(7);
    expect(html).not.toContain("(0.0%)");
    expect(html.match(/>N\/R</g)).toHaveLength(7);
    expect(html).toContain('data-source-status="not_reported"');
    expect(html).toContain('disabled="" aria-label="Next page"');
    expect(html).not.toContain("cost-waterfall");
  });
  it("keeps loss bars and undefined ratios readable", () => {
    const value = company();
    value.companyStory!.costStructure!.interestComparison![0].operatingIncome.displayValue = -200;
    value.companyStory!.costStructure!.interestComparison![0].interestExpense.percentOfOperatingIncome = null;
    const html = renderToStaticMarkup(<UsWhereTheMoneyGoes company={value} initialPage={3} />);
    expect(html).toContain("loss");
    expect(html).toContain("(—)");
    expect(html).not.toContain("NaN");
    expect(html).not.toContain("Infinity");
  });
  it("transports the separate signed net interest/other series and supplied ratios without replacing pure interest", () => {
    const next = {
      ...payload,
      interest_comparison_note: "Broader SEC-filed line; income positive and expense negative.",
      interest_comparison: payload.interest_comparison.map(point => ({ ...point,
        net_interest_other_income_expense: { ...reported("-284300000"), percent_of_operating_income: "-9.60" },
      })),
    };
    const value = company();
    const story = costStructureStory(next, "USD");
    value.companyStory!.costStructure = story;
    expect(story.interestComparison?.[0].netInterestOtherIncomeExpense?.displayValue).toBe(-284.3);
    expect(story.interestComparison?.[0].netInterestOtherIncomeExpense?.percentOfOperatingIncome).toBe(-9.6);
    expect(story.interestComparison?.[0].netInterestOtherIncomeExpense?.inputs).toEqual([source]);
    expect(story.interestComparison?.[0].interestExpense.displayValue).toBeNull();
    const html = renderToStaticMarkup(<UsWhereTheMoneyGoes company={value} initialPage={3} />);
    expect(html.match(/data-series="netInterestOtherIncomeExpense"/g)).toHaveLength(7);
    expect(html.match(/data-series="interestExpense"/g)).toHaveLength(7);
    expect(html.match(/>N\/R</g)).toHaveLength(7);
    expect(html).toContain("Net interest &amp; other income/(expense)");
    expect(html).toContain("−$284.30");
    expect(html).toContain("−9.6%");
    expect(html).toContain(next.interest_comparison_note);
  });
});

describe("API native-currency cost periods", () => {
  function nativePayload() {
    const result = structuredClone(payload);
    return { ...result, reporting_currency: "TWD", annual_breakdown: { ...result.annual_breakdown,
      display_unit: "Native reporting currency millions", periods: result.annual_breakdown.periods.map((period, index) => ({ ...period, reporting_currency: index ? "TWD" : "USD" })) },
      median: { ...result.median, periods: result.median.periods.map(period => ({ ...period, reporting_currency: "TWD" })) },
      interest_comparison: result.interest_comparison.map((period, index) => ({ ...period, reporting_currency: index ? "TWD" : "USD" })) };
  }
  it("retains explicit currency on annual, median and interest descriptors without changing values or source inputs", () => {
    const story = costStructureStory(nativePayload(), "USD");
    expect(story.annualBreakdown?.periods[0].reportingCurrency).toBe("USD");
    expect(story.annualBreakdown?.periods[1].reportingCurrency).toBe("TWD");
    expect(story.median?.periods[0].reportingCurrency).toBe("TWD");
    expect(story.interestComparison?.[1].reportingCurrency).toBe("TWD");
    expect(story.annualBreakdown?.lines[0].values["2020"].value).toBe(2000);
    expect(story.annualBreakdown?.lines[0].values["2020"].inputs).toEqual([source]);
  });
  it("labels every annual amount with its API currency and uses a table for a mixed-currency interest history", () => {
    const value = company();
    value.companyStory!.costStructure = costStructureStory(nativePayload(), "USD");
    const annual = renderToStaticMarkup(<UsWhereTheMoneyGoes company={value} initialPage={2} />);
    expect(annual).toContain("Figures in native currency B");
    expect(annual).toContain("FY2019<small>USD</small>");
    expect(annual).toContain("FY2020<small>TWD</small>");
    expect(annual).toContain("$2.00");
    expect(annual).toContain("TWD 2.00");
    const interest = renderToStaticMarkup(<UsWhereTheMoneyGoes company={value} initialPage={3} />);
    expect(interest).toContain("Figures in native currency millions");
    expect(interest).toContain("TWD 800.00");
    expect(interest).toContain("$800.00");
    expect(interest).not.toContain('class="cost-interest-grid"');
    expect(interest).toContain("10.0% of operating income");
  });
  it("keeps the single-currency chart and labels it using the API currency even if the company fallback differs", () => {
    const next = nativePayload();
    next.interest_comparison = next.interest_comparison.map(point => ({ ...point, reporting_currency: "TWD" }));
    const value = company();
    value.companyStory!.costStructure = costStructureStory(next, "USD");
    const interest = renderToStaticMarkup(<UsWhereTheMoneyGoes company={value} initialPage={3} />);
    expect(interest).toContain("Millions of TWD ");
    expect(interest).toContain("TWD 800.00");
    expect(interest).not.toContain("$800.00");
    expect(interest).toContain('class="cost-interest-grid"');
  });
});


describe("annual plus TTM interest comparison", () => {
  it("passes the distinct TTM period and its own inputs through to all four series", () => {
    const trailing = { key: "TTM", label: "TTM to 2026-06-30", type: "ttm" as const,
      fiscal_year: null, period_end: "2026-06-30", reporting_currency: "USD",
      operating_income: reported("2791900000"), interest_income: { ...reported("344800000"), percent_of_operating_income: "12.35" },
      interest_expense: { value: null, display_value: null, status: "not_reported" as const,
        display_policy: "unavailable", percent_of_operating_income: null, inputs: [] },
      net_interest_other_income_expense: { ...reported("69600000"), percent_of_operating_income: "2.49" } };
    const value = company();
    const story = costStructureStory({ ...payload, interest_comparison: [...payload.interest_comparison, trailing] }, "USD");
    value.companyStory!.costStructure = story;
    expect(story.interestComparison).toHaveLength(8);
    expect(story.interestComparison?.at(-1)).toMatchObject({ key: "TTM", type: "ttm", fiscalYear: null,
      periodEnd: "2026-06-30", interestIncome: { displayValue: 344.8, percentOfOperatingIncome: 12.35, inputs: [source] } });
    const html = renderToStaticMarkup(<UsWhereTheMoneyGoes company={value} initialPage={3} />);
    for (const series of ["operatingIncome", "interestExpense", "interestIncome", "netInterestOtherIncomeExpense"])
      expect(html.match(new RegExp(`data-series="${series}"`, "g"))).toHaveLength(8);
    expect(html.match(/data-period-key="TTM"/g)).toHaveLength(1);
    expect(html).toContain('data-period-type="ttm"');
    expect(html).toContain("TTM to 2026-06-30");
    expect(html).toContain(">TTM</text>");
    expect(html).toContain(">to 2026-06-30</text>");
    expect(html).toContain("(12.3%)");
    expect(html).not.toContain("FYnull");
    expect(html).not.toContain("FY2026");
    expect(html).not.toContain("FY2018");
  });
});
