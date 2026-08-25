import { describe, expect, it } from "vitest";
import { aggregateReserveRows, parseFredCsv, parseIciFlows, parseIsharesValuation, parseYahooChart } from "./marketOverviewProvider";

describe("market overview public-source parsers", () => {
  it("derives benchmark returns from date-stamped closes", () => {
    const day = 86400;
    const start = Date.UTC(2021, 0, 1) / 1000;
    const timestamp = Array.from({ length: 6 }, (_, index) => start + ([0, 1, 7, 365, 365 * 4, 365 * 5][index] * day));
    const result = parseYahooChart({ chart: { result: [{ timestamp, indicators: { quote: [{ close: [100, 101, 105, 120, 150, 200] }] } }] } });
    expect(result.oneWeekPercent).toBeCloseTo(33.3333, 3);
    expect(result.oneYearPercent).toBeCloseTo(33.3333, 3);
    expect(result.fiveYearCagrPercent).toBeCloseTo(14.87, 1);
  });

  it("reads iShares portfolio ratios with their source date", () => {
    const html = [
      "&quot;priceEarnings&quot;:{&quot;formattedAsOfDate&quot;:&quot;Aug 21, 2026&quot;,&quot;formattedValue&quot;:&quot;30.15&quot;}",
      "&quot;priceBook&quot;:{&quot;formattedAsOfDate&quot;:&quot;Aug 21, 2026&quot;,&quot;formattedValue&quot;:&quot;5.63&quot;}",
      "&quot;twelveMonTrlYld&quot;:{&quot;formattedAsOfDate&quot;:&quot;Jul 31, 2026&quot;,&quot;formattedValue&quot;:&quot;1.09%&quot;}",
    ].join("");
    expect(parseIsharesValuation(html)).toEqual({ peRatio: 30.15, pbRatio: 5.63, trailingYieldPercent: 1.09, asOf: "2026-08-21" });
  });

  it("sums only real-country reserve rows and skips an incomplete latest year", () => {
    const countries = [{}, [
      { iso2Code: "US", region: { id: "NAC" } },
      { iso2Code: "JP", region: { id: "EAS" } },
      { iso2Code: "1W", region: { id: "NA" } },
    ]];
    const reserves = [{}, [
      { date: "2024", value: 2e12, country: { id: "US" } },
      { date: "2024", value: 1e12, country: { id: "JP" } },
      { date: "2024", value: 99e12, country: { id: "1W" } },
      { date: "2025", value: 2.2e12, country: { id: "US" } },
    ]];
    expect(aggregateReserveRows(countries, reserves)).toMatchObject({ value: 3, asOf: "2024" });
  });

  it("reads the latest five ICI equity and bond flow observations", () => {
    const cells = (label: string, values: number[]) => `<tr><td><p>${label}</p></td>${values.map((value) => `<td><p>${value}</p></td>`).join("")}</tr>`;
    const html = `Net Sales of Worldwide Regulated Open-End Funds<table>${cells("Equity", [144, 83, -141, 378, 196])}${cells("Bond", [223, 308, 422, 385, 385])}</table>`;
    const parsed = parseIciFlows(html, "https://www.ici.org/statistical-report/ww_q1_26");
    expect(parsed.equity).toEqual([144, 83, -141, 378, 196]);
    expect(parsed.bond).toEqual([223, 308, 422, 385, 385]);
    expect(parsed.periods.at(-1)).toBe("2026 Q1");
  });

  it("drops missing FRED observations", () => {
    expect(parseFredCsv("observation_date,DGS10\n2026-08-20,4.69\n2026-08-21,4.74\n2026-08-22,\n")).toEqual([
      { date: "2026-08-20", value: 4.69 },
      { date: "2026-08-21", value: 4.74 },
    ]);
  });
});
