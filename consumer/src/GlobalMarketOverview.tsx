import { useEffect, useState } from "react";
import { fetchMarketOverview } from "./marketOverview";
import type { CapitalFlowIndicator, MarketIndexOverview, MarketOverview } from "./marketOverview";

function displayDate(value: string): string {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return value;
  return new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", year: "numeric", timeZone: "UTC" }).format(new Date(`${value}T00:00:00Z`));
}

function finite(value: number | null): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function formatPercent(value: number | null, digits = 2): string {
  if (!finite(value)) return "—";
  const sign = value > 0 ? "+" : value < 0 ? "−" : "";
  return `${sign}${Math.abs(value).toFixed(digits)}%`;
}

function tone(value: number | null, invert = false): string {
  if (!finite(value) || value === 0) return "neutral";
  const positive = invert ? value < 0 : value > 0;
  return positive ? "positive" : "negative";
}

function Sparkline({ values, direction }: { values: number[]; direction?: number | null }) {
  const usable = values.filter(Number.isFinite);
  if (usable.length < 2) return <span className="market-sparkline-empty" aria-hidden="true" />;
  const minimum = Math.min(...usable);
  const maximum = Math.max(...usable);
  const spread = maximum - minimum || 1;
  const points = usable.map((value, index) => {
    const x = (index / (usable.length - 1)) * 84;
    const y = 27 - (((value - minimum) / spread) * 22);
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");
  return <svg className={`market-sparkline ${tone(direction ?? usable.at(-1)! - usable[0])}`} viewBox="0 0 84 32" role="img" aria-label="Recent trend"><polyline points={points} /></svg>;
}

function PerformanceCell({ value, sparkline }: { value: number | null; sparkline?: number[] }) {
  return <div className={`market-performance-cell ${tone(value)}`}><strong>{formatPercent(value)}</strong>{sparkline && <Sparkline values={sparkline} direction={value} />}</div>;
}

function RatioCell({ value, suffix = "" }: { value: number | null; suffix?: string }) {
  return <span className="market-ratio-value">{finite(value) ? `${value.toFixed(2)}${suffix}` : "—"}</span>;
}

function IndexTable({ rows }: { rows: MarketIndexOverview[] }) {
  return <div className="market-index-scroll">
    <table className="market-index-table">
      <thead>
        <tr className="market-super-head">
          <th rowSpan={2}>Country / Index</th>
          <th colSpan={3}>Index performance</th>
          <th colSpan={4}>Valuation &amp; macro ratios <span>latest reported</span></th>
        </tr>
        <tr>
          <th>1 week<small>% change</small></th>
          <th>1 year<small>% change</small></th>
          <th>5 year<small>CAGR</small></th>
          <th>P/E<small>ETF proxy</small></th>
          <th>P/B<small>ETF proxy</small></th>
          <th>Yield<small>12m ETF</small></th>
          <th>GDP /<br />market cap<small>ratio</small></th>
        </tr>
      </thead>
      <tbody>
        {rows.map((row) => <tr key={row.id}>
          <td>
            <div className="market-country-cell"><span className="market-country-flag" aria-hidden="true">{row.flag}</span><div><strong>{row.country}</strong><a href={row.sourceUrl} target="_blank" rel="noreferrer">{row.indexName}</a><small>Prices through {displayDate(row.asOf)}</small></div></div>
          </td>
          <td><PerformanceCell value={row.oneWeekPercent} sparkline={row.sparkline} /></td>
          <td><PerformanceCell value={row.oneYearPercent} /></td>
          <td><PerformanceCell value={row.fiveYearCagrPercent} /></td>
          <td title={`${row.valuationProxy}, as of ${displayDate(row.valuationAsOf)}`}><RatioCell value={row.peRatio} /></td>
          <td title={`${row.valuationProxy}, as of ${displayDate(row.valuationAsOf)}`}><RatioCell value={row.pbRatio} /></td>
          <td title={`${row.valuationProxy}; trailing distribution yield`}><RatioCell value={row.trailingYieldPercent} suffix="%" /></td>
          <td title={`World Bank market capitalization of listed domestic companies, ${row.marketCapAsOfYear}`}><RatioCell value={row.gdpToMarketCapRatio} /><small className="market-ratio-period">{row.marketCapAsOfYear}</small></td>
        </tr>)}
      </tbody>
    </table>
  </div>;
}

function formatIndicatorValue(indicator: CapitalFlowIndicator): string {
  if (!finite(indicator.value)) return "—";
  if (indicator.id === "reserves") return indicator.value.toFixed(2);
  if (indicator.id === "equity-flows" || indicator.id === "bond-flows") return `${indicator.value > 0 ? "+" : indicator.value < 0 ? "−" : ""}${Math.abs(indicator.value).toFixed(0)}`;
  if (indicator.id === "treasury" || indicator.id === "fed-funds") return `${indicator.value.toFixed(2)}%`;
  return indicator.value.toFixed(2);
}

function formatIndicatorChange(indicator: CapitalFlowIndicator): string {
  if (!finite(indicator.change)) return "—";
  if (indicator.changeKind === "percentage-points") {
    const sign = indicator.change > 0 ? "+" : indicator.change < 0 ? "−" : "";
    return `${sign}${Math.abs(indicator.change).toFixed(2)} pp`;
  }
  return formatPercent(indicator.change, 1);
}

function CapitalIndicators({ indicators }: { indicators: CapitalFlowIndicator[] }) {
  return <div className="capital-indicator-list">
    {indicators.map((indicator) => <article className="capital-indicator-row" key={indicator.id}>
      <div><a href={indicator.sourceUrl} target="_blank" rel="noreferrer">{indicator.label}</a><small>{indicator.unit} · {indicator.asOf}</small></div>
      <strong>{formatIndicatorValue(indicator)}</strong>
      <span className={tone(indicator.change, indicator.id === "vix")}>{formatIndicatorChange(indicator)}</span>
      <Sparkline values={indicator.sparkline} direction={indicator.id === "vix" && finite(indicator.change) ? -indicator.change : indicator.change} />
      <small className="capital-comparison">{indicator.comparison}</small>
    </article>)}
  </div>;
}

function LoadingOverview() {
  return <section className="global-market-page market-overview-loading" aria-live="polite">
    <div className="market-page-heading"><div><span /><span /></div></div>
    <div className="market-overview-grid"><div className="market-loading-card" /><div className="market-loading-card" /></div>
  </section>;
}

export function GlobalMarketOverview() {
  const [overview, setOverview] = useState<MarketOverview | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [generation, setGeneration] = useState(0);

  useEffect(() => {
    let active = true;
    setError(null);
    fetchMarketOverview().then((data) => active && setOverview(data)).catch((cause) => active && setError(cause instanceof Error ? cause.message : "The global market overview is temporarily unavailable."));
    return () => { active = false; };
  }, [generation]);

  if (!overview && !error) return <LoadingOverview />;
  if (!overview) return <section className="global-market-page market-overview-error" aria-live="polite"><div><span>!</span><p className="eyebrow">Global market overview</p><h1>Market data could not be loaded.</h1><p>{error}</p><button className="button secondary" onClick={() => setGeneration((value) => value + 1)}>Try again</button></div></section>;

  return <section className="global-market-page">
    <header className="market-page-heading">
      <div><h1>Global Market Overview</h1><p>Track major indices, country valuation proxies, macro ratios, and international capital flows.</p></div>
      <div className="market-data-date"><span>Data through {displayDate(overview.dataAsOf)}</span><span className="market-info-dot" title="Daily, quarterly, and annual series keep their own source dates. Values do not share a single reporting period.">i</span></div>
    </header>

    <div className="market-overview-grid">
      <section className="market-overview-card market-index-card">
        <div className="market-card-heading"><div><h2>Major Indices &amp; Valuation Metrics by Country</h2><p>Returns use the named benchmark; valuation ratios use the labeled iShares country ETF proxy.</p></div><span>5 markets</span></div>
        <IndexTable rows={overview.indices} />
        <div className="market-methodology-note"><span>*</span><p><strong>GDP / market cap ratio</strong> = nominal GDP ÷ end-of-year market capitalization of listed domestic companies. The source year appears in each row.</p></div>
        <div className="market-source-row"><span>Sources:</span><a href="https://finance.yahoo.com/" target="_blank" rel="noreferrer">Yahoo Finance</a><a href="https://www.ishares.com/us" target="_blank" rel="noreferrer">iShares</a><a href="https://data.worldbank.org/indicator/CM.MKT.LCAP.GD.ZS" target="_blank" rel="noreferrer">World Bank / WFE</a></div>
      </section>

      <aside className="market-overview-card capital-flow-card">
        <div className="market-card-heading"><div><h2>International Capital Flow Indicators</h2><p>Each change is measured against the comparison shown below it.</p></div></div>
        <CapitalIndicators indicators={overview.capitalIndicators} />
        <div className="market-source-row"><span>Sources:</span><a href="https://www.ici.org/statistical-report/ww_q1_26" target="_blank" rel="noreferrer">ICI / IIFA</a><a href="https://data.worldbank.org/indicator/FI.RES.TOTL.CD" target="_blank" rel="noreferrer">World Bank / IMF</a><a href="https://fred.stlouisfed.org/" target="_blank" rel="noreferrer">Federal Reserve / FRED</a></div>
      </aside>
    </div>

    <div className="market-page-callout"><span aria-hidden="true">i</span><p><strong>Markets move. Capital flows. Valuations matter.</strong> Stay informed, stay ahead.</p><small>Zero paid data feeds · no market-data API key · source-dated fallbacks are shown if a public endpoint is temporarily unavailable.</small></div>
    {overview.warnings.length > 0 && <details className="market-source-status"><summary>Some live sources used their dated fallback</summary><ul>{overview.warnings.map((warning) => <li key={warning}>{warning}</li>)}</ul></details>}
  </section>;
}
