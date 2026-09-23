import "./revenueOfferings.css";
import type {
  AdjustedRevenuePoint,
  Company,
  RevenueAdjustmentImpact,
  RevenueOffering,
} from "./types";
import { revenueGrowth, revenuePeriods } from "./revenueOfferingMath";

const OFFERING_COLORS = ["#2f86ca", "#53b184", "#7653a8", "#d5a64c", "#6f91bd", "#4d7f91", "#b86f60", "#7e9463"];
const UNDISCLOSED_COLOR = "#cbd2d6";

function offeringLeaves(items: RevenueOffering[]): RevenueOffering[] {
  return items.flatMap(item => item.children?.length ? offeringLeaves(item.children) : [item]);
}

function currencySymbol(company: Company) {
  if (company.currency.startsWith("US$") || company.currency.startsWith("USD")) return "$";
  if (company.currency.startsWith("₹") || company.currency.startsWith("INR")) return "₹";
  return `${company.currency.split(" ")[0]} `;
}

function formatAmount(company: Company, value: number | null, digits = 2) {
  if (value === null || !Number.isFinite(value)) return "—";
  const sign = value < 0 ? "−" : "";
  const absolute = Math.abs(value);
  if (absolute >= 1000) return `${sign}${currencySymbol(company)}${(absolute / 1000).toFixed(digits)}B`;
  return `${sign}${currencySymbol(company)}${absolute.toLocaleString("en-US", { maximumFractionDigits: 1 })}M`;
}

function formatSignedAmount(company: Company, value: number | null) {
  if (value === null || !Number.isFinite(value)) return "—";
  return `${value > 0 ? "+" : value < 0 ? "−" : ""}${formatAmount(company, Math.abs(value))}`;
}

function formatPercent(value: number | null, showPlus = false) {
  if (value === null || !Number.isFinite(value)) return "—";
  return `${value < 0 ? "−" : showPlus && value > 0 ? "+" : ""}${Math.abs(value).toFixed(1)}%`;
}

function niceRevenueCeiling(value: number) {
  if (!Number.isFinite(value) || value <= 0) return 1000;
  const roughStep = value / 4;
  const magnitude = 10 ** Math.floor(Math.log10(roughStep));
  const normalized = roughStep / magnitude;
  const factor = normalized <= 1 ? 1 : normalized <= 2 ? 2 : normalized <= 5 ? 5 : 10;
  return factor * magnitude * 4;
}

function formatAxisBillions(value: number) {
  const billions = value / 1000;
  return billions < 10 ? billions.toFixed(2).replace(/\.00$/, "").replace(/0$/, "") : Math.round(billions);
}

function uniqueUrls(urls: Array<string | undefined>) {
  return [...new Set(urls.filter((url): url is string => Boolean(url)))];
}

export function RevenueSourcesDisclosure({
  methodology,
  sourceUrls,
  note,
}: {
  methodology?: string | null;
  sourceUrls: string[];
  note?: string | null;
}) {
  const urls = uniqueUrls(sourceUrls);
  return <details className="revenue-sources-disclosure">
    <summary>Sources &amp; derivations <span aria-hidden="true">↗</span></summary>
    <div>
      {methodology && <p>{methodology}</p>}
      {note && <p>{note}</p>}
      {urls.length > 0 && <nav aria-label="Source filings">{urls.map((url, index) => <a href={url} target="_blank" rel="noreferrer" key={url}>Source filing {index + 1} ↗</a>)}</nav>}
    </div>
  </details>;
}

interface ChartSegment {
  id: string;
  label: string;
  value: number;
  color: string;
  share: number;
  disclosed: boolean;
}

function compositionForPeriod(offerings: RevenueOffering[], period: string, total: number | null, colorById: Map<string, string>) {
  if (total === null || total <= 0) return { segments: [] as ChartSegment[], exact: false };
  const disclosed = offerings.flatMap(item => {
    const value = item.revenueByPeriod[period];
    return value !== null && value !== undefined && Number.isFinite(value) && value > 0
      ? [{ id: item.id, label: item.label, value, color: colorById.get(item.id) ?? OFFERING_COLORS[0], share: value / total * 100, disclosed: true }]
      : [];
  });
  const disclosedTotal = disclosed.reduce((sum, item) => sum + item.value, 0);
  const tolerance = Math.max(.01, Math.abs(total) * .000001);
  if (disclosedTotal > total + tolerance) return {
    segments: [{ id: "unsafe-composition", label: "Composition withheld: reported items do not reconcile", value: total, color: UNDISCLOSED_COLOR, share: 100, disclosed: false }],
    exact: false,
  };
  const remainder = Math.max(0, total - disclosedTotal);
  const segments = [...disclosed];
  if (remainder > tolerance) segments.push({
    id: "not-separately-disclosed",
    label: disclosed.length ? "Other / not separately disclosed" : "Composition not disclosed",
    value: remainder,
    color: UNDISCLOSED_COLOR,
    share: remainder / total * 100,
    disclosed: false,
  });
  return { segments, exact: Math.abs(disclosedTotal - total) <= tolerance };
}

function periodLabel(period: string) {
  return period === "TTM" ? "TTM" : `FY${period}`;
}

function ImpactDirection({ value }: { value: number | null }) {
  const tone = value === null || value === 0 ? "neutral" : value > 0 ? "positive" : "negative";
  return <span className={`adjustment-direction ${tone}`} aria-label={value === null ? "Not available" : value > 0 ? "Adds to adjusted revenue" : value < 0 ? "Reduces adjusted revenue" : "No impact"}>
    <b aria-hidden="true">{value === null || value === 0 ? "→" : value > 0 ? "↑" : "↓"}</b>
  </span>;
}

function AdjustmentCard({ company, period, total, impacts }: { company: Company; period: string; total: number | null; impacts: RevenueAdjustmentImpact[] }) {
  return <article className="revenue-adjustment-card">
    <h5>{periodLabel(period)}</h5>
    <ul>{impacts.length === 0 ? <li className="unavailable empty">
      <span>No source-backed timing movement</span>
      <small>Not disclosed</small>
    </li> : impacts.map(item => {
      const value = item.valueByPeriod[period] ?? null;
      const share = value !== null && total !== null && total > 0 ? value / total * 100 : null;
      const partial = item.coverageByPeriod[period]?.includes("partial");
      return <li className={value === null ? "unavailable" : value > 0 ? "positive" : value < 0 ? "negative" : "neutral"} key={item.id} title={item.derivationByPeriod[period] ?? item.definition}>
        <span>{item.label}{partial && <em>Partial</em>}</span>
        <ImpactDirection value={value} />
        <strong>{formatSignedAmount(company, value)}</strong>
        <small>{share === null ? "Not disclosed" : `${formatPercent(share, true)} of revenue`}</small>
      </li>;
    })}</ul>
  </article>;
}

export function RevenueOfferingHistory({ company }: { company: Company }) {
  const data = company.companyStory?.revenueOfferings;
  const periods = revenuePeriods(company, false);
  const annual = new Map(company.metrics.revenue.map(point => [String(point.year), point.value]));
  const total = (period: string) => period === "TTM" ? data?.ttm?.revenue ?? null : data?.annualTotals?.[period] ?? annual.get(period) ?? null;
  const growth = (period: string) => period === "TTM"
    ? revenueGrowth(total(period), data?.ttm?.priorYearRevenue ?? null)
    : revenueGrowth(total(period), total(String(Number(period) - 1)));
  const offerings = offeringLeaves(data?.offerings ?? []);
  const impacts = data?.adjustmentImpacts ?? [];
  const colorById = new Map(offerings.map((item, index) => [item.id, OFFERING_COLORS[index % OFFERING_COLORS.length]]));
  const compositions = new Map(periods.map(period => [period, compositionForPeriod(offerings, period, total(period), colorById)]));
  const showUndisclosed = [...compositions.values()].some(item => item.segments.some(segment => !segment.disclosed));
  const legend = offerings.filter(item => periods.some(period => Number.isFinite(item.revenueByPeriod[period])));
  const chartWidth = 1120;
  const chartHeight = 326;
  const margin = { top: 62, right: 18, bottom: 45, left: 58 };
  const plotWidth = chartWidth - margin.left - margin.right;
  const plotHeight = chartHeight - margin.top - margin.bottom;
  const axisMax = niceRevenueCeiling(Math.max(0, ...periods.map(period => total(period) ?? 0)));
  const y = (value: number) => margin.top + plotHeight - value / axisMax * plotHeight;
  const slotWidth = plotWidth / Math.max(1, periods.length);
  const barWidth = Math.min(94, slotWidth * .56);
  const sourceUrls = uniqueUrls([
    ...periods.flatMap(period => data?.totalSourcesByPeriod?.[period] ?? []),
    ...offerings.flatMap(item => periods.flatMap(period => item.sourcesByPeriod?.[period] ?? [])),
    ...impacts.flatMap(item => periods.flatMap(period => item.sourcesByPeriod[period] ?? [])),
  ]);

  return <section className="offering-history" aria-label="Revenue by products, services and offerings">
    <div className="offering-chart-scroll"><div className="offering-chart">
      <svg viewBox={`0 0 ${chartWidth} ${chartHeight}`} role="img" aria-labelledby="revenue-offering-title revenue-offering-description">
        <title id="revenue-offering-title">Revenue by products, services and offerings</title>
        <desc id="revenue-offering-description">Annual and available trailing-twelve-month stacked revenue bars, each reconciled to total company revenue before display.</desc>
        <defs>
          <marker id="offering-growth-arrow" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto" markerUnits="strokeWidth"><path d="M0,0 L0,6 L7,3 z" fill="context-stroke" /></marker>
          {periods.map((period, index) => {
            const x = margin.left + index * slotWidth + (slotWidth - barWidth) / 2;
            return <clipPath id={`offering-bar-${index}`} key={period}><rect x={x} y={margin.top} width={barWidth} height={plotHeight} rx="4" /></clipPath>;
          })}
        </defs>
        <text className="offering-axis-title" x="4" y="20">Revenue</text>
        <text className="offering-axis-unit" x="4" y="37">({company.currency.replace(/millions?/i, "billions")})</text>
        {Array.from({ length: 5 }, (_, index) => {
          const value = axisMax / 4 * index;
          const gridY = y(value);
          return <g key={value}><line className="offering-gridline" x1={margin.left} x2={chartWidth - margin.right} y1={gridY} y2={gridY} /><text className="offering-tick" x={margin.left - 11} y={gridY + 4}>{formatAxisBillions(value)}</text></g>;
        })}
        <line className="offering-axis" x1={margin.left} x2={margin.left} y1={margin.top} y2={margin.top + plotHeight} />
        <line className="offering-axis" x1={margin.left} x2={chartWidth - margin.right} y1={margin.top + plotHeight} y2={margin.top + plotHeight} />
        {periods.slice(1).map((period, index) => {
          // A trailing year must be compared with the same trailing period one
          // year earlier, not visually connected to the last fiscal year.
          if (period === "TTM") return null;
          const leftPeriod = periods[index];
          const value = growth(period);
          const earlierTotal = total(leftPeriod);
          const laterTotal = total(period);
          if (value === null || earlierTotal === null || laterTotal === null) return null;
          const x1 = margin.left + index * slotWidth + slotWidth / 2 + barWidth / 2 + 8;
          const x2 = margin.left + (index + 1) * slotWidth + slotWidth / 2 - barWidth / 2 - 8;
          const lineY = Math.max(39, Math.min(y(earlierTotal), y(laterTotal)) - 25);
          const tone = value < 0 ? "negative" : "positive";
          return <g className={`offering-growth-arrow ${tone}`} key={`${leftPeriod}-${period}`}><text x={(x1 + x2) / 2} y={lineY - 7}>{formatPercent(value, true)}</text><line x1={x1} x2={x2} y1={lineY} y2={lineY + (laterTotal < earlierTotal ? 4 : -4)} markerEnd="url(#offering-growth-arrow)" /></g>;
        })}
        {periods.map((period, index) => {
          const periodTotal = total(period);
          const x = margin.left + index * slotWidth + (slotWidth - barWidth) / 2;
          const composition = compositions.get(period)!;
          let cumulative = 0;
          return <g key={period}>
            {periodTotal !== null && <text className="offering-total-label" x={x + barWidth / 2} y={Math.max(57, y(periodTotal) - 9)}>{formatAmount(company, periodTotal)}</text>}
            {periodTotal !== null && composition.segments.map(segment => {
              const segmentHeight = segment.value / axisMax * plotHeight;
              const segmentY = y(cumulative + segment.value);
              cumulative += segment.value;
              return <g key={segment.id} clipPath={`url(#offering-bar-${index})`}><rect className={segment.disclosed ? "offering-segment" : "offering-segment undisclosed"} x={x} y={segmentY} width={barWidth} height={segmentHeight} fill={segment.color}><title>{`${segment.label}: ${formatAmount(company, segment.value)} (${segment.share.toFixed(1)}% of total revenue)`}</title></rect>{segmentHeight >= 23 && segment.share >= 4 && <text className={segment.disclosed ? "offering-share" : "offering-share dark"} x={x + barWidth / 2} y={segmentY + segmentHeight / 2 + 4}>{Math.round(segment.share)}%</text>}</g>;
            })}
            {periodTotal === null && <text className="offering-unavailable" x={x + barWidth / 2} y={margin.top + plotHeight - 18}>Unavailable</text>}
            <text className="offering-period-label" x={x + barWidth / 2} y={margin.top + plotHeight + 28}>{periodLabel(period)}</text>
          </g>;
        })}
      </svg>
      <div className="offering-legend" aria-label="Revenue offering legend">{legend.map(item => <span key={item.id}><i style={{ background: colorById.get(item.id) }} />{item.label}</span>)}{showUndisclosed && <span><i style={{ background: UNDISCLOSED_COLOR }} />Other / composition not separately disclosed</span>}</div>
    </div></div>
    {periods.includes("TTM") && <p className="offering-ttm-context">TTM through {data!.ttm!.periodEnd}: {formatAmount(company, data!.ttm!.revenue)} reported revenue{growth("TTM") === null ? "" : ` · ${formatPercent(growth("TTM"), true)} versus the prior comparable TTM`}.</p>}
    {(data?.qualityWarnings?.length ?? 0) > 0 && <div className="offering-quality-note"><span aria-hidden="true">ⓘ</span><p>{data!.qualityWarnings!.join(" ")} Total revenue remains displayed; an unsafe composition is not estimated.</p></div>}
    <section className="revenue-adjustments" aria-labelledby="revenue-adjustments-title">
      <header><div><h3 id="revenue-adjustments-title">Revenue-related adjustment entries</h3><p>Source-backed twelve-month movements applied to the adjusted-revenue view</p></div><div className="adjustment-impact-legend"><span className="positive">↑ <b>Adds to revenue</b></span><span className="negative">↓ <b>Reduces revenue</b></span></div></header>
      <div className="revenue-adjustment-scroll" role="region" aria-label="Revenue adjustment entries by period" tabIndex={0}><div className="revenue-adjustment-grid">{periods.map(period => <AdjustmentCard key={period} company={company} period={period} total={total(period)} impacts={impacts} />)}</div></div>
      <aside className="adjustment-next-note"><span aria-hidden="true">ⓘ</span><p>Page 3 compares reported revenue with the source-backed adjusted view after applying these signed movements.</p><strong>Next: Adjusted revenue view</strong></aside>
    </section>
    <RevenueSourcesDisclosure methodology={data?.adjustmentMethodology ?? data?.methodology} sourceUrls={sourceUrls} note={data?.reason} />
  </section>;
}

function linePath(points: Array<{ x: number; y: number }>) {
  return points.map((point, index) => `${index ? "L" : "M"}${point.x},${point.y}`).join(" ");
}

function linePathSegments(points: Array<{ x: number; y: number } | null>) {
  const paths: string[] = [];
  let segment: Array<{ x: number; y: number }> = [];
  for (const point of points) {
    if (point) {
      segment.push(point);
    } else if (segment.length) {
      paths.push(linePath(segment));
      segment = [];
    }
  }
  if (segment.length) paths.push(linePath(segment));
  return paths;
}

function latestAdjustedPoint(points: AdjustedRevenuePoint[]) {
  return points.at(-1) ?? null;
}

export function AdjustedRevenueComparison({ company }: { company: Company }) {
  const data = company.companyStory?.revenueOfferings;
  const story = data?.adjustedRevenue;
  const annualPoints = (story?.points ?? []).filter(point => point.periodType !== "ttm").sort((a, b) => a.periodEnd.localeCompare(b.periodEnd)).slice(-6);
  const latestAnnualEnd = annualPoints.at(-1)?.periodEnd ?? "";
  const trailingPoint = (story?.points ?? []).filter(point => point.periodType === "ttm" && point.periodEnd > latestAnnualEnd).sort((a, b) => a.periodEnd.localeCompare(b.periodEnd)).at(-1);
  const points = [...annualPoints, ...(trailingPoint ? [trailingPoint] : [])];
  const available = points.length > 0;
  const latest = latestAdjustedPoint(points);
  const label = (point: AdjustedRevenuePoint) => point.periodType === "ttm" ? "TTM" : `FY${point.fiscalYear}`;
  const sourceUrls = uniqueUrls(points.flatMap(point => point.sourceUrls));
  if (!available || !latest) return <section className="adjusted-revenue-empty" aria-label="Adjusted revenue unavailable">
    <div><span aria-hidden="true">ⓘ</span><h3>Adjusted revenue is not available</h3><p>{story?.reason ?? "TaRaShaData does not have paired issuer-reported opening and closing timing balances for at least two annual periods."}</p><small>No missing adjustment is estimated and reported revenue remains unchanged.</small></div>
    <RevenueSourcesDisclosure methodology={story?.methodology ?? data?.adjustmentMethodology} sourceUrls={sourceUrls} />
  </section>;

  const width = 820;
  const height = 300;
  const plot = { left: 50, right: 790, top: 34, bottom: 238 };
  const values = points.flatMap(point => [point.reportedRevenue, point.adjustedRevenue].filter((value): value is number => value !== null));
  const axisMax = niceRevenueCeiling(Math.max(...values));
  const y = (value: number) => plot.bottom - value / axisMax * (plot.bottom - plot.top);
  const slot = (plot.right - plot.left) / Math.max(1, points.length - 1);
  const x = (index: number) => plot.left + index * slot;
  const reportedPath = points.map((point, index) => ({ x: x(index), y: y(point.reportedRevenue) }));
  const adjustedPaths = linePathSegments(points.map((point, index) => (
    point.adjustedRevenue === null ? null : { x: x(index), y: y(point.adjustedRevenue) }
  )));
  const latestDeltaPercent = latest.totalAdjustment !== null && latest.reportedRevenue !== 0 ? latest.totalAdjustment / latest.reportedRevenue * 100 : null;

  return <section className="adjusted-revenue-page" aria-label="Reported versus adjusted revenue">
    <div className="adjusted-formula-note"><span aria-hidden="true">ⓘ</span><div><strong>Adjusted revenue = reported revenue + source-backed timing movements</strong><p>Only paired comparable twelve-month balances from TaRaShaData are applied. This analytical view is non-GAAP and is not a replacement for reported revenue.</p></div></div>
    <div className="adjusted-revenue-main">
      <section className="adjusted-chart-panel">
        <header><div><h3>Reported vs Adjusted Revenue</h3><p>{company.currency.replace(/millions?/i, "billions")}</p></div></header>
        <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-labelledby="adjusted-chart-title adjusted-chart-description">
          <title id="adjusted-chart-title">Reported and adjusted annual and trailing revenue</title><desc id="adjusted-chart-description">Reported revenue is compared with source-backed timing-adjusted revenue for six annual periods and an available trailing twelve months.</desc>
          {Array.from({ length: 5 }, (_, index) => { const value = axisMax / 4 * index; return <g key={value}><line className="adjusted-gridline" x1={plot.left} x2={plot.right} y1={y(value)} y2={y(value)} /><text className="adjusted-axis-label" x={plot.left - 9} y={y(value) + 3}>{formatAxisBillions(value)}</text></g>; })}
          <line className="adjusted-axis" x1={plot.left} x2={plot.left} y1={plot.top} y2={plot.bottom} /><line className="adjusted-axis" x1={plot.left} x2={plot.right} y1={plot.bottom} y2={plot.bottom} />
          <path className="adjusted-line reported" d={linePath(reportedPath)} />{adjustedPaths.map((path, index) => <path className="adjusted-line adjusted" d={path} key={index} />)}
          {points.map((point, index) => <g key={label(point)}>
            <circle className="adjusted-dot reported" cx={x(index)} cy={y(point.reportedRevenue)} r="4" /><text className="adjusted-value-label reported" x={x(index)} y={y(point.reportedRevenue) + 22}>{formatAmount(company, point.reportedRevenue, 1)}</text>
            {point.adjustedRevenue !== null && <><circle className="adjusted-dot adjusted" cx={x(index)} cy={y(point.adjustedRevenue)} r="4" /><text className="adjusted-value-label adjusted" x={x(index)} y={y(point.adjustedRevenue) - 10}>{formatAmount(company, point.adjustedRevenue, 1)}</text></>}
            <text className="adjusted-year-label" x={x(index)} y={plot.bottom + 28}>{label(point)}</text>
          </g>)}
        </svg>
        <div className="adjusted-chart-legend"><span><i className="reported" />Reported Revenue</span><span><i className="adjusted" />Adjusted Revenue (timing adjusted)</span></div>
      </section>
      <aside className="adjusted-summary-panel">
        <div className="adjusted-summary-grid">
          <article><span>Reported Revenue</span><small>{label(latest)}</small><strong>{formatAmount(company, latest.reportedRevenue)}</strong></article>
          <article><span>Adjusted Revenue</span><small>{label(latest)}</small><strong>{formatAmount(company, latest.adjustedRevenue)}</strong><b className={(latestDeltaPercent ?? 0) >= 0 ? "positive" : "negative"}>{latestDeltaPercent === null ? "—" : `${formatPercent(latestDeltaPercent, true)} vs reported`}</b></article>
          <article><span>Reported Growth</span><small>{label(latest)} (YoY)</small><strong>{formatPercent(latest.reportedGrowthPercent)}</strong></article>
          <article><span>Adjusted Growth</span><small>{label(latest)} (YoY)</small><strong>{formatPercent(latest.adjustedGrowthPercent)}</strong><b className={(latest.growthDifferencePp ?? 0) >= 0 ? "positive" : "negative"}>{latest.growthDifferencePp === null ? "—" : `${formatPercent(latest.growthDifferencePp, true).replace("%", " pp")} vs reported`}</b></article>
        </div>
        <div className="adjusted-takeaway"><span aria-hidden="true">♧</span><div><strong>A clearer view of timing</strong><p>The adjusted line isolates the reported movements TaRaShaData can substantiate. It does not estimate undisclosed balances.</p></div></div>
      </aside>
    </div>
    <section className="adjusted-comparison-table-wrap">
      <h3>Revenue and growth comparison</h3>
      <table className="adjusted-comparison-table"><thead><tr><th />{points.map(point => <th key={label(point)}>{label(point)}</th>)}</tr></thead><tbody>
        <tr><th><i className="reported" />Reported Revenue</th>{points.map(point => <td key={label(point)}>{formatAmount(company, point.reportedRevenue)}</td>)}</tr>
        <tr className="adjusted"><th><i className="adjusted" />Adjusted Revenue</th>{points.map(point => <td key={label(point)}>{formatAmount(company, point.adjustedRevenue)}</td>)}</tr>
        <tr><th>Reported YoY Growth</th>{points.map(point => <td key={label(point)}>{formatPercent(point.reportedGrowthPercent)}</td>)}</tr>
        <tr><th>Adjusted YoY Growth</th>{points.map(point => <td key={label(point)}>{formatPercent(point.adjustedGrowthPercent)}</td>)}</tr>
        <tr><th>Growth Difference (pp)</th>{points.map(point => <td className={point.growthDifferencePp === null ? "neutral" : point.growthDifferencePp >= 0 ? "positive" : "negative"} key={label(point)}>{point.growthDifferencePp === null ? "—" : formatPercent(point.growthDifferencePp, true).replace("%", "")}</td>)}</tr>
      </tbody></table>
    </section>
    <RevenueSourcesDisclosure methodology={story?.methodology} sourceUrls={sourceUrls} />
  </section>;
}
