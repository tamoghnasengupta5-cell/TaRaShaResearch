import { useEffect, useMemo, useState } from "react";
import type { BusinessSegmentRevenue, Company, CompanyCashConversionBridgeLine, CompanyCashConversionStory, CompanyCashConversionTrendPoint, CompanyCostStructureLine, MetricKey, YearValue } from "./types";

const storyItems = [
  { id: "makes-money", number: "01", title: "How it makes money", subtitle: "Revenue breakdown and growth" },
  { id: "money-goes", number: "02", title: "Where the money goes", subtitle: "Cost structure and operating leverage" },
  { id: "becomes-cash", number: "03", title: "What becomes cash", subtitle: "From profit to free cash flow" },
  { id: "balance-sheet", number: "04", title: "The balance sheet", subtitle: "Debt, liquidity and capital structure" },
  { id: "stock-behaves", number: "05", title: "How the stock behaves", subtitle: "Risk and volatility profile" },
  { id: "market-pricing", number: "06", title: "What the market is pricing", subtitle: "Valuation and multiples" },
  { id: "stock-got-here", number: "07", title: "How the stock got here", subtitle: "Price history and key milestones" },
  { id: "tarasha-view", number: "☆", title: "TaRaSha view", subtitle: "Our synthesis of the investment picture" },
] as const;

const segmentColors = ["#337fc9", "#58b183", "#7553a7", "#d49b3a", "#5b93a8", "#b86876", "#7e8d52"];

function latestPoint(series: YearValue[]): YearValue | null {
  return [...series].sort((left, right) => right.year - left.year)[0] ?? null;
}

function previousPoint(series: YearValue[]): YearValue | null {
  return [...series].sort((left, right) => right.year - left.year)[1] ?? null;
}

function percentGrowth(series: YearValue[]): number | null {
  const current = latestPoint(series);
  const prior = previousPoint(series);
  return current && prior && prior.value !== 0 ? ((current.value - prior.value) / Math.abs(prior.value)) * 100 : null;
}

function currencyCode(company: Company): string {
  if (company.currency.startsWith("US$")) return "USD";
  return company.currency.split(" ")[0] || "";
}

function currencySymbol(company: Company): string {
  const code = currencyCode(company);
  return code === "USD" ? "$" : code === "INR" || company.currency.startsWith("₹") ? "₹" : `${code} `;
}

function formatAmount(company: Company, value: number | null): string {
  if (value === null || !Number.isFinite(value)) return "Not available";
  const sign = value < 0 ? "−" : "";
  const absolute = Math.abs(value);
  const symbol = currencySymbol(company);
  if (absolute >= 1000) return `${sign}${symbol}${(absolute / 1000).toFixed(2)}B`;
  return `${sign}${symbol}${absolute.toLocaleString("en-US", { maximumFractionDigits: 1 })}M`;
}

function formatPercent(value: number | null, showPlus = false): string {
  if (value === null || !Number.isFinite(value)) return "—";
  const sign = value < 0 ? "−" : showPlus && value > 0 ? "+" : "";
  return `${sign}${Math.abs(value).toFixed(1)}%`;
}

function companyDisplayName(name: string): string {
  if (name !== name.toUpperCase()) return name;
  return name.toLowerCase().replace(/(^|[\s&.-])\p{L}/gu, (letter) => letter.toUpperCase());
}

function companyInitials(name: string): string {
  const words = name.split(/\s+/).filter(Boolean);
  return `${words[0]?.[0] ?? ""}${words.length > 1 ? words[words.length - 1][0] : ""}`.toUpperCase();
}

function HeaderMetric({ label, value, change, tone }: { label: string; value: string; change?: string; tone?: "positive" | "negative" }) {
  return <div className="story-headline-metric"><strong>{value}</strong><span>{label}</span>{change && <small className={tone}>{change}</small>}</div>;
}

function CompanyStoryHeader({ company, watched, toggleWatch }: { company: Company; watched: boolean; toggleWatch: (id: string) => void }) {
  const [logoLoaded, setLogoLoaded] = useState(false);
  const [logoFailed, setLogoFailed] = useState(false);
  useEffect(() => {
    setLogoLoaded(false);
    setLogoFailed(false);
  }, [company.id, company.logoUrl]);
  const revenue = latestPoint(company.metrics.revenue);
  const revenueGrowth = percentGrowth(company.metrics.revenue);
  const operatingMargin = latestPoint(company.metrics.operatingMargin);
  const freeCashFlow = latestPoint(company.metrics.freeCashFlow);
  const netDebt = latestPoint(company.metrics.netDebt);
  return <section className="company-story-header">
    <div className="story-company-identity">
      <span className={`story-company-mark ${company.logoUrl && !logoFailed ? "has-logo" : ""}`}>
        {company.logoUrl && !logoFailed && <img src={company.logoUrl} alt={`${companyDisplayName(company.name)} logo`} className={logoLoaded ? "loaded" : ""} onLoad={() => setLogoLoaded(true)} onError={() => setLogoFailed(true)} />}
        {!logoLoaded && <span aria-hidden="true">{companyInitials(company.name)}</span>}
      </span>
      <div><div className="story-company-name-row"><h1>{companyDisplayName(company.name)}</h1><span>{company.symbol}</span>{company.exchange && <i />}{company.exchange && <span>{company.exchange}</span>}</div><p>{company.sector}</p></div>
    </div>
    <button className={`story-watch-button ${watched ? "watched" : ""}`} onClick={() => toggleWatch(company.id)}><span>{watched ? "✓" : "+"}</span>{watched ? "In watchlist" : "Add to watchlist"}</button>
    <div className="story-headline-metrics">
      <HeaderMetric label={`Revenue${revenue ? ` (FY${revenue.year})` : ""}`} value={formatAmount(company, revenue?.value ?? null)} />
      <HeaderMetric label="Revenue Growth (YoY)" value={formatPercent(revenueGrowth)} change={revenueGrowth === null ? undefined : revenueGrowth >= 0 ? "Growing" : "Declining"} tone={revenueGrowth !== null && revenueGrowth < 0 ? "negative" : "positive"} />
      <HeaderMetric label="Operating Margin" value={formatPercent(operatingMargin?.value ?? null)} />
      <HeaderMetric label="Free Cash Flow" value={formatAmount(company, freeCashFlow?.value ?? null)} />
      <HeaderMetric label="Net Debt" value={formatAmount(company, netDebt?.value ?? null)} change={netDebt && netDebt.value < 0 ? "Net cash" : undefined} tone="positive" />
    </div>
    <div className="story-data-date"><span>Latest reported period</span><strong>{company.reportingPeriod}</strong><small>Updated {company.updatedAt}</small></div>
  </section>;
}

function StoryNavigation({ activeStory, onChange }: { activeStory: string; onChange: (story: string) => void }) {
  return <aside className="company-story-navigation">
    <h2>The company story</h2>
    <nav aria-label="The company story">
      {storyItems.map((item) => <button key={item.id} className={activeStory === item.id ? "active" : ""} onClick={() => onChange(item.id)} aria-current={activeStory === item.id ? "page" : undefined}>
        <span>{item.number}</span><div><strong>{item.title}</strong><small>{item.subtitle}</small></div>
      </button>)}
    </nav>
  </aside>;
}

function SegmentDonut({ company, segments, total, year }: { company: Company; segments: BusinessSegmentRevenue[]; total: number; year: number }) {
  let cumulative = 0;
  return <div className="segment-donut-wrap">
    <svg viewBox="0 0 120 120" role="img" aria-label={`Revenue share by segment for fiscal year ${year}`}>
      <circle className="segment-donut-track" cx="60" cy="60" r="43" pathLength="100" />
      {segments.map((segment, index) => {
        const percentage = segment.percentageOfTotal ?? (total ? (segment.latestRevenue / total) * 100 : 0);
        const offset = -cumulative;
        cumulative += percentage;
        return <circle key={segment.member} className="segment-donut-slice" cx="60" cy="60" r="43" pathLength="100" stroke={segmentColors[index % segmentColors.length]} strokeDasharray={`${Math.max(0, percentage)} ${Math.max(0, 100 - percentage)}`} strokeDashoffset={offset}><title>{segment.name}: {formatPercent(percentage)}</title></circle>;
      })}
    </svg>
    <div><strong>{formatAmount(company, total)}</strong><span>FY{year} Revenue</span></div>
    {segments.map((segment, index) => {
      const percentage = segment.percentageOfTotal ?? (total ? (segment.latestRevenue / total) * 100 : 0);
      const midpoint = segments.slice(0, index).reduce((sum, item) => sum + (item.percentageOfTotal ?? (item.latestRevenue / total) * 100), 0) + percentage / 2;
      const angle = ((midpoint / 100) * Math.PI * 2) - Math.PI / 2;
      return <b key={segment.member} style={{ left: `${50 + Math.cos(angle) * 35}%`, top: `${50 + Math.sin(angle) * 35}%` }}>{Math.round(percentage)}%</b>;
    })}
  </div>;
}

function MetricTone({ value }: { value: number | null }) {
  return <span className={value === null ? "neutral" : value >= 0 ? "positive" : "negative"}>{formatPercent(value)}</span>;
}

function HowItMakesMoney({ company }: { company: Company }) {
  const story = company.companyStory?.revenueSegments;
  const available = story?.status === "available" && story.segments.length > 0 && story.totalRevenue !== null && story.latestFiscalYear !== null;
  const source = story?.segments.find((segment) => segment.sourceUrl)?.sourceUrl;
  return <section className="story-chapter-page">
    <header className="story-chapter-header">
      <div><span>01</span><hgroup><h2>How it makes money</h2><p>{available ? `Revenue breakdown for FY${story.latestFiscalYear}` : "Revenue breakdown and growth"}</p></hgroup></div>
      <div className="story-period-toggle" role="group" aria-label="Reporting period"><button className="active">Annual</button><button disabled title="Quarterly story view is in progress">Quarterly</button></div>
    </header>
    {available ? <>
      <div className="revenue-story-grid">
        <SegmentDonut company={company} segments={story.segments} total={story.totalRevenue!} year={story.latestFiscalYear!} />
        <div className="segment-table-wrap"><table className="segment-table">
          <thead><tr><th>Business Segment</th><th>Revenue (FY{story.latestFiscalYear})</th><th>% of Total</th><th>YoY Growth</th><th>3Y CAGR</th></tr></thead>
          <tbody>{story.segments.map((segment, index) => <tr key={segment.member}>
            <td><i style={{ background: segmentColors[index % segmentColors.length] }} /><div><strong>{segment.name}</strong><small>Issuer-reported revenue segment</small></div></td>
            <td>{formatAmount(company, segment.latestRevenue)}</td>
            <td>{formatPercent(segment.percentageOfTotal)}</td>
            <td><MetricTone value={segment.yoyGrowthPercent} /></td>
            <td><MetricTone value={segment.threeYearCagrPercent} /></td>
          </tr>)}</tbody>
        </table>
        <div className="story-insight"><span aria-hidden="true">↗</span><p>{story.summary}</p></div>
        </div>
      </div>
      <footer className="story-methodology"><div><strong>TaRaShaData derivation</strong><p>{story.methodology}</p></div>{source && <a href={source} target="_blank" rel="noreferrer">Open source filing ↗</a>}</footer>
    </> : <div className="story-unavailable">
      <span aria-hidden="true">01</span><div><h3>Reported segment revenue is not available</h3><p>{story?.reason ?? "TaRaShaData did not return a business-segment revenue breakdown for this company."}</p><small>No estimate or substitute source has been used.</small></div>
    </div>}
  </section>;
}

function formatPerHundred(value: number | null, expense = false): string {
  if (value === null || !Number.isFinite(value)) return "—";
  if (expense && value < 0) return `($${Math.abs(value).toFixed(2)})`;
  if (expense && value > 0) return `+$${value.toFixed(2)}`;
  return value < 0 ? `−$${Math.abs(value).toFixed(2)}` : `$${value.toFixed(2)}`;
}

function compactCostLabel(line: CompanyCostStructureLine): string {
  const labels: Record<string, string> = {
    cost_of_revenue: "Cost of revenue",
    gross_profit: "Gross profit",
    research_development: "R&D",
    selling_and_marketing: "Sales & marketing",
    general_and_admin: "G&A",
    selling_general_admin: "SG&A",
    other_operating_expenses: "Other operating",
    unallocated_operating_expenses: "Other / unallocated",
    operating_income: "Operating profit",
  };
  return labels[line.key] ?? line.label;
}

function WaterfallChart({ lines, fiscalYear }: { lines: CompanyCostStructureLine[]; fiscalYear: number }) {
  const visible = lines.filter((line) => line.latestValuePerHundred !== null && (line.role !== "expense" || Math.abs(line.latestValuePerHundred) >= .005));
  let running = 0;
  const bars = visible.map((line) => {
    const value = line.latestValuePerHundred!;
    const start = line.role === "expense" ? running : 0;
    const end = line.role === "expense" ? running + value : value;
    running = end;
    return { line, value, start, end };
  });
  const chartWidth = Math.max(570, bars.length * 78);
  const chartHeight = 310;
  const plotTop = 38;
  const plotBottom = 235;
  const extrema = bars.flatMap((bar) => [bar.start, bar.end]);
  const maximum = Math.max(105, ...extrema) + 5;
  const minimum = Math.min(0, ...extrema) - (Math.min(...extrema) < 0 ? 7 : 0);
  const range = Math.max(1, maximum - minimum);
  const y = (value: number) => plotTop + ((maximum - value) / range) * (plotBottom - plotTop);
  const slot = (chartWidth - 54) / Math.max(1, bars.length);
  const barWidth = Math.min(56, slot * .58);
  return <div className="cost-waterfall-scroll" data-horizontal-scroll>
    <svg className="cost-waterfall" viewBox={`0 0 ${chartWidth} ${chartHeight}`} role="img" aria-labelledby="cost-waterfall-title cost-waterfall-description">
      <title id="cost-waterfall-title">For every $100 of revenue in fiscal year {fiscalYear}</title>
      <desc id="cost-waterfall-description">Revenue less the company&apos;s reported operating cost lines reconciles to operating profit.</desc>
      <defs>
        <linearGradient id="waterfall-gold" x1="0" x2="0" y1="0" y2="1"><stop offset="0" stopColor="#e8bd70" /><stop offset="1" stopColor="#f6dfb4" /></linearGradient>
        <linearGradient id="waterfall-red" x1="0" x2="0" y1="0" y2="1"><stop offset="0" stopColor="#dc4440" /><stop offset="1" stopColor="#ef7972" /></linearGradient>
        <linearGradient id="waterfall-green" x1="0" x2="0" y1="0" y2="1"><stop offset="0" stopColor="#3f9664" /><stop offset="1" stopColor="#7bc091" /></linearGradient>
      </defs>
      {minimum < 0 && <line className="waterfall-zero" x1="18" x2={chartWidth - 18} y1={y(0)} y2={y(0)} />}
      {bars.map((bar, index) => {
        const x = 27 + index * slot + (slot - barWidth) / 2;
        const yStart = y(bar.start);
        const yEnd = y(bar.end);
        const height = Math.max(2, Math.abs(yStart - yEnd));
        const top = Math.min(yStart, yEnd);
        const fill = bar.line.role === "expense" ? (bar.value > 0 ? "url(#waterfall-green)" : "url(#waterfall-red)") : "url(#waterfall-gold)";
        const nextX = 27 + (index + 1) * slot + (slot - barWidth) / 2;
        const labelWords = compactCostLabel(bar.line).split(" ");
        const midpoint = Math.ceil(labelWords.length / 2);
        const labelLines = labelWords.length > 2 ? [labelWords.slice(0, midpoint).join(" "), labelWords.slice(midpoint).join(" ")] : [labelWords.join(" ")];
        return <g key={bar.line.key}>
          {index < bars.length - 1 && <line className="waterfall-connector" x1={x + barWidth} x2={nextX} y1={y(bar.end)} y2={y(bar.end)} />}
          <rect className={`waterfall-bar ${bar.line.role}`} x={x} y={top} width={barWidth} height={height} fill={fill} rx="1" />
          <text className={`waterfall-value ${bar.value < 0 ? "negative" : bar.line.role === "expense" ? "positive" : ""}`} x={x + barWidth / 2} y={Math.max(17, top - 9)} textAnchor="middle">{formatPerHundred(bar.value, bar.line.role === "expense")}</text>
          <text className="waterfall-label" x={x + barWidth / 2} y={plotBottom + 28} textAnchor="middle">
            {labelLines.map((label, labelIndex) => <tspan key={label} x={x + barWidth / 2} dy={labelIndex ? 13 : 0}>{label}</tspan>)}
          </text>
        </g>;
      })}
    </svg>
  </div>;
}

function CostHistoryTable({ lines, years }: { lines: CompanyCostStructureLine[]; years: number[] }) {
  const tableLines = lines.filter((line) => line.key !== "revenue" && line.key !== "cost_of_revenue");
  return <div className="cost-history-wrap" data-horizontal-scroll><table className="cost-history-table">
    <thead><tr><th>Economics per $100 Revenue</th>{years.map((year) => <th key={year}>FY{String(year).slice(-2)}</th>)}</tr></thead>
    <tbody>{tableLines.map((line) => {
      const byYear = new Map(line.history.map((point) => [point.fiscalYear, point.valuePerHundred]));
      return <tr key={line.key} className={line.key === "operating_income" ? "operating-profit" : ""}>
        <th>{line.label}{line.key === "unallocated_operating_expenses" && <small>Reconciliation only; no category estimated</small>}</th>
        {years.map((year) => <td key={year} className={(byYear.get(year) ?? 0) > 0 && line.role === "expense" ? "credit" : ""}>{formatPerHundred(byYear.get(year) ?? null, line.role === "expense")}</td>)}
      </tr>;
    })}</tbody>
  </table></div>;
}

function WhereTheMoneyGoes({ company }: { company: Company }) {
  const story = company.companyStory?.costStructure;
  const available = story?.status === "available" && story.latestFiscalYear !== null && story.lines.some((line) => line.key === "operating_income" && line.latestValuePerHundred !== null);
  return <section className="story-chapter-page cost-story-page">
    <header className="story-chapter-header">
      <div><span>02</span><hgroup><h2>Where the money goes</h2><p>{available ? `For every $100 of revenue · FY${story.latestFiscalYear}` : "Cost structure and operating leverage"}</p></hgroup></div>
      <div className="story-period-toggle" role="group" aria-label="Reporting period"><button className="active">Annual</button><button disabled title="Quarterly story view is in progress">Quarterly</button></div>
    </header>
    {available ? <>
      <div className="cost-story-grid">
        <div className="cost-waterfall-panel"><WaterfallChart lines={story.lines} fiscalYear={story.latestFiscalYear!} /><div className="story-insight"><span aria-hidden="true">↗</span><p>{story.summary}</p></div></div>
        <CostHistoryTable lines={story.lines} years={story.years} />
      </div>
      <footer className="story-methodology"><div><strong>TaRaShaData derivation</strong><p>{story.methodology}</p></div>{story.sourceUrl && <a href={story.sourceUrl} target="_blank" rel="noreferrer">Open source filing ↗</a>}</footer>
    </> : <div className="story-unavailable">
      <span aria-hidden="true">02</span><div><h3>Operating cost structure is not available</h3><p>{story?.reason ?? "TaRaShaData did not return annual revenue and operating profit for this company."}</p><small>No expense category or substitute source has been invented.</small></div>
    </div>}
  </section>;
}

function formatBridgeAmount(company: Company, value: number, operation?: CompanyCashConversionBridgeLine["operation"]): string {
  const amount = formatAmount(company, Math.abs(value));
  if (operation === "subtract") return `(${amount})`;
  if (operation === "add") return `+${amount}`;
  return value < 0 ? `−${amount}` : amount;
}

function formatSignedAmount(company: Company, value: number): string {
  const amount = formatAmount(company, Math.abs(value));
  return value > 0 ? `+${amount}` : value < 0 ? `(${amount})` : amount;
}

function CashBridge({ company, story }: { company: Company; story: CompanyCashConversionStory }) {
  return <section className="cash-bridge-panel" aria-labelledby="cash-bridge-title">
    <header><div><h3 id="cash-bridge-title">From Operating Profit to FCFF</h3><span>FY{story.latestFiscalYear}</span></div><small>{story.displayUnit}</small></header>
    <div className="cash-bridge-steps">
      {story.bridge.map((line) => {
        const row = <div className={`cash-bridge-row ${line.operation}`}>
          <span className="cash-bridge-operation" aria-hidden="true">{line.operation === "subtract" ? "−" : line.operation === "add" ? "+" : line.operation === "subtotal" || line.operation === "total" ? "=" : "●"}</span>
          <div><strong>{line.operation === "subtract" ? "Less: " : line.operation === "add" ? "Add: " : ""}{line.label}</strong><small>{line.formula ?? (line.key === "ebit" ? "Operating income before interest and tax" : line.key === "depreciation_and_amortization" ? "Non-cash charge" : "TaRaShaData annual normalized value")}</small></div>
          <b>{formatBridgeAmount(company, line.value, line.operation)}</b>
        </div>;
        return line.components.length ? <details className="cash-working-capital" key={line.key} open>
          <summary>{row}<span className="cash-detail-caret" aria-hidden="true">⌄</span></summary>
          <div className="cash-component-list">
            {line.components.map((component) => <div key={component.key}><span>{component.label}</span><b className={component.value >= 0 ? "positive" : "negative"}>{formatSignedAmount(company, component.value)}</b></div>)}
            <div className="cash-component-total"><span>Net working-capital cash effect</span><b>{formatSignedAmount(company, line.value)}</b></div>
          </div>
        </details> : <div key={line.key}>{row}</div>;
      })}
    </div>
  </section>;
}

function chartAxisAmount(company: Company, value: number): string {
  const symbol = currencySymbol(company);
  const absolute = Math.abs(value);
  const signed = value < 0 ? "−" : "";
  return absolute >= 1000 ? `${signed}${symbol}${(absolute / 1000).toFixed(0)}B` : `${signed}${symbol}${absolute.toFixed(0)}M`;
}

function CashConversionTrend({ company, points, summary }: { company: Company; points: CompanyCashConversionTrendPoint[]; summary: string | null }) {
  const width = 760;
  const height = 310;
  const plot = { left: 58, right: 704, top: 48, bottom: 244 };
  const values = points.map((point) => point.fcff);
  const rawMin = Math.min(0, ...values);
  const rawMax = Math.max(0, ...values);
  const amountRange = Math.max(1, rawMax - rawMin);
  const amountPadding = amountRange * .16;
  const amountMin = rawMin < 0 ? rawMin - amountPadding : 0;
  const amountMax = rawMax + amountPadding;
  const yAmount = (value: number) => plot.top + ((amountMax - value) / Math.max(1, amountMax - amountMin)) * (plot.bottom - plot.top);
  const conversions = points.map((point) => point.conversionPercent).filter((value): value is number => value !== null && Number.isFinite(value));
  const conversionMin = Math.min(0, ...conversions);
  const conversionMax = Math.max(10, ...conversions) * 1.2;
  const yConversion = (value: number) => plot.top + ((conversionMax - value) / Math.max(1, conversionMax - conversionMin)) * (plot.bottom - plot.top);
  const slot = (plot.right - plot.left) / Math.max(1, points.length);
  const barWidth = Math.min(62, slot * .52);
  const linePoints = points.flatMap((point, index) => point.conversionPercent === null ? [] : [`${plot.left + slot * index + slot / 2},${yConversion(point.conversionPercent)}`]);
  const ticks = Array.from({ length: 5 }, (_, index) => amountMin + ((amountMax - amountMin) * index) / 4);
  return <section className="cash-trend-panel" aria-labelledby="cash-trend-title">
    <header><div><h3 id="cash-trend-title">Free Cash Flow to the Firm Trend</h3><div className="cash-trend-legend"><span><i />FCFF</span><span><i />FCFF / Operating Profit</span></div></div></header>
    <div className="cash-trend-scroll" data-horizontal-scroll><svg viewBox={`0 0 ${width} ${height}`} role="img" aria-labelledby="cash-trend-chart-title cash-trend-chart-description">
      <title id="cash-trend-chart-title">Annual free cash flow to the firm and conversion from operating profit</title>
      <desc id="cash-trend-chart-description">Bars show annual FCFF. The line shows FCFF divided by operating profit.</desc>
      {ticks.map((tick) => <g key={tick}><line className="cash-chart-grid" x1={plot.left} x2={plot.right} y1={yAmount(tick)} y2={yAmount(tick)} /><text className="cash-chart-axis" x={plot.left - 10} y={yAmount(tick) + 3} textAnchor="end">{chartAxisAmount(company, tick)}</text></g>)}
      <line className="cash-chart-baseline" x1={plot.left} x2={plot.right} y1={yAmount(0)} y2={yAmount(0)} />
      {points.map((point, index) => {
        const center = plot.left + slot * index + slot / 2;
        const zero = yAmount(0);
        const valueY = yAmount(point.fcff);
        return <g key={point.fiscalYear}>
          <rect className={`cash-chart-bar ${point.fcff < 0 ? "negative" : ""}`} x={center - barWidth / 2} y={Math.min(zero, valueY)} width={barWidth} height={Math.max(2, Math.abs(zero - valueY))} rx="4" />
          <text className="cash-chart-value" x={center} y={point.fcff >= 0 ? valueY - 9 : valueY + 15} textAnchor="middle">{formatAmount(company, point.fcff)}</text>
          <text className="cash-chart-year" x={center} y={plot.bottom + 28} textAnchor="middle">FY{String(point.fiscalYear).slice(-2)}</text>
        </g>;
      })}
      {linePoints.length > 1 && <polyline className="cash-conversion-line" points={linePoints.join(" ")} />}
      {points.map((point, index) => point.conversionPercent === null ? null : <g key={`conversion-${point.fiscalYear}`}>
        <circle className="cash-conversion-dot" cx={plot.left + slot * index + slot / 2} cy={yConversion(point.conversionPercent)} r="4" />
        <text className="cash-conversion-value" x={plot.left + slot * index + slot / 2} y={yConversion(point.conversionPercent) + 21} textAnchor="middle">{formatPercent(point.conversionPercent)}</text>
      </g>)}
      <text className="cash-chart-axis" x={plot.right + 15} y={plot.top + 3}>{formatPercent(conversionMax)}</text>
      <text className="cash-chart-axis" x={plot.right + 15} y={plot.bottom + 3}>{formatPercent(conversionMin)}</text>
    </svg></div>
    {summary && <div className="story-insight"><span aria-hidden="true">↗</span><p>{summary}</p></div>}
  </section>;
}

function CashMetric({ label, value, prior, formula, current, previous }: { label: string; value: string; prior: string; formula: string; current: number | null; previous: number | null }) {
  const movement = current !== null && previous !== null ? current - previous : null;
  return <div className="cash-efficiency-metric">
    <span>{label}</span><strong>{value}</strong><small>{prior}{movement !== null && movement !== 0 && <b className={movement > 0 ? "positive" : "negative"}>{movement > 0 ? "↑" : "↓"}</b>}</small><p><i>Formula</i>{formula}</p>
  </div>;
}

function CashEfficiencyMetrics({ company, story }: { company: Company; story: CompanyCashConversionStory }) {
  const metrics = story.metrics!;
  return <section className="cash-efficiency-panel" aria-labelledby="cash-efficiency-title">
    <header><h3 id="cash-efficiency-title">FCFF & Efficiency Metrics <span>(FY{story.latestFiscalYear})</span></h3></header>
    <div>
      <CashMetric label="FCFF" value={formatAmount(company, metrics.fcff)} prior={metrics.priorFiscalYear ? `vs FY${metrics.priorFiscalYear}: ${formatAmount(company, metrics.priorFcff)}` : "No prior annual value"} formula="NOPAT + D&A + WC effect − Capex" current={metrics.fcff} previous={metrics.priorFcff} />
      <CashMetric label="FCFF / Operating Profit" value={formatPercent(metrics.conversionPercent)} prior={metrics.priorFiscalYear ? `vs FY${metrics.priorFiscalYear}: ${formatPercent(metrics.priorConversionPercent)}` : "No prior annual value"} formula="FCFF / Operating Profit" current={metrics.conversionPercent} previous={metrics.priorConversionPercent} />
      <CashMetric label="FCFF Growth" value={formatPercent(metrics.growthPercent)} prior={metrics.priorGrowthPercent === null ? "Prior growth unavailable" : `prior: ${formatPercent(metrics.priorGrowthPercent)}`} formula="FCFF year-over-year growth" current={metrics.growthPercent} previous={metrics.priorGrowthPercent} />
      <CashMetric label="FCFF / Revenue" value={formatPercent(metrics.revenuePercent)} prior={metrics.priorFiscalYear ? `vs FY${metrics.priorFiscalYear}: ${formatPercent(metrics.priorRevenuePercent)}` : "No prior annual value"} formula="FCFF / Revenue" current={metrics.revenuePercent} previous={metrics.priorRevenuePercent} />
    </div>
  </section>;
}

function WhatBecomesCash({ company }: { company: Company }) {
  const story = company.companyStory?.cashConversion;
  const available = story?.status === "available" && story.latestFiscalYear !== null && story.bridge.length > 0 && story.trend.length > 0 && story.metrics !== null;
  return <section className="story-chapter-page cash-story-page">
    <header className="story-chapter-header">
      <div><span>03</span><hgroup><h2>What becomes cash</h2><p>{available ? `From Operating Profit to Free Cash Flow to the Firm · FY${story.latestFiscalYear}` : "From operating profit to free cash flow"}</p></hgroup></div>
      <div className="story-period-toggle" role="group" aria-label="Reporting period"><button className="active">Annual</button><button disabled title="Quarterly story view is in progress">Quarterly</button></div>
    </header>
    {available ? <>
      <div className="cash-story-grid">
        <CashBridge company={company} story={story} />
        <CashConversionTrend company={company} points={story.trend} summary={story.summary} />
        <CashEfficiencyMetrics company={company} story={story} />
      </div>
      {story.takeaway && <aside className="cash-takeaway"><span aria-hidden="true">☆</span><div><strong>TaRaSha Takeaway</strong><p>{story.takeaway}</p></div></aside>}
      <footer className="story-methodology"><div><strong>TaRaShaData derivation</strong><p>{story.methodology}</p></div>{story.sourceUrl && <a href={story.sourceUrl} target="_blank" rel="noreferrer">Open source filing ↗</a>}</footer>
    </> : <div className="story-unavailable">
      <span aria-hidden="true">03</span><div><h3>Operating profit to FCFF is not available</h3><p>{story?.reason ?? "TaRaShaData did not return the complete annual facts required for the FCFF bridge."}</p><small>No missing financial input or substitute source has been estimated.</small></div>
    </div>}
  </section>;
}

function WorkInProgress({ storyId, backToDiscover }: { storyId: string; backToDiscover: () => void }) {
  const item = storyItems.find((candidate) => candidate.id === storyId) ?? storyItems[1];
  return <section className="story-wip" role="status">
    <span>{item.number}</span><p>The company story</p><h2>{item.title}</h2><strong>Work in Progress</strong><small>{item.subtitle} will be built in the next story-page task.</small><button onClick={backToDiscover}>Search another company</button>
  </section>;
}

export function CompanyStoryHome({ company, watched, toggleWatch, backToDiscover }: { company: Company; watched: boolean; toggleWatch: (id: string) => void; backToDiscover: () => void }) {
  const [activeStory, setActiveStory] = useState("makes-money");
  useEffect(() => setActiveStory("makes-money"), [company.id]);
  const content = useMemo(() => activeStory === "makes-money"
    ? <HowItMakesMoney company={company} />
    : activeStory === "money-goes"
      ? <WhereTheMoneyGoes company={company} />
      : activeStory === "becomes-cash"
        ? <WhatBecomesCash company={company} />
        : <WorkInProgress storyId={activeStory} backToDiscover={backToDiscover} />, [activeStory, company, backToDiscover]);
  return <div className="company-story-page">
    <CompanyStoryHeader company={company} watched={watched} toggleWatch={toggleWatch} />
    <div className="company-story-shell"><StoryNavigation activeStory={activeStory} onChange={setActiveStory} /><div className="company-story-content">{content}</div></div>
  </div>;
}
