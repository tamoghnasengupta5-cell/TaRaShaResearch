import { useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import { MaximizableStorySection } from "./MaximizableStorySection";
import { StockHistoryChapter } from "./StockHistoryChapter";
import { AdjustedRevenueComparison, RevenueOfferingHistory, RevenueSourcesDisclosure } from "./RevenueOfferingHistory";
import { hasNewerRevenueTtm, latestAnnualRevenueEnd, revenueGrowth, revenuePeriods, threePeriodRevenueCagr } from "./revenueOfferingMath";
import type {
  BusinessSegmentRevenue,
  Company,
  CompanyBalanceSheetComponent,
  CompanyBalanceSheetStory,
  CompanyBalanceSheetTrendPoint,
  CompanyCashConversionBridgeLine,
  CompanyCashConversionStory,
  CompanyCashConversionTrendPoint,
  CompanyCostStructureLine,
  CompanyMarketPricingHistoryMetric,
  CompanyMarketPricingMetric,
  CompanyMarketPricingStory,
  CompanyStockRiskPeriod,
  CompanyStockRiskPoint,
  CompanyStockRiskSeries,
  CompanyStockRiskStory,
  MetricKey,
  StockRiskRange,
  YearValue,
} from "./types";

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

function pointForYear(series: YearValue[], year: number | null): YearValue | null {
  return year === null ? null : series.find(point => point.year === year) ?? null;
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

function formatRatePercent(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return "—";
  return `${value < 0 ? "−" : ""}${Math.abs(value).toFixed(2)}%`;
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
  const reportingYear = revenue?.year ?? null;
  const trailing = company.companyStory?.revenueOfferings?.ttm;
  const recentTtm = trailing && hasNewerRevenueTtm(company) ? trailing : null;
  const revenueGrowth = recentTtm
    ? recentTtm.priorYearRevenue && recentTtm.priorYearRevenue > 0
      ? (recentTtm.revenue / recentTtm.priorYearRevenue - 1) * 100 : null
    : percentGrowth(company.metrics.revenue);
  const operatingMargin = pointForYear(company.metrics.operatingMargin, reportingYear);
  const freeCashFlow = pointForYear(company.metrics.freeCashFlow, reportingYear);
  const netDebt = pointForYear(company.metrics.netDebt, reportingYear);
  const yearSuffix = reportingYear === null ? "" : ` (FY${reportingYear})`;
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
      <HeaderMetric label={recentTtm ? `Revenue (TTM to ${recentTtm.periodEnd})` : `Revenue${revenue ? ` (FY${revenue.year})` : ""}`} value={formatAmount(company, recentTtm?.revenue ?? revenue?.value ?? null)} />
      <HeaderMetric label={`Revenue Growth (YoY)${recentTtm ? " (TTM)" : yearSuffix}`} value={formatPercent(revenueGrowth)} change={revenueGrowth === null ? undefined : revenueGrowth >= 0 ? "Growing" : "Declining"} tone={revenueGrowth !== null && revenueGrowth < 0 ? "negative" : "positive"} />
      <HeaderMetric label={`Operating Margin${yearSuffix}`} value={formatPercent(operatingMargin?.value ?? null)} />
      <HeaderMetric label={`Free Cash Flow${yearSuffix}`} value={formatAmount(company, freeCashFlow?.value ?? null)} />
      <HeaderMetric label={`Net Debt${yearSuffix}`} value={formatAmount(company, netDebt?.value ?? null)} change={netDebt && netDebt.value < 0 ? "Net cash" : undefined} tone="positive" />
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

function SegmentDonut({ company, segments, total, period }: { company: Company; segments: BusinessSegmentRevenue[]; total: number; period: string }) {
  let cumulative = 0;
  return <div className="segment-donut-wrap">
    <svg viewBox="0 0 120 120" role="img" aria-label={`Revenue share by category for ${period}`}>
      <circle className="segment-donut-track" cx="60" cy="60" r="43" pathLength="100" />
      {segments.map((segment, index) => {
        const percentage = segment.percentageOfTotal ?? (total ? (segment.latestRevenue / total) * 100 : 0);
        const offset = -cumulative;
        cumulative += percentage;
        const color = segment.member === "__undetermined__" ? "#c7ced2" : segmentColors[index % segmentColors.length];
        return <circle key={segment.member} className="segment-donut-slice" cx="60" cy="60" r="43" pathLength="100" stroke={color} strokeDasharray={`${Math.max(0, percentage)} ${Math.max(0, 100 - percentage)}`} strokeDashoffset={offset}><title>{`${segment.name}: ${formatPercent(percentage)}`}</title></circle>;
      })}
    </svg>
    <div><strong>{formatAmount(company, total)}</strong><span>{period} Revenue</span></div>
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

export function HowItMakesMoney({ company }: { company: Company }) {
  const [storyPage, setStoryPage] = useState(1);
  useEffect(() => setStoryPage(1), [company.id]);
  const story = company.companyStory?.revenueSegments;
  const offeringStory = company.companyStory?.revenueOfferings;
  const latestRevenue = latestPoint(company.metrics.revenue);
  const fiscalYear = story?.latestFiscalYear ?? latestRevenue?.year ?? null;
  const trailing = offeringStory?.ttm;
  const annualEnd = latestAnnualRevenueEnd(company) ?? "";
  const productAxis = offeringStory?.offeringAxis;
  const offeringLeaves = (offeringStory?.offerings ?? []).flatMap(item => item.children?.length ? item.children : [item]);
  const trailingOfferings = trailing && trailing.periodEnd > annualEnd
    ? offeringLeaves.filter(item => (item.revenueByPeriod.TTM ?? 0) > 0)
    : [];
  const [priorComparablePeriod, priorToPriorComparablePeriod] = revenuePeriods(company, true, false);
  const reconciledOfferings = trailing && trailingOfferings.length >= 1 && Math.abs(trailingOfferings.reduce((sum, item) => sum + (item.revenueByPeriod.TTM ?? 0), 0) - trailing.revenue) <= Math.max(.01, trailing.revenue * .000001);
  const isOfferingView = Boolean(reconciledOfferings);
  const isProductServiceView = Boolean(isOfferingView && productAxis && /product|service/i.test(productAxis) && trailingOfferings.every(item => item.id.startsWith(`${productAxis}/`)));
  const totalRevenue = isOfferingView ? trailing!.revenue : story?.totalRevenue ?? latestRevenue?.value ?? null;
  const period = isOfferingView ? "TTM" : fiscalYear === null ? "" : `FY${fiscalYear}`;
  const determined = isOfferingView || (story?.status === "available" && story.segments.length > 0);
  const segments: BusinessSegmentRevenue[] = isOfferingView
    ? trailingOfferings.map(item => {
        const currentRevenue = item.revenueByPeriod.TTM!;
        const priorComparableRevenue = item.revenueByPeriod[priorComparablePeriod] ?? null;
        const priorToPriorComparableRevenue = item.revenueByPeriod[priorToPriorComparablePeriod] ?? null;
        return {
          member: item.id,
          name: item.label,
          latestRevenue: currentRevenue,
          percentageOfTotal: currentRevenue / trailing!.revenue * 100,
          yoyGrowthPercent: revenueGrowth(currentRevenue, priorComparableRevenue),
          threeYearCagrPercent: threePeriodRevenueCagr(currentRevenue, priorToPriorComparableRevenue),
          history: [],
          accession: null,
          filedDate: null,
          sourceUrl: item.sourceByPeriod?.TTM ?? null,
        };
      })
    : story?.segments.length
    ? story.segments
    : totalRevenue !== null && fiscalYear !== null
      ? [{
        member: "__undetermined__",
        name: "Undetermined business segment",
        latestRevenue: totalRevenue,
        percentageOfTotal: 100,
        yoyGrowthPercent: percentGrowth(company.metrics.revenue),
        threeYearCagrPercent: null,
        history: [],
        accession: null,
        filedDate: null,
        sourceUrl: company.companyStory?.revenueOfferings?.totalSourceByPeriod?.[String(fiscalYear)] ?? null,
      }]
      : [];
  const hasRevenueView = totalRevenue !== null && (fiscalYear !== null || isOfferingView) && segments.length > 0;
  const offeringComparisonSourceUrls = isOfferingView
    ? trailingOfferings.flatMap(item => ["TTM", priorComparablePeriod, priorToPriorComparablePeriod].flatMap(comparisonPeriod => (
        item.sourcesByPeriod?.[comparisonPeriod]
        ?? (item.sourceByPeriod?.[comparisonPeriod] ? [item.sourceByPeriod[comparisonPeriod]] : [])
      )))
    : [];
  const sourceUrls = [...new Set([
    ...segments.flatMap(segment => segment.sourceUrl ? [segment.sourceUrl] : []),
    ...offeringComparisonSourceUrls,
    ...(fiscalYear === null ? [] : company.companyStory?.revenueOfferings?.totalSourcesByPeriod?.[String(fiscalYear)] ?? []),
    ...(isOfferingView ? offeringStory?.totalSourcesByPeriod?.TTM ?? [] : []),
  ])];
  const subtitles = [
    period ? `Revenue breakdown for ${period}` : "Revenue breakdown and growth",
    "Revenue by products, services & offerings",
    "Reported vs adjusted revenue",
  ];
  const goToPage = (page: number) => setStoryPage(Math.min(3, Math.max(1, page)));
  return <section className="story-chapter-page">
    <header className="story-chapter-header">
      <div><span>01</span><hgroup><h2>How it makes money</h2><p>{subtitles[storyPage - 1]}</p></hgroup></div>
      <div className="story-chapter-actions">
        <nav className="story-page-navigation" aria-label="How it makes money pages">
          <button type="button" onClick={() => goToPage(storyPage - 1)} disabled={storyPage === 1} aria-label="Previous page"><span aria-hidden="true">‹</span> Previous</button>
          <strong aria-live="polite">{storyPage} of 3</strong>
          <button type="button" onClick={() => goToPage(storyPage + 1)} disabled={storyPage === 3} aria-label="Next page">Next <span aria-hidden="true">›</span></button>
        </nav>
        <div className="story-period-toggle" role="group" aria-label="Reporting period"><button className="active">{trailing && trailing.periodEnd > annualEnd ? "Annual + TTM" : "Annual"}</button><button disabled title="Quarterly story view is in progress">Quarterly</button></div>
      </div>
    </header>
    <div className="story-internal-page" key={storyPage} data-story-page={storyPage}>
    {storyPage === 1 && (hasRevenueView ? <>
      <div className={`revenue-story-grid ${determined ? "" : "undetermined"}`}>
        <SegmentDonut company={company} segments={segments} total={totalRevenue!} period={period} />
        <div className="segment-table-wrap"><table className="segment-table">
          <thead><tr><th>{isOfferingView ? isProductServiceView ? "Product / Service" : "Reported Revenue Category" : "Business Segment"}</th><th>Revenue ({period})</th><th>% of Total</th><th>YoY Growth</th><th>3Y CAGR</th></tr></thead>
          <tbody>{segments.map((segment, index) => <tr key={segment.member}>
            <td><i style={{ background: segment.member === "__undetermined__" ? "#a9b1b6" : segmentColors[index % segmentColors.length] }} /><div><strong>{segment.name}</strong><small>{segment.member === "__undetermined__" ? "Revenue detail not separately disclosed" : isOfferingView ? isProductServiceView ? "Issuer-disclosed product/service category" : "Issuer-disclosed revenue category" : "Issuer-reported revenue segment"}</small></div></td>
            <td>{formatAmount(company, segment.latestRevenue)}</td>
            <td>{formatPercent(segment.percentageOfTotal)}</td>
            <td><MetricTone value={segment.yoyGrowthPercent} /></td>
            <td><MetricTone value={segment.threeYearCagrPercent} /></td>
          </tr>)}</tbody>
        </table>
        <div className="story-insight"><span aria-hidden="true">{determined ? "↗" : "ⓘ"}</span><p>{isOfferingView ? `The TTM ${isProductServiceView ? "product/service" : "issuer-reported revenue"} categories reconcile to consolidated revenue through ${trailing!.periodEnd}. Growth uses TTM, the prior comparable, and the prior-to-prior comparable values; unavailable comparisons are not estimated. These categories are not necessarily reportable operating segments.` : determined ? story?.summary : "Revenue segment detail is not separately disclosed in the available issuer facts. The chart and row therefore classify 100% of consolidated revenue as undetermined; no segment split is estimated."}</p></div>
        </div>
      </div>
      <RevenueSourcesDisclosure methodology={isOfferingView ? offeringStory?.methodology : story?.methodology ?? "Consolidated revenue comes from TaRaShaData normalized issuer filings. No unavailable segment is estimated."} sourceUrls={sourceUrls} note={!determined ? story?.reason : isOfferingView ? story?.reason : null} />
    </> : <div className="revenue-availability"><div><strong>{formatAmount(company, null)}</strong><span>Total company revenue</span></div><div><h3>Revenue is not available</h3><p>TaRaShaData did not return a source-backed annual revenue fact for this company.</p></div></div>)}
    {storyPage === 2 && <RevenueOfferingHistory company={company} />}
    {storyPage === 3 && <AdjustedRevenueComparison company={company} />}
    </div>
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

export function WaterfallChart({ lines, fiscalYear }: { lines: CompanyCostStructureLine[]; fiscalYear: number }) {
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
  const zeroY = y(0);
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
        <linearGradient id="waterfall-slate" x1="0" x2="0" y1="0" y2="1"><stop offset="0" stopColor="#66758a" /><stop offset="1" stopColor="#8793a2" /></linearGradient>
        <linearGradient id="waterfall-bronze" x1="0" x2="0" y1="0" y2="1"><stop offset="0" stopColor="#9a6635" /><stop offset="1" stopColor="#704522" /></linearGradient>
        <linearGradient id="waterfall-underwater" x1="0" x2="0" y1="0" y2="1"><stop offset="0" stopColor="#f2f7fb" /><stop offset="1" stopColor="#eaf2f8" /></linearGradient>
      </defs>
      <rect className="waterfall-negative-zone" x="18" y={zeroY} width={chartWidth - 36} height={plotBottom + 22 - zeroY} fill="url(#waterfall-underwater)" />
      {bars.map((bar, index) => {
        const x = 27 + index * slot + (slot - barWidth) / 2;
        const low = Math.min(bar.start, bar.end);
        const high = Math.max(bar.start, bar.end);
        const segments: { region: "positive" | "negative"; low: number; high: number }[] = [];
        if (high > 0) segments.push({ region: "positive", low: Math.max(0, low), high });
        if (low < 0) segments.push({ region: "negative", low, high: Math.min(0, high) });
        if (!segments.length) segments.push({ region: "positive", low: 0, high: 0 });
        const top = y(high);
        const nextX = 27 + (index + 1) * slot + (slot - barWidth) / 2;
        const labelWords = compactCostLabel(bar.line).split(" ");
        const midpoint = Math.ceil(labelWords.length / 2);
        const labelLines = labelWords.length > 2 ? [labelWords.slice(0, midpoint).join(" "), labelWords.slice(midpoint).join(" ")] : [labelWords.join(" ")];
        return <g key={bar.line.key}>
          {index < bars.length - 1 && <line className="waterfall-connector" x1={x + barWidth} x2={nextX} y1={y(bar.end)} y2={y(bar.end)} />}
          {segments.map((segment) => {
            const fill = segment.region === "negative"
              ? bar.line.role === "expense" ? "url(#waterfall-slate)" : "url(#waterfall-bronze)"
              : bar.line.role === "expense" ? (bar.value > 0 ? "url(#waterfall-green)" : "url(#waterfall-red)") : "url(#waterfall-gold)";
            return <rect
              key={segment.region}
              className={`waterfall-bar ${bar.line.role} ${segment.region}`}
              data-line-key={bar.line.key}
              data-region={segment.region}
              x={x}
              y={y(segment.high)}
              width={barWidth}
              height={Math.max(2, y(segment.low) - y(segment.high))}
              fill={fill}
              rx="1"
            />;
          })}
          <text className={`waterfall-value ${bar.value < 0 ? "negative" : bar.line.role === "expense" ? "positive" : ""}`} x={x + barWidth / 2} y={Math.max(17, top - 9)} textAnchor="middle">{formatPerHundred(bar.value, bar.line.role === "expense")}</text>
          <text className="waterfall-label" x={x + barWidth / 2} y={plotBottom + 28} textAnchor="middle">
            {labelLines.map((label, labelIndex) => <tspan key={label} x={x + barWidth / 2} dy={labelIndex ? 13 : 0}>{label}</tspan>)}
          </text>
        </g>;
      })}
      <line className="waterfall-zero" x1="18" x2={chartWidth - 18} y1={zeroY} y2={zeroY} />
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

function formatMultiple(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return "—";
  return `${value < 0 ? "−" : ""}${Math.abs(value).toFixed(1)}x`;
}

function trendMovement(points: CompanyBalanceSheetTrendPoint[]): { text: string; tone: "up" | "down" | "flat" } | null {
  const percentages = points.filter((point) => point.percentageOfBase !== null).sort((left, right) => left.fiscalYear - right.fiscalYear);
  if (percentages.length < 2) return null;
  const first = percentages[0];
  const last = percentages[percentages.length - 1];
  const movement = last.percentageOfBase! - first.percentageOfBase!;
  if (Math.abs(movement) < .05) return { text: `Flat vs FY${String(first.fiscalYear).slice(-2)}`, tone: "flat" };
  return {
    text: `${movement > 0 ? "↑" : "↓"} ${Math.abs(movement).toFixed(1)} pts vs FY${String(first.fiscalYear).slice(-2)}`,
    tone: movement > 0 ? "up" : "down",
  };
}

type BalanceSectionTone = "assets" | "liabilities" | "equity";

const balanceComponentExplanations: Record<string, string> = {
  cash: "Cash and investments that can usually be accessed quickly to pay bills or fund the business.",
  operating_current_assets: "Near-term resources such as customer IOUs, goods waiting to be sold, and prepaid costs.",
  property_plant_equipment: "Long-lived physical assets—such as buildings, machinery, and equipment—used to run the business.",
  goodwill: "The premium paid in acquisitions for benefits such as a brand, customer relationships, or expected synergies.",
  other_intangible_assets: "Non-physical assets such as patents, software, licenses, and acquired customer relationships.",
  unearned_revenue: "Cash received from customers before the company has delivered the promised product or service.",
  borrowings: "Loans, bonds, and lease obligations that the company is expected to repay over time.",
  accounts_payable: "Bills owed to suppliers for goods or services the company has already received.",
  common_stock: "The accounting value assigned to issued shares. It is not the company’s stock-market value.",
  additional_paid_in_capital: "The amount investors paid for shares above their stated accounting value.",
  retained_earnings: "Profits kept in the business over time, after dividends and accumulated losses.",
  comprehensive_income: "Certain gains and losses recorded outside net income, such as currency or pension adjustments.",
};

function balanceComponentExplanation(component: CompanyBalanceSheetComponent, tone: BalanceSectionTone): string {
  if (component.key === "common_stock" && component.label.includes("APIC")) {
    return "The recorded value of issued shares plus the amount investors paid above that value. It is not the company’s stock-market value.";
  }
  return balanceComponentExplanations[component.key] ?? (tone === "assets"
    ? "A reported resource the company owns or expects to turn into future economic value."
    : tone === "liabilities"
      ? "A reported obligation the company is expected to settle with cash, goods, or services."
      : "A reported component of the value remaining for shareholders after liabilities.");
}

function BalanceComponentIcon({ componentKey }: { componentKey: string }) {
  const svgProps = { viewBox: "0 0 24 24", "aria-hidden": true } as const;
  if (componentKey === "cash") return <svg {...svgProps}><path d="M4 7.5h14a2 2 0 0 1 2 2V18H4a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h12" /><path d="M15 11h5v4h-5a2 2 0 0 1 0-4Z" /></svg>;
  if (componentKey === "operating_current_assets") return <svg {...svgProps}><path d="M4 8h16v11H4z" /><path d="M8 8V5h8v3M8 13h8M12 10v6" /></svg>;
  if (componentKey === "property_plant_equipment") return <svg {...svgProps}><path d="M4 20V8l8-4v16M12 10h8v10M7 10h2M7 14h2M15 13h2M15 17h2" /></svg>;
  if (componentKey === "goodwill") return <svg {...svgProps}><path d="M12 20s-7-4.3-7-10a4 4 0 0 1 7-2.6A4 4 0 0 1 19 10c0 5.7-7 10-7 10Z" /><path d="m9.5 12 1.7 1.7 3.5-4" /></svg>;
  if (componentKey === "other_intangible_assets") return <svg {...svgProps}><path d="m12 3 7 7-7 11-7-11 7-7Z" /><path d="m5 10 7 3 7-3M12 13v8" /></svg>;
  if (componentKey === "unearned_revenue") return <svg {...svgProps}><path d="M6 3h12M6 21h12M7 3c0 5 3 6 5 8-2 2-5 3-5 10M17 3c0 5-3 6-5 8 2 2 5 3 5 10" /></svg>;
  if (componentKey === "borrowings") return <svg {...svgProps}><path d="m3 9 9-5 9 5M5 10h14M6 10v8M10 10v8M14 10v8M18 10v8M4 19h16" /></svg>;
  if (componentKey === "accounts_payable") return <svg {...svgProps}><path d="M6 3h12v18l-3-2-3 2-3-2-3 2V3Z" /><path d="M9 8h6M9 12h6M9 16h3" /></svg>;
  if (componentKey === "common_stock") return <svg {...svgProps}><rect x="3" y="5" width="18" height="14" rx="2" /><path d="M7 9h7M7 13h5M17 12a2 2 0 1 0 0 4 2 2 0 0 0 0-4Z" /></svg>;
  if (componentKey === "additional_paid_in_capital") return <svg {...svgProps}><ellipse cx="9" cy="7" rx="5" ry="2.5" /><path d="M4 7v4c0 1.4 2.2 2.5 5 2.5M14 7v3M9 13.5v3c0 1.4-2.2 2.5-5 2.5s-5-1.1-5-2.5v-4M15 12h6M18 9v6" /></svg>;
  if (componentKey === "retained_earnings") return <svg {...svgProps}><path d="M5 10h14l-1 10H6L5 10Z" /><path d="M8 10V7a4 4 0 0 1 8 0v3M12 4V2M9.5 2h5" /></svg>;
  if (componentKey === "comprehensive_income") return <svg {...svgProps}><circle cx="12" cy="12" r="8" /><path d="M4 12h16M12 4c2.2 2.3 3.2 5 3.2 8S14.2 17.7 12 20c-2.2-2.3-3.2-5-3.2-8S9.8 6.3 12 4ZM17.5 5.5l2-2M19.5 3.5v3M19.5 3.5h-3" /></svg>;
  return <svg {...svgProps}><path d="m12 3 7 7-7 11-7-11 7-7Z" /><path d="m5 10 7 3 7-3" /></svg>;
}

function BalanceBarChart({ company, points, label, tone }: { company: Company; points: CompanyBalanceSheetTrendPoint[]; label: string; tone: BalanceSectionTone }) {
  const usable = points.filter((point) => Number.isFinite(point.value)).sort((left, right) => left.fiscalYear - right.fiscalYear);
  if (usable.length < 2) return <div className="balance-trend-unavailable"><span aria-hidden="true">—</span><strong>Five-year trend unavailable</strong><p>Comparable annual history was not separately reported.</p></div>;
  const width = 500;
  const height = 166;
  const plotTop = 28;
  const plotBottom = 120;
  const min = Math.min(0, ...usable.map((point) => point.value));
  const max = Math.max(0, ...usable.map((point) => point.value));
  const range = max - min || 1;
  const y = (value: number) => plotTop + ((max - value) / range) * (plotBottom - plotTop);
  const baseline = y(0);
  const groupWidth = width / usable.length;
  const barWidth = Math.min(46, groupWidth * .48);
  const aria = usable.map((point) => `FY${point.fiscalYear} ${formatAmount(company, point.value)}`).join(", ");
  return <div className="balance-component-chart">
    <div><span>Five-year trend</span><span>Reported value</span></div>
    <svg className={`balance-bar-chart ${tone}`} viewBox={`0 0 ${width} ${height}`} role="img" aria-label={`${label}: ${aria}`}>
      <line className="balance-bar-grid" x1="8" x2={width - 8} y1={(plotTop + plotBottom) / 2} y2={(plotTop + plotBottom) / 2} />
      <line className="balance-bar-zero" x1="8" x2={width - 8} y1={baseline} y2={baseline} />
      {usable.map((point, index) => {
        const center = groupWidth * index + groupWidth / 2;
        const valueY = y(point.value);
        const rectY = Math.min(valueY, baseline);
        const rectHeight = Math.max(2, Math.abs(baseline - valueY));
        const labelY = point.value >= 0 ? Math.max(12, rectY - 7) : Math.min(139, rectY + rectHeight + 12);
        const latest = index === usable.length - 1;
        return <g key={point.fiscalYear}>
          <rect className={`balance-bar ${latest ? "latest" : ""}`} x={center - barWidth / 2} y={rectY} width={barWidth} height={rectHeight} rx="4"><title>{`FY${point.fiscalYear}: ${formatAmount(company, point.value)}`}</title></rect>
          <text className="balance-bar-value" x={center} y={labelY} textAnchor="middle">{formatAmount(company, point.value)}</text>
          <text className="balance-bar-year" x={center} y="155" textAnchor="middle">FY{String(point.fiscalYear).slice(-2)}</text>
        </g>;
      })}
    </svg>
  </div>;
}

function BalanceComponentCard({ company, component, tone, shareLabel, latestFiscalYear }: { company: Company; component: CompanyBalanceSheetComponent; tone: BalanceSectionTone; shareLabel: string; latestFiscalYear: number }) {
  const movement = trendMovement(component.trend);
  const available = component.value !== null;
  return <article className={`balance-component-card ${tone} ${available ? "" : "unavailable"}`}>
    <header>
      <div className="balance-component-card-copy">
        <h4>{component.label}</h4>
        <p>{available ? `${formatPercent(component.percentageOfBase)} of ${shareLabel}` : "Not separately reported"}</p>
        {movement && <small className={movement.tone}>{movement.text}</small>}
      </div>
      <div className="balance-component-latest"><strong>{available ? formatAmount(company, component.value) : "—"}</strong><small>{available ? `Latest · FY${String(latestFiscalYear).slice(-2)}` : "Latest value unavailable"}</small></div>
    </header>
    <div className="balance-component-explainer">
      <span className="balance-component-icon"><BalanceComponentIcon componentKey={component.key} /></span>
      <div><strong>What this means</strong><p>{balanceComponentExplanation(component, tone)}</p></div>
    </div>
    {available ? <BalanceBarChart company={company} points={component.trend} label={component.label} tone={tone} /> : <div className="balance-component-unavailable"><span aria-hidden="true">—</span><p>No trend is shown because TaRaSha does not infer missing balances.</p></div>}
    {component.children.length > 0 && <details className="balance-component-details">
      <summary>View latest reported breakdown</summary>
      <div className="balance-component-children">
        {component.children.map((child) => <div key={child.key} className={child.value === null ? "unavailable" : ""}>
          <span>{child.label}</span><strong>{formatAmount(company, child.value)}</strong>
          <small>{child.percentageOfBase === null ? "Not reported" : `${formatPercent(child.percentageOfBase)} of ${shareLabel}`}{child.interestRatePercent === null ? "" : ` · ${formatRatePercent(child.interestRatePercent)} rate`}</small>
        </div>)}
      </div>
    </details>}
  </article>;
}

function BalanceSection({ company, title, description, components, tone, shareLabel, latestFiscalYear, active }: { company: Company; title: string; description: string; components: CompanyBalanceSheetComponent[]; tone: BalanceSectionTone; shareLabel: string; latestFiscalYear: number; active: boolean }) {
  return <section className={`balance-section-panel ${tone}`} id={`balance-${tone}-panel`} role="tabpanel" aria-labelledby={`balance-${tone}-tab`} hidden={!active}>
    <header className="balance-section-intro"><div><h3>{title}</h3><p>{description}</p></div><span>{components.length} components</span></header>
    <div className="balance-component-cards">{components.map((component) => <BalanceComponentCard key={component.key} company={company} component={component} tone={tone} shareLabel={shareLabel} latestFiscalYear={latestFiscalYear} />)}</div>
  </section>;
}

function balanceHealth(story: CompanyBalanceSheetStory): { label: string; tone: "strong" | "steady" | "watch" | "limited" } {
  const { netCashDebt, netDebtToEbitda, interestCoverage } = story.health;
  const inputs = [netCashDebt, netDebtToEbitda, interestCoverage].filter((value) => value !== null).length;
  if (inputs < 2) return { label: "Limited data", tone: "limited" };
  let score = netCashDebt !== null && netCashDebt >= 0 ? 2 : 0;
  if (netDebtToEbitda !== null) score += netDebtToEbitda < 0 ? 2 : netDebtToEbitda <= 1 ? 2 : netDebtToEbitda <= 2.5 ? 1 : -1;
  if (interestCoverage !== null) score += interestCoverage >= 5 ? 2 : interestCoverage >= 2 ? 1 : -2;
  if (score >= 5) return { label: "Resilient", tone: "strong" };
  if (score >= 2) return { label: "Balanced", tone: "steady" };
  return { label: "Needs attention", tone: "watch" };
}

function BalanceEquation({ company, story }: { company: Company; story: CompanyBalanceSheetStory }) {
  const { assets, liabilities, shareholdersEquity } = story.equation;
  const liabilitiesShare = percentOfBaseForDisplay(liabilities, assets);
  const equityShare = percentOfBaseForDisplay(shareholdersEquity, assets);
  const health = balanceHealth(story);
  return <section className="balance-equation-card" aria-labelledby="balance-equation-title">
    <header><div><h3 id="balance-equation-title">Assets are funded by liabilities and shareholders’ equity.</h3><p>The accounting equation reconciles for the latest reported fiscal year.</p></div><div className={`balance-health-badge ${health.tone}`}><i />{health.label}</div></header>
    <div className="balance-equation">
      <div className="balance-equation-side assets"><small>WHAT IT OWNS</small><strong>{formatAmount(company, assets)}</strong><span className="balance-equation-label"><b>Assets</b><i>· 100%</i></span></div>
      <em>=</em>
      <div className="balance-equation-funding">
        <small>HOW IT IS FUNDED</small>
        <div className="balance-equation-funding-item liabilities"><strong>{formatAmount(company, liabilities)}</strong><span className="balance-equation-label"><b>Liabilities</b><i>· {formatPercent(liabilitiesShare)}</i></span></div>
        <div className="balance-equation-funding-item equity"><strong>{formatAmount(company, shareholdersEquity)}</strong><span className="balance-equation-label"><b>Shareholders’ equity</b><i>· {formatPercent(equityShare)}</i></span></div>
        <div className="balance-funding-bar" aria-hidden="true"><i style={{ width: `${Math.max(0, Math.min(100, liabilitiesShare ?? 0))}%` }} /><i style={{ width: `${Math.max(0, Math.min(100, equityShare ?? 0))}%` }} /></div>
      </div>
    </div>
  </section>;
}

function percentOfBaseForDisplay(value: number | null, base: number | null): number | null {
  return value !== null && base !== null && base !== 0 ? (value / Math.abs(base)) * 100 : null;
}

function BalanceHealthMetrics({ company, story }: { company: Company; story: CompanyBalanceSheetStory }) {
  const metrics = [
    { label: "Total debt", value: formatAmount(company, story.health.totalDebt), note: story.health.totalDebt === null ? "No debt separately reported" : "Interest-bearing obligations", tone: "neutral" },
    { label: "Net cash (debt)", value: formatAmount(company, story.health.netCashDebt), note: "Cash − total debt", tone: story.health.netCashDebt !== null && story.health.netCashDebt >= 0 ? "positive" : "negative" },
    { label: "Net debt / EBITDA", value: formatMultiple(story.health.netDebtToEbitda), note: "Lower means more headroom", tone: story.health.netDebtToEbitda !== null && story.health.netDebtToEbitda <= 1 ? "positive" : "neutral" },
    { label: "Interest coverage", value: formatMultiple(story.health.interestCoverage), note: "EBIT ÷ interest expense", tone: story.health.interestCoverage !== null && story.health.interestCoverage >= 5 ? "positive" : story.health.interestCoverage !== null && story.health.interestCoverage < 2 ? "negative" : "neutral" },
    { label: "Weighted avg. cost of debt", value: formatRatePercent(story.health.weightedAverageCostOfDebt), note: story.health.weightedAverageCostOfDebtNote, tone: "neutral" },
  ];
  return <section className="balance-health-metrics" aria-label={`Balance-sheet health metrics for fiscal year ${story.latestFiscalYear}`}>
    {metrics.map((metric) => <div key={metric.label} className={metric.tone}><span>{metric.label}</span><strong>{metric.value}</strong><small>{metric.note}</small></div>)}
  </section>;
}

export function BalanceSheetChapter({ company }: { company: Company }) {
  const [activeSection, setActiveSection] = useState<BalanceSectionTone>("assets");
  const story = company.companyStory?.balanceSheet;
  const available = story?.status === "available" && story.latestFiscalYear !== null && story.equation.assets !== null && story.equation.liabilities !== null && story.equation.shareholdersEquity !== null;
  useEffect(() => setActiveSection("assets"), [company.id]);
  const sections = available ? [
    { tone: "assets" as const, title: "Assets · what the company owns", tabTitle: "Assets", tabSubtitle: "What the company owns", total: story.equation.assets, components: story.assets, shareLabel: "total assets", description: "Five-year values are separated from the latest mix, so the direction and current balance are easy to scan." },
    { tone: "liabilities" as const, title: "Liabilities · what the company owes", tabTitle: "Liabilities", tabSubtitle: "What the company owes", total: story.equation.liabilities, components: story.liabilities, shareLabel: "total liabilities", description: "Reported obligations are shown with their five-year values; financing detail stays collapsed until requested." },
    { tone: "equity" as const, title: "Shareholders’ equity · what belongs to shareholders", tabTitle: "Shareholders’ equity", tabSubtitle: "What belongs to shareholders", total: story.equation.shareholdersEquity, components: story.shareholdersEquity, shareLabel: "shareholders’ equity", description: "Negative balances use a visible zero line, making accumulated losses and other adjustments easier to interpret." },
  ] : [];
  const selectAdjacentSection = (current: BalanceSectionTone, direction: -1 | 1) => {
    const order: BalanceSectionTone[] = ["assets", "liabilities", "equity"];
    const next = order[(order.indexOf(current) + direction + order.length) % order.length];
    setActiveSection(next);
    window.requestAnimationFrame(() => document.getElementById(`balance-${next}-tab`)?.focus());
  };
  return <section className="story-chapter-page balance-story-page">
    <header className="story-chapter-header">
      <div><span>04</span><hgroup><h2>The balance sheet</h2><p>{available ? `Assets = Liabilities + Shareholders’ Equity · FY${story.latestFiscalYear}` : "What the company owns, owes and has built for shareholders"}</p></hgroup></div>
      <div className="story-period-toggle" role="group" aria-label="Reporting period"><button className="active">Annual</button><button disabled title="Quarterly story view is in progress">Quarterly</button></div>
    </header>
    {available ? <>
      <BalanceHealthMetrics company={company} story={story} />
      <BalanceEquation company={company} story={story} />
      <section className="balance-section-selector" aria-label="Balance sheet section selector">
        <div className="balance-unit-note">Bars show reported values · {story.displayUnit}</div>
        <div role="tablist" aria-label="Balance sheet sections">
          {sections.map((section) => <button key={section.tone} className={`balance-section-tab ${section.tone}`} id={`balance-${section.tone}-tab`} role="tab" aria-selected={activeSection === section.tone} aria-controls={`balance-${section.tone}-panel`} tabIndex={activeSection === section.tone ? 0 : -1} onClick={() => setActiveSection(section.tone)} onKeyDown={(event) => {
            if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
            event.preventDefault();
            selectAdjacentSection(section.tone, event.key === "ArrowRight" ? 1 : -1);
          }}>
            <span aria-hidden="true">{section.tone === "assets" ? "A" : section.tone === "liabilities" ? "L" : "E"}</span>
            <span><strong>{section.tabTitle}</strong><small>{section.tabSubtitle}</small></span>
            <b>{formatAmount(company, section.total)}</b>
          </button>)}
        </div>
      </section>
      <div className="balance-section-panels">
        {sections.map((section) => <BalanceSection key={section.tone} company={company} title={section.title} description={section.description} components={section.components} tone={section.tone} shareLabel={section.shareLabel} latestFiscalYear={story.latestFiscalYear!} active={activeSection === section.tone} />)}
      </div>
      {story.summary && <aside className="balance-takeaway"><span aria-hidden="true">☆</span><div><strong>TaRaSha balance-sheet read</strong><p>{story.summary}</p></div></aside>}
      <footer className="story-methodology"><div><strong>TaRaShaData derivation</strong><p>{story.methodology}</p></div>{story.sourceUrl && <a href={story.sourceUrl} target="_blank" rel="noreferrer">Open source filing ↗</a>}</footer>
    </> : <div className="story-unavailable">
      <span aria-hidden="true">04</span><div><h3>The accounting equation is not available</h3><p>{story?.reason ?? "TaRaShaData did not return total assets and shareholders’ equity for a common annual period."}</p><small>No missing balance-sheet component or leverage input has been estimated.</small></div>
    </div>}
  </section>;
}

const stockRiskRanges: StockRiskRange[] = ["1Y", "3Y", "5Y", "10Y"];
const stockRiskSeriesLabels: Record<CompanyStockRiskSeries["key"], string> = {
  company: "Company",
  market: "Broad market",
  sector: "Sector",
};

function initialStockRiskRange(story: CompanyStockRiskStory | undefined): StockRiskRange {
  if (story?.defaultRange && stockRiskRanges.includes(story.defaultRange) && story.periods[story.defaultRange]) {
    return story.defaultRange;
  }
  return stockRiskRanges.find((range) => story?.availableRanges.includes(range) && story.periods[range]) ?? "5Y";
}

function formatRiskPercent(value: number | null, digits = 1): string {
  if (value === null || !Number.isFinite(value)) return "—";
  return `${value < 0 ? "−" : ""}${Math.abs(value).toFixed(digits)}%`;
}

function formatRiskBeta(value: number | null): string {
  return value === null || !Number.isFinite(value) ? "—" : value.toFixed(2);
}

function formatRiskDate(value: string): string {
  const parsed = new Date(`${value}T00:00:00Z`);
  return Number.isNaN(parsed.getTime())
    ? value
    : parsed.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric", timeZone: "UTC" });
}

function sampledRiskPoints(points: CompanyStockRiskPoint[], maximum = 380): CompanyStockRiskPoint[] {
  if (points.length <= maximum) return points;
  const stride = Math.ceil(points.length / maximum);
  const sampled = points.filter((_, index) => index % stride === 0);
  const last = points.at(-1);
  if (last && sampled.at(-1)?.date !== last.date) sampled.push(last);
  return sampled;
}

function RiskLegend({ series }: { series: CompanyStockRiskSeries[] }) {
  return <div className="stock-risk-legend" aria-label="Chart series">
    {series.map((item) => <span key={item.key}><i className={item.key} />{item.symbol}<small>{stockRiskSeriesLabels[item.key]}</small></span>)}
  </div>;
}

function RiskLineChart({ series, dataKey, label }: { series: CompanyStockRiskSeries[]; dataKey: "drawdown" | "rollingVolatility"; label: string }) {
  const [hoveredTime, setHoveredTime] = useState<number | null>(null);
  const width = dataKey === "rollingVolatility" ? 520 : 690;
  const height = 250;
  const left = 49;
  const right = 16;
  const top = 13;
  const bottom = 33;
  const chartSeries = series.map((item) => ({ ...item, points: sampledRiskPoints(item[dataKey]) })).filter((item) => item.points.length > 1);
  const allPoints = chartSeries.flatMap((item) => item.points);
  if (!allPoints.length) return <div className="stock-risk-chart-empty">The chart needs more historical observations.</div>;
  const timestamps = allPoints.map((point) => Date.parse(`${point.date}T00:00:00Z`));
  const minimumTime = Math.min(...timestamps);
  const maximumTime = Math.max(...timestamps);
  const values = allPoints.map((point) => point.value);
  const rawMinimum = Math.min(...values);
  const rawMaximum = Math.max(...values);
  const yMinimum = dataKey === "drawdown" ? Math.min(-10, Math.floor(rawMinimum / 10) * 10) : 0;
  const yMaximum = dataKey === "drawdown" ? 0 : Math.max(10, Math.ceil(rawMaximum / 10) * 10);
  const plotWidth = width - left - right;
  const plotHeight = height - top - bottom;
  const x = (dateValue: string) => left + ((Date.parse(`${dateValue}T00:00:00Z`) - minimumTime) / Math.max(1, maximumTime - minimumTime)) * plotWidth;
  const y = (value: number) => top + ((yMaximum - value) / Math.max(1, yMaximum - yMinimum)) * plotHeight;
  const yTicks = Array.from({ length: 6 }, (_, index) => yMaximum - ((yMaximum - yMinimum) * index) / 5);
  const xTicks = Array.from({ length: 6 }, (_, index) => minimumTime + ((maximumTime - minimumTime) * index) / 5);
  const xTickLabel = (timestamp: number) => {
    const dateValue = new Date(timestamp);
    const shortRange = maximumTime - minimumTime < 370 * 24 * 60 * 60 * 1000;
    return dateValue.toLocaleDateString("en-US", shortRange
      ? { month: "short", year: "2-digit", timeZone: "UTC" }
      : { year: "numeric", timeZone: "UTC" });
  };
  const hoverRatio = hoveredTime === null ? null : (hoveredTime - minimumTime) / Math.max(1, maximumTime - minimumTime);
  const hoverPoints = hoveredTime === null ? [] : chartSeries.map((item) => ({
    item,
    point: item.points.reduce((nearest, point) => Math.abs(Date.parse(`${point.date}T00:00:00Z`) - hoveredTime) < Math.abs(Date.parse(`${nearest.date}T00:00:00Z`) - hoveredTime) ? point : nearest, item.points[0]),
  }));
  const hoverDate = hoverPoints.find(({ item }) => item.key === "company")?.point.date ?? hoverPoints[0]?.point.date;
  return <div className="story-interactive-chart risk-interactive-chart">
    <svg className="stock-risk-line-chart" viewBox={`0 0 ${width} ${height}`} role="img" aria-label={label}>
      <title>{label}</title>
      <desc>TaRaShaData-derived percentage series. Hover across the plot for the exact date and values.</desc>
      {yTicks.map((tick) => <g key={tick}>
        <line className="stock-risk-grid-line" x1={left} x2={width - right} y1={y(tick)} y2={y(tick)} />
        <text className="stock-risk-axis-label" x={left - 8} y={y(tick) + 3} textAnchor="end">{Math.round(tick)}%</text>
      </g>)}
      {xTicks.map((tick, index) => <text key={tick} className="stock-risk-axis-label" x={left + (plotWidth * index) / 5} y={height - 8} textAnchor={index === 0 ? "start" : index === 5 ? "end" : "middle"}>{xTickLabel(tick)}</text>)}
      {chartSeries.map((item) => <path key={item.key} className={`stock-risk-series-line ${item.key}`} d={item.points.map((point, index) => `${index ? "L" : "M"}${x(point.date).toFixed(2)},${y(point.value).toFixed(2)}`).join(" ")}><title>{item.symbol}</title></path>)}
      {hoverRatio !== null && <g className="story-chart-hover-layer" aria-hidden="true">
        <line className="story-chart-crosshair" x1={left + hoverRatio * plotWidth} x2={left + hoverRatio * plotWidth} y1={top} y2={height - bottom} />
        {hoverPoints.map(({ item, point }) => <circle key={item.key} className={`story-chart-marker ${item.key}`} cx={x(point.date)} cy={y(point.value)} r="4" />)}
      </g>}
      <rect
        className="story-chart-hit-area"
        data-chart-hit="risk-line"
        x={left}
        y={top}
        width={plotWidth}
        height={plotHeight}
        onMouseMove={(event) => {
          const bounds = event.currentTarget.getBoundingClientRect();
          const ratio = Math.max(0, Math.min(1, (event.clientX - bounds.left) / Math.max(1, bounds.width)));
          setHoveredTime(minimumTime + ratio * (maximumTime - minimumTime));
        }}
        onMouseLeave={() => setHoveredTime(null)}
      />
    </svg>
    {hoverRatio !== null && hoverDate && <div className="story-chart-tooltip" data-chart-tooltip="risk-line" style={{ left: `${Math.max(10, Math.min(82, hoverRatio * 100))}%` }}>
      <strong>{formatRiskDate(hoverDate)}</strong>
      {hoverPoints.map(({ item, point }) => <span key={item.key}><i className={item.key} />{item.symbol}<b>{formatRiskPercent(point.value, 2)}</b></span>)}
    </div>}
  </div>;
}

function RiskCharacteristics({ story, period, focusControls }: { story: CompanyStockRiskStory; period: CompanyStockRiskPeriod; focusControls: ReactNode }) {
  const series = period.series;
  const seriesValue = (seriesKey: CompanyStockRiskSeries["key"], range: StockRiskRange, metric: keyof CompanyStockRiskSeries) => story.periods[range]?.series.find((item) => item.key === seriesKey)?.[metric] as number | null | undefined;
  const currentValue = (seriesKey: CompanyStockRiskSeries["key"], metric: keyof CompanyStockRiskSeries) => period.series.find((item) => item.key === seriesKey)?.[metric] as number | null | undefined;
  const rows = [
    { label: "1Y Volatility (Annualized)", tone: "neutral", format: formatRiskPercent, value: (key: CompanyStockRiskSeries["key"]) => seriesValue(key, "1Y", "annualizedVolatilityPercent") ?? null },
    { label: "3Y Volatility (Annualized)", tone: "neutral", format: formatRiskPercent, value: (key: CompanyStockRiskSeries["key"]) => seriesValue(key, "3Y", "annualizedVolatilityPercent") ?? null },
    { label: "Beta (vs S&P 500)", tone: "neutral", format: formatRiskBeta, value: (key: CompanyStockRiskSeries["key"]) => currentValue(key, "betaToMarket") ?? null },
    { label: "Maximum Drawdown", tone: "negative", format: formatRiskPercent, value: (key: CompanyStockRiskSeries["key"]) => currentValue(key, "maximumDrawdownPercent") ?? null },
    { label: "Worst 1-Month Return", tone: "negative", format: formatRiskPercent, value: (key: CompanyStockRiskSeries["key"]) => currentValue(key, "worstMonthlyReturnPercent") ?? null },
    { label: "Best 1-Month Return", tone: "positive", format: formatRiskPercent, value: (key: CompanyStockRiskSeries["key"]) => currentValue(key, "bestMonthlyReturnPercent") ?? null },
  ];
  return <MaximizableStorySection className="stock-risk-card risk-characteristics-card" label="Risk Characteristics" chapterTitle="How the stock behaves" description="Compare volatility, beta, drawdown, and best or worst months across the company and its benchmarks." focusControls={focusControls} focusMeta={`${period.range} comparison`} focusSource="TaRaShaData historical market data">
    <header><h3>Risk Characteristics</h3><small>{period.range} comparison</small></header>
    <div className="stock-risk-table-scroll"><table className="stock-risk-table">
      <thead><tr><th>Metric</th>{series.map((item) => <th key={item.key}><span className={`risk-series-dot ${item.key}`} />{item.symbol}</th>)}</tr></thead>
      <tbody>{rows.map((row) => <tr key={row.label}><th>{row.label}</th>{series.map((item) => <td key={item.key} className={row.tone}>{row.format(row.value(item.key))}</td>)}</tr>)}</tbody>
    </table></div>
    <div className="stock-risk-insight"><span aria-hidden="true">↗</span><p>{period.riskSummary}</p></div>
    {story.sectorBenchmark && <p className="stock-risk-benchmark-note"><sup>1</sup> {story.sectorBenchmark.name}; selected from {story.sectorBenchmark.selectionBasis}.</p>}
  </MaximizableStorySection>;
}

function DrawdownAnalysis({ period, focusControls }: { period: CompanyStockRiskPeriod; focusControls: ReactNode }) {
  return <MaximizableStorySection className="stock-risk-card drawdown-card" label="Drawdown Analysis" chapterTitle="How the stock behaves" description="Inspect peak-to-trough losses for the company, broad market, and sector benchmark." focusControls={focusControls} focusMeta={`${period.range} history`} focusSource="TaRaShaData historical market data">
    <header><h3>Drawdown Analysis</h3><small>Peak-to-trough decline · {period.range}</small></header>
    <RiskLegend series={period.series} />
    <div className="stock-risk-chart-with-rail">
      <RiskLineChart series={period.series} dataKey="drawdown" label={`Drawdown comparison over ${period.range}`} />
      <aside><strong>Largest drawdown</strong>{period.series.map((item) => <div key={item.key}><span>{item.symbol}</span><b className={item.key === "company" ? "company" : ""}>{formatRiskPercent(item.maximumDrawdownPercent)}</b></div>)}</aside>
    </div>
    <div className="stock-risk-insight"><span aria-hidden="true">↗</span><p>{period.drawdownSummary}</p></div>
  </MaximizableStorySection>;
}

function RollingVolatility({ period, focusControls }: { period: CompanyStockRiskPeriod; focusControls: ReactNode }) {
  return <MaximizableStorySection className="stock-risk-card rolling-volatility-card" label="Rolling Volatility" chapterTitle="How the stock behaves" description="Follow annualized volatility through time using a trailing 252-trading-day window." focusControls={focusControls} focusMeta={`${period.range} history`} focusSource="TaRaShaData historical market data">
    <header><h3>Rolling Volatility (Annualized)</h3><small>Trailing 252 trading days</small></header>
    <RiskLegend series={period.series} />
    <div className="stock-risk-chart-with-rail compact">
      <RiskLineChart series={period.series} dataKey="rollingVolatility" label={`Rolling annualized volatility over ${period.range}`} />
      <aside><strong>Current (1Y)</strong>{period.series.map((item) => <div key={item.key}><span>{item.symbol}</span><b className={item.key === "company" ? "company" : ""}>{formatRiskPercent(item.annualizedVolatilityPercent)}</b></div>)}</aside>
    </div>
  </MaximizableStorySection>;
}

function MonthlyDistributionChart({ period }: { period: CompanyStockRiskPeriod }) {
  const [hoveredBin, setHoveredBin] = useState<number | null>(null);
  const width = 520;
  const height = 250;
  const left = 38;
  const right = 10;
  const top = 12;
  const bottom = 58;
  const bins = period.series[0]?.monthlyDistribution ?? [];
  const maximum = Math.max(10, Math.ceil(Math.max(...period.series.flatMap((series) => series.monthlyDistribution.map((bin) => bin.percentage ?? 0))) / 10) * 10);
  const plotWidth = width - left - right;
  const plotHeight = height - top - bottom;
  const groupWidth = plotWidth / Math.max(1, bins.length);
  const barWidth = Math.min(13, (groupWidth - 12) / Math.max(1, period.series.length));
  const y = (value: number) => top + ((maximum - value) / maximum) * plotHeight;
  const selectedBin = hoveredBin === null ? null : bins[hoveredBin];
  return <div className="story-interactive-chart distribution-interactive-chart">
    <svg className="stock-risk-distribution-chart" viewBox={`0 0 ${width} ${height}`} role="img" aria-label={`Monthly return distribution over ${period.range}`}>
      <title>Monthly return distribution over {period.range}</title>
      {[0, maximum / 2, maximum].map((tick) => <g key={tick}><line className="stock-risk-grid-line" x1={left} x2={width - right} y1={y(tick)} y2={y(tick)} /><text className="stock-risk-axis-label" x={left - 7} y={y(tick) + 3} textAnchor="end">{tick}%</text></g>)}
      {bins.map((bin, binIndex) => {
        const groupX = left + binIndex * groupWidth;
        return <g key={bin.key} className={hoveredBin === binIndex ? "is-hovered" : undefined}>
          {hoveredBin === binIndex && <rect className="story-chart-hover-band" x={groupX + 1} y={top} width={groupWidth - 2} height={plotHeight} rx="3" />}
          {period.series.map((series, seriesIndex) => {
            const value = series.monthlyDistribution.find((item) => item.key === bin.key)?.percentage ?? 0;
            const x = groupX + (groupWidth - period.series.length * barWidth) / 2 + seriesIndex * barWidth;
            return <rect key={series.key} className={`stock-risk-distribution-bar ${series.key}`} x={x} y={y(value)} width={Math.max(3, barWidth - 2)} height={Math.max(0, top + plotHeight - y(value))}><title>{series.symbol}: {formatRiskPercent(value)}</title></rect>;
          })}
          <rect className="story-chart-hit-area" data-chart-hit="monthly-distribution" x={groupX} y={top} width={groupWidth} height={plotHeight} onMouseEnter={() => setHoveredBin(binIndex)} onMouseLeave={() => setHoveredBin(null)} />
          <text className="stock-risk-bin-label" x={groupX + groupWidth / 2} y={height - 28} textAnchor="middle">{bin.label.includes(" to ") ? <><tspan x={groupX + groupWidth / 2}>{bin.label.split(" to ")[0]} to</tspan><tspan x={groupX + groupWidth / 2} dy="11">{bin.label.split(" to ")[1]}</tspan></> : bin.label}</text>
        </g>;
      })}
    </svg>
    {selectedBin && hoveredBin !== null && <div className="story-chart-tooltip distribution-tooltip" data-chart-tooltip="monthly-distribution" style={{ left: `${Math.max(12, Math.min(80, ((hoveredBin + .5) / Math.max(1, bins.length)) * 100))}%` }}>
      <strong>{selectedBin.label}</strong>
      {period.series.map((series) => {
        const bin = series.monthlyDistribution.find((item) => item.key === selectedBin.key);
        return <span key={series.key}><i className={series.key} />{series.symbol}<b>{formatRiskPercent(bin?.percentage ?? null, 1)} · {bin?.months ?? 0} mo.</b></span>;
      })}
    </div>}
  </div>;
}

function MonthlyReturnsDistribution({ period }: { period: CompanyStockRiskPeriod | undefined }) {
  const focusControls = <div className="story-focus-static-period"><b>7Y</b><span>Monthly history</span></div>;
  return <MaximizableStorySection className="stock-risk-card monthly-distribution-card" label="Monthly Returns Distribution" chapterTitle="How the stock behaves" description="See how frequently monthly returns landed in each return band over seven years." focusControls={focusControls} focusMeta={period ? `${period.series[0]?.monthlyObservations ?? 0} company months` : "History unavailable"} focusSource="TaRaShaData historical market data">
    <header><h3>Monthly Returns Distribution (7Y)</h3><small>{period ? `${period.series[0]?.monthlyObservations ?? 0} company months` : "7-year history required"}</small></header>
    {period ? <>
      <RiskLegend series={period.series} />
      <MonthlyDistributionChart period={period} />
    </> : <div className="stock-risk-chart-empty">Seven years of aligned company and benchmark monthly returns are required.</div>}
  </MaximizableStorySection>;
}

function UpDownMonths({ period, focusControls }: { period: CompanyStockRiskPeriod; focusControls: ReactNode }) {
  const [hoveredDirection, setHoveredDirection] = useState<"up" | "down" | "flat" | null>(null);
  const company = period.series.find((item) => item.key === "company")!;
  const positive = Math.max(0, Math.min(100, company.positiveMonthsPercent ?? 0));
  const negative = Math.max(0, Math.min(100, company.negativeMonthsPercent ?? 0));
  const flat = Math.max(0, Math.min(100, company.flatMonthsPercent ?? 0));
  const directionValues = {
    up: { label: "Up months", percentage: positive },
    down: { label: "Down months", percentage: negative },
    flat: { label: "Flat months", percentage: flat },
  };
  const selected = hoveredDirection ? directionValues[hoveredDirection] : null;
  return <MaximizableStorySection className="stock-risk-card up-down-card" label="Up vs Down Months" chapterTitle="How the stock behaves" description="Compare the frequency and observed count of positive, negative, and flat months." focusControls={focusControls} focusMeta={`${company.monthlyObservations} observations`} focusSource="TaRaShaData historical market data">
    <header><h3>Up vs Down Months ({period.range})</h3><small>{company.monthlyObservations} observations</small></header>
    <div className="stock-risk-donut-layout">
      <div className="stock-risk-donut story-interactive-chart">
        <svg viewBox="0 0 120 120" role="img" aria-label={`${company.symbol} had ${positive.toFixed(0)} percent positive months`}>
          <circle className="down" data-chart-hit="down-months" cx="60" cy="60" r="42" pathLength="100" onMouseEnter={() => setHoveredDirection("down")} onMouseLeave={() => setHoveredDirection(null)} />
          <circle className="up" data-chart-hit="up-months" cx="60" cy="60" r="42" pathLength="100" strokeDasharray={`${positive} ${100 - positive}`} onMouseEnter={() => setHoveredDirection("up")} onMouseLeave={() => setHoveredDirection(null)} />
        </svg>
        <div><strong>{company.symbol}</strong><b>{(selected?.percentage ?? positive).toFixed(0)}%</b><span>{selected?.label ?? "Up Months"}</span></div>
        {selected && <div className="story-chart-tooltip donut-tooltip" data-chart-tooltip="up-down-months"><strong>{selected.label}</strong><span>{Math.round(company.monthlyObservations * selected.percentage / 100)} of {company.monthlyObservations} months<b>{selected.percentage.toFixed(1)}%</b></span></div>}
      </div>
      <div className="stock-risk-direction-legend"><p onMouseEnter={() => setHoveredDirection("up")} onMouseLeave={() => setHoveredDirection(null)}><i className="up" />Up Months <b>{positive.toFixed(0)}%</b></p><p onMouseEnter={() => setHoveredDirection("down")} onMouseLeave={() => setHoveredDirection(null)}><i className="down" />Down Months <b>{negative.toFixed(0)}%</b></p>{flat > 0 && <p onMouseEnter={() => setHoveredDirection("flat")} onMouseLeave={() => setHoveredDirection(null)}><i className="flat" />Flat Months <b>{formatRiskPercent(flat)}</b></p>}</div>
    </div>
    <div className="stock-risk-insight"><span aria-hidden="true">↗</span><p>{period.positiveMonthsSummary}</p></div>
  </MaximizableStorySection>;
}

export function StockRiskChapter({ company }: { company: Company }) {
  const story = company.companyStory?.stockRisk;
  const initialRange = initialStockRiskRange(story);
  const [activeRange, setActiveRange] = useState<StockRiskRange>(initialRange);
  useEffect(() => setActiveRange(initialStockRiskRange(story)), [company.id, story?.defaultRange]);
  const period = story?.periods[activeRange];
  const available = story?.status === "available" && Boolean(period);
  const sourceLinks = period?.series ?? [];
  const rangeControls = <div className="story-period-toggle stock-risk-period-toggle" role="group" aria-label="Focused risk analysis period">
    {stockRiskRanges.map((range) => <button key={range} className={activeRange === range ? "active" : ""} disabled={!story?.availableRanges.includes(range)} onClick={() => setActiveRange(range)} aria-pressed={activeRange === range}>{range}</button>)}
  </div>;
  return <section className="story-chapter-page stock-risk-page">
    <header className="story-chapter-header">
      <div><span>05</span><hgroup><h2>How the stock behaves</h2><p>{available ? `Historical risk behavior through ${formatRiskDate(period!.endDate)}` : "Historical risk and volatility profile"}</p></hgroup></div>
      <div className="story-period-toggle stock-risk-period-toggle" role="group" aria-label="Risk analysis period">
        {stockRiskRanges.map((range) => <button key={range} className={activeRange === range ? "active" : ""} disabled={!story?.availableRanges.includes(range)} onClick={() => setActiveRange(range)} aria-pressed={activeRange === range}>{range}</button>)}
      </div>
    </header>
    {available && story && period ? <>
      <div className="stock-risk-top-grid">
        <RiskCharacteristics story={story} period={period} focusControls={rangeControls} />
        <DrawdownAnalysis period={period} focusControls={rangeControls} />
      </div>
      <div className="stock-risk-bottom-grid">
        <RollingVolatility period={period} focusControls={rangeControls} />
        <MonthlyReturnsDistribution period={story.periods["7Y"]} />
        <UpDownMonths period={period} focusControls={rangeControls} />
      </div>
      <footer className="stock-risk-footnote">
        <span className="stock-risk-info" aria-hidden="true">i</span>
        <div>
          <strong>Past performance is not indicative of future results.</strong>
          <p><b>Source:</b> TaRaShaData · {story.source.name} (delayed, non-authoritative market history). {story.source.priceBasis}. Data through {formatRiskDate(story.asOf ?? period.endDate)}.</p>
          <p><b>Derivation:</b> {story.methodology}</p>
          {story.sectorBenchmark && <p><b>Sector benchmark:</b> {story.sectorBenchmark.selectionBasis}. Coverage uses {period.series[0]?.tradingDays.toLocaleString()} company trading-day returns and {period.series[0]?.monthlyObservations} company monthly returns for {period.range}.</p>}
        </div>
        <div className="stock-risk-source-links">{sourceLinks.map((item) => <a key={item.key} href={item.sourceUrl} target="_blank" rel="noreferrer">{item.symbol} history ↗</a>)}</div>
      </footer>
    </> : <div className="story-unavailable">
      <span aria-hidden="true">05</span><div><h3>Risk history is not available</h3><p>{story?.reason ?? "TaRaShaData did not return sufficient aligned company and benchmark adjusted-price history."}</p><small>No volatility, beta, drawdown or monthly-return metric has been estimated.</small></div>
    </div>}
  </section>;
}

type MarketPricingView = "current" | "5Y" | "10Y";

const marketMetricGlyphs: Record<CompanyMarketPricingMetric["key"], string> = {
  pe: "P",
  forward_pe: "F",
  ev_ebitda: "E",
  ev_ebit: "E",
  price_sales: "S",
  price_book: "B",
  fcf_yield: "%",
  peg: "G",
};

function formatMarketValue(value: number | null, unit: "multiple" | "percent", digits = 1): string {
  if (value === null || !Number.isFinite(value)) return "—";
  if (unit === "percent") return `${value < 0 ? "−" : ""}${Math.abs(value).toFixed(digits)}%`;
  return `${value < 0 ? "−" : ""}${Math.abs(value).toFixed(digits)}x`;
}

function formatMarketPercent(value: number | null, showPlus = false): string {
  if (value === null || !Number.isFinite(value)) return "—";
  const sign = value < 0 ? "−" : showPlus && value > 0 ? "+" : "";
  return `${sign}${Math.abs(value).toFixed(1)}%`;
}

function formatMarketPrice(currency: string | null, value: number | null): string {
  if (value === null || !Number.isFinite(value)) return "—";
  const code = currency && /^[A-Z]{3}$/.test(currency) ? currency : "USD";
  try {
    return new Intl.NumberFormat("en-US", { style: "currency", currency: code, minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(value);
  } catch {
    return `${code} ${value.toFixed(2)}`;
  }
}

function formatMarketDate(value: string | null): string {
  if (!value) return "date unavailable";
  const parsed = new Date(value.length <= 10 ? `${value}T00:00:00Z` : value);
  return Number.isNaN(parsed.getTime()) ? value : parsed.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric", timeZone: "UTC" });
}

function ValuationSummaryCard({ story, focusControls }: { story: CompanyMarketPricingStory; focusControls: ReactNode }) {
  return <MaximizableStorySection className="market-pricing-card valuation-summary-card" label="Valuation Summary" chapterTitle="What the market is pricing" description="Review the company’s current trading multiples and the financial basis behind each metric." focusControls={focusControls} focusMeta="Current valuation" focusSource="TaRaShaData normalized filings and delayed market data">
    <header><h3>Valuation Summary</h3><p>How the market values {story.symbol ?? "the company"} today</p></header>
    <div className="valuation-summary-list">{story.currentMetrics.map((metric) => <div key={metric.key}>
      <span className="valuation-summary-icon" aria-hidden="true">{marketMetricGlyphs[metric.key]}</span>
      <span><b>{metric.label}</b><small>{metric.basis}</small></span>
      <strong>{formatMarketValue(metric.current, metric.unit)}</strong>
    </div>)}</div>
    {story.valuationRead && <aside className="market-pricing-insight"><span aria-hidden="true">☆</span><p>{story.valuationRead}</p></aside>}
  </MaximizableStorySection>;
}

function RangeMarker({ metric }: { metric: CompanyMarketPricingHistoryMetric }) {
  const position = metric.high === metric.low ? 50 : ((metric.current - metric.low) / (metric.high - metric.low)) * 100;
  const clamped = Math.max(0, Math.min(100, position));
  return <div className="valuation-range" aria-label={`${metric.label} historical range ${formatMarketValue(metric.low, metric.unit)} to ${formatMarketValue(metric.high, metric.unit)}, current ${formatMarketValue(metric.current, metric.unit)}`}>
    <span>{formatMarketValue(metric.low, metric.unit)}</span>
    <i data-chart-hit="valuation-range"><b style={{ left: `${clamped}%` }} /></i>
    <span>{formatMarketValue(metric.high, metric.unit)}</span>
    <span className="valuation-range-tooltip" data-chart-tooltip="valuation-range"><strong>{metric.label}</strong><small>Low <b>{formatMarketValue(metric.low, metric.unit)}</b></small><small>Median <b>{formatMarketValue(metric.median, metric.unit)}</b></small><small>Current <b>{formatMarketValue(metric.current, metric.unit)}</b></small><small>Percentile <b>{metric.percentile.toFixed(0)}%</b></small></span>
  </div>;
}

function HistoryContextCard({ story, windowKey, focusControls }: { story: CompanyMarketPricingStory; windowKey: "5Y" | "10Y"; focusControls: ReactNode }) {
  const window = story.windows[windowKey];
  const title = `Current vs ${windowKey === "5Y" ? "5-Year" : "10-Year"} History`;
  return <MaximizableStorySection className="market-pricing-card valuation-history-card" label={title} chapterTitle="What the market is pricing" description="Place today’s valuation inside its observed historical range and percentile." focusControls={focusControls} focusMeta={`${windowKey} context`} focusSource="TaRaShaData fiscal-year observations">
    <header><h3>{title}</h3><p>{window ? `${window.observations} fiscal-year observations${window.startDate ? ` · ${formatMarketDate(window.startDate)} to ${formatMarketDate(window.endDate)}` : ""}` : "Insufficient historical observations"}</p></header>
    {window?.metrics.length ? <>
      <div className="valuation-history-table-wrap"><table className="valuation-history-table">
        <thead><tr><th>Metric</th><th>Current</th><th>{windowKey} Median</th><th>{windowKey} Range</th><th>Percentile</th></tr></thead>
        <tbody>{window.metrics.map((metric) => <tr key={metric.key}>
          <th>{metric.label}</th>
          <td>{formatMarketValue(metric.current, metric.unit)}</td>
          <td>{formatMarketValue(metric.median, metric.unit)}</td>
          <td><RangeMarker metric={metric} /></td>
          <td><strong>{metric.percentile.toFixed(0)}%</strong></td>
        </tr>)}</tbody>
      </table></div>
      <div className="valuation-percentile-scale"><span>Percentile vs {windowKey} history</span><div>{[
        ["0–20%", "Undervalued"],
        ["20–40%", "Below Avg."],
        ["40–60%", "Average"],
        ["60–80%", "Above Avg."],
        ["80–100%", "Expensive"],
      ].map(([range, label]) => <p key={range}><b>{range}</b><small>{label}</small></p>)}</div></div>
    </> : <div className="market-pricing-empty"><span>—</span><p>At least three matched fiscal-year price and financial observations are required.</p></div>}
  </MaximizableStorySection>;
}

function ImpliedExpectationsCard({ story, focusControls }: { story: CompanyMarketPricingStory; focusControls: ReactNode }) {
  const glyphs: Record<string, string> = { revenue_growth: "↗", fcf_growth: "⌁", operating_margin: "◒", discount_rate: "%" };
  return <MaximizableStorySection className="market-pricing-card implied-expectations-card" label="Implied Expectations" chapterTitle="What the market is pricing" description="Read the normalized growth, margin, and discount-rate assumptions embedded in current multiples." focusControls={focusControls} focusMeta="Reverse-valuation context" focusSource="TaRaShaData-derived valuation shorthand">
    <header><h3>Implied Expectations</h3><p>What today’s multiples suggest</p></header>
    {story.impliedExpectations.length ? <div className="implied-expectations-list">{story.impliedExpectations.map((item) => <div key={item.key} title={item.detail}>
      <span aria-hidden="true">{glyphs[item.key]}</span><p><b>{item.label}</b><small>{item.detail}</small></p><strong>~{formatMarketPercent(item.value)}</strong>
    </div>)}</div> : <div className="market-pricing-empty compact"><span>—</span><p>No implied expectation is shown without the required current and historical multiples.</p></div>}
    {story.impliedExpectations.length > 0 && <aside className="market-pricing-insight"><span aria-hidden="true">☆</span><p>These are reverse-valuation shorthands—not forecasts. Hover or focus each row for the stated normalization assumption.</p></aside>}
  </MaximizableStorySection>;
}

function PeerComparisonCard({ story, focusControls }: { story: CompanyMarketPricingStory; focusControls: ReactNode }) {
  return <MaximizableStorySection className="market-pricing-card peer-comparison-card" label="Peer Comparison" chapterTitle="What the market is pricing" description="Compare key valuation measures across the company’s stored SEC-SIC screening set." focusControls={focusControls} focusMeta={`${story.peers.length} comparable companies`} focusSource="TaRaShaData peer framework and delayed prices">
    <header><h3>Peer Comparison</h3><p>{story.peerFramework}</p></header>
    {story.peers.length ? <div className="peer-comparison-table-wrap"><table className="peer-comparison-table">
      <thead><tr><th>Company</th><th>P/E</th><th>EV / EBITDA</th><th>FCF Yield</th></tr></thead>
      <tbody>{story.peers.map((peer) => <tr key={`${peer.cik}-${peer.symbol}`} className={peer.isCompany ? "current-company" : ""}>
        <th><span>{peer.name}</span><small>({peer.symbol})</small></th>
        <td>{formatMarketValue(peer.pe, "multiple")}</td>
        <td>{formatMarketValue(peer.evEbitda, "multiple")}</td>
        <td>{formatMarketValue(peer.fcfYield, "percent")}</td>
      </tr>)}</tbody>
    </table></div> : <div className="market-pricing-empty compact"><span>—</span><p>No SEC-SIC peer has both a delayed price and comparable TaRaShaData denominators.</p></div>}
  </MaximizableStorySection>;
}

function TreasuryComparisonCard({ story, focusControls }: { story: CompanyMarketPricingStory; focusControls: ReactNode }) {
  const comparison = story.treasuryComparison;
  const maximum = Math.max(1, comparison?.fcfYield ?? 0, comparison?.treasuryYield ?? 0) * 1.15;
  const width = (value: number | null | undefined) => `${Math.max(0, Math.min(100, ((value ?? 0) / maximum) * 100))}%`;
  const spreadTone = comparison?.spread === null || comparison?.spread === undefined ? "neutral" : comparison.spread >= 0 ? "positive" : "negative";
  return <MaximizableStorySection className="market-pricing-card treasury-comparison-card" label="FCF Yield vs 10Y U.S. Treasury" chapterTitle="What the market is pricing" description="Compare current free-cash-flow yield with the 10-year risk-free reference and inspect the spread." focusControls={focusControls} focusMeta={comparison?.treasuryAsOf ? `Treasury as of ${formatMarketDate(comparison.treasuryAsOf)}` : "Current comparison"} focusSource="TaRaShaData market data and FRED Treasury observation">
    <header><h3>FCF Yield vs 10Y U.S. Treasury</h3><p>Current FCF yield versus the risk-free reference</p></header>
    {comparison ? <>
      <div className="yield-comparison-bars">
        <div data-chart-hit="fcf-yield"><span>{story.symbol} FCF Yield</span><strong>{formatMarketValue(comparison.fcfYield, "percent", 2)}</strong><i><b className="fcf" style={{ width: width(comparison.fcfYield) }} /></i><span className="yield-hover-tooltip" data-chart-tooltip="yield-comparison"><b>{story.symbol} FCF Yield</b>{formatMarketValue(comparison.fcfYield, "percent", 2)}</span></div>
        <div data-chart-hit="treasury-yield"><span>10Y U.S. Treasury</span><strong>{formatMarketValue(comparison.treasuryYield, "percent", 2)}</strong><i><b className="treasury" style={{ width: width(comparison.treasuryYield) }} /></i><span className="yield-hover-tooltip" data-chart-tooltip="yield-comparison"><b>10Y U.S. Treasury</b>{formatMarketValue(comparison.treasuryYield, "percent", 2)}</span></div>
      </div>
      <div className="yield-spread"><span>Spread (FCF Yield − Treasury)</span><strong className={spreadTone}>{formatMarketPercent(comparison.spread)}</strong></div>
      <aside className="market-pricing-insight"><span aria-hidden="true">☆</span><p>{comparison.spread === null ? "Treasury comparison is awaiting a recent FRED observation." : comparison.spread < 0 ? "FCF yield is below the 10-year Treasury yield." : "FCF yield is above the 10-year Treasury yield."}</p></aside>
    </> : <div className="market-pricing-empty compact"><span>—</span><p>The FCF-yield comparison is unavailable.</p></div>}
  </MaximizableStorySection>;
}

function AnalystEstimatesCard({ story, focusControls }: { story: CompanyMarketPricingStory; focusControls: ReactNode }) {
  const estimates = story.analystEstimates;
  const available = estimates?.status === "available" && estimates.baseCase !== null && estimates.bullCase !== null && estimates.bearCase !== null;
  return <MaximizableStorySection className="market-pricing-card analyst-estimates-card" label="Analysts Estimate" chapterTitle="What the market is pricing" description="Review the available third-party low, mean, and high 12-month price scenarios." focusControls={focusControls} focusMeta={estimates?.analystCount ? `${estimates.analystCount} contributing opinions` : "Third-party estimates"} focusSource="Third-party estimates displayed with source context">
    <header><h3>Analysts Estimate</h3><p>{estimates?.analystCount ? `${estimates.analystCount} contributing opinions · third-party estimates` : "Third-party 12-month price estimates"}</p></header>
    {available && estimates ? <>
      <div className="analyst-scenarios">
        <div><span>Base Case Value</span><strong className="base">{formatMarketPrice(story.currency, estimates.baseCase)}</strong></div>
        <div><span>Bull Case Value</span><strong className="bull">{formatMarketPrice(story.currency, estimates.bullCase)}</strong></div>
        <div><span>Bear Case Value</span><strong className="bear">{formatMarketPrice(story.currency, estimates.bearCase)}</strong></div>
      </div>
      <div className="analyst-current-comparison"><div><span>Current Price</span><strong>{formatMarketPrice(story.currency, estimates.currentPrice)}</strong></div><div><span>Current vs. Base Case</span><strong className={(estimates.currentVsBasePercent ?? 0) >= 0 ? "positive" : "negative"}>{formatMarketPercent(estimates.currentVsBasePercent, true)}</strong></div></div>
    </> : <div className="market-pricing-empty compact"><span>—</span><p>{estimates?.reason ?? "A complete zero-cost analyst low, mean, and high target range is not available."}</p></div>}
  </MaximizableStorySection>;
}

export function MarketPricingChapter({ company }: { company: Company }) {
  const story = company.companyStory?.marketPricing;
  const [view, setView] = useState<MarketPricingView>("current");
  useEffect(() => setView("current"), [company.id]);
  const windowKey: "5Y" | "10Y" = view === "10Y" ? "10Y" : "5Y";
  const available = story?.status === "available" && story.currentPrice !== null && story.currentMetrics.length > 0;
  const focusControls = <div className="story-period-toggle market-pricing-period-toggle" role="group" aria-label="Focused valuation context period">
    <button className={view === "current" ? "active" : ""} onClick={() => setView("current")} aria-pressed={view === "current"}>Current</button>
    <button className={view === "5Y" ? "active" : ""} onClick={() => setView("5Y")} disabled={!story?.windows["5Y"]} aria-pressed={view === "5Y"}>5Y View</button>
    <button className={view === "10Y" ? "active" : ""} onClick={() => setView("10Y")} disabled={!story?.windows["10Y"]} aria-pressed={view === "10Y"}>10Y View</button>
  </div>;
  return <section className="story-chapter-page market-pricing-page">
    <header className="story-chapter-header">
      <div><span>06</span><hgroup><h2>What the market is pricing</h2><p>{available ? `Valuation and multiples · ${formatMarketPrice(story.currency, story.currentPrice)} as of ${formatMarketDate(story.asOf)}` : "Valuation and multiples"}</p></hgroup></div>
      <div className="story-period-toggle market-pricing-period-toggle" role="group" aria-label="Valuation context period">
        <button className={view === "current" ? "active" : ""} onClick={() => setView("current")} aria-pressed={view === "current"}>Current</button>
        <button className={view === "5Y" ? "active" : ""} onClick={() => setView("5Y")} disabled={!story?.windows["5Y"]} aria-pressed={view === "5Y"}>5Y View</button>
        <button className={view === "10Y" ? "active" : ""} onClick={() => setView("10Y")} disabled={!story?.windows["10Y"]} aria-pressed={view === "10Y"}>10Y View</button>
      </div>
    </header>
    {available && story ? <>
      <div className="market-pricing-top-grid">
        <ValuationSummaryCard story={story} focusControls={focusControls} />
        <HistoryContextCard story={story} windowKey={windowKey} focusControls={focusControls} />
        <ImpliedExpectationsCard story={story} focusControls={focusControls} />
      </div>
      <div className="market-pricing-bottom-grid">
        <PeerComparisonCard story={story} focusControls={focusControls} />
        <TreasuryComparisonCard story={story} focusControls={focusControls} />
        <AnalystEstimatesCard story={story} focusControls={focusControls} />
      </div>
      <aside className="market-pricing-takeaway"><span aria-hidden="true">☆</span><div><strong>TaRaSha Takeaway</strong><p>{story.takeaway ?? story.valuationRead}</p></div></aside>
      <footer className="market-pricing-footnote">
        <span className="stock-risk-info" aria-hidden="true">i</span>
        <div>
          <strong>Sources &amp; derivations</strong>
          <p><b>Financials and peers:</b> {story.source?.financials ?? "TaRaShaData normalized filings"}; {story.source?.peerFramework ?? story.peerFramework}. The peer table is a screening set from the stored framework, not a hand-selected core comp set.</p>
          <p><b>Market and analyst data:</b> {story.source?.marketName ?? "TaRaShaData market pipeline"} (delayed, non-authoritative, transient). {story.source?.priceBasis} Analyst scenarios are third-party Yahoo Finance estimates; bear/base/bull map to low/mean/high.</p>
          <p><b>Derivation:</b> {story.methodology}</p>
          {story.quality?.warnings.length ? <p><b>Availability notes:</b> {story.quality.warnings.join(" ")}</p> : null}
        </div>
        <div className="market-pricing-source-links">
          {story.source?.marketUrl && <a href={story.source.marketUrl} target="_blank" rel="noreferrer">Price history ↗</a>}
          {story.analystEstimates?.sourceUrl && <a href={story.analystEstimates.sourceUrl} target="_blank" rel="noreferrer">Third-party estimates ↗</a>}
          {story.treasuryComparison?.sourceUrl && <a href={story.treasuryComparison.sourceUrl} target="_blank" rel="noreferrer">10Y Treasury ↗</a>}
        </div>
      </footer>
    </> : <div className="story-unavailable">
      <span aria-hidden="true">06</span><div><h3>Market-pricing context is not available</h3><p>{story?.reason ?? "TaRaShaData did not return both a delayed price and the normalized denominators required for valuation."}</p><small>No valuation multiple, peer value, implied expectation, or analyst scenario has been fabricated.</small></div>
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
        : activeStory === "balance-sheet"
          ? <BalanceSheetChapter company={company} />
          : activeStory === "stock-behaves"
            ? <StockRiskChapter company={company} />
            : activeStory === "market-pricing"
              ? <MarketPricingChapter company={company} />
              : activeStory === "stock-got-here"
                ? <StockHistoryChapter company={company} />
          : <WorkInProgress storyId={activeStory} backToDiscover={backToDiscover} />, [activeStory, company, backToDiscover]);
  return <div className="company-story-page">
    <CompanyStoryHeader company={company} watched={watched} toggleWatch={toggleWatch} />
    <div className="company-story-shell"><StoryNavigation activeStory={activeStory} onChange={setActiveStory} /><div className="company-story-content">{content}</div></div>
  </div>;
}
