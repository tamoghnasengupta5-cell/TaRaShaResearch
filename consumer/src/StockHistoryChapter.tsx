import { useEffect, useState } from "react";
import { MaximizableStorySection } from "./MaximizableStorySection";
import type {
  Company,
  CompanyStockHistoryEvent,
  CompanyStockHistoryFundamentalPoint,
  CompanyStockHistoryPricePoint,
  CompanyStockHistoryStory,
  StockHistoryMode,
  StockHistoryRange,
} from "./types";

type DatedValue = { date: string; value: number; volume?: number | null; close?: number };
type HistoryHover = { date: string; value: number; volume?: number | null; mode: StockHistoryMode } | null;

const modes: Array<{ key: StockHistoryMode; label: string }> = [
  { key: "price", label: "Price" },
  { key: "total_return", label: "Total Return" },
  { key: "revenue", label: "Revenue" },
  { key: "eps", label: "EPS" },
  { key: "free_cash_flow", label: "Free Cash Flow" },
];
const ranges: StockHistoryRange[] = ["1Y", "3Y", "5Y", "10Y", "Max"];
const millisecondsPerYear = 365.2425 * 24 * 60 * 60 * 1000;

function time(value: string): number {
  return new Date(`${value.slice(0, 10)}T00:00:00Z`).getTime();
}

function fullDate(value: string | null): string {
  if (!value) return "—";
  const parsed = new Date(`${value.slice(0, 10)}T00:00:00Z`);
  return Number.isNaN(parsed.getTime())
    ? "—"
    : parsed.toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
      year: "numeric",
      timeZone: "UTC",
    });
}

function shortDate(value: string): string {
  return new Date(`${value.slice(0, 10)}T00:00:00Z`).toLocaleDateString("en-US", {
    month: "short",
    year: "2-digit",
    timeZone: "UTC",
  });
}

function percent(value: number | null | undefined, plus = false): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "—";
  const sign = value < 0 ? "−" : plus && value > 0 ? "+" : "";
  return `${sign}${Math.abs(value).toFixed(1)}%`;
}

function selectedPrices(
  story: CompanyStockHistoryStory,
  range: StockHistoryRange,
): CompanyStockHistoryPricePoint[] {
  const period = story.performance[range];
  if (!period) return [];
  return story.prices.filter(
    (point) => time(point.date) >= time(period.startDate) && time(point.date) <= time(period.endDate),
  );
}

function selectedFundamentals(
  points: CompanyStockHistoryFundamentalPoint[],
  story: CompanyStockHistoryStory,
  range: StockHistoryRange,
) {
  const period = story.performance[range];
  if (!period) return [];
  return points.filter(
    (point) => time(point.date) >= time(period.startDate) && time(point.date) <= time(period.endDate),
  );
}

function modePoints(
  story: CompanyStockHistoryStory,
  range: StockHistoryRange,
  mode: StockHistoryMode,
): DatedValue[] {
  if (mode === "price" || mode === "total_return") {
    const prices = selectedPrices(story, range);
    const base = prices[0]?.adjustedClose;
    return prices.map((point) => ({
      date: point.date,
      value: mode === "price" ? point.close : base ? (point.adjustedClose / base) * 100 : 0,
      volume: point.volume,
      close: point.close,
    }));
  }
  const series = mode === "revenue"
    ? story.fundamentals.revenue.points
    : mode === "eps"
      ? story.fundamentals.eps.points
      : story.fundamentals.freeCashFlow.points;
  return selectedFundamentals(series, story, range).map((point) => ({
    date: point.date,
    value: point.value,
  }));
}

function linePlot(
  points: DatedValue[],
  width: number,
  height: number,
  inset = { left: 42, right: 16, top: 18, bottom: 30 },
) {
  if (!points.length) {
    return {
      path: "",
      area: "",
      x: () => inset.left,
      y: () => inset.top,
      minimum: 0,
      maximum: 1,
    };
  }
  const times = points.map((point) => time(point.date));
  const values = points.map((point) => point.value);
  const first = Math.min(...times);
  const last = Math.max(...times);
  const rawMinimum = Math.min(...values);
  const rawMaximum = Math.max(...values);
  const padding = Math.max((rawMaximum - rawMinimum) * 0.1, Math.abs(rawMaximum) * 0.025, 0.1);
  const minimum = rawMinimum - padding;
  const maximum = rawMaximum + padding;
  const x = (day: string) => inset.left
    + ((time(day) - first) / Math.max(1, last - first)) * (width - inset.left - inset.right);
  const y = (value: number) => inset.top
    + ((maximum - value) / Math.max(0.0001, maximum - minimum))
      * (height - inset.top - inset.bottom);
  const path = points.map(
    (point, index) => `${index ? "L" : "M"}${x(point.date).toFixed(2)},${y(point.value).toFixed(2)}`,
  ).join(" ");
  const area = `${path} L${x(points.at(-1)!.date).toFixed(2)},${height - inset.bottom}`
    + ` L${x(points[0].date).toFixed(2)},${height - inset.bottom} Z`;
  return { path, area, x, y, minimum, maximum };
}

function modeUnit(story: CompanyStockHistoryStory, mode: StockHistoryMode): string {
  if (mode === "price") return `Price (${story.currency ?? "USD"})`;
  if (mode === "total_return") return "Total Return (Indexed to 100)";
  if (mode === "eps") return `${story.currency ?? "USD"} per share`;
  return `${mode === "revenue" ? "Revenue" : "Free Cash Flow"} (${story.currency ?? "USD"})`;
}

function valueLabel(story: CompanyStockHistoryStory, mode: StockHistoryMode, value: number): string {
  const symbol = story.currency === "USD" || !story.currency ? "$" : `${story.currency} `;
  if (mode === "price" || mode === "eps") {
    return `${symbol}${value.toLocaleString("en-US", { maximumFractionDigits: 2 })}`;
  }
  if (mode === "total_return") return value.toFixed(1);
  const absolute = Math.abs(value);
  const formatted = absolute >= 1_000_000_000_000
    ? `${(absolute / 1_000_000_000_000).toFixed(2)}T`
    : absolute >= 1_000_000_000
      ? `${(absolute / 1_000_000_000).toFixed(1)}B`
      : absolute >= 1_000_000
        ? `${(absolute / 1_000_000).toFixed(0)}M`
        : absolute >= 1000
          ? `${(absolute / 1000).toFixed(0)}K`
          : absolute.toFixed(0);
  return `${value < 0 ? "−" : ""}${symbol}${formatted}`;
}

function chartEvents(
  events: CompanyStockHistoryEvent[],
  points: DatedValue[],
  enabled: boolean,
): CompanyStockHistoryEvent[] {
  if (!enabled || !points.length) return [];
  const start = time(points[0].date);
  const end = time(points.at(-1)!.date);
  const inRange = events.filter((event) => time(event.date) >= start && time(event.date) <= end);
  const prioritized = [
    ...inRange.filter((event) => event.type === "corporate_action"),
    ...inRange.filter((event) => event.title.includes("Annual")),
    ...inRange.filter(
      (event) => event.type !== "corporate_action" && !event.title.includes("Annual"),
    ),
  ];
  if (prioritized.length <= 5) {
    return prioritized.sort((left, right) => time(left.date) - time(right.date));
  }
  const selected = Array.from(
    { length: 5 },
    (_, index) => prioritized[Math.round(index * (prioritized.length - 1) / 4)],
  );
  return [...new Map(
    selected.map((event) => [`${event.date}-${event.title}`, event]),
  ).values()].sort((left, right) => time(left.date) - time(right.date));
}

function StockHistoryChart({
  story,
  range,
  mode,
  showEvents,
  onHover,
}: {
  story: CompanyStockHistoryStory;
  range: StockHistoryRange;
  mode: StockHistoryMode;
  showEvents: boolean;
  onHover: (value: HistoryHover) => void;
}) {
  const [hoveredIndex, setHoveredIndex] = useState<number | null>(null);
  const points = modePoints(story, range, mode);
  const width = 850;
  const chartHeight = mode === "price" || mode === "total_return" ? 280 : 345;
  const plot = linePlot(points, width, chartHeight);
  const events = chartEvents(
    story.events,
    points,
    showEvents && (mode === "price" || mode === "total_return"),
  );
  const first = points[0];
  const last = points.at(-1);
  const displayedChange = points.length >= 2 && first && last && first.value !== 0
    ? ((last.value - first.value) / Math.abs(first.value)) * 100
    : null;
  const performance = story.performance[range];
  const change = mode === "price"
    ? performance?.priceChangePercent ?? displayedChange
    : mode === "total_return"
      ? performance?.totalReturnPercent ?? displayedChange
      : displayedChange;
  const displayedStart = (mode === "price" || mode === "total_return")
    ? performance?.startDate ?? first?.date
    : first?.date;
  const displayedEnd = (mode === "price" || mode === "total_return")
    ? performance?.endDate ?? last?.date
    : last?.date;
  const labels = points.length
    ? [...new Map(
      Array.from(
        { length: Math.min(7, points.length) },
        (_, index) => points[Math.round(index * (points.length - 1) / Math.max(1, Math.min(6, points.length - 1)))],
      ).map((point) => [point.date, point]),
    ).values()]
    : [];
  const volumeMax = Math.max(1, ...points.map((point) => point.volume ?? 0));
  const hoveredPoint = hoveredIndex === null ? null : points[hoveredIndex];
  return <div className="history-chart-panel">
    <div className="history-chart-meta">
      <span>{modeUnit(story, mode)}</span>
      <strong>{points.length >= 2
        ? `${fullDate(displayedStart ?? null)} – ${fullDate(displayedEnd ?? null)}`
        : points.length === 1
          ? "1 TaRaShaData observation in this range"
          : "No TaRaShaData observations in this range"}</strong>
      <b className={change !== null && change < 0 ? "negative" : "positive"}>
        {percent(change, true)}
      </b>
    </div>
    {points.length >= 2 ? <>
      <svg
        className="history-primary-chart"
        viewBox={`0 0 ${width} ${chartHeight}`}
        role="img"
        aria-label={`${modes.find((item) => item.key === mode)?.label} history for ${range}`}
      >
        <defs>
          <linearGradient id="history-price-area" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0" stopColor="#d7a344" stopOpacity=".23" />
            <stop offset="1" stopColor="#d7a344" stopOpacity="0" />
          </linearGradient>
        </defs>
        {Array.from({ length: 5 }, (_, index) => {
          const value = plot.maximum - (index / 4) * (plot.maximum - plot.minimum);
          const y = 18 + (index / 4) * (chartHeight - 48);
          return <g key={index}>
            <line className="history-grid" x1="42" x2={width - 16} y1={y} y2={y} />
            <text className="history-axis" x="35" y={y + 3} textAnchor="end">
              {valueLabel(story, mode, value)}
            </text>
          </g>;
        })}
        <path className="history-price-area" d={plot.area} />
        <path className="history-price-line" d={plot.path} />
        {events.map((event, index) => {
          const nearest = points.reduce(
            (best, point) => Math.abs(time(point.date) - time(event.date))
              < Math.abs(time(best.date) - time(event.date)) ? point : best,
            points[0],
          );
          const x = plot.x(nearest.date);
          const y = plot.y(nearest.value);
          const cardX = Math.min(width - 137, Math.max(48, x - 58));
          const cardY = 25 + (index % 3) * 63;
          return <g key={`${event.date}-${event.title}`} className="history-event">
            <line x1={x} x2={x} y1={cardY + 43} y2={y} />
            <circle cx={x} cy={y} r="3" />
            <rect x={cardX} y={cardY} width="126" height="45" rx="4" />
            <text x={cardX + 9} y={cardY + 13}>
              <tspan className="event-date">{shortDate(event.date)}</tspan>
              <tspan className="event-title" x={cardX + 9} dy="13">{event.title}</tspan>
              <tspan className="event-detail" x={cardX + 9} dy="10">
                {event.detail.slice(0, 28)}{event.detail.length > 28 ? "…" : ""}
              </tspan>
            </text>
          </g>;
        })}
        {labels.map((point) => <text
          key={point.date}
          className="history-axis"
          x={plot.x(point.date)}
          y={chartHeight - 8}
          textAnchor="middle"
        >
          {shortDate(point.date)}
        </text>)}
        {last && <circle
          className="history-last-dot"
          cx={plot.x(last.date)}
          cy={plot.y(last.value)}
          r="4"
        />}
        {hoveredPoint && <g className="story-chart-hover-layer" aria-hidden="true">
          <line className="story-chart-crosshair" x1={plot.x(hoveredPoint.date)} x2={plot.x(hoveredPoint.date)} y1="18" y2={chartHeight - 30} />
          <circle className="history-hover-dot" cx={plot.x(hoveredPoint.date)} cy={plot.y(hoveredPoint.value)} r="4.5" />
        </g>}
        <rect
          className="story-chart-hit-area"
          data-chart-hit="stock-history"
          x="42"
          y="18"
          width={width - 58}
          height={chartHeight - 48}
          onMouseMove={(event) => {
            const bounds = event.currentTarget.getBoundingClientRect();
            const ratio = Math.max(0, Math.min(1, (event.clientX - bounds.left) / Math.max(1, bounds.width)));
            const index = Math.round(ratio * (points.length - 1));
            setHoveredIndex(index);
            const point = points[index];
            onHover(point ? { ...point, mode } : null);
          }}
          onMouseLeave={() => { setHoveredIndex(null); onHover(null); }}
        />
      </svg>
      {hoveredPoint && <div className="story-chart-tooltip history-chart-tooltip" data-chart-tooltip="stock-history" style={{ left: `${Math.max(12, Math.min(82, (hoveredIndex! / Math.max(1, points.length - 1)) * 100))}%` }}>
        <strong>{fullDate(hoveredPoint.date)}</strong>
        <span><i className="company" />{modes.find((item) => item.key === mode)?.label}<b>{valueLabel(story, mode, hoveredPoint.value)}</b></span>
        {(mode === "price" || mode === "total_return") && <span><i />Volume<b>{compact(hoveredPoint.volume ?? null, "volume")}</b></span>}
      </div>}
      {(mode === "price" || mode === "total_return") && <div className="history-volume">
        <span>Volume</span>
        <svg viewBox={`0 0 ${width} 48`} aria-label="Trading volume">
          {points.map((point, index) => {
            const x = 42 + (index / Math.max(1, points.length - 1)) * (width - 58);
            const barHeight = ((point.volume ?? 0) / volumeMax) * 38;
            const prior = points[index - 1];
            return <line
              key={point.date}
              className={prior && (point.close ?? point.value) < (prior.close ?? prior.value)
                ? "down"
                : "up"}
              x1={x}
              x2={x}
              y1={45}
              y2={45 - barHeight}
            />;
          })}
        </svg>
      </div>}
    </> : <div className="history-chart-empty">
      This mode has fewer than two TaRaShaData observations in the selected range.
    </div>}
  </div>;
}

function compact(value: number | null, kind: "currency" | "shares" | "volume") {
  if (value === null || !Number.isFinite(value)) return "—";
  const prefix = kind === "currency" ? "$" : "";
  if (Math.abs(value) >= 1_000_000_000_000) {
    return `${prefix}${(value / 1_000_000_000_000).toFixed(2)}T`;
  }
  if (Math.abs(value) >= 1_000_000_000) {
    return `${prefix}${(value / 1_000_000_000).toFixed(kind === "shares" ? 2 : 1)}B`;
  }
  if (Math.abs(value) >= 1_000_000) {
    return `${prefix}${(value / 1_000_000).toFixed(1)}M`;
  }
  if (Math.abs(value) >= 1000) return `${prefix}${(value / 1000).toFixed(1)}K`;
  return `${prefix}${value.toFixed(0)}`;
}

function HistorySideRail({
  story,
  range,
  hover,
}: {
  story: CompanyStockHistoryStory;
  range: StockHistoryRange;
  hover: HistoryHover;
}) {
  const performance = story.performance[range];
  const relative = performance?.totalReturnPercent !== null
    && performance?.totalReturnPercent !== undefined
    && performance.sp500TotalReturnPercent !== null
    ? performance.totalReturnPercent - performance.sp500TotalReturnPercent
    : null;
  return <aside className="history-side-rail">
    {hover && <section className="history-hover-summary">
      <h3>Hovered observation</h3>
      <dl>
        <div><dt>Date</dt><dd>{fullDate(hover.date)}</dd></div>
        <div><dt>{modes.find((item) => item.key === hover.mode)?.label}</dt><dd>{valueLabel(story, hover.mode, hover.value)}</dd></div>
        {(hover.mode === "price" || hover.mode === "total_return") && <div><dt>Volume</dt><dd>{compact(hover.volume ?? null, "volume")}</dd></div>}
      </dl>
    </section>}
    <section>
      <h3>Performance summary</h3>
      <dl>
        <div><dt>Price Change ({range})</dt><dd className={(performance?.priceChangePercent ?? 0) < 0 ? "negative" : "positive"}>{percent(performance?.priceChangePercent, true)}</dd></div>
        <div><dt>CAGR ({range})</dt><dd className={(performance?.cagrPercent ?? 0) < 0 ? "negative" : "positive"}>{percent(performance?.cagrPercent, true)}</dd></div>
        <div><dt>vs S&amp;P 500 ({range})</dt><dd className={relative !== null && relative < 0 ? "negative" : "positive"}>{percent(relative, true)}</dd></div>
        <div><dt>{range} High</dt><dd>{performance?.high === null || performance?.high === undefined ? "—" : `$${performance.high.toFixed(2)}`}</dd></div>
        <div><dt>{range} Low</dt><dd>{performance?.low === null || performance?.low === undefined ? "—" : `$${performance.low.toFixed(2)}`}</dd></div>
      </dl>
    </section>
    <section>
      <h3>Price at a glance</h3>
      <dl>
        <div><dt>Current Price</dt><dd>{story.glance.currentPrice === null ? "—" : `$${story.glance.currentPrice.toFixed(2)}`}</dd></div>
        <div><dt>Market Cap</dt><dd>{compact(story.glance.marketCap, "currency")}</dd></div>
        <div><dt>Shares Outstanding</dt><dd>{compact(story.glance.dilutedShares, "shares")}</dd></div>
        <div><dt>Avg. Daily Volume (3M)</dt><dd>{compact(story.glance.averageDailyVolume3m, "volume")}</dd></div>
      </dl>
    </section>
  </aside>;
}

function indexedSeries(story: CompanyStockHistoryStory, range: StockHistoryRange) {
  const prices = selectedPrices(story, range).map((point) => ({
    date: point.date,
    value: point.close,
  }));
  const candidates = [
    { key: "price", label: "Price", color: "#c1811d", points: prices },
    {
      key: "revenue",
      label: "Revenue",
      color: "#4e7eb5",
      points: selectedFundamentals(story.fundamentals.revenue.points, story, range),
    },
    {
      key: "eps",
      label: "EPS",
      color: "#55a56f",
      points: selectedFundamentals(story.fundamentals.eps.points, story, range),
    },
    {
      key: "free-cash-flow",
      label: "Free Cash Flow",
      color: "#7658bd",
      points: selectedFundamentals(story.fundamentals.freeCashFlow.points, story, range),
    },
  ];
  return candidates.flatMap((series) => {
    const first = series.points[0];
    if (!first || first.value <= 0 || series.points.length < 2) return [];
    const points = series.points.map((point) => ({
      date: point.date,
      value: (point.value / first.value) * 100,
    }));
    const last = points.at(-1)!;
    const years = (time(last.date) - time(first.date)) / millisecondsPerYear;
    const cagr = years > 0 ? ((last.value / 100) ** (1 / years) - 1) * 100 : null;
    return [{ ...series, points, cagr }];
  });
}

function IndexedFundamentalsChart({
  story,
  range,
}: {
  story: CompanyStockHistoryStory;
  range: StockHistoryRange;
}) {
  const [hoveredTime, setHoveredTime] = useState<number | null>(null);
  const series = indexedSeries(story, range);
  const allPoints = series.flatMap((item) => item.points);
  const width = 760;
  const height = 170;
  const inset = { left: 38, right: 18, top: 15, bottom: 24 };
  const plot = linePlot(allPoints, width, height, inset);
  const period = story.performance[range];
  const sharedY = (value: number) => inset.top
    + ((plot.maximum - value) / Math.max(0.0001, plot.maximum - plot.minimum))
      * (height - inset.top - inset.bottom);
  const minimumTime = allPoints.length ? Math.min(...allPoints.map((point) => time(point.date))) : 0;
  const maximumTime = allPoints.length ? Math.max(...allPoints.map((point) => time(point.date))) : 1;
  const hoverRatio = hoveredTime === null ? null : (hoveredTime - minimumTime) / Math.max(1, maximumTime - minimumTime);
  const hoverValues = hoveredTime === null ? [] : series.map((item) => ({
    item,
    point: item.points.reduce((nearest, point) => Math.abs(time(point.date) - hoveredTime) < Math.abs(time(nearest.date) - hoveredTime) ? point : nearest, item.points[0]),
  }));
  const hoverDate = hoverValues.find(({ item }) => item.key === "price")?.point.date ?? hoverValues[0]?.point.date;
  return <div className="history-index-chart">
    <header>
      <h3>Price performance vs key fundamentals <span>(Indexed to 100)</span></h3>
      <div>{series.map((item) => <span key={item.key}>
        <i style={{ background: item.color }} />{item.label}
      </span>)}</div>
    </header>
    {series.length ? <svg
      viewBox={`0 0 ${width} ${height}`}
      role="img"
      aria-label="Price performance versus fundamentals indexed to 100"
    >
      {[100, 200, 300, 400]
        .filter((value) => value >= plot.minimum && value <= plot.maximum)
        .map((value) => <g key={value}>
          <line className="history-grid" x1="38" x2={width - 18} y1={sharedY(value)} y2={sharedY(value)} />
          <text className="history-axis" x="31" y={sharedY(value) + 3} textAnchor="end">{value}</text>
        </g>)}
      {series.map((item) => {
        const path = item.points.map(
          (point, index) => `${index ? "L" : "M"}${plot.x(point.date).toFixed(2)},${sharedY(point.value).toFixed(2)}`,
        ).join(" ");
        return <g key={item.key}>
          <path className="history-index-line" d={path} stroke={item.color} />
          <circle
            cx={plot.x(item.points.at(-1)!.date)}
            cy={sharedY(item.points.at(-1)!.value)}
            r="2.5"
            fill={item.color}
          />
        </g>;
      })}
      {period && [0, 0.25, 0.5, 0.75, 1].map((ratio) => {
        const day = new Date(
          time(period.startDate) + ratio * (time(period.endDate) - time(period.startDate)),
        ).toISOString().slice(0, 10);
        return <text
          key={ratio}
          className="history-axis"
          x={38 + ratio * (width - 56)}
          y={height - 6}
          textAnchor="middle"
        >
          {shortDate(day)}
        </text>;
      })}
      {hoverRatio !== null && <g className="story-chart-hover-layer" aria-hidden="true">
        <line className="story-chart-crosshair" x1={inset.left + hoverRatio * (width - inset.left - inset.right)} x2={inset.left + hoverRatio * (width - inset.left - inset.right)} y1={inset.top} y2={height - inset.bottom} />
        {hoverValues.map(({ item, point }) => <circle key={item.key} className="history-index-hover-dot" cx={plot.x(point.date)} cy={sharedY(point.value)} r="3.8" fill={item.color} />)}
      </g>}
      <rect
        className="story-chart-hit-area"
        data-chart-hit="indexed-fundamentals"
        x={inset.left}
        y={inset.top}
        width={width - inset.left - inset.right}
        height={height - inset.top - inset.bottom}
        onMouseMove={(event) => {
          const bounds = event.currentTarget.getBoundingClientRect();
          const ratio = Math.max(0, Math.min(1, (event.clientX - bounds.left) / Math.max(1, bounds.width)));
          setHoveredTime(minimumTime + ratio * (maximumTime - minimumTime));
        }}
        onMouseLeave={() => setHoveredTime(null)}
      />
    </svg> : <div className="history-index-empty">
      Positive price and fundamental bases are required for indexed comparison.
    </div>}
    {hoverRatio !== null && hoverDate && <div className="story-chart-tooltip history-index-tooltip" data-chart-tooltip="indexed-fundamentals" style={{ left: `${Math.max(14, Math.min(72, hoverRatio * 82))}%` }}>
      <strong>{fullDate(hoverDate)}</strong>
      {hoverValues.map(({ item, point }) => <span key={item.key}><i style={{ background: item.color }} />{item.label}<b>{point.value.toFixed(1)}</b></span>)}
    </div>}
    <aside>
      <strong>{hoverDate ? `Indexed values · ${shortDate(hoverDate)}` : `${range} CAGR`}</strong>
      {series.map((item) => <div key={item.key}>
        <span style={{ color: item.color }}>{item.label}</span>
        <b style={{ color: item.color }}>{hoverDate ? hoverValues.find((value) => value.item.key === item.key)?.point.value.toFixed(1) ?? "—" : percent(item.cagr)}</b>
      </div>)}
    </aside>
  </div>;
}

function downloadHistoryData(company: Company, story: CompanyStockHistoryStory) {
  const rows: string[][] = [[
    "series",
    "date",
    "value",
    "secondary_value",
    "volume",
    "source_url",
  ]];
  for (const point of story.prices) {
    rows.push([
      "price",
      point.date,
      String(point.close),
      String(point.adjustedClose),
      point.volume === null ? "" : String(point.volume),
      story.source?.marketUrl ?? "",
    ]);
  }
  for (const [key, series] of Object.entries(story.fundamentals)) {
    for (const point of series.points) {
      rows.push([key, point.date, String(point.value), "", "", point.sourceUrl ?? ""]);
    }
  }
  const csv = rows.map((row) => row.map(
    (cell) => `"${cell.replaceAll('"', '""')}"`,
  ).join(",")).join("\n");
  const url = URL.createObjectURL(new Blob([csv], { type: "text/csv;charset=utf-8" }));
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = `${company.symbol.toLowerCase()}-stock-history.csv`;
  anchor.click();
  URL.revokeObjectURL(url);
}

export function StockHistoryChapter({ company }: { company: Company }) {
  const story = company.companyStory?.stockHistory;
  const available = story?.status === "available"
    && story.prices.length > 1
    && story.availableRanges.length > 0;
  const [range, setRange] = useState<StockHistoryRange>(story?.defaultRange ?? "5Y");
  const [mode, setMode] = useState<StockHistoryMode>("price");
  const [showEvents, setShowEvents] = useState(true);
  const [historyHover, setHistoryHover] = useState<HistoryHover>(null);
  useEffect(() => {
    setRange(story?.defaultRange ?? story?.availableRanges[0] ?? "5Y");
    setMode("price");
    setShowEvents(true);
    setHistoryHover(null);
  }, [company.id, story?.defaultRange, story?.availableRanges]);

  const focusedRangeControls = <div className="story-period-toggle history-range-toggle" role="group" aria-label="Focused stock history period">
    {ranges.map((item) => <button key={item} className={range === item ? "active" : ""} disabled={!story?.availableRanges.includes(item)} onClick={() => setRange(item)} aria-pressed={range === item}>{item}</button>)}
  </div>;
  const primaryFocusControls = <div className="history-focus-controls">
    {focusedRangeControls}
    <div className="history-mode-tabs" role="tablist" aria-label="Focused historical metric">
      {modes.map((item) => <button key={item.key} role="tab" aria-selected={mode === item.key} className={mode === item.key ? "active" : ""} onClick={() => setMode(item.key)}>{item.label}</button>)}
    </div>
  </div>;

  return <section className="story-chapter-page stock-history-page">
    <header className="story-chapter-header stock-history-header">
      <div>
        <span>07</span>
        <hgroup>
          <h2>How the stock got here</h2>
          <p>{available
            ? "Price history and key milestones"
            : "Connect stock-price history to fundamental history"}</p>
        </hgroup>
      </div>
      <div className="history-header-actions">
        <div
          className="story-period-toggle history-range-toggle"
          role="group"
          aria-label="Stock history period"
        >
          {ranges.map((item) => <button
            key={item}
            className={range === item ? "active" : ""}
            disabled={!story?.availableRanges.includes(item)}
            onClick={() => setRange(item)}
            aria-pressed={range === item}
          >
            {item}
          </button>)}
        </div>
        <button
          className="history-download"
          aria-label="Download chart data"
          title="Download the source-linked chart data as CSV"
          disabled={!available || !story}
          onClick={() => story && downloadHistoryData(company, story)}
        >
          ⇩
        </button>
      </div>
    </header>
    {available && story ? <>
      <div className="history-control-row">
        <div className="history-mode-tabs" role="tablist" aria-label="Historical metric">
          {modes.map((item) => <button
            key={item.key}
            role="tab"
            aria-selected={mode === item.key}
            className={mode === item.key ? "active" : ""}
            onClick={() => setMode(item.key)}
          >
            {item.label}
          </button>)}
        </div>
        <label className="history-events-toggle">
          Show events
          <input
            type="checkbox"
            checked={showEvents}
            onChange={(event) => setShowEvents(event.target.checked)}
          />
          <span />
        </label>
      </div>
      <MaximizableStorySection className="history-chart-section history-primary-section" label="Historical performance chart" chapterTitle="How the stock got here" description="Inspect the selected price or fundamental series at an exact historical observation." focusControls={primaryFocusControls} focusMeta={`${modes.find((item) => item.key === mode)?.label} · ${range}`} focusSource="TaRaShaData delayed market history and normalized filings">
        <header className="history-chart-section-header">
          <div><h3>Historical performance</h3><p>{modes.find((item) => item.key === mode)?.label} · {range}</p></div>
        </header>
        <div className="history-main-grid">
          <StockHistoryChart
            story={story}
            range={range}
            mode={mode}
            showEvents={showEvents}
            onHover={setHistoryHover}
          />
          <HistorySideRail story={story} range={range} hover={historyHover} />
        </div>
      </MaximizableStorySection>
      <MaximizableStorySection className="history-chart-section history-comparison-section" label="Price performance vs key fundamentals chart" chapterTitle="How the stock got here" description="Compare price, revenue, earnings, and free cash flow on a common indexed-to-100 basis." focusControls={focusedRangeControls} focusMeta={`${range} · Indexed to 100`} focusSource="TaRaShaData delayed prices and normalized filings">
        <header className="history-chart-section-header">
          <div><h3>Fundamental comparison</h3><p>Price, revenue, EPS, and free cash flow</p></div>
        </header>
        <div className="history-comparison-panel">
          <IndexedFundamentalsChart story={story} range={range} />
          <aside className="history-strategy">
            <h3>Management’s projected strategy</h3>
            {story.strategy.status === "available"
              ? <ul>{story.strategy.points.map((point) => <li key={point}>{point}</li>)}</ul>
              : <p>{story.strategy.reason}</p>}
            <small>{story.strategy.sourceDate
              ? `Management source · ${fullDate(story.strategy.sourceDate)}`
              : "No management forecast inferred"}</small>
            {story.strategy.sourceUrl && <a
              href={story.strategy.sourceUrl}
              target="_blank"
              rel="noreferrer"
            >
              Open management source ↗
            </a>}
          </aside>
        </div>
      </MaximizableStorySection>
      <footer className="history-footnote">
        <span className="stock-risk-info" aria-hidden="true">i</span>
        <div>
          <strong>Sources &amp; derivations</strong>
          <p>
            <b>Financials and events:</b> {story.source?.financials}; {story.source?.filings}.{" "}
            <b>Market:</b> {story.source?.marketName} (delayed, non-authoritative,
            transient). {story.source?.priceBasis}
          </p>
          <p><b>Derivation:</b> {story.methodology}</p>
          <p><b>Strategy:</b> {story.strategy.methodology}</p>
          {story.quality?.warnings.length
            ? <p><b>Availability notes:</b> {story.quality.warnings.join(" ")}</p>
            : null}
        </div>
        <div className="history-source-links">
          {story.source?.marketUrl && <a
            href={story.source.marketUrl}
            target="_blank"
            rel="noreferrer"
          >
            Price history ↗
          </a>}
          {story.source?.benchmarkUrl && <a
            href={story.source.benchmarkUrl}
            target="_blank"
            rel="noreferrer"
          >
            S&amp;P 500 ↗
          </a>}
        </div>
        <small>Past performance is not indicative of future results.</small>
      </footer>
    </> : <div className="story-unavailable">
      <span aria-hidden="true">07</span>
      <div>
        <h3>Historical price context is not available</h3>
        <p>{story?.reason
          ?? "TaRaShaData did not return valid delayed market history for this company."}</p>
        <small>
          No price, return, fundamental comparison, event, or strategy statement has been fabricated.
        </small>
      </div>
    </div>}
  </section>;
}
