import { useEffect, useMemo, useState } from "react";
import { companies, learningCards, metricMeta } from "./data/demo";
import { companySearch, formatMetric, latest, percentChange } from "./domain";
import type { Company, MetricKey, Page, YearValue } from "./types";

const navItems: { page: Page; label: string; icon: string }[] = [
  { page: "home", label: "Home", icon: "⌂" },
  { page: "discover", label: "Discover", icon: "⌕" },
  { page: "compare", label: "Compare", icon: "⇄" },
  { page: "watchlist", label: "Watchlist", icon: "♡" },
  { page: "learn", label: "Learn", icon: "◫" },
];

function App() {
  const [page, setPage] = useState<Page>("home");
  const [activeCompanyId, setActiveCompanyId] = useState(companies[0].id);
  const [watchlist, setWatchlist] = useState<string[]>(() => {
    try {
      return JSON.parse(localStorage.getItem("tarasha-watchlist") ?? "[]");
    } catch {
      return [];
    }
  });

  useEffect(() => localStorage.setItem("tarasha-watchlist", JSON.stringify(watchlist)), [watchlist]);

  const navigate = (next: Page) => {
    setPage(next);
    window.scrollTo({ top: 0, behavior: "smooth" });
  };

  const openCompany = (id: string) => {
    setActiveCompanyId(id);
    navigate("company");
  };

  const toggleWatch = (id: string) => {
    setWatchlist((current) => current.includes(id) ? current.filter((item) => item !== id) : [...current, id]);
  };

  return (
    <div className="app-shell">
      <BetaStrip />
      <Header page={page} navigate={navigate} />
      <main>
        {page === "home" && <Home navigate={navigate} openCompany={openCompany} watchlist={watchlist} toggleWatch={toggleWatch} />}
        {page === "discover" && <Discover openCompany={openCompany} watchlist={watchlist} toggleWatch={toggleWatch} />}
        {page === "company" && <CompanyDetail company={companies.find((item) => item.id === activeCompanyId) ?? companies[0]} watched={watchlist.includes(activeCompanyId)} toggleWatch={toggleWatch} navigate={navigate} />}
        {page === "compare" && <Compare openCompany={openCompany} />}
        {page === "watchlist" && <Watchlist ids={watchlist} openCompany={openCompany} toggleWatch={toggleWatch} navigate={navigate} />}
        {page === "learn" && <Learn />}
      </main>
      <Footer navigate={navigate} />
      <MobileNav page={page} navigate={navigate} />
    </div>
  );
}

function BetaStrip() {
  return <div className="beta-strip"><span>Private founding-user preview</span><span>Fictional companies · Illustrative data</span></div>;
}

function Brand() {
  return (
    <div className="brand" aria-label="TaRaSha home">
      <span className="brand-mark">T</span>
      <span><strong>TaRaSha</strong><small>Company Lens</small></span>
    </div>
  );
}

function Header({ page, navigate }: { page: Page; navigate: (page: Page) => void }) {
  return (
    <header className="site-header">
      <button className="brand-button" onClick={() => navigate("home")}><Brand /></button>
      <nav className="desktop-nav" aria-label="Main navigation">
        {navItems.map((item) => (
          <button key={item.page} className={page === item.page || (page === "company" && item.page === "discover") ? "active" : ""} onClick={() => navigate(item.page)}>{item.label}</button>
        ))}
      </nav>
      <button className="beta-avatar" aria-label="Founding user account">TU</button>
    </header>
  );
}

function MobileNav({ page, navigate }: { page: Page; navigate: (page: Page) => void }) {
  return (
    <nav className="mobile-nav" aria-label="Mobile navigation">
      {navItems.map((item) => (
        <button key={item.page} className={page === item.page || (page === "company" && item.page === "discover") ? "active" : ""} onClick={() => navigate(item.page)}>
          <span>{item.icon}</span>{item.label}
        </button>
      ))}
    </nav>
  );
}

function Home({ navigate, openCompany, watchlist, toggleWatch }: {
  navigate: (page: Page) => void;
  openCompany: (id: string) => void;
  watchlist: string[];
  toggleWatch: (id: string) => void;
}) {
  return (
    <>
      <section className="hero">
        <div className="hero-copy">
          <p className="eyebrow">Company fundamentals, made human</p>
          <h1>Understand the business behind the numbers.</h1>
          <p className="hero-text">Explore reported sales, profitability, cash and debt in language that does not require a finance degree.</p>
          <div className="hero-actions">
            <button className="button primary" onClick={() => navigate("discover")}>Explore companies <span>→</span></button>
            <button className="button secondary" onClick={() => navigate("learn")}>Learn the basics</button>
          </div>
          <div className="trust-row">
            <span>✓ No stock tips</span><span>✓ No price targets</span><span>✓ Sources shown</span>
          </div>
        </div>
        <HeroCard />
      </section>

      <section className="content-section intro-section">
        <div>
          <p className="eyebrow">Start with four simple questions</p>
          <h2>A clearer way to read a company</h2>
        </div>
        <p className="section-lede">We organise reported numbers around the questions people naturally ask—not around accounting statements.</p>
        <div className="question-grid">
          <QuestionCard number="01" color="mint" title="Is the business growing?" text="Follow sales over time, not a single quarter." />
          <QuestionCard number="02" color="gold" title="Is it profitable?" text="See how much of every ₹100 remains after operating costs." />
          <QuestionCard number="03" color="blue" title="Does profit become cash?" text="Check whether reported profit is supported by cash generation." />
          <QuestionCard number="04" color="coral" title="How much pressure does debt add?" text="View borrowings alongside cash and the direction of travel." />
        </div>
      </section>

      <section className="content-section soft-section">
        <div className="section-heading-row">
          <div><p className="eyebrow">Explore the demo</p><h2>Recently updated</h2></div>
          <button className="text-button" onClick={() => navigate("discover")}>View all companies →</button>
        </div>
        <div className="company-grid">
          {companies.slice(0, 3).map((company) => <CompanyCard key={company.id} company={company} openCompany={openCompany} watched={watchlist.includes(company.id)} toggleWatch={toggleWatch} />)}
        </div>
      </section>

      <section className="content-section learning-preview">
        <div><p className="eyebrow">Learn as you explore</p><h2>Small lessons. Useful context.</h2><p>Every metric includes a plain-language explanation, and the learning library goes one step deeper.</p><button className="button secondary" onClick={() => navigate("learn")}>Visit learning library</button></div>
        <div className="mini-lessons">
          {learningCards.slice(0, 3).map((card, index) => <div className="mini-lesson" key={card.id}><span>0{index + 1}</span><div><small>{card.time} read</small><strong>{card.title}</strong></div></div>)}
        </div>
      </section>
    </>
  );
}

function HeroCard() {
  const series = companies[0].metrics.revenue;
  return (
    <div className="hero-card">
      <div className="hero-card-head"><span className="company-monogram">A</span><div><strong>Aarohan Consumer</strong><small>Illustrative company</small></div><span className="fresh-badge">Updated</span></div>
      <div className="hero-metric"><span>Revenue</span><strong>₹5,780 cr</strong><small>FY 2026</small></div>
      <LineChart series={series} color="#f2bb62" compact />
      <div className="hero-insight"><span>In plain language</span><p>Sales increased in each of the last five reported years.</p></div>
    </div>
  );
}

function QuestionCard({ number, color, title, text }: { number: string; color: string; title: string; text: string }) {
  return <article className={`question-card ${color}`}><span>{number}</span><h3>{title}</h3><p>{text}</p></article>;
}

function Discover({ openCompany, watchlist, toggleWatch }: { openCompany: (id: string) => void; watchlist: string[]; toggleWatch: (id: string) => void }) {
  const [query, setQuery] = useState("");
  const [sector, setSector] = useState("All sectors");
  const sectors = ["All sectors", ...Array.from(new Set(companies.map((company) => company.sector)))];
  const filtered = companies.filter((company) => companySearch(company, query, sector));
  return (
    <div className="page-wrap">
      <PageIntro eyebrow="Discover" title="Find a company. Follow the facts." text="Search by company, symbol or sector. Results are alphabetical—not ranked or recommended." />
      <div className="search-panel">
        <label className="search-box"><span>⌕</span><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Search company or symbol" /></label>
        <select value={sector} onChange={(event) => setSector(event.target.value)} aria-label="Filter by sector">{sectors.map((item) => <option key={item}>{item}</option>)}</select>
      </div>
      <div className="result-meta"><strong>{filtered.length} illustrative companies</strong><span>Sorted A–Z</span></div>
      {filtered.length ? <div className="company-grid wide">{filtered.sort((a, b) => a.name.localeCompare(b.name)).map((company) => <CompanyCard key={company.id} company={company} openCompany={openCompany} watched={watchlist.includes(company.id)} toggleWatch={toggleWatch} />)}</div> : <Empty title="No companies found" text="Try a different company name or choose all sectors." />}
    </div>
  );
}

function CompanyCard({ company, openCompany, watched, toggleWatch }: { company: Company; openCompany: (id: string) => void; watched: boolean; toggleWatch: (id: string) => void }) {
  const revenue = company.metrics.revenue;
  const change = percentChange(revenue);
  return (
    <article className="company-card">
      <button className={`watch-button ${watched ? "watched" : ""}`} onClick={() => toggleWatch(company.id)} aria-label={watched ? "Remove from watchlist" : "Add to watchlist"}>{watched ? "♥" : "♡"}</button>
      <button className="card-main" onClick={() => openCompany(company.id)}>
        <div className="company-heading"><span className="company-monogram">{company.name[0]}</span><div><h3>{company.name}</h3><span>{company.symbol} · {company.sector}</span></div></div>
        <p>{company.description}</p>
        <div className="card-metric"><span>Latest reported revenue</span><strong>{formatMetric("revenue", latest(revenue).value)}</strong><small>{change === null ? "—" : `${change >= 0 ? "+" : ""}${change.toFixed(1)}%`} from prior year</small></div>
        <div className="card-footer"><span>Data through {company.reportingPeriod}</span><strong>View company →</strong></div>
      </button>
    </article>
  );
}

function CompanyDetail({ company, watched, toggleWatch, navigate }: { company: Company; watched: boolean; toggleWatch: (id: string) => void; navigate: (page: Page) => void }) {
  const [metric, setMetric] = useState<MetricKey>("revenue");
  const series = company.metrics[metric];
  const notes: Record<MetricKey, string> = { revenue: company.notes.growth, operatingMargin: company.notes.profitability, freeCashFlow: company.notes.cash, netDebt: company.notes.debt };
  return (
    <div className="page-wrap company-page">
      <button className="back-button" onClick={() => navigate("discover")}>← Back to discover</button>
      <section className="company-profile">
        <div className="company-title"><span className="company-monogram large">{company.name[0]}</span><div><p>{company.symbol} · Illustrative company</p><h1>{company.name}</h1><span>{company.sector}</span></div></div>
        <button className={`button ${watched ? "secondary" : "primary"}`} onClick={() => toggleWatch(company.id)}>{watched ? "♥ In your watchlist" : "♡ Add to watchlist"}</button>
      </section>
      <p className="company-description">{company.description}</p>
      <div className="company-facts"><span>Founded <strong>{company.founded}</strong></span><span>Employees <strong>{company.employees}</strong></span><span>Latest period <strong>{company.reportingPeriod}</strong></span><span>Updated <strong>{company.updatedAt}</strong></span></div>

      <section className="metric-overview">
        <p className="eyebrow">The four-question view</p><h2>What do the reported numbers show?</h2>
        <div className="metric-tabs" role="tablist">
          {(Object.keys(metricMeta) as MetricKey[]).map((key) => {
            const itemSeries = company.metrics[key];
            return <button role="tab" aria-selected={metric === key} className={metric === key ? "active" : ""} key={key} onClick={() => setMetric(key)}><small>{metricMeta[key].short}</small><strong>{formatMetric(key, latest(itemSeries).value)}</strong><span>{metricMeta[key].label}</span></button>;
          })}
        </div>
        <div className="metric-detail">
          <div className="chart-column">
            <div className="chart-title"><div><span>{metricMeta[metric].label}</span><strong>{formatMetric(metric, latest(series).value)}</strong></div><small>{company.currency} · Annual</small></div>
            <LineChart series={series} color={metric === "revenue" ? "#146c5b" : metric === "operatingMargin" ? "#c98722" : metric === "freeCashFlow" ? "#397ca8" : "#ce6f59"} />
          </div>
          <aside className="plain-insight"><span>In plain language</span><h3>{notes[metric]}</h3><p>{metricMeta[metric].explanation}</p><details><summary>How this metric is calculated</summary><p>This preview uses a simplified illustrative series. Production data will show the exact formula, source filing and any adjustments.</p></details></aside>
        </div>
      </section>
      <DataTrust company={company} />
    </div>
  );
}

function LineChart({ series, color, compact = false }: { series: YearValue[]; color: string; compact?: boolean }) {
  const width = 520;
  const height = compact ? 140 : 240;
  const pad = compact ? 12 : 28;
  const values = series.map((item) => item.value);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const spread = max - min || 1;
  const points = series.map((item, index) => ({ x: pad + (index * (width - pad * 2)) / (series.length - 1), y: height - pad - ((item.value - min) / spread) * (height - pad * 2), ...item }));
  const line = points.map((point) => `${point.x},${point.y}`).join(" ");
  const area = `${pad},${height - pad} ${line} ${width - pad},${height - pad}`;
  return (
    <div className={`line-chart ${compact ? "compact" : ""}`}>
      <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Five-year trend chart">
        <defs><linearGradient id={`fade-${series[0].value}-${color.replace("#", "")}`} x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor={color} stopOpacity="0.25" /><stop offset="100%" stopColor={color} stopOpacity="0" /></linearGradient></defs>
        {!compact && [0.25, 0.5, 0.75].map((ratio) => <line key={ratio} x1={pad} x2={width - pad} y1={height * ratio} y2={height * ratio} stroke="#dfe5e1" strokeDasharray="4 6" />)}
        <polygon points={area} fill={`url(#fade-${series[0].value}-${color.replace("#", "")})`} />
        <polyline points={line} fill="none" stroke={color} strokeWidth={compact ? 4 : 3} strokeLinecap="round" strokeLinejoin="round" />
        {points.map((point) => <circle key={point.year} cx={point.x} cy={point.y} r={compact ? 3.5 : 4.5} fill="#fff" stroke={color} strokeWidth="3" />)}
      </svg>
      {!compact && <div className="chart-years">{series.map((item) => <span key={item.year}>FY {String(item.year).slice(-2)}</span>)}</div>}
    </div>
  );
}

function DataTrust({ company }: { company: Company }) {
  return (
    <section className="data-trust">
      <div className="trust-icon">✓</div><div><p className="eyebrow">Know where the number came from</p><h2>Data you can trace</h2><p>This initial version uses fictional companies and illustrative numbers. Production company pages will link every figure to its source filing and preserve revisions.</p></div>
      <div className="source-card"><span>Dataset</span><strong>TaRaSha illustrative preview</strong><span>Reporting period</span><strong>{company.reportingPeriod}</strong><span>Last refreshed</span><strong>{company.updatedAt}</strong></div>
    </section>
  );
}

function Compare({ openCompany }: { openCompany: (id: string) => void }) {
  const [selected, setSelected] = useState<string[]>(companies.slice(0, 3).map((company) => company.id));
  const selectedCompanies = companies.filter((company) => selected.includes(company.id));
  const toggle = (id: string) => setSelected((current) => current.includes(id) ? current.filter((item) => item !== id) : current.length < 3 ? [...current, id] : current);
  return (
    <div className="page-wrap">
      <PageIntro eyebrow="Compare" title="Put the same facts side by side." text="Choose up to three companies. We use matching periods and definitions wherever possible." />
      <div className="compare-picker">{companies.map((company) => <button className={selected.includes(company.id) ? "selected" : ""} disabled={!selected.includes(company.id) && selected.length >= 3} onClick={() => toggle(company.id)} key={company.id}><span>{selected.includes(company.id) ? "✓" : "+"}</span>{company.name}</button>)}</div>
      {selectedCompanies.length < 2 ? <Empty title="Choose at least two companies" text="Comparison becomes available after you select a second company." /> : <ComparisonTable companies={selectedCompanies} openCompany={openCompany} />}
    </div>
  );
}

function ComparisonTable({ companies: selected, openCompany }: { companies: Company[]; openCompany: (id: string) => void }) {
  return (
    <div className="comparison-wrap">
      <div className="comparison-note"><strong>Remember:</strong> A comparison describes differences. It does not decide which company is suitable for anyone.</div>
      <div className="comparison-table" style={{ "--company-count": selected.length } as React.CSSProperties}>
        <div className="comparison-row company-row"><span>Company</span>{selected.map((company) => <button key={company.id} onClick={() => openCompany(company.id)}><span className="company-monogram">{company.name[0]}</span><strong>{company.name}</strong><small>{company.sector}</small></button>)}</div>
        {(Object.keys(metricMeta) as MetricKey[]).map((key) => <div className="comparison-row" key={key}><span><strong>{metricMeta[key].label}</strong><small>{metricMeta[key].explanation}</small></span>{selected.map((company) => { const value = latest(company.metrics[key]); return <div key={company.id}><strong>{formatMetric(key, value.value)}</strong><small>{company.reportingPeriod}</small></div>; })}</div>)}
      </div>
    </div>
  );
}

function Watchlist({ ids, openCompany, toggleWatch, navigate }: { ids: string[]; openCompany: (id: string) => void; toggleWatch: (id: string) => void; navigate: (page: Page) => void }) {
  const watched = companies.filter((company) => ids.includes(company.id));
  return (
    <div className="page-wrap">
      <PageIntro eyebrow="Watchlist" title="Keep the companies you follow in one place." text="This list organises factual updates. It does not generate alerts to buy, sell or hold." />
      {!watched.length ? <div className="large-empty"><span>♡</span><h2>Your watchlist is empty</h2><p>Add an illustrative company to see its latest reported period here.</p><button className="button primary" onClick={() => navigate("discover")}>Discover companies</button></div> : <><div className="watch-summary"><strong>{watched.length} of 10 beta watchlist places used</strong><span>Saved only on this device in the initial preview</span></div><div className="company-grid wide">{watched.map((company) => <CompanyCard key={company.id} company={company} openCompany={openCompany} watched toggleWatch={toggleWatch} />)}</div></>}
    </div>
  );
}

function Learn() {
  const [openId, setOpenId] = useState<string | null>(null);
  const active = learningCards.find((item) => item.id === openId);
  return (
    <div className="page-wrap learn-page">
      <PageIntro eyebrow="Learning library" title="Build understanding, one idea at a time." text="Short, practical lessons explain what company numbers mean—and where their limits are." />
      <div className="lesson-grid">{learningCards.map((card, index) => <button className="lesson-card" key={card.id} onClick={() => setOpenId(card.id)}><span className="lesson-number">0{index + 1}</span><small>{card.time} read</small><h3>{card.title}</h3><p>{card.text}</p><strong>Open lesson →</strong></button>)}</div>
      <section className="glossary"><div><p className="eyebrow">Quick glossary</p><h2>Finance words, translated</h2></div><div className="glossary-list">{Object.values(metricMeta).map((metric) => <details key={metric.label}><summary>{metric.label}<span>+</span></summary><p>{metric.explanation}</p></details>)}</div></section>
      {active && <div className="lesson-modal" role="dialog" aria-modal="true" aria-label={active.title}><button className="modal-backdrop" onClick={() => setOpenId(null)} aria-label="Close lesson" /><article><button className="modal-close" onClick={() => setOpenId(null)}>×</button><p className="eyebrow">{active.time} lesson</p><h2>{active.title}</h2><p>{active.text}</p><h3>What to look for</h3><p>Look at several reporting periods, read the source notes, and compare like with like. One number rarely explains an entire business.</p><h3>What it cannot tell you</h3><p>A historical metric cannot predict a share price or determine whether a security is appropriate for you.</p><button className="button primary" onClick={() => setOpenId(null)}>Done</button></article></div>}
    </div>
  );
}

function PageIntro({ eyebrow, title, text }: { eyebrow: string; title: string; text: string }) {
  return <section className="page-intro"><p className="eyebrow">{eyebrow}</p><h1>{title}</h1><p>{text}</p></section>;
}

function Empty({ title, text }: { title: string; text: string }) {
  return <div className="empty"><strong>{title}</strong><span>{text}</span></div>;
}

function Footer({ navigate }: { navigate: (page: Page) => void }) {
  return (
    <footer>
      <div><Brand /><p>Plain-language company fundamentals for curious investors.</p></div>
      <div><strong>Explore</strong><button onClick={() => navigate("discover")}>Companies</button><button onClick={() => navigate("compare")}>Compare</button><button onClick={() => navigate("learn")}>Learn</button></div>
      <div><strong>Important</strong><p>TaRaSha Company Lens is an educational information platform, not an investment adviser or research analyst. It does not provide recommendations, price targets or suitability assessments.</p></div>
      <div className="footer-bottom"><span>© 2026 TaRaSha · Founding-user preview</span><span>Illustrative data · Not for investment decisions</span></div>
    </footer>
  );
}

export default App;
