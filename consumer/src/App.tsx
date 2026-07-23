import { useEffect, useRef, useState } from "react";
import { learningCards, metricMeta } from "./data/demo";
import { authenticateUser, getSecurityQuestion, registerUser, resetPassword, SECURITY_QUESTIONS } from "./authStore";
import type { AuthenticatedUser } from "./authStore";
import { formatMetric, latest, percentChange } from "./domain";
import { liveDataEnabled, MAX_SESSION_COMPANIES, MAX_YEAR_RANGE, pullCompanyResearch, recalculateIndustryConstituents, searchCompanyCatalog } from "./liveData";
import { arithmeticMean, extremeOutlierBounds, isExtremeOutlier, sampleStandardDeviation } from "./statistics";
import type { CatalogCompany, Company, DistributionObservation, EarningsFlowMetricKey, EarningsFlowYear, GrowthComparison, IndustryConstituent, IndustryDeltaPoint, IndustryLevelPoint, MetricKey, Page, PerformanceThresholds, ProfitabilityMetricBands, ProfitabilityMetricKey, ProfitabilityYearPoint, RawIncomePoint, ResearchShelfAnalysis, StatementFact, StatementGroup, ValuationMetricKey, YearValue } from "./types";
import researchJourneyHero from "./assets/research-journey-hero-v2.jpg";
import tarashaLogo from "./assets/tarasha-logo.png";

const navItems: { page: Page; label: string; icon: string }[] = [
  { page: "discover", label: "Discover", icon: "⌕" },
  { page: "compare", label: "Compare", icon: "⇄" },
  { page: "watchlist", label: "Watchlist", icon: "♡" },
  { page: "learn", label: "Learn", icon: "◫" },
];

const SESSION_USER_KEY = "tarasha-auth-session";
const protectedPages = new Set<Page>(["discover", "company", "compare", "watchlist", "learn"]);

function pageFromHash(): Page {
  const value = window.location.hash.replace(/^#\/?/, "") as Page;
  return ["home", "login", "discover", "compare", "watchlist", "learn"].includes(value) ? value : "home";
}

function sessionUser(): AuthenticatedUser | null {
  try {
    const value = JSON.parse(sessionStorage.getItem(SESSION_USER_KEY) ?? "null");
    return value && typeof value.name === "string" && typeof value.username === "string" ? value : null;
  } catch {
    return null;
  }
}

function App() {
  const [page, setPage] = useState<Page>(pageFromHash);
  const [authUser, setAuthUser] = useState<AuthenticatedUser | null>(sessionUser);
  const [activeCompanyId, setActiveCompanyId] = useState<string | null>(null);
  const [sessionCompanies, setSessionCompanies] = useState<Company[]>([]);
  const [watchlist, setWatchlist] = useState<string[]>(() => {
    try {
      return JSON.parse(localStorage.getItem("tarasha-watchlist") ?? "[]");
    } catch {
      return [];
    }
  });

  useEffect(() => localStorage.setItem("tarasha-watchlist", JSON.stringify(watchlist)), [watchlist]);
  useEffect(() => {
    const onHashChange = () => {
      const requestedPage = pageFromHash();
      if (!authUser && protectedPages.has(requestedPage)) {
        setPage("login");
        window.history.replaceState(null, "", "#/login");
      } else {
        setPage(requestedPage);
      }
    };
    onHashChange();
    window.addEventListener("hashchange", onHashChange);
    return () => window.removeEventListener("hashchange", onHashChange);
  }, [authUser]);

  const navigate = (next: Page) => {
    const destination = !authUser && protectedPages.has(next) ? "login" : next;
    setPage(destination);
    if (destination !== "company") window.history.pushState(null, "", `#/${destination}`);
    window.scrollTo({ top: 0, behavior: "smooth" });
  };

  const completeAuthentication = (user: AuthenticatedUser) => {
    sessionStorage.setItem(SESSION_USER_KEY, JSON.stringify(user));
    setAuthUser(user);
    setPage("discover");
    window.history.pushState(null, "", "#/discover");
    window.scrollTo({ top: 0, behavior: "smooth" });
  };

  const logout = () => {
    sessionStorage.removeItem(SESSION_USER_KEY);
    setAuthUser(null);
    setSessionCompanies([]);
    setActiveCompanyId(null);
    setPage("home");
    window.history.pushState(null, "", "#/home");
    window.scrollTo({ top: 0, behavior: "smooth" });
  };

  const openCompany = (id: string) => {
    setActiveCompanyId(id);
    navigate("company");
  };

  const toggleWatch = (id: string) => {
    setWatchlist((current) => current.includes(id) ? current.filter((item) => item !== id) : [...current, id]);
  };

  const addSessionCompany = (company: Company) => {
    setSessionCompanies((current) => [...current.filter((item) => item.id !== company.id), company]);
  };

  const updateSessionCompany = (company: Company) => {
    setSessionCompanies((current) => current.map((item) => item.id === company.id ? company : item));
  };

  const removeSessionCompany = (id: string) => {
    setSessionCompanies((current) => current.filter((item) => item.id !== id));
    setActiveCompanyId((current) => current === id ? null : current);
  };

  const activeCompany = sessionCompanies.find((item) => item.id === activeCompanyId);
  const visiblePage = !authUser && protectedPages.has(page) ? "login" : page;

  return (
    <div className="app-shell">
      <BetaStrip />
      <Header page={visiblePage} navigate={navigate} user={authUser} logout={logout} />
      <main>
        {visiblePage === "home" && <Home navigate={navigate} isAuthenticated={Boolean(authUser)} />}
        {visiblePage === "login" && <LoginPage onAuthenticated={completeAuthentication} navigate={navigate} />}
        {authUser && visiblePage === "discover" && <Discover sessionCompanies={sessionCompanies} onResearchPulled={addSessionCompany} onCompanyUpdated={updateSessionCompany} onCompanyRemoved={removeSessionCompany} openCompany={openCompany} watchlist={watchlist} toggleWatch={toggleWatch} />}
        {authUser && visiblePage === "company" && activeCompany && <CompanyDetail company={activeCompany} watched={watchlist.includes(activeCompany.id)} toggleWatch={toggleWatch} navigate={navigate} />}
        {authUser && visiblePage === "company" && !activeCompany && <div className="page-wrap"><Empty title="No company research is active" text="Pull a company from Discover before opening its research dossier." /></div>}
        {authUser && visiblePage === "compare" && <Compare companies={sessionCompanies} openCompany={openCompany} />}
        {authUser && visiblePage === "watchlist" && <Watchlist companies={sessionCompanies} ids={watchlist} openCompany={openCompany} toggleWatch={toggleWatch} navigate={navigate} />}
        {authUser && visiblePage === "learn" && <Learn />}
      </main>
      <Footer navigate={navigate} isAuthenticated={Boolean(authUser)} />
      {authUser && <MobileNav page={visiblePage} navigate={navigate} />}
    </div>
  );
}

function BetaStrip() {
  return <div className="beta-strip"><span>Private non-commercial preview</span><span>{liveDataEnabled ? "Live research provider · Browser-session data" : "Preview mode · Illustrative data"}</span></div>;
}

function Brand() {
  return (
    <div className="brand" aria-label="TaRaSha home">
      <span className="brand-logo-window"><img src={tarashaLogo} alt="TaRaSha" /></span>
      <span className="brand-edition">Discover</span>
    </div>
  );
}

function Header({ page, navigate, user, logout }: { page: Page; navigate: (page: Page) => void; user: AuthenticatedUser | null; logout: () => void }) {
  return (
    <header className="site-header">
      <button className="brand-button" onClick={() => navigate("home")}><Brand /></button>
      {user && <nav className="desktop-nav" aria-label="Main navigation">
        {navItems.map((item) => (
          <button key={item.page} className={page === item.page || (page === "company" && item.page === "discover") ? "active" : ""} onClick={() => navigate(item.page)}>{item.label}</button>
        ))}
      </nav>}
      {user && <div className="header-account"><span><small>Signed in as</small><strong>{user.username}</strong></span><button onClick={logout}>Log out</button></div>}
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

function Home({ navigate, isAuthenticated }: {
  navigate: (page: Page) => void;
  isAuthenticated: boolean;
}) {
  return (
    <section className="home-hero">
      <img className="home-hero-image" src={researchJourneyHero} alt="A path of research notes leading through a landscape toward the horizon" />
      <div className="home-hero-shade" />
      <div className="home-hero-copy">
        <p className="eyebrow">Structured company intelligence</p>
        <h1>Company data, organized for understanding.</h1>
        <p>Explore the financial performance, operating trends, and fundamentals of publicly listed companies.</p>
        <button className="button primary hero-login-button" onClick={() => navigate(isAuthenticated ? "discover" : "login")}>{isAuthenticated ? "Continue to Discover" : "Log in to Discover"}<span>→</span></button>
      </div>
    </section>
  );
}

function LoginPage({ onAuthenticated, navigate }: { onAuthenticated: (user: AuthenticatedUser) => void; navigate: (page: Page) => void }) {
  const [mode, setMode] = useState<"login" | "register" | "forgot">("login");
  const [registeredUsername, setRegisteredUsername] = useState("");
  const showLogin = (username = registeredUsername) => {
    setRegisteredUsername(username);
    setMode("login");
  };
  return <section className="auth-page">
    <div className="auth-intro">
      <p className="eyebrow">Private research workspace</p>
      <h1>Welcome to<br />TaRaSha Discover.</h1>
      <p>Sign in to search companies, compare evidence, maintain a watchlist, and keep learning.</p>
      <button className="auth-back" onClick={() => navigate("home")}>← Return home</button>
    </div>
    <div className="auth-panel">
      <div className="auth-tabs" role="tablist">
        <button role="tab" aria-selected={mode === "login"} className={mode === "login" ? "active" : ""} onClick={() => showLogin()}>Existing user</button>
        <button role="tab" aria-selected={mode === "register"} className={mode === "register" ? "active" : ""} onClick={() => setMode("register")}>New user</button>
      </div>
      {mode === "login" && <LoginForm onAuthenticated={onAuthenticated} showForgot={() => setMode("forgot")} initialUsername={registeredUsername} />}
      {mode === "register" && <RegistrationForm onRegistered={setRegisteredUsername} showLogin={showLogin} />}
      {mode === "forgot" && <ForgotPasswordForm returnToLogin={() => showLogin()} />}
    </div>
  </section>;
}

function LoginForm({ onAuthenticated, showForgot, initialUsername = "" }: { onAuthenticated: (user: AuthenticatedUser) => void; showForgot: () => void; initialUsername?: string }) {
  const [username, setUsername] = useState(initialUsername);
  const [password, setPassword] = useState("");
  const [message, setMessage] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const submit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault(); setBusy(true); setMessage(null);
    try { onAuthenticated(await authenticateUser(username, password)); }
    catch (error) { setMessage(error instanceof Error ? error.message : "Login failed."); }
    finally { setBusy(false); }
  };
  return <form className="auth-form" onSubmit={submit}>
    <div><small>Account access</small><h2>Log in</h2><p>Use your TaRaSha username and password.</p></div>
    <label>Username<input autoComplete="username" value={username} onChange={(event) => setUsername(event.target.value)} required /></label>
    <label>Password<input type="password" autoComplete="current-password" value={password} onChange={(event) => setPassword(event.target.value)} required /></label>
    {message && <div className="auth-message error" role="alert">{message}</div>}
    <button className="button primary" disabled={busy}>{busy ? "Logging in…" : "Log in"}</button>
    <button className="forgot-link" type="button" onClick={showForgot}>Forgot password?</button>
  </form>;
}

function RegistrationForm({ onRegistered, showLogin }: { onRegistered: (username: string) => void; showLogin: (username: string) => void }) {
  const [fields, setFields] = useState({ name: "", securityQuestion: "", securityAnswer: "", username: "", password: "" });
  const [message, setMessage] = useState<string | null>(null);
  const [createdUsername, setCreatedUsername] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const update = (key: keyof typeof fields, value: string) => setFields((current) => ({ ...current, [key]: value }));
  const submit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault(); setBusy(true); setMessage(null);
    try {
      const user = await registerUser(fields);
      setCreatedUsername(user.username);
      onRegistered(user.username);
    }
    catch (error) { setMessage(error instanceof Error ? error.message : "Registration failed."); }
    finally { setBusy(false); }
  };
  if (createdUsername) return <div className="auth-form registration-success">
    <div><small>Account created</small><h2>Registration complete</h2><p>Your account has been created. Sign in to begin a new authenticated session.</p></div>
    <div className="auth-message success" role="status"><strong>{createdUsername}</strong> is created successfully, login <a href="#/login" onClick={(event) => { event.preventDefault(); showLogin(createdUsername); }}>here</a>.</div>
  </div>;
  return <form className="auth-form registration-form" onSubmit={submit}>
    <div><small>Create your workspace</small><h2>Register</h2><p>Your recovery answer is checked only when you reset your password.</p></div>
    <label>Name<input autoComplete="name" value={fields.name} onChange={(event) => update("name", event.target.value)} required /></label>
    <label>Security question<select value={fields.securityQuestion} onChange={(event) => update("securityQuestion", event.target.value)} required><option value="" disabled>Select a security question</option>{SECURITY_QUESTIONS.map((question) => <option key={question} value={question}>{question}</option>)}</select></label>
    <label>Answer<input type="password" autoComplete="off" value={fields.securityAnswer} onChange={(event) => update("securityAnswer", event.target.value)} required /></label>
    <label>Username<input autoComplete="username" value={fields.username} onChange={(event) => update("username", event.target.value)} required /></label>
    <label>Password<input type="password" autoComplete="new-password" minLength={8} value={fields.password} onChange={(event) => update("password", event.target.value)} required /><small>At least 8 characters</small></label>
    {message && <div className="auth-message error" role="alert">{message}</div>}
    <button className="button primary" disabled={busy}>{busy ? "Creating account…" : "Create account"}</button>
  </form>;
}

function ForgotPasswordForm({ returnToLogin }: { returnToLogin: () => void }) {
  const [username, setUsername] = useState("");
  const [question, setQuestion] = useState<string | null>(null);
  const [answer, setAnswer] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [message, setMessage] = useState<{ tone: "error" | "success"; text: string } | null>(null);
  const [busy, setBusy] = useState(false);
  const findQuestion = () => {
    setMessage(null);
    try { setQuestion(getSecurityQuestion(username)); }
    catch (error) { setQuestion(null); setMessage({ tone: "error", text: error instanceof Error ? error.message : "Account lookup failed." }); }
  };
  const submit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault(); setBusy(true); setMessage(null);
    try {
      await resetPassword(username, answer, newPassword);
      setMessage({ tone: "success", text: "Password reset. You can now log in with your new password." });
      setAnswer(""); setNewPassword("");
    } catch (error) { setMessage({ tone: "error", text: error instanceof Error ? error.message : "Password reset failed." }); }
    finally { setBusy(false); }
  };
  return <form className="auth-form" onSubmit={submit}>
    <div><small>Account recovery</small><h2>Reset password</h2><p>Find your security question, then provide the answer you registered.</p></div>
    <label>Username<div className="auth-inline"><input autoComplete="username" value={username} onChange={(event) => { setUsername(event.target.value); setQuestion(null); }} required /><button type="button" onClick={findQuestion}>Find question</button></div></label>
    {question && <><div className="security-question"><small>Your security question</small><strong>{question}</strong></div>
      <label>Answer<input type="password" autoComplete="off" value={answer} onChange={(event) => setAnswer(event.target.value)} required /></label>
      <label>New password<input type="password" autoComplete="new-password" minLength={8} value={newPassword} onChange={(event) => setNewPassword(event.target.value)} required /><small>At least 8 characters</small></label>
      <button className="button primary" disabled={busy}>{busy ? "Resetting…" : "Reset password"}</button></>}
    {message && <div className={`auth-message ${message.tone}`} role={message.tone === "error" ? "alert" : "status"}>{message.text}</div>}
    <button className="forgot-link" type="button" onClick={returnToLogin}>← Back to login</button>
  </form>;
}

function Discover({ sessionCompanies, onResearchPulled, onCompanyUpdated, onCompanyRemoved, openCompany, watchlist, toggleWatch }: { sessionCompanies: Company[]; onResearchPulled: (company: Company) => void; onCompanyUpdated: (company: Company) => void; onCompanyRemoved: (id: string) => void; openCompany: (id: string) => void; watchlist: string[]; toggleWatch: (id: string) => void }) {
  const [query, setQuery] = useState("");
  const [country, setCountry] = useState<"USA" | "India">("USA");
  const currentYear = new Date().getFullYear();
  const [fromYear, setFromYear] = useState(currentYear - 6);
  const [toYear, setToYear] = useState(currentYear);
  const [results, setResults] = useState<CatalogCompany[]>([]);
  const [searching, setSearching] = useState(false);
  const [pullingId, setPullingId] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);

  useEffect(() => {
    setMessage(null);
    if (query.trim().length < 2) { setResults([]); return; }
    const timer = window.setTimeout(async () => {
      setSearching(true);
      try { setResults(await searchCompanyCatalog(query, country)); }
      catch (error) { setMessage(error instanceof Error ? error.message : "Company search failed."); }
      finally { setSearching(false); }
    }, 250);
    return () => window.clearTimeout(timer);
  }, [query, country]);

  const pull = async (catalog: CatalogCompany) => {
    if (!sessionCompanies.some((item) => item.id === catalog.id) && sessionCompanies.length >= MAX_SESSION_COMPANIES) {
      setMessage(`This session already contains ${MAX_SESSION_COMPANIES} companies—the maximum allowed.`); return;
    }
    setPullingId(catalog.id); setMessage(null);
    try { onResearchPulled(await pullCompanyResearch(catalog, fromYear, toYear)); setMessage(`${catalog.name} is now available in your session research shelf.`); }
    catch (error) { setMessage(error instanceof Error ? error.message : "Research pull failed."); }
    finally { setPullingId(null); }
  };

  return (
    <div className="page-wrap">
      <PageIntro eyebrow="Discover" title="Search the catalogue. Pull the research." text={`Choose up to ${MAX_YEAR_RANGE} years. The application retrieves approved financial history into browser memory and stores no separate Consumer copy.`} />
      <div className="session-limit-banner"><span>{sessionCompanies.length} / {MAX_SESSION_COMPANIES}</span><div><strong>Session research capacity</strong><small>Closing or refreshing this browser session clears the pulled financial data.</small></div></div>
      <div className="market-notice"><strong>Private evaluation data source</strong><p>Companies with TaRaSha Research coverage use financial spreadsheets downloaded through StockAnalysis.com and bulk-uploaded into the private Research platform. This preview is non-commercial; source-provider permission is required before paid distribution.</p></div>
      <div className="search-panel">
        <label className="search-box"><span>⌕</span><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Search company or symbol" /></label>
        <select value={country} onChange={(event) => setCountry(event.target.value as "USA" | "India")} aria-label="Choose market"><option>USA</option><option>India</option></select>
      </div>
      <div className="year-range-panel"><div><strong>Reporting-year range</strong><small>Maximum {MAX_YEAR_RANGE} years</small></div><label>From<input type="number" min="1995" max={toYear} value={fromYear} onChange={(event) => setFromYear(Number(event.target.value))} /></label><span>—</span><label>To<input type="number" min={fromYear} max={currentYear} value={toYear} onChange={(event) => setToYear(Number(event.target.value))} /></label></div>
      {message && <div className="data-message">{message}</div>}
      {query.trim().length >= 2 && <section className="catalog-results"><div className="result-meta"><strong>{searching ? "Searching catalogue…" : `${results.length} matches`}</strong><span>Search results are not company cards</span></div>{results.map((company) => <article key={company.id}><div className="company-monogram">{company.name[0]}</div><div><h3>{company.name}</h3><p>{company.ticker} · {company.exchange} · {company.provider}</p></div><button className="button secondary" disabled={pullingId === company.id} onClick={() => pull(company)}>{pullingId === company.id ? "Pulling research…" : sessionCompanies.some((item) => item.id === company.id) ? "Refresh research" : "Pull research"}</button></article>)}</section>}
      <section className="session-research-section"><div className="section-heading-row"><div><p className="eyebrow">Pulled in this browser session</p><h2>Research shelf</h2></div><span>{sessionCompanies.length} companies</span></div>{sessionCompanies.length ? <div className="research-shelf-stack">{sessionCompanies.map((company) => <CompanyCard key={company.id} company={company} openCompany={openCompany} watched={watchlist.includes(company.id)} toggleWatch={toggleWatch} onRemove={() => onCompanyRemoved(company.id)} onCompanyUpdated={onCompanyUpdated} />)}</div> : <Empty title="Your research shelf is empty" text="Search the catalogue above and pull a company to create its session card." />}</section>
    </div>
  );
}

function CompanyCard({ company, openCompany, watched, toggleWatch, onRemove, onCompanyUpdated }: { company: Company; openCompany: (id: string) => void; watched: boolean; toggleWatch: (id: string) => void; onRemove?: () => void; onCompanyUpdated?: (company: Company) => void }) {
  if (company.researchShelf) return <ResearchStoryCard company={company} shelf={company.researchShelf} openCompany={openCompany} watched={watched} toggleWatch={toggleWatch} onRemove={onRemove} onCompanyUpdated={onCompanyUpdated} />;
  const revenue = company.metrics.revenue;
  const change = revenue.length ? percentChange(revenue) : null;
  const revenueText = revenue.length ? formatCompanyMetric(company, "revenue", latest(revenue).value) : "Not tagged";
  return (
    <article className="company-card">
      <button className={`watch-button ${watched ? "watched" : ""}`} onClick={() => toggleWatch(company.id)} aria-label={watched ? "Remove from watchlist" : "Add to watchlist"}>{watched ? "♥" : "♡"}</button>
      {onRemove && <button className="remove-shelf-button compact" onClick={onRemove} aria-label={`Remove ${company.name} from the research shelf`}>×</button>}
      <button className="card-main" onClick={() => openCompany(company.id)}>
        <div className="company-heading"><span className="company-monogram">{company.name[0]}</span><div><h3>{company.name}</h3><span>{company.symbol} · {company.sector}</span></div></div>
        <p>{company.description}</p>
        <div className="card-metric"><span>Latest reported revenue</span><strong>{revenueText}</strong><small>{change === null ? "Review filing facts" : `${change >= 0 ? "+" : ""}${change.toFixed(1)}%`} from prior year</small></div>
        <div className="card-footer"><span>Data through {company.reportingPeriod}</span><strong>View company →</strong></div>
      </button>
    </article>
  );
}

const researchCardLabels = ["Growth quality", "Margins & costs", "Profit flow & valuation", "Balance sheet", "Capital allocation"] as const;

const researchChartPalette = [
  { color: "#4f7fa6", negativeColor: "#b45a63" },
  { color: "#6c91ae", negativeColor: "#c36b73" },
  { color: "#315f86", negativeColor: "#96434d" },
  { color: "#789db8", negativeColor: "#b96b70" },
] as const;

function researchChartSeries(label: string, index: number) {
  return { label, ...researchChartPalette[index % researchChartPalette.length] };
}

function ResearchStoryCard({ company, shelf, openCompany, watched, toggleWatch, onRemove, onCompanyUpdated }: { company: Company; shelf: ResearchShelfAnalysis; openCompany: (id: string) => void; watched: boolean; toggleWatch: (id: string) => void; onRemove?: () => void; onCompanyUpdated?: (company: Company) => void }) {
  const [activeCard, setActiveCard] = useState(0);
  const [recalculatingConstituents, setRecalculatingConstituents] = useState(false);
  const [constituentMessage, setConstituentMessage] = useState<string | null>(null);
  const swipeStartX = useRef<number | null>(null);
  const chartUnit = company.currency.startsWith("US$") ? "USD millions" : "INR crores";
  const companyDeltaData = shelf.companyDeltas.map((item) => ({
    interval: yearInterval(item.fromYear, item.toYear),
    percentValues: [item.revenueChangePercent, item.grossProfitChangePercent, item.operatingIncomeChangePercent],
    absoluteValues: [item.revenue, item.grossProfit, item.operatingIncome],
  }));
  const moveCard = (direction: -1 | 1) => setActiveCard((current) => Math.min(researchCardLabels.length - 1, Math.max(0, current + direction)));
  const beginSwipe = (event: React.PointerEvent<HTMLElement>) => {
    if ((event.target as HTMLElement).closest("[data-horizontal-scroll]")) return;
    swipeStartX.current = event.clientX;
  };
  const finishSwipe = (event: React.PointerEvent<HTMLElement>) => {
    if (swipeStartX.current === null) return;
    const distance = event.clientX - swipeStartX.current;
    swipeStartX.current = null;
    if (distance > 55) moveCard(1);
    if (distance < -55) moveCard(-1);
  };
  const updateConstituents = async (constituentIds?: string[]) => {
    if (!onCompanyUpdated) return;
    setRecalculatingConstituents(true);
    setConstituentMessage(null);
    try {
      const updated = await recalculateIndustryConstituents(company, constituentIds);
      onCompanyUpdated(updated);
      setConstituentMessage(constituentIds ? `Industry comparison recalculated using ${updated.researchShelf?.industryCompanyCount ?? constituentIds.length} selected companies.` : "The default industry bucket has been restored.");
    } catch (error) {
      setConstituentMessage(error instanceof Error ? error.message : "Industry comparison recalculation failed.");
    } finally {
      setRecalculatingConstituents(false);
    }
  };

  return (
    <article className="company-card research-story-card shelf-company">
      <div className="story-card-head">
        <div className="company-heading"><span className="company-monogram">{company.name[0]}</span><div><h3>{company.name}</h3><span>{company.symbol} · FY {shelf.fromYear}–{shelf.toYear}</span><small>{shelf.industryBucket}</small></div></div>
        <div className="shelf-company-actions">
          <button className={`watch-button ${watched ? "watched" : ""}`} onClick={() => toggleWatch(company.id)} aria-label={watched ? "Remove from watchlist" : "Add to watchlist"}>{watched ? "♥" : "♡"}</button>
          {onRemove && <button className="remove-shelf-button" onClick={onRemove} aria-label={`Remove ${company.name} from the research shelf`}>Remove</button>}
        </div>
      </div>

      <nav className="research-card-nav" aria-label={`${company.name} research cards`}>
        <button className="card-cursor" onClick={() => moveCard(-1)} disabled={activeCard === 0} aria-label="Show previous research card">←</button>
        <div className="research-card-tabs" role="tablist">{researchCardLabels.map((label, index) => <button key={label} role="tab" aria-selected={activeCard === index} className={activeCard === index ? "active" : ""} onClick={() => setActiveCard(index)}>{label}{index > 2 && <small>Coming soon</small>}</button>)}</div>
        <button className="card-cursor" onClick={() => moveCard(1)} disabled={activeCard === researchCardLabels.length - 1} aria-label="Show next research card">→</button>
      </nav>

      <div className="research-card-position"><span>{String(activeCard + 1).padStart(2, "0")} / {String(researchCardLabels.length).padStart(2, "0")}</span><small>Swipe right for next · left for previous</small></div>
      <section className="research-card-viewport" onPointerDown={beginSwipe} onPointerUp={finishSwipe} onPointerCancel={() => { swipeStartX.current = null; }}>
        {activeCard === 0 ? <div className="research-note growth-quality-note">
          <div className="growth-quality-heading">
            <div><small>Company and industry bucket</small><h4>Growth Quality &amp; Operating Leverage</h4></div>
            <span>{shelf.industryCompanyCount} bucket companies · FY {shelf.fromYear}–{shelf.toYear}</span>
          </div>

          <GrowthComparisonTable shelf={shelf.growthComparisons} />
          <ClusteredColumnChart
            title="Company growth bridge"
            subtitle="Bar height shows YoY percentage change · absolute delta appears above each bar"
            absoluteUnit={chartUnit}
            data={companyDeltaData}
            series={[
              researchChartSeries("Revenue change", 0),
              researchChartSeries("Gross profit change", 1),
              researchChartSeries("Operating income change", 2),
            ]}
          />
          <IndustryComparisonChart title="Industry bucket comparison · Revenue" unit={chartUnit} data={shelf.industryDeltas.revenue} />
          <IndustryComparisonChart title="Industry bucket comparison · Gross profit" unit={chartUnit} data={shelf.industryDeltas.grossProfit} />
          <IndustryComparisonChart title="Industry bucket comparison · Operating income" unit={chartUnit} data={shelf.industryDeltas.operatingIncome} />
          <RawIncomeTable company={company} rows={shelf.rawIncome} />
          <IndustryConstituentManager
            constituents={shelf.industryConstituents}
            customized={shelf.industryConstituentsCustomized}
            country={company.currency.startsWith("US$") ? "USA" : "India"}
            busy={recalculatingConstituents}
            message={constituentMessage}
            onChange={onCompanyUpdated ? updateConstituents : undefined}
          />
        </div> : activeCard === 1 ? <MarginsAndCostStructureCard company={company} shelf={shelf} /> : activeCard === 2 ? <ProfitFlowAndValuationCard company={company} shelf={shelf} /> : <ResearchCardPlaceholder label={researchCardLabels[activeCard]} position={activeCard + 1} />}
      </section>

      <div className="card-footer story-footer"><span>Session data · {company.currency}</span><button className="text-button" onClick={() => openCompany(company.id)}>Open full research dossier →</button></div>
    </article>
  );
}

function ResearchCardPlaceholder({ label, position }: { label: string; position: number }) {
  return <div className={`research-note placeholder-note note-${position}`}><small>Research card {String(position).padStart(2, "0")}</small><h4>{label}</h4><p>This analytical card is in the research roadmap and will appear here when its lens is ready.</p><span className="placeholder-stamp">In development</span></div>;
}

const profitabilityMetrics: Array<{
  key: ProfitabilityMetricKey;
  label: string;
  compactLabel: string;
  direction: "higher" | "lower" | "contextual";
  aboveMeaning: string;
  belowMeaning: string;
}> = [
  { key: "grossMargin", label: "Gross margin", compactLabel: "Gross margin", direction: "higher", aboveMeaning: "More revenue remained after direct costs than in the bucket benchmark.", belowMeaning: "Less revenue remained after direct costs than in the bucket benchmark." },
  { key: "operatingMargin", label: "Operating margin", compactLabel: "Operating margin", direction: "higher", aboveMeaning: "More operating profit remained from each unit of revenue than in the bucket benchmark.", belowMeaning: "Less operating profit remained from each unit of revenue than in the bucket benchmark." },
  { key: "cogsRatio", label: "COGS (% of revenue)", compactLabel: "COGS / revenue", direction: "lower", aboveMeaning: "Direct costs consumed a larger share of revenue than in the bucket benchmark.", belowMeaning: "Direct costs consumed a smaller share of revenue than in the bucket benchmark." },
  { key: "sgaRatio", label: "SG&A (% of revenue)", compactLabel: "SG&A / revenue", direction: "lower", aboveMeaning: "Selling and administrative costs consumed a larger share of revenue than in the bucket benchmark.", belowMeaning: "Selling and administrative costs consumed a smaller share of revenue than in the bucket benchmark." },
  { key: "daRatio", label: "D&A expense (% of revenue)", compactLabel: "D&A / revenue", direction: "lower", aboveMeaning: "The reported depreciation and amortization burden was heavier than the bucket benchmark.", belowMeaning: "The reported depreciation and amortization burden was lighter than the bucket benchmark." },
  { key: "rdRatio", label: "R&D expense (% of revenue)", compactLabel: "R&D / revenue", direction: "contextual", aboveMeaning: "Reported R&D intensity was higher; this may reflect greater reinvestment rather than weaker performance.", belowMeaning: "Reported R&D intensity was lower; this is not automatically better because it may reflect less reinvestment." },
];

function MarginsAndCostStructureCard({ company, shelf }: { company: Company; shelf: ResearchShelfAnalysis }) {
  const analysis = shelf.profitability;
  const chartUnit = company.currency.startsWith("US$") ? "USD millions" : "INR crores";
  const yearlyData = analysis.yearly.map((row) => ({ label: `FY ${row.year}`, row }));
  return <div className="research-note profitability-note">
    <div className="growth-quality-heading">
      <div><small>Profitability and reinvestment lens</small><h4>Margins &amp; Cost Structure</h4></div>
      <span>{shelf.industryCompanyCount} bucket companies · FY {shelf.fromYear}–{shelf.toYear}</span>
    </div>
    <p className="profitability-intro">Track how much revenue remains after direct and operating costs, then compare the company’s cost structure with its selected industry bucket.</p>
    <ProfitabilityStatisticsTable analysis={analysis} />
    <PercentageLevelChart
      title="Margin profile by year"
      subtitle="Percentage appears inside each bar · absolute profit appears above"
      absoluteUnit={chartUnit}
      data={yearlyData.map(({ label, row }) => ({ label, values: [row.grossMargin, row.operatingMargin], absoluteValues: [row.grossProfit, row.operatingIncome] }))}
      series={[
        researchChartSeries("Gross margin", 0),
        researchChartSeries("Operating margin", 1),
      ]}
    />
    <PercentageLevelChart
      title="Operating cost structure by year"
      subtitle="Percentage of revenue appears inside each bar · absolute expense appears above"
      absoluteUnit={chartUnit}
      data={yearlyData.map(({ label, row }) => ({ label, values: [row.cogsRatio, row.sgaRatio, row.daRatio, row.rdRatio], absoluteValues: [row.cogs, row.sga, row.da, row.rd] }))}
      series={[
        researchChartSeries("COGS / revenue", 0),
        researchChartSeries("SG&A / revenue", 1),
        researchChartSeries("D&A / revenue", 2),
        researchChartSeries("R&D / revenue", 3),
      ]}
    />
    <ProfitabilityIndustryChart title="Industry bucket comparison · Gross margin" unit={chartUnit} data={analysis.industryComparisons.grossMargin} />
    <ProfitabilityIndustryChart title="Industry bucket comparison · Operating margin" unit={chartUnit} data={analysis.industryComparisons.operatingMargin} />
    <ProfitabilityIndustryChart title="Industry bucket comparison · D&A expense / revenue" unit={chartUnit} data={analysis.industryComparisons.daRatio} />
    <ProfitabilityIndustryChart title="Industry bucket comparison · R&D expense / revenue" unit={chartUnit} data={analysis.industryComparisons.rdRatio} />
    <ProfitabilityYearTable rows={analysis.yearly} bands={analysis.performanceBands} />
  </div>;
}

const earningsFlowSteps: Array<{
  key: EarningsFlowMetricKey;
  label: string;
  explanation: string;
  operator: "" | "−" | "=" | "÷" | "±";
  tone: "source" | "expense" | "subtotal" | "unsupported";
  signedValue?: boolean;
}> = [
  { key: "revenue", label: "Revenue", explanation: "Money earned from customers before costs.", operator: "", tone: "source" },
  { key: "cogs", label: "Cost of goods sold", explanation: "Direct cost of delivering the product or service.", operator: "−", tone: "expense" },
  { key: "grossProfit", label: "Gross profit", explanation: "What remains after direct costs.", operator: "=", tone: "subtotal" },
  { key: "sga", label: "SG&A", explanation: "Sales, administration and everyday corporate costs.", operator: "−", tone: "expense" },
  { key: "researchAndDevelopment", label: "R&D expense", explanation: "Investment in research and product development when separately disclosed.", operator: "−", tone: "expense" },
  { key: "otherOperatingExpense", label: "Other operating costs", explanation: "Derived residual needed to reconcile gross profit to EBITDA; it absorbs undisclosed operating lines.", operator: "−", tone: "expense" },
  { key: "ebitda", label: "EBITDA", explanation: "Operating earnings before depreciation, amortization, interest and tax.", operator: "=", tone: "subtotal" },
  { key: "depreciationAndAmortization", label: "D&A", explanation: "The accounting cost of using long-lived tangible and intangible assets.", operator: "−", tone: "expense" },
  { key: "ebit", label: "EBIT", explanation: "Operating profit after D&A, before financing and tax.", operator: "=", tone: "subtotal" },
  { key: "interestExpense", label: "Interest", explanation: "Financing cost on borrowings and related obligations.", operator: "−", tone: "expense" },
  { key: "ebt", label: "Earnings before tax", explanation: "Profit remaining before the tax provision.", operator: "=", tone: "subtotal" },
  { key: "taxes", label: "Taxes", explanation: "Derived as reported pre-tax income less reported net profit.", operator: "−", tone: "expense" },
  { key: "netProfit", label: "Net profit", explanation: "Reported profit after operating costs, financing and tax.", operator: "=", tone: "subtotal" },
  { key: "minorityInterestInEarnings", label: "Minority interest in earnings", explanation: "Deducted for earnings attributable to non-controlling interests.", operator: "−", tone: "expense" },
  { key: "earningsFromDiscontinuedOperations", label: "Earnings from discontinued operations", explanation: "A reported profit adds to the bridge; a reported loss deducts from it.", operator: "±", tone: "expense", signedValue: true },
  { key: "other", label: "Other", explanation: "Calculated residual that reconciles net profit to reported net income to common.", operator: "±", tone: "expense", signedValue: true },
  { key: "netIncomeToCommon", label: "Net income to common", explanation: "Reported income attributable to common shareholders after the bridge items above.", operator: "=", tone: "subtotal" },
  { key: "commonDividendsPaid", label: "Common dividends paid", explanation: "Deducted from net income to common using the positive dividend amount stored by Research.", operator: "−", tone: "expense" },
  { key: "currentYearEarningsRetained", label: "Current year earnings retained", explanation: "Net income to common remaining after common dividends paid.", operator: "=", tone: "subtotal" },
];

const valuationMetrics: Array<{ key: ValuationMetricKey; label: string; explanation: string }> = [
  { key: "evRevenue", label: "EV / Revenue", explanation: "Enterprise value attached to each unit of sales." },
  { key: "evGrossProfit", label: "EV / Gross profit", explanation: "Enterprise value attached to each unit left after direct costs." },
  { key: "evEbitda", label: "EV / EBITDA", explanation: "Enterprise value relative to operating earnings before D&A." },
  { key: "evEbit", label: "EV / EBIT", explanation: "Enterprise value relative to operating profit after D&A." },
  { key: "pe", label: "P / E", explanation: "Equity price relative to trailing earnings." },
];

function ProfitFlowAndValuationCard({ company, shelf }: { company: Company; shelf: ResearchShelfAnalysis }) {
  const analysis = shelf.earningsAndValuation;
  const availableYears = analysis.earningsFlow.map((point) => point.year).sort((left, right) => left - right);
  const yearSet = new Set(availableYears);
  const windowEnds = availableYears.filter((year) => yearSet.has(year - 1) && yearSet.has(year - 2)).sort((left, right) => right - left);
  const [selectedEndYear, setSelectedEndYear] = useState(windowEnds[0] ?? shelf.toYear);
  useEffect(() => {
    if (windowEnds.length && !windowEnds.includes(selectedEndYear)) setSelectedEndYear(windowEnds[0]);
  }, [shelf.fromYear, shelf.toYear, windowEnds.join(","), selectedEndYear]);
  const selectedYears = analysis.earningsFlow
    .filter((point) => point.year >= selectedEndYear - 2 && point.year <= selectedEndYear)
    .sort((left, right) => right.year - left.year);
  const unit = company.currency.startsWith("US$") ? "USD millions" : "INR crores";
  return <div className="research-note earnings-valuation-note">
    <div className="growth-quality-heading earnings-flow-heading">
      <div><small>Income statement and market lens</small><h4>Profit Flow &amp; Valuation</h4></div>
      <span>{shelf.industryCompanyCount} bucket companies · {unit}</span>
    </div>
    <p className="profitability-intro">Follow one unit of revenue down to reported profit, then see how the market values the latest operating results. Every available line is paired with the selected industry bucket median.</p>
    <section className="earnings-flow-section">
      <div className="earnings-section-heading"><div><small>Section 1</small><h5>How revenue becomes profit</h5><p>Three consecutive fiscal years, newest first</p></div>{windowEnds.length > 0 && <label>Three-year window<select value={selectedEndYear} onChange={(event) => setSelectedEndYear(Number(event.target.value))}>{windowEnds.map((year) => <option value={year} key={year}>FY {year - 2}–FY {year}</option>)}</select></label>}</div>
      {windowEnds.length === 0 ? <div className="flow-availability-note">This pull contains fewer than three consecutive reporting years. Pull at least three years to use the profit-flow view.</div> : <div className="earnings-year-stack">{selectedYears.map((point) => <EarningsFlowYearPanel key={point.year} company={company} point={point} />)}</div>}
      <div className="flow-coverage-note"><strong>Bridge convention</strong><p>Minority interest is deducted before net income to common. Discontinued operations and Other carry their own plus or minus sign, with Other making that bridge reconcile exactly. Common dividends are then deducted from net income to common to show current year earnings retained.</p></div>
    </section>
    <ValuationComparisonSection company={company} shelf={shelf} />
  </div>;
}

function EarningsFlowYearPanel({ company, point }: { company: Company; point: EarningsFlowYear }) {
  return <article className="earnings-year-panel">
    <header><div><small>Fiscal year</small><h6>FY {point.year}</h6></div><span>Company</span><span>Industry median</span></header>
    <div className="earnings-flow-rows">{earningsFlowSteps.map((step) => {
      const metric = point.metrics[step.key];
      if (!metric) return <div className={`earnings-flow-row ${step.tone}`} key={step.key}>
        <span className="flow-operator" aria-hidden="true">{step.operator}</span>
        <div className="flow-label"><strong>{step.label}</strong><small>{step.explanation}</small></div>
        <div className="flow-value"><strong>Not available</strong><small>Refresh research</small></div>
        <div className="flow-value benchmark"><strong>Not available</strong><small>Refresh research</small></div>
      </div>;
      const companyAmount = formatFlowAmount(company, metric.companyValue, step.signedValue === true);
      const industryAmount = formatFlowAmount(company, metric.industryMedian, step.signedValue === true);
      return <div className={`earnings-flow-row ${step.tone}`} key={step.key}>
        <span className="flow-operator" aria-hidden="true">{step.operator}</span>
        <div className="flow-label"><strong>{step.label}</strong><small>{step.explanation}</small></div>
        <div className="flow-value"><div className="flow-value-line"><strong>{companyAmount}</strong>{metric.companyMarginPercent !== null ? <span className="flow-margin">{formatLevelPercent(metric.companyMarginPercent)} margin</span> : null}</div>{step.key === "otherOperatingExpense" || step.key === "taxes" || step.key === "other" ? <small>Derived</small> : null}</div>
        <div className="flow-value benchmark"><div className="flow-value-line"><strong>{industryAmount}</strong>{metric.industryMedianMarginPercent !== null ? <span className="flow-margin">{formatLevelPercent(metric.industryMedianMarginPercent)} margin</span> : null}</div><small>{metric.industryObservations ? `${metric.industryObservations} observations` : "No comparable facts"}</small></div>
      </div>;
    })}</div>
  </article>;
}

function formatFlowAmount(company: Company, value: number | null, signedValue: boolean): string {
  if (value === null || !Number.isFinite(value)) return "Not available";
  if (!signedValue) return formatCompanyAmount(company, value);
  const sign = value > 0 ? "+ " : value < 0 ? "− " : "";
  return `${sign}${formatCompanyAmount(company, Math.abs(value))}`;
}

function ValuationComparisonSection({ company, shelf }: { company: Company; shelf: ResearchShelfAnalysis }) {
  const valuation = shelf.earningsAndValuation.valuation;
  const sourceLabel = valuation.enterpriseValueSource === "yfinance_direct"
    ? "Direct yfinance Enterprise Value"
    : valuation.enterpriseValueSource === "market_cap_api"
      ? "yfinance market-cap bridge"
      : valuation.enterpriseValueSource === "live_price_shares"
        ? "Live price × shares bridge"
        : valuation.enterpriseValueSource === "calculated"
          ? "Stored Research fallback"
          : "No saved EV snapshot";
  return <section className="valuation-comparison-section">
    <div className="earnings-section-heading"><div><small>Section 2</small><h5>What the market pays for the result</h5><p>Latest saved market snapshot versus the industry bucket median</p></div><div className="valuation-snapshot"><span>{sourceLabel}</span><strong>{valuation.enterpriseValue === null ? "EV unavailable" : formatCompanyAmount(company, valuation.enterpriseValue)}</strong><small>{valuation.enterpriseValueAsOf ? `As of ${formatMarketAsOf(valuation.enterpriseValueAsOf)}` : "Refresh EV & P/E in TaRaSha Research"}</small></div></div>
    <div className="valuation-plain-language"><strong>How to read a multiple</strong><p>A 5× EV/Revenue multiple means the market values the whole operating business at five times one year’s revenue. A lower multiple is not automatically better: growth, margins, risk and accounting comparability all matter.</p></div>
    <div className="valuation-comparison-list">{valuationMetrics.map((metric) => {
      const comparison = valuation.comparisons[metric.key];
      const maximum = Math.max(comparison.companyValue ?? 0, comparison.industryMedian ?? 0, 1);
      return <article key={metric.key}>
        <div className="valuation-label"><strong>{metric.label}</strong><small>{metric.explanation}</small></div>
        <div className="valuation-bars">
          <div><span>Company</span><i><b style={{ width: `${Math.max(0, ((comparison.companyValue ?? 0) / maximum) * 100)}%` }} /></i><strong>{formatMultiple(comparison.companyValue)}</strong></div>
          <div className="industry"><span>Industry median</span><i><b style={{ width: `${Math.max(0, ((comparison.industryMedian ?? 0) / maximum) * 100)}%` }} /></i><strong>{formatMultiple(comparison.industryMedian)}</strong></div>
        </div>
        <span className="valuation-observations">{comparison.industryObservations ? `${comparison.industryObservations} comparable companies` : "No peer snapshot coverage"}</span>
      </article>;
    })}</div>
    <details className="ev-method-note"><summary>How TaRaSha Research obtains Enterprise Value</summary><p>When the Research valuation dashboard is refreshed, the server calls yfinance for the company’s Yahoo symbol and first reads its reported <code>enterpriseValue</code>. Research converts that value to local-currency millions and stores the amount, source and timestamp. If the direct field is absent, Research can calculate EV from a yfinance market-cap input plus stored debt less stored cash, then fall back to live price × shares or stored market-cap data. Consumer reads only that saved snapshot; the browser does not call yfinance.</p><small>{valuation.enterpriseValueDetail}</small></details>
    <p className="valuation-period-note">EV-based multiples use the saved point-in-time Enterprise Value and FY {valuation.denominatorYear ?? "—"} annual denominators. P/E uses the saved trailing P/E. Mixed snapshot/reporting dates are shown for context and should not be treated as a fully synchronized market-data feed.</p>
  </section>;
}

function formatMultiple(value: number | null): string {
  return value === null || !Number.isFinite(value) ? "—" : `${value.toFixed(value >= 100 ? 0 : 1)}×`;
}

function formatMarketAsOf(value: string): string {
  const parsed = new Date(value.includes("T") ? value : value.replace(" ", "T") + "Z");
  return Number.isNaN(parsed.getTime()) ? value : parsed.toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric" });
}

function PerformanceBandLegend() {
  return <div className="performance-band-guide"><div className="growth-band-key"><span className="negative">Negative</span><span className="moderate">Moderate</span><span className="good">Good</span><span className="excellent">Excellent</span></div><p>Ranges use the selected industry bucket’s observed quartiles. Higher margins and lower expense ratios rank better.</p></div>;
}

type InsightMetricData = {
  companyMedian: number | null;
  companySpread: number | null;
  companyObservations: number;
  companyDistribution: DistributionObservation[];
  bucketMedian: number | null;
  bucketSpread: number | null;
  bucketObservations: number;
  bucketDistribution: DistributionObservation[];
};

function StatisticalReadingGuide() {
  return <aside className="statistics-reading-guide" aria-label="How to read the statistical summary">
    <div><span className="statistics-guide-icon median" aria-hidden="true"><i /><i /><i /><i /><i /></span><p><strong>Median = typical result</strong><small>The middle observation after results are sorted. Half were higher and half lower, so one extreme year has less influence.</small></p></div>
    <div><span className="statistics-guide-icon spread" aria-hidden="true"><i /><i /><i /></span><p><strong>Year-to-year spread = consistency</strong><small>This is standard deviation in plain language. A smaller spread means steadier results; 23.8 pts is more consistent than 49.8 pts.</small></p></div>
    <div><span className="statistics-guide-icon points" aria-hidden="true">pts</span><p><strong>Points compare percentages</strong><small>If one result is 20% and another is 15%, the difference is 5 percentage points.</small></p></div>
  </aside>;
}

function MetricComparisonBars({ label, companyValue, bucketValue, spread = false, bucketLabel = "Industry bucket" }: { label: string; companyValue: number | null; bucketValue: number | null; spread?: boolean; bucketLabel?: string }) {
  const finiteValues = [companyValue, bucketValue].filter((value): value is number => value !== null && Number.isFinite(value));
  const maximum = Math.max(1, ...finiteValues.map((value) => Math.abs(value)));
  const formatValue = spread ? formatLevelPoints : formatLevelPercent;
  const row = (name: string, value: number | null, role: "company" | "bucket") => {
    const available = value !== null && Number.isFinite(value);
    const width = available ? Math.max(3, (Math.abs(value) / maximum) * 100) : 0;
    const negative = available && value < 0;
    return <div className={`metric-comparison-row ${role}`} key={role}>
      <span>{name}</span>
      <div className="metric-comparison-track"><i className={negative ? "negative" : ""} style={{ width: `${width}%` }} /></div>
      <strong>{formatValue(value)}</strong>
    </div>;
  };
  return <div className="metric-comparison-block" aria-label={`${label}: company compared with industry bucket`}>
    <div className="metric-comparison-label"><strong>{label}</strong>{spread && <small>Lower is steadier</small>}</div>
    {row("Company", companyValue, "company")}
    {row(bucketLabel, bucketValue, "bucket")}
  </div>;
}

type DistributionSplit = {
  nonOutliers: DistributionObservation[];
  outliers: DistributionObservation[];
  lowerFence: number | null;
  upperFence: number | null;
  sufficientData: boolean;
};

function splitExtremeOutliers(observations: DistributionObservation[]): DistributionSplit {
  const finite = observations.filter((observation) => Number.isFinite(observation.value));
  const bounds = extremeOutlierBounds(finite.map((observation) => observation.value));
  return {
    nonOutliers: finite.filter((observation) => !isExtremeOutlier(observation.value, bounds)),
    outliers: finite.filter((observation) => isExtremeOutlier(observation.value, bounds)),
    lowerFence: bounds.lowerFence,
    upperFence: bounds.upperFence,
    sufficientData: bounds.sufficientData,
  };
}

function outlierShare(outliers: number, observations: number): string {
  if (!observations || !outliers) return "0%";
  const percentage = (outliers / observations) * 100;
  return percentage < 0.1 ? "<0.1%" : `${percentage.toFixed(percentage < 10 ? 1 : 0)}%`;
}

function StandardNormalDistributionCurve({ label, companyObservations, bucketObservations, activeBucketObservations, outliers, exclusionsActive, activeSpread }: {
  label: string;
  companyObservations: DistributionObservation[];
  bucketObservations: DistributionObservation[];
  activeBucketObservations: DistributionObservation[];
  outliers: DistributionObservation[];
  exclusionsActive: boolean;
  activeSpread: number | null;
}) {
  const activeMean = arithmeticMean(activeBucketObservations.map((observation) => observation.value));
  const originalMean = arithmeticMean(bucketObservations.map((observation) => observation.value));
  if (activeMean === null || !bucketObservations.length) return <div className="distribution-unavailable">A distribution needs industry observations. None are available for this metric.</div>;
  const width = 560;
  const height = 218;
  const left = 58;
  const right = 18;
  const curveTop = 17;
  const curveBaseline = 116;
  const companyLane = 143;
  const bucketLane = 169;
  const axisY = 193;
  const plotWidth = width - left - right;
  const x = (zScore: number) => left + ((zScore + 4) / 8) * plotWidth;
  const curvePoints = Array.from({ length: 81 }, (_, index) => {
    const zScore = -4 + (index / 10);
    const relativeDensity = Math.exp(-0.5 * (zScore ** 2));
    return { x: x(zScore), y: curveBaseline - (relativeDensity * (curveBaseline - curveTop)) };
  });
  const curvePath = curvePoints.map((point, index) => `${index ? "L" : "M"}${point.x.toFixed(1)},${point.y.toFixed(1)}`).join(" ");
  const areaPath = `${curvePath} L${x(4)},${curveBaseline} L${x(-4)},${curveBaseline} Z`;
  const standardize = (value: number) => {
    const denominator = activeSpread !== null && activeSpread > 0 ? activeSpread : 1;
    return Math.max(-4, Math.min(4, (value - activeMean) / denominator));
  };
  const outlierSet = new Set(outliers);
  const nonOutliers = bucketObservations.filter((observation) => !outlierSet.has(observation));
  const markerTitle = (observation: DistributionObservation, kind: string) => {
    const rawZScore = activeSpread !== null && activeSpread > 0 ? (observation.value - activeMean) / activeSpread : 0;
    const clipped = Math.abs(rawZScore) > 4 ? " · shown at the ±4σ edge" : "";
    return `${kind} · ${observation.label} · ${formatLevelPercent(observation.value)} · ${rawZScore.toFixed(1)} standard deviations from the current bucket average${clipped}`;
  };
  return <div className="normal-distribution-view">
    <div className="distribution-chart-heading">
      <div><strong>Standard normal distribution</strong><small>Each observation is positioned by its distance from the current bucket average.</small></div>
      <span>Mean {formatLevelPercent(activeMean)} · σ {formatLevelPoints(activeSpread)}</span>
    </div>
    <div className="distribution-legend" aria-label="Distribution legend">
      <span><i className="curve" />Industry curve</span>
      <span><i className="bucket" />Bucket non-outlier</span>
      <span><i className="company" />Company</span>
      <span><i className={`outlier ${exclusionsActive ? "faded" : ""}`} />Extreme outlier</span>
    </div>
    <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label={`${label} standard normal distribution showing company observations, industry observations, and extreme outliers`}>
      <path d={areaPath} fill="#e7f0f6" />
      <path d={curvePath} fill="none" stroke="#5f8dad" strokeWidth="2" />
      <line x1={x(0)} x2={x(0)} y1={curveTop} y2={curveBaseline} stroke="#7192aa" strokeDasharray="3 4" />
      <text x="7" y={companyLane + 3} className="distribution-lane-label">Company</text>
      <text x="7" y={bucketLane + 3} className="distribution-lane-label">Bucket</text>
      {companyObservations.filter((observation) => Number.isFinite(observation.value)).map((observation, index) => {
        const markerX = x(standardize(observation.value));
        const markerY = companyLane + ((index % 2) * 7) - 3.5;
        return <polygon key={`${observation.label}-${index}`} points={`${markerX},${markerY - 4.5} ${markerX + 4.5},${markerY} ${markerX},${markerY + 4.5} ${markerX - 4.5},${markerY}`} fill="#315f86" stroke="#f8fbfd" strokeWidth="1.2"><title>{markerTitle(observation, "Company")}</title></polygon>;
      })}
      {nonOutliers.map((observation, index) => <circle key={`${observation.label}-${index}`} cx={x(standardize(observation.value))} cy={bucketLane + ((index % 3) * 6) - 6} r="3.3" fill="#8fb0c8" stroke="#4f7fa6" strokeWidth="1"><title>{markerTitle(observation, "Industry bucket")}</title></circle>)}
      {outliers.map((observation, index) => <circle className={exclusionsActive ? "distribution-outlier excluded" : "distribution-outlier"} key={`${observation.label}-${observation.value}-${index}`} cx={x(standardize(observation.value))} cy={bucketLane + ((index % 3) * 6) - 6} r="4.3" fill="#b54f5a" stroke="#7f3039" strokeWidth="1.2"><title>{markerTitle(observation, exclusionsActive ? "Excluded extreme outlier" : "Extreme outlier")}</title></circle>)}
      <line x1={left} x2={width - right} y1={axisY} y2={axisY} stroke="#a7b2b9" />
      {[-3, -2, -1, 0, 1, 2, 3].map((tick) => <g key={tick}><line x1={x(tick)} x2={x(tick)} y1={axisY} y2={axisY + 4} stroke="#8798a3" /><text x={x(tick)} y={axisY + 15} textAnchor="middle" className="distribution-axis-label">{tick === 0 ? "Mean" : `${tick > 0 ? "+" : "−"}${Math.abs(tick)}σ`}</text></g>)}
    </svg>
    <p className="distribution-caption">The curve is a normal reference, not a claim that company results are perfectly normal. Hover over a marker for its company, year and value.{exclusionsActive && originalMean !== null ? ` The curve positions now use the recalculated bucket spread after excluding the red observations.` : ""}</p>
  </div>;
}

function ExtremeOutlierPanel({ split, totalObservations, exclusionsActive, onToggle }: { split: DistributionSplit; totalObservations: number; exclusionsActive: boolean; onToggle: () => void }) {
  const percentage = outlierShare(split.outliers.length, totalObservations);
  if (!split.sufficientData) return <aside className="outlier-explanation neutral"><div><strong>Outlier check unavailable</strong><p>At least four bucket observations are needed before an extreme-outlier rule is reliable.</p></div></aside>;
  if (!split.outliers.length) return <aside className="outlier-explanation clear"><div><strong>0% extreme outliers</strong><p>No bucket observation sits beyond the cautious three-IQR extreme fence. The reported spread uses all {totalObservations} observations.</p></div><button type="button" disabled>No extreme outliers to exclude</button></aside>;
  return <aside className={`outlier-explanation flagged ${exclusionsActive ? "excluded" : ""}`}>
    <div className="outlier-explanation-copy">
      <strong>{percentage} extreme outliers · {split.outliers.length} of {totalObservations}</strong>
      <p>These observations sit unusually far beyond the bucket’s middle 50%—outside {formatLevelPercent(split.lowerFence)} to {formatLevelPercent(split.upperFence)}. Because standard deviation gives extra weight to large distances, a small number can materially widen the reported spread. An outlier may reflect a one-off event, a low comparison base or a data issue; it is not automatically incorrect.</p>
      <ul>{split.outliers.slice(0, 4).map((observation) => {
        const distance = observation.value < split.lowerFence! ? split.lowerFence! - observation.value : observation.value - split.upperFence!;
        return <li key={`${observation.label}-${observation.value}`}><strong>{formatLevelPercent(observation.value)}</strong><span>{observation.label} · {formatLevelPoints(distance)} beyond the extreme fence</span></li>;
      })}</ul>
      {split.outliers.length > 4 && <small>+ {split.outliers.length - 4} additional extreme observations are shown in red on the curve.</small>}
    </div>
    <button type="button" role="switch" aria-checked={exclusionsActive} onClick={onToggle}><span aria-hidden="true"><i /></span>{exclusionsActive ? "Restore outliers to spread" : "Exclude extreme outliers from spread"}</button>
  </aside>;
}

function metricComparisonTakeaway(label: string, data: InsightMetricData, aboveMeaning?: string, belowMeaning?: string): string {
  if (data.companyMedian === null || data.bucketMedian === null) return `There are not enough observations to compare typical ${label.toLowerCase()} with the industry bucket.`;
  const difference = data.companyMedian - data.bucketMedian;
  if (Math.abs(difference) < 0.5) return `Typical ${label.toLowerCase()} was close to the industry bucket median.`;
  const position = difference > 0 ? "above" : "below";
  const meaning = difference > 0 ? aboveMeaning : belowMeaning;
  return `Typical ${label.toLowerCase()} was ${Math.abs(difference).toFixed(1)} pts ${position} the industry bucket.${meaning ? ` ${meaning}` : ""}`;
}

function metricSpreadTakeaway(data: InsightMetricData): string {
  if (data.companySpread === null || data.bucketSpread === null) return "A consistency comparison is unavailable because there are too few annual observations.";
  const difference = data.companySpread - data.bucketSpread;
  if (Math.abs(difference) < 0.5) return "The company and industry bucket showed a similar degree of year-to-year variation.";
  return difference < 0
    ? `The company’s spread was ${Math.abs(difference).toFixed(1)} pts narrower, so its annual results were steadier than the bucket.`
    : `The company’s spread was ${Math.abs(difference).toFixed(1)} pts wider, so its annual results were less steady than the bucket.`;
}

function MetricInsightCard({ label, data, aboveMeaning, belowMeaning }: { label: string; data: InsightMetricData; aboveMeaning?: string; belowMeaning?: string }) {
  const [excludeExtremeOutliers, setExcludeExtremeOutliers] = useState(false);
  const distributionFingerprint = data.bucketDistribution.map((observation) => `${observation.label}:${observation.value}`).join("|");
  useEffect(() => setExcludeExtremeOutliers(false), [distributionFingerprint]);
  const split = splitExtremeOutliers(data.bucketDistribution);
  const bucketDistributionAvailable = data.bucketDistribution.length > 0;
  const exclusionsActive = excludeExtremeOutliers && split.outliers.length > 0;
  const activeBucketObservations = exclusionsActive ? split.nonOutliers : data.bucketDistribution;
  const adjustedBucketSpread = exclusionsActive ? sampleStandardDeviation(split.nonOutliers.map((observation) => observation.value)) : data.bucketSpread;
  const adjustedData = { ...data, bucketSpread: adjustedBucketSpread };
  const medianDifference = data.companyMedian !== null && data.bucketMedian !== null ? data.companyMedian - data.bucketMedian : null;
  const badge = data.companyMedian !== null && data.companyMedian < 0
    ? { label: "Negative typical result", tone: "negative" }
    : medianDifference === null
      ? { label: "Comparison unavailable", tone: "neutral" }
      : Math.abs(medianDifference) < 0.5
        ? { label: "Near bucket", tone: "neutral" }
        : { label: medianDifference > 0 ? "Above bucket" : "Below bucket", tone: "comparison" };
  return <article className="metric-insight-card">
    <header><h6>{label}</h6><span className={`metric-summary-badge ${badge.tone}`}>{badge.label}</span></header>
    <p className="metric-primary-takeaway">{metricComparisonTakeaway(label, data, aboveMeaning, belowMeaning)}</p>
    <MetricComparisonBars label="Typical result (median)" companyValue={data.companyMedian} bucketValue={data.bucketMedian} />
    <section className="metric-spread-section">
      <MetricComparisonBars label="Year-to-year spread" companyValue={data.companySpread} bucketValue={adjustedBucketSpread} spread bucketLabel={exclusionsActive ? "Bucket · adjusted" : "Industry bucket"} />
      {exclusionsActive && <p className="spread-recalculation">Industry bucket spread recalculated: <del>{formatLevelPoints(data.bucketSpread)}</del><strong>{formatLevelPoints(adjustedBucketSpread)}</strong><span>{split.outliers.length} extreme {split.outliers.length === 1 ? "observation" : "observations"} excluded from this calculation only.</span></p>}
      <p className="metric-spread-takeaway">{metricSpreadTakeaway(adjustedData)}</p>
      {bucketDistributionAvailable ? <>
        <StandardNormalDistributionCurve label={label} companyObservations={data.companyDistribution} bucketObservations={data.bucketDistribution} activeBucketObservations={activeBucketObservations} outliers={split.outliers} exclusionsActive={exclusionsActive} activeSpread={adjustedBucketSpread} />
        <ExtremeOutlierPanel split={split} totalObservations={data.bucketDistribution.length} exclusionsActive={exclusionsActive} onToggle={() => setExcludeExtremeOutliers((current) => !current)} />
      </> : <aside className="distribution-unavailable"><strong>{data.bucketObservations ? "Detailed industry distribution temporarily unavailable" : "Industry distribution unavailable"}</strong><p>{data.bucketObservations ? "This Research API response includes the original bucket standard deviation but not its individual peer observations. The company remains available on the research shelf; distribution and outlier controls will appear automatically when the updated API response is available." : "There are no industry observations available for this metric."}</p></aside>}
    </section>
    <small className="metric-observation-count">Based on {data.companyObservations} company observations and {data.bucketObservations} bucket observations.{exclusionsActive ? ` The adjusted spread uses ${split.nonOutliers.length} bucket observations.` : ""}</small>
  </article>;
}

function ProfitabilityStatisticsTable({ analysis }: { analysis: ResearchShelfAnalysis["profitability"] }) {
  return <section className="statistics-summary profitability-statistics-summary">
    <StatisticalReadingGuide />
    <div className="metric-insight-grid profitability-insight-grid">{profitabilityMetrics.map((metric) => {
    const companyStatistics = analysis.statistics[metric.key];
    const industryStatistics = analysis.industryStatistics[metric.key];
    return <MetricInsightCard key={metric.key} label={metric.label} aboveMeaning={metric.aboveMeaning} belowMeaning={metric.belowMeaning} data={{
      companyMedian: companyStatistics.median,
      companySpread: companyStatistics.standardDeviation,
      companyObservations: companyStatistics.observations,
      companyDistribution: companyStatistics.distribution,
      bucketMedian: industryStatistics.median,
      bucketSpread: industryStatistics.standardDeviation,
      bucketObservations: industryStatistics.observations,
      bucketDistribution: industryStatistics.distribution,
    }} />;
  })}</div>
  </section>;
}

function ProfitabilityIndustryChart({ title, unit, data }: { title: string; unit: string; data: IndustryLevelPoint[] }) {
  return <PercentageLevelChart
    title={title}
    subtitle="Percentage appears inside each bar · absolute amount appears above"
    absoluteUnit={unit}
    data={data.map((item) => ({ label: `FY ${item.year}`, values: [item.companyValue, item.industryMedian], absoluteValues: [item.companyAbsoluteValue, item.industryMedianAbsoluteValue] }))}
    series={[
      researchChartSeries("Company", 0),
      researchChartSeries("Industry median", 1),
    ]}
  />;
}

function PercentageLevelChart({ title, subtitle, absoluteUnit, data, series }: {
  title: string;
  subtitle: string;
  absoluteUnit: string;
  data: Array<{ label: string; values: Array<number | null>; absoluteValues: Array<number | null> }>;
  series: Array<{ label: string; color: string; negativeColor: string }>;
}) {
  const available = data.flatMap((item) => item.values.filter((value): value is number => value !== null && Number.isFinite(value)));
  if (!available.length) return <section className="shelf-chart"><div className="shelf-chart-heading"><div><h5>{title}</h5><p>{subtitle}</p></div><span>Bar: percentage · top label: {absoluteUnit}</span></div><div className="empty-chart">No complete annual observations are available.</div></section>;
  const left = 72;
  const right = 20;
  const width = Math.max(940, left + right + (data.length * (series.length > 2 ? 180 : 130)));
  const height = 330;
  const top = 48;
  const bottom = 58;
  const minimum = Math.min(0, ...available);
  const maximum = Math.max(0, ...available);
  const rawSpread = maximum - minimum || 1;
  const padding = rawSpread * 0.08;
  const minValue = minimum < 0 ? minimum - padding : 0;
  const maxValue = maximum + padding;
  const spread = maxValue - minValue || 1;
  const plotHeight = height - top - bottom;
  const plotWidth = width - left - right;
  const y = (value: number) => top + ((maxValue - value) / spread) * plotHeight;
  const baseline = y(0);
  const groupWidth = plotWidth / Math.max(data.length, 1);
  const barGap = series.length > 2 ? 6 : 10;
  const barWidth = Math.min(series.length > 2 ? 25 : 34, Math.max(9, (groupWidth - 18 - (series.length - 1) * barGap) / series.length));
  const clusterWidth = series.length * barWidth + (series.length - 1) * barGap;
  const tickValues = Array.from({ length: 5 }, (_, index) => minValue + (spread * index) / 4);
  return <section className="shelf-chart profitability-chart">
    <div className="shelf-chart-heading"><div><h5>{title}</h5><p>{subtitle}</p></div><span>Bar: percentage · top label: {absoluteUnit}</span></div>
    <div className="chart-legend">{series.map((item) => <span key={item.label}><i className="series-swatch" style={{ background: `linear-gradient(90deg, ${item.color} 0 50%, ${item.negativeColor} 50% 100%)` }} /><span>{item.label}<small>positive | negative</small></span></span>)}</div>
    <div className="clustered-chart-scroll" data-horizontal-scroll><svg viewBox={`0 0 ${width} ${height}`} style={{ minWidth: width }} role="img" aria-label={`${title}, annual percentage values`}>
      {tickValues.map((tick) => <g key={tick}><line x1={left} x2={width - right} y1={y(tick)} y2={y(tick)} stroke="#ded6c6" strokeDasharray="4 6" /><text x={left - 9} y={y(tick) + 4} textAnchor="end" className="chart-axis-text">{formatCompactPercent(tick)}</text></g>)}
      <line x1={left} x2={width - right} y1={baseline} y2={baseline} stroke="#746c60" strokeWidth="1.3" />
      {data.map((item, dataIndex) => {
        const center = left + groupWidth * dataIndex + groupWidth / 2;
        return <g key={item.label}>{item.values.map((value, seriesIndex) => {
          if (value === null || !Number.isFinite(value)) return null;
          const definition = series[seriesIndex];
          const x = center - clusterWidth / 2 + seriesIndex * (barWidth + barGap);
          const valueY = y(value);
          const rectY = Math.min(valueY, baseline);
          const rectHeight = Math.max(1, Math.abs(baseline - valueY));
          const absoluteValue = item.absoluteValues[seriesIndex];
          const barColor = value < 0 ? definition.negativeColor : definition.color;
          return <g key={definition.label}><rect x={x} y={rectY} width={barWidth} height={rectHeight} fill={barColor} rx="2"><title>{`${item.label} · ${definition.label}: ${formatLevelPercent(value)}; absolute value ${formatChartAbsolute(absoluteValue)} ${absoluteUnit}`}</title></rect><text x={x + barWidth / 2} y={Math.max(12, rectY - 7)} textAnchor="middle" className="bar-absolute-label" style={{ fill: barColor }}>{formatChartAbsolute(absoluteValue)}</text><text x={x + barWidth / 2} y={rectY + rectHeight / 2} textAnchor="middle" dominantBaseline="middle" className="bar-percent-in-bar">{formatCompactPercent(value)}</text></g>;
        })}<text x={center} y={height - 25} textAnchor="middle" className="chart-axis-text interval-label">{item.label}</text></g>;
      })}
    </svg></div>
  </section>;
}

function ProfitabilityYearTable({ rows, bands }: { rows: ProfitabilityYearPoint[]; bands: Record<ProfitabilityMetricKey, ProfitabilityMetricBands> }) {
  return <section className="raw-income-section profitability-year-section"><div className="shelf-chart-heading"><div><h5>Annual margin and cost-structure record</h5><p>Selected company values for every year in the requested range</p></div><span>Industry-relative performance bands</span></div><PerformanceBandLegend /><div className="raw-income-table-wrap" data-horizontal-scroll><table className="raw-income-table profitability-year-table"><thead><tr><th>Fiscal year</th>{profitabilityMetrics.map((metric) => <th key={metric.key}>{metric.compactLabel}</th>)}</tr></thead><tbody>{rows.map((row) => <tr key={row.year}><th>FY {row.year}</th>{profitabilityMetrics.map((metric) => <PerformanceBandCell key={metric.key} value={row[metric.key]} thresholds={bands[metric.key].level} />)}</tr>)}</tbody></table></div></section>;
}

type PerformanceBand = "negative" | "moderate" | "good" | "excellent" | "unavailable";

function performanceBand(value: number | null, thresholds: PerformanceThresholds): PerformanceBand {
  if (value === null || !Number.isFinite(value) || thresholds.lowerQuartile === null || thresholds.median === null || thresholds.upperQuartile === null) return "unavailable";
  if (thresholds.direction === "higher") return value < thresholds.lowerQuartile ? "negative" : value < thresholds.median ? "moderate" : value < thresholds.upperQuartile ? "good" : "excellent";
  return value > thresholds.upperQuartile ? "negative" : value > thresholds.median ? "moderate" : value > thresholds.lowerQuartile ? "good" : "excellent";
}

function bandLabel(band: PerformanceBand): string {
  return band === "unavailable" ? "Band unavailable" : `${band[0].toUpperCase()}${band.slice(1)} performance`;
}

function PerformanceBandCell({ value, thresholds, suffix = "" }: { value: number | null; thresholds: PerformanceThresholds; suffix?: string }) {
  const band = performanceBand(value, thresholds);
  const thresholdSummary = thresholds.observations
    ? `Observed bucket quartiles: ${formatLevelPercent(thresholds.lowerQuartile)}, ${formatLevelPercent(thresholds.median)}, ${formatLevelPercent(thresholds.upperQuartile)}. ${bandLabel(band)}.`
    : "Industry performance band is unavailable.";
  return <td className={`growth-band profitability-band ${band}`} title={thresholdSummary}><span>{value === null || !Number.isFinite(value) ? "Not available" : `${formatLevelPercent(value).replace(/%$/, "")}${suffix || "%"}`}</span></td>;
}

function formatLevelPercent(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return "Not available";
  return `${value < 0 ? "−" : ""}${Math.abs(value).toFixed(1)}%`;
}

function formatLevelPoints(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return "Not available";
  return `${Math.abs(value).toFixed(1)} pts`;
}

function GrowthComparisonTable({ shelf }: { shelf: ResearchShelfAnalysis["growthComparisons"] }) {
  const rows: Array<{ label: string; comparison: GrowthComparison }> = [
    { label: "Revenue growth", comparison: shelf.revenue },
    { label: "Gross profit growth", comparison: shelf.grossProfit },
    { label: "Operating income growth", comparison: shelf.operatingIncome },
  ];
  return (
    <section className="statistics-summary growth-statistics-summary">
      <StatisticalReadingGuide />
      <div className="metric-insight-grid growth-insight-grid">{rows.map(({ label, comparison }) => <MetricInsightCard key={label} label={label} data={{
        companyMedian: comparison.company.median,
        companySpread: comparison.company.standardDeviation,
        companyObservations: comparison.company.observations,
        companyDistribution: comparison.company.distribution,
        bucketMedian: comparison.industryBucket.median,
        bucketSpread: comparison.industryBucket.standardDeviation,
        bucketObservations: comparison.industryBucket.observations,
        bucketDistribution: comparison.industryBucket.distribution,
      }} />)}</div>
    </section>
  );
}

function yearInterval(fromYear: number, toYear: number): string {
  return `FY${String(fromYear).slice(-2)}–FY${String(toYear).slice(-2)}`;
}

function IndustryComparisonChart({ title, unit, data }: { title: string; unit: string; data: IndustryDeltaPoint[] }) {
  return <ClusteredColumnChart
    title={title}
    subtitle="Bar height shows YoY percentage change · absolute delta appears above each bar"
    absoluteUnit={unit}
    data={data.map((item) => ({ interval: yearInterval(item.fromYear, item.toYear), percentValues: [item.companyChangePercent, item.industryMedianChangePercent], absoluteValues: [item.company, item.industryMedian] }))}
    series={[
      researchChartSeries("Company change", 0),
      researchChartSeries("Industry median change", 1),
    ]}
  />;
}

function ClusteredColumnChart({ title, subtitle, absoluteUnit, data, series }: {
  title: string;
  subtitle: string;
  absoluteUnit: string;
  data: Array<{ interval: string; percentValues: Array<number | null>; absoluteValues: Array<number | null> }>;
  series: Array<{ label: string; color: string; negativeColor: string }>;
}) {
  const available = data.flatMap((item) => item.percentValues.filter((value): value is number => value !== null && Number.isFinite(value)));
  if (!available.length) return <section className="shelf-chart"><div className="shelf-chart-heading"><div><h5>{title}</h5><p>{subtitle}</p></div><span>YoY change (%) · labels: {absoluteUnit}</span></div><div className="empty-chart">No complete year-over-year observations are available.</div></section>;
  const width = 940;
  const height = 330;
  const left = 78;
  const right = 20;
  const top = 40;
  const bottom = 62;
  const minValue = Math.min(0, ...available);
  const maxValue = Math.max(0, ...available);
  const spread = maxValue - minValue || 1;
  const plotHeight = height - top - bottom;
  const plotWidth = width - left - right;
  const y = (value: number) => top + ((maxValue - value) / spread) * plotHeight;
  const baseline = y(0);
  const groupWidth = plotWidth / Math.max(data.length, 1);
  const barGap = 10;
  const barWidth = Math.min(30, Math.max(12, (groupWidth - 16 - (series.length - 1) * barGap) / series.length));
  const clusterWidth = series.length * barWidth + (series.length - 1) * barGap;
  const tickValues = Array.from({ length: 5 }, (_, index) => minValue + (spread * index) / 4);
  return (
    <section className="shelf-chart">
      <div className="shelf-chart-heading"><div><h5>{title}</h5><p>{subtitle}</p></div><span>YoY change (%) · labels: {absoluteUnit}</span></div>
      <div className="chart-legend">{series.map((item) => <span key={item.label}><i className="series-swatch" style={{ background: `linear-gradient(90deg, ${item.color} 0 50%, ${item.negativeColor} 50% 100%)` }} /><span>{item.label}<small>positive | negative</small></span></span>)}</div>
      <div className="clustered-chart-scroll" data-horizontal-scroll><svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label={`${title}, percentage change with absolute labels in ${absoluteUnit}`}>
        {tickValues.map((tick) => <g key={tick}><line x1={left} x2={width - right} y1={y(tick)} y2={y(tick)} stroke="#ded6c6" strokeDasharray="4 6" /><text x={left - 10} y={y(tick) + 4} textAnchor="end" className="chart-axis-text">{formatCompactPercent(tick)}</text></g>)}
        <line x1={left} x2={width - right} y1={baseline} y2={baseline} stroke="#746c60" strokeWidth="1.3" />
        {data.map((item, dataIndex) => {
          const center = left + groupWidth * dataIndex + groupWidth / 2;
          return <g key={item.interval}>{item.percentValues.map((percentValue, seriesIndex) => {
            if (percentValue === null || !Number.isFinite(percentValue)) return null;
            const x = center - clusterWidth / 2 + seriesIndex * (barWidth + barGap);
            const valueY = y(percentValue);
            const rectY = Math.min(valueY, baseline);
            const rectHeight = Math.max(1, Math.abs(baseline - valueY));
            const absoluteValue = item.absoluteValues[seriesIndex];
            const barColor = percentValue < 0 ? series[seriesIndex].negativeColor : series[seriesIndex].color;
            return <g key={series[seriesIndex].label}>
              <rect x={x} y={rectY} width={barWidth} height={rectHeight} fill={barColor} rx="2"><title>{`${item.interval} · ${series[seriesIndex].label}: ${formatPercent(percentValue)}; absolute delta ${formatChartAbsolute(absoluteValue)} ${absoluteUnit}`}</title></rect>
              <text x={x + barWidth / 2} y={Math.max(11, rectY - 6)} textAnchor="middle" className="bar-absolute-label" style={{ fill: barColor }}>{formatChartAbsolute(absoluteValue)}</text>
              <text x={x + barWidth / 2} y={rectY + rectHeight / 2} textAnchor="middle" dominantBaseline="middle" className="bar-percent-in-bar">{formatCompactPercent(percentValue)}</text>
            </g>;
          })}<text x={center} y={height - 27} textAnchor="middle" className="chart-axis-text interval-label">{item.interval}</text></g>;
        })}
      </svg></div>
    </section>
  );
}

function RawIncomeTable({ company, rows }: { company: Company; rows: RawIncomePoint[] }) {
  return <section className="raw-income-section"><div className="shelf-chart-heading"><div><h5>Company reported figures and YoY movement</h5><p>Raw annual values for the selected range</p></div><span>{company.currency.startsWith("US$") ? "USD millions" : "INR crores"}</span></div><div className="growth-band-key"><span className="negative">Negative &lt; 0%</span><span className="moderate">Moderate 0–15%</span><span className="good">Good 15–32%</span><span className="excellent">Excellent &gt; 32%</span></div><div className="raw-income-table-wrap" data-horizontal-scroll><table className="raw-income-table"><thead><tr><th>Fiscal year</th><th>Revenue</th><th>YoY revenue change</th><th>Gross profit</th><th>YoY gross profit change</th><th>Operating income</th><th>YoY operating income change</th></tr></thead><tbody>{rows.map((row) => <tr key={row.year}><th>FY {row.year}</th><td>{formatNullableAmount(company, row.revenue)}</td><GrowthBandCell value={row.revenueChangePercent} /><td>{formatNullableAmount(company, row.grossProfit)}</td><GrowthBandCell value={row.grossProfitChangePercent} /><td>{formatNullableAmount(company, row.operatingIncome)}</td><GrowthBandCell value={row.operatingIncomeChangePercent} /></tr>)}</tbody></table></div></section>;
}

function IndustryConstituentManager({ constituents, customized, country, busy, message, onChange }: {
  constituents: IndustryConstituent[];
  customized: boolean;
  country: "USA" | "India";
  busy: boolean;
  message: string | null;
  onChange?: (constituentIds?: string[]) => Promise<void>;
}) {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<CatalogCompany[]>([]);
  const [searching, setSearching] = useState(false);
  const [searchError, setSearchError] = useState<string | null>(null);
  const constituentIds = constituents.map((item) => item.id);
  const selectedIds = new Set(constituentIds);

  useEffect(() => {
    setSearchError(null);
    if (!onChange || query.trim().length < 2) { setResults([]); setSearching(false); return; }
    let cancelled = false;
    const timer = window.setTimeout(async () => {
      setSearching(true);
      try {
        const matches = await searchCompanyCatalog(query, country);
        if (!cancelled) setResults(matches.filter((item) => item.data_access === "normalized" && !selectedIds.has(item.id)).slice(0, 8));
      } catch (error) {
        if (!cancelled) setSearchError(error instanceof Error ? error.message : "Constituent search failed.");
      } finally {
        if (!cancelled) setSearching(false);
      }
    }, 250);
    return () => { cancelled = true; window.clearTimeout(timer); };
  }, [query, country, onChange, constituentIds.join("|")]);

  const addConstituent = async (candidate: CatalogCompany) => {
    if (!onChange || busy || constituentIds.length >= 100) return;
    await onChange([...constituentIds, candidate.id]);
    setQuery("");
    setResults([]);
  };
  const removeConstituent = async (id: string) => {
    if (!onChange || busy || constituentIds.length <= 1) return;
    await onChange(constituentIds.filter((item) => item !== id));
  };

  return <section className="industry-constituents-section">
    <div className="constituents-heading">
      <div><small>Benchmark composition</small><h5>Industry bucket constituents</h5><p>These companies feed every industry median, deviation and comparison chart above.</p></div>
      <div className="constituent-heading-actions"><span>{constituents.length} selected</span>{customized && onChange && <button disabled={busy} onClick={() => onChange()}>Restore default bucket</button>}</div>
    </div>
    <div className="constituent-session-note"><strong>{customized ? "Custom session basket" : "Default Research bucket"}</strong><span>Changes apply to this browser session only and do not alter the master industry taxonomy.</span></div>
    <div className="constituent-list">
      {constituents.map((item) => <article key={item.id}>
        <span className="constituent-monogram">{item.name[0]}</span>
        <div><strong>{item.name}</strong><small>{item.ticker} · {item.industryBucket}</small><span>Revenue {item.revenueObservations} yrs · Gross profit {item.grossProfitObservations} yrs · Operating income {item.operatingIncomeObservations} yrs</span></div>
        {onChange && <button disabled={busy || constituents.length <= 1} onClick={() => removeConstituent(item.id)} aria-label={`Remove ${item.name} from the comparison bucket`}>Remove</button>}
      </article>)}
      {!constituents.length && <div className="constituent-empty">Constituent identities are unavailable from the current provider response.</div>}
    </div>
    {onChange ? <div className="constituent-editor">
      <div><strong>Add a company</strong><small>Search the {country} Research catalogue. Up to 100 companies can be used.</small></div>
      <label><span>⌕</span><input value={query} disabled={busy || constituents.length >= 100} onChange={(event) => setQuery(event.target.value)} placeholder="Search company or symbol" /></label>
      {query.trim().length >= 2 && <div className="constituent-search-results">
        {searching && <span>Searching…</span>}
        {!searching && !searchError && !results.length && <span>No unselected Research companies found.</span>}
        {searchError && <span className="error">{searchError}</span>}
        {results.map((candidate) => <button key={candidate.id} disabled={busy} onClick={() => addConstituent(candidate)}><span>{candidate.name[0]}</span><div><strong>{candidate.name}</strong><small>{candidate.ticker} · {candidate.industryBucket ?? "Unclassified"}</small></div><b>＋ Add</b></button>)}
      </div>}
    </div> : <div className="constituent-readonly">Open this company in Discover to customize its comparison basket.</div>}
    {busy && <div className="constituent-status" aria-live="polite">Recalculating every industry comparison…</div>}
    {!busy && message && <div className="constituent-status" aria-live="polite">{message}</div>}
  </section>;
}

function GrowthBandCell({ value }: { value: number | null }) {
  const band = value === null || !Number.isFinite(value) ? "unavailable" : value < 0 ? "negative" : value < 15 ? "moderate" : value <= 32 ? "good" : "excellent";
  return <td className={`growth-band ${band}`}><span>{formatPercent(value)}</span></td>;
}

function formatPercent(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return "Not available";
  return `${value > 0 ? "+" : value < 0 ? "−" : ""}${Math.abs(value).toFixed(1)}%`;
}

function formatPercentagePoints(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return "Not available";
  return `${value > 0 ? "+" : value < 0 ? "−" : ""}${Math.abs(value).toFixed(1)} pts`;
}

function formatCompactPercent(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return "—";
  return `${value > 0 ? "+" : value < 0 ? "−" : ""}${Math.abs(value).toFixed(1)}%`;
}

function formatChartAbsolute(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return "—";
  return compactNumber(value).replace("-", "−");
}

function formatCompanyAmount(company: Company, value: number): string {
  const sign = value < 0 ? "−" : "";
  const amount = Math.abs(value).toLocaleString(company.currency.startsWith("US$") ? "en-US" : "en-IN", { maximumFractionDigits: 1, notation: Math.abs(value) >= 100_000 ? "compact" : "standard" });
  return company.currency.startsWith("US$") ? `${sign}$${amount}m` : `${sign}₹${amount} cr`;
}

function formatNullableAmount(company: Company, value: number | null): string {
  return value === null ? "—" : formatCompanyAmount(company, value);
}

function compactNumber(value: number): string {
  return Intl.NumberFormat("en", { notation: "compact", maximumFractionDigits: 1 }).format(value);
}

function CompanyDetail({ company, watched, toggleWatch, navigate }: { company: Company; watched: boolean; toggleWatch: (id: string) => void; navigate: (page: Page) => void }) {
  const availableMetrics = (Object.keys(metricMeta) as MetricKey[]).filter((key) => company.metrics[key].length);
  const [metric, setMetric] = useState<MetricKey>(availableMetrics[0] ?? "revenue");
  useEffect(() => { if (!company.metrics[metric].length && availableMetrics[0]) setMetric(availableMetrics[0]); }, [company.id, metric]);
  const series = company.metrics[metric];
  const notes: Record<MetricKey, string> = { revenue: company.notes.growth, operatingMargin: company.notes.profitability, freeCashFlow: company.notes.cash, netDebt: company.notes.debt };
  return (
    <div className="page-wrap company-page">
      <button className="back-button" onClick={() => navigate("discover")}>← Back to discover</button>
      <section className="company-profile">
        <div className="company-title"><span className="company-monogram large">{company.name[0]}</span><div><p>{company.symbol} · {company.dataMode === "research-db" ? "Private Research database session" : company.dataMode === "sec-live" ? "Live SEC session" : "Illustrative preview"}</p><h1>{company.name}</h1><span>{company.sector}</span></div></div>
        <button className={`button ${watched ? "secondary" : "primary"}`} onClick={() => toggleWatch(company.id)}>{watched ? "♥ In your watchlist" : "♡ Add to watchlist"}</button>
      </section>
      <p className="company-description">{company.description}</p>
      <div className="company-facts">{company.founded && <span>Founded <strong>{company.founded}</strong></span>}{company.employees && <span>Employees <strong>{company.employees}</strong></span>}<span>Latest period <strong>{company.reportingPeriod}</strong></span><span>Session pull <strong>{company.updatedAt}</strong></span><span>Persistence <strong>Browser memory only</strong></span></div>

      <ResearchDepthMap />

      <section className="metric-overview">
        <p className="eyebrow">Financial history · Layer III</p><h2>Interrogate the reported record</h2>
        {availableMetrics.length ? <><div className="metric-tabs" role="tablist">
          {availableMetrics.map((key) => {
            const itemSeries = company.metrics[key];
            return <button role="tab" aria-selected={metric === key} className={metric === key ? "active" : ""} key={key} onClick={() => setMetric(key)}><small>{metricMeta[key].short}</small><strong>{formatCompanyMetric(company, key, latest(itemSeries).value)}</strong><span>{metricMeta[key].label}</span></button>;
          })}
        </div>
        <div className="metric-detail">
          <div className="chart-column">
            <div className="chart-title"><div><span>{metricMeta[metric].label}</span><strong>{formatCompanyMetric(company, metric, latest(series).value)}</strong></div><small>{company.currency} · Annual</small></div>
            <LineChart series={series} color={metric === "revenue" ? "#b58932" : metric === "operatingMargin" ? "#7d3f49" : metric === "freeCashFlow" ? "#5a6f78" : "#9b6741"} />
          </div>
          <aside className="plain-insight"><span>In plain language</span><h3>{notes[metric]}</h3><p>{metricMeta[metric].explanation}</p><details><summary>How this metric is calculated</summary><p>{company.dataMode === "research-db" ? "This series is calculated from approved fields retrieved from the shared TaRaSha Research database for the selected years." : company.dataMode === "sec-live" ? "This derived series uses the SEC XBRL tags described in the filing-facts section below. It appears only when the required tagged concepts are available." : "This preview uses a simplified illustrative series."}</p></details></aside>
        </div>
        </> : <Empty title="No standard derived series found" text="Open the filing facts below. The issuer may use non-standard XBRL tags for these concepts." />}
      </section>
      {company.statements && <StatementExplorer company={company} />}
      <DataTrust company={company} />
    </div>
  );
}

function formatCompanyMetric(company: Company, key: MetricKey, value: number): string {
  if (key === "operatingMargin") return `${value.toFixed(1)}%`;
  if (company.currency.startsWith("US$")) return `${value < 0 ? "−" : ""}$${Math.abs(value).toLocaleString("en-US", { maximumFractionDigits: 1 })}m`;
  return formatMetric(key, value);
}

function formatStatementValue(company: Company, fact: StatementFact, value: number): string {
  if (fact.unit === "US$ per share") return `$${value.toFixed(2)}`;
  if (fact.unit === "million shares") return `${value.toLocaleString("en-US", { maximumFractionDigits: 1 })}m`;
  if (fact.unit === "₹ crore") return `${value < 0 ? "−" : ""}₹${Math.abs(value).toLocaleString("en-IN", { maximumFractionDigits: 1 })} cr`;
  if (!company.currency.startsWith("US$")) return `${value.toLocaleString("en-IN", { maximumFractionDigits: 1 })}`;
  return `${value < 0 ? "−" : ""}$${Math.abs(value).toLocaleString("en-US", { maximumFractionDigits: 1 })}m`;
}

function StatementExplorer({ company }: { company: Company }) {
  const groups = (company.statements ?? []).filter((group) => group.facts.length);
  const [activeKey, setActiveKey] = useState<StatementGroup["key"]>(groups[0]?.key ?? "income");
  const active = groups.find((group) => group.key === activeKey) ?? groups[0];
  const years = [...new Set(active?.facts.flatMap((fact) => fact.values.map((value) => value.year)) ?? [])].sort((a, b) => a - b);
  return (
    <section className="statement-explorer">
      <div className="statement-heading"><div><p className="eyebrow">{company.dataMode === "research-db" ? "Imported historical financial facts" : "Directly extracted filing facts"}</p><h2>Open the statements</h2></div><span>{company.dataMode === "research-db" ? "Transient · Shared Research DB" : "Transient · SEC EDGAR"}</span></div>
      <div className="statement-tabs">{groups.map((group) => <button className={group.key === active?.key ? "active" : ""} onClick={() => setActiveKey(group.key)} key={group.key}>{group.label}<small>{group.facts.length} facts</small></button>)}</div>
      {active && <div className="statement-table-wrap"><table><thead><tr><th>Reported fact</th>{years.map((year) => <th key={year}>FY {year}</th>)}</tr></thead><tbody>{active.facts.map((fact) => { const values = new Map(fact.values.map((value) => [value.year, value.value])); return <tr key={fact.key}><td><strong>{fact.label}</strong><small>{fact.description}<br />{fact.unit}</small></td>{years.map((year) => <td key={year}>{values.has(year) ? formatStatementValue(company, fact, values.get(year)!) : "—"}</td>)}</tr>; })}</tbody></table></div>}
      <div className="filing-and-limits">
        <div><h3>{company.dataMode === "research-db" ? "Source provenance" : "Source filings"}</h3>{company.dataMode === "research-db" ? <p>Retrieved from the shared TaRaSha Research database. The underlying figures were bulk-uploaded from financial spreadsheets downloaded through <a href="https://stockanalysis.com/" target="_blank" rel="noreferrer">StockAnalysis.com</a>. No spreadsheet or financial payload is stored separately by TaRaSha Consumer.</p> : company.filings?.length ? <div className="filing-list">{company.filings.slice(0, 12).map((filing) => <a href={filing.url} target="_blank" rel="noreferrer" key={filing.accession}><span>{filing.form}</span><div><strong>{filing.title}</strong><small>Filed {filing.filed} · Period {filing.period || "not stated"}</small></div><b>↗</b></a>)}</div> : <p>No matching recent 10-K, 10-Q or earnings-related 8-K links were returned for this range.</p>}</div>
        <aside><h3>Read with these limits</h3><ul>{company.limitations?.map((item) => <li key={item}>{item}</li>)}</ul></aside>
      </div>
    </section>
  );
}

function ResearchDepthMap() {
  const layers = ["Enterprise", "Revenue engine", "Profit structure", "Cash conversion", "Financial resilience", "Capital allocation"];
  return (
    <section className="depth-map">
      <div><p className="eyebrow">Research depth map</p><h2>One company. Six connected lenses.</h2><p>The first preview opens the financial-history lens. Future iterations will connect every observation to the business and industry context around it.</p></div>
      <ol>{layers.map((layer, index) => <li className={index === 2 ? "active" : index < 2 ? "available" : ""} key={layer}><span>{String(index + 1).padStart(2, "0")}</span><strong>{layer}</strong><small>{index <= 2 ? "Explore" : "In development"}</small></li>)}</ol>
    </section>
  );
}

function LineChart({ series, color, compact = false }: { series: YearValue[]; color: string; compact?: boolean }) {
  if (!series.length) return <div className="empty-chart">No comparable annual series was tagged.</div>;
  const width = 520;
  const height = compact ? 140 : 240;
  const pad = compact ? 12 : 28;
  const values = series.map((item) => item.value);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const spread = max - min || 1;
  const points = series.map((item, index) => ({ x: series.length === 1 ? width / 2 : pad + (index * (width - pad * 2)) / (series.length - 1), y: height - pad - ((item.value - min) / spread) * (height - pad * 2), ...item }));
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
      <div className="trust-icon">✓</div><div><p className="eyebrow">Know where the number came from</p><h2>Data you can trace</h2><p>{company.dataMode === "research-db" ? "Facts were retrieved from the shared TaRaSha Research database. They originate in financial spreadsheets downloaded through StockAnalysis.com and bulk-uploaded by the Research administrator. Consumer keeps them only in this browser session." : company.dataMode === "sec-live" ? "Facts were extracted from SEC EDGAR in this browser session. Filing links remain available for verification; the extracted numbers are not saved by TaRaSha." : "This review mode uses fictional companies and illustrative numbers."}</p></div>
      <div className="source-card"><span>Dataset</span><strong>{company.source?.dataset ?? (company.dataMode === "sec-live" ? "SEC EDGAR XBRL" : "Illustrative preview")}</strong><span>Upstream</span><strong>{company.source?.upstream ?? (company.dataMode === "sec-live" ? "Issuer SEC filings" : "TaRaSha demo")}</strong><span>Persistence</span><strong>{company.source?.persistence ?? (company.dataMode === "sec-live" ? "Session memory only" : "Local demo module")}</strong><span>Use</span><strong>{company.source?.usage ?? "Educational research"}</strong><span>Session pull</span><strong>{company.updatedAt}</strong></div>
    </section>
  );
}

function Compare({ companies, openCompany }: { companies: Company[]; openCompany: (id: string) => void }) {
  const [selected, setSelected] = useState<string[]>([]);
  const selectedCompanies = companies.filter((company) => selected.includes(company.id));
  const toggle = (id: string) => setSelected((current) => current.includes(id) ? current.filter((item) => item !== id) : current.length < 3 ? [...current, id] : current);
  return (
    <div className="page-wrap">
      <PageIntro eyebrow="Compare" title="Put the same facts side by side." text="Choose up to three companies. We use matching periods and definitions wherever possible." />
      {!companies.length && <div className="market-notice"><strong>Nothing to compare yet.</strong><p>Pull two or more companies into the current session from Discover.</p></div>}
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
        {(Object.keys(metricMeta) as MetricKey[]).map((key) => <div className="comparison-row" key={key}><span><strong>{metricMeta[key].label}</strong><small>{metricMeta[key].explanation}</small></span>{selected.map((company) => { const series = company.metrics[key]; return <div key={company.id}><strong>{series.length ? formatCompanyMetric(company, key, latest(series).value) : "—"}</strong><small>{series.length ? company.reportingPeriod : "Tag unavailable"}</small></div>; })}</div>)}
      </div>
    </div>
  );
}

function Watchlist({ companies, ids, openCompany, toggleWatch, navigate }: { companies: Company[]; ids: string[]; openCompany: (id: string) => void; toggleWatch: (id: string) => void; navigate: (page: Page) => void }) {
  const watched = companies.filter((company) => ids.includes(company.id));
  return (
    <div className="page-wrap">
      <PageIntro eyebrow="Watchlist" title="Keep the companies you follow in one place." text="This list organises factual updates. It does not generate alerts to buy, sell or hold." />
      {!watched.length ? <div className="large-empty"><span>♡</span><h2>No watched company is active</h2><p>Pull company research in this session, then add its card to the watchlist.</p><button className="button primary" onClick={() => navigate("discover")}>Discover companies</button></div> : <><div className="watch-summary"><strong>{watched.length} active watched companies</strong><span>Company facts vanish when the current browser session ends</span></div><div className="company-grid wide">{watched.map((company) => <CompanyCard key={company.id} company={company} openCompany={openCompany} watched toggleWatch={toggleWatch} />)}</div></>}
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

function Footer({ navigate, isAuthenticated }: { navigate: (page: Page) => void; isAuthenticated: boolean }) {
  return (
    <footer>
      <div><Brand /><p>Go as deep as curiosity demands. Follow the business, the history and the source.</p></div>
      <div><strong>{isAuthenticated ? "Explore" : "Access"}</strong>{isAuthenticated ? <><button onClick={() => navigate("discover")}>Discover</button><button onClick={() => navigate("compare")}>Compare</button><button onClick={() => navigate("learn")}>Learn</button></> : <button onClick={() => navigate("login")}>Log in</button>}</div>
      <div><strong>Important</strong><p>TaRaSha Discover is an educational information platform, not an investment adviser or research analyst. It does not provide recommendations, price targets or suitability assessments.</p></div>
      <div className="footer-bottom"><span>© 2026 TaRaSha · Founding-user preview</span><span>Illustrative data · Not for investment decisions</span></div>
    </footer>
  );
}

export default App;
