import { useCallback, useEffect, useMemo, useState } from "react";
import {
  createReconciliationSchedule,
  loadReconciliationBatches,
  loadReconciliationCompany,
  loadReconciliationDashboard,
  loadReconciliationSchedules,
  runReconciliationBatch,
  runReconciliationSchedule,
  setReconciliationScheduleActive,
  updateReconciliationCheck,
  type ReconciliationBatch,
  type ReconciliationCheck,
  type ReconciliationCompany,
  type ReconciliationCompanyDetail,
  type ReconciliationDashboard,
  type ReconciliationSchedule,
  type ReconciliationStatement,
  type ReconciliationStatus,
} from "./reconciliationStore";

type View = "overview" | "runs" | "schedules";

const STATEMENTS: Array<{ key: ReconciliationStatement; label: string; shortLabel: string }> = [
  { key: "income_statement", label: "Income Statement", shortLabel: "Income" },
  { key: "balance_sheet", label: "Balance Sheet", shortLabel: "Balance" },
  { key: "cash_flow_statement", label: "Cash Flow Statement", shortLabel: "Cash Flow" },
];

function displayDate(value: string | null | undefined): string {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString([], { dateStyle: "medium", timeStyle: "short" });
}

function score(value: string | null): string {
  return value === null ? "—" : `${Number(value).toFixed(1)}%`;
}

function companyMatches(company: ReconciliationCompany, query: string): boolean {
  return [company.ticker, company.exchange, company.company_name, String(company.batch_id)]
    .some((value) => String(value || "").toLowerCase().includes(query));
}

function statusLabel(value: string): string {
  return value.replace(/_/g, " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function StatusPill({ status }: { status: string }) {
  return <span className={`recon-status ${status.toLowerCase()}`}>{statusLabel(status)}</span>;
}

function UploadHint() {
  return <p className="recon-upload-hint">Use one worksheet with a required <strong>ticker</strong> column and an optional <strong>exchange</strong> column. Accepted: .xlsx and .csv, up to 500 companies.</p>;
}

function RunBatchDialog({ onClose, onRun }: { onClose: () => void; onRun: (file: File) => Promise<void> }) {
  const [file, setFile] = useState<File | null>(null);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const submit = async (event: React.FormEvent) => {
    event.preventDefault();
    if (!file) return;
    setBusy(true); setMessage(null);
    try { await onRun(file); onClose(); }
    catch (error) { setMessage(error instanceof Error ? error.message : "Batch could not be started."); }
    finally { setBusy(false); }
  };
  return <div className="recon-modal" role="dialog" aria-modal="true" aria-label="Run reconciliation batch">
    <button className="recon-modal-backdrop" onClick={onClose} aria-label="Close" />
    <form className="recon-modal-card" onSubmit={submit}>
      <button type="button" className="recon-modal-close" onClick={onClose}>×</button>
      <p className="eyebrow">Manual run</p><h2>Run a ticker batch</h2>
      <UploadHint />
      <label className="recon-file-field"><span>Company ticker file</span><input type="file" accept=".xlsx,.csv" onChange={(event) => setFile(event.target.files?.[0] || null)} required />{file && <small>{file.name} · {(file.size / 1024).toFixed(1)} KB</small>}</label>
      {message && <div className="auth-message error" role="alert">{message}</div>}
      <div className="recon-modal-actions"><button type="button" onClick={onClose}>Cancel</button><button className="button primary" disabled={!file || busy}>{busy ? "Validating and starting…" : "Validate and run"}</button></div>
    </form>
  </div>;
}

function ScheduleDialog({ onClose, onCreate }: { onClose: () => void; onCreate: (input: Parameters<typeof createReconciliationSchedule>[1]) => Promise<void> }) {
  const [name, setName] = useState("Nightly financial statement reconciliation");
  const [cadence, setCadence] = useState<"daily" | "weekly">("daily");
  const [runTime, setRunTime] = useState("02:00");
  const [dayOfWeek, setDayOfWeek] = useState(0);
  const [timezone, setTimezone] = useState(Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC");
  const [file, setFile] = useState<File | null>(null);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const submit = async (event: React.FormEvent) => {
    event.preventDefault();
    if (!file) return;
    setBusy(true); setMessage(null);
    try {
      await onCreate({ name, cadence, runTime, dayOfWeek: cadence === "weekly" ? dayOfWeek : null, timezone, file });
      onClose();
    } catch (error) { setMessage(error instanceof Error ? error.message : "Schedule could not be created."); }
    finally { setBusy(false); }
  };
  return <div className="recon-modal" role="dialog" aria-modal="true" aria-label="Schedule reconciliation">
    <button className="recon-modal-backdrop" onClick={onClose} aria-label="Close" />
    <form className="recon-modal-card" onSubmit={submit}>
      <button type="button" className="recon-modal-close" onClick={onClose}>×</button>
      <p className="eyebrow">Recurring run</p><h2>Schedule a ticker list</h2>
      <UploadHint />
      <div className="recon-form-grid">
        <label className="wide">Schedule name<input value={name} onChange={(event) => setName(event.target.value)} required /></label>
        <label>Repeat<select value={cadence} onChange={(event) => setCadence(event.target.value as "daily" | "weekly")}><option value="daily">Daily</option><option value="weekly">Weekly</option></select></label>
        {cadence === "weekly" && <label>Day<select value={dayOfWeek} onChange={(event) => setDayOfWeek(Number(event.target.value))}>{["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"].map((day, index) => <option value={index} key={day}>{day}</option>)}</select></label>}
        <label>Run time<input type="time" value={runTime} onChange={(event) => setRunTime(event.target.value)} required /></label>
        <label>Timezone<input value={timezone} onChange={(event) => setTimezone(event.target.value)} required /></label>
        <label className="wide recon-file-field"><span>Company ticker file</span><input type="file" accept=".xlsx,.csv" onChange={(event) => setFile(event.target.files?.[0] || null)} required />{file && <small>{file.name}</small>}</label>
      </div>
      {message && <div className="auth-message error" role="alert">{message}</div>}
      <div className="recon-modal-actions"><button type="button" onClick={onClose}>Cancel</button><button className="button primary" disabled={!file || busy}>{busy ? "Creating…" : "Create schedule"}</button></div>
    </form>
  </div>;
}

function EvidenceView({ check }: { check: ReconciliationCheck }) {
  const evidence = check.evidence as {
    target?: { metric?: string; value?: string; unit?: string; period?: Record<string, string>; fact_id?: number };
    source?: { concept?: string; accession?: string; form?: string; source_url?: string; artifact_sha256?: string };
    derivation?: { method?: string; formula?: string; inputs?: unknown[] };
    specification?: {
      method?: string;
      acceptance?: string;
      failure_hint?: string;
      accounting_notes?: string;
      implementation_notes?: string;
    };
    comparisons?: Array<{
      label?: string;
      metric?: string;
      expected?: string;
      observed?: string;
      target?: string;
      source?: string;
      difference?: string;
      tolerance?: string;
      passed?: boolean;
    }>;
    difference?: string;
  };
  return <div className="recon-evidence">
    <div><span>Expected</span><strong>{check.expected_value ?? "Not available"}</strong></div>
    <div><span>Observed</span><strong>{check.observed_value ?? "Not available"}</strong></div>
    <div><span>Tolerance</span><strong>{check.tolerance ?? "Not applicable"}</strong></div>
    {evidence.source && <section><h4>SEC source</h4><dl><dt>Concept</dt><dd>{evidence.source.concept || "—"}</dd><dt>Accession</dt><dd>{evidence.source.accession || "—"}</dd><dt>Form</dt><dd>{evidence.source.form || "—"}</dd><dt>Artifact hash</dt><dd>{evidence.source.artifact_sha256 || "—"}</dd></dl>{evidence.source.source_url && <a href={evidence.source.source_url} target="_blank" rel="noreferrer">Open SEC source ↗</a>}</section>}
    {evidence.derivation?.method && <section><h4>TaRaSha calculation</h4><p><strong>{statusLabel(evidence.derivation.method)}</strong></p><p>{evidence.derivation.formula}</p><small>{evidence.derivation.inputs?.length || 0} source inputs preserved</small></section>}
    {evidence.comparisons?.length ? <section><h4>Comparison evidence</h4><div className="recon-comparison-list">{evidence.comparisons.slice(0, 12).map((comparison, index) => <div key={`${comparison.label || comparison.metric || "comparison"}-${index}`}><span>{comparison.label || comparison.metric || `Comparison ${index + 1}`}</span><strong>{comparison.passed === true ? "Matched" : comparison.passed === false ? "Mismatch" : "Review"}</strong><small>Expected {comparison.expected ?? comparison.source ?? "—"} · Observed {comparison.observed ?? comparison.target ?? "—"}</small><small>Difference {comparison.difference ?? "—"} · Tolerance {comparison.tolerance ?? "—"}</small></div>)}</div>{evidence.comparisons.length > 12 && <small>{evidence.comparisons.length - 12} more comparisons are preserved in the raw evidence.</small>}</section> : null}
    {evidence.specification && <section><h4>Control specification</h4><dl><dt>Method</dt><dd>{evidence.specification.method || "—"}</dd><dt>Acceptance</dt><dd>{evidence.specification.acceptance || "—"}</dd><dt>Failure hint</dt><dd>{evidence.specification.failure_hint || "—"}</dd><dt>Accounting notes</dt><dd>{evidence.specification.accounting_notes || "—"}</dd><dt>Implementation notes</dt><dd>{evidence.specification.implementation_notes || "—"}</dd></dl></section>}
    <details><summary>Raw technical evidence</summary><pre>{JSON.stringify(check.evidence, null, 2)}</pre></details>
  </div>;
}

function CheckCard({ check, token, onUpdated }: { check: ReconciliationCheck; token: string; onUpdated: () => Promise<void> }) {
  const [open, setOpen] = useState(check.status === "FAIL");
  const [investigationStatus, setInvestigationStatus] = useState(check.investigation_status);
  const [note, setNote] = useState(check.developer_note || "");
  const [busy, setBusy] = useState(false);
  const save = async () => {
    setBusy(true);
    try { await updateReconciliationCheck(token, check.id, investigationStatus, note); await onUpdated(); }
    finally { setBusy(false); }
  };
  return <article className={`recon-check-card ${check.status.toLowerCase()}`}>
    <button className="recon-check-summary" onClick={() => setOpen((value) => !value)}>
      <StatusPill status={check.status} /><span><small>{check.check_id} · {check.control_area} · {check.severity}</small><strong>{check.title}</strong><em>{check.message}</em></span><b>{open ? "−" : "+"}</b>
    </button>
    {open && <div className="recon-check-body"><EvidenceView check={check} />
      {check.status !== "PASS" && check.status !== "NA" && <div className="recon-investigation"><h4>Developer investigation</h4><label>Status<select value={investigationStatus} onChange={(event) => setInvestigationStatus(event.target.value as ReconciliationCheck["investigation_status"])}><option value="open">Open</option><option value="investigating">Investigating</option><option value="fixed">Root cause fixed</option><option value="accepted">Accepted exception</option></select></label><label>Root-cause note<textarea value={note} onChange={(event) => setNote(event.target.value)} placeholder="What failed, where it was fixed, and what should be re-run" /></label><button onClick={save} disabled={busy}>{busy ? "Saving…" : "Save investigation"}</button></div>}
    </div>}
  </article>;
}

function CompanyDrawer({ token, company, initialStatement, onClose }: { token: string; company: ReconciliationCompany; initialStatement: ReconciliationStatement; onClose: () => void }) {
  const [detail, setDetail] = useState<ReconciliationCompanyDetail | null>(null);
  const [filter, setFilter] = useState<ReconciliationStatus | "ALL">("ALL");
  const [statement, setStatement] = useState<ReconciliationStatement>(initialStatement);
  const [message, setMessage] = useState<string | null>(null);
  const load = useCallback(async () => {
    try { setDetail(await loadReconciliationCompany(token, company.id)); setMessage(null); }
    catch (error) { setMessage(error instanceof Error ? error.message : "Company evidence could not be loaded."); }
  }, [company.id, token]);
  useEffect(() => { void load(); }, [load]);
  const statementSummary = detail?.statement_scores?.[statement];
  const checks = detail?.checks.filter((check) => (check.statement || "income_statement") === statement && (filter === "ALL" || check.status === filter)) || [];
  return <aside className="recon-drawer" aria-label={`${company.ticker} reconciliation details`}>
    <div className="recon-drawer-head"><button onClick={onClose}>← Dashboard</button><div><small>{company.exchange}</small><h2>{company.ticker} · {company.company_name}</h2></div><div className={`recon-score-ring ${statementSummary?.trust_status || company.trust_status}`}><strong>{score(statementSummary?.score || null)}</strong><span>{STATEMENTS.find((item) => item.key === statement)?.shortLabel}</span></div></div>
    <div className="recon-drawer-meta"><span>Latest FY <strong>{company.latest_fy_end || "—"}</strong></span><span>TTM end <strong>{company.ttm_end || "—"}</strong></span><span>SEC lineage <strong>{statementSummary?.source_lineage_percent != null ? `${Number(statementSummary.source_lineage_percent).toFixed(1)}%` : "—"}</strong></span><span>P0 failures <strong>{statementSummary?.p0_fail_count ?? "—"}</strong></span></div>
    <nav className="recon-statement-tabs" aria-label="Financial statement drill-down">{STATEMENTS.map((item) => <button key={item.key} className={statement === item.key ? "active" : ""} onClick={() => { setStatement(item.key); setFilter("ALL"); }}><span>{item.label}</span><strong>{score(detail?.statement_scores?.[item.key]?.score || null)}</strong></button>)}</nav>
    <div className="recon-filter-row">{(["ALL", "FAIL", "REVIEW", "PASS", "NA"] as const).map((value) => <button key={value} className={filter === value ? "active" : ""} onClick={() => setFilter(value)}>{value}{value !== "ALL" && statementSummary ? ` ${statementSummary.counts[value.toLowerCase() as keyof typeof statementSummary.counts]}` : ""}</button>)}</div>
    {message && <div className="auth-message error">{message}</div>}
    {!detail && !message && <p className="recon-loading">Loading source and target evidence…</p>}
    <div className="recon-check-list">{checks.map((check) => <CheckCard key={check.id} check={check} token={token} onUpdated={load} />)}</div>
  </aside>;
}

function CompanyTable({ companies, onOpen }: { companies: ReconciliationCompany[]; onOpen: (company: ReconciliationCompany, statement: ReconciliationStatement) => void }) {
  return <div className="recon-table-wrap"><table className="recon-table"><thead><tr><th>Company</th>{STATEMENTS.map((item) => <th key={item.key}>{item.shortLabel} score</th>)}<th>Overall result</th><th>Checks</th><th>Period</th><th /></tr></thead><tbody>{companies.map((company) => <tr key={company.id}><td><strong>{company.ticker}</strong><span>{company.company_name}</span></td>{STATEMENTS.map((item) => { const summary = company.statement_scores?.[item.key]; return <td key={item.key}><button className={`recon-statement-score ${summary?.trust_status || "pending"}`} onClick={() => onOpen(company, item.key)} aria-label={`Open ${company.ticker} ${item.label} evaluation`}><strong>{score(summary?.score || null)}</strong><span>Drill down</span></button></td>; })}<td><StatusPill status={company.trust_status} /></td><td><span className="recon-count pass">{company.counts.pass} pass</span><span className="recon-count review">{company.counts.review} review</span>{company.counts.fail > 0 && <span className="recon-count fail">{company.counts.fail} fail</span>}</td><td><small>FY {company.latest_fy_end || "—"}</small><small>TTM {company.ttm_end || "—"}</small></td><td><button onClick={() => onOpen(company, "income_statement")}>Investigate →</button></td></tr>)}</tbody></table>{companies.length === 0 && <p className="recon-empty">No matching company reconciliation results.</p>}</div>;
}

export function DataReconciliationAdmin({ token }: { token: string }) {
  const [view, setView] = useState<View>("overview");
  const [dashboard, setDashboard] = useState<ReconciliationDashboard | null>(null);
  const [batches, setBatches] = useState<ReconciliationBatch[]>([]);
  const [schedules, setSchedules] = useState<ReconciliationSchedule[]>([]);
  const [selectedCompany, setSelectedCompany] = useState<{ company: ReconciliationCompany; statement: ReconciliationStatement } | null>(null);
  const [search, setSearch] = useState("");
  const [showBatch, setShowBatch] = useState(false);
  const [showSchedule, setShowSchedule] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const refresh = useCallback(async () => {
    try {
      const [nextDashboard, nextBatches, nextSchedules] = await Promise.all([
        loadReconciliationDashboard(token), loadReconciliationBatches(token), loadReconciliationSchedules(token),
      ]);
      setDashboard(nextDashboard); setBatches(nextBatches); setSchedules(nextSchedules); setMessage(null);
    } catch (error) { setMessage(error instanceof Error ? error.message : "Data reconciliation could not be loaded."); }
    finally { setLoading(false); }
  }, [token]);
  useEffect(() => { void refresh(); }, [refresh]);
  const activeRun = useMemo(() => batches.some((batch) => ["queued", "running"].includes(batch.status)), [batches]);
  useEffect(() => {
    if (!activeRun) return;
    const timer = window.setInterval(() => void refresh(), 2500);
    return () => window.clearInterval(timer);
  }, [activeRun, refresh]);
  const runBatch = async (file: File) => { await runReconciliationBatch(token, file); await refresh(); setView("runs"); };
  const createSchedule = async (input: Parameters<typeof createReconciliationSchedule>[1]) => { await createReconciliationSchedule(token, input); await refresh(); setView("schedules"); };
  const runSchedule = async (id: number) => { await runReconciliationSchedule(token, id); await refresh(); setView("runs"); };
  const toggleSchedule = async (item: ReconciliationSchedule) => { await setReconciliationScheduleActive(token, item.id, !item.active); await refresh(); };
  const normalizedSearch = search.trim().toLowerCase().replace(/^#/, "");
  const visibleCompanies = !normalizedSearch ? dashboard?.companies || [] : (dashboard?.companies || []).filter((company) => companyMatches(company, normalizedSearch));
  const visibleBatches = !normalizedSearch ? batches : batches.filter((batch) => [String(batch.id), batch.original_filename, batch.created_by].some((value) => String(value || "").toLowerCase().includes(normalizedSearch)) || (batch.companies || []).some((company) => companyMatches(company, normalizedSearch)));
  const openCompany = (company: ReconciliationCompany, statement: ReconciliationStatement) => setSelectedCompany({ company, statement });
  if (selectedCompany) return <CompanyDrawer token={token} company={selectedCompany.company} initialStatement={selectedCompany.statement} onClose={() => { setSelectedCompany(null); void refresh(); }} />;
  return <section className="recon-page">
    <header className="recon-page-head"><div><p className="eyebrow">Admin-only control room</p><h1>Data Reconciliation</h1><p>Income Statement, Balance Sheet, and Cash Flow Statement FY/TTM checks on ingested SEC data. Missing proof is never counted as a pass.</p></div><div><button onClick={() => setShowSchedule(true)}>Schedule run</button><button className="button primary" onClick={() => setShowBatch(true)}>Run batch</button></div></header>
    <nav className="recon-tabs" aria-label="Reconciliation views">{(["overview", "runs", "schedules"] as const).map((item) => <button key={item} className={view === item ? "active" : ""} onClick={() => setView(item)}>{statusLabel(item)}</button>)}</nav>
    {view !== "schedules" && <label className="recon-search"><span>Search batches or companies</span><input type="search" value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Batch #, ticker, or company name" /></label>}
    {message && <div className="auth-message error" role="alert">{message}</div>}
    {loading && <p className="recon-loading">Loading reconciliation evidence…</p>}
    {!loading && dashboard && view === "overview" && <>
      <div className="recon-kpis recon-statement-kpis">{STATEMENTS.map((item) => <article key={item.key}><span>{item.label} average</span><strong>{score(dashboard.summary.statement_average_scores?.[item.key] || null)}</strong><small>Target ≥ {Number(dashboard.target_score).toFixed(0)}%</small></article>)}<article><span>Blocking issues</span><strong>{dashboard.summary.blocked}</strong><small>{dashboard.summary.open_issues} open controls</small></article></div>
      <div className="recon-section-head"><div><h2>Company quality</h2><p>A company is trusted only at 98%+, with zero P0 failures and 100% source lineage.</p></div></div>
      <CompanyTable companies={visibleCompanies} onOpen={openCompany} />
      <div className="recon-section-head"><div><h2>Issues needing attention</h2><p>Open a company to see the SEC fact, TaRaSha value, formula, tolerance, and source link.</p></div></div>
      <div className="recon-issue-grid">{dashboard.issues.filter((issue) => !normalizedSearch || [issue.ticker, issue.company_name, issue.check_id, issue.title].some((value) => String(value || "").toLowerCase().includes(normalizedSearch))).slice(0, 12).map((issue) => <button key={issue.id} onClick={() => { const company = dashboard.companies.find((item) => item.id === issue.company_run_id); if (company) openCompany(company, issue.statement || "income_statement"); }}><StatusPill status={issue.status} /><span><small>{issue.ticker} · {STATEMENTS.find((item) => item.key === (issue.statement || "income_statement"))?.shortLabel} · {issue.check_id} · {issue.severity}</small><strong>{issue.title}</strong><em>{issue.message}</em></span><b>→</b></button>)}</div>
    </>}
    {!loading && view === "runs" && <div className="recon-run-list">{visibleBatches.map((batch) => { const batchMatches = !normalizedSearch || [String(batch.id), batch.original_filename, batch.created_by].some((value) => String(value || "").toLowerCase().includes(normalizedSearch)); const companies = batchMatches ? batch.companies || [] : (batch.companies || []).filter((company) => companyMatches(company, normalizedSearch)); return <article key={batch.id}><header><div><small>Run #{batch.id} · {statusLabel(batch.trigger_kind)}</small><h2>{batch.original_filename}</h2><p>SHA-256 {batch.upload_sha256.slice(0, 16)}… · Rules {batch.source_version}</p></div><StatusPill status={batch.status} /></header><div className="recon-progress"><span style={{ width: `${batch.total_companies ? batch.completed_companies / batch.total_companies * 100 : 0}%` }} /></div><footer><span>{batch.completed_companies} / {batch.total_companies} companies</span><span>Average {score(batch.summary.average_score == null ? null : String(batch.summary.average_score))}</span><span>{batch.trusted_companies} trusted</span><span>{batch.failed_companies} execution failures</span><span>Started {displayDate(batch.created_at)}</span></footer><CompanyTable companies={companies} onOpen={openCompany} /></article>; })}{visibleBatches.length === 0 && <p className="recon-empty">No matching batch or company was found among the five retained runs.</p>}</div>}
    {!loading && view === "schedules" && <div className="recon-schedule-list">{schedules.map((item) => <article key={item.id}><header><div><small>{item.cadence} · {item.timezone}</small><h2>{item.name}</h2></div><StatusPill status={item.active ? "active" : "paused"} /></header><dl><div><dt>Ticker file</dt><dd>{item.original_filename}</dd></div><div><dt>Companies</dt><dd>{item.ticker_count}</dd></div><div><dt>Next run</dt><dd>{displayDate(item.next_run_at)}</dd></div><div><dt>Last batch</dt><dd>{item.last_batch_id ? `#${item.last_batch_id}` : "Not run yet"}</dd></div></dl><p>{item.tickers.slice(0, 8).map((ticker) => ticker.ticker).join(" · ")}{item.tickers.length > 8 ? ` · +${item.tickers.length - 8}` : ""}</p><footer><button onClick={() => void toggleSchedule(item)}>{item.active ? "Pause" : "Resume"}</button><button className="button primary" disabled={!item.active} onClick={() => void runSchedule(item.id)}>Run now</button></footer></article>)}{schedules.length === 0 && <p className="recon-empty">No schedules yet. Every schedule starts with its own uploaded ticker file.</p>}</div>}
    {showBatch && <RunBatchDialog onClose={() => setShowBatch(false)} onRun={runBatch} />}
    {showSchedule && <ScheduleDialog onClose={() => setShowSchedule(false)} onCreate={createSchedule} />}
  </section>;
}
