export type ReconciliationStatus = "PASS" | "REVIEW" | "FAIL" | "NA";
export type ReconciliationStatement = "income_statement" | "balance_sheet" | "cash_flow_statement";

export interface ReconciliationStatementScore {
  score: string;
  trust_status: "trusted" | "review" | "blocked";
  counts: { pass: number; review: number; fail: number; na: number };
  p0_fail_count: number;
  source_lineage_percent: string;
  control_catalog_total: number;
}

export interface ReconciliationCompany {
  id: number;
  batch_id: number;
  ticker: string;
  exchange: string;
  company_name: string;
  status: string;
  score: string | null;
  trust_status: "trusted" | "review" | "blocked" | "pending";
  counts: { pass: number; review: number; fail: number; na: number };
  p0_fail_count: number;
  source_lineage_percent: string | null;
  latest_fy_end: string | null;
  ttm_end: string | null;
  error_message?: string | null;
  statement_scores?: Partial<Record<ReconciliationStatement, ReconciliationStatementScore>>;
}

export interface ReconciliationIssue {
  id: number;
  company_run_id: number;
  ticker: string;
  company_name: string;
  check_id: string;
  control_area: string;
  title: string;
  severity: string;
  status: "FAIL" | "REVIEW";
  message: string;
  investigation_status: string;
  statement?: ReconciliationStatement;
}

export interface ReconciliationCheck {
  id: number;
  check_id: string;
  control_area: string;
  title: string;
  applies_to: string;
  severity: string;
  status: ReconciliationStatus;
  message: string;
  expected_value: string | null;
  observed_value: string | null;
  tolerance: string | null;
  evidence: Record<string, unknown>;
  investigation_status: "open" | "investigating" | "fixed" | "accepted";
  developer_note: string | null;
  statement?: ReconciliationStatement;
}

export interface ReconciliationCompanyDetail extends ReconciliationCompany {
  checks: ReconciliationCheck[];
}

export interface ReconciliationBatch {
  id: number;
  trigger_kind: string;
  schedule_id: number | null;
  original_filename: string;
  upload_sha256: string;
  source_version: string;
  status: string;
  total_companies: number;
  completed_companies: number;
  trusted_companies: number;
  failed_companies: number;
  summary: Record<string, string | number>;
  created_by: string;
  created_at: string;
  completed_at: string | null;
  companies: ReconciliationCompany[];
}

export interface ReconciliationSchedule {
  id: number;
  name: string;
  cadence: "daily" | "weekly";
  run_time_utc: string;
  day_of_week: number | null;
  timezone: string;
  original_filename: string;
  upload_sha256: string;
  ticker_count: number;
  tickers: Array<{ ticker: string; exchange: string }>;
  source_version: string;
  active: boolean;
  next_run_at: string;
  last_run_at: string | null;
  last_batch_id: number | null;
}

export interface ReconciliationDashboard {
  target_score: string;
  latest_batch: ReconciliationBatch | null;
  summary: {
    companies: number;
    average_score: string | null;
    trusted: number;
    review: number;
    blocked: number;
    open_issues: number;
    statement_average_scores?: Record<ReconciliationStatement, string | null>;
  };
  companies: ReconciliationCompany[];
  issues: ReconciliationIssue[];
}

const apiBase = String(import.meta.env.VITE_API_BASE_URL || "").replace(/\/$/, "");

function errorMessage(payload: unknown, fallback: string): string {
  const value = payload as { error?: string; detail?: string | { message?: string; rows?: Array<{ row?: number; message?: string }> } };
  if (value?.error) return value.error;
  if (typeof value?.detail === "string") return value.detail;
  if (value?.detail && typeof value.detail === "object") {
    const firstRow = value.detail.rows?.[0];
    return `${value.detail.message || fallback}${firstRow?.message ? ` Row ${firstRow.row}: ${firstRow.message}` : ""}`;
  }
  return fallback;
}

async function adminRequest<T>(token: string, path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers);
  headers.set("authorization", `Bearer ${token}`);
  if (init.body && !(init.body instanceof FormData)) headers.set("content-type", "application/json");
  const response = await fetch(`${apiBase}/api/admin/data-reconciliation${path}`, { ...init, headers });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(errorMessage(payload, `Reconciliation request failed with status ${response.status}.`));
  return payload as T;
}

export function loadReconciliationDashboard(token: string): Promise<ReconciliationDashboard> {
  return adminRequest(token, "/dashboard");
}

export function loadReconciliationBatches(token: string): Promise<ReconciliationBatch[]> {
  return adminRequest(token, "/batches");
}

export function loadReconciliationSchedules(token: string): Promise<ReconciliationSchedule[]> {
  return adminRequest(token, "/schedules");
}

export function loadReconciliationCompany(token: string, id: number): Promise<ReconciliationCompanyDetail> {
  return adminRequest(token, `/companies/${id}`);
}

export function runReconciliationBatch(token: string, file: File): Promise<ReconciliationBatch> {
  const form = new FormData();
  form.append("ticker_file", file);
  return adminRequest(token, "/batches", { method: "POST", body: form });
}

export function createReconciliationSchedule(
  token: string,
  input: {
    name: string;
    cadence: "daily" | "weekly";
    runTime: string;
    dayOfWeek: number | null;
    timezone: string;
    file: File;
  },
): Promise<ReconciliationSchedule> {
  const form = new FormData();
  form.append("name", input.name);
  form.append("cadence", input.cadence);
  form.append("run_time_utc", input.runTime);
  form.append("timezone", input.timezone);
  if (input.dayOfWeek !== null) form.append("day_of_week", String(input.dayOfWeek));
  form.append("ticker_file", input.file);
  return adminRequest(token, "/schedules", { method: "POST", body: form });
}

export function runReconciliationSchedule(token: string, id: number): Promise<ReconciliationBatch> {
  return adminRequest(token, `/schedules/${id}/run`, { method: "POST" });
}

export function setReconciliationScheduleActive(token: string, id: number, active: boolean): Promise<ReconciliationSchedule> {
  return adminRequest(token, `/schedules/${id}`, { method: "PATCH", body: JSON.stringify({ active }) });
}

export function updateReconciliationCheck(
  token: string,
  id: number,
  investigationStatus: ReconciliationCheck["investigation_status"],
  developerNote: string,
): Promise<{ id: number; investigation_status: string }> {
  return adminRequest(token, `/checks/${id}`, {
    method: "PATCH",
    body: JSON.stringify({ investigation_status: investigationStatus, developer_note: developerNote }),
  });
}
