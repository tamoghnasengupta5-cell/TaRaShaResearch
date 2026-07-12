/// <reference types="@cloudflare/workers-types" />

interface Env {
  DB: D1Database;
  ADMIN_SYNC_KEY: string;
  SEC_USER_AGENT: string;
}

interface CatalogRow {
  id: string;
  cik: string | null;
  name: string;
  ticker: string;
  exchange: string;
  country: "USA" | "India";
  provider: string;
  research_available: number;
}

interface AdminCatalogRow {
  cik: number;
  name: string;
  ticker: string;
  exchange: string;
}

const MAX_SESSION_COMPANIES = 3;
const MAX_YEAR_RANGE = 5;
const SESSION_HOURS = 12;
const SEC_MAX_ATTEMPTS = 5;
const SEC_RETRYABLE_STATUSES = new Set([403, 429, 500, 502, 503, 504]);

const json = (data: unknown, status = 200, extraHeaders: HeadersInit = {}) => new Response(JSON.stringify(data), {
  status,
  headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store", ...extraHeaders },
});

const error = (message: string, status = 400) => json({ error: message }, status);

const wait = (milliseconds: number) => new Promise((resolve) => setTimeout(resolve, milliseconds));

function secRetryDelay(response: Response, attempt: number): number {
  const retryAfter = Number(response.headers.get("retry-after"));
  if (Number.isFinite(retryAfter) && retryAfter > 0) return Math.min(retryAfter * 1000, 5_000);
  return Math.min(250 * (2 ** attempt) + Math.floor(Math.random() * 200), 4_000);
}

export async function fetchSecWithBackoff(url: string, userAgent: string): Promise<Response> {
  let response: Response | undefined;
  for (let attempt = 0; attempt < SEC_MAX_ATTEMPTS; attempt += 1) {
    response = await fetch(url, {
      headers: {
        "User-Agent": userAgent,
        "Accept-Encoding": "gzip, deflate",
        Accept: "application/json",
      },
    });
    if (response.ok || !SEC_RETRYABLE_STATUSES.has(response.status) || attempt === SEC_MAX_ATTEMPTS - 1) return response;
    const delay = secRetryDelay(response, attempt);
    await response.body?.cancel().catch(() => undefined);
    await wait(delay);
  }
  return response as Response;
}

function pathParts(request: Request): string[] {
  const pathname = new URL(request.url).pathname.replace(/^\/api\/?/, "");
  return pathname.split("/").filter(Boolean);
}

function validSessionId(value: unknown): value is string {
  return typeof value === "string" && /^[a-zA-Z0-9_-]{20,80}$/.test(value);
}

function cleanTicker(value: unknown): string {
  return String(value ?? "").trim().toUpperCase().replace(/[^A-Z0-9.\-]/g, "").slice(0, 24);
}

function cleanText(value: unknown, max = 180): string {
  return String(value ?? "").trim().replace(/[\u0000-\u001f]/g, "").slice(0, max);
}

async function searchCompanies(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  const query = cleanText(url.searchParams.get("query"), 80);
  const country = url.searchParams.get("country") === "India" ? "India" : "USA";
  if (query.length < 2) return json({ companies: [] });
  const pattern = `%${query.replace(/[%_]/g, "")}%`;
  const result = await env.DB.prepare(`
    select id, cik, name, ticker, exchange, country, provider, research_available
    from company_catalog
    where country = ?1 and (lower(name) like lower(?2) or lower(ticker) like lower(?2))
    order by case when lower(ticker) = lower(?3) then 0 else 1 end, name
    limit 30
  `).bind(country, pattern, query).all<CatalogRow>();
  return json({ companies: result.results ?? [] });
}

async function adminCatalogBatch(request: Request, env: Env): Promise<Response> {
  if (!env.ADMIN_SYNC_KEY || request.headers.get("x-admin-key") !== env.ADMIN_SYNC_KEY) return error("Admin authorization failed.", 401);
  const payload: { rows?: AdminCatalogRow[] } = await request.json<{ rows?: AdminCatalogRow[] }>().catch(() => ({ rows: [] }));
  if (!Array.isArray(payload.rows) || payload.rows.length === 0 || payload.rows.length > 100) return error("Provide between 1 and 100 catalogue rows.");
  const statements = payload.rows.map((row) => {
    const cik = String(Math.max(0, Number(row.cik) || 0)).padStart(10, "0");
    const ticker = cleanTicker(row.ticker);
    const exchange = cleanText(row.exchange || "US", 40);
    const name = cleanText(row.name, 180);
    const id = `us-sec-${cik}-${ticker.toLowerCase()}`;
    return env.DB.prepare(`
      insert into company_catalog(id, cik, name, ticker, exchange, country, provider, research_available, updated_at)
      values(?1, ?2, ?3, ?4, ?5, 'USA', 'SEC EDGAR', 1, unixepoch())
      on conflict(id) do update set name=excluded.name, ticker=excluded.ticker, exchange=excluded.exchange,
        provider=excluded.provider, research_available=1, updated_at=excluded.updated_at
    `).bind(id, cik, name, ticker, exchange);
  });
  await env.DB.batch(statements);
  return json({ accepted: statements.length });
}

async function claimCompany(request: Request, env: Env): Promise<Response> {
  const body: { companyId?: string; sessionId?: string; fromYear?: number; toYear?: number } = await request
    .json<{ companyId?: string; sessionId?: string; fromYear?: number; toYear?: number }>()
    .catch(() => ({}));
  if (!validSessionId(body.sessionId)) return error("A valid browser session is required.");
  const companyId = cleanText(body.companyId, 100);
  const fromYear = Number(body.fromYear);
  const toYear = Number(body.toYear);
  const currentYear = new Date().getUTCFullYear();
  if (!Number.isInteger(fromYear) || !Number.isInteger(toYear) || fromYear > toYear || toYear > currentYear || fromYear < 1995 || toYear - fromYear + 1 > MAX_YEAR_RANGE) {
    return error(`Select no more than ${MAX_YEAR_RANGE} reporting years.`);
  }
  const company = await env.DB.prepare("select * from company_catalog where id = ?1").bind(companyId).first<CatalogRow>();
  if (!company) return error("Company was not found.", 404);
  if (!company.research_available || company.country !== "USA" || !company.cik) {
    return error("A free structured filing source is not available for this company yet.", 422);
  }
  await env.DB.prepare("delete from session_claims where created_at < unixepoch() - ?1").bind(SESSION_HOURS * 3600).run();
  const existing = await env.DB.prepare("select 1 as found from session_claims where session_id = ?1 and company_id = ?2")
    .bind(body.sessionId, companyId).first();
  if (!existing) {
    const count = await env.DB.prepare("select count(*) as total from session_claims where session_id = ?1").bind(body.sessionId).first<{ total: number }>();
    if (Number(count?.total ?? 0) >= MAX_SESSION_COMPANIES) return error(`A session can research at most ${MAX_SESSION_COMPANIES} companies.`, 429);
    await env.DB.prepare("insert into session_claims(session_id, company_id, from_year, to_year, created_at) values(?1, ?2, ?3, ?4, unixepoch())")
      .bind(body.sessionId, companyId, fromYear, toYear).run();
  } else {
    await env.DB.prepare("update session_claims set from_year=?3, to_year=?4 where session_id=?1 and company_id=?2")
      .bind(body.sessionId, companyId, fromYear, toYear).run();
  }
  return json({ company, limits: { maxCompanies: MAX_SESSION_COMPANIES, maxYears: MAX_YEAR_RANGE } });
}

async function secProxy(request: Request, env: Env, resource: "companyfacts" | "submissions"): Promise<Response> {
  const url = new URL(request.url);
  const companyId = cleanText(url.searchParams.get("companyId"), 100);
  const sessionId = url.searchParams.get("sessionId");
  if (!validSessionId(sessionId)) return error("A valid browser session is required.");
  const claim = await env.DB.prepare(`
    select c.cik from session_claims s join company_catalog c on c.id=s.company_id
    where s.session_id=?1 and s.company_id=?2 and s.created_at >= unixepoch() - ?3
  `).bind(sessionId, companyId, SESSION_HOURS * 3600).first<{ cik: string }>();
  if (!claim?.cik) return error("Research this company from Discover before requesting filings.", 403);
  if (!env.SEC_USER_AGENT || !env.SEC_USER_AGENT.includes("@")) return error("SEC_USER_AGENT must identify the application and an administrator email.", 503);
  const secUrl = resource === "companyfacts"
    ? `https://data.sec.gov/api/xbrl/companyfacts/CIK${claim.cik}.json`
    : `https://data.sec.gov/submissions/CIK${claim.cik}.json`;
  const upstream = await fetchSecWithBackoff(secUrl, env.SEC_USER_AGENT);
  if (!upstream.ok || !upstream.body) {
    const temporary = SEC_RETRYABLE_STATUSES.has(upstream.status);
    return error(
      temporary ? "SEC EDGAR is temporarily rate-limiting automated access. Please try again shortly." : "The requested SEC filing data is currently unavailable.",
      temporary ? 503 : 502,
    );
  }
  return new Response(upstream.body, {
    status: 200,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "private, no-store, max-age=0",
      "x-content-type-options": "nosniff",
    },
  });
}

export const onRequest: PagesFunction<Env> = async (context) => {
  const { request, env } = context;
  const [first, second, third] = pathParts(request);
  if (request.method === "GET" && first === "health") return json({ ok: true, storage: "catalogue-and-session-claims-only" });
  if (request.method === "GET" && first === "companies") return searchCompanies(request, env);
  if (request.method === "POST" && first === "admin" && second === "catalog" && third === "batch") return adminCatalogBatch(request, env);
  if (request.method === "POST" && first === "session" && second === "claim") return claimCompany(request, env);
  if (request.method === "GET" && first === "sec" && second === "companyfacts") return secProxy(request, env, "companyfacts");
  if (request.method === "GET" && first === "sec" && second === "submissions") return secProxy(request, env, "submissions");
  return error("Endpoint not found.", 404);
};
