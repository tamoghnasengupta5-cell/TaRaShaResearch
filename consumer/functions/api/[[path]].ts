import {
  fetchDataCompanyLogo,
  pullDataCompany,
  searchDataCompanies,
  type DataProviderEnv,
} from "../dataProvider";
import {
  accountSummary,
  authenticateAccount,
  AuthError,
  registerAccount,
  resetAccountPassword,
  securityQuestionFor,
  type AuthDatabase,
} from "../authProvider";
import { getMarketOverview } from "../marketOverviewProvider";

interface Env extends DataProviderEnv {
  DB: AuthDatabase;
  ADMIN_SYNC_KEY?: string;
  CONSUMER_AUTH_SECRET: string;
}

const MAX_YEAR_RANGE = 7;

function json(data: unknown, status = 200): Response {
  return Response.json(data, {
    status,
    headers: {
      "cache-control": "private, no-store, max-age=0",
      "x-content-type-options": "nosniff",
      "referrer-policy": "no-referrer",
    },
  });
}

function error(message: string, status = 400): Response {
  return json({ error: message }, status);
}

function pathParts(request: Request): string[] {
  return new URL(request.url).pathname.replace(/^\/api\/?/, "").split("/").filter(Boolean);
}

function cleanText(value: unknown, max = 180): string {
  return String(value ?? "").trim().replace(/[\u0000-\u001f]/g, "").slice(0, max);
}

function validYearRange(fromYear: unknown, toYear: unknown): { fromYear: number; toYear: number } {
  const from = Number(fromYear);
  const to = Number(toYear);
  const currentYear = new Date().getUTCFullYear();
  if (!Number.isInteger(from) || !Number.isInteger(to) || from > to || to > currentYear || from < 1995 || to - from + 1 > MAX_YEAR_RANGE) {
    throw new AuthError(`Select no more than ${MAX_YEAR_RANGE} reporting years.`);
  }
  return { fromYear: from, toYear: to };
}

async function searchCompanies(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  const query = cleanText(url.searchParams.get("query"), 60);
  const country = url.searchParams.get("country") === "India" ? "India" : "USA";
  if (query.length < 2) return json({ companies: [] });
  try {
    return json({ companies: await searchDataCompanies(env, query, country) });
  } catch (cause) {
    return error(cause instanceof Error ? cause.message : "TaRaShaData.ai company search failed.", 502);
  }
}

async function dataCompany(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  try {
    const years = validYearRange(url.searchParams.get("fromYear"), url.searchParams.get("toYear"));
    return json(await pullDataCompany(env, cleanText(url.searchParams.get("companyId"), 100), years.fromYear, years.toYear));
  } catch (cause) {
    return cause instanceof AuthError
      ? error(cause.message, cause.status)
      : error(cause instanceof Error ? cause.message : "TaRaShaData.ai company pull failed.", 502);
  }
}

async function dataCompanyLogo(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  try {
    return await fetchDataCompanyLogo(env, cleanText(url.searchParams.get("companyId"), 100));
  } catch (cause) {
    return error(cause instanceof Error ? cause.message : "Company logo could not be loaded.", 404);
  }
}

async function recalculateDataCompany(request: Request, env: Env): Promise<Response> {
  type Body = {
    companyId?: string;
    fromYear?: number;
    toYear?: number;
    constituentIds?: string[] | null;
  };
  const body = await request.json<Body>().catch(() => ({} as Body));
  try {
    const years = validYearRange(body.fromYear, body.toYear);
    let constituentIds: string[] | undefined;
    if (body.constituentIds !== null && body.constituentIds !== undefined) {
      if (!Array.isArray(body.constituentIds) || !body.constituentIds.length || body.constituentIds.length > 100) {
        return error("Select between 1 and 100 industry constituents.");
      }
      if (body.constituentIds.some((id) => !/^data-[0-9]{10}$/.test(String(id)))) {
        return error("One or more TaRaShaData.ai constituent identifiers are invalid.");
      }
      constituentIds = [...new Set(body.constituentIds)];
    }
    return json(await pullDataCompany(
      env,
      cleanText(body.companyId, 100),
      years.fromYear,
      years.toYear,
      constituentIds,
    ));
  } catch (cause) {
    return cause instanceof AuthError
      ? error(cause.message, cause.status)
      : error(cause instanceof Error ? cause.message : "Industry comparison recalculation failed.", 502);
  }
}

async function registerUser(request: Request, env: Env): Promise<Response> {
  type Body = {
    name?: string;
    username?: string;
    securityQuestion?: string;
    securityAnswer?: string;
    password?: string;
    confirmPassword?: string;
  };
  const body = await request.json<Body>().catch(() => ({} as Body));
  try {
    return json(await registerAccount(env.DB, env.CONSUMER_AUTH_SECRET, {
      name: String(body.name ?? ""),
      username: String(body.username ?? ""),
      securityQuestion: String(body.securityQuestion ?? ""),
      securityAnswer: String(body.securityAnswer ?? ""),
      password: String(body.password ?? ""),
      confirmPassword: String(body.confirmPassword ?? ""),
    }), 201);
  } catch (cause) {
    if (!(cause instanceof AuthError)) console.error("Account registration failed", cause);
    return cause instanceof AuthError ? error(cause.message, cause.status) : error("Account registration is temporarily unavailable.", 503);
  }
}

async function loginUser(request: Request, env: Env): Promise<Response> {
  type Body = { username?: string; password?: string };
  const body = await request.json<Body>().catch(() => ({} as Body));
  try {
    return json(await authenticateAccount(env.DB, env.CONSUMER_AUTH_SECRET, String(body.username ?? ""), String(body.password ?? "")));
  } catch (cause) {
    if (!(cause instanceof AuthError)) console.error("Account login failed", cause);
    return cause instanceof AuthError ? error(cause.message, cause.status) : error("Account login is temporarily unavailable.", 503);
  }
}

async function userSecurityQuestion(request: Request, env: Env): Promise<Response> {
  type Body = { username?: string };
  const body = await request.json<Body>().catch(() => ({} as Body));
  try {
    return json({ question: await securityQuestionFor(env.DB, env.CONSUMER_AUTH_SECRET, String(body.username ?? "")) });
  } catch (cause) {
    if (!(cause instanceof AuthError)) console.error("Account recovery lookup failed", cause);
    return cause instanceof AuthError ? error(cause.message, cause.status) : error("Account recovery is temporarily unavailable.", 503);
  }
}

async function resetUserPassword(request: Request, env: Env): Promise<Response> {
  type Body = { username?: string; answer?: string; newPassword?: string };
  const body = await request.json<Body>().catch(() => ({} as Body));
  try {
    await resetAccountPassword(env.DB, env.CONSUMER_AUTH_SECRET, String(body.username ?? ""), String(body.answer ?? ""), String(body.newPassword ?? ""));
    return json({ reset: true });
  } catch (cause) {
    if (!(cause instanceof AuthError)) console.error("Account password reset failed", cause);
    return cause instanceof AuthError ? error(cause.message, cause.status) : error("Account recovery is temporarily unavailable.", 503);
  }
}

async function adminUserSummary(request: Request, env: Env): Promise<Response> {
  if (!env.ADMIN_SYNC_KEY || request.headers.get("x-admin-key") !== env.ADMIN_SYNC_KEY) return error("Admin authorization failed.", 401);
  try {
    return json(await accountSummary(env.DB));
  } catch {
    return error("User summary is temporarily unavailable.", 503);
  }
}

export const onRequest: PagesFunction<Env> = async (context) => {
  const { request, env } = context;
  const [first, second, third] = pathParts(request);
  if (request.method === "GET" && first === "health") {
    return json({ ok: true, provider: "tarasha-data", configured: Boolean(env.TARASHA_DATA_API_URL), financialStorage: "browser-session-only" });
  }
  if (request.method === "GET" && first === "market-overview") {
    try {
      return Response.json(await getMarketOverview(), {
        headers: {
          "cache-control": "public, max-age=900, s-maxage=21600, stale-while-revalidate=86400",
          "x-content-type-options": "nosniff",
          "referrer-policy": "no-referrer",
        },
      });
    } catch (cause) {
      return error(cause instanceof Error ? cause.message : "The global market overview is temporarily unavailable.", 502);
    }
  }
  if (request.method === "GET" && first === "companies") return searchCompanies(request, env);
  if (request.method === "GET" && first === "data" && second === "company-logo") return dataCompanyLogo(request, env);
  if (request.method === "GET" && first === "data" && second === "company") return dataCompany(request, env);
  if (request.method === "POST" && first === "data" && second === "company") return recalculateDataCompany(request, env);
  if (request.method === "POST" && first === "auth" && second === "register") return registerUser(request, env);
  if (request.method === "POST" && first === "auth" && second === "login") return loginUser(request, env);
  if (request.method === "POST" && first === "auth" && second === "security-question") return userSecurityQuestion(request, env);
  if (request.method === "POST" && first === "auth" && second === "reset-password") return resetUserPassword(request, env);
  if (request.method === "GET" && first === "admin" && second === "users" && third === "summary") return adminUserSummary(request, env);
  return error("Endpoint not found.", 404);
};
