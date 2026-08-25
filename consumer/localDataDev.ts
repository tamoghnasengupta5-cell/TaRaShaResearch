import type { IncomingMessage, ServerResponse } from "node:http";
import type { Plugin } from "vite";
import {
  fetchDataCompanyLogo,
  pullDataCompany,
  searchDataCompanies,
  type DataProviderEnv,
} from "./functions/dataProvider";
import { getMarketOverview } from "./functions/marketOverviewProvider";

const MAX_YEAR_RANGE = 7;

class RequestError extends Error {
  constructor(message: string, readonly status = 400) {
    super(message);
  }
}

function sendJson(response: ServerResponse, status: number, data: unknown): void {
  response.statusCode = status;
  response.setHeader("content-type", "application/json; charset=utf-8");
  response.setHeader("cache-control", "no-store");
  response.end(JSON.stringify(data));
}

async function sendResponse(response: ServerResponse, upstream: Response): Promise<void> {
  response.statusCode = upstream.status;
  upstream.headers.forEach((value, key) => response.setHeader(key, value));
  response.end(Buffer.from(await upstream.arrayBuffer()));
}

function validYearRange(fromYear: unknown, toYear: unknown): { fromYear: number; toYear: number } {
  const from = Number(fromYear);
  const to = Number(toYear);
  const currentYear = new Date().getUTCFullYear();
  if (!Number.isInteger(from) || !Number.isInteger(to) || from > to || to > currentYear || from < 1995 || to - from + 1 > MAX_YEAR_RANGE) {
    throw new RequestError(`Select no more than ${MAX_YEAR_RANGE} reporting years.`);
  }
  return { fromYear: from, toYear: to };
}

async function readJsonBody(request: IncomingMessage): Promise<Record<string, unknown>> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    size += buffer.length;
    if (size > 64 * 1024) throw new RequestError("Request body is too large.", 413);
    chunks.push(buffer);
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}") as Record<string, unknown>;
  } catch {
    throw new RequestError("Request body must be valid JSON.");
  }
}

function constituentIds(value: unknown): string[] | undefined {
  if (value === null || value === undefined) return undefined;
  if (!Array.isArray(value) || !value.length || value.length > 100 || value.some((id) => !/^data-[0-9]{10}$/.test(String(id)))) {
    throw new RequestError("Select between 1 and 100 valid TaRaShaData.ai constituents.");
  }
  return [...new Set(value.map(String))];
}

async function handleDataRequest(request: IncomingMessage, response: ServerResponse, env: DataProviderEnv): Promise<void> {
  const url = new URL(request.url || "/", "http://localhost");
  if (request.method === "GET" && url.pathname === "/api/health") {
    sendJson(response, 200, { ok: true, provider: "tarasha-data", financialStorage: "browser-session-only", localProvider: true });
    return;
  }
  if (request.method === "GET" && url.pathname === "/api/market-overview") {
    sendJson(response, 200, await getMarketOverview());
    return;
  }
  if (request.method === "GET" && url.pathname === "/api/companies") {
    const query = String(url.searchParams.get("query") || "");
    const country = url.searchParams.get("country") === "India" ? "India" : "USA";
    sendJson(response, 200, { companies: await searchDataCompanies(env, query, country) });
    return;
  }
  if (request.method === "GET" && url.pathname === "/api/data/company-logo") {
    await sendResponse(
      response,
      await fetchDataCompanyLogo(env, String(url.searchParams.get("companyId") || "")),
    );
    return;
  }
  if (request.method === "GET" && url.pathname === "/api/data/company") {
    const years = validYearRange(url.searchParams.get("fromYear"), url.searchParams.get("toYear"));
    sendJson(response, 200, await pullDataCompany(env, String(url.searchParams.get("companyId") || ""), years.fromYear, years.toYear));
    return;
  }
  if (request.method === "POST" && url.pathname === "/api/data/company") {
    const body = await readJsonBody(request);
    const years = validYearRange(body.fromYear, body.toYear);
    sendJson(response, 200, await pullDataCompany(env, String(body.companyId || ""), years.fromYear, years.toYear, constituentIds(body.constituentIds)));
    return;
  }
  throw new RequestError("Local TaRaShaData.ai API endpoint not found.", 404);
}

export function localDataApiPlugin(apiUrl: string, apiKey = ""): Plugin | null {
  const base = apiUrl.trim().replace(/\/$/, "");
  if (!base) return null;
  const env: DataProviderEnv = {
    TARASHA_DATA_API_URL: base,
    TARASHA_DATA_API_KEY: apiKey.trim(),
  };
  return {
    name: "tarasha-local-data-api",
    apply: "serve",
    configureServer(server) {
      server.middlewares.use(async (request, response, next) => {
        if (!request.url?.startsWith("/api/")) {
          next();
          return;
        }
        try {
          await handleDataRequest(request, response, env);
        } catch (cause) {
          const status = cause instanceof RequestError ? cause.status : 502;
          const message = cause instanceof Error ? cause.message : "Local TaRaShaData.ai API request failed.";
          sendJson(response, status, { error: message });
        }
      });
    },
  };
}
