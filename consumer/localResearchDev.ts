import { spawnSync } from "node:child_process";
import type { IncomingMessage, ServerResponse } from "node:http";
import type { Plugin } from "vite";
import {
  pullResearchCompany,
  searchResearchCompanies,
  type ResearchProviderEnv,
} from "./functions/researchProvider";

const MAX_YEAR_RANGE = 7;
const SHARED_RESEARCH_URL = "https://fgmijnuwplxasztyjfqr.supabase.co";

class RequestError extends Error {
  constructor(message: string, readonly status = 400) {
    super(message);
  }
}

function readServiceRoleKey(): string {
  const configured = String(process.env.SHARED_RESEARCH_SERVICE_KEY || "").trim();
  if (configured) return configured;
  if (process.platform !== "darwin") return "";
  const keychain = spawnSync("security", [
    "find-generic-password",
    "-a", "fgmijnuwplxasztyjfqr",
    "-s", "TaRaSha Shared Supabase Service Role",
    "-w",
  ], { encoding: "utf8", timeout: 3_000 });
  return keychain.status === 0 ? keychain.stdout.trim() : "";
}

function sendJson(response: ServerResponse, status: number, data: unknown): void {
  response.statusCode = status;
  response.setHeader("content-type", "application/json; charset=utf-8");
  response.setHeader("cache-control", "no-store");
  response.end(JSON.stringify(data));
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

function constituentIds(value: unknown): number[] | undefined {
  if (value === null || value === undefined) return undefined;
  if (!Array.isArray(value) || !value.length || value.length > 100 || value.some((id) => !/^research-[1-9]\d*$/.test(String(id)))) {
    throw new RequestError("Select between 1 and 100 valid industry constituents.");
  }
  return [...new Set(value.map((id) => Number(String(id).replace(/^research-/, ""))))];
}

async function handleResearchRequest(request: IncomingMessage, response: ServerResponse, env: ResearchProviderEnv): Promise<void> {
  const url = new URL(request.url || "/", "http://localhost");
  if (request.method === "GET" && url.pathname === "/api/health") {
    sendJson(response, 200, { ok: true, provider: "research-db", financialStorage: "browser-session-only", localProvider: true });
    return;
  }
  if (request.method === "GET" && url.pathname === "/api/companies") {
    const query = String(url.searchParams.get("query") || "");
    const country = url.searchParams.get("country") === "India" ? "India" : "USA";
    sendJson(response, 200, { companies: await searchResearchCompanies(env, query, country) });
    return;
  }
  if (request.method === "GET" && url.pathname === "/api/research/company") {
    const years = validYearRange(url.searchParams.get("fromYear"), url.searchParams.get("toYear"));
    const company = await pullResearchCompany(env, String(url.searchParams.get("companyId") || ""), years.fromYear, years.toYear);
    if (!company) throw new RequestError("Company was not found in TaRaSha Research.", 404);
    sendJson(response, 200, company);
    return;
  }
  if (request.method === "POST" && url.pathname === "/api/research/company") {
    const body = await readJsonBody(request);
    const years = validYearRange(body.fromYear, body.toYear);
    const company = await pullResearchCompany(env, String(body.companyId || ""), years.fromYear, years.toYear, constituentIds(body.constituentIds));
    if (!company) throw new RequestError("Company was not found in TaRaSha Research.", 404);
    sendJson(response, 200, company);
    return;
  }
  throw new RequestError("Local Research API endpoint not found.", 404);
}

export function localResearchApiPlugin(): Plugin | null {
  const serviceRoleKey = readServiceRoleKey();
  if (!serviceRoleKey) return null;
  const env: ResearchProviderEnv = {
    SHARED_RESEARCH_URL,
    SHARED_RESEARCH_SERVICE_KEY: serviceRoleKey,
  };
  return {
    name: "tarasha-local-research-api",
    apply: "serve",
    configureServer(server) {
      server.middlewares.use(async (request, response, next) => {
        if (!request.url?.startsWith("/api/")) {
          next();
          return;
        }
        try {
          await handleResearchRequest(request, response, env);
        } catch (cause) {
          const status = cause instanceof RequestError ? cause.status : 502;
          const message = cause instanceof Error ? cause.message : "Local Research API request failed.";
          sendJson(response, status, { error: message });
        }
      });
    },
  };
}
