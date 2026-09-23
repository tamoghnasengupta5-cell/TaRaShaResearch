import { randomBytes } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import type { IncomingMessage, ServerResponse } from "node:http";
import { resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { DatabaseSync, type StatementSync } from "node:sqlite";
import type { Plugin } from "vite";
import {
  accountSummary,
  authenticateApplicationAccount,
  AUTH_SCHEMA_SQL,
  AuthError,
  type AuthDatabase,
  type AuthPreparedStatement,
  registerAccount,
  resetAccountPassword,
  securityQuestionFor,
  verifyAdminSession,
} from "./functions/authProvider";

class LocalPreparedStatement implements AuthPreparedStatement {
  private values: Array<string | number | null> = [];

  constructor(private readonly statement: StatementSync) {}

  bind(...values: Array<string | number | null>): AuthPreparedStatement {
    this.values = values;
    return this;
  }

  async first<T>(): Promise<T | null> {
    return (this.statement.get(...this.values) as T | undefined) ?? null;
  }

  async run(): Promise<unknown> {
    return this.statement.run(...this.values);
  }
}

class LocalAuthDatabase implements AuthDatabase {
  constructor(private readonly database: DatabaseSync) {}

  prepare(query: string): AuthPreparedStatement {
    return new LocalPreparedStatement(this.database.prepare(query));
  }
}

function localDataDirectory(): string {
  const directory = resolve(process.cwd(), ".local-data");
  mkdirSync(directory, { recursive: true, mode: 0o700 });
  return directory;
}

function readKeychainSecret(): string {
  if (process.platform !== "darwin") return "";
  const result = spawnSync("security", [
    "find-generic-password",
    "-a", "local-development",
    "-s", "TaRaSha Consumer Local Auth Secret",
    "-w",
  ], { encoding: "utf8", timeout: 3_000 });
  return result.status === 0 ? result.stdout.trim() : "";
}

function saveKeychainSecret(secret: string): boolean {
  if (process.platform !== "darwin") return false;
  const result = spawnSync("security", [
    "add-generic-password",
    "-a", "local-development",
    "-s", "TaRaSha Consumer Local Auth Secret",
    "-w", secret,
    "-U",
  ], { encoding: "utf8", timeout: 3_000 });
  return result.status === 0;
}

function localAuthSecret(environment: NodeJS.ProcessEnv = process.env): string {
  const configured = String(environment.CONSUMER_AUTH_SECRET || "").trim();
  if (configured) return configured;
  const keychain = readKeychainSecret();
  if (keychain) return keychain;

  const fallbackPath = resolve(localDataDirectory(), "auth-secret");
  if (existsSync(fallbackPath)) return readFileSync(fallbackPath, "utf8").trim();

  const generated = randomBytes(32).toString("base64url");
  if (saveKeychainSecret(generated)) return generated;
  writeFileSync(fallbackPath, generated, { encoding: "utf8", mode: 0o600 });
  return generated;
}

function envFileValue(contents: string, name: string): string {
  const assignment = new RegExp(`^\\s*(?:export\\s+)?${name}\\s*=\\s*(.*)$`);
  for (const line of contents.split(/\r?\n/)) {
    const match = line.match(assignment);
    if (!match) continue;
    const raw = match[1].trim();
    if (
      raw.length >= 2
      && ((raw.startsWith('"') && raw.endsWith('"')) || (raw.startsWith("'") && raw.endsWith("'")))
    ) {
      return raw.slice(1, -1).trim();
    }
    return raw.replace(/\s+#.*$/, "").trim();
  }
  return "";
}

export function localDataReconciliationAdminKey(
  environment: NodeJS.ProcessEnv = process.env,
  candidateFiles?: string[],
): string {
  const configured = String(
    environment.TARASHA_DATA_ADMIN_KEY || environment.ADMIN_API_KEY || "",
  ).trim();
  if (configured) return configured;

  const files = candidateFiles ?? [
    String(environment.TARASHA_DATA_ENV_PATH || "").trim(),
    resolve(process.cwd(), "../../TaRaShaData.ai/.env"),
    resolve(process.cwd(), "../TaRaShaData.ai/.env"),
  ];
  for (const filename of new Set(files.filter(Boolean))) {
    if (!existsSync(filename)) continue;
    const value = envFileValue(readFileSync(filename, "utf8"), "ADMIN_API_KEY");
    if (value) return value;
  }
  return "";
}

function sendJson(response: ServerResponse, status: number, data: unknown): void {
  response.statusCode = status;
  response.setHeader("content-type", "application/json; charset=utf-8");
  response.setHeader("cache-control", "no-store");
  response.end(JSON.stringify(data));
}

async function readJsonBody(request: IncomingMessage): Promise<Record<string, unknown>> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    size += buffer.length;
    if (size > 32 * 1024) throw new AuthError("Request body is too large.", 413);
    chunks.push(buffer);
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}") as Record<string, unknown>;
  } catch {
    throw new AuthError("Request body must be valid JSON.");
  }
}

async function readRawBody(request: IncomingMessage, maximumBytes = 3 * 1024 * 1024): Promise<Buffer> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    size += buffer.length;
    if (size > maximumBytes) throw new AuthError("Request body is too large.", 413);
    chunks.push(buffer);
  }
  return Buffer.concat(chunks);
}

async function proxyDataReconciliation(
  request: IncomingMessage,
  response: ServerResponse,
  authSecret: string,
  environment: NodeJS.ProcessEnv,
): Promise<void> {
  const authorization = String(request.headers.authorization || "");
  const token = authorization.startsWith("Bearer ") ? authorization.slice(7).trim() : "";
  if (!await verifyAdminSession(authSecret, token)) throw new AuthError("Admin authorization failed.", 401);
  const base = String(environment.TARASHA_DATA_API_URL || "http://127.0.0.1:8000").trim().replace(/\/$/, "");
  const adminKey = localDataReconciliationAdminKey(environment);
  if (!adminKey) throw new AuthError("Data reconciliation is not configured.", 503);
  const incoming = new URL(request.url || "/", "http://localhost");
  const suffix = incoming.pathname.replace(/^\/api\/admin\/data-reconciliation\/?/, "");
  const upstream = new URL(`/v1/admin/data-reconciliation${suffix ? `/${suffix}` : ""}`, base);
  upstream.search = incoming.search;
  const headers = new Headers({ Accept: "application/json", "X-Admin-Key": adminKey, "X-Admin-Actor": "Admin" });
  const contentType = request.headers["content-type"];
  if (contentType) headers.set("Content-Type", contentType);
  const method = request.method || "GET";
  const rawBody = ["GET", "HEAD"].includes(method) ? undefined : await readRawBody(request);
  const body = rawBody
    ? rawBody.buffer.slice(rawBody.byteOffset, rawBody.byteOffset + rawBody.byteLength) as ArrayBuffer
    : undefined;
  const upstreamResponse = await fetch(upstream, { method, headers, body });
  response.statusCode = upstreamResponse.status;
  response.setHeader("cache-control", "no-store");
  const upstreamContentType = upstreamResponse.headers.get("content-type");
  if (upstreamContentType) response.setHeader("content-type", upstreamContentType);
  response.end(Buffer.from(await upstreamResponse.arrayBuffer()));
}

export function localAuthApiPlugin(environment: NodeJS.ProcessEnv = process.env): Plugin {
  const directory = localDataDirectory();
  const databasePath = String(environment.CONSUMER_AUTH_DB_PATH || "").trim() || resolve(directory, "tarasha-consumer-auth.db");
  const sqlite = new DatabaseSync(databasePath);
  sqlite.exec("PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;");
  sqlite.exec(AUTH_SCHEMA_SQL);
  const db = new LocalAuthDatabase(sqlite);
  const authSecret = localAuthSecret(environment);
  const adminKey = String(environment.ADMIN_SYNC_KEY || authSecret).trim();

  return {
    name: "tarasha-local-auth-api",
    apply: "serve",
    configureServer(server) {
      server.middlewares.use(async (request, response, next) => {
        const url = new URL(request.url || "/", "http://localhost");
        const isAuth = url.pathname.startsWith("/api/auth/");
        const isAdminSummary = url.pathname === "/api/admin/users/summary";
        const isDataReconciliation = url.pathname.startsWith("/api/admin/data-reconciliation");
        if (!isAuth && !isAdminSummary && !isDataReconciliation) {
          next();
          return;
        }
        try {
          if (request.method === "POST" && url.pathname === "/api/auth/register") {
            const body = await readJsonBody(request);
            const account = await registerAccount(db, authSecret, {
              name: String(body.name ?? ""),
              username: String(body.username ?? ""),
              securityQuestion: String(body.securityQuestion ?? ""),
              securityAnswer: String(body.securityAnswer ?? ""),
              password: String(body.password ?? ""),
              confirmPassword: String(body.confirmPassword ?? ""),
            });
            sendJson(response, 201, account);
            return;
          }
          if (request.method === "POST" && url.pathname === "/api/auth/login") {
            const body = await readJsonBody(request);
            sendJson(response, 200, await authenticateApplicationAccount(
              db,
              authSecret,
              String(body.username ?? ""),
              String(body.password ?? ""),
              String(environment.CONSUMER_ADMIN_PASSWORD || "Admin@123"),
            ));
            return;
          }
          if (isDataReconciliation) {
            await proxyDataReconciliation(request, response, authSecret, environment);
            return;
          }
          if (request.method === "POST" && url.pathname === "/api/auth/security-question") {
            const body = await readJsonBody(request);
            sendJson(response, 200, { question: await securityQuestionFor(db, authSecret, String(body.username ?? "")) });
            return;
          }
          if (request.method === "POST" && url.pathname === "/api/auth/reset-password") {
            const body = await readJsonBody(request);
            await resetAccountPassword(db, authSecret, String(body.username ?? ""), String(body.answer ?? ""), String(body.newPassword ?? ""));
            sendJson(response, 200, { reset: true });
            return;
          }
          if (request.method === "GET" && isAdminSummary) {
            if (request.headers["x-admin-key"] !== adminKey) throw new AuthError("Admin authorization failed.", 401);
            sendJson(response, 200, await accountSummary(db));
            return;
          }
          throw new AuthError("Local authentication endpoint not found.", 404);
        } catch (cause) {
          const status = cause instanceof AuthError ? cause.status : 503;
          const message = cause instanceof AuthError ? cause.message : "Local account service is temporarily unavailable.";
          sendJson(response, status, { error: message });
        }
      });
    },
  };
}
