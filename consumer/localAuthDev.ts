import { randomBytes } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import type { IncomingMessage, ServerResponse } from "node:http";
import { resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { DatabaseSync, type StatementSync } from "node:sqlite";
import type { Plugin } from "vite";
import {
  accountSummary,
  authenticateAccount,
  AUTH_SCHEMA_SQL,
  AuthError,
  type AuthDatabase,
  type AuthPreparedStatement,
  registerAccount,
  resetAccountPassword,
  securityQuestionFor,
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

function localAuthSecret(): string {
  const configured = String(process.env.CONSUMER_AUTH_SECRET || "").trim();
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

export function localAuthApiPlugin(): Plugin {
  const directory = localDataDirectory();
  const databasePath = String(process.env.CONSUMER_AUTH_DB_PATH || "").trim() || resolve(directory, "tarasha-consumer-auth.db");
  const sqlite = new DatabaseSync(databasePath);
  sqlite.exec("PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;");
  sqlite.exec(AUTH_SCHEMA_SQL);
  const db = new LocalAuthDatabase(sqlite);
  const authSecret = localAuthSecret();
  const adminKey = String(process.env.ADMIN_SYNC_KEY || authSecret).trim();

  return {
    name: "tarasha-local-auth-api",
    apply: "serve",
    configureServer(server) {
      server.middlewares.use(async (request, response, next) => {
        const url = new URL(request.url || "/", "http://localhost");
        const isAuth = url.pathname.startsWith("/api/auth/");
        const isAdminSummary = url.pathname === "/api/admin/users/summary";
        if (!isAuth && !isAdminSummary) {
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
            sendJson(response, 200, await authenticateAccount(db, authSecret, String(body.username ?? ""), String(body.password ?? "")));
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
