export interface AuthPreparedStatement {
  bind(...values: Array<string | number | null>): AuthPreparedStatement;
  first<T>(): Promise<T | null>;
  run(): Promise<unknown>;
}

export interface AuthDatabase {
  prepare(query: string): AuthPreparedStatement;
}

export interface AuthenticatedAccount {
  name: string;
  username: string;
  role: "admin" | "user";
  adminToken?: string;
}

export interface RegistrationPayload {
  name: string;
  username: string;
  securityQuestion: string;
  securityAnswer: string;
  password: string;
  confirmPassword: string;
}

export interface AuthServiceOptions {
  pbkdf2Iterations?: number;
  now?: () => number;
}

interface AccountRow {
  id: string;
  username_ciphertext: string;
  name_ciphertext: string;
  security_question_code: string;
  password_hash: string;
  security_answer_hash: string;
  status: "active" | "disabled";
}

export class AuthError extends Error {
  constructor(message: string, readonly status = 400) {
    super(message);
  }
}

export const SECURITY_QUESTION_CODES = {
  "What was the name of your first pet?": "first_pet",
  "What was the name of the street you grew up on?": "childhood_street",
  "What was the name of your first school?": "first_school",
  "What was your childhood nickname?": "childhood_nickname",
  "What is the first name of your oldest cousin?": "oldest_cousin",
  "What was the make of your first car?": "first_car",
  "What was the first company you worked for?": "first_employer",
} as const;

const SECURITY_QUESTIONS_BY_CODE = Object.fromEntries(
  Object.entries(SECURITY_QUESTION_CODES).map(([question, code]) => [code, question]),
) as Record<string, string>;

export const AUTH_SCHEMA_SQL = `
create table if not exists consumer_users (
  id text primary key,
  username_lookup text not null unique,
  username_ciphertext text not null,
  name_ciphertext text not null,
  security_question_code text not null,
  password_hash text not null,
  security_answer_hash text not null,
  status text not null default 'active' check (status in ('active', 'disabled')),
  created_at integer not null,
  updated_at integer not null,
  last_login_at integer,
  password_reset_at integer
);
create unique index if not exists consumer_users_username_lookup on consumer_users(username_lookup);
create index if not exists consumer_users_created_at on consumer_users(created_at);

create table if not exists consumer_auth_limits (
  limit_key text primary key,
  window_started_at integer not null,
  attempts integer not null
);
create index if not exists consumer_auth_limits_window on consumer_auth_limits(window_started_at);
`;

const encoder = new TextEncoder();
const DEFAULT_PBKDF2_ITERATIONS = 600_000;
const RATE_LIMIT_WINDOW_SECONDS = 15 * 60;
const RATE_LIMIT_ATTEMPTS = 10;
const ADMIN_SESSION_SECONDS = 8 * 60 * 60;
const derivedKeyCache = new Map<string, Promise<CryptoKey>>();

function cleanName(value: unknown): string {
  return String(value ?? "").trim().replace(/[\u0000-\u001f]/g, "").slice(0, 120);
}

function cleanUsername(value: unknown): string {
  return String(value ?? "").trim().slice(0, 32);
}

function normalizedUsername(value: unknown): string {
  return cleanUsername(value).toLowerCase();
}

function normalizedAnswer(value: unknown): string {
  return String(value ?? "").trim().toLowerCase().replace(/\s+/g, " ");
}

function requireAuthSecret(secret: string): string {
  const value = String(secret ?? "").trim();
  if (value.length < 32) throw new AuthError("Account storage is not configured.", 503);
  return value;
}

function bytesToBase64Url(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function base64UrlToBytes(value: string): Uint8Array {
  const normalized = value.replace(/-/g, "+").replace(/_/g, "/");
  const padded = normalized.padEnd(Math.ceil(normalized.length / 4) * 4, "=");
  const binary = atob(padded);
  return Uint8Array.from(binary, (character) => character.charCodeAt(0));
}

function randomBytes(length: number): Uint8Array {
  const bytes = new Uint8Array(length);
  crypto.getRandomValues(bytes);
  return bytes;
}

function ownedArrayBuffer(bytes: Uint8Array): ArrayBuffer {
  return bytes.slice().buffer as ArrayBuffer;
}

async function derivedKey(
  authSecret: string,
  purpose: string,
  algorithm: AesKeyGenParams | HmacImportParams,
  usages: KeyUsage[],
): Promise<CryptoKey> {
  const secret = requireAuthSecret(authSecret);
  const cacheKey = `${secret}:${purpose}:${algorithm.name}`;
  let pending = derivedKeyCache.get(cacheKey);
  if (!pending) {
    pending = (async () => {
      const source = await crypto.subtle.importKey("raw", encoder.encode(secret), "HKDF", false, ["deriveKey"]);
      return crypto.subtle.deriveKey(
        { name: "HKDF", hash: "SHA-256", salt: encoder.encode("tarasha-consumer-auth-v1"), info: encoder.encode(purpose) },
        source,
        algorithm,
        false,
        usages,
      );
    })();
    derivedKeyCache.set(cacheKey, pending);
  }
  return pending;
}

async function usernameLookup(authSecret: string, username: string): Promise<string> {
  const key = await derivedKey(authSecret, "username-lookup", { name: "HMAC", hash: "SHA-256", length: 256 }, ["sign"]);
  const digest = await crypto.subtle.sign("HMAC", key, encoder.encode(normalizedUsername(username)));
  return bytesToBase64Url(new Uint8Array(digest));
}

async function pepperedCredential(authSecret: string, purpose: "password" | "security-answer", value: string): Promise<ArrayBuffer> {
  const key = await derivedKey(authSecret, `${purpose}-pepper`, { name: "HMAC", hash: "SHA-256", length: 256 }, ["sign"]);
  return crypto.subtle.sign("HMAC", key, encoder.encode(value));
}

async function hashCredential(
  authSecret: string,
  purpose: "password" | "security-answer",
  value: string,
  iterations: number,
  salt = randomBytes(16),
): Promise<string> {
  const peppered = await pepperedCredential(authSecret, purpose, value);
  const key = await crypto.subtle.importKey("raw", peppered, "PBKDF2", false, ["deriveBits"]);
  const digest = await crypto.subtle.deriveBits({ name: "PBKDF2", hash: "SHA-256", salt: ownedArrayBuffer(salt), iterations }, key, 256);
  return `pbkdf2-sha256$${iterations}$${bytesToBase64Url(salt)}$${bytesToBase64Url(new Uint8Array(digest))}`;
}

function constantTimeEqual(left: Uint8Array, right: Uint8Array): boolean {
  if (left.length !== right.length) return false;
  let difference = 0;
  for (let index = 0; index < left.length; index += 1) difference |= left[index] ^ right[index];
  return difference === 0;
}

async function signedValue(authSecret: string, purpose: string, value: string): Promise<Uint8Array> {
  const key = await derivedKey(
    authSecret,
    purpose,
    { name: "HMAC", hash: "SHA-256", length: 256 },
    ["sign"],
  );
  return new Uint8Array(await crypto.subtle.sign("HMAC", key, encoder.encode(value)));
}

async function secureTextEqual(authSecret: string, purpose: string, left: string, right: string): Promise<boolean> {
  const [leftDigest, rightDigest] = await Promise.all([
    signedValue(authSecret, purpose, left),
    signedValue(authSecret, purpose, right),
  ]);
  return constantTimeEqual(leftDigest, rightDigest);
}

async function createAdminSession(authSecret: string, now: number): Promise<string> {
  const payload = bytesToBase64Url(encoder.encode(JSON.stringify({
    sub: "Admin",
    role: "admin",
    iat: now,
    exp: now + ADMIN_SESSION_SECONDS,
    nonce: bytesToBase64Url(randomBytes(16)),
  })));
  const signature = await signedValue(authSecret, "admin-session-v1", payload);
  return `v1.${payload}.${bytesToBase64Url(signature)}`;
}

export async function verifyAdminSession(
  authSecret: string,
  token: string,
  now = Math.floor(Date.now() / 1000),
): Promise<boolean> {
  const [version, payload, signatureText] = String(token || "").split(".");
  if (version !== "v1" || !payload || !signatureText) return false;
  const expected = await signedValue(authSecret, "admin-session-v1", payload);
  if (!constantTimeEqual(expected, base64UrlToBytes(signatureText))) return false;
  try {
    const claims = JSON.parse(new TextDecoder().decode(base64UrlToBytes(payload))) as {
      sub?: string;
      role?: string;
      exp?: number;
    };
    return claims.sub === "Admin" && claims.role === "admin" && Number(claims.exp) > now;
  } catch {
    return false;
  }
}

async function verifyCredential(
  authSecret: string,
  purpose: "password" | "security-answer",
  value: string,
  stored: string,
): Promise<boolean> {
  const [algorithm, iterationsText, saltText, digestText] = stored.split("$");
  const iterations = Number(iterationsText);
  if (algorithm !== "pbkdf2-sha256" || !Number.isInteger(iterations) || iterations < 1 || !saltText || !digestText) return false;
  const candidate = await hashCredential(authSecret, purpose, value, iterations, base64UrlToBytes(saltText));
  const candidateDigest = candidate.split("$")[3];
  return constantTimeEqual(base64UrlToBytes(candidateDigest), base64UrlToBytes(digestText));
}

async function consumeCredentialWork(authSecret: string, purpose: "password" | "security-answer", value: string, iterations: number): Promise<void> {
  await hashCredential(authSecret, purpose, value, iterations, new Uint8Array(16));
}

async function encryptField(authSecret: string, plaintext: string): Promise<string> {
  const key = await derivedKey(authSecret, "account-field-encryption", { name: "AES-GCM", length: 256 }, ["encrypt", "decrypt"]);
  const iv = randomBytes(12);
  const ciphertext = await crypto.subtle.encrypt({ name: "AES-GCM", iv: ownedArrayBuffer(iv) }, key, encoder.encode(plaintext));
  return `v1.${bytesToBase64Url(iv)}.${bytesToBase64Url(new Uint8Array(ciphertext))}`;
}

async function decryptField(authSecret: string, value: string): Promise<string> {
  const [version, ivText, ciphertextText] = value.split(".");
  if (version !== "v1" || !ivText || !ciphertextText) throw new AuthError("Stored account data could not be read.", 500);
  const key = await derivedKey(authSecret, "account-field-encryption", { name: "AES-GCM", length: 256 }, ["encrypt", "decrypt"]);
  const plaintext = await crypto.subtle.decrypt(
    { name: "AES-GCM", iv: ownedArrayBuffer(base64UrlToBytes(ivText)) },
    key,
    ownedArrayBuffer(base64UrlToBytes(ciphertextText)),
  );
  return new TextDecoder().decode(plaintext);
}

function optionsWithDefaults(options: AuthServiceOptions): Required<AuthServiceOptions> {
  return {
    pbkdf2Iterations: options.pbkdf2Iterations ?? DEFAULT_PBKDF2_ITERATIONS,
    now: options.now ?? (() => Math.floor(Date.now() / 1000)),
  };
}

async function enforceRateLimit(db: AuthDatabase, key: string, now: number): Promise<void> {
  await db.prepare("delete from consumer_auth_limits where window_started_at < ?1")
    .bind(now - RATE_LIMIT_WINDOW_SECONDS).run();
  const current = await db.prepare("select window_started_at, attempts from consumer_auth_limits where limit_key = ?1")
    .bind(key).first<{ window_started_at: number; attempts: number }>();
  if (current && current.attempts >= RATE_LIMIT_ATTEMPTS) throw new AuthError("Too many attempts. Try again later.", 429);
  await db.prepare(`
    insert into consumer_auth_limits(limit_key, window_started_at, attempts) values(?1, ?2, 1)
    on conflict(limit_key) do update set attempts = attempts + 1
  `).bind(key, now).run();
}

async function clearRateLimit(db: AuthDatabase, key: string): Promise<void> {
  await db.prepare("delete from consumer_auth_limits where limit_key = ?1").bind(key).run();
}

export async function registerAccount(
  db: AuthDatabase,
  authSecret: string,
  payload: RegistrationPayload,
  options: AuthServiceOptions = {},
): Promise<AuthenticatedAccount> {
  const configured = optionsWithDefaults(options);
  const name = cleanName(payload.name);
  const username = cleanUsername(payload.username);
  const normalized = normalizedUsername(username);
  const questionCode = SECURITY_QUESTION_CODES[payload.securityQuestion as keyof typeof SECURITY_QUESTION_CODES];
  if (name.length < 2) throw new AuthError("Enter your full name.");
  if (!/^[a-zA-Z0-9._-]{3,32}$/.test(username)) throw new AuthError("Username must be 3–32 characters and use only letters, numbers, dots, hyphens, or underscores.");
  if (!questionCode) throw new AuthError("Choose a security question from the list.");
  if (!normalizedAnswer(payload.securityAnswer)) throw new AuthError("Enter an answer to your security question.");
  if (String(payload.password ?? "").length < 8) throw new AuthError("Password must contain at least 8 characters.");
  if (payload.password !== payload.confirmPassword) throw new AuthError("Password and confirm password must match.");
  const lookup = await usernameLookup(authSecret, normalized);
  const existing = await db.prepare("select id from consumer_users where username_lookup = ?1").bind(lookup).first<{ id: string }>();
  if (existing) throw new AuthError("That username is already registered.", 409);
  const [usernameCiphertext, nameCiphertext, passwordHash, securityAnswerHash] = await Promise.all([
    encryptField(authSecret, username),
    encryptField(authSecret, name),
    hashCredential(authSecret, "password", payload.password, configured.pbkdf2Iterations),
    hashCredential(authSecret, "security-answer", normalizedAnswer(payload.securityAnswer), configured.pbkdf2Iterations),
  ]);
  const now = configured.now();
  try {
    await db.prepare(`
      insert into consumer_users(
        id, username_lookup, username_ciphertext, name_ciphertext, security_question_code,
        password_hash, security_answer_hash, status, created_at, updated_at
      ) values(?1, ?2, ?3, ?4, ?5, ?6, ?7, 'active', ?8, ?8)
    `).bind(crypto.randomUUID(), lookup, usernameCiphertext, nameCiphertext, questionCode, passwordHash, securityAnswerHash, now).run();
  } catch (cause) {
    if (cause instanceof Error && /unique|constraint/i.test(cause.message)) {
      throw new AuthError("That username is already registered.", 409);
    }
    throw new AuthError("Account registration is temporarily unavailable.", 503);
  }
  return { name, username, role: "user" };
}

export async function authenticateAccount(
  db: AuthDatabase,
  authSecret: string,
  username: string,
  password: string,
  options: AuthServiceOptions = {},
): Promise<AuthenticatedAccount> {
  const configured = optionsWithDefaults(options);
  const lookup = await usernameLookup(authSecret, username);
  const limitKey = `login:${lookup}`;
  await enforceRateLimit(db, limitKey, configured.now());
  const account = await db.prepare(`
    select id, username_ciphertext, name_ciphertext, security_question_code, password_hash, security_answer_hash, status
    from consumer_users where username_lookup = ?1
  `).bind(lookup).first<AccountRow>();
  if (!account) {
    await consumeCredentialWork(authSecret, "password", password, configured.pbkdf2Iterations);
    throw new AuthError("Username or password is incorrect.", 401);
  }
  const valid = account.status === "active" && await verifyCredential(authSecret, "password", password, account.password_hash);
  if (!valid) throw new AuthError("Username or password is incorrect.", 401);
  await clearRateLimit(db, limitKey);
  await db.prepare("update consumer_users set last_login_at = ?2, updated_at = ?2 where id = ?1")
    .bind(account.id, configured.now()).run();
  return {
    name: await decryptField(authSecret, account.name_ciphertext),
    username: await decryptField(authSecret, account.username_ciphertext),
    role: "user",
  };
}

export async function authenticateApplicationAccount(
  db: AuthDatabase,
  authSecret: string,
  username: string,
  password: string,
  adminPassword: string,
  options: AuthServiceOptions = {},
): Promise<AuthenticatedAccount> {
  const configured = optionsWithDefaults(options);
  if (normalizedUsername(username) !== "admin") {
    return authenticateAccount(db, authSecret, username, password, options);
  }
  const lookup = await usernameLookup(authSecret, "admin");
  const limitKey = `login:${lookup}`;
  await enforceRateLimit(db, limitKey, configured.now());
  const valid = await secureTextEqual(authSecret, "admin-password-check", password, adminPassword);
  if (!valid) throw new AuthError("Username or password is incorrect.", 401);
  await clearRateLimit(db, limitKey);
  return {
    name: "TaRaSha Administrator",
    username: "Admin",
    role: "admin",
    adminToken: await createAdminSession(authSecret, configured.now()),
  };
}

export async function securityQuestionFor(db: AuthDatabase, authSecret: string, username: string): Promise<string> {
  const lookup = await usernameLookup(authSecret, username);
  const account = await db.prepare("select security_question_code from consumer_users where username_lookup = ?1 and status = 'active'")
    .bind(lookup).first<{ security_question_code: string }>();
  if (account && SECURITY_QUESTIONS_BY_CODE[account.security_question_code]) return SECURITY_QUESTIONS_BY_CODE[account.security_question_code];
  const decoyIndex = base64UrlToBytes(lookup)[0] % Object.keys(SECURITY_QUESTION_CODES).length;
  return Object.keys(SECURITY_QUESTION_CODES)[decoyIndex];
}

export async function resetAccountPassword(
  db: AuthDatabase,
  authSecret: string,
  username: string,
  answer: string,
  newPassword: string,
  options: AuthServiceOptions = {},
): Promise<void> {
  const configured = optionsWithDefaults(options);
  if (String(newPassword ?? "").length < 8) throw new AuthError("New password must contain at least 8 characters.");
  const lookup = await usernameLookup(authSecret, username);
  const limitKey = `reset:${lookup}`;
  await enforceRateLimit(db, limitKey, configured.now());
  const account = await db.prepare(`
    select id, username_ciphertext, name_ciphertext, security_question_code, password_hash, security_answer_hash, status
    from consumer_users where username_lookup = ?1
  `).bind(lookup).first<AccountRow>();
  if (!account) {
    await consumeCredentialWork(authSecret, "security-answer", normalizedAnswer(answer), configured.pbkdf2Iterations);
    throw new AuthError("The security answer is incorrect.", 401);
  }
  const valid = account.status === "active"
    && await verifyCredential(authSecret, "security-answer", normalizedAnswer(answer), account.security_answer_hash);
  if (!valid) throw new AuthError("The security answer is incorrect.", 401);
  const passwordHash = await hashCredential(authSecret, "password", newPassword, configured.pbkdf2Iterations);
  const now = configured.now();
  await db.prepare("update consumer_users set password_hash = ?2, password_reset_at = ?3, updated_at = ?3 where id = ?1")
    .bind(account.id, passwordHash, now).run();
  await clearRateLimit(db, limitKey);
}

export async function accountSummary(db: AuthDatabase): Promise<{
  uniqueUsers: number;
  activeUsers: number;
  firstSignupAt: number | null;
  latestSignupAt: number | null;
}> {
  const row = await db.prepare(`
    select count(*) as unique_users,
           sum(case when status = 'active' then 1 else 0 end) as active_users,
           min(created_at) as first_signup_at,
           max(created_at) as latest_signup_at
    from consumer_users
  `).first<{ unique_users: number; active_users: number | null; first_signup_at: number | null; latest_signup_at: number | null }>();
  return {
    uniqueUsers: Number(row?.unique_users ?? 0),
    activeUsers: Number(row?.active_users ?? 0),
    firstSignupAt: row?.first_signup_at ?? null,
    latestSignupAt: row?.latest_signup_at ?? null,
  };
}
