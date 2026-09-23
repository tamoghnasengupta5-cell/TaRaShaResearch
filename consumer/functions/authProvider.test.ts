import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { DatabaseSync } from "node:sqlite";
import {
  accountSummary,
  authenticateApplicationAccount,
  authenticateAccount,
  AUTH_SCHEMA_SQL,
  type AuthDatabase,
  type AuthPreparedStatement,
  registerAccount,
  resetAccountPassword,
  securityQuestionFor,
  verifyAdminSession,
} from "./authProvider";

class TestStatement implements AuthPreparedStatement {
  private values: Array<string | number | null> = [];
  constructor(private readonly statement: ReturnType<DatabaseSync["prepare"]>) {}
  bind(...values: Array<string | number | null>) { this.values = values; return this; }
  async first<T>() { return (this.statement.get(...this.values) as T | undefined) ?? null; }
  async run() { return this.statement.run(...this.values); }
}

class TestDatabase implements AuthDatabase {
  constructor(readonly sqlite: DatabaseSync) {}
  prepare(query: string) { return new TestStatement(this.sqlite.prepare(query)); }
}

const authSecret = "test-only-auth-secret-with-at-least-32-characters";
const options = { pbkdf2Iterations: 1_000, now: () => 1_785_166_400 };
const registration = {
  name: "Ada Lovelace",
  username: "ada.research",
  securityQuestion: "What was the name of your first school?",
  securityAnswer: "Analytical Engines",
  password: "evidence-first",
  confirmPassword: "evidence-first",
};

describe("database-backed Consumer accounts", () => {
  let sqlite: DatabaseSync;
  let db: TestDatabase;

  beforeEach(() => {
    sqlite = new DatabaseSync(":memory:");
    sqlite.exec(AUTH_SCHEMA_SQL);
    db = new TestDatabase(sqlite);
  });

  afterEach(() => sqlite.close());

  it("stores encrypted identifiers and one-way credential hashes", async () => {
    await registerAccount(db, authSecret, registration, options);
    const stored = sqlite.prepare("select * from consumer_users").get() as Record<string, unknown>;

    expect(stored.username_ciphertext).not.toContain(registration.username);
    expect(stored.name_ciphertext).not.toContain(registration.name);
    expect(stored.username_lookup).not.toContain(registration.username);
    expect(stored.password_hash).toMatch(/^pbkdf2-sha256\$1000\$/);
    expect(stored.security_answer_hash).toMatch(/^pbkdf2-sha256\$1000\$/);
    expect(JSON.stringify(stored)).not.toContain(registration.password);
    expect(JSON.stringify(stored)).not.toContain(registration.securityAnswer);
    await expect(authenticateAccount(db, authSecret, "ADA.RESEARCH", registration.password, options))
      .resolves.toEqual({ name: registration.name, username: registration.username, role: "user" });
  });

  it("resets a password only after the recovery answer matches", async () => {
    await registerAccount(db, authSecret, registration, options);
    await expect(resetAccountPassword(db, authSecret, registration.username, "wrong", "new-password", options))
      .rejects.toThrow("incorrect");
    await resetAccountPassword(db, authSecret, registration.username, "  analytical   engines ", "new-password", options);
    await expect(authenticateAccount(db, authSecret, registration.username, registration.password, options)).rejects.toThrow("incorrect");
    await expect(authenticateAccount(db, authSecret, registration.username, "new-password", options)).resolves.toMatchObject({ username: registration.username });
  });

  it("rejects registration when the password confirmation differs", async () => {
    await expect(registerAccount(db, authSecret, { ...registration, confirmPassword: "different-password" }, options))
      .rejects.toThrow("must match");
    await expect(accountSummary(db)).resolves.toMatchObject({ uniqueUsers: 0 });
  });

  it("returns an aggregate count without exposing account fields", async () => {
    await registerAccount(db, authSecret, registration, options);
    await registerAccount(db, authSecret, { ...registration, name: "Grace Hopper", username: "grace.research" }, options);

    await expect(accountSummary(db)).resolves.toEqual({
      uniqueUsers: 2,
      activeUsers: 2,
      firstSignupAt: options.now(),
      latestSignupAt: options.now(),
    });
  });

  it("does not reveal whether an unknown username exists during question lookup", async () => {
    await registerAccount(db, authSecret, registration, options);
    await expect(securityQuestionFor(db, authSecret, registration.username)).resolves.toBe(registration.securityQuestion);
    await expect(securityQuestionFor(db, authSecret, "missing.user")).resolves.toMatch(/\?$/);
  });

  it("issues a signed Admin session only for the configured Admin credentials", async () => {
    const admin = await authenticateApplicationAccount(
      db,
      authSecret,
      "Admin",
      "Admin@123",
      "Admin@123",
      options,
    );

    expect(admin).toMatchObject({ username: "Admin", role: "admin" });
    expect(admin.adminToken).toMatch(/^v1\./);
    await expect(verifyAdminSession(authSecret, admin.adminToken || "", options.now()))
      .resolves.toBe(true);
    await expect(authenticateApplicationAccount(
      db,
      authSecret,
      "Admin",
      "wrong-password",
      "Admin@123",
      options,
    )).rejects.toThrow("incorrect");
  });
});
