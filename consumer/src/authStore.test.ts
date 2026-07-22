import { describe, expect, it } from "vitest";
import { authenticateUser, getSecurityQuestion, registerUser, resetPassword } from "./authStore";

class MemoryStorage implements Storage {
  private values = new Map<string, string>();
  get length() { return this.values.size; }
  clear() { this.values.clear(); }
  getItem(key: string) { return this.values.get(key) ?? null; }
  key(index: number) { return [...this.values.keys()][index] ?? null; }
  removeItem(key: string) { this.values.delete(key); }
  setItem(key: string, value: string) { this.values.set(key, value); }
}

const registration = {
  name: "Ada Lovelace",
  username: "ada.research",
  securityQuestion: "What was my first research topic?",
  securityAnswer: "Analytical Engines",
  password: "evidence-first",
};

describe("local authentication store", () => {
  it("registers and authenticates a user without persisting plaintext secrets", async () => {
    const storage = new MemoryStorage();
    await registerUser(registration, storage);
    await expect(authenticateUser("ADA.RESEARCH", registration.password, storage)).resolves.toEqual({ name: registration.name, username: registration.username });
    const persisted = storage.getItem("tarasha-local-users-v1") ?? "";
    expect(persisted).not.toContain(registration.password);
    expect(persisted).not.toContain(registration.securityAnswer);
  });

  it("resets a password only when the security answer matches", async () => {
    const storage = new MemoryStorage();
    await registerUser(registration, storage);
    expect(getSecurityQuestion(registration.username, storage)).toBe(registration.securityQuestion);
    await expect(resetPassword(registration.username, "wrong", "new-password", storage)).rejects.toThrow("incorrect");
    await resetPassword(registration.username, "  analytical   engines ", "new-password", storage);
    await expect(authenticateUser(registration.username, registration.password, storage)).rejects.toThrow("incorrect");
    await expect(authenticateUser(registration.username, "new-password", storage)).resolves.toMatchObject({ username: registration.username });
  });
});
