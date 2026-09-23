import { afterEach, describe, expect, it, vi } from "vitest";
import { authenticateUser, getSecurityQuestion, registerUser, resetPassword, SECURITY_QUESTIONS } from "./authStore";

const registration = {
  name: "Ada Lovelace",
  username: "ada.research",
  securityQuestion: SECURITY_QUESTIONS[2],
  securityAnswer: "Analytical Engines",
  password: "evidence-first",
  confirmPassword: "evidence-first",
};

describe("account API client", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("registers through the server account endpoint", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify({ name: registration.name, username: registration.username }), { status: 201 }));
    vi.stubGlobal("fetch", fetchMock);

    await expect(registerUser(registration)).resolves.toEqual({ name: registration.name, username: registration.username });
    expect(fetchMock).toHaveBeenCalledWith("/api/auth/register", expect.objectContaining({
      method: "POST",
      body: JSON.stringify(registration),
    }));
  });

  it("authenticates without creating a browser-local credential store", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify({ name: registration.name, username: registration.username }), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    await expect(authenticateUser(registration.username, registration.password)).resolves.toMatchObject({ username: registration.username });
    expect(fetchMock).toHaveBeenCalledWith("/api/auth/login", expect.objectContaining({ method: "POST" }));
  });

  it("retrieves the recovery question and submits a password reset", async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ question: registration.securityQuestion }), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ reset: true }), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    await expect(getSecurityQuestion(registration.username)).resolves.toBe(registration.securityQuestion);
    await expect(resetPassword(registration.username, registration.securityAnswer, "new-password")).resolves.toBeUndefined();
    expect(fetchMock).toHaveBeenLastCalledWith("/api/auth/reset-password", expect.objectContaining({ method: "POST" }));
  });

  it("surfaces the server error message", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify({ error: "Username or password is incorrect." }), { status: 401 })));
    await expect(authenticateUser("missing", "wrong-password")).rejects.toThrow("incorrect");
  });
});
