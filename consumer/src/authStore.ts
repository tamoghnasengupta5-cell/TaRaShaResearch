export interface AuthenticatedUser {
  name: string;
  username: string;
}

export interface RegistrationInput extends AuthenticatedUser {
  securityQuestion: string;
  securityAnswer: string;
  password: string;
  confirmPassword: string;
}

export const SECURITY_QUESTIONS = [
  "What was the name of your first pet?",
  "What was the name of the street you grew up on?",
  "What was the name of your first school?",
  "What was your childhood nickname?",
  "What is the first name of your oldest cousin?",
  "What was the make of your first car?",
  "What was the first company you worked for?",
] as const;

interface SecurityQuestionResponse { question: string }

const apiBase = String(import.meta.env.VITE_API_BASE_URL || "").replace(/\/$/, "");
const legacyStorageKey = "tarasha-local-users-v1";

function clearLegacyBrowserAccounts(): void {
  try { if (typeof window !== "undefined") window.localStorage.removeItem(legacyStorageKey); }
  catch { /* Browser storage may be unavailable in privacy-restricted contexts. */ }
}

async function requestJson<T>(path: string, body: Record<string, string>): Promise<T> {
  const response = await fetch(`${apiBase}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const payload = await response.json().catch(() => ({})) as { error?: string };
  if (!response.ok) throw new Error(payload.error || `Account request failed with status ${response.status}.`);
  return payload as T;
}

export async function registerUser(input: RegistrationInput): Promise<AuthenticatedUser> {
  const user = await requestJson<AuthenticatedUser>("/api/auth/register", { ...input });
  clearLegacyBrowserAccounts();
  return user;
}

export async function authenticateUser(username: string, password: string): Promise<AuthenticatedUser> {
  const user = await requestJson<AuthenticatedUser>("/api/auth/login", { username, password });
  clearLegacyBrowserAccounts();
  return user;
}

export async function getSecurityQuestion(username: string): Promise<string> {
  const result = await requestJson<SecurityQuestionResponse>("/api/auth/security-question", { username });
  if (!result.question) throw new Error("Account recovery is temporarily unavailable.");
  return result.question;
}

export async function resetPassword(username: string, answer: string, newPassword: string): Promise<void> {
  await requestJson<{ reset: boolean }>("/api/auth/reset-password", { username, answer, newPassword });
}
