export interface AuthenticatedUser {
  name: string;
  username: string;
}

export interface RegistrationInput extends AuthenticatedUser {
  securityQuestion: string;
  securityAnswer: string;
  password: string;
}

interface StoredUser extends AuthenticatedUser {
  securityQuestion: string;
  passwordSalt: string;
  passwordHash: string;
  answerSalt: string;
  answerHash: string;
}

const USERS_KEY = "tarasha-local-users-v1";

function normalizedUsername(username: string): string {
  return username.trim().toLowerCase();
}

function normalizedAnswer(answer: string): string {
  return answer.trim().toLowerCase().replace(/\s+/g, " ");
}

function readUsers(storage: Storage): Record<string, StoredUser> {
  try {
    const parsed = JSON.parse(storage.getItem(USERS_KEY) ?? "{}");
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

function writeUsers(storage: Storage, users: Record<string, StoredUser>): void {
  storage.setItem(USERS_KEY, JSON.stringify(users));
}

function createSalt(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

async function hashSecret(secret: string, salt: string): Promise<string> {
  const input = new TextEncoder().encode(`${salt}:${secret}`);
  const digest = await crypto.subtle.digest("SHA-256", input);
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function publicUser(user: StoredUser): AuthenticatedUser {
  return { name: user.name, username: user.username };
}

export async function registerUser(input: RegistrationInput, storage: Storage = localStorage): Promise<AuthenticatedUser> {
  const name = input.name.trim();
  const username = input.username.trim();
  const key = normalizedUsername(username);
  const securityQuestion = input.securityQuestion.trim();
  if (name.length < 2) throw new Error("Enter your full name.");
  if (!/^[a-zA-Z0-9._-]{3,32}$/.test(username)) throw new Error("Username must be 3–32 characters and use only letters, numbers, dots, hyphens, or underscores.");
  if (securityQuestion.length < 5) throw new Error("Enter a security question you will remember.");
  if (!input.securityAnswer.trim()) throw new Error("Enter an answer to your security question.");
  if (input.password.length < 8) throw new Error("Password must contain at least 8 characters.");

  const users = readUsers(storage);
  if (users[key]) throw new Error("That username is already registered.");
  const passwordSalt = createSalt();
  const answerSalt = createSalt();
  const user: StoredUser = {
    name,
    username,
    securityQuestion,
    passwordSalt,
    passwordHash: await hashSecret(input.password, passwordSalt),
    answerSalt,
    answerHash: await hashSecret(normalizedAnswer(input.securityAnswer), answerSalt),
  };
  users[key] = user;
  writeUsers(storage, users);
  return publicUser(user);
}

export async function authenticateUser(username: string, password: string, storage: Storage = localStorage): Promise<AuthenticatedUser> {
  const user = readUsers(storage)[normalizedUsername(username)];
  if (!user || await hashSecret(password, user.passwordSalt) !== user.passwordHash) {
    throw new Error("Username or password is incorrect.");
  }
  return publicUser(user);
}

export function getSecurityQuestion(username: string, storage: Storage = localStorage): string {
  const user = readUsers(storage)[normalizedUsername(username)];
  if (!user) throw new Error("No account was found for that username.");
  return user.securityQuestion;
}

export async function resetPassword(username: string, answer: string, newPassword: string, storage: Storage = localStorage): Promise<void> {
  if (newPassword.length < 8) throw new Error("New password must contain at least 8 characters.");
  const users = readUsers(storage);
  const key = normalizedUsername(username);
  const user = users[key];
  if (!user || await hashSecret(normalizedAnswer(answer), user.answerSalt) !== user.answerHash) {
    throw new Error("The security answer is incorrect.");
  }
  const passwordSalt = createSalt();
  users[key] = {
    ...user,
    passwordSalt,
    passwordHash: await hashSecret(newPassword, passwordSalt),
  };
  writeUsers(storage, users);
}
