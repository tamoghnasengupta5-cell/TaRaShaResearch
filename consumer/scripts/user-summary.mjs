const apiBase = String(process.env.API_BASE_URL || "").replace(/\/$/, "");
const adminKey = String(process.env.ADMIN_SYNC_KEY || "");
const accessClientId = String(process.env.CF_ACCESS_CLIENT_ID || "");
const accessClientSecret = String(process.env.CF_ACCESS_CLIENT_SECRET || "");

if (!apiBase || !adminKey) {
  console.error("Set API_BASE_URL and ADMIN_SYNC_KEY. Add the Cloudflare Access service-token values when the deployment is protected.");
  process.exit(1);
}

const headers = { "x-admin-key": adminKey, accept: "application/json" };
if (accessClientId && accessClientSecret) {
  headers["CF-Access-Client-Id"] = accessClientId;
  headers["CF-Access-Client-Secret"] = accessClientSecret;
}

const response = await fetch(`${apiBase}/api/admin/users/summary`, { headers });
const payload = await response.json().catch(() => ({}));
if (!response.ok) throw new Error(payload.error || `User summary failed with status ${response.status}.`);

const formatTimestamp = (value) => value === null || value === undefined
  ? "none"
  : new Date(Number(value) * 1000).toISOString();
console.log(`Unique users: ${Number(payload.uniqueUsers || 0)}`);
console.log(`Active users: ${Number(payload.activeUsers || 0)}`);
console.log(`First signup: ${formatTimestamp(payload.firstSignupAt)}`);
console.log(`Latest signup: ${formatTimestamp(payload.latestSignupAt)}`);
