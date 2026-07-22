const SEC_DIRECTORY = "https://www.sec.gov/files/company_tickers_exchange.json";
const apiBase = String(process.env.API_BASE_URL || "").replace(/\/$/, "");
const adminKey = process.env.ADMIN_SYNC_KEY;
const contact = process.env.SEC_USER_AGENT;
const accessClientId = process.env.CF_ACCESS_CLIENT_ID;
const accessClientSecret = process.env.CF_ACCESS_CLIENT_SECRET;

if (!apiBase || !adminKey || !contact || !contact.includes("@")) {
  console.error("Set API_BASE_URL, ADMIN_SYNC_KEY, and SEC_USER_AGENT (application name plus administrator email).");
  process.exit(1);
}

const response = await fetch(SEC_DIRECTORY, { headers: { "User-Agent": contact, Accept: "application/json" } });
if (!response.ok) throw new Error(`SEC company directory returned ${response.status}`);
const payload = await response.json();
const fields = payload.fields || [];
const index = Object.fromEntries(fields.map((field, position) => [field, position]));
const rows = (payload.data || []).map((row) => ({
  cik: row[index.cik],
  name: row[index.name],
  ticker: row[index.ticker],
  exchange: row[index.exchange] || "US",
})).filter((row) => row.cik && row.name && row.ticker);

let accepted = 0;
const batchSize = 100;
for (let offset = 0; offset < rows.length; offset += batchSize) {
  const batch = rows.slice(offset, offset + batchSize);
  const headers = { "content-type": "application/json", "x-admin-key": adminKey };
  if (accessClientId && accessClientSecret) {
    headers["CF-Access-Client-Id"] = accessClientId;
    headers["CF-Access-Client-Secret"] = accessClientSecret;
  }
  const result = await fetch(`${apiBase}/api/admin/catalog/batch`, {
    method: "POST",
    headers,
    body: JSON.stringify({ rows: batch }),
  });
  if (!result.ok) throw new Error(`Catalogue batch ${offset / batchSize + 1} failed: ${result.status} ${await result.text()}`);
  accepted += batch.length;
  console.log(`Synced ${accepted} of ${rows.length}`);
}

console.log(`US catalogue sync complete: ${accepted} SEC ticker/exchange associations.`);
