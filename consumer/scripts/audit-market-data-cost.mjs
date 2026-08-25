import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

const providerPath = resolve("functions/marketOverviewProvider.ts");
const source = await readFile(providerPath, "utf8");
const allowedHosts = new Set([
  "api.worldbank.org",
  "data.worldbank.org",
  "finance.yahoo.com",
  "fred.stlouisfed.org",
  "query2.finance.yahoo.com",
  "www.ici.org",
  "www.ishares.com",
]);
const hosts = [...source.matchAll(/https:\/\/([a-z0-9.-]+)/gi)].map((match) => match[1].toLowerCase());
const unexpectedHosts = [...new Set(hosts.filter((host) => !allowedHosts.has(host)))];
const credentialPatterns = [
  /authorization\s*:/i,
  /x-api-key/i,
  /[?&](?:api_?key|token)=/i,
  /process\.env/i,
  /\benv\.[A-Z0-9_]+/,
];
const credentialMatches = credentialPatterns.filter((pattern) => pattern.test(source)).map(String);

if (unexpectedHosts.length || credentialMatches.length) {
  if (unexpectedHosts.length) console.error(`Unexpected market-data hosts: ${unexpectedHosts.join(", ")}`);
  if (credentialMatches.length) console.error(`Credential-dependent market-data patterns: ${credentialMatches.join(", ")}`);
  process.exitCode = 1;
} else {
  console.log(`Market-data cost audit passed: ${new Set(hosts).size} allowlisted public hosts, no credential-bearing request path.`);
}
