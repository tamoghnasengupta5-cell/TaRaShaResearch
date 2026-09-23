import { existsSync } from "node:fs";
import { resolve } from "node:path";
import { DatabaseSync } from "node:sqlite";

const databasePath = String(process.env.CONSUMER_AUTH_DB_PATH || "").trim()
  || resolve(process.cwd(), ".local-data", "tarasha-consumer-auth.db");

if (!existsSync(databasePath)) {
  console.log("No local account database exists yet. Unique users: 0");
  process.exit(0);
}

const database = new DatabaseSync(databasePath, { readOnly: true });
try {
  const row = database.prepare(`
    select count(*) as unique_users,
           sum(case when status = 'active' then 1 else 0 end) as active_users,
           min(created_at) as first_signup_at,
           max(created_at) as latest_signup_at
    from consumer_users
  `).get();
  const formatTimestamp = (value) => value === null || value === undefined
    ? "none"
    : new Date(Number(value) * 1000).toISOString();
  console.log(`Unique users: ${Number(row.unique_users || 0)}`);
  console.log(`Active users: ${Number(row.active_users || 0)}`);
  console.log(`First signup: ${formatTimestamp(row.first_signup_at)}`);
  console.log(`Latest signup: ${formatTimestamp(row.latest_signup_at)}`);
} finally {
  database.close();
}
