import { readFile, readdir } from "node:fs/promises";
import { dirname, extname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const ignoredDirectories = new Set([
  ".git",
  ".local-data",
  ".wrangler",
  "coverage",
  "dist",
  "node_modules",
]);
const textExtensions = new Set([
  "",
  ".css",
  ".html",
  ".js",
  ".json",
  ".jsonc",
  ".md",
  ".mjs",
  ".sql",
  ".ts",
  ".tsx",
  ".txt",
  ".yaml",
  ".yml",
]);
const forbidden = [
  ["TaRaSha", "Research"].join(""),
  ["tarasha", "research"].join("-"),
  ["research", "db"].join("-"),
  ["TARASHA", "DB", "URL"].join("_"),
  ["TaRaSha Shared", "Database"].join(" "),
  ["CONSUMER", "API", "ORIGIN"].join("_"),
  ["agent-consumer-friendly-init", "tarasha-consumer-platform", "pages.dev"].join("."),
];

async function sourceFiles(directory) {
  const files = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    if (ignoredDirectories.has(entry.name)) continue;
    const path = resolve(directory, entry.name);
    if (entry.isDirectory()) files.push(...await sourceFiles(path));
    else if (entry.isFile() && textExtensions.has(extname(entry.name))) files.push(path);
  }
  return files;
}

const violations = [];
for (const path of await sourceFiles(repositoryRoot)) {
  const contents = await readFile(path, "utf8");
  for (const token of forbidden) {
    if (contents.toLowerCase().includes(token.toLowerCase())) {
      violations.push(`${path.slice(repositoryRoot.length + 1)} contains ${JSON.stringify(token)}`);
    }
  }
}

if (violations.length) {
  console.error("Legacy data-source boundary audit failed:\n" + violations.join("\n"));
  process.exitCode = 1;
} else {
  console.log("Data-source boundary audit passed: Discover source contains no legacy provider dependency.");
}
