import { createReadStream } from "node:fs";
import { stat } from "node:fs/promises";
import { createServer } from "node:http";
import { extname, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const port = Number.parseInt(process.env.PORT || "8080", 10);
const apiOrigin = String(
  process.env.CONSUMER_API_ORIGIN ||
    "https://agent-consumer-friendly-init.tarasha-consumer-platform.pages.dev",
).replace(/\/$/, "");
const distRoot = resolve(fileURLToPath(new URL("./dist/", import.meta.url)));

const mimeTypes = new Map([
  [".css", "text/css; charset=utf-8"],
  [".html", "text/html; charset=utf-8"],
  [".ico", "image/x-icon"],
  [".js", "text/javascript; charset=utf-8"],
  [".json", "application/json; charset=utf-8"],
  [".map", "application/json; charset=utf-8"],
  [".png", "image/png"],
  [".svg", "image/svg+xml"],
  [".webp", "image/webp"],
  [".woff", "font/woff"],
  [".woff2", "font/woff2"],
]);

function applySecurityHeaders(response) {
  response.setHeader("X-Content-Type-Options", "nosniff");
  response.setHeader("Referrer-Policy", "strict-origin-when-cross-origin");
  response.setHeader("X-Frame-Options", "SAMEORIGIN");
}

function sendJson(response, status, payload) {
  const body = JSON.stringify(payload);
  response.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Content-Length": Buffer.byteLength(body),
    "Cache-Control": "no-store",
  });
  response.end(body);
}

async function readRequestBody(request) {
  const chunks = [];
  for await (const chunk of request) chunks.push(chunk);
  return chunks.length ? Buffer.concat(chunks) : undefined;
}

async function proxyApi(request, response) {
  const target = new URL(request.url || "/api", apiOrigin);
  const headers = new Headers();

  for (const [name, value] of Object.entries(request.headers)) {
    if (value === undefined || ["connection", "host", "transfer-encoding"].includes(name)) continue;
    headers.set(name, Array.isArray(value) ? value.join(", ") : value);
  }
  headers.set("X-Forwarded-Host", request.headers.host || "");
  headers.set("X-Forwarded-Proto", "https");

  const method = request.method || "GET";
  const body = method === "GET" || method === "HEAD" ? undefined : await readRequestBody(request);
  const upstream = await fetch(target, {
    method,
    headers,
    body,
    redirect: "manual",
  });
  const responseBody = Buffer.from(await upstream.arrayBuffer());

  for (const [name, value] of upstream.headers) {
    if (["connection", "content-encoding", "content-length", "transfer-encoding"].includes(name)) continue;
    response.setHeader(name, value);
  }
  response.setHeader("Content-Length", responseBody.length);
  response.statusCode = upstream.status;
  response.end(method === "HEAD" ? undefined : responseBody);
}

async function serveApplication(request, response) {
  const requestUrl = new URL(request.url || "/", "http://localhost");
  let pathname;

  try {
    pathname = decodeURIComponent(requestUrl.pathname);
  } catch {
    sendJson(response, 400, { error: "Invalid request path." });
    return;
  }

  const relativePath = pathname === "/" ? "index.html" : pathname.replace(/^\/+/, "");
  let filePath = resolve(distRoot, relativePath);
  if (filePath !== distRoot && !filePath.startsWith(`${distRoot}${sep}`)) {
    sendJson(response, 400, { error: "Invalid request path." });
    return;
  }

  let fileStat;
  try {
    fileStat = await stat(filePath);
    if (!fileStat.isFile()) throw new Error("Not a file");
  } catch {
    filePath = resolve(distRoot, "index.html");
    fileStat = await stat(filePath);
  }

  const contentType = mimeTypes.get(extname(filePath).toLowerCase()) || "application/octet-stream";
  const cacheControl = filePath.includes(`${sep}assets${sep}`)
    ? "public, max-age=31536000, immutable"
    : "no-cache";
  response.writeHead(200, {
    "Content-Type": contentType,
    "Content-Length": fileStat.size,
    "Cache-Control": cacheControl,
  });
  if (request.method === "HEAD") response.end();
  else createReadStream(filePath).pipe(response);
}

const server = createServer(async (request, response) => {
  applySecurityHeaders(response);

  try {
    const pathname = new URL(request.url || "/", "http://localhost").pathname;
    if (pathname === "/healthz") {
      sendJson(response, 200, { status: "ok" });
    } else if (pathname === "/api" || pathname.startsWith("/api/")) {
      await proxyApi(request, response);
    } else if (["GET", "HEAD"].includes(request.method || "GET")) {
      await serveApplication(request, response);
    } else {
      sendJson(response, 405, { error: "Method not allowed." });
    }
  } catch (error) {
    console.error("Request failed", error);
    if (!response.headersSent) sendJson(response, 502, { error: "Upstream service unavailable." });
    else response.end();
  }
});

server.listen(port, "0.0.0.0", () => {
  console.log(`TaRaSha Discover listening on port ${port}`);
});
