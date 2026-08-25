# TaRaSha Discover

Consumer-facing company-discovery and financial-learning application. Live mode uses TaRaShaData.ai as its only financial-data source. Review mode remains a no-network, fictional-data preview.

After authentication, Home opens a source-dated global market overview rather than automatically loading Apple. Benchmark returns, country valuation proxies, market-cap/GDP ratios, reserves, worldwide fund flows, volatility, the dollar index, and US rates come from public no-key sources through a same-origin endpoint. See [the market overview data contract](docs/MARKET_OVERVIEW_DATA.md) for exact definitions, source periods, fallbacks, and the zero-paid-data-provider audit.

## Live data architecture

The browser calls same-origin `/api` endpoints. The local Vite middleware or Cloudflare Pages Function then calls TaRaShaData.ai server-side:

- `GET /v1/companies/search` supplies the company catalogue.
- `GET /v1/companies/{identifier}/coverage` exposes normalized annual coverage by statement and fiscal-year span.
- `GET /v1/companies/{identifier}/logo` supplies the CIK-resolved, provenance-backed company logo through the same-origin `/api/data/company-logo` proxy.
- `POST /v1/discover/company-dataset` supplies company metadata, normalized annual income/balance/cash-flow statements, filing links, governed revenue/cost/cash-conversion story contracts, and the selected industry constituent datasets in one response.
- Discover derives growth, profitability, earnings bridges, cash-flow bridges, and industry statistics from those TaRaShaData.ai fields in transient browser memory.
- Enterprise Value and trailing P/E remain visibly unavailable until TaRaShaData.ai publishes a governed company-level market-data contract.

There is no legacy product database connection, direct SEC browser proxy, or alternate live financial-data fallback.

## Included

- Responsive landing page and navigation
- Searchable TaRaShaData.ai company catalogue
- On-demand financial retrieval for a selected range of up to seven years
- Fifty-company transient research shelf
- Normalized income-statement, balance-sheet, cash-flow, and share-fact tables
- Growth, profitability, earnings, working-capital, FCFF, and FCFE analysis
- Company Story chapters for revenue mix, operating cost structure, and source-traceable operating-profit-to-FCFF conversion
- Editable industry constituent basket backed by TaRaShaData.ai peer metadata
- Side-by-side comparison for up to three companies
- Device-local watchlist
- Learning library and glossary
- Source/freshness treatment and filing links
- Company-specific logos supplied exclusively by TaRaShaData.ai, with a local monogram fallback when no governed asset is available
- D1-backed Consumer accounts with encrypted identifiers and one-way credential hashes

## Run locally

Start TaRaShaData.ai first:

```bash
cd /Users/tamoghna/Documents/TaRaShaData.ai
source .venv/bin/activate
tarasha-data serve --reload
```

`--reload` keeps the local API contract synchronized with backend edits during
development. If the backend was started without it, restart the service after
pulling or applying TaRaShaData.ai code changes.

Then start Discover:

```bash
cd /Users/tamoghna/Documents/TaRaShaConsumer/consumer
cp .env.example .env.local
# Set VITE_DATA_MODE=live in .env.local.
npm install
npm run dev
```

The default local API URL is `http://127.0.0.1:8000`. If that port is already
occupied, start the current TaRaShaData.ai build on another port and set the
server-only `TARASHA_DATA_API_URL` in `.env.local`. If a deployment requires a
read credential, set `TARASHA_DATA_API_KEY`; never expose it through a `VITE_*`
variable.

`DEV_API_TARGET` remains an optional way to proxy all `/api` requests to an already deployed Discover Pages environment. When it is set, the local TaRaShaData.ai middleware is disabled.

Local registrations are stored in `.local-data/tarasha-consumer-auth.db`. On macOS, the local field-encryption secret is created in Keychain automatically. On other systems, set `CONSUMER_AUTH_SECRET` to a value of at least 32 characters.

## Validate

```bash
npm run check
```

For the production Node static server, set `TARASHA_DISCOVER_API_ORIGIN` to the
deployed Discover Pages origin. Startup verifies that `/api/health` reports a
configured `tarasha-data` provider and refuses to serve if that check fails.

## Cloudflare Pages preview

1. Set the project root to `consumer`, build command to `npm run build`, and output directory to `dist`.
2. Bind the existing D1 account database as `DB` in the preview environment.
3. Configure `TARASHA_DATA_API_URL` as a preview environment variable.
4. If needed, configure `TARASHA_DATA_API_KEY` as an encrypted secret.
5. Configure `CONSUMER_AUTH_SECRET` and `ADMIN_SYNC_KEY` as encrypted secrets.
6. Set the preview build variable `VITE_DATA_MODE=live`.
7. Keep the founding-user deployment behind the existing Cloudflare Access policy.

The Pages Function applies `no-store`; financial responses are not copied into D1, KV, or Cache. D1 is used only for Consumer account data and historical, unused catalogue/session tables retained for migration compatibility.

## Data boundary

- **Directory and facts:** TaRaShaData.ai versioned APIs.
- **Upstream:** issuer filings and SEC XBRL processed by TaRaShaData.ai.
- **Industry context:** TaRaShaData.ai universe categories and ingested peer relationships.
- **Persistence:** financial responses remain in browser session memory; Discover stores no financial copy.
- **Coverage:** currently USA companies with ingested, standardized TaRaShaData.ai observations. India searches return no live matches until TaRaShaData.ai adds that coverage.
- **Traceability:** filing links and TaRaShaData.ai provenance remain available in the response contract.
- **Company identity:** logos are fetched from TaRaShaData.ai's stored identity-asset endpoint; Discover never hotlinks an external logo provider.

## Product boundary

The application provides filing-derived facts and educational explanations. It deliberately excludes scores, ranked lists, intrinsic values, price targets, upside/downside, buy/sell/hold language, portfolio suitability, and ungoverned live prices.
