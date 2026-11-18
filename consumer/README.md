# TaRaSha Discover

Consumer-facing company-discovery and financial-learning application. Live mode uses TaRaShaData.ai as its only financial-data source. Review mode remains a no-network, fictional-data preview.

After authentication, Home opens a source-dated global market overview rather than automatically loading Apple. Benchmark returns, country valuation proxies, market-cap/GDP ratios, reserves, worldwide fund flows, volatility, the dollar index, and US rates come from public no-key sources through a same-origin endpoint. See [the market overview data contract](docs/MARKET_OVERVIEW_DATA.md) for exact definitions, source periods, fallbacks, and the zero-paid-data-provider audit.

## Live data architecture

The browser calls same-origin `/api` endpoints. The local Vite middleware or Cloudflare Pages Function then calls TaRaShaData.ai server-side:

- `GET /v1/companies/search` supplies the company catalogue.
- `GET /v1/companies/{identifier}/coverage` exposes normalized annual coverage by statement and fiscal-year span.
- `GET /v1/companies/{identifier}/logo` supplies the CIK-resolved, provenance-backed company logo through the same-origin `/api/data/company-logo` proxy.
- `POST /v1/discover/company-dataset` supplies company metadata, normalized annual statements, filing links, governed Company Story contracts (including price/fundamental history), and the selected industry constituent datasets in one response.
- Discover derives growth, profitability, earnings bridges, cash-flow bridges, and industry statistics from those TaRaShaData.ai fields in transient browser memory.
- Delayed market displays, benchmarks, analyst fields, and management-commentary excerpts are acquired only inside TaRaShaData.ai through explicit zero-cost, source-linked contracts.

There is no legacy product database connection, direct SEC browser proxy, or alternate live financial-data fallback.

## Included

- Responsive landing page and navigation
- Searchable TaRaShaData.ai company catalogue
- On-demand financial retrieval for a selected range of up to seven years for US issuers; all verified available annual history for Indian NSE/BSE issuers
- Fifty-company transient research shelf
- Normalized income-statement, balance-sheet, cash-flow, and share-fact tables
- Growth, profitability, earnings, working-capital, FCFF, and FCFE analysis
- Company Story chapters 01–07 for US issuers and 01–04 for India, including source-traceable revenue, costs, cash conversion, and balance sheet; stock risk, valuation, and historical price-versus-fundamentals views retain their US scope
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

The **Data Reconciliation** tab is available only after signing in as `Admin`.
For local acceptance testing, the approved initial password is `Admin@123`.
Configure `CONSUMER_ADMIN_PASSWORD` as a server-side secret, set
`TARASHA_DATA_ADMIN_KEY` to the TaRaShaData `ADMIN_API_KEY`, and use a stable
`CONSUMER_AUTH_SECRET` of at least 32 characters. The browser receives a signed,
time-limited Admin session; it never receives the upstream admin key.

For local development, Discover automatically reads `ADMIN_API_KEY` from the
sibling `TaRaShaData.ai/.env` when `TARASHA_DATA_ADMIN_KEY` is not present in
Discover's `.env.local`. If the repositories are stored elsewhere, set the
server-only `TARASHA_DATA_ENV_PATH` to the upstream `.env` file. Production does
not use filesystem discovery and still requires `TARASHA_DATA_ADMIN_KEY`.

From the Admin tab, **Run Batch** requires a `.xlsx` or `.csv` file containing a
`ticker` column and an optional `exchange` column. **Schedule** requires its own
ticker file and supports daily or weekly execution in the selected timezone.
Every result can be opened to inspect the SEC source, TaRaShaData target,
calculation evidence, and developer investigation note. Company quality is shown
separately for the Income Statement, Balance Sheet, and Cash Flow Statement; each
score opens its own control-level drill-down. The dashboard retains and searches
the five most recent batch runs by batch number, ticker, or company name.

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
5. Configure `TARASHA_DATA_ADMIN_KEY`, `CONSUMER_ADMIN_PASSWORD`,
   `CONSUMER_AUTH_SECRET`, and `ADMIN_SYNC_KEY` as encrypted secrets.
6. Set the preview build variable `VITE_DATA_MODE=live`.
7. Keep the founding-user deployment behind the existing Cloudflare Access policy.

The Pages Function applies `no-store`; financial responses are not copied into D1, KV, or Cache. D1 is used only for Consumer account data and historical, unused catalogue/session tables retained for migration compatibility.

## Data boundary

- **Directory and facts:** TaRaShaData.ai versioned APIs.
- **Upstream:** SEC XBRL for US issuers, and issuer-filed NSE/BSE financial XBRL plus verified primary financial PDFs and notes for India, processed by TaRaShaData.ai.
- **Industry context:** TaRaShaData.ai universe categories and ingested peer relationships.
- **Persistence:** financial responses remain in browser session memory; Discover stores no financial copy.
- **Coverage:** USA and Indian NSE/BSE companies with ingested, standardized annual TaRaShaData.ai observations are searchable. India is not restricted to the initial anchor companies; Company Stories 01–04 use the same shared layouts and explicitly mark unsupported calculations unavailable. Exchange identity, filing access and verified disclosures determine actual coverage.
- **Traceability:** filing, market-history, benchmark, and management-source links plus TaRaShaData.ai derivations remain available in the response contract.
- **Company identity:** logos are fetched from TaRaShaData.ai's stored identity-asset endpoint; Discover never hotlinks an external logo provider.

For Indian company datasets, Discover requests annual history from 1900 and
retains each verified period's reporting basis and currency. TTM revenue and its
breakdown appear only when supported; a missing TTM breakdown can use the latest
verified annual breakdown with its annual label. Older periods without proven
comparability remain unavailable. The primary company is not subject to the US
seven-year request limit; selected peer datasets retain seven years and the
filing-summary list is capped at 100. Direct TaRaShaData financial requests default
to `start_year=2005`; use `start_year=1900` for all available Indian annual
history. Their public `limit` is 1–30 (default 30), but it does not truncate Indian
annual columns.

## Product boundary

The application provides filing-derived facts and educational explanations. It deliberately excludes scores, ranked lists, intrinsic values, price targets, upside/downside, buy/sell/hold language, portfolio suitability, and ungoverned live prices.

### US Company Story 02: Where the money goes

The US story uses the schema-version-2 `operating_cost_structure` returned by
TaRaShaData's existing `/v1/discover/company-dataset` API. Page 1 shows the median
economics per $100 of revenue. A newer interim TTM uses six observations: TTM plus
five consecutive fiscal years, skipping the latest full FY. An annual-equivalent
TTM is deduplicated, so the annual set uses latest FY plus four predecessors.
Page 2 shows seven consecutive annual periods in actual amounts; Page 3 shows
operating income, interest expense, and disclosed interest/investment income with
the upstream percentages. Missing expense is displayed as zero by owner policy,
while its canonical value remains missing and source status stays `not_reported`.

The adapter only changes display units and naming. It retains source references;
all fiscal selection, canonical amounts, medians, and interest percentages are
calculated upstream from CIK-specific SEC Company Facts JSON. Discover performs
no financial acquisition or independent ingestion for this feature. India keeps
its existing story. The upstream `docs/US_MONEY_GOES.md` documents ingestion,
canonical mappings, provenance, and the standalone US story API.
