# TaRaSha Company Lens

Consumer-facing founding-user application. It is intentionally separate from the Streamlit research interface while reading an approved, read-only contract from the same PostgreSQL database. Review mode uses fictional data; live private-preview mode retrieves bulk-uploaded Research facts into browser memory without storing a Consumer copy.

The zero-cost Cloudflare live preview has now been provisioned and its US SEC catalogue populated. See `CLOUDFLARE_DEPLOYMENT_RUNBOOK.md` for the actual resource names, deployment URL, catalogue-refresh command and verification procedure.

## Included

- Responsive landing page and navigation
- Searchable company catalogue without recommendations or rankings
- On-demand shared Research database retrieval for a user-selected range of up to seven years
- Fifty-company transient research shelf; a refresh or closed tab clears the financial facts
- Full income-statement, balance-sheet, cash-flow and share-fact tables when standard tags exist
- Provider-neutral server adapter; the legacy SEC provider remains available as a fallback
- Plain-language company pages organised around growth, profitability, cash and debt
- Side-by-side comparison for up to three companies
- Device-local watchlist
- Learning library and glossary
- Data-source/freshness treatment
- Explicit founding-user, data-source and persistence labelling
- Cloudflare Pages Function provider boundary; shared-database credentials remain server-only
- Supabase free-tier schema with invite, profile, watchlist, consent and row-level security foundations

## Run locally

Requires Node.js 22 or newer.

```bash
cd consumer
npm install
npm run dev
```

Open the local URL shown by Vite. No account, API key or external service is required in review mode. Search for an illustrative company, pull it, and confirm that cards appear only on the session research shelf.

To use the already-provisioned live US catalogue from localhost, create `.env.local` with:

```text
VITE_DATA_MODE=live
VITE_API_BASE_URL=
DEV_API_TARGET=https://agent-consumer-friendly-init.tarasha-consumer-platform.pages.dev
```

`npm run dev` will then proxy same-origin `/api` requests from port 5173 to the Cloudflare preview. `.env.local` is git-ignored. Restart Vite after changing it because these values are loaded when the development server starts.

## Validate

```bash
npm run check
```

## Zero-cost founding-user deployment

1. Create a Cloudflare Pages project connected to this repository and branch.
2. Set the root directory to `consumer`.
3. Use `npm run build` as the build command.
4. Use `dist` as the output directory.
5. Keep this consumer branch as a **preview deployment**, then enable the project's preview access policy in **Settings → General**. Allow only the founding subscribers' email addresses. Do not expose the production `*.pages.dev` deployment with live data: Cloudflare's one-click Pages Access switch protects previews, not that production hostname.
6. Create a D1 database from the Cloudflare dashboard and run `migrations/0001_catalog_and_session_claims.sql` in its console.
7. Bind that database to the Pages project's **preview environment** using the exact variable name `DB`.
8. Add encrypted preview Function secrets:
   - `ADMIN_SYNC_KEY`: a long random value used only for catalogue refreshes
   - `SEC_USER_AGENT`: `TaRaSha Company Lens your-admin-email@example.com`
   - `SHARED_RESEARCH_SERVICE_KEY`: the server-only Supabase service-role key
9. Add preview Function variables:
   - `DATA_PROVIDER=research-db`
   - `SHARED_RESEARCH_URL=https://<project-ref>.supabase.co`
10. Add the preview build variable `VITE_DATA_MODE=live` and redeploy.
11. Create a Cloudflare Access service token and a Service Auth policy for the preview application so the admin sync script can pass the login gate.

The included `_headers` file applies baseline browser security headers. This branch-preview arrangement keeps the first ten users invite-only without adding a paid authentication service or buying a domain. Pages Functions share the Workers Free allowance, while D1 Free currently includes far more reads, writes and storage than a ten-user catalogue requires.

### Admin-triggered US catalogue sync

Run this only after the live Pages project and D1 binding are ready:

```bash
cd consumer
API_BASE_URL="https://your-project.pages.dev" \
ADMIN_SYNC_KEY="the-same-encrypted-admin-secret" \
SEC_USER_AGENT="TaRaSha Company Lens your-admin-email@example.com" \
CF_ACCESS_CLIENT_ID="your-service-token-id" \
CF_ACCESS_CLIENT_SECRET="your-service-token-secret" \
npm run sync:us-catalog
```

The Cloudflare Access values are required when the preview is protected; keep them in your shell or password manager, never in a browser environment file. The script downloads the SEC's ticker/exchange association file and sends it to D1 in batches of 100. The SEC expressly says this directory is periodically updated but does not guarantee accuracy or scope, so the UI calls it the SEC catalogue rather than claiming perfect coverage.

## Optional future in-app identity preparation

The existing Consumer-auth schema is optional and separate from the shared Research tables. It is only a foundation for invite-only accounts and watchlists:

1. Create a new Supabase project dedicated to Company Lens.
2. Run `supabase/001_initial_schema.sql` in its SQL editor.
3. Insert up to ten lowercase email addresses into `beta_invites` from the protected SQL editor.
4. Add the public project URL and anonymous key to `.env.local` using `.env.example`.
5. Add the authentication screens before moving away from the Access-protected preview; the schema already rejects uninvited users and an eleventh profile.

Never expose the shared Research service-role key in the browser. Only the Pages Function may use it, and it may query only the restricted `consumer_*` views defined by the shared Research migrations.

## Data-source boundary

- **Private-preview directory:** companies already present in TaRaSha Research, for USA and India.
- **Financial facts:** approved annual fields retrieved from the shared Research PostgreSQL views.
- **Upstream source:** spreadsheets downloaded through StockAnalysis.com and bulk-uploaded by the Research administrator. StockAnalysis.com may source downloads from third-party providers.
- **Persistence:** Consumer keeps the response only in browser session memory and stores no separate financial copy.
- **Commercial boundary:** private non-commercial evaluation only. Obtain explicit source-provider redistribution permission or switch to a licensed API before paid distribution.
- **Fallback:** the prior SEC EDGAR provider remains in the backend and can be restored with `DATA_PROVIDER=sec`.

See `ZERO_COST_DATA_STRATEGY.md` for feasibility details and source links.

## Product boundary

The application provides filing facts and educational explanations. It deliberately excludes scores, ranked lists, intrinsic values, price targets, upside/downside, buy/sell/hold language, portfolio suitability and live prices.
