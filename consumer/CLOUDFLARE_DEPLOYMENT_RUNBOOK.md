# TaRaSha Consumer Cloudflare runbook

## Current live-preview resources

Created on 2026-07-11 in the authenticated Cloudflare account:

| Resource | Value |
|---|---|
| Pages project | `tarasha-consumer-platform` |
| Production branch marker | `Local_Test` |
| Consumer preview branch | `agent/consumer-friendly-initial` |
| Stable preview URL | <https://agent-consumer-friendly-init.tarasha-consumer-platform.pages.dev> |
| D1 database | `tarasha-consumer-catalog` |
| D1 database ID | `1f4f4171-938f-4f9d-adbe-6ac8f940df67` |
| D1 region | Eastern North America (`ENAM`) |
| D1 binding | `DB` |

The production branch marker intentionally differs from the consumer branch so direct uploads of the consumer branch remain preview deployments.

## One-time setup performed

1. Authenticated Wrangler through Cloudflare OAuth.
2. Created the Pages project and D1 database on the free plan.
3. Added `wrangler.jsonc` with the Pages project and D1 binding.
4. Applied `migrations/0001_catalog_and_session_claims.sql` to the remote database.
5. Generated a 64-character random `ADMIN_SYNC_KEY`.
6. Stored that admin key in macOS Keychain under service `TaRaSha Consumer Admin Sync Key` and account `tarasha-consumer-platform`.
7. Stored the SEC identifier in macOS Keychain under service `TaRaSha Consumer SEC User Agent`, then configured encrypted `ADMIN_SYNC_KEY` and `SEC_USER_AGENT` values for the Pages project.
8. Configured the same secrets and the `DB` binding for preview Functions.
9. Built the frontend with `VITE_DATA_MODE=live` and deployed the consumer branch preview.
10. Ran the SEC catalogue sync. It inserted 9,304 ticker/exchange associations.
11. Verified `MSFT`, `ANET`, the session-claim endpoint and Microsoft's live SEC Company Facts response.

No secret values are committed to Git.

## Use the live catalogue on localhost

The local Vite server does not run Pages Functions or D1. The checked-in Vite configuration can proxy local `/api` requests to the deployed preview. Create the git-ignored `.env.local` file:

```text
VITE_DATA_MODE=live
VITE_API_BASE_URL=
DEV_API_TARGET=https://agent-consumer-friendly-init.tarasha-consumer-platform.pages.dev
```

Then restart `npm run dev` and open <http://localhost:5173/#/discover>. Leaving `VITE_API_BASE_URL` empty is intentional: browser requests remain same-origin and Vite performs the development-only proxying.

## Deploy a future consumer update

From the consumer worktree:

```bash
cd /Users/tamoghna/Documents/TaRaShaConsumer/consumer
VITE_DATA_MODE=live npm run check
npx wrangler pages deploy dist \
  --project-name tarasha-consumer-platform \
  --branch agent/consumer-friendly-initial \
  --commit-hash "$(git rev-parse HEAD)" \
  --commit-message "Deploy consumer preview"
```

Keep `VITE_DATA_MODE=live` on the build command. Vite substitutes this variable while building, not after deployment.

## Refresh the US company catalogue

The SEC directory changes over time. Refresh it manually when needed; monthly is adequate for the founding-user phase.

```bash
cd /Users/tamoghna/Documents/TaRaShaConsumer/consumer
ADMIN_SYNC_KEY="$(security find-generic-password \
  -a 'tarasha-consumer-platform' \
  -s 'TaRaSha Consumer Admin Sync Key' -w)" \
API_BASE_URL="https://agent-consumer-friendly-init.tarasha-consumer-platform.pages.dev" \
SEC_USER_AGENT="$(security find-generic-password \
  -a 'tarasha-consumer-platform' \
  -s 'TaRaSha Consumer SEC User Agent' -w)" \
npm run sync:us-catalog
```

The command reads the admin secret from Keychain and does not print it.

## Verify the deployed catalogue

```bash
BASE="https://agent-consumer-friendly-init.tarasha-consumer-platform.pages.dev"
curl -fsS "$BASE/api/health"
curl -fsS "$BASE/api/companies?query=Microsoft&country=USA"
curl -fsS "$BASE/api/companies?query=Arista&country=USA"
npx wrangler d1 execute tarasha-consumer-catalog --remote \
  --command "select count(*) as company_count from company_catalog"
```

Expected tickers are `MSFT` and `ANET`; the initial catalogue count is 9,304.

## Secret rotation

To rotate the admin key, generate a new value, replace the Keychain entry, and upload the same value to the Pages project before the next catalogue sync. Never place it in `.env.local`, a `VITE_*` variable, a shell-history command, or Git.

## Access-control note

The preview URL is currently an unadvertised technical preview. Before inviting subscribers, enable Cloudflare Pages preview access control and allow only subscriber email addresses, or finish the prepared in-app invite authentication. Cloudflare's Pages preview-access switch and any Access service token must be configured before treating this as an invite-only paid beta.
