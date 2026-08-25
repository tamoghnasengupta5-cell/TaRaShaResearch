# TaRaSha Discover Cloudflare preview runbook

## Build configuration

| Setting | Value |
|---|---|
| Root directory | `consumer` |
| Build command | `npm run build` |
| Output directory | `dist` |
| Live data provider | `TaRaShaData.ai API` |
| Financial persistence | Browser session only |

## Preview environment

Configure these values in the Cloudflare Pages preview environment:

- `VITE_DATA_MODE=live` as a build variable.
- `TARASHA_DATA_API_URL` as the deployed TaRaShaData.ai base URL.
- `TARASHA_DATA_API_KEY` as an encrypted secret only if the Data API requires a read credential.
- `CONSUMER_AUTH_SECRET` as an encrypted secret of at least 32 characters.
- `ADMIN_SYNC_KEY` as an encrypted secret for the aggregate user-summary endpoint.
- D1 binding `DB` for Consumer accounts.

Keep the preview deployment behind Cloudflare Access for the founding-user cohort.

## Deploy validation

1. Run `npm run check` locally.
2. Deploy the preview branch.
3. Confirm `/api/health` returns `provider: "tarasha-data"` and `configured: true`.
4. Search for an ingested USA ticker.
5. Pull at least three fiscal years and verify income, balance, cash flow, growth, profitability, and filing links.
6. Change the industry constituent basket and confirm all bucket medians recalculate.
7. Confirm India search returns no live results until TaRaShaData.ai publishes India coverage.
8. Confirm Enterprise Value is shown as unavailable rather than populated from another source.
9. Confirm account registration/login and the admin aggregate user summary still work.

## Security checks

- No `TARASHA_DATA_API_KEY` appears in browser bundles or `VITE_*` variables.
- Financial responses include `private, no-store`.
- D1 contains account data only; the historical catalogue/session tables are not queried by the active provider.
- The Pages Function exposes only the normalized Discover contract, account endpoints, health, and aggregate admin summary.

## Rollback

Rollback means redeploying the prior application build. Do not reactivate a second financial-data provider inside the current build; the current contract intentionally has no live fallback outside TaRaShaData.ai.
