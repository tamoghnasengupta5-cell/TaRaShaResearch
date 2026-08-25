# TaRaShaDiscover

TaRaShaDiscover is the consumer-facing company-discovery and financial-learning
application in [`consumer/`](consumer/README.md).

## Data boundary

- TaRaShaData.ai is the only live company, filing, financial-statement, coverage,
  industry, and peer-data provider.
- Browser requests use same-origin `/api` routes implemented by the local Vite
  middleware or Cloudflare Pages Functions.
- Server-side adapters call only versioned TaRaShaData.ai `/v1` endpoints.
- Financial responses remain in browser session memory; the Consumer account
  database stores authentication data only.
- Preview mode uses fictional bundled examples and makes no live data request.

## Work locally

```bash
cd consumer
cp .env.example .env.local
# Set VITE_DATA_MODE=live and TARASHA_DATA_API_URL.
npm install
npm run dev
```

Run the complete verification suite with:

```bash
cd consumer
npm run check
```

Deployment and provider configuration are documented in
[`consumer/CLOUDFLARE_DEPLOYMENT_RUNBOOK.md`](consumer/CLOUDFLARE_DEPLOYMENT_RUNBOOK.md).
