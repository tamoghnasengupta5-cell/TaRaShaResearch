# TaRaShaResearch and TaRaShaDiscover

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


## Research application

The Research application lives at the repository root and starts with:

```bash
python -m pip install -r requirements.txt
python -m streamlit run app.py
```

Research setup and database configuration are documented in
[RESEARCH_README.md](RESEARCH_README.md). Its regression suite runs with:

```bash
python -m unittest discover -s tests -v
```

Local financial databases are not published with source code. Database changes
and migrations require separate review and authorization before production use.

## Publication and production release

Changes are committed and pushed to `main` only on the owner's explicit request.
They remain on GitHub until the owner separately approves an Azure release.
Research and Discover deployment workflows are manual-only, run from `main`,
and require the full approved commit SHA. Verification checks can run on pushes
without deploying. TaRaShaData.ai is maintained in its own repository and
follows the same separate publication and production approval policy.

See [AGENTS.md](AGENTS.md) for the owner release policy.
