# TaRaShaData.ai provider strategy

## Decision

TaRaShaData.ai is the only live financial-data provider for TaRaSha Discover. The integration is server-to-server through the local Vite adapter or Cloudflare Pages Function; the browser never receives an API credential.

## API contracts

| Discover need | TaRaShaData.ai API | Discover responsibility |
|---|---|---|
| Company search | `GET /v1/companies/search` | Map CIK-backed results into catalogue cards |
| Company and peer dataset | `POST /v1/discover/company-dataset` | Build transient UI models and calculated analyses |
| Company operating cost story | `operating_cost_structure` in the batched company dataset | Render TaRaShaData's reported expense structure, $100 waterfall, and five-year intensity table without category assumptions |
| Company cash-conversion story | `cash_conversion` in the batched company dataset | Render TaRaShaData's annual EBIT-to-FCFF bridge, signed working-capital components, six-year trend, efficiency metrics, methodology, and filing lineage without estimating a missing input |
| Company metadata | Included in the batched dataset from `/v1/companies/{identifier}` semantics | Display issuer, ticker, currency, freshness, and sector context |
| Filing traceability | Included in the batched dataset from `/v1/companies/{identifier}/filings` semantics | Link users to source filings |
| Industry constituents | Included in the batched dataset from `/v1/companies/{identifier}/peers` semantics | Recalculate bucket statistics for the selected basket |

## Runtime boundary

- Pages Functions set `cache-control: private, no-store`.
- Discover stores no financial facts in D1, KV, browser local storage, or application caches.
- D1 remains the account store only.
- TaRaShaData.ai owns source ingestion, normalization, units, filing provenance, and data-quality controls.
- Discover owns presentation calculations such as growth rates, margins, medians, standard deviations, FCFF, and FCFE.
- The "Where the money goes" chapter consumes only the deterministic TaRaShaData story payload. It does not call the SEC or another financial-data provider from the browser or Discover worker.
- The "What becomes cash" chapter consumes only the deterministic TaRaShaData cash-conversion payload. All displayed values and narrative amounts are derived inside TaRaShaData from normalized annual filing facts; Discover only maps and renders that contract.

## Coverage and absence handling

Discover does not fall back to a legacy database, direct SEC browser calls, or a third-party market-data feed. A missing TaRaShaData.ai field is displayed as unavailable. Enterprise Value and P/E remain unavailable until a governed TaRaShaData.ai endpoint exists.

## Required configuration

- Local: `TARASHA_DATA_API_URL=http://127.0.0.1:8000`
- Cloudflare preview: `TARASHA_DATA_API_URL=<deployed TaRaShaData.ai base URL>`
- Optional protected API: `TARASHA_DATA_API_KEY` as a server-only encrypted secret
- Browser build: `VITE_DATA_MODE=live`
