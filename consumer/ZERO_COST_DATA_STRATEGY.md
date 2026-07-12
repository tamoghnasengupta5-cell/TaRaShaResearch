# Zero-cost transient filing strategy

## Decision

The idea is partially feasible at zero infrastructure cost.

| Requested capability | Decision | Reason |
|---|---|---|
| Persist company names and tickers | Implemented for the SEC catalogue | SEC provides a ticker, CIK and exchange association file, but does not guarantee perfect scope or accuracy. |
| Store no company financial data | Implemented | D1 stores catalogue rows and short-lived session claims only. |
| Pull US 10-K and 10-Q facts on demand | Implemented | SEC Company Facts and Submissions APIs are official, public APIs. |
| Keep extracted facts transient | Implemented | The Function streams JSON; normalization and storage occur only in browser memory. |
| Earnings-call transcripts | Not feasible from SEC | Transcripts are not a standard EDGAR dataset. Earnings-related 8-K filings are linked instead. |
| Complete shareholder ownership from 10-K | Not feasible | A 10-K may expose shares outstanding, but beneficial/institutional ownership requires other filings and does not form one complete ownership table. |
| India structured statements at zero cost | Not enabled | NSE exposes filings and XBRL mechanisms, but no equivalent free, supported public API and no assumed commercial redistribution permission. |

## Official sources

- SEC company ticker/exchange directory and archive access: <https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data>
- SEC Company Facts and Submissions APIs: <https://www.sec.gov/search-filings/edgar-application-programming-interfaces>
- SEC programmatic access requirements: <https://www.sec.gov/about/webmaster-frequently-asked-questions>
- NSE XBRL filing information: <https://www.nseindia.com/static/companies-listing/xbrl-information>
- NSE data usage and redistribution policy: <https://www.nseindia.com/static/market-data/nse-data-policy>
- Cloudflare Pages Function free-plan accounting: <https://developers.cloudflare.com/pages/functions/pricing/>
- Cloudflare D1 free-plan limits: <https://developers.cloudflare.com/d1/platform/pricing/>
- Cloudflare Pages preview access control: <https://developers.cloudflare.com/pages/configuration/preview-deployments/>
- Cloudflare Access service tokens: <https://developers.cloudflare.com/cloudflare-one/access-controls/service-credentials/service-tokens/>

## Limits

- Maximum three distinct companies per browser session.
- Maximum five reporting years per company pull.
- A session claim expires server-side after 12 hours.
- Refreshing or closing the application clears all extracted financial facts because they are React memory only.
- Search results are compact catalogue rows. A full company card is created only after a successful research pull.

Three companies is a deliberate founding-user limit. Each live company requires one SEC Company Facts response and one Submissions response; large filers can return several megabytes of XBRL history. Three keeps browser memory, SEC traffic and free-tier bandwidth predictable while still enabling a useful comparison.

The SEC caps automated access at ten requests per second and may temporarily reject traffic. The Function retries transient SEC 403/429/5xx responses up to five times with exponential backoff and jitter, while the browser spaces Company Facts and Submissions calls. A rejected upstream response is never exposed to users as a raw 403.

## Founding-user access

The live beta is deployed as a Cloudflare Pages branch preview with its preview access policy enabled and only the first ten subscriber emails allowed. This is important: Cloudflare documents that the Pages switch protects preview deployments, not the production `*.pages.dev` hostname. The administrator's catalogue sync uses an Access service token in addition to `ADMIN_SYNC_KEY`.

This is a launch-stage control, not the permanent 10,000-subscriber identity architecture. Before promoting the consumer app to a public production hostname, replace it with the prepared invite/account flow or another properly authenticated application layer.

## Data flow

```text
Admin sync → SEC ticker directory → batched catalogue rows → D1

User search → D1 catalogue only
User clicks Pull → session claim (D1, no financials)
                 → SEC Company Facts streamed through Pages Function
                 → SEC Submissions streamed through Pages Function
                 → browser normalizes selected years
                 → React session memory
                 → cards / statements / comparison
Browser refresh or close → financial facts disappear
```

The Pages Function sets `no-store` on all SEC responses and does not use the Cache API, KV, D1 or logs for financial payloads.
