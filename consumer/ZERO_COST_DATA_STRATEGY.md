# Zero-cost shared Research provider strategy

## Decision

The private-preview strategy is implemented at zero additional infrastructure cost.

| Requested capability | Decision | Reason |
|---|---|---|
| Share the Research database | Implemented | Research uses PostgreSQL; Consumer queries only two read-only views through its server adapter. |
| Store no Consumer financial copy | Implemented | Retrieved financial facts exist only in browser session memory. |
| Reflect future bulk uploads | Implemented | Research writes to the shared database; subsequent Consumer pulls read the same records. |
| USA and India Research coverage | Implemented | Search is limited to companies already bulk-uploaded into Research. |
| Switch to a licensed API | Prepared | The Pages Function provider boundary returns one normalized contract regardless of upstream provider. |
| Paid/commercial redistribution | Disabled | Explicit source-provider permission or a licensed API is required before commercial launch. |

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
- Supabase free-plan database limits: <https://supabase.com/docs/guides/platform/database-size>
- StockAnalysis.com terms: <https://stockanalysis.com/terms-of-use/>
- StockAnalysis.com financial-source explanation: <https://stockanalysis.com/financial-sources/>

## Limits

- Maximum fifty distinct companies per browser session.
- Maximum seven reporting years per company pull.
- Refreshing or closing the application clears all extracted financial facts because they are React memory only.
- Search results are compact catalogue rows. A full company card is created only after a successful research pull.

Fifty companies and seven reporting years support a meaningful personal research shelf while each pull remains restricted to approved fields. Financial payloads remain transient in browser memory; monitor Supabase free-tier egress as founding-user activity grows.

The SEC caps automated access at ten requests per second and may temporarily reject traffic. The Function retries transient SEC 403/429/5xx responses up to five times with exponential backoff and jitter, while the browser spaces Company Facts and Submissions calls. A rejected upstream response is never exposed to users as a raw 403.

## Founding-user access

The live beta is deployed as a Cloudflare Pages branch preview with its preview access policy enabled and only the first ten subscriber emails allowed. This is important: Cloudflare documents that the Pages switch protects preview deployments, not the production `*.pages.dev` hostname. The administrator's catalogue sync uses an Access service token in addition to `ADMIN_SYNC_KEY`.

This is a launch-stage control, not the permanent 10,000-subscriber identity architecture. Before promoting the consumer app to a public production hostname, replace it with the prepared invite/account flow or another properly authenticated application layer.

## Data flow

```text
Research bulk upload → shared PostgreSQL database
                     → restricted consumer_companies / consumer_financial_facts views

User search → Pages Function → active provider → restricted company view
User clicks Pull → Pages Function → active provider → approved financial rows
                 → normalized provider contract
                 → React session memory
                 → cards / statements / comparison
Browser refresh or close → financial facts disappear
```

The Pages Function sets `no-store`, does not place financial payloads in D1/KV/Cache, and never sends the shared-database service key to the browser. Setting `DATA_PROVIDER=sec` restores the legacy SEC flow; a future licensed provider should implement the same normalized response contract.
