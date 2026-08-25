# Global market overview data contract

The authenticated Home page uses same-origin `GET /api/market-overview`. The endpoint retrieves only public, unauthenticated sources and requires no market-data API key.

## Metric map

| Displayed metric | Source | Frequency | Calculation |
|---|---|---|---|
| S&P 500, FTSE 100, Nikkei 225, Shanghai Composite, and NIFTY 50 returns | Yahoo Finance public chart response | Daily market close | One week and one year are close-to-close changes. Five year is annualized CAGR using the nearest available close at or before the anniversary. |
| P/E and P/B | iShares country ETF portfolio characteristics | Daily | The named ETF is an explicit valuation proxy. These values are not represented as the benchmark index's own ratios. |
| Yield | iShares country ETF 12-month trailing distribution yield | Monthly | The named ETF's trailing distribution yield. |
| GDP / market capitalization | World Bank WDI `CM.MKT.LCAP.GD.ZS`, sourced from WFE | Annual | `100 / market capitalization of listed domestic companies (% of GDP)`. Each row shows the source year. |
| Global foreign reserves | World Bank WDI `FI.RES.TOTL.CD`, sourced from IMF IFS | Annual | Sum of reporting economies. World Bank aggregate rows are excluded; the newest year must have at least 90% of the best recent country coverage. |
| Global equity and bond fund flows | ICI / IIFA worldwide regulated open-end fund release | Quarterly | Latest reported worldwide net sales and quarter-over-quarter change. |
| VIX and DXY | Yahoo Finance public chart response | Daily market close | Current close and one-week change. |
| US 10-year yield and fed funds upper bound | Federal Reserve data through FRED (`DGS10`, `DFEDTARU`) | Daily | Latest observation and change in percentage points over one week. |

## Freshness and failure behavior

- Every metric retains its own source date or reporting period. The UI does not imply that annual, quarterly, monthly, and daily values share a common date.
- The response is cached for 15 minutes in browsers and up to six hours at the edge.
- A source failure uses the committed, source-derived snapshot. Its date remains visible, and the UI exposes a source-status detail rather than silently presenting the fallback as live.
- No estimate is substituted for a missing value.

## Cost boundary

- The provider has a closed hostname allowlist and no authorization header, token, API key, or billable market-data SDK.
- `npm run audit:market-data-cost` fails if the provider introduces a non-allowlisted hostname or a credential-bearing request pattern.
- Therefore the repository contains zero paid **data-provider** calls for the market overview.
- Hosting, network, CI, or Cloudflare account charges are operational-account concerns outside the data-fetching contract. The repository cannot certify the billing configuration of an external account.

The endpoint is for the existing private, non-commercial preview. Public-source availability and terms can change, so the allowlist and source terms must be reviewed before any broader distribution.
