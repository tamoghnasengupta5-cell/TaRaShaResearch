# TaRaSha Discover → TaRaShaData.ai mapping

This is the source-of-truth mapping used by the live Discover adapter. All API paths are relative to `TARASHA_DATA_API_URL`.

The original source-replacement mappings are documented in the main tables below. The normalized-coverage repair from the current exercise is recorded separately first so the two implementation exercises can be audited together.

## Normalized coverage and pull eligibility (current exercise)

| Discover data or decision | TaRaShaData.ai API / field | Transformation |
|---|---|---|
| Search-result inclusion | `GET /v1/companies/search` → `normalized_coverage.available` | Include only ticker-bearing companies with published annual normalized observations. |
| `CatalogCompany.data_available` | Search `normalized_coverage.available` | Boolean converted to `1` or `0`; current search results are always `1` because unavailable rows are excluded server-side and again in the adapter. |
| `CatalogCompany.data_access` | Search `normalized_coverage.data_access` | Published as constant contract value `normalized`. |
| Pull-research availability check | Dataset `normalized_coverage.available` | The server adapter rejects a dataset explicitly marked unavailable before building the Discover company model. |
| Available fiscal-year span | `GET /v1/companies/{identifier}/coverage` → `normalized_coverage.first_fiscal_year`, `latest_fiscal_year` | Direct API availability metadata; not currently rendered in Discover. |
| Statement coverage | Coverage `statements[]` | Provides statement, observation count, metric count, and fiscal-year bounds for income, balance sheet, and cash flow; not currently rendered. |
| Coverage provenance | Coverage `source` and `data_access` | `SEC filings` and `normalized`; not currently rendered. |
| Batched coverage status | `POST /v1/discover/company-dataset` → `normalized_coverage` | Same contract embedded with the financial payload so pull eligibility and returned data cannot drift. |
| Search contract compatibility | Search `normalized_coverage` object | A missing object identifies an outdated running TaRaShaData.ai build; Discover surfaces an explicit restart message instead of silently filtering every match. |

## API operations

| Discover operation | TaRaShaData.ai API | Notes |
|---|---|---|
| Company catalogue search | `GET /v1/companies/search?q={query}&country={country}&limit=30` | Live coverage currently returns USA companies only. |
| Normalized coverage check | `GET /v1/companies/{identifier}/coverage` | Returns availability, annual observation/metric counts, statement coverage, and fiscal-year bounds. |
| Company logo | `GET /v1/companies/{identifier}/logo` through same-origin `GET /api/data/company-logo?companyId=data-{cik}` | Returns TaRaShaData.ai's stored identity asset; the browser never receives the upstream source URL. |
| Initial company pull | `POST /v1/discover/company-dataset` | Sends `identifier`, `from_year`, `to_year`; returns metadata, peers, normalized annual statements, and filings. |
| Recalculate industry basket | `POST /v1/discover/company-dataset` | Adds `constituent_identifiers`; Discover removes its `data-` prefix before sending CIKs. |
| Peer discovery semantics | `GET /v1/companies/{identifier}/peers` | Selects TaRaShaData.ai companies with normalized coverage that share the target issuer's SEC SIC code. |
| Company metadata semantics | `GET /v1/companies/{identifier}` | Embedded in each batched dataset entry. |
| Normalized statement semantics | `GET /v1/companies/{identifier}/financials?statement={income|balance|cash_flow}&period=annual` | Embedded for the target and selected peers. |
| Filing semantics | `GET /v1/companies/{identifier}/filings?financial_only=true` | Embedded for the target company. |

## Catalogue and company metadata

| Discover field | TaRaShaData.ai field | Transformation |
|---|---|---|
| `CatalogCompany.id` | Search `cik` | Prefix with `data-`. |
| `CatalogCompany.cik` | Search `cik` | Direct. |
| `CatalogCompany.name` | Search `name` | Direct. |
| `CatalogCompany.ticker` | Search `ticker` | Direct; rows without a ticker are excluded. |
| `CatalogCompany.exchange` | Search `exchange` | Direct; blank when unavailable. |
| `CatalogCompany.country` | Search `country` | Normalized to `USA` for current coverage. |
| `CatalogCompany.provider` | API identity | Constant `TaRaShaData.ai API`. |
| `CatalogCompany.industryBucket` | Search `industry_buckets[0].name` | SEC `sicDescription` stored by TaRaShaData.ai; the numeric bucket identifier is the issuer's SIC code. |
| `Company.id` | Dataset `company.cik` | Prefix with `data-`. |
| `Company.name` | Dataset `company.name` | Direct. |
| `Company.symbol` | Dataset current `company.aliases[].ticker` | First current alias, then first alias, then CIK. |
| `Company.sector` | Constituent `industry_buckets[0].name` | SEC `sicDescription` from TaRaShaData.ai; falls back to `SIC {code}`, then `Unclassified`. |
| `Company.currency` | Dataset `company.reporting_currency` | `USD` becomes `US$ million`; other currencies become `{code} million`. |
| `Company.reportingPeriod` | Latest mapped annual item `period.end` | Calendar year rendered as `FY {year}`. |
| `Company.updatedAt` | Dataset `company.updated_at` | UTC date formatted for display. |
| `Company.dataMode` | API provider identity | Constant `tarasha-data`. |
| `Company.logoUrl` | Same-origin company-logo proxy for dataset `company.cik` | Builds `/api/data/company-logo?companyId=data-{cik}&contract=1`; the contract token invalidates pre-contract browser caches. The Home header loads the image and keeps the monogram only as a failure/loading fallback. |

## Normalized financial facts

Every annual item uses `display.value`; TaRaShaData.ai therefore owns source selection, derivation, split adjustment, scaling, and presentation units. Items not named below pass through to the statement explorer with their canonical TaRaShaData.ai metric key.

| Discover fact key | TaRaShaData.ai `items[].metric` | Statement API | Transformation |
|---|---|---|---|
| `revenue` | `revenue` | income | Direct displayed value. |
| `costOfRevenue` | `cost_of_revenue` | income | Direct displayed value. |
| `sga` | `selling_general_admin` | income | Direct displayed value. |
| `researchAndDevelopment` | `research_development` | income | Direct displayed value. |
| `operatingIncome` | `operating_income` | income | Direct displayed value. |
| `interestExpense` | `interest_expense` | income | Statement keeps the API sign; earnings bridge uses absolute value with a deduction operator. |
| `pretaxIncome` | `pretax_income` | income | Direct displayed value. |
| `effectiveTaxRate` | `effective_tax_rate` | income | API ratio retained; Discover converts to percent only for display/bridge math. |
| `netIncome` | `net_income`; fallback duplicate from `cash_flow_net_income` | income/cash flow | Deduplicated by fiscal year, preferring income. |
| `minorityInterestInEarnings` | `minority_interest_earnings` | income | Direct; earnings bridge displays the absolute deduction. |
| `netIncomeToCommon` | `net_income_common` | income | Direct displayed value. |
| `sharesOutstanding` | `shares_outstanding_basic` | income → shares group | Direct, in millions. |
| `dilutedShares` | `shares_outstanding_diluted` | income → shares group | Direct, in millions. |
| `epsBasic` | `eps_basic` | income → shares group | Direct, reporting currency per share. |
| `eps` | `eps_diluted` | income → shares group | Direct, reporting currency per share. |
| `ebitda` | `ebitda` | income | Direct/derived displayed value published by TaRaShaData.ai. |
| `ebit` | `ebit` | income | Direct/derived displayed value published by TaRaShaData.ai. |
| `depreciation` | `depreciation_amortization`; fallback `cash_flow_depreciation_amortization` | income/cash flow | Deduplicated by fiscal year, preferring income. |
| `cash` | `cash` | balance | Direct displayed value. |
| `shortTermInvestments` | `short_term_investments` | balance | Direct displayed value. |
| `accountsReceivable` | `accounts_receivable` | balance | Direct displayed value. |
| `inventory` | `inventory` | balance | Direct displayed value. |
| `currentAssets` | `current_assets` | balance | Direct displayed value. |
| `assets` | `assets` | balance | Direct displayed value. |
| `accountsPayable` | `accounts_payable` | balance | Direct displayed value. |
| `currentDebt` | `short_term_debt` | balance | Used as aggregate current interest-bearing debt. |
| `currentLiabilities` | `current_liabilities` | balance | Direct displayed value. |
| `longTermLiabilities` | `long_term_liabilities` | balance | Direct displayed value. |
| `totalDebt` | `total_debt` | balance | Direct/derived displayed value published by TaRaShaData.ai. |
| `equity` | `shareholders_equity` | balance | Direct/derived displayed value published by TaRaShaData.ai. |
| `operatingCash` | `operating_cash_flow` | cash flow | Direct/derived displayed value published by TaRaShaData.ai. |
| `capex` | `capital_expenditures` | cash flow | API sign retained; cash bridges use absolute expenditure. |
| `shareBasedCompensation` | `share_based_compensation` | cash flow | Direct displayed value. |
| `otherAdjustments` | `other_adjustments` | cash flow | Direct displayed value with source sign. |
| `netDebtIssuedPaid` | `net_long_term_debt_issued_repaid` | cash flow | Direct/derived displayed value with source sign. |
| `commonDividendsPaid` | `common_dividends_paid` | cash flow | Statement keeps API sign; earnings bridge uses absolute deduction. |
| `nonCashWorkingCapital` | `current_assets`, `cash`, `current_liabilities`, `short_term_debt` | balance | `current assets − cash − (current liabilities − current debt)`. |

## Company Story: revenue by products, services & offerings

The view consumes only `revenue_offerings` from `POST /v1/discover/company-dataset` plus the normalized revenue series already returned by the same TaRaShaData response. The browser never calls an issuer, the SEC, or a third-party financial source.

| UI output | TaRaShaData.ai field | Derivation / safety rule |
|---|---|---|
| Six fiscal-year totals | `annual_totals[year].revenue_display` | Direct TaRaShaData display value; normalized `metrics.revenue` is only the same-response fallback when a total key is absent. |
| TTM total | `ttm.revenue_display` | TaRaShaData-governed fiscal-year + current-YTD − prior-YTD result. |
| Offering stack | `offerings[].history` / `offerings[].ttm` | Values are rendered only after TaRaShaData returns a source-substantiated partition. The UI independently refuses an over-total stack and never normalizes percentages to hide an overlap. |
| Offering share | Offering value and total revenue for the same period | `offering value ÷ total revenue × 100`. An undisclosed residual is neutral grey; an unreconciled over-total payload is withheld as unsafe. |
| YoY growth / decline | Current and prior period totals; `ttm.prior_year_revenue_display` for TTM | `(current ÷ prior − 1) × 100`; suppressed for missing or non-positive prior revenue. Arrow direction remains chronological when visual sort is reversed. |
| Deferred (unearned) revenue | `adjustment_entries[id=deferred_revenue]` | Direct unclassified balance, otherwise reported current + non-current components; missing components are not estimated. |
| Accrued revenue | `adjustment_entries[id=accrued_revenue]` | Only the explicitly mapped contract-receivable concept family; never substituted from contract assets. |
| Contract liabilities | `adjustment_entries[id=contract_liabilities]` | Direct unclassified balance, otherwise reported current + non-current components. |
| Contract assets | `adjustment_entries[id=contract_assets]` | Direct unclassified balance, otherwise reported current + non-current components. |
| Remaining performance obligations | `adjustment_entries[id=remaining_performance_obligations]` | Direct reported balance. |
| Revenue adjustments | `adjustment_entries[id=revenue_adjustments]` | Direct revenue recognized from opening contract liabilities; TTM appears only when TaRaShaData has the governed inputs. |
| Adjustment percentage | Entry value and total revenue for the same period | `entry value ÷ total revenue × 100`. The six entries are distinct disclosures and are not additive. |
| Availability warnings | `reason`, `quality_warnings`, and per-point `coverage` | Total revenue remains visible while absent or unsafe composition is explicitly labelled. Partial current/non-current entry coverage receives a visible `Partial` badge. |
| Footnote provenance | `methodology`, `adjustment_methodology`, and all point `source_url` values | Displays source/derivation text and every unique contributing filing link, including all inputs to a derived TTM or component sum. |

All API amounts use reporting-currency millions. The chart and cards divide by 1,000 for billions; no other scaling or financial derivation occurs in the browser.

## Company Story: balance-sheet equation

The Balance Sheet story is built from the same annual normalized statement payload. It uses at most the five latest common fiscal years and shows an unavailable state when total assets and shareholders’ equity cannot be reconciled for the same year.

| Story output | Canonical input / aliases | Transformation |
|---|---|---|
| Accounting equation | `assets`, `liabilities`, `shareholders_equity` | Uses reported totals. If total liabilities is absent, derives `assets − shareholders’ equity` and discloses the method. |
| Cash cushion | `total_cash`, `cash_and_short_term_investments`; fallback `cash` + `short_term_investments` | Cash, equivalents and short-term investments ÷ total assets for each year. |
| Receivables, inventory and prepaids | `accounts_receivable`, `inventory`, `prepaid_expenses`; broader fallback `other_current_assets` | Shows direct prepaids when separately tagged. Otherwise the child remains honestly labelled “Prepaids & other current assets”; the available components are summed and divided by total assets. |
| Property, plant and equipment | `property_plant_equipment_net` | Reported value ÷ total assets. |
| Goodwill | `goodwill` | Reported value ÷ total assets. |
| Other intangible assets | `intangible_assets_net_excluding_goodwill` | Reported value ÷ total assets. |
| Unearned revenue | `unearned_revenue`, `unearned_revenue_noncurrent`; deferred-revenue and contract-liability aliases | Shows current and long-term reported values; combined value ÷ total liabilities. |
| Borrowings | `total_debt`, `short_term_debt`, `long_term_debt`, `current_lease_liabilities`, `long_term_leases` | Total debt ÷ total liabilities, with reported debt-type and lease splits. Canonical weighted debt rates and operating/finance lease discount rates are shown when present. |
| Accounts payable | `accounts_payable` | Reported value ÷ total liabilities. |
| Equity components | `common_stock_value`, `additional_paid_in_capital`, `common_stock_and_additional_paid_in_capital`, `retained_earnings`, `accumulated_other_comprehensive_income` | Each reported value ÷ absolute shareholders’ equity. If an issuer reports Common Stock and APIC only as a combined line, the UI labels the combination and does not duplicate it as separate APIC. |
| Net cash (debt) | cash cushion, `total_debt` | `cash + short-term investments − total debt`; positive means net cash and negative means net debt. |
| Net debt / EBITDA | cash cushion, `total_debt`, `ebitda` | `(total debt − cash − short-term investments) ÷ EBITDA`, using the same fiscal year. |
| Interest coverage | `ebit`, `interest_expense` | `EBIT ÷ abs(interest expense)`, using the same fiscal year. |
| Weighted average cost of debt | `debt_weighted_average_interest_rate`, `short_term_debt_weighted_average_interest_rate`, annual `interest_expense`, short/long-term debt, operating/finance lease discount rates | Prefers a current issuer-reported weighted borrowing rate. Otherwise calculates `abs(interest expense) ÷ average short- and long-term borrowings`. For lease-only funding, uses the issuer-reported lease discount rate. If current inputs are unavailable, the latest calculable fiscal year is labelled; genuinely debt-free/unreported cases display “Not applicable” rather than a fabricated zero. |
| Plain-language health label | Net cash (debt), Net Debt / EBITDA, interest coverage | Transparent screen using leverage thresholds of `≤1x` / `≤2.5x` and coverage thresholds of `≥5x` / `≥2x`, plus net-cash status. Presented as context, not an investment recommendation. |

## Company Story: stock risk and volatility

Section 05 receives the complete `stock_risk` contract from `POST /v1/discover/company-dataset`. The browser does not call a market-data provider or derive risk metrics. TaRaShaData acquires zero-cost, delayed adjusted-close history on demand, caches the derived contract for six hours, and returns an explicit unavailable state when aligned history is insufficient. This non-persisted acquisition path does not require manual company re-ingestion.

| Story output | TaRaShaData.ai input | Derivation |
|---|---|---|
| Supported range buttons | Company and S&P 500 adjusted-close coverage | Enables `1Y`, `3Y`, `5Y`, or `10Y` only after both series pass minimum trading-day and calendar-span checks. Defaults to `5Y` when available. The monthly-return distribution separately consumes an exact `7Y` period. |
| Company series | Current TaRaShaData ticker → Yahoo Finance chart service | Daily adjusted closes; invalid, missing, duplicate, and non-positive observations are excluded inside TaRaShaData. |
| Broad-market benchmark | S&P 500 (`^GSPC`) | Same adjusted-close acquisition and derivation as the company. |
| Sector benchmark | SEC SIC stored by TaRaShaData → one of the eleven broad SPDR sector ETFs | Mapping basis and ETF symbol are returned and displayed. The sector comparison is omitted when SIC cannot be mapped or ETF history is insufficient. |
| Annualized volatility | Daily simple returns | Sample standard deviation of daily returns × `√252`. |
| Beta | Company or sector and S&P 500 daily returns | Sample covariance on exact shared trading dates ÷ S&P 500 sample variance. |
| Maximum drawdown | Adjusted-close series within the selected range | Minimum of `adjusted close ÷ running peak − 1`. |
| Worst / best 1-month return | Month-end adjusted closes | Minimum / maximum month-over-month return. |
| Drawdown chart | Daily adjusted closes | Daily percent decline from the running peak; no stock-price chart is rendered in Section 05. |
| Rolling volatility chart | Daily simple returns | Trailing 252-trading-day sample standard deviation × `√252`. |
| Monthly-return distribution | Month-end returns | Seven fixed percentage buckets from below `−15%` to above `10%`; bars show share of observed months. |
| Up vs down months | Month-end returns | Positive, negative, and flat month counts divided by total monthly observations. |
| Footnote provenance | `stock_risk.source`, `methodology`, period coverage, series source URLs, and SEC-SIC selection basis | Displayed under the chapter with delayed/non-authoritative status, adjusted-close basis, observation coverage, derivation formulas, and source-history links. Acquisition cost is intentionally omitted from the customer-facing footnote. |

## Company Story: what the market is pricing

Section 06 receives the complete `market_pricing` contract from `POST /v1/discover/company-dataset`. Discover does not call Yahoo Finance, FRED, or any other financial-data source. TaRaShaData fetches zero-cost external display data on demand, caches it for six hours, and combines it with its own normalized statements and SEC-SIC peer framework. The external data is not persisted and does not enter authoritative fact tables. No manual company re-ingestion or database migration is required; an API-service restart is required after deployment.

| Story output | TaRaShaData.ai input | Derivation |
|---|---|---|
| Current price | Current ticker → Yahoo Finance chart service | Valid positive regular-market price; latest daily close is the fallback. Delayed/non-authoritative status and timestamp are preserved. |
| Market capitalization | Current price, latest TTM diluted shares; annual diluted/basic fallback | `current price × shares`. TaRaShaData financial values remain in raw units inside the backend calculation. |
| Enterprise value | Derived market capitalization, `total_debt`, and `total_cash`; cash-and-short-term-investment fallbacks | `market capitalization + total debt − total cash`. |
| P/E | Current price and TaRaShaData `eps_diluted` | `price ÷ diluted EPS`; non-positive EPS is unavailable. |
| Forward P/E | Yahoo Finance public forward P/E; next-year analyst EPS fallback when present | External valuation metric allowed by the Section 06 method; shown as a third-party forward estimate because the public field does not include a complete contributor methodology or estimate-set date. |
| EV / EBITDA | Derived enterprise value and TaRaShaData `ebitda` | `enterprise value ÷ EBITDA`; non-positive denominator is unavailable. |
| EV / EBIT | Derived enterprise value and TaRaShaData `ebit` | `enterprise value ÷ EBIT`; non-positive denominator is unavailable. |
| Price / Sales | Derived market capitalization and TaRaShaData `revenue` | `market capitalization ÷ revenue`. |
| Price / Book | Derived market capitalization and TaRaShaData `stockholders_equity` / `shareholders_equity` | `market capitalization ÷ shareholders’ equity`; non-positive equity is unavailable. |
| FCF Yield | TaRaShaData `operating_cash_flow`, `capital_expenditures`, and derived market capitalization | `(operating cash flow − abs(capital expenditures)) ÷ market capitalization × 100`. |
| PEG | Yahoo Finance public five-year expected PEG | External valuation metric allowed by the Section 06 method; omitted when unavailable. |
| 5Y / 10Y valuation context | Split-adjusted daily close and matching TaRaShaData annual denominators | Uses the last valid close on or before each fiscal year end (maximum ten-day gap). Displays median, low/high range, empirical current-value percentile, and observation count only with at least three matched observations. |
| Peer comparison | Up to five default constituents from the stored TaRaShaData SEC-SIC peer framework | Displayed explicitly as a framework-defined screening set, not hand-selected core valuation anchors. Uses the same current P/E, EV / EBITDA, and FCF-yield formulas for each peer. A peer is omitted if its quote or denominator is unavailable; no peer is invented outside the framework. |
| Implied revenue growth | Current EV / Sales and five-year median EV / Sales | `((current multiple ÷ historical median)^(1/5) − 1) × 100`, holding enterprise value constant. |
| Implied FCF growth | Current FCF yield and five-year median FCF yield | `((historical median yield ÷ current yield)^(1/5) − 1) × 100`, holding market value constant. |
| Implied operating margin | Current EV / Sales and five-year median EV / EBIT | `current EV / Sales ÷ historical median EV / EBIT × 100`. |
| Implied discount rate | Current FCF yield | `FCF yield + 2% long-run growth assumption`; explicitly labelled a perpetuity shorthand, not a forecast or WACC estimate. |
| FCF yield vs Treasury | Current derived FCF yield and latest non-null FRED `DGS10` observation | Spread is `FCF yield − 10Y Treasury yield`; FRED date and source link are displayed. |
| Analyst bear/base/bull | Yahoo Finance public third-party analyst target low/mean/high and opinion count | Bear = low, Base = mean, Bull = high. The comparison is `(current price − base case) ÷ base case × 100`. The entire scenario set is unavailable unless all three targets are present. The UI says “third-party estimates,” not “consensus,” because the public fields do not expose a complete contributor methodology or estimate-set date. |
| Footnote provenance | `market_pricing.source`, `methodology`, `quality`, analyst source, Treasury source, and peer framework | Displays TaRaShaData financial origin, delayed/transient status, formulas, dates, warnings, and direct source links under the section. Acquisition cost is intentionally omitted from the customer-facing footnote. |

## Company Story: how the stock got here

Section 07 receives the complete `stock_history` contract from `POST /v1/discover/company-dataset`. Discover never calls the market or SEC directly. TaRaShaData combines normalized annual fundamentals and stored filing/corporate-action metadata with zero-cost delayed daily market history and source-verbatim management commentary. External display data is cached for six hours and is not persisted in the financial-fact data plane. The pipeline is on demand, so no company re-ingestion or schema migration is required; restart the API service to deploy the contract.

| Story output | TaRaShaData.ai input | Derivation |
|---|---|---|
| `1Y / 3Y / 5Y / 10Y / Max` | Current ticker → validated daily close history | A range is enabled only when at least 80% of its requested calendar span is covered. `Max` begins with the issuer’s first valid observation. |
| Price | Daily close | Split-adjusted historical close from the delayed display feed. |
| Total Return | Daily adjusted close | Rebased to 100 at the first in-window observation; adjusted close incorporates applicable distributions. |
| Revenue | TaRaShaData normalized annual `revenue` | Direct source-linked annual series. |
| EPS | TaRaShaData normalized annual `eps_diluted`; basic fallback | Direct source-linked annual per-share series. |
| Free Cash Flow | TaRaShaData annual `operating_cash_flow` and `capital_expenditures` | `operating cash flow − abs(capital expenditures)`. |
| Performance summary | First/last in-window close and S&P 500 adjusted close | Price change, elapsed-time CAGR, total return, relative S&P 500 total return, and in-window high/low. |
| Price at a glance | Latest delayed price, TaRaShaData diluted shares, recent daily volume | Market cap is `price × diluted shares`; average volume is the mean of valid observations from the latest 100 calendar days. |
| Event overlays | Stored TaRaShaData 10-K, 10-Q, 8-K, 20-F, 6-K, and corporate-action metadata | Source-linked filing and split dates only; no acquisition or strategy event is inferred from an unclassified filing. |
| Indexed comparison | Price, Revenue, EPS, and FCF series | Each positive series is divided by its first in-window observation and multiplied by 100. A non-positive or one-point series is omitted. |
| Future projected strategy | Latest available management 10-K/20-F/10-Q/6-K/8-K language | TaRaShaData selects up to three source-verbatim, future-oriented strategy statements; risk boilerplate and generic statements are excluded. No forecast is generated when specific management language is unavailable. |
| Footnote provenance | `stock_history.source`, `strategy`, `methodology`, and `quality` | Displays the financial, filing, market, benchmark, and management sources; delayed/transient status; formulas; warnings; and direct links. Acquisition cost is intentionally omitted from the customer-facing footnote. |

## Discover calculations

| Discover output | TaRaShaData.ai inputs | Calculation |
|---|---|---|
| `metrics.revenue` | `revenue` | Direct annual series. |
| `metrics.operatingMargin` | `operating_income`, `revenue` | `operating income ÷ revenue × 100`. |
| `metrics.freeCashFlow` | `operating_cash_flow`, `capital_expenditures` | `operating cash flow − abs(capex)`. |
| `metrics.netDebt` | `total_debt`, `cash`, `short_term_investments` | `total debt − cash − short-term investments`. |
| Gross profit series | `revenue`, `cost_of_revenue` | `revenue − cost of revenue`. |
| Growth deltas | Annual company/peer revenue, gross profit, operating income | Current minus prior; percent change divides by absolute prior value. |
| Gross operating leverage | Annual revenue and gross profit | `change in gross profit ÷ change in revenue × 100`. |
| Growth statistics | Company/peer growth deltas | Median, sample standard deviation, observations, start/end, total change. |
| Gross margin | Revenue and gross profit | `gross profit ÷ revenue × 100`. |
| Operating margin | Operating income and revenue | `operating income ÷ revenue × 100`. |
| COGS / revenue | Cost of revenue and revenue | `cost of revenue ÷ revenue × 100`. |
| SG&A / revenue | SG&A and revenue | `SG&A ÷ revenue × 100`. |
| D&A / revenue | D&A and revenue | `D&A ÷ revenue × 100`. |
| R&D / revenue | R&D and revenue | `R&D ÷ revenue × 100`. |
| Profitability bands | Company and peer ratio observations | Lower quartile, median, upper quartile, observation count, and direction. |
| Gross profit bridge | Revenue and cost of revenue | `revenue − COGS`. |
| Other operating expense bridge | Gross profit, SG&A, R&D, EBITDA | `gross profit − SG&A − R&D − EBITDA`. |
| Taxes bridge | Pretax income and net income | `pretax income − net income`. |
| Other common-income bridge | Net income, net income to common, minority interest, discontinued operations | Residual that ties to net income to common. |
| Current-year earnings retained | Net income to common and common dividends | `net income to common − abs(common dividends paid)`. |
| NOPAT | EBIT and effective tax rate | `EBIT × (1 − tax rate)`. |
| Working-capital movement | Derived non-cash working capital for current/prior year | Current minus prior; cash impact is the inverse sign. |
| FCFF | NOPAT, D&A, capex, working-capital cash impact | `NOPAT + D&A − abs(capex) + working-capital impact`. |
| FCFE | Net income to common, D&A, share compensation, other adjustments, capex, working-capital impact, net borrowing | Sum inflows/adjustments, subtract `abs(capex)`, add working-capital impact and net borrowing. Missing net borrowing is zero. |
| Industry medians/distributions | Same mapped fields for `constituents[]` datasets | Per-observation distributions, medians, and sample standard deviations. |

## Traceability and intentional gaps

| Discover field | TaRaShaData.ai source/status |
|---|---|
| `statements[]` | All normalized annual API items; known metrics use the crosswalk above and remaining canonical keys pass through unchanged. |
| `filings[]` | Dataset `filings[]`: accession, form, filing date, report date, primary document, and source URL. |
| `researchShelf.industryBucket` | Dataset `industry_buckets[0].name`, derived solely from TaRaShaData.ai's SEC SIC metadata. |
| `researchShelf.industryConstituents` | Up to 15 default TaRaShaData.ai companies sharing the target SEC SIC code, plus observation counts calculated from mapped annual facts. |
| Legacy `researchShelf.earningsAndValuation` Enterprise Value | Remains unavailable because that older research-shelf contract does not consume transient market data. Section 06 instead receives the governed `companyStory.marketPricing` contract from TaRaShaData.ai. |
| Section 06 P/E and EV multiples | Available through `companyStory.marketPricing` when TaRaShaData.ai returns a valid delayed price and required normalized denominator; otherwise explicitly unavailable. |
| Section 07 price/fundamental history | Available through `companyStory.stockHistory`; financial series remain TaRaShaData normalized data and external display fields are labeled delayed, `$0`, non-authoritative, transient, and source-linked. |
| Earnings from discontinued operations | Unavailable unless TaRaShaData.ai adds a canonical metric; no fallback source is used. |
| Separate short-term borrowings vs current portion of long-term debt | Unavailable in the current canonical contract; `short_term_debt` is shown as aggregate current debt. |
| India live coverage | Unavailable in the current TaRaShaData.ai first slice; search returns no live matches. |
