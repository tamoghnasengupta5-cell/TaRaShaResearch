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
| Enterprise Value | Unavailable: TaRaShaData.ai does not yet publish a governed company EV field. |
| P/E and EV multiples | Unavailable because the required governed company market-data inputs are absent. |
| Earnings from discontinued operations | Unavailable unless TaRaShaData.ai adds a canonical metric; no fallback source is used. |
| Separate short-term borrowings vs current portion of long-term debt | Unavailable in the current canonical contract; `short_term_debt` is shown as aggregate current debt. |
| India live coverage | Unavailable in the current TaRaShaData.ai first slice; search returns no live matches. |
