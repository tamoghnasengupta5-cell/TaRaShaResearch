# Where the money goes

## Runtime contract

TaRaSha Discover renders this chapter only from the
`operating_cost_structure` object returned by TaRaShaData's existing
`POST /v1/discover/company-dataset` request. The browser and Discover worker do
not contact the SEC or any third-party financial-data provider for this story.

## Derivation

TaRaShaData builds the story from its normalized annual income statement:

1. Select the latest five fiscal years containing positive revenue and an
   operating-income observation.
2. Divide every line by the same year's reported revenue and multiply by 100.
3. Present cost and operating-expense values as deductions.
4. Preserve a source-reported operating-income credit as an addition.
5. Keep separately reported selling/marketing and G&A inputs separate when the
   TaRaShaData provenance contains both inputs.
6. Keep combined SG&A combined when that is what the issuer reports.
7. Omit R&D, selling/marketing, or G&A when TaRaShaData does not contain the
   corresponding reported line.
8. Label any deterministic reconciliation amount as "Operating costs not
   separately reported"; never assign it to an invented category.

The waterfall and historical table use the same payload, so their values and
signs cannot diverge.

## Source and cost boundary

The authoritative financial observations originate from issuer filings in the
SEC archives and are already persisted and normalized by TaRaShaData. The SEC
source acquisition cost is $0. This chapter adds no paid provider, billable SDK,
market-data credential, browser-side data call, or new external acquisition
pipeline.

## Deployment and ingestion

Manual financial re-ingestion is not required for already-ingested companies;
the required normalized facts and provenance inputs already exist in
TaRaShaData. Restart or redeploy the TaRaShaData API so the running service
publishes the new story-contract field. Future companies still require the
normal TaRaShaData SEC ingestion workflow before they can appear in Discover.
