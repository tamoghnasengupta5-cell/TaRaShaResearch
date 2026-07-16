# US Industrials taxonomy migration

This directory contains the strict StockAnalysis US Industrials universe and the
taxonomy inputs approved for TaRaShaResearch.

- `us_industrials_universe_2026-07-16.csv`: 709 downloadable public-company
  rows, each mapped to one of 12 Industry Buckets and one of 9 relative-
  valuation Sub-Categories.
- `us_industrials_bucket_definitions_2026-01-09.csv`: the 12 bucket beta rows.
  The unlevered and cash-adjusted betas use the January 2026 US industry data;
  consolidated buckets are constituent-count weighted and rounded to two
  decimals.
- `us_industrials_subcategory_definitions_2026-07-16.csv`: expected universe
  counts by Sub-Category.

The source page reported 711 Industrials securities on the capture date. Its
downloadable public table contained 709 company rows; this snapshot intentionally
uses those 709 rows to preserve strict sector purity. Source references:

- [StockAnalysis US Industrials sector](https://stockanalysis.com/stocks/sector/industrials/)
- [Damodaran data updates](https://pages.stern.nyu.edu/~adamodar/New_Home_Page/dataupdate.html)

## Run the migration

Preview the current database reconciliation (no writes):

```bash
python scripts/migrate_us_industrials_taxonomy.py
```

Apply to SQLite. A timestamped database backup is created automatically before
the transaction starts:

```bash
python scripts/migrate_us_industrials_taxonomy.py --apply
```

Apply to PostgreSQL only after taking an external database backup:

```bash
python scripts/migrate_us_industrials_taxonomy.py \
  --apply \
  --confirm-backup \
  --db-url "$TARASHA_DB_URL"
```

The migration is idempotent. It rejects unknown `Industrials : ...` groups or
beta rows instead of guessing how to consolidate them. Companies absent from
TaRaShaResearch are reported but are not created without financial data.
