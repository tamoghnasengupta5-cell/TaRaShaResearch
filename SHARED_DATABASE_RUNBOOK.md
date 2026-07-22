# TaRaSha shared Research database runbook

## Purpose

TaRaSha Research and the private TaRaSha Consumer preview now use one PostgreSQL database. Research remains the only writer. Consumer receives a narrow, read-only projection through its Cloudflare server adapter and keeps pulled facts only in browser memory.

This configuration is for private, non-commercial evaluation. Financial spreadsheets were downloaded through StockAnalysis.com and bulk-uploaded into Research. Obtain explicit redistribution permission or activate a licensed finance API before paid distribution.

## Provisioned resource

| Item | Value |
|---|---|
| Supabase project | `TaRaSha Shared Research` |
| Project ref | `fgmijnuwplxasztyjfqr` |
| Region | Canada Central |
| Database | PostgreSQL 17 |
| Initial migrated size | approximately 276 MB |
| Initial companies | 3,398 |
| Approved Consumer fact rows | 1,445,910 |

The database password, full SQLAlchemy URL, and service-role key are stored in macOS Keychain. They are not committed.

## How Research selects the shared database

`db_config.get_db_url()` uses this order:

1. `TARASHA_DB_URL`, when explicitly set.
2. On the owner's Mac, Keychain service `TaRaSha Shared Database URL`, account `TaRaShaResearch`.
3. The existing Azure/local SQLite fallback.

Consequently, the normal local Research launch now writes bulk uploads directly to shared PostgreSQL. To force the old local SQLite database for a diagnostic session:

```bash
TARASHA_DB_URL="sqlite:////Users/tamoghna/Documents/TaRaShaResearch/app.db" \
  .venv/bin/python -m streamlit run app.py
```

## Schema and Consumer boundary

- Current SQLAlchemy metadata contains all 130 Research tables.
- Alembic revision `20260712_0001` brings four formerly runtime-created tables under migration ownership.
- `consumer_companies` exposes only company identity and market.
- `consumer_financial_facts` exposes an allowlisted set of annual statement facts.
- `anon` and `authenticated` have no grants on those views.
- The Consumer service-role key is encrypted in Cloudflare and never sent to the browser.

## Research latency controls

Research reuses a bounded PostgreSQL connection pool and labels its sessions
`TaRaShaResearch`. Each uploaded company is written with set-based upserts and
one transaction, including its industry membership and dependent metrics. The
Combined Dashboard is read-only and fetches its annual metric series in one
batched query; it does not refresh the entire database during page rendering.

The defaults can be tuned without changing Consumer or creating another database:

| Setting | Default | Purpose |
|---|---:|---|
| `TARASHA_DB_POOL_SIZE` | `5` | Persistent Research connections per app process |
| `TARASHA_DB_MAX_OVERFLOW` | `5` | Short-lived burst capacity |
| `TARASHA_DB_POOL_TIMEOUT` | `30` | Seconds to wait for a Research connection |
| `TARASHA_DB_POOL_RECYCLE` | `900` | Seconds before replacing an idle connection |
| `TARASHA_DB_APPLICATION_NAME` | `TaRaShaResearch` | PostgreSQL workload label |

PostgreSQL schema creation is migration-owned. Research therefore skips
`metadata.create_all()` on normal PostgreSQL startup. Set
`TARASHA_AUTO_CREATE_SCHEMA=true` only for a controlled bootstrap; normal
deployments should continue to use `alembic upgrade head`.

## Re-run a complete SQLite migration

Make a database backup first. The migration is atomic and excludes SQLite rows whose foreign-key parent has been deleted.

```bash
cd /Users/tamoghna/Documents/TaRaShaResearch
DB_PASSWORD="$(security find-generic-password \
  -a 'fgmijnuwplxasztyjfqr' \
  -s 'TaRaSha Shared Postgres Password' -w)"
POSTGRES_URL="postgresql+psycopg://postgres.fgmijnuwplxasztyjfqr:${DB_PASSWORD}@aws-0-ca-central-1.pooler.supabase.com:5432/postgres?sslmode=require" \
  .venv/bin/python scripts/migrate_sqlite_to_postgres.py \
  --sqlite-path app.db --truncate --batch-size 1000
unset DB_PASSWORD
```

The script filters orphaned foreign-key rows, reports each skipped count, and synchronizes PostgreSQL identity sequences.

## Azure Research deployment

The current Azure fallback continues using `/home/app.db` until the Azure App Service setting `TARASHA_DB_URL` is set to the shared pooler URL. Do not change that setting without first backing up `/home/app.db` and comparing it with PostgreSQL; uploads performed only in Azure after the local snapshot date must be migrated before cutover.

## Future licensed API migration

Consumer calls a provider-neutral Pages Function contract. The current `research-db` provider and the retained `sec` provider are selected server-side. A paid provider should implement the same catalogue and normalized company response, after which changing `DATA_PROVIDER` switches the source without rewriting Discover, the Research Shelf, statements, comparison, or watchlist UI.
