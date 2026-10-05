# TaRaShaResearch DB Migration & ORM Compatibility Report

Timestamp: 2026-01-26 15:52:13 -05:00

## Overview
This document summarizes the database migration/compatibility work performed to move from direct SQLite usage toward SQLAlchemy + Postgres compatibility, while keeping SQLite support. It also outlines recommended test scenarios to validate application behavior after these changes.

## High-level goals
- Keep the app functional on SQLite for local/dev.
- Enable Postgres compatibility with minimal disruption.
- Introduce SQLAlchemy ORM scaffolding (models + session) and Alembic migrations.
- Provide compatibility helpers so legacy SQL continues to work with SQLAlchemy sessions.

## Changes Applied

### 1) SQLAlchemy + Alembic scaffolding
- Added SQLAlchemy metadata-based table definitions.
  - File: `db_models.py`
- Added ORM class mappings for tables.
  - File: `db_orm.py`
- Added DB configuration helper for URL and SQLite path.
  - File: `db_config.py`
- Added SQLAlchemy engine/session and DB-API compatibility wrapper.
  - File: `db_session.py`
- Initialized Alembic, configured to use `TARASHA_DB_URL`, and generated initial migration.
  - Files: `alembic/`, `alembic.ini`, `alembic/env.py`, `alembic/versions/3373347030cf_init_schema.py`
- Added migration helper script for SQLite ? Postgres data transfer.
  - File: `scripts/migrate_sqlite_to_postgres.py`
- Updated requirements for SQLAlchemy, Alembic, psycopg.
  - File: `requirements.txt`

### 2) Connection/session handling
- Streamlit shared DB now returns SQLAlchemy-backed `DbCompat` wrapper.
  - File: `core_shared.py`
- `core_backend.init_db` uses SQLAlchemy metadata to create tables, then wraps with DbCompat for legacy logic.
  - File: `core_backend.py`

### 3) Read-path compatibility
- Introduced `_read_df` / `read_df` helper to route reads through SQLAlchemy session when available.
  - File: `core_backend.py`
- Replaced direct `pd.read_sql_query(...)` usage in UI modules with `read_df(...)`.
  - Files: `combined_dashboard.py`, `cap_structure_cost.py`, `cash_flow_spread.py`, `bs_metrics.py`, `admin.py`, `key_data.py`, `pl_metrics.py`
- Fixed missing `read_df` import in `cash_flow_spread.py` (runtime error fix).

### 4) Write-path compatibility + ORM usage
- Removed SQLite-only `INSERT OR IGNORE/REPLACE` and replaced with `ON CONFLICT`.
  - Files: `core_backend.py`, `admin.py`, `bs_metrics.py`, `cap_structure_cost.py`, `key_data.py`, `pl_metrics.py`
- Fixed `ON CONFLICT` placement for `INSERT ... SELECT ...` statements to be valid in Postgres.
  - File: `core_backend.py`

### 5) ORM helpers for bucket/group operations
- Added ORM-aware helpers for group CRUD and membership management.
  - File: `core_backend.py`
    - `get_company_group_id`
    - `add_company_group_members`
    - `remove_company_group_members`
    - `delete_company_group`
- Updated bucket save/assign/remove flows to use helpers (works across SQLite/Postgres).
  - Files: `bs_metrics.py`, `cap_structure_cost.py`, `pl_metrics.py`, `key_data.py`, `admin.py`

### 6) ORM helpers for Admin tab write flows
- Added ORM-aware helpers for admin settings writes.
  - File: `core_backend.py`
    - `update_growth_weight_factors`
    - `update_stddev_weight_factors`
    - `upsert_risk_free_rate`
    - `upsert_index_annual_price_movement`
    - `update_implied_equity_risk_premium`
    - `replace_marginal_corporate_tax_rates`
    - `replace_industry_betas`
- Updated `admin.py` to use these helpers instead of raw SQL.

### 7) Smoke tests added
- Added a group membership smoke script (in-memory SQLite) with foreign key enforcement to verify cascades.
  - File: `scripts/smoke_group_membership.py`
- Added Postgres admin write helper smoke script (requires `TARASHA_DB_URL`).
  - File: `scripts/smoke_postgres_admin_writes.py`

## Recommended Test Scenarios

### A) UI smoke tests (manual)
1. **Open App**
   - Start Streamlit app.
   - Verify no immediate errors on load.

2. **Equity Research ? Combined Dashboard**
   - Open Combined Dashboard.
   - Validate weights and group/bucket driven dropdowns render.
   - Confirm no `read_df` or SQL errors in logs.

3. **Equity Research ? Balance Sheet / P&L / Cash Flow**
   - Create a new bucket from selected companies.
   - Re-open tab, select bucket, ensure selection works.

4. **Admin ? Buckets**
   - Assign orphan companies to bucket.
   - Remove a company from a bucket.
   - Delete a bucket.
   - Confirm memberships update in UI and no orphan SQL errors.

5. **Admin ? Weights (Growth/Stddev)**
   - Update a few weights, save, reload page.
   - Ensure updated values persist.

6. **Admin ? Risk-Free Rate / Index Annual Price Movement**
   - Edit latest year, save.
   - Add ensuing year, save.
   - Verify table reloads with updated data.

7. **Admin ? Implied ERP / Marginal Corporate Tax Rates / Industry Beta**
   - Edit and save; confirm values persist on reload.

### B) Local automated smoke tests
1. **Group membership test**
   - Run: `python scripts\smoke_group_membership.py`
   - Expected: add/remove/delete flows complete without errors.

2. **Admin write helper test (Postgres)**
   - Set `TARASHA_DB_URL` to a test Postgres DB.
   - Run: `python scripts\smoke_postgres_admin_writes.py`
   - Expected output: `ok`.

### C) DB migration tests
1. **SQLite compatibility**
   - Run app with default SQLite `app.db` and validate all tabs.

2. **Postgres compatibility**
   - Set `TARASHA_DB_URL` to Postgres.
   - Run Alembic migration.
   - Optionally migrate data from SQLite using `scripts/migrate_sqlite_to_postgres.py`.
   - Validate all tabs and admin writes.

## Known Notes / Risks
- SQLite uses `PRAGMA foreign_keys=ON` for cascade deletes; Postgres always enforces FKs. Ensure FK cascades are expected.
- Some UI strings include legacy Unicode; not changed here.
- If any remaining direct SQL uses DB-specific syntax, it should be reviewed before production.

## Files Changed (Summary)
- DB + ORM: `db_models.py`, `db_orm.py`, `db_config.py`, `db_session.py`
- Migrations: `alembic/`, `alembic.ini`
- Core logic: `core_backend.py`, `core_shared.py`, `core.py`
- UI modules: `admin.py`, `combined_dashboard.py`, `cap_structure_cost.py`, `cash_flow_spread.py`, `bs_metrics.py`, `key_data.py`, `pl_metrics.py`
- Scripts: `scripts/migrate_sqlite_to_postgres.py`, `scripts/smoke_group_membership.py`, `scripts/smoke_postgres_admin_writes.py`
- Docs: `README.md`
- Dependencies: `requirements.txt`

## Suggested Next Actions
- If you plan to move to Postgres in production, validate the app against a staging Postgres DB using the tests above.
- Decide whether to incrementally refactor remaining SQL to ORM or keep compatibility wrapper only.
- If desired, add automated CI tests to run the smoke scripts on every change.
