# What changed (fix for "sqlite3.OperationalError: database is locked")

1) The app now uses **one SQLite connection per Streamlit session** (`get_db()` in `core.py`)
   instead of opening new connections repeatedly.

2) SQLite is configured with:
   - `busy_timeout=30000` (wait up to 30 seconds for locks)
   - `journal_mode=DELETE` (more reliable on Azure file systems than WAL)
   - `timeout=30` at connect time

3) On Azure App Service, the DB file is stored at `/home/app.db` (persistent storage).

If you later expect multiple users uploading data at the same time, consider moving from SQLite to
a server database (PostgreSQL, Azure SQL) to avoid concurrency limits.
