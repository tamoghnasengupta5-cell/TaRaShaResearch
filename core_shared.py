import threading
import os

import streamlit as st
from sqlalchemy import text

from db_config import get_db_url, is_sqlite_url
from db_session import DbCompat, SessionLocal, get_engine
from core_backend import init_db

# ---------------------------
# Shared connection helper (Streamlit cached)
# ---------------------------

_DB_INIT_LOCK = threading.Lock()


@st.cache_resource
def _initialize_database_runtime():
    """Initialize the engine once without sharing a mutable Session."""
    with _DB_INIT_LOCK:
        engine = get_engine()
        url = get_db_url()
        auto_create = os.environ.get("TARASHA_AUTO_CREATE_SCHEMA", "").strip().lower() in {
            "1", "true", "yes", "on"
        }
        if is_sqlite_url(url) or auto_create:
            # Preserve local/Azure SQLite bootstrap behavior. Shared PostgreSQL
            # is migration-owned and must not perform 130 catalog checks during
            # an interactive page load.
            session = SessionLocal()
            try:
                init_db(session)
            finally:
                session.close()
        else:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1 FROM companies LIMIT 1"))
    return engine


def get_db():
    """Return an operation-scoped database wrapper on the shared engine."""
    _initialize_database_runtime()
    return DbCompat(SessionLocal())
