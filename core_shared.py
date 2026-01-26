import threading
import sqlite3

import streamlit as st

from core_backend import get_conn, init_db

# ---------------------------
# Shared connection helper (Streamlit cached)
# ---------------------------

_DB_INIT_LOCK = threading.Lock()


@st.cache_resource
def _get_shared_conn() -> sqlite3.Connection:
    """
    One shared DB connection per app process.
    This dramatically reduces 'database is locked' errors on Azure.
    """
    with _DB_INIT_LOCK:
        conn = get_conn()
        init_db(conn)
    return conn


def get_db() -> sqlite3.Connection:
    """Get the shared SQLite connection."""
    return _get_shared_conn()
