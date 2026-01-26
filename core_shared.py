import threading

import streamlit as st

from db_session import DbCompat, SessionLocal
from core_backend import init_db

# ---------------------------
# Shared connection helper (Streamlit cached)
# ---------------------------

_DB_INIT_LOCK = threading.Lock()


@st.cache_resource
def _get_shared_session():
    """
    One shared DB session per app process.
    """
    with _DB_INIT_LOCK:
        session = SessionLocal()
        init_db(session)
    return DbCompat(session)


def get_db():
    """Get the shared SQLAlchemy-backed DB compatibility wrapper."""
    return _get_shared_session()
