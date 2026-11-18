"""Read recorded financial-upload dates without changing score calculations."""

import sqlite3

import pandas as pd
from sqlalchemy import inspect

from core_backend import read_df


UPLOAD_DATE_COLUMN = "Latest Uploaded Date"


def add_latest_upload_dates(conn, frame):
    """Add UTC dates scoped to the current company ID, ticker, name and country."""
    out = frame.copy()
    out[UPLOAD_DATE_COLUMN] = pd.NaT
    if out.empty or "_company_id" not in out or conn is None:
        return out
    session = getattr(conn, "session", None)
    if session is not None:
        exists = inspect(session.get_bind()).has_table("company_bulk_upload_status")
    elif isinstance(conn, sqlite3.Connection):
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            ("company_bulk_upload_status",),
        ).fetchone() is not None
    else:
        raise TypeError("Upload dates require a managed database session or SQLite connection.")
    # Older local databases may have no upload history yet.
    if not exists:
        return out
    ids = sorted({int(value) for value in out["_company_id"]})
    placeholders = ",".join("?" for _ in ids)
    dates = read_df(
        """
        SELECT s.company_id, MAX(s.latest_uploaded_at_utc) AS latest_uploaded_at_utc
        FROM company_bulk_upload_status s
        JOIN companies c ON c.id = s.company_id
            AND c.ticker = s.ticker AND c.name = s.company_name AND c.country = s.country
        WHERE c.id IN (""" + placeholders + ") GROUP BY s.company_id",
        conn, params=ids,
    )
    if not dates.empty:
        mapping = dates.set_index("company_id")["latest_uploaded_at_utc"]
        values = pd.to_datetime(out["_company_id"].map(mapping), errors="coerce", utc=True, format="mixed")
        out[UPLOAD_DATE_COLUMN] = values.dt.tz_localize(None).dt.normalize()
    return out
