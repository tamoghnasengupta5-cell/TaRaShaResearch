from __future__ import annotations

from contextlib import contextmanager
from functools import lru_cache
import os
from typing import Any, Iterable, Mapping, Tuple

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from db_config import get_db_url, is_sqlite_url


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    url = get_db_url()
    connect_args = {}
    engine_kwargs: dict[str, Any] = {
        "future": True,
        "pool_pre_ping": True,
        "pool_reset_on_return": "rollback",
    }
    if is_sqlite_url(url):
        connect_args = {"check_same_thread": False}
    else:
        # Research and Consumer use separate clients/roles against the same
        # PostgreSQL database. Keep Research's pool bounded so Consumer traffic
        # cannot exhaust writer connections, while still reusing warm sockets.
        connect_args = {
            "application_name": os.environ.get("TARASHA_DB_APPLICATION_NAME", "TaRaShaResearch"),
        }
        engine_kwargs.update(
            pool_size=max(1, int(os.environ.get("TARASHA_DB_POOL_SIZE", "5"))),
            max_overflow=max(0, int(os.environ.get("TARASHA_DB_MAX_OVERFLOW", "5"))),
            pool_timeout=max(1, int(os.environ.get("TARASHA_DB_POOL_TIMEOUT", "30"))),
            pool_recycle=max(60, int(os.environ.get("TARASHA_DB_POOL_RECYCLE", "900"))),
        )
    return create_engine(url, connect_args=connect_args, **engine_kwargs)


class ManagedSession(Session):
    """Session that can defer legacy helper commits into one outer transaction."""

    _DEFER_KEY = "tarasha_deferred_commit_depth"

    def commit(self) -> None:
        if int(self.info.get(self._DEFER_KEY, 0)) > 0:
            # Existing persistence helpers call commit after each metric family.
            # During a company upload, flush those changes but leave the actual
            # COMMIT to DbCompat.transaction().
            self.flush()
            return
        super().commit()


SessionLocal = sessionmaker(
    bind=get_engine(),
    class_=ManagedSession,
    autoflush=False,
    autocommit=False,
    expire_on_commit=False,
    future=True,
)


@contextmanager
def session_scope() -> Iterable[Session]:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _to_named_params(sql: str, params: Tuple[Any, ...] | Mapping[str, Any] | None) -> tuple[str, Mapping[str, Any]]:
    if params is None:
        return sql, {}
    if isinstance(params, Mapping):
        return sql, params

    parts = sql.split("?")
    if len(parts) == 1:
        return sql, {}

    named: dict[str, Any] = {}
    out = []
    for i, part in enumerate(parts[:-1]):
        key = f"p{i}"
        out.append(part)
        out.append(f":{key}")
        named[key] = params[i]
    out.append(parts[-1])
    return "".join(out), named


def execute(session: Session, sql: str, params: Tuple[Any, ...] | Mapping[str, Any] | None = None):
    named_sql, named_params = _to_named_params(sql, params)
    return session.execute(text(named_sql), named_params)


def executemany(session: Session, sql: str, seq_params: Iterable[Tuple[Any, ...]] | Iterable[Mapping[str, Any]]):
    seq_params = list(seq_params)
    if not seq_params:
        return None

    first = seq_params[0]
    if isinstance(first, Mapping):
        named_sql, _ = _to_named_params(sql, first)
        return session.execute(text(named_sql), seq_params)

    named_sql, _ = _to_named_params(sql, tuple(first))
    payload = []
    for row in seq_params:
        _, named = _to_named_params(sql, tuple(row))
        payload.append(named)
    return session.execute(text(named_sql), payload)


def read_sql_df(session: Session, sql: str, params: Tuple[Any, ...] | Mapping[str, Any] | None = None) -> pd.DataFrame:
    named_sql, named_params = _to_named_params(sql, params)
    # Reuse the operation's checked-out connection. Passing the Engine here
    # made every pandas read check out and pre-ping another pooled connection.
    return pd.read_sql_query(text(named_sql), session.connection(), params=named_params)


class CompatCursor:
    def __init__(self, session: Session):
        self._session = session
        self._result = None
        self.description = None

    def execute(self, sql: str, params: Tuple[Any, ...] | Mapping[str, Any] | None = None):
        self._result = execute(self._session, sql, params)
        self.description = [(c, None, None, None, None, None, None) for c in self._result.keys()]
        return self

    def executemany(self, sql: str, seq_params: Iterable[Tuple[Any, ...]] | Iterable[Mapping[str, Any]]):
        self._result = executemany(self._session, sql, seq_params)
        return self

    def fetchone(self):
        if self._result is None:
            return None
        return self._result.fetchone()

    def fetchmany(self, size: int | None = None):
        if self._result is None:
            return []
        return self._result.fetchmany(size)

    def fetchall(self):
        if self._result is None:
            return []
        return self._result.fetchall()

    def close(self):
        return None


class DbCompat:
    def __init__(self, session: Session):
        self._session = session
        self.session = session

    def cursor(self) -> CompatCursor:
        return CompatCursor(self._session)

    def execute(self, sql: str, params: Tuple[Any, ...] | Mapping[str, Any] | None = None):
        return execute(self._session, sql, params)

    def executemany(self, sql: str, seq_params: Iterable[Tuple[Any, ...]] | Iterable[Mapping[str, Any]]):
        return executemany(self._session, sql, seq_params)

    def commit(self):
        self._session.commit()

    def rollback(self):
        self._session.rollback()

    def close(self):
        self._session.close()

    @contextmanager
    def transaction(self):
        """Group legacy helper commits into one atomic transaction."""
        key = ManagedSession._DEFER_KEY
        depth = int(self._session.info.get(key, 0))
        self._session.info[key] = depth + 1
        try:
            yield self
        except Exception:
            self._session.info[key] = 0
            self._session.rollback()
            raise
        else:
            remaining = max(0, int(self._session.info.get(key, 1)) - 1)
            self._session.info[key] = remaining
            if remaining == 0:
                # Bypass ManagedSession.commit so the outer boundary performs
                # exactly one real database commit.
                Session.commit(self._session)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is not None:
            self._session.rollback()
        self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def get_bind(self):
        return self._session.get_bind()
