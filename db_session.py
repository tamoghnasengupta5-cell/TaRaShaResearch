from __future__ import annotations

from contextlib import contextmanager
from functools import lru_cache
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
    if is_sqlite_url(url):
        connect_args = {"check_same_thread": False}
    return create_engine(url, future=True, pool_pre_ping=True, connect_args=connect_args)


SessionLocal = sessionmaker(bind=get_engine(), autoflush=False, autocommit=False, expire_on_commit=False, future=True)


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
    return pd.read_sql_query(text(named_sql), session.get_bind(), params=named_params)


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

    def get_bind(self):
        return self._session.get_bind()
