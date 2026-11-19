"""Export only Research's application tables into a verified SQLite snapshot.

The source is read in one read-only, repeatable-read PostgreSQL transaction.
Credentials remain in memory; the manifest contains only table counts/digests.
Run locally with --source-repo pointing at the current localhost application.
The destination must be outside the Git checkout and must not already exist.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import sys
import traceback

from sqlalchemy import Text, create_engine, inspect, select, text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from db_models import metadata

_PRIVATE_CONFIGURATION = re.compile(
    r"(?:postgres(?:ql)?(?:\+[a-z0-9_]+)?|mysql(?:\+[a-z0-9_]+)?|smtp[s]?)://"
    r"|(?:https?://)[^/\s:@]+:[^/\s@]+@"
    r"|(?:password|accountkey|client_secret|api_key)\s*="
    r"|\b(?:ghp_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}|sk-[A-Za-z0-9_-]{20,})",
    re.IGNORECASE,
)


def validate_public_values(values):
    if any(isinstance(value, str) and _PRIVATE_CONFIGURATION.search(value) for value in values):
        raise ValueError("Research source contains private configuration; review is required before publishing.")


def _canonical(value):
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value.isoformat(timespec="microseconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float):
        if math.isnan(value):
            return None  # SQLite represents NaN as NULL.
        if value == 0:
            return 0.0.hex()  # SQLite does not preserve a zero's IEEE sign bit.
        return value.hex()
    return value


def _digest_rows(rows, digest):
    for row in rows:
        validate_public_values(row)
        encoded = json.dumps([_canonical(v) for v in row], ensure_ascii=False, separators=(",", ":"))
        digest.update(encoded.encode("utf-8") + b"\n")


def _ordering(table, dialect):
    # Explicit byte order is shared by PostgreSQL and SQLite. A server's
    # locale collation must not cause a false value-digest mismatch.
    columns = list(table.primary_key.columns) or list(table.c)
    collation = "C" if dialect == "postgresql" else "BINARY"
    return [column.collate(collation) if isinstance(column.type, Text) else column for column in columns]


def export_snapshot(source_url: str, destination: Path) -> dict:
    destination = destination.resolve()
    repo_root = Path(__file__).resolve().parents[1]
    if destination.is_relative_to(repo_root):
        raise ValueError("The preview snapshot must be outside the Git checkout.")
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    destination.parent.chmod(0o700)
    # Reserve with private permissions before SQLite opens the file.
    descriptor = os.open(destination, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(descriptor)
    source_engine = create_engine(source_url, echo=False, hide_parameters=True)
    destination_engine = create_engine(f"sqlite:///{destination.as_posix()}", echo=False)
    manifest = {
        "format": 1,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_kind": source_engine.dialect.name,
        "source_fingerprint": hashlib.sha256(source_url.encode()).hexdigest(),
        "tables": {},
        "excluded_tables": [],
    }
    try:
        metadata.create_all(destination_engine)
        options = {}
        if source_engine.dialect.name == "postgresql":
            options = {"isolation_level": "REPEATABLE READ", "postgresql_readonly": True}
        elif source_engine.dialect.name != "sqlite":
            raise ValueError("Unsupported Research source database.")
        with source_engine.connect().execution_options(**options) as source, source.begin():
            if source_engine.dialect.name == "postgresql":
                if source.execute(text("SHOW transaction_read_only")).scalar_one() != "on":
                    raise ValueError("The source transaction must be read-only.")
            else:
                source.exec_driver_sql("PRAGMA query_only=ON")
            inspector = inspect(source)
            source_tables = set(inspector.get_table_names())
            missing = sorted(set(metadata.tables) - source_tables)
            if missing:
                raise ValueError("Research source is missing required tables: " + ", ".join(missing))
            source_columns = inspector.get_multi_columns(filter_names=sorted(metadata.tables))
            for table in metadata.sorted_tables:
                available = {column["name"] for column in source_columns[(None, table.name)]}
                if not set(table.c.keys()) <= available:
                    raise ValueError("Research source schema is incomplete for table " + table.name)
            # The allowlist is code-owned financial/Research metadata. It has
            # no account, auth, session, secret, SMTP or connection tables.
            for table_name in sorted(source_tables - set(metadata.tables)):
                quoted = source.dialect.identifier_preparer.quote(table_name)
                count = source.execute(text(f"SELECT COUNT(*) FROM {quoted}")).scalar_one()
                manifest["excluded_tables"].append({"table": table_name, "rows": int(count)})
            with destination_engine.begin() as target:
                for table in metadata.sorted_tables:
                    ordering = _ordering(table, source_engine.dialect.name)
                    statement = select(table).order_by(*ordering)
                    digest = hashlib.sha256()
                    count = 0
                    rows = source.execution_options(stream_results=True).execute(statement)
                    for batch in rows.partitions(1000):
                        _digest_rows(batch, digest)
                        payload = []
                        for row in batch:
                            values = dict(row._mapping)
                            for key, value in values.items():
                                if isinstance(value, datetime) and value.tzinfo is not None:
                                    values[key] = value.astimezone(timezone.utc).replace(tzinfo=None)
                            payload.append(values)
                        target.execute(table.insert(), payload)
                        count += len(batch)
                    manifest["tables"][table.name] = {"rows": count, "sha256": digest.hexdigest()}
        # Re-read the SQLite artifact, proving row count and every application
        # value match the same consistent source transaction.
        with destination_engine.connect() as target:
            for table in metadata.sorted_tables:
                digest = hashlib.sha256()
                ordering = _ordering(table, "sqlite")
                count = 0
                for batch in target.execute(select(table).order_by(*ordering)).partitions(1000):
                    _digest_rows(batch, digest)
                    count += len(batch)
                expected = manifest["tables"][table.name]
                if expected != {"rows": count, "sha256": digest.hexdigest()}:
                    raise ValueError("Snapshot reconciliation failed for table " + table.name)
            if target.exec_driver_sql("PRAGMA integrity_check").scalar_one() != "ok":
                raise ValueError("Snapshot integrity check failed.")
            if target.exec_driver_sql("PRAGMA foreign_key_check").first() is not None:
                raise ValueError("Snapshot foreign-key reconciliation failed.")
        manifest["snapshot_bytes"] = destination.stat().st_size
        manifest["verification"] = "passed"
        with destination.open("rb") as snapshot:
            manifest["snapshot_sha256"] = hashlib.file_digest(snapshot, "sha256").hexdigest()
        manifest_path = destination.with_suffix(".manifest.json")
        descriptor = os.open(manifest_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, "w") as stream:
            json.dump(manifest, stream, indent=2)
            stream.write("\n")
        return manifest
    except BaseException:
        destination_engine.dispose()
        if len(manifest["tables"]) == len(metadata.tables):
            # Retain a fully copied but unverified export privately for local
            # diagnostics. It is deliberately never named as a release file.
            quarantined = destination.with_suffix(".unverified")
            if not quarantined.exists():
                destination.rename(quarantined)
                manifest["verification"] = "failed"
                report = quarantined.with_suffix(".unverified.manifest.json")
                descriptor = os.open(report, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                with os.fdopen(descriptor, "w") as stream:
                    json.dump(manifest, stream, indent=2)
            else:
                destination.unlink(missing_ok=True)
        else:
            destination.unlink(missing_ok=True)
        raise
    finally:
        source_engine.dispose()
        destination_engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-repo", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    args = parser.parse_args()
    try:
        spec = importlib.util.spec_from_file_location("localhost_db_config", args.source_repo / "db_config.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        result = export_snapshot(module.get_db_url(), args.destination)
    except Exception as exc:
        # Driver exceptions can contain connection strings or record values.
        details = {"export": "failed", "error_type": type(exc).__name__}
        frame = traceback.extract_tb(exc.__traceback__)[-1]
        details["failure_location"] = f"{Path(frame.filename).name}:{frame.lineno}"
        if isinstance(exc, ValueError) and str(exc).startswith(("Research source ", "Snapshot reconciliation ", "Snapshot integrity ", "Snapshot foreign-key ")):
            details["validation"] = str(exc)
        print(json.dumps(details), file=sys.stderr)
        return 1
    print(json.dumps({
        "export": "verified",
        "tables": len(result["tables"]),
        "rows": sum(table["rows"] for table in result["tables"].values()),
        "companies": result["tables"]["companies"]["rows"],
        "snapshot_bytes": result["snapshot_bytes"],
        "excluded_tables": result["excluded_tables"],
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
