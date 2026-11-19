"""Recompute existing Research formulas on a verified private snapshot copy.

This utility never connects to the source PostgreSQL database. It attempts all
existing company formulas, relying on each formula's own missing-input contract.
No company or formula is skipped by this utility. The destination is never
considered publishable until every source-table invariant and integrity check
passes and a new manifest is written.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

FORMULAS = [
    "total_equity_and_roe", "debt_equity", "levered_beta", "cost_of_equity",
    "default_spread", "pre_tax_cost_of_debt", "wacc", "rd_spend_rate",
    "fcff_and_reinvestment_rate", "fcfe", "roic_wacc_spread",
]
DERIVED_TABLES = {
    "total_equity_annual", "average_equity_annual", "roe_annual",
    "debt_equity_annual", "levered_beta_annual", "cost_of_equity_annual",
    "default_spread_annual", "pre_tax_cost_of_debt_annual", "wacc_annual",
    "roic_wacc_spread_annual", "rd_spend_rate_annual", "fcff_annual",
    "reinvestment_rate_annual", "fcfe_annual",
}


def file_digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def private_json(path, value):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")
    path.chmod(0o600)


def compare_derived(source, destination, metadata):
    differences = {}
    with sqlite3.connect(destination.as_uri() + "?mode=ro", uri=True) as conn:
        conn.execute("ATTACH DATABASE ? AS original", (source.as_uri() + "?mode=ro&immutable=1",))
        for name in sorted(DERIVED_TABLES):
            table = metadata.tables[name]
            primary = [column.name for column in table.primary_key.columns]
            match = " AND ".join(f't."{key}"=s."{key}"' for key in primary)
            inserted = conn.execute(f'SELECT COUNT(*) FROM main."{name}" t WHERE NOT EXISTS (SELECT 1 FROM original."{name}" s WHERE {match})').fetchone()[0]
            removed = conn.execute(f'SELECT COUNT(*) FROM original."{name}" s WHERE NOT EXISTS (SELECT 1 FROM main."{name}" t WHERE {match})').fetchone()[0]
            exact = material = 0
            columns = list(table.c.keys())
            selected = ",".join([f's."{column}"' for column in columns] + [f't."{column}"' for column in columns])
            for row in conn.execute(f'SELECT {selected} FROM original."{name}" s JOIN main."{name}" t ON {match}'):
                old, new = row[:len(columns)], row[len(columns):]
                if old == new:
                    continue
                exact += 1
                for first, second in zip(old, new):
                    if first == second:
                        continue
                    if isinstance(first, (int, float)) and isinstance(second, (int, float)) and math.isclose(first, second, rel_tol=1e-9, abs_tol=1e-9):
                        continue
                    material += 1
                    break
            differences[name] = {"inserted": inserted, "removed": removed, "changed_exact": exact, "changed_beyond_1e_9_tolerance": material}
    return differences


def recompute(source, destination):
    source = source.resolve(strict=True)
    destination = destination.resolve()
    repo = Path(__file__).resolve().parents[1]
    if destination.is_relative_to(repo):
        raise ValueError("The recomputed snapshot must be outside Git.")
    manifest = json.loads(source.with_suffix(".manifest.json").read_text())
    original_digest = file_digest(source)
    if manifest.get("verification") != "passed" or original_digest != manifest["snapshot_sha256"]:
        raise ValueError("A verified source snapshot is required.")
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    destination.parent.chmod(0o700)
    descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.close(descriptor)
    shutil.copyfile(source, destination)
    os.environ["TARASHA_READ_ONLY"] = "0"
    os.environ["TARASHA_DB_URL"] = f"sqlite:///{destination.as_posix()}"
    os.environ["TARASHA_DB_PATH"] = str(destination)
    from sqlalchemy import event, select
    import core_backend as core
    from db_models import metadata
    from db_session import DbCompat, SessionLocal, get_engine
    from scripts.export_research_preview import _digest_rows, _ordering

    engine = get_engine()
    assert engine.dialect.name == "sqlite"
    assert Path(engine.url.database).resolve() == destination
    current = {"company_id": None, "formula": None}
    errors = []
    @event.listens_for(engine, "connect")
    def configure(connection, _record):
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
    @event.listens_for(engine, "handle_error")
    def record_error(context):
        errors.append(dict(current, error_type=type(context.original_exception).__name__))

    started = time.monotonic()
    completed = calls = 0
    report = {"verification": "in_progress", "original_snapshot_sha256": original_digest, "formulas": FORMULAS, "errors": errors, "externally_skipped_companies": 0, "externally_skipped_formulas": 0}
    report_path = destination.with_suffix(".recalculation.json")
    try:
        with engine.connect() as conn:
            company_ids = list(conn.exec_driver_sql("SELECT id FROM companies ORDER BY id").scalars())
        report["source_companies"] = len(company_ids)
        with DbCompat(SessionLocal()) as db:
            for company_id in company_ids:
                current["company_id"] = int(company_id)
                # Legacy helpers share this Session/connection. Defer their
                # individual commits into one existing atomic company boundary.
                with db.transaction():
                    for formula in FORMULAS:
                        current["formula"] = formula
                        try:
                            getattr(core, "compute_and_store_" + formula)(db, int(company_id))
                        except Exception as error:
                            errors.append(dict(current, error_type=type(error).__name__))
                            raise RuntimeError("A Research calculation failed; review the private report.") from None
                        calls += 1
                        if errors:
                            raise RuntimeError("A database calculation error requires review.")
                completed += 1
                if completed <= 100 and completed % 25 == 0 or completed % 250 == 0:
                    elapsed = time.monotonic() - started
                    progress = {"companies_completed": completed, "companies_total": len(company_ids), "formula_calls": calls, "elapsed_seconds": round(elapsed, 2), "estimated_remaining_seconds": round((len(company_ids) - completed) * elapsed / completed, 2)}
                    report.update(progress)
                    private_json(report_path, report)
                    print(json.dumps(progress), flush=True)
        engine.dispose()
        with sqlite3.connect(destination) as conn:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            conn.execute("PRAGMA journal_mode=DELETE")
            conn.execute("VACUUM")
            if conn.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise RuntimeError("Recomputed snapshot integrity check failed.")
            if conn.execute("PRAGMA foreign_key_check").fetchone() is not None:
                raise RuntimeError("Recomputed snapshot relationship check failed.")
        # Re-read every record, retaining source-table invariants while
        # recording new counts/digests for the explicitly approved caches.
        from sqlalchemy import create_engine
        verifier = create_engine(f"sqlite:///{destination}")
        new_tables = {}
        with verifier.connect() as conn:
            for table in metadata.sorted_tables:
                digest = hashlib.sha256()
                rows = 0
                for batch in conn.execute(select(table).order_by(*_ordering(table, "sqlite"))).partitions(1000):
                    _digest_rows(batch, digest)
                    rows += len(batch)
                result = {"rows": rows, "sha256": digest.hexdigest()}
                if table.name not in DERIVED_TABLES and result != manifest["tables"][table.name]:
                    raise RuntimeError("An unapproved source table changed: " + table.name)
                new_tables[table.name] = result
        verifier.dispose()
        changes = compare_derived(source, destination, metadata)
        if file_digest(source) != original_digest:
            raise RuntimeError("The original snapshot changed.")
        new_manifest = dict(manifest, tables=new_tables, snapshot_bytes=destination.stat().st_size, snapshot_sha256=file_digest(destination), original_snapshot_sha256=original_digest, recalculated_with_existing_formulas=FORMULAS, verification="passed")
        private_json(destination.with_suffix(".manifest.json"), new_manifest)
        report.update(verification="passed", companies_completed=completed, formula_calls=calls, elapsed_seconds=round(time.monotonic() - started, 2), estimated_remaining_seconds=0, table_changes=changes, new_snapshot_sha256=new_manifest["snapshot_sha256"], new_snapshot_bytes=new_manifest["snapshot_bytes"], original_snapshot_unchanged=True, source_tables_unchanged=True)
        private_json(report_path, report)
        print(json.dumps(report), flush=True)
        return 0
    except Exception as error:
        report.update(verification="failed", companies_completed=completed, formula_calls=calls, failure_type=type(error).__name__, elapsed_seconds=round(time.monotonic() - started, 2))
        private_json(report_path, report)
        print(json.dumps(report), flush=True)
        return 1
    finally:
        engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    arguments = parser.parse_args()
    raise SystemExit(recompute(arguments.source, arguments.destination))
