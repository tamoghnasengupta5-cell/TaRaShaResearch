import argparse
import os
import sys
from pathlib import Path

import sqlalchemy as sa

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db_models import metadata


def _env_or_fail(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"Missing required env var: {name}")
    return value


def _table_source_name(table_name: str) -> str:
    if table_name == "country_risk_premium":
        return "Country_Risk_Premium"
    return table_name


def _foreign_key_parent_values(src_conn: sa.Connection) -> dict[tuple[str, str], set[object]]:
    values: dict[tuple[str, str], set[object]] = {}
    for table in metadata.sorted_tables:
        for foreign_key in table.foreign_keys:
            parent = foreign_key.column
            key = (parent.table.name, parent.name)
            if key in values:
                continue
            source_table = _table_source_name(parent.table.name)
            rows = src_conn.execute(sa.text(f'SELECT "{parent.name}" FROM "{source_table}"'))
            values[key] = {row[0] for row in rows}
    return values


def copy_table(
    src_conn: sa.Connection,
    dst_conn: sa.Connection,
    table: sa.Table,
    batch_size: int,
    parent_values: dict[tuple[str, str], set[object]],
) -> tuple[int, int]:
    src_name = _table_source_name(table.name)
    total = 0
    skipped = 0
    foreign_keys = [
        (foreign_key.parent.name, (foreign_key.column.table.name, foreign_key.column.name))
        for foreign_key in table.foreign_keys
    ]

    result = src_conn.execute(sa.text(f"SELECT * FROM {src_name}"))
    while True:
        batch = result.mappings().fetchmany(batch_size)
        if not batch:
            break
        valid_batch = [
            row
            for row in batch
            if all(row[local_column] in parent_values[parent_key] for local_column, parent_key in foreign_keys)
        ]
        skipped += len(batch) - len(valid_batch)
        if valid_batch:
            dst_conn.execute(table.insert(), valid_batch)
            total += len(valid_batch)
    return total, skipped


def reset_postgres_sequences(dst_conn: sa.Connection) -> None:
    if dst_conn.dialect.name != "postgresql":
        return
    for table in metadata.sorted_tables:
        for column in table.primary_key.columns:
            if not isinstance(column.type, sa.Integer):
                continue
            sequence = dst_conn.execute(
                sa.text("SELECT pg_get_serial_sequence(:table_name, :column_name)"),
                {"table_name": table.name, "column_name": column.name},
            ).scalar_one_or_none()
            if not sequence:
                continue
            maximum = dst_conn.execute(sa.text(f'SELECT max("{column.name}") FROM "{table.name}"')).scalar_one_or_none()
            if maximum is None:
                dst_conn.execute(sa.text("SELECT setval(:sequence, 1, false)"), {"sequence": sequence})
            else:
                dst_conn.execute(sa.text("SELECT setval(:sequence, :maximum, true)"), {"sequence": sequence, "maximum": maximum})


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate SQLite data to Postgres.")
    parser.add_argument("--sqlite-path", default=os.environ.get("SQLITE_PATH", "app.db"))
    parser.add_argument("--postgres-url", default=os.environ.get("POSTGRES_URL"))
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--truncate", action="store_true", help="Truncate target tables before insert.")
    args = parser.parse_args()

    if not args.postgres_url:
        _env_or_fail("POSTGRES_URL")

    src_engine = sa.create_engine(f"sqlite:///{args.sqlite_path}")
    dst_engine = sa.create_engine(args.postgres_url)

    with src_engine.connect() as src_conn, dst_engine.begin() as dst_conn:
        parent_values = _foreign_key_parent_values(src_conn)
        if args.truncate:
            for table in reversed(metadata.sorted_tables):
                dst_conn.execute(sa.text(f"TRUNCATE TABLE {table.name} CASCADE;"))

        for table in metadata.sorted_tables:
            count, skipped = copy_table(src_conn, dst_conn, table, args.batch_size, parent_values)
            suffix = f"; skipped {skipped} orphan row(s)" if skipped else ""
            print(f"{table.name}: {count} rows{suffix}")
        reset_postgres_sequences(dst_conn)
        print("PostgreSQL identity sequences synchronized.")


if __name__ == "__main__":
    main()
