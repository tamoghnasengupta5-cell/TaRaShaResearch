import argparse
import os

import sqlalchemy as sa

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


def copy_table(src_conn: sa.Connection, dst_conn: sa.Connection, table: sa.Table, batch_size: int) -> int:
    src_name = _table_source_name(table.name)
    total = 0

    result = src_conn.execute(sa.text(f"SELECT * FROM {src_name}"))
    while True:
        batch = result.mappings().fetchmany(batch_size)
        if not batch:
            break
        dst_conn.execute(table.insert(), list(batch))
        total += len(batch)
    return total


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
        if args.truncate:
            for table in reversed(metadata.sorted_tables):
                dst_conn.execute(sa.text(f"TRUNCATE TABLE {table.name} CASCADE;"))

        for table in metadata.sorted_tables:
            count = copy_table(src_conn, dst_conn, table, args.batch_size)
            print(f"{table.name}: {count} rows")


if __name__ == "__main__":
    main()
