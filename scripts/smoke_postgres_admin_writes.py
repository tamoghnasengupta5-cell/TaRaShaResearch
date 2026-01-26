import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from db_session import DbCompat
from core_backend import (
    init_db,
    update_growth_weight_factors,
    update_stddev_weight_factors,
    upsert_risk_free_rate,
    upsert_index_annual_price_movement,
    update_implied_equity_risk_premium,
    replace_marginal_corporate_tax_rates,
    replace_industry_betas,
    read_df,
)


def main():
    db_url = os.environ.get("TARASHA_DB_URL")
    if not db_url:
        raise SystemExit("TARASHA_DB_URL is not set")

    engine = create_engine(db_url, future=True)
    SessionLocal = sessionmaker(bind=engine, future=True)
    session = SessionLocal()
    try:
        init_db(session)
        conn = DbCompat(session)

        growth_df = read_df("SELECT id FROM growth_weight_factors ORDER BY id", conn)
        if not growth_df.empty:
            first_id = int(growth_df.iloc[0]["id"])
            update_growth_weight_factors(conn, {first_id: 42.0})

        stddev_df = read_df("SELECT id FROM stddev_weight_factors ORDER BY id", conn)
        if not stddev_df.empty:
            first_id = int(stddev_df.iloc[0]["id"])
            update_stddev_weight_factors(conn, {first_id: 24.0})

        upsert_risk_free_rate(conn, 2099, 1.0, 2.0, 3.0, 4.0, "2099-01-01T00:00:00Z")
        upsert_index_annual_price_movement(conn, 2099, 5.0, 6.0, "2099-01-01T00:00:00Z")
        update_implied_equity_risk_premium(conn, 2099, 7.0, "2099-01-01T00:00:00Z")

        replace_marginal_corporate_tax_rates(
            conn,
            [("Testland", 12.34, "Smoke", "2099-01-01T00:00:00Z")],
        )
        replace_industry_betas(
            conn,
            [("SmokeBucket", "SmokeSector", 1.23, 1.11, "2099-01-01T00:00:00Z")],
        )

        print("ok")
    finally:
        session.close()


if __name__ == "__main__":
    main()
