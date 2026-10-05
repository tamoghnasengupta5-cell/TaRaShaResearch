import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import db_models
from db_orm import Companies
from db_session import DbCompat
from core_backend import (
    init_db,
    get_company_group_id,
    add_company_group_members,
    remove_company_group_members,
    delete_company_group,
    read_df,
)


def main():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    SessionLocal = sessionmaker(bind=engine, future=True)
    session = SessionLocal()
    try:
        session.execute(text("PRAGMA foreign_keys=ON"))
        init_db(session)

        session.add_all([
            Companies(name="TestCo A", ticker="TCA"),
            Companies(name="TestCo B", ticker="TCB"),
        ])
        session.commit()

        conn = DbCompat(session)

        gid = get_company_group_id(conn, "SmokeBucket", create=True)
        if gid is None:
            raise RuntimeError("Failed to create group")

        add_company_group_members(conn, gid, [1, 2])
        members = read_df(
            "SELECT group_id, company_id FROM company_group_members ORDER BY company_id",
            conn,
        )
        print("members_after_add", members.to_dict(orient="records"))

        remove_company_group_members(conn, gid, [1])
        members = read_df(
            "SELECT group_id, company_id FROM company_group_members ORDER BY company_id",
            conn,
        )
        print("members_after_remove", members.to_dict(orient="records"))

        delete_company_group(conn, gid)
        groups = read_df("SELECT id, name FROM company_groups", conn)
        members = read_df("SELECT group_id, company_id FROM company_group_members", conn)
        print("groups_after_delete", groups.to_dict(orient="records"))
        print("members_after_delete", members.to_dict(orient="records"))
    finally:
        session.close()


if __name__ == "__main__":
    main()
