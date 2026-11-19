import hashlib
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from sqlalchemy import create_engine

from db_models import metadata, companies, revenues_annual
from scripts.export_research_preview import _canonical, export_snapshot, validate_public_values


class ReadOnlyPreviewTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="research preview ")
        self.path = Path(self.temp.name) / "snapshot data.db"
        engine = create_engine(f"sqlite:///{self.path}")
        metadata.create_all(engine)
        with engine.begin() as conn:
            conn.execute(companies.insert(), {"id": 1, "name": "Preview Co", "ticker": "PRE", "country": "USA"})
            conn.execute(revenues_annual.insert(), {"company_id": 1, "fiscal_year": 2025, "revenue": 123.5})
        engine.dispose()
        self.digest = hashlib.sha256(self.path.read_bytes()).hexdigest()

    def tearDown(self):
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), self.digest)
        self.temp.cleanup()

    def run_isolated(self, code, **overrides):
        env = dict(os.environ, TARASHA_READ_ONLY="1", TARASHA_RESEARCH_SNAPSHOT=str(self.path))
        # Prove a configured writer URL cannot supersede a preview snapshot.
        env["TARASHA_DB_URL"] = "postgresql://must-not-connect.invalid/private"
        env.update(overrides)
        result = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr[-6000:])

    def test_snapshot_is_required_without_writer_or_keychain_fallback(self):
        self.run_isolated('''
from unittest.mock import patch
from db_config import get_db_url
from preview_mode import ReadOnlyPreviewError
with patch("db_config.subprocess.run", side_effect=AssertionError("must not use Keychain")):
    try:
        get_db_url()
    except ReadOnlyPreviewError:
        pass
    else:
        raise AssertionError("missing preview snapshot must fail closed")
''', TARASHA_RESEARCH_SNAPSHOT="")

    def test_relative_snapshot_resolves_in_the_extracted_application_directory(self):
        self.run_isolated(f'''
import os
os.chdir({self.temp.name!r})
from db_config import get_sqlite_path
from db_session import get_engine
assert get_sqlite_path().name == "snapshot data.db"
with get_engine().connect() as connection:
    assert connection.exec_driver_sql("SELECT revenue FROM revenues_annual").scalar_one() == 123.5
''', TARASHA_RESEARCH_SNAPSHOT="snapshot data.db", PYTHONPATH=str(Path(__file__).resolve().parents[1]))

    def test_database_layers_allow_reads_and_reject_sql_orm_and_schema_writes(self):
        self.run_isolated('''
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from db_orm import Companies
from db_session import get_engine, SessionLocal
from core_backend import get_conn, init_db, compute_and_store_total_equity_and_roe
from core_shared import get_db
from preview_mode import ReadOnlyPreviewError
engine = get_engine()
with engine.connect() as conn:
    assert conn.execute(text("SELECT revenue FROM revenues_annual")).scalar_one() == 123.5
    for statement in ["INSERT INTO companies(name,ticker,country) VALUES('X','X','USA')",
                      "UPDATE revenues_annual SET revenue=0", "DELETE FROM companies",
                      "CREATE TABLE forbidden(id INTEGER)"]:
        try:
            conn.execute(text(statement))
        except OperationalError:
            conn.rollback()
        else:
            raise AssertionError("write was accepted")
session = SessionLocal()
session.add(Companies(name="Forbidden", ticker="NO", country="USA"))
try:
    session.commit()
except ReadOnlyPreviewError:
    session.rollback()
else:
    raise AssertionError("ORM write was accepted")
try:
    init_db(session)
except ReadOnlyPreviewError:
    pass
else:
    raise AssertionError("schema initialization was accepted")
session.close()
db = get_db()
assert db.execute("SELECT COUNT(*) FROM companies").scalar_one() == 1
compute_and_store_total_equity_and_roe(db, 1)
db.commit()
db.close()
raw = get_conn("/must/not/create/a/writable/fallback.db")
assert raw.execute("SELECT COUNT(*) FROM companies").fetchone()[0] == 1
raw.execute("PRAGMA query_only=OFF")
try:
    raw.execute("UPDATE companies SET name='Forbidden'")
except Exception as error:
    assert "readonly" in str(error).lower()
else:
    raise AssertionError("immutable URI accepted a write")
raw.close()
''')

    def test_navigation_hides_persistent_edits_and_direct_admin_calls_are_guarded(self):
        self.run_isolated('''
from unittest.mock import patch
from streamlit.testing.v1 import AppTest
app = AppTest.from_file("app.py", default_timeout=25).run()
assert not app.exception
app.radio(key="app_top_nav_active").set_value("Equity Research").run()
assert not app.exception
assert "Data Upload" not in app.radio(key="equity_research_primary_active").options
assert "Admin" not in app.radio(key="value_creation_tabs_active").options
assert app.button(key="save_bucket_button").disabled
app.radio(key="equity_research_primary_active").set_value("Quarterly Business Trend Score").run()
assert not app.exception
assert "Categories" not in app.radio(key="quarterly_business_trend_tabs_active").options
app.radio(key="quarterly_business_trend_tabs_active").set_value("Formula").run()
assert not app.exception
assert app.button(key="quarterly_business_trend_save_weights").disabled
app.radio(key="equity_research_primary_active").set_value("Through-the-Cycle Efficiency Score").run()
assert not app.exception
assert "Admin" not in app.radio(key="ttc_tabs_active").options
app.radio(key="app_top_nav_active").set_value("Valuations").run()
assert not app.exception
assert "Settings" not in app.radio(key="dcf_primary_tabs_active").options
import admin, data_upload, quarterly_business_trend, ttc_efficiency, dcf_valuation
for module, name in [(admin, "render_admin_tab"), (data_upload, "render_data_upload_tab"),
                     (quarterly_business_trend, "render_quarterly_business_trend_categories_tab"),
                     (ttc_efficiency, "render_through_the_cycle_assumptions_tab"),
                     (dcf_valuation, "_render_settings_tab")]:
    with patch.object(module, "get_db", side_effect=AssertionError("must not access editor database")):
        with patch.object(module.st, "info"):
            getattr(module, name)(None) if name == "_render_settings_tab" else getattr(module, name)()
''')

    def test_export_reconciles_financial_rows_and_excludes_account_tokens(self):
        self.assertEqual(_canonical(-0.0), _canonical(0.0))
        with self.assertRaisesRegex(ValueError, "private configuration"):
            validate_public_values(["postgresql://private-fixture.invalid/database"])
        source_engine = create_engine(f"sqlite:///{self.path}")
        with source_engine.begin() as conn:
            conn.exec_driver_sql("CREATE TABLE account_sessions(token TEXT)")
            conn.exec_driver_sql("INSERT INTO account_sessions VALUES ('excluded-private-fixture')")
        source_engine.dispose()
        self.digest = hashlib.sha256(self.path.read_bytes()).hexdigest()
        target = Path(self.temp.name) / "export" / "preview.db"
        manifest = export_snapshot(f"sqlite:///{self.path}", target)
        self.assertEqual(manifest["tables"]["companies"]["rows"], 1)
        self.assertEqual(manifest["tables"]["revenues_annual"]["rows"], 1)
        self.assertEqual(manifest["excluded_tables"], [{"table": "account_sessions", "rows": 1}])
        self.assertEqual(target.stat().st_mode & 0o777, 0o600)
        self.assertEqual(target.parent.stat().st_mode & 0o777, 0o700)
        self.assertNotIn(b"excluded-private-fixture", target.read_bytes())
        with self.assertRaises(FileExistsError):
            export_snapshot(f"sqlite:///{self.path}", target)


if __name__ == "__main__":
    unittest.main()
