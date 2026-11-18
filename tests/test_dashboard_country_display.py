import io
import sqlite3
import unittest

import pandas as pd
from streamlit.testing.v1 import AppTest

from combined_dashboard import _add_dashboard_countries, _combined_dashboard_excel_bytes
from test_combined_debt_penalty import DASHBOARD_APP


class OverallDashboardCountryTests(unittest.TestCase):
    def test_country_uses_company_id_when_tickers_repeat_without_changing_scores(self):
        with sqlite3.connect(":memory:") as conn:
            conn.executescript("""
                CREATE TABLE companies(id INTEGER PRIMARY KEY, name TEXT, ticker TEXT, country TEXT);
                INSERT INTO companies VALUES
                (1,'US company','SHARED','USA'), (2,'Indian company','SHARED','India'),
                (3,'No country','MISSING',NULL);
            """)
            companies = pd.read_sql_query("SELECT id,name,ticker FROM companies", conn)
            before = pd.DataFrame({"_company_id": [2, 3, 1], "Ticker": ["SHARED", "MISSING", "SHARED"], "Score": [90., 80., 70.]}, index=[5, 3, 9])
            result = _add_dashboard_countries(conn, before, companies)
            self.assertEqual(result["Country"].tolist(), ["India", "", "USA"])
            pd.testing.assert_frame_equal(result.drop(columns=["Country"]), before)
            self.assertNotIn("Country", before)
            exported = pd.read_excel(io.BytesIO(_combined_dashboard_excel_bytes(result.drop(columns=["_company_id"]))))
            self.assertEqual(exported["Country"].fillna("").tolist(), ["India", "", "USA"])

    def test_display_and_export_include_country_in_all_existing_selection_modes(self):
        for mode in ["Company", "Industry Bucket", "Category / Sub-Category"]:
            with self.subTest(mode=mode):
                app = AppTest.from_string(DASHBOARD_APP).run()
                app.radio(key="combined_score_mode").set_value(mode).run()
                if mode == "Company":
                    app.multiselect(key="combined_score_company_select").set_value([1]).run()
                elif mode == "Industry Bucket":
                    app.multiselect(key="combined_score_bucket_select").set_value(["Example Bucket"]).run()
                app.button(key="compute_overall_scores").click().run()
                self.assertFalse(app.exception)
                frame = app.session_state["overall_result"]
                self.assertEqual(frame["Country"].tolist(), ["USA"])
                self.assertEqual(list(frame.columns[:4]), ["Company Name", "Ticker", "Country", "Latest Uploaded Date"])
                self.assertEqual(app.dataframe[0].value["Country"].tolist(), ["USA"])
                exported = pd.read_excel(io.BytesIO(app.session_state["excel_result"]))
                self.assertEqual(exported["Country"].tolist(), ["USA"])


if __name__ == "__main__":
    unittest.main()
