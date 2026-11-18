import io
import sqlite3
import unittest
from unittest.mock import patch

import pandas as pd
from streamlit.proto.WidgetStates_pb2 import WidgetState
from streamlit.runtime.memory_uploaded_file_manager import MemoryUploadedFileManager
from streamlit.runtime.uploaded_file_manager import UploadedFileRec
from streamlit.testing.v1 import AppTest
from streamlit.testing.v1 import element_tree

from dashboard_spreadsheet_filter import parse_ticker_spreadsheet, resolve_ticker_rows


CATALOG = pd.DataFrame([
    (1, "Unique Ltd", "ONE", "India"),
    (2, "US Shared", "SHARED", "USA"),
    (3, "Indian Shared", "SHARED", "India"),
    (4, "India A", "SAME", "India"),
    (5, "India B", "SAME", "India"),
    (6, "Numeric", "000123", "China"),
    (7, "Class B", "BRK.B", "USA"),
    (8, "NA symbol", "NA", "USA"),
], columns=["id", "name", "ticker", "country"])


def csv_rows(text):
    return parse_ticker_spreadsheet(text.encode(), "list.csv")


class TickerParsingTests(unittest.TestCase):
    def test_excel_format_preserves_text_and_numeric_tickers_with_optional_country(self):
        for columns in [
            {"Ticker": ["000123", 526433, "NA", "BRK.B"]},
            {"Ticker": ["000123", 526433, "NA", "BRK.B"], "Country": ["CN", "India", "", "US"]},
        ]:
            output = io.BytesIO()
            pd.DataFrame(columns).to_excel(output, index=False, engine="openpyxl")
            rows = parse_ticker_spreadsheet(output.getvalue(), "Ticker_List.xlsx")
            self.assertEqual(rows["Ticker"].tolist(), ["000123", "526433", "NA", "BRK.B"])
            if "Country" not in columns:
                self.assertEqual(rows["Country"].tolist(), [""] * 4)

    def test_optional_country_headers_and_identifiers_are_preserved(self):
        rows = csv_rows("\ufeff tIcKeR , CoUnTrY \n one , in \n000123,CN\nbrk.b,us\nNA,\n\n")
        self.assertEqual(rows["Ticker"].tolist(), ["ONE", "000123", "BRK.B", "NA"])
        self.assertEqual(rows["Country"].tolist(), ["India", "China", "USA", ""])
        self.assertEqual(rows["Row"].tolist(), [2, 3, 4, 5])
        self.assertEqual(csv_rows("Ticker\nONE\n")["Country"].tolist(), [""])

    def test_invalid_missing_empty_or_duplicate_headers_and_rows_are_rejected(self):
        for text in ["Country\nUSA\n", "Ticker,Ticker\nONE,TWO\n", "Ticker,Country,Country\nONE,US,IN\n", "Ticker\n", "Ticker,Country\n,USA\n", ""]:
            with self.subTest(text=text), self.assertRaises(ValueError):
                csv_rows(text)
        with self.assertRaisesRegex(ValueError, "valid"):
            parse_ticker_spreadsheet(b"not a workbook", "bad.xlsx")
        with self.assertRaisesRegex(ValueError, "Excel"):
            parse_ticker_spreadsheet(b"Ticker\nONE", "bad.xls")

    def test_country_is_only_requested_for_real_cross_country_conflicts(self):
        unique = resolve_ticker_rows(csv_rows("Ticker\nONE\nNA\n"), CATALOG)
        self.assertEqual(unique.company_ids, [1, 8])
        self.assertFalse(unique.country_conflicts)
        self.assertFalse(unique.company_conflicts)
        conflict = resolve_ticker_rows(csv_rows("Ticker\nSHARED\n"), CATALOG)
        self.assertEqual(conflict.company_ids, [])
        self.assertEqual(conflict.country_conflicts, {("SHARED", ""): ["India", "USA"]})
        resolved = resolve_ticker_rows(csv_rows("Ticker\nSHARED\n"), CATALOG, country_choices={("SHARED", ""): "India"})
        self.assertEqual(resolved.company_ids, [3])

    def test_supplied_country_is_honored_and_never_falls_back_to_another_country(self):
        result = resolve_ticker_rows(csv_rows("Ticker,Country\nSHARED,us\nONE,USA\n"), CATALOG)
        self.assertEqual(result.company_ids, [2])
        self.assertEqual(result.unmatched_count, 1)
        self.assertFalse(result.country_conflicts)

    def test_same_country_duplicates_need_company_selection_and_stale_choices_are_rejected(self):
        rows = csv_rows("Ticker,Country\nSAME,India\n")
        result = resolve_ticker_rows(rows, CATALOG)
        self.assertFalse(result.country_conflicts)
        self.assertEqual(len(result.company_conflicts[("SAME", "india")]), 2)
        self.assertEqual(resolve_ticker_rows(rows, CATALOG, company_choices={("SAME", "india"): 5}).company_ids, [5])
        self.assertEqual(resolve_ticker_rows(rows, CATALOG, company_choices={("SAME", "india"): 1}).company_ids, [])

    def test_deduplication_uses_company_identity_and_preserves_distinct_countries(self):
        rows = csv_rows("Ticker,Country\none,\nONE,IN\nSHARED,US\nSHARED,India\nMISSING,\n")
        result = resolve_ticker_rows(rows, CATALOG)
        self.assertEqual(result.company_ids, [1, 2, 3])
        self.assertEqual(result.preview["Status"].tolist(), ["Matched", "Duplicate — ignored", "Matched", "Matched", "Ticker not found"])
        self.assertEqual(result.unmatched_count, 1)


class UploadAppTestCase(unittest.TestCase):
    """Exercise the real Streamlit uploader serde and file manager in AppTest.

    Streamlit 1.50 exposes uploaders as UnknownElement, so supply their native
    widget protobuf; application code and uploaded-file parsing are not mocked.
    """

    def setUp(self):
        self.manager = MemoryUploadedFileManager("/mock/upload")
        self.manager_patch = patch(
            "streamlit.testing.v1.local_script_runner.MemoryUploadedFileManager",
            return_value=self.manager,
        )
        self.manager_patch.start()
        self.addCleanup(self.manager_patch.stop)
        original_widget_state = element_tree.get_widget_state

        def widget_state(node):
            if getattr(node, "type", None) != "file_uploader":
                return original_widget_state(node)
            state = WidgetState(id=node.proto.id)
            uploaded = node.value
            if uploaded is not None:
                state.file_uploader_state_value.uploaded_file_info.add(
                    file_id=uploaded.file_id, name=uploaded.name, size=uploaded.size,
                )
            return state

        self.widget_patch = patch.object(element_tree, "get_widget_state", side_effect=widget_state)
        self.widget_patch.start()
        self.addCleanup(self.widget_patch.stop)

    def upload(self, app, data, filename="list.csv"):
        file_id = str(len(self.manager.file_storage.get("test session id", {})) + 1)
        self.manager.add_file("test session id", UploadedFileRec(file_id, filename, "application/octet-stream", data))
        uploader = app.get("file_uploader")[0]
        state = WidgetState(id=uploader.proto.id)
        state.file_uploader_state_value.uploaded_file_info.add(file_id=file_id, name=filename, size=len(data))
        states = app._tree.get_widget_states()
        remaining = [widget for widget in states.widgets if widget.id != uploader.proto.id]
        del states.widgets[:]
        states.widgets.extend([*remaining, state])
        app._run(states)
        self.assertFalse(app.exception)
        return app


WIDGET_APP = '''
import sqlite3
import streamlit as st
from dashboard_spreadsheet_filter import select_spreadsheet_company_ids
conn = sqlite3.connect(':memory:')
conn.executescript("""
CREATE TABLE companies (id INTEGER, name TEXT, ticker TEXT, country TEXT);
INSERT INTO companies VALUES
 (1, 'Unique Ltd', 'ONE', 'India'), (2, 'US Shared', 'SHARED', 'USA'),
 (3, 'Indian Shared', 'SHARED', 'India'), (4, 'India A', 'SAME', 'India'),
 (5, 'India B', 'SAME', 'India');
""")
st.session_state['result'] = select_spreadsheet_company_ids(conn, key='test_upload', result_state_key='computed')
conn.close()
'''


class TickerUploadWidgetTests(UploadAppTestCase):
    def app(self):
        app = AppTest.from_string(WIDGET_APP).run()
        self.assertFalse(app.exception)
        return app

    def test_real_upload_country_conflict_rerun_and_file_replacement(self):
        app = self.app()
        self.upload(app, b"Ticker\nSHARED\n")
        self.assertEqual(app.session_state["result"], [])
        self.assertEqual(len(app.selectbox), 1)
        app.selectbox[0].set_value("India").run()
        self.assertFalse(app.exception)
        self.assertEqual(app.session_state["result"], [3])
        app.session_state["computed"] = True
        app.selectbox[0].set_value("USA").run()
        self.assertEqual(app.session_state["result"], [2])
        self.assertNotIn("computed", app.session_state)
        self.upload(app, b"Ticker\nONE\n")
        self.assertEqual(app.session_state["result"], [1])
        self.assertFalse(app.selectbox)
        self.upload(app, b"Ticker\nSHARED\n")
        self.assertEqual(app.session_state["result"], [])

    def test_provided_country_needs_no_prompt_and_same_country_needs_company_only(self):
        app = self.app()
        self.upload(app, b"Ticker,Country\nSHARED,IN\n")
        self.assertEqual(app.session_state["result"], [3])
        self.assertFalse(app.selectbox)
        self.upload(app, b"Ticker,Country\nSAME,India\n")
        self.assertEqual(len(app.selectbox), 1)
        self.assertEqual(app.selectbox[0].label, "Company for SAME")
        app.selectbox[0].set_value(5).run()
        self.assertEqual(app.session_state["result"], [5])

    def test_unmatched_rows_need_acknowledgement_and_invalid_replacement_hides_result(self):
        app = self.app()
        self.upload(app, b"Ticker\nONE\none\nMISSING\n")
        self.assertEqual(app.session_state["result"], [])
        self.assertEqual(len(app.checkbox), 1)
        app.checkbox[0].check().run()
        self.assertEqual(app.session_state["result"], [1])
        app.session_state["computed"] = True
        self.upload(app, b"Country\nIndia\n")
        self.assertEqual(app.session_state["result"], [])
        self.assertTrue(app.error)
        self.assertNotIn("computed", app.session_state)

    def test_unmatched_tickers_and_supplied_countries_are_visible_outside_preview(self):
        app = self.app()
        self.upload(app, b"Ticker,Country\nONE,India\nUNKNOWN,US\nONE,USA\nMISSING,\n")
        self.assertEqual(app.session_state["result"], [])
        self.assertEqual(len(app.dataframe), 2)
        expected = pd.DataFrame({
            "Row": [3, 4, 5], "Ticker": ["UNKNOWN", "ONE", "MISSING"],
            "Country": ["USA", "USA", "Not provided"],
            "Status": ["Ticker / Country not found", "Ticker / Country not found", "Ticker not found"],
        })
        pd.testing.assert_frame_equal(app.dataframe[1].value.reset_index(drop=True), expected)
        self.assertTrue(any(element.value == "**Unmatched rows**" for element in app.markdown))
        app.checkbox[0].check().run()
        self.assertEqual(app.session_state["result"], [1])
        pd.testing.assert_frame_equal(app.dataframe[1].value.reset_index(drop=True), expected)

    def test_unmatched_table_is_shown_when_all_rows_fail_or_other_conflicts_remain(self):
        app = self.app()
        for data in [b"Ticker,Country\nMISSING,India\n", b"Ticker,Country\nMISSING,India\nSHARED,\n"]:
            with self.subTest(data=data):
                self.upload(app, data)
                self.assertEqual(app.session_state["result"], [])
                self.assertEqual(len(app.dataframe), 2)
                self.assertEqual(app.dataframe[1].value["Ticker"].tolist(), ["MISSING"])
                self.assertEqual(app.dataframe[1].value["Country"].tolist(), ["India"])

    def test_unmatched_table_is_absent_for_a_fully_matched_list(self):
        app = self.app()
        self.upload(app, b"Ticker\nONE\n")
        self.assertEqual(app.session_state["result"], [1])
        self.assertEqual(len(app.dataframe), 1)
        self.assertFalse(any(element.value == "**Unmatched rows**" for element in app.markdown))


if __name__ == "__main__":
    unittest.main()
