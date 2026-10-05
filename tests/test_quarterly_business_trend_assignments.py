import unittest
from unittest.mock import patch

import pandas as pd

import quarterly_business_trend as business_trend


class _FormContext:
    def __init__(self, streamlit):
        self.streamlit = streamlit

    def __enter__(self):
        self.streamlit.in_form = True

    def __exit__(self, exc_type, exc_value, traceback):
        self.streamlit.in_form = False


class _FakeStreamlit:
    def __init__(self):
        self.in_form = False
        self.selectbox_calls = []
        self.session_state = {}

    def selectbox(self, label, options, **kwargs):
        options = list(options)
        self.selectbox_calls.append((label, options, self.in_form, kwargs))
        if label == "Master Category":
            return "Beta"
        return options[0]

    def form(self, _key):
        return _FormContext(self)

    def multiselect(self, *_args, **_kwargs):
        return []

    def form_submit_button(self, *_args, **_kwargs):
        return False

    def info(self, *_args, **_kwargs):
        return None


class QuarterlyBusinessTrendAssignmentTests(unittest.TestCase):
    def test_master_category_is_reactive_and_filters_subcategories(self):
        fake_st = _FakeStreamlit()
        companies = pd.DataFrame([{"id": 10, "name": "Example", "ticker": "EX"}])
        categories = pd.DataFrame(
            [
                {
                    "master_category": "Alpha",
                    "subcategory_id": 1,
                    "subcategory": "Alpha One",
                },
                {
                    "master_category": "Beta",
                    "subcategory_id": 2,
                    "subcategory": "Beta One",
                },
            ]
        )

        with (
            patch.object(business_trend, "st", fake_st),
            patch.object(business_trend, "get_db", return_value=object()),
            patch.object(business_trend, "list_companies", return_value=companies),
            patch.object(
                business_trend,
                "get_relative_valuation_categories",
                return_value=categories,
            ),
            patch.object(
                business_trend,
                "get_relative_valuation_company_assignments",
                return_value=pd.DataFrame(),
            ),
        ):
            business_trend.render_quarterly_business_trend_company_assignment_tab()

        master_call, subcategory_call = fake_st.selectbox_calls
        self.assertFalse(master_call[2], "Master Category must be outside the form to rerun on change")
        self.assertIs(master_call[3]["on_change"], business_trend._reset_assignment_subcategory)
        self.assertTrue(subcategory_call[2])
        self.assertEqual(subcategory_call[1], [2])

    def test_master_category_change_clears_stale_subcategory(self):
        fake_st = _FakeStreamlit()
        fake_st.session_state["relative_valuation_assignment_subcategory"] = 1

        with patch.object(business_trend, "st", fake_st):
            business_trend._reset_assignment_subcategory()

        self.assertNotIn("relative_valuation_assignment_subcategory", fake_st.session_state)


if __name__ == "__main__":
    unittest.main()
