import unittest

import pandas as pd
from streamlit.testing.v1 import AppTest

from dashboard_category_filter import (
    _category_subcategory_options,
    resolve_subcategory_ids,
)


TAXONOMY_SQL = """
CREATE TABLE relative_valuation_categories (id INTEGER, name TEXT);
CREATE TABLE relative_valuation_subcategories (id INTEGER, category_id INTEGER, name TEXT);
CREATE TABLE relative_valuation_company_assignments (company_id INTEGER, subcategory_id INTEGER);
INSERT INTO relative_valuation_categories VALUES (1, 'Alpha'), (2, 'Beta');
INSERT INTO relative_valuation_subcategories VALUES
    (11, 1, 'Shared'), (12, 1, 'Software'), (21, 2, 'Shared'), (22, 2, 'Hardware');
INSERT INTO relative_valuation_company_assignments VALUES
    (101, 11), (102, 12), (102, 21), (103, 22), (101, 12);
"""


class CategoryResolutionTests(unittest.TestCase):
    def setUp(self):
        self.categories = pd.DataFrame([
            ('Alpha', 11, 'Shared'), ('Alpha', 12, 'Software'),
            ('Beta', 21, 'Shared'), ('Beta', 22, 'Hardware'),
        ], columns=['master_category', 'subcategory_id', 'subcategory'])

    def test_selection_matrix(self):
        cases = [
            ([], [], []),
            (['Alpha'], [], []),
            (['Alpha'], [11], [11]),
            (['Alpha', 'Beta'], [12, 21], [12, 21]),
            (['Alpha'], ['All - Alpha'], [11, 12]),
            (['Alpha', 'Beta'], ['All - Alpha', 'All - Beta'], [11, 12, 21, 22]),
            (['Alpha', 'Beta'], ['All - Alpha', 11, 22], [11, 12, 22]),
            (['Alpha'], ['All - Beta', 21, 11], [11]),
            ([], ['All - Alpha', 11], []),
        ]
        for categories, choices, expected in cases:
            with self.subTest(categories=categories, choices=choices):
                self.assertEqual(
                    resolve_subcategory_ids(self.categories, categories, choices), expected
                )

    def test_union_and_duplicate_names(self):
        options, labels, _ = _category_subcategory_options(self.categories, ['Alpha', 'Beta'])
        self.assertEqual(set(options), {'All - Alpha', 'All - Beta', 11, 12, 21, 22})
        self.assertEqual(labels[11], 'Alpha / Shared')
        self.assertEqual(labels[21], 'Beta / Shared')
        self.assertEqual(labels[12], 'Software')
        self.assertEqual(len(options), len(set(options)))


class CategoryWidgetTests(unittest.TestCase):
    def app(self):
        # Real Streamlit widgets and real SQL exercise reruns and mixed option types.
        source = f'''
import sqlite3
import streamlit as st
from dashboard_category_filter import select_category_company_ids
conn = sqlite3.connect(':memory:')
conn.executescript({TAXONOMY_SQL!r})
st.session_state['result'] = select_category_company_ids(
    conn, category_key='categories', subcategory_key='subcategories',
    subcategory_label='Sub-Categories')
conn.close()
'''
        return AppTest.from_string(source).run()

    def assert_result(self, app, expected):
        self.assertFalse(app.exception)
        self.assertFalse(app.warning)
        self.assertEqual(app.session_state['result'], expected)

    def test_widget_selection_and_category_removal(self):
        app = self.app()
        self.assert_result(app, [])
        self.assertEqual(app.multiselect(key='subcategories').options, [])
        app.multiselect(key='categories').set_value(['Alpha']).run()
        self.assert_result(app, [])
        app.multiselect(key='subcategories').set_value([11]).run()
        self.assert_result(app, [101])
        app.multiselect(key='subcategories').set_value(['All - Alpha', 11]).run()
        self.assert_result(app, [101, 102])
        app.multiselect(key='categories').set_value(['Alpha', 'Beta']).run()
        self.assert_result(app, [101, 102])
        app.multiselect(key='subcategories').set_value(['All - Alpha', 'All - Beta', 22]).run()
        self.assert_result(app, [101, 102, 103])
        app.multiselect(key='categories').set_value(['Beta']).run()
        self.assert_result(app, [102, 103])
        self.assertEqual(app.multiselect(key='subcategories').value, ['All - Beta', 22])
        app.multiselect(key='categories').set_value([]).run()
        self.assert_result(app, [])

    def test_individual_union(self):
        app = self.app()
        app.multiselect(key='categories').set_value(['Alpha', 'Beta']).run()
        app.multiselect(key='subcategories').set_value([12, 21]).run()
        self.assert_result(app, [101, 102])

    def test_empty_taxonomy(self):
        source = f'''
import sqlite3
import streamlit as st
from dashboard_category_filter import select_category_company_ids
conn = sqlite3.connect(':memory:')
conn.executescript({TAXONOMY_SQL!r})
conn.execute('DELETE FROM relative_valuation_subcategories')
st.session_state['result'] = select_category_company_ids(
    conn, category_key='categories', subcategory_key='subcategories',
    subcategory_label='Sub-Categories')
conn.close()
'''
        app = AppTest.from_string(source).run()
        self.assert_result(app, None)
        self.assertEqual(app.info[0].value, 'No categories found yet.')


if __name__ == '__main__':
    unittest.main()
