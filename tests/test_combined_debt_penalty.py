import ast
import io
import math
from pathlib import Path
import random
import sqlite3
import statistics
import unittest

import pandas as pd
from streamlit.testing.v1 import AppTest

from core_backend import (
    COMBINED_DASHBOARD_METRICS,
    compute_annual_interest_penalty,
    compute_available_interest_penalty,
    compute_interest_penalty_stats,
    compute_median_interest_penalty,
    get_combined_dashboard_series_batch,
)


class InterestPenaltyTests(unittest.TestCase):
    def frames(self, incomes, expenses, years=None):
        years = years or list(range(2020, 2020 + len(incomes)))
        return (
            pd.DataFrame({"year": years, "operating_income": incomes}),
            pd.DataFrame({"year": years, "interest_expense": expenses}),
        )

    def test_expected_burdens_and_zero_boundaries(self):
        for income, expense, expected in (
            (100, 10, 1 / 11), (100, 50, 1 / 3), (100, 100, 0.5),
            (0, 10, 1), (-100, 10, 1), (100, 0, 0), (0, 0, 0), (-100, 0, 0),
            (1e308, 1e308, 0.5),
        ):
            with self.subTest(income=income, expense=expense):
                self.assertAlmostEqual(compute_annual_interest_penalty(income, expense), expected)

    def test_invalid_inputs_never_become_zero_burden(self):
        for invalid in (None, float("nan"), float("inf"), -float("inf"), "unknown", pd.NA):
            with self.subTest(invalid=invalid):
                self.assertIsNone(compute_annual_interest_penalty(invalid, 0))
                self.assertIsNone(compute_annual_interest_penalty(100, invalid))
        self.assertIsNone(compute_annual_interest_penalty(100, -1))

    def test_monotonicity_and_score_direction_for_both_score_signs(self):
        rng = random.Random(42)
        for _ in range(500):
            income = rng.uniform(-1000, 1000)
            expense = rng.uniform(0, 1000)
            original = compute_annual_interest_penalty(income, expense)
            for worse in (
                compute_annual_interest_penalty(income, expense + rng.random() * 1000),
                compute_annual_interest_penalty(income - rng.random() * 1000, expense),
            ):
                self.assertGreaterEqual(worse, original)
                for score in (-10.0, 0.0, 10.0):
                    for strength in (0.1, 1.0, 5.0):
                        self.assertLessEqual(score - strength * worse, score - strength * original)

    def test_median_uses_annual_ratios_and_only_selected_years(self):
        incomes, expenses = self.frames([100, 10, -1, None], [10, 100, 1, None])
        actual = compute_median_interest_penalty(incomes, expenses, 2022, 2020)
        self.assertAlmostEqual(actual, 100 / 110)
        self.assertEqual(actual, compute_median_interest_penalty(incomes, expenses, 2020, 2022))
        self.assertNotAlmostEqual(actual, 10 / (10 + 10))  # Ratio of medians is different.

    def test_missing_invalid_duplicate_or_mismatched_years_are_unavailable(self):
        incomes, expenses = self.frames([100, 100, 100], [10, 10, 10])
        for bad_income, bad_expense in (
            (incomes.drop(index=1), expenses),
            (incomes, expenses.drop(index=1)),
            (incomes, expenses.assign(interest_expense=[10, None, 10])),
            (incomes.assign(operating_income=[100, None, 100]), expenses.assign(interest_expense=0)),
            (incomes, expenses.assign(interest_expense=[10, -10, 10])),
            (incomes, expenses.assign(interest_expense=[10, math.inf, 10])),
            (pd.concat([incomes, incomes.iloc[[0]]]), expenses),
            (incomes, expenses.assign(year=[2020, 2022, 2023])),
            (None, expenses),
            (incomes, pd.DataFrame()),
        ):
            with self.subTest(incomes=bad_income, expenses=bad_expense):
                self.assertIsNone(compute_median_interest_penalty(bad_income, bad_expense, 2022, 2020))
        self.assertIsNone(compute_median_interest_penalty(incomes, expenses, 2023, 2020))

    def test_median_retains_monotonicity_for_even_and_odd_histories(self):
        for length in (3, 4):
            incomes, expenses = self.frames([100] * length, [10 * (i + 1) for i in range(length)])
            original = compute_median_interest_penalty(incomes, expenses, 2019 + length, 2020)
            for index in range(length):
                higher = expenses.copy()
                higher.loc[index, "interest_expense"] += 100
                lower = incomes.copy()
                lower.loc[index, "operating_income"] = -10
                self.assertGreaterEqual(compute_median_interest_penalty(incomes, higher, 2019 + length, 2020), original)
                self.assertGreaterEqual(compute_median_interest_penalty(lower, expenses, 2019 + length, 2020), original)

    def test_interest_penalty_dispersion_respects_sample_population_and_units(self):
        incomes, expenses = self.frames([100, 100, -100], [0, 100, 100])
        median, sample = compute_interest_penalty_stats(incomes, expenses, 2022, 2020)
        _, population = compute_interest_penalty_stats(incomes, expenses, 2022, 2020, stdev_sample=False)
        self.assertEqual(median, 0.5)
        self.assertEqual(sample, 0.5)
        self.assertAlmostEqual(population, math.sqrt(1 / 6))
        self.assertEqual(compute_interest_penalty_stats(incomes, expenses.iloc[:2], 2022, 2020), (None, None))

    def test_kore_regression_from_reviewed_2020_2025_inputs(self):
        incomes, expenses = self.frames(
            [-9.541, -15.559, -85.372, -118.761, -102.793, -24.974],
            [23.493, 23.26, 31.835, 43.232, 52.516, 53.037],
        )
        penalty = compute_median_interest_penalty(incomes, expenses, 2025, 2020)
        self.assertEqual(penalty, 1.0)
        unadjusted_total = -0.8581763001066853
        self.assertAlmostEqual(unadjusted_total - penalty, -1.8581763001066853)

    def test_available_years_match_same_year_and_do_not_fill_missing_inputs(self):
        incomes, expenses = self.frames([100, 100, 100, 100], [10, 20, 30, 40])
        original_income, original_expense = incomes.copy(), expenses.copy()
        partial_expense = expenses.drop(index=1)
        median, count = compute_available_interest_penalty(incomes, partial_expense, 2026, 2020)
        self.assertAlmostEqual(median, 30 / 130)
        self.assertEqual(count, 3)
        self.assertEqual((median, count), compute_available_interest_penalty(incomes, partial_expense, 2020, 2026))
        self.assertEqual(compute_available_interest_penalty(incomes, expenses, 2022, 2021), ((20 / 120 + 30 / 130) / 2, 2))
        pd.testing.assert_frame_equal(incomes, original_income)
        pd.testing.assert_frame_equal(expenses, original_expense)

    def test_available_years_exclude_invalid_and_duplicate_years(self):
        incomes, expenses = self.frames([100] * 3, [10, 20, 30])
        expected = ((10 / 110 + 30 / 130) / 2, 2)
        for invalid in (None, math.nan, math.inf, -math.inf, -10, "unknown"):
            with self.subTest(invalid=invalid):
                bad_expenses = self.frames([100] * 3, [10, invalid, 30])[1]
                self.assertEqual(compute_available_interest_penalty(incomes, bad_expenses, 2026, 2020), expected)
        duplicate_income = pd.concat([incomes, incomes.iloc[[1]]])
        duplicate_expense = pd.concat([expenses, expenses.iloc[[1]]])
        for bad_income, bad_expense in ((duplicate_income, expenses), (incomes, duplicate_expense)):
            self.assertEqual(compute_available_interest_penalty(bad_income, bad_expense, 2026, 2020), expected)
        missing_income = incomes.assign(operating_income=[100, None, 100])
        zero_expenses = expenses.assign(interest_expense=0)
        self.assertEqual(compute_available_interest_penalty(missing_income, zero_expenses, 2026, 2020), (0.0, 2))

    def test_available_years_with_no_usable_pair_remain_unavailable(self):
        incomes, expenses = self.frames([None, None], [0, 0])
        for income, expense in (
            (incomes, expenses), (None, expenses), (incomes, pd.DataFrame()),
            (incomes.assign(operating_income=100), expenses.assign(year=[2018, 2019])),
            (incomes.rename(columns={"operating_income": "other"}), expenses),
        ):
            self.assertEqual(compute_available_interest_penalty(income, expense, 2026, 2020), (None, 0))
        self.assertEqual(compute_available_interest_penalty(incomes.assign(operating_income=-100), expenses, 2021, 2021), (0.0, 1))

    def test_available_years_preserve_complete_history_penalty(self):
        for length in (1, 2, 3, 4):
            incomes, expenses = self.frames([100] * length, [10 * (i + 1) for i in range(length)])
            penalty, count = compute_available_interest_penalty(incomes, expenses, 2019 + length, 2020)
            self.assertEqual(penalty, compute_median_interest_penalty(incomes, expenses, 2019 + length, 2020))
            self.assertEqual(count, length)
            for index in range(length):
                higher = expenses.copy()
                higher.loc[index, "interest_expense"] += 100
                lower = incomes.copy()
                lower.loc[index, "operating_income"] = -10
                self.assertGreaterEqual(compute_available_interest_penalty(incomes, higher, 2026, 2020)[0], penalty)
                self.assertGreaterEqual(compute_available_interest_penalty(lower, expenses, 2026, 2020)[0], penalty)

    def test_kpel_styrenix_reviewed_inputs_use_two_of_seven_years(self):
        for ticker, operating_income, interest_expense, expected in (
            ("KPEL", [1651.266, 2998.868], [249.334, 517.375], 0.13916278651250016),
            ("STYRENIX", [3024.4, 3471.7], [0.651166, 192.9], 0.026427008937514943),
        ):
            with self.subTest(ticker=ticker):
                incomes, expenses = self.frames(operating_income, interest_expense, years=[2025, 2026])
                median, count = compute_available_interest_penalty(incomes, expenses, 2026, 2020)
                self.assertAlmostEqual(median, expected)
                self.assertEqual(f"{count}/{2026 - 2020 + 1}", "2/7")

    def test_batch_loads_raw_inputs_in_one_read_without_changing_existing_series(self):
        with sqlite3.connect(":memory:") as conn:
            for table, column in COMBINED_DASHBOARD_METRICS.values():
                conn.execute(f"CREATE TABLE {table} (company_id INTEGER, fiscal_year INTEGER, {column} REAL)")
            for table, values in (
                ("revenues_annual", [(1, 2020, 100), (1, 2021, 110)]),
                ("operating_income_annual", [(1, 2020, -10), (1, 2021, 20)]),
                ("interest_expense_annual", [(1, 2020, 5), (1, 2021, 0)]),
            ):
                conn.executemany(f"INSERT INTO {table} VALUES (?, ?, ?)", values)
            statements = []
            conn.set_trace_callback(statements.append)
            series = get_combined_dashboard_series_batch(conn, [1, 1, 2])
            self.assertEqual(len(statements), 1)
            self.assertEqual(series[1]["operating_income"]["operating_income"].tolist(), [-10, 20])
            self.assertEqual(series[1]["interest_expense"]["interest_expense"].tolist(), [5, 0])
            self.assertEqual(series[1]["revenue"]["revenue"].tolist(), [100, 110])
            self.assertTrue(series[2]["interest_expense"].empty)


# Exercise the actual Streamlit control flow and exports without opening the
# application database or invoking schema/data initialization.
DASHBOARD_APP = '''
import copy
from unittest.mock import patch
import pandas as pd
import streamlit as st
import combined_dashboard as dashboard

companies = pd.DataFrame([
    {"id": 1, "name": "Example", "ticker": "EX", "country": "USA"},
])
values = {
    "accumulated_profit": [100, 110, 125, 140],
    "roe": [.10, .11, .09, .12], "roce": [.15, .16, .14, .18],
    "interest_load_pct": [-100, -100, -100, -100],
    "operating_income": [100, 100, 100, 100], "interest_expense": [10, 20, 30, 40],
    "revenue": [100, 110, 121, 133.1], "pretax_income": [10, 12, 13, 15],
    "net_income": [8, 9, 10, 12], "nopat": [9, 10, 12, 14],
    "margin": [.20, .21, .19, .23], "fcff": [5, 7, 6, 9],
    "spread_pct": [3, 4, 2, 5], "price_change": [.1, -.1, .2, .3],
}
series = {1: {key: pd.DataFrame({"year": [2020, 2021, 2022, 2023], key: vals}) for key, vals in values.items()}}
if st.session_state.get("omit_interest_year"):
    series[1]["interest_expense"] = series[1]["interest_expense"].iloc[[0, 2, 3]]
if st.session_state.get("newer_raw_inputs"):
    for key in ("operating_income", "interest_expense"):
        series[1][key] = pd.concat([series[1][key], pd.DataFrame({"year": [2024], key: [10]})])
if st.session_state.get("no_valid_interest_year"):
    series[1]["interest_expense"]["interest_expense"] = None
if st.session_state.get("two_year_ticker"):
    ticker = st.session_state["two_year_ticker"]
    companies.loc[0, "ticker"] = ticker
    for key, frame in series[1].items():
        series[1][key] = frame.iloc[-2:].assign(year=[2025, 2026])
    incomes, expenses = {
        "KPEL": ([1651.266, 2998.868], [249.334, 517.375]),
        "STYRENIX": ([3024.4, 3471.7], [0.651166, 192.9]),
    }[ticker]
    series[1]["operating_income"]["operating_income"] = incomes
    series[1]["interest_expense"]["interest_expense"] = expenses
factors = ["Accumulated Equity Growth", "ROE", "ROCE", "Revenue Growth", "Pretax Income Growth", "Net Income Growth", "NOPAT Growth", "Operating Margin", "YoY Operating Margin Growth", "FCFF Growth", "Spread"]
weights = pd.DataFrame({"factor": factors, "weight": [1.0] * len(factors)})
def read(sql, conn, params=None):
    if "weight_factors" in sql:
        return weights
    if "company_group_members" in sql:
        return pd.DataFrame({"company_id": [1, 1]})
    if "company_groups" in sql:
        return pd.DataFrame({"id": [1], "name": ["Example Bucket"]})
    raise AssertionError(sql)
def taxonomy(conn, frame):
    frame = frame.drop(columns=["_company_id"]).copy()
    frame["Category"] = "Example Category"
    frame["Sub-Category"] = "Example Sub-Category"
    return frame
real_export = dashboard._combined_dashboard_excel_bytes
def capture_export(overall, breakup):
    st.session_state["overall_result"] = overall.copy()
    st.session_state["breakup_result"] = breakup.copy()
    payload = real_export(overall, breakup)
    st.session_state["excel_result"] = payload
    return payload
with patch.multiple(dashboard,
    get_db=lambda: None, list_companies=lambda conn: companies,
    read_df=read,
    get_combined_dashboard_series_batch=lambda conn, ids: copy.deepcopy(series),
    get_company_valuation_industry_bucket_map=lambda conn, ids: {1: "Example Bucket"},
    add_company_taxonomy_columns=taxonomy,
    select_category_company_ids=lambda *args, **kwargs: [1],
    _combined_dashboard_excel_bytes=capture_export):
    dashboard.render_combined_dashboard_tab()
'''


class CombinedDashboardDebtTests(unittest.TestCase):
    total = "Total Debt-Adjusted Scaled Volatility-Adjusted Score"
    debt_bs = "Debt-Adjusted Balance Sheet Strength Score"
    scaled = "Total Scaled Volatility-Adjusted Score"
    coverage = "Included Years for Calculation"

    def app(self, mode="Company", **state):
        app = AppTest.from_string(DASHBOARD_APP).run()
        for key, value in state.items():
            app.session_state[key] = value
        app.radio(key="combined_score_mode").set_value(mode).run()
        if mode == "Company":
            app.multiselect(key="combined_score_company_select").set_value([1]).run()
        elif mode == "Industry Bucket":
            app.multiselect(key="combined_score_bucket_select").set_value(["Example Bucket"]).run()
        app.button(key="compute_overall_scores").click().run()
        self.assertFalse(app.exception)
        return app

    def test_lambda_default_and_all_selection_modes_agree(self):
        reference = None
        for mode in ("Company", "Industry Bucket", "Category / Sub-Category"):
            with self.subTest(mode=mode):
                app = self.app(mode)
                self.assertEqual(app.number_input(key="combined_debt_penalty_lambda").value, 1.0)
                self.assertFalse(app.warning)
                overall = app.session_state["overall_result"]
                breakup = app.session_state["breakup_result"]
                expected_penalty = (20 / 120 + 30 / 130) / 2
                self.assertAlmostEqual(overall[self.total].iloc[0], overall[self.scaled].iloc[0] - expected_penalty)
                self.assertEqual(overall[self.total].iloc[0], breakup[self.total].iloc[0])
                self.assertEqual(breakup[self.coverage].iloc[0], "4/4")
                self.assertNotIn(self.coverage, overall.columns)
                if reference is not None:
                    pd.testing.assert_frame_equal(overall, reference)
                reference = overall
                book = pd.read_excel(io.BytesIO(app.session_state["excel_result"]), sheet_name=None)
                self.assertEqual(list(book), ["Overall Score Dashboard", "Overall Score Dashboard Breakup"])
                for sheet in book.values():
                    self.assertAlmostEqual(sheet["Debt-Adj. Total Score"].iloc[0], overall[self.total].iloc[0])
                self.assertEqual(book["Overall Score Dashboard Breakup"][self.coverage].iloc[0], "4/4")

    def test_changing_lambda_changes_only_debt_columns_and_exports(self):
        app = self.app()
        before = app.session_state["overall_result"].copy()
        before_breakup = app.session_state["breakup_result"].copy()
        app.number_input(key="combined_debt_penalty_lambda").set_value(2.0).run()
        app.button(key="compute_overall_scores").click().run()
        self.assertFalse(app.exception)
        after = app.session_state["overall_result"]
        pd.testing.assert_frame_equal(before.drop(columns=[self.total, self.debt_bs]), after.drop(columns=[self.total, self.debt_bs]))
        pd.testing.assert_frame_equal(before_breakup.drop(columns=[self.total]), app.session_state["breakup_result"].drop(columns=[self.total]))
        self.assertAlmostEqual(before[self.total].iloc[0] - after[self.total].iloc[0], (20 / 120 + 30 / 130) / 2)
        book = pd.read_excel(io.BytesIO(app.session_state["excel_result"]), sheet_name=None)
        for sheet in book.values():
            self.assertAlmostEqual(sheet["Debt-Adj. Total Score"].iloc[0], after[self.total].iloc[0])

    def test_missing_year_does_not_suppress_other_scores_or_become_zero_penalty(self):
        app = self.app()
        before = app.session_state["overall_result"].copy()
        app.session_state["omit_interest_year"] = True
        app.button(key="compute_overall_scores").click().run()
        self.assertFalse(app.exception)
        after = app.session_state["overall_result"]
        self.assertAlmostEqual(after[self.total].iloc[0], after[self.scaled].iloc[0] - 30 / 130)
        self.assertEqual(app.session_state["breakup_result"][self.coverage].iloc[0], "3/4")
        self.assertFalse(app.warning)
        pd.testing.assert_frame_equal(before.drop(columns=[self.total, self.debt_bs]), after.drop(columns=[self.total, self.debt_bs]))

    def test_no_valid_year_keeps_score_unavailable_and_other_scores_unchanged(self):
        app = self.app()
        before = app.session_state["overall_result"].copy()
        app.session_state["no_valid_interest_year"] = True
        app.button(key="compute_overall_scores").click().run()
        self.assertFalse(app.exception)
        after = app.session_state["overall_result"]
        self.assertTrue(pd.isna(after[self.total].iloc[0]))
        self.assertTrue(pd.isna(after[self.debt_bs].iloc[0]))
        self.assertEqual(app.session_state["breakup_result"][self.coverage].iloc[0], "0/4")
        self.assertIn("no year in the selected", app.warning[0].value)
        pd.testing.assert_frame_equal(before.drop(columns=[self.total, self.debt_bs]), after.drop(columns=[self.total, self.debt_bs]))

    def test_two_year_examples_show_two_of_seven_in_all_modes_and_export(self):
        for ticker, expected in (("KPEL", 0.13916278651250016), ("STYRENIX", 0.026427008937514943)):
            for mode in ("Company", "Industry Bucket", "Category / Sub-Category"):
                with self.subTest(ticker=ticker, mode=mode):
                    app = self.app(mode, two_year_ticker=ticker)
                    self.assertFalse(app.warning)
                    overall = app.session_state["overall_result"]
                    breakup = app.session_state["breakup_result"]
                    self.assertAlmostEqual(overall[self.total].iloc[0], overall[self.scaled].iloc[0] - expected)
                    self.assertEqual(breakup[self.coverage].iloc[0], "2/7")
                    self.assertEqual(breakup.columns.get_loc(self.coverage), breakup.columns.get_loc(self.total) + 1)
                    self.assertEqual(app.dataframe[1].value[self.coverage].iloc[0], "2/7")
                    book = pd.read_excel(io.BytesIO(app.session_state["excel_result"]), sheet_name=None)
                    self.assertEqual(book["Overall Score Dashboard Breakup"][self.coverage].iloc[0], "2/7")
                    self.assertAlmostEqual(book["Overall Score Dashboard"]["Debt-Adj. Total Score"].iloc[0], overall[self.total].iloc[0])

    def test_new_raw_inputs_do_not_extend_existing_recent_range(self):
        app = self.app()
        before = app.session_state["overall_result"].copy()
        app.session_state["newer_raw_inputs"] = True
        app.button(key="compute_overall_scores").click().run()
        self.assertFalse(app.exception)
        pd.testing.assert_frame_equal(before, app.session_state["overall_result"])

    def test_explicit_range_and_population_mode_use_same_debt_penalty(self):
        app = self.app()
        app.text_input(key="combined_year_range").set_value("2022-2021").run()
        app.radio(key="combined_stdev_mode").set_value("Population (ddof=0)").run()
        app.button(key="compute_overall_scores").click().run()
        self.assertFalse(app.exception)
        overall = app.session_state["overall_result"]
        self.assertAlmostEqual(overall[self.scaled].iloc[0] - overall[self.total].iloc[0], (20 / 120 + 30 / 130) / 2)
        self.assertEqual(app.session_state["breakup_result"][self.coverage].iloc[0], "2/2")
        app.text_input(key="combined_year_range").set_value("2021-2022").run()
        app.button(key="compute_overall_scores").click().run()
        self.assertFalse(app.exception)
        pd.testing.assert_frame_equal(overall, app.session_state["overall_result"])
        self.assertEqual(app.session_state["breakup_result"][self.coverage].iloc[0], "2/2")


BALANCE_SHEET_APP = DASHBOARD_APP.split("with patch.multiple(dashboard,")[0] + '''
import sqlite3
import bs_metrics as balance_sheet
if st.session_state.get("zero_expense"):
    series[1]["interest_expense"]["interest_expense"] = 0.0
if st.session_state.get("loss_income"):
    series[1]["operating_income"]["operating_income"] = -10.0
conn = sqlite3.connect(':memory:')
for metric in ("operating_income", "interest_expense"):
    frame = series[1][metric].rename(columns={"year": "fiscal_year"}).assign(company_id=1)
    frame.to_sql(metric + '_annual', conn, index=False)
conn.execute('PRAGMA query_only=ON')
def read_bs(sql, connection, params=None):
    if "operating_income_annual" in sql:
        return pd.read_sql_query(sql, connection, params=params)
    if sql.strip() == "SELECT DISTINCT company_id FROM company_group_members":
        return pd.DataFrame(columns=["company_id"])
    return read(sql, connection, params)
def extra(column, vals):
    return pd.DataFrame({"year": [2020, 2021, 2022, 2023], column: vals})
with patch.multiple(balance_sheet,
    get_db=lambda: conn, list_companies=lambda connection: companies,
    read_df=read_bs,
    compute_and_store_total_equity_and_roe=lambda *args: None,
    get_annual_accumulated_profit_series=lambda *args: series[1]["accumulated_profit"],
    get_annual_roe_series=lambda *args: series[1]["roe"],
    get_annual_roce_series=lambda *args: series[1]["roce"],
    get_annual_interest_load_series=lambda *args: series[1]["interest_load_pct"],
    get_annual_non_cash_working_capital_series=lambda *args: extra('non_cash_working_capital', [10, 11, 12, 14]),
    get_annual_revenue_yield_non_cash_working_capital_series=lambda *args: extra('revenue_yield_ncwc', [.1, .2, .3, .4]),
    get_annual_interest_coverage_series=lambda *args: extra('interest_coverage_ratio', [2, 3, 4, 5])):
    balance_sheet.render_balance_sheet_metrics_tab()
conn.close()
'''


class StandaloneBalanceSheetDebtTests(unittest.TestCase):
    debt = "Debt-Adjusted Balance Sheet Strength Score"
    scaled = "Scaled Volatility-Adjusted Balance Sheet Strength Score"
    interest_columns = ["Median Interest Load %", "Interest Load Standard Deviation"]

    def app(self):
        app = AppTest.from_string(BALANCE_SHEET_APP).run()
        app.multiselect(key="bs_score_bucket_select").set_value(["Example Bucket"]).run()
        app.button(key="compute_bs_scores").click().run()
        self.assertFalse(app.exception)
        return app

    def test_corrected_percentage_statistics_and_score_match_combined_dashboard(self):
        app = self.app()
        self.assertFalse(app.warning)
        self.assertEqual(app.number_input(key="bs_score_debt_penalty_lambda").value, 1.0)
        penalties = [1 / 11, 1 / 6, 3 / 13, 2 / 7]
        metrics, scores = [table.value for table in app.dataframe]
        self.assertEqual(metrics[self.interest_columns[0]].iloc[0], f"{statistics.median(penalties) * 100:.2f}%")
        self.assertEqual(metrics[self.interest_columns[1]].iloc[0], f"{statistics.stdev(penalties) * 100:.2f}%")
        self.assertEqual(metrics["Median Interest Coverage Ratio"].iloc[0], "3.50")
        combined = AppTest.from_string(DASHBOARD_APP).run()
        combined.multiselect(key="combined_score_company_select").set_value([1]).run()
        combined.button(key="compute_overall_scores").click().run()
        self.assertFalse(combined.exception)
        self.assertEqual(scores[self.debt].iloc[0], f"{combined.session_state['overall_result'][self.debt].iloc[0]:.2f}")

    def test_lambda_changes_only_standalone_debt_score(self):
        app = self.app()
        metrics, scores = [table.value.copy() for table in app.dataframe]
        app.number_input(key="bs_score_debt_penalty_lambda").set_value(2.0).run()
        app.button(key="compute_bs_scores").click().run()
        self.assertFalse(app.exception)
        pd.testing.assert_frame_equal(metrics, app.dataframe[0].value)
        pd.testing.assert_frame_equal(scores.drop(columns=[self.debt]), app.dataframe[1].value.drop(columns=[self.debt]))
        self.assertAlmostEqual(float(scores[self.debt].iloc[0]) - float(app.dataframe[1].value[self.debt].iloc[0]), 0.20, places=2)

    def test_missing_interest_year_keeps_other_metrics_and_scores(self):
        app = self.app()
        metrics, scores = [table.value.copy() for table in app.dataframe]
        app.session_state["omit_interest_year"] = True
        app.button(key="compute_bs_scores").click().run()
        self.assertFalse(app.exception)
        self.assertEqual(len(app.warning), 2)
        self.assertEqual(app.dataframe[0].value[self.interest_columns[0]].iloc[0], "—")
        self.assertEqual(app.dataframe[0].value[self.interest_columns[1]].iloc[0], "—")
        self.assertEqual(app.dataframe[1].value[self.debt].iloc[0], "—")
        pd.testing.assert_frame_equal(metrics.drop(columns=self.interest_columns), app.dataframe[0].value.drop(columns=self.interest_columns))
        pd.testing.assert_frame_equal(scores.drop(columns=[self.debt]), app.dataframe[1].value.drop(columns=[self.debt]))

    def test_losses_and_reported_zero_expense_have_bounded_penalties(self):
        app = self.app()
        app.session_state["loss_income"] = True
        app.button(key="compute_bs_scores").click().run()
        self.assertFalse(app.exception)
        self.assertEqual(app.dataframe[0].value[self.interest_columns[0]].iloc[0], "100.00%")
        self.assertEqual(app.dataframe[0].value[self.interest_columns[1]].iloc[0], "0.00%")
        score = app.dataframe[1].value.iloc[0]
        self.assertAlmostEqual(float(score[self.scaled]) - float(score[self.debt]), 1.0, places=2)
        app.session_state["zero_expense"] = True
        app.button(key="compute_bs_scores").click().run()
        self.assertFalse(app.exception)
        self.assertEqual(app.dataframe[0].value[self.interest_columns[0]].iloc[0], "0.00%")
        score = app.dataframe[1].value.iloc[0]
        self.assertEqual(score[self.scaled], score[self.debt])

    def test_standalone_population_and_explicit_year_selection(self):
        app = self.app()
        app.text_input(key="bs_year_range_input").set_value("2022-2021").run()
        app.radio(key="bs_stdev_mode").set_value("Population (ddof=0)").run()
        self.assertFalse(app.exception)
        metrics = app.dataframe[0].value
        penalties = [1 / 6, 3 / 13]
        self.assertEqual(metrics[self.interest_columns[0]].iloc[0], f"{statistics.median(penalties) * 100:.2f}%")
        self.assertEqual(metrics[self.interest_columns[1]].iloc[0], f"{statistics.pstdev(penalties) * 100:.2f}%")


class AdminDebtFormulaTests(unittest.TestCase):
    def test_actual_formula_tab_renders_updated_and_standalone_formulas(self):
        import admin

        tree = ast.parse(Path(admin.__file__).read_text(encoding="utf-8-sig"))
        formula_tab = next(
            node for node in ast.walk(tree)
            if isinstance(node, ast.With)
            and any(isinstance(item.context_expr, ast.Name) and item.context_expr.id == "tab_formula" for item in node.items)
        )
        # Execute only the real read-only Formula tab; Admin's other tabs have writes.
        source = (
            "import streamlit as st\nimport pandas as pd\nfrom core_backend import normalize_score_weight_factors\nconn = None\n"
            "def read_df(*args, **kwargs):\n"
            "    return pd.DataFrame({'factor': ['ROE'], 'weight': [20.0]})\n"
            + ast.unparse(ast.Module(body=formula_tab.body, type_ignores=[]))
        )
        app = AppTest.from_string(source).run()
        self.assertFalse(app.exception)
        rendered = "\n".join(element.value for element in app.markdown)
        self.assertIn("Scaled Balance Sheet Score - Lambda * Median Annual Interest Penalty", rendered)
        self.assertIn("Lambda defaults to 1", rendered)
        self.assertIn("Both inputs are required for every year", rendered)
        self.assertIn("available valid years within the selected balance-sheet year range", rendered)
        self.assertIn("Included Years for Calculation", rendered)
        self.assertIn("2/7", rendered)
        self.assertIn("Both dashboards use Debt-Adjusted Balance Sheet Score", rendered)
        self.assertIn("Median Annual Interest Penalty * 100", rendered)
        self.assertNotIn("Scaled Balance Sheet Score / (1 + Median Interest Load % / 100)", rendered)


if __name__ == "__main__":
    unittest.main()
