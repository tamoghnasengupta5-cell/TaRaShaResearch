"""Window generation, exact score agreement, missing data and selectable UI scenarios."""
import copy
import math
import unittest

import numpy as np
import pandas as pd
from streamlit.testing.v1 import AppTest

from score_range_dashboard import (
    COVERAGE, DEBT_TOTAL, ScoreWindow, build_score_range_frame, company_chart_frame,
    company_score_chart, component_recent_years, score_windows, selected_company_row,
)
from test_scaled_volatility_score import APP_TEMPLATE, FIXTURE


class ScoreRangeMathTests(unittest.TestCase):
    def test_recent_columns_follow_selected_range_and_latest_fiscal_year(self):
        for first in (2018, 2020, 2022, 2025, 2026):
            with self.subTest(first=first):
                windows = score_windows(f"Recent - {first}", 2026)
                self.assertEqual([w.label for w in windows], [f"Recent - {y}" for y in range(first, 2027)])
                self.assertEqual([w.first_year for w in windows], list(range(first, 2027)))
        self.assertEqual(score_windows("Recent - 2027", 2026), [])
        self.assertEqual(score_windows("Recent - 2020", None), [])
        self.assertEqual(score_windows("  rEcEnT – 2025  ", 2026), score_windows("Recent - 2025", 2026))
        self.assertEqual(score_windows("Recent - 2020", 2025)[-1].label, "Recent - 2025")

    def test_explicit_and_reversed_ranges_preserve_the_selected_upper_bound(self):
        expected = [f"2023 - {y}" for y in range(2018, 2024)]
        self.assertEqual([w.label for w in score_windows("2023-2018", 2026)], expected)
        self.assertEqual([w.label for w in score_windows("2018-2023", 2026)], expected)
        with self.assertRaises(ValueError):
            score_windows("invalid", 2026)

    def test_recent_uses_existing_scoring_sources_not_newer_raw_inputs_or_prices(self):
        def frame(key, year):
            return pd.DataFrame({"year": [year], key: [1.0]})
        series = {key: frame(key, year) for key, year in (
            ("roe", 2024), ("accumulated_profit", 2023), ("roce", 2025),
            ("revenue", 2023), ("margin", 2026), ("fcff", 2024), ("spread_pct", 2026),
            ("operating_income", 2027), ("interest_expense", 2027), ("price_change", 2028),
        )}
        self.assertEqual(component_recent_years(series), {"BS": 2025, "PL": 2023, "FS": 2026})
        self.assertEqual(component_recent_years({}), {"BS": None, "PL": None, "FS": None})

    def test_older_companies_do_not_get_reversed_future_scores_and_base_is_reused(self):
        rows = [{"_company_id": cid, "Company Name": "Duplicate", "Ticker": "DUP"} for cid in (1, 2)]
        series = {cid: {name: pd.DataFrame({"year": [latest], name: [1.]})
                        for name in ("roe", "revenue", "spread_pct")} for cid, latest in ((1, 2023), (2, 2026))}
        base = {cid: {"scores": {DEBT_TOTAL: value}, "breakup": {COVERAGE: "2/7"}}
                for cid, value in ((1, 0.0), (2, -5.0))}
        calls = []
        def calculate(cid, window):
            calls.append((cid, window))
            return {"scores": {DEBT_TOTAL: float(cid)}, "breakup": {COVERAGE: "1/2"}}
        result, windows, coverage = build_score_range_frame(rows, series, "Recent - 2020", base, calculate)
        self.assertEqual(result["_company_id"].tolist(), [1, 2])
        self.assertEqual(result["Recent - 2020"].tolist(), [0.0, -5.0])
        self.assertNotIn((1, "Recent - 2020"), calls)
        self.assertTrue(result.loc[0, [f"Recent - {y}" for y in (2024, 2025, 2026)]].isna().all())
        self.assertFalse(any(cid == 1 and int(window[-4:]) > 2023 for cid, window in calls))
        self.assertEqual(coverage[1]["Recent - 2024"], None)
        self.assertEqual(len(windows), 7)

    def test_trend_uses_actual_year_coordinates_and_keeps_missing_and_zero_scores_distinct(self):
        windows = [ScoreWindow(f"Recent - {y}", y) for y in range(2020, 2025)]
        row = pd.Series(dict(zip([w.label for w in windows], [-4., -2., None, 2., 4.])))
        chart = company_chart_frame(row, windows, {})
        self.assertTrue(pd.isna(chart.loc[2, "Debt-Adj. Total Score"]))
        self.assertTrue(pd.isna(chart.loc[2, "Trend"]))
        self.assertEqual(company_score_chart(chart, [w.label for w in windows]).to_dict()["layer"][0]["encoding"]["x"]["scale"]["domain"], [w.label for w in windows])
        spec = company_score_chart(chart, [w.label for w in windows]).to_dict()
        self.assertEqual(spec["layer"][0]["encoding"]["y"]["field"], "Score")
        self.assertEqual(spec["layer"][1]["encoding"]["text"]["field"], "Score")
        np.testing.assert_allclose(chart["Trend"].dropna(), [-4., -2., 2., 4.], atol=1e-12)
        reverse = company_chart_frame(row, list(reversed(windows)), {})
        np.testing.assert_allclose(reverse["Trend"].dropna(), [4., 2., -2., -4.], atol=1e-12)
        row[:] = 0.0
        np.testing.assert_array_equal(company_chart_frame(row, windows, {})["Debt-Adj. Total Score"], np.zeros(5))

    def test_no_fabricated_trend_for_singleton_or_unavailable_scores(self):
        windows = [ScoreWindow("Recent - 2026", 2026)]
        for value in (None, math.inf, -math.inf, math.nan, 0., -12.):
            chart = company_chart_frame(pd.Series({windows[0].label: value}), windows, {})
            self.assertTrue(chart["Trend"].isna().all())
            self.assertEqual(len(company_score_chart(chart, [windows[0].label]).to_dict()["layer"]), 2)
        row = pd.Series({"Recent - 2020": 1e308, "Recent - 2021": -1e308})
        chart = company_chart_frame(row, [ScoreWindow(k, int(k[-4:])) for k in row.index], {})
        self.assertTrue(np.isfinite(chart["Trend"]).all())

    def test_selection_maps_positions_to_company_identity_with_duplicate_labels(self):
        frame = pd.DataFrame({"_company_id": [42, 17], "Company": ["Same", "Same"], "Ticker": ["DUP", "DUP"]})
        self.assertEqual(selected_company_row(frame, [1])["_company_id"], 17)
        for invalid in ([], [-1], [2]):
            self.assertIsNone(selected_company_row(frame, invalid))


CAPTURE = r'''
from types import SimpleNamespace
real_dataframe = st.dataframe
real_altair_chart = st.altair_chart
real_loader = dashboard.get_combined_dashboard_series_batch
st.session_state.pop("chart_spec", None)
def capture_frame(data, *args, **kwargs):
    event = real_dataframe(data, *args, **kwargs)
    if str(kwargs.get("key", "")).startswith("combined_score_range_table_"):
        st.session_state["range_table"] = data.copy()
        st.session_state["range_table_config"] = kwargs
        return SimpleNamespace(selection=SimpleNamespace(rows=st.session_state.get("selected_row_for_test", [])))
    return event
def capture_chart(chart, *args, **kwargs):
    st.session_state["chart_spec"] = chart.to_dict()
    return real_altair_chart(chart, *args, **kwargs)
def capture_loader(*args, **kwargs):
    st.session_state["batch_read_count"] = st.session_state.get("batch_read_count", 0) + 1
    return real_loader(*args, **kwargs)
'''


class ScoreRangeDashboardScenariosTests(unittest.TestCase):
    def app(self, mode="Company", **state):
        source = APP_TEMPLATE.replace("__FIXTURE_PATH__", repr(str(FIXTURE)))
        source = source.replace('overrides = {"get_db": lambda: conn,', CAPTURE + '\noverrides = {"get_combined_dashboard_series_batch": capture_loader, "get_db": lambda: conn,')
        source = source.replace('with patch.multiple(target, **overrides):', 'with patch.multiple(target, **overrides), patch.object(st, "dataframe", capture_frame), patch.object(st, "altair_chart", capture_chart):')
        app = AppTest.from_string(source, default_timeout=15).run()
        for key, value in state.items():
            app.session_state[key] = value
        app.radio(key="combined_score_mode").set_value(mode).run()
        if mode == "Company":
            app.multiselect(key="combined_score_company_select").set_value([1, 2]).run()
        elif mode == "Industry Bucket":
            app.multiselect(key="combined_score_bucket_select").set_value(["Example Bucket"]).run()
        else:
            import json
            category = json.loads(FIXTURE.read_text())["companies"][0]["category"]
            app.multiselect(key="combined_score_categories_select").set_value([category]).run()
            app.multiselect(key="combined_score_subcategory_select").set_value(["All - " + category, 1]).run()
        app.button(key="compute_overall_scores").click().run()
        self.assertFalse(app.exception)
        return app

    def test_window_columns_and_selected_score_agree_in_all_selection_modes(self):
        previous = None
        for mode in ("Company", "Industry Bucket", "Category / Sub-Category"):
            app = self.app(mode)
            frame = app.session_state["range_table"]
            self.assertEqual(frame.columns.tolist(), ["Company", "Ticker", "Included Years", *[f"Recent - {y}" for y in range(2020, 2027)]])
            self.assertEqual(app.session_state["range_table_config"]["selection_mode"], "single-row")
            actual = frame.set_index("Ticker")["Recent - 2020"].sort_index()
            expected = app.session_state["overall_result"].set_index("Ticker")[DEBT_TOTAL].sort_index()
            pd.testing.assert_series_equal(actual, expected, check_names=False, check_exact=True)
            expected_years = app.session_state["breakup_result"].set_index("Ticker")[COVERAGE].sort_index()
            pd.testing.assert_series_equal(frame.set_index("Ticker")["Included Years"].sort_index(), expected_years, check_names=False)
            if previous is not None:
                pd.testing.assert_frame_equal(previous, frame)
            previous = frame

    def test_every_shorter_window_equals_running_the_existing_dashboard_for_that_range(self):
        app = self.app()
        all_ranges = app.session_state["range_table"].set_index("Ticker").copy()
        for year in range(2021, 2027):
            app.text_input(key="combined_year_range").set_value(f"Recent - {year}").run()
            app.button(key="compute_overall_scores").click().run()
            self.assertFalse(app.exception)
            expected = app.session_state["overall_result"].set_index("Ticker")[DEBT_TOTAL].sort_index()
            pd.testing.assert_series_equal(all_ranges[f"Recent - {year}"].sort_index(), expected, check_names=False, check_exact=True)

    def test_earlier_and_later_ranges_change_columns_without_changing_original_scores(self):
        app = self.app()
        for first in (2022, 2018):
            app.text_input(key="combined_year_range").set_value(f"Recent - {first}").run()
            self.assertEqual(len(app.dataframe), 0)
            app.button(key="compute_overall_scores").click().run()
            self.assertFalse(app.exception)
            frame = app.session_state["range_table"]
            self.assertEqual(frame.columns[3:].tolist(), [f"Recent - {y}" for y in range(first, 2027)])
            self.assertTrue(all(math.isfinite(x) for x in frame[f"Recent - {first}"]))

    def test_row_selection_chart_values_and_direction_survive_reruns_without_metric_reload(self):
        app = self.app()
        original = app.session_state["overall_result"].copy()
        reads = app.session_state["batch_read_count"]
        app.session_state["selected_row_for_test"] = [1]
        app.run()
        self.assertFalse(app.exception)
        self.assertEqual(len(app.dataframe), 3)
        self.assertEqual(app.session_state["batch_read_count"], reads)
        pd.testing.assert_frame_equal(original, app.session_state["overall_result"], check_exact=True)
        spec = app.session_state["chart_spec"]
        plotted = next(iter(spec["datasets"].values()))
        selected = app.session_state["range_table"].iloc[1]
        self.assertEqual([p["Year Range"] for p in plotted], [f"Recent - {y}" for y in range(2020, 2027)])
        self.assertEqual([p["Score"] for p in plotted], [selected[f"Recent - {y}"] for y in range(2020, 2027)])
        self.assertEqual(len(spec["layer"]), 3)
        self.assertEqual(len(app.metric), 7)
        # The real browser clears selection when the dataframe column order changes.
        app.session_state["selected_row_for_test"] = []
        app.radio(key="combined_score_range_direction").set_value("Latest start year first").run()
        self.assertFalse(app.exception)
        self.assertEqual(app.session_state["range_table"].columns[3:].tolist(), [f"Recent - {y}" for y in range(2026, 2019, -1)])
        self.assertEqual([m.label for m in app.metric], [f"Recent - {y}" for y in range(2026, 2019, -1)])
        self.assertEqual(app.session_state["batch_read_count"], reads)
        pd.testing.assert_frame_equal(original, app.session_state["overall_result"], check_exact=True)
        app.session_state["selected_row_for_test"] = [0]
        app.run()
        self.assertFalse(app.exception)
        app.session_state["selected_row_for_test"] = []
        app.run()
        self.assertFalse(app.exception)
        self.assertNotIn("chart_spec", app.session_state)

    def test_lambda_and_standard_deviation_controls_apply_to_every_window(self):
        app = self.app()
        original = app.session_state["range_table"].set_index("Ticker").copy()
        app.number_input(key="combined_debt_penalty_lambda").set_value(2.).run()
        self.assertEqual(len(app.dataframe), 0)
        app.button(key="compute_overall_scores").click().run()
        self.assertFalse(app.exception)
        after = app.session_state["range_table"].set_index("Ticker")
        self.assertTrue((after.iloc[:, 2:] < original.iloc[:, 2:]).all().all())
        app.radio(key="combined_stdev_mode").set_value("Population (ddof=0)").run()
        app.button(key="compute_overall_scores").click().run()
        self.assertFalse(app.exception)
        population = app.session_state["range_table"].set_index("Ticker")
        self.assertFalse(population.iloc[:, 2:].equals(after.iloc[:, 2:]))
        pd.testing.assert_series_equal(population["Recent - 2020"].sort_index(), app.session_state["overall_result"].set_index("Ticker")[DEBT_TOTAL].sort_index(), check_names=False)

    def test_unavailable_debt_scores_are_never_plotted_as_zero(self):
        app = self.app(missing_interest=True)
        self.assertTrue(app.session_state["range_table"].iloc[:, 3:].isna().all().all())
        app.session_state["selected_row_for_test"] = [0]
        app.run()
        self.assertFalse(app.exception)
        self.assertNotIn("chart_spec", app.session_state)
        self.assertTrue(all(metric.value == "—" for metric in app.metric))

    def test_explicit_range_and_single_year_chart(self):
        app = self.app()
        app.text_input(key="combined_year_range").set_value("2023-2021").run()
        app.button(key="compute_overall_scores").click().run()
        self.assertFalse(app.exception)
        self.assertEqual(app.session_state["range_table"].columns[3:].tolist(), ["2023 - 2021", "2023 - 2022", "2023 - 2023"])
        app.text_input(key="combined_year_range").set_value("Recent - 2026").run()
        app.button(key="compute_overall_scores").click().run()
        app.session_state["selected_row_for_test"] = [0]
        app.run()
        self.assertFalse(app.exception)
        self.assertEqual(len(app.metric), 1)
        self.assertEqual(len(app.session_state["chart_spec"]["layer"]), 2)


if __name__ == "__main__":
    unittest.main()
