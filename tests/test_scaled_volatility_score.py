"""Issue #2 properties and real dashboard/SQLite/control/export scenarios."""

import ast
from decimal import Decimal, localcontext
import io
import json
import math
from pathlib import Path
import random
import statistics
import sys
import unittest

import pandas as pd
from streamlit.testing.v1 import AppTest

from core_backend import compute_scaled_volatility_score, sum_finite_scores


FIXTURE = Path(__file__).parent / "fixtures" / "issue2_company_scores.json"
SCALED_COLUMNS = [
    "Scaled Volatility-Adjusted Balance Sheet Strength Score",
    "Scaled Volatility-Adjusted P&L Growth Score",
    "Scaled FCFF and Spread Score",
]
TOTAL = "Total Scaled Volatility-Adjusted Score"
DEBT_TOTAL = "Total Debt-Adjusted Scaled Volatility-Adjusted Score"
DEBT_BS = "Debt-Adjusted Balance Sheet Strength Score"
CHANGED_COLUMNS = SCALED_COLUMNS + [TOTAL, DEBT_TOTAL, DEBT_BS]


def reference_score(growth, volatility):
    # An independent high-precision evaluation of the agreed mathematical rule.
    with localcontext() as context:
        context.prec = 80
        g, v = Decimal(str(growth)), Decimal(str(volatility))
        return float(g - abs(g) * v / (1 + v))


class ScaledVolatilityPropertiesTests(unittest.TestCase):
    def test_known_positive_negative_and_zero_examples(self):
        for g, v, expected in (
            (20, 0, 20), (20, 1, 10), (20, 4, 4),
            (-20, 0, -20), (-20, 1, -30), (-20, 4, -36),
            (0, 0, 0), (0, 1, 0), (0, 1e308, 0),
        ):
            with self.subTest(g=g, v=v):
                self.assertEqual(compute_scaled_volatility_score(g, v), expected)

    def test_positive_results_are_bit_for_bit_unchanged_including_large_volatility(self):
        rng = random.Random(20261005)
        for _ in range(5000):
            g, v = 10 ** rng.uniform(-250, 300), 10 ** rng.uniform(-250, 300)
            self.assertEqual(compute_scaled_volatility_score(g, v), g / (1 + v))
        # Direct G - ABS(G)*V/(1+V) cancels to zero here; the original result is 1.
        self.assertEqual(compute_scaled_volatility_score(1e300, 1e300), 1.0)

    def test_greater_volatility_never_improves_either_sign(self):
        vs = [0, 5e-324, 1e-16, .1, 1, 5, 100, 1e10, 1e100, sys.float_info.max]
        for g in [-1e307, -1e100, -20, -1e-12, -1e-300, 0, 1e-300, 20, 1e307]:
            scores = [compute_scaled_volatility_score(g, v) for v in vs]
            self.assertTrue(all(math.isfinite(s) for s in scores))
            self.assertTrue(all(b <= a for a, b in zip(scores, scores[1:])), (g, scores))
            for s in scores:
                self.assertLessEqual(s, g)
                self.assertGreaterEqual(s, 2 * g if g < 0 else 0)

    def test_random_negative_scores_match_independent_decimal_reference(self):
        rng = random.Random(42)
        for _ in range(5000):
            g, v = -10 ** rng.uniform(-250, 250), 10 ** rng.uniform(-200, 300)
            self.assertTrue(math.isclose(
                compute_scaled_volatility_score(g, v), reference_score(g, v),
                rel_tol=1e-14, abs_tol=0,
            ), (g, v))

    def test_score_remains_monotone_in_growth_and_continuous_at_zero(self):
        gs = [-1e100, -20, -1, -1e-12, 0, 1e-12, 1, 20, 1e100]
        for v in [0, .1, 1, 100, 1e308]:
            scores = [compute_scaled_volatility_score(g, v) for g in gs]
            self.assertTrue(all(b >= a for a, b in zip(scores, scores[1:])))
            self.assertLessEqual(abs(compute_scaled_volatility_score(-1e-12, v)), 2e-12)

    def test_invalid_and_missing_inputs_are_unavailable(self):
        for invalid in [None, float("nan"), float("inf"), -float("inf"), "unknown", pd.NA]:
            with self.subTest(invalid=invalid):
                self.assertIsNone(compute_scaled_volatility_score(invalid, 1))
                self.assertIsNone(compute_scaled_volatility_score(-20, invalid))
                self.assertIsNone(compute_scaled_volatility_score(0, invalid))
        for v in [-.1, -1, -100]:
            self.assertIsNone(compute_scaled_volatility_score(-20, v))

    def test_component_and_cumulative_overflow_do_not_produce_infinite_scores(self):
        self.assertEqual(compute_scaled_volatility_score(-1e308, 0), -1e308)
        self.assertEqual(compute_scaled_volatility_score(-1e308, 1), -1.5e308)
        self.assertIsNone(compute_scaled_volatility_score(-1e308, 10))
        self.assertIsNone(sum_finite_scores(-1e308, -1e308, 0))
        self.assertIsNone(sum_finite_scores(-1e308, -1e308))  # debt deduction boundary
        self.assertIsNone(sum_finite_scores(None, 1, 2))
        self.assertIsNone(sum_finite_scores(float("nan"), 1))
        self.assertIsNone(sum_finite_scores(float("inf"), -float("inf")))
        self.assertEqual(sum_finite_scores(0, 0, 0), 0)
        for values in [(1 / 3, 1 / 7, 1 / 11), (-30, .2, .4), (1e100, -1e100, 7)]:
            self.assertEqual(sum_finite_scores(*values), values[0] + values[1] + values[2])


# Only the connection and export capture are replaced. Company/category/bucket
# queries, annual input loading, statistics, weights, scoring, sorting, tables,
# and Excel generation all run through the real application code.
APP_TEMPLATE = r'''
import ast
import copy
import importlib
import json
from pathlib import Path
import sqlite3
from unittest.mock import patch
import streamlit as st
import combined_dashboard as dashboard
from core_backend import COMBINED_DASHBOARD_METRICS

fixture = json.loads(Path(__FIXTURE_PATH__).read_text())
scenario = st.session_state.get("scenario", "snapshot")
kind = st.session_state.get("kind", "combined")
multiplier = st.session_state.get("volatility_multiplier", 1.0)
companies = copy.deepcopy(fixture["companies"])
weights = copy.deepcopy(fixture["weights"])
if scenario != "snapshot":
    base = {
        "accumulated_profit": [100, 110, 125, 140],
        "roe": [.2]*4, "roce": [.15]*4, "interest_load_pct": [20]*4,
        "operating_income": [100]*4, "interest_expense": [10, 20, 30, 40],
        "revenue": [100, 110, 121, 133.1], "pretax_income": [10, 12, 13, 15],
        "net_income": [8, 9, 10, 12], "nopat": [9, 10, 12, 14],
        "margin": [.2]*4, "fcff": [5, 7, 6, 9], "spread_pct": [3]*4,
        "price_change": [.1, -.1, .2, .3],
    }
    centers = {"BS": 20.0, "PL": 20.0, "FS": 3.0}
    spreads = {"BS": 10.0, "PL": 10.0, "FS": 10.0}
    if scenario.endswith("negative"):
        affected = scenario.split("_")[0]
        if affected == "all":
            centers = {block: -20.0 for block in centers}
            spreads = {block: 10.0*multiplier for block in centers}
        else:
            centers[affected] = -20.0
            spreads[affected] *= multiplier
    if scenario == "zero_growth":
        centers["FS"] = 0.0
    if scenario == "zero_volatility":
        centers = {block: -20.0 for block in centers}
        spreads = {block: 0.0 for block in centers}
    for block, metric in [("BS", "roe"), ("PL", "margin"), ("FS", "spread_pct")]:
        unit = 100.0 if metric != "spread_pct" else 1.0
        base[metric] = [(centers[block] + d*spreads[block])/unit for d in [-1, 0, 0, 1]]
    companies = [{"name": "Synthetic", "ticker": "SYN", "category": fixture["companies"][0]["category"],
        "series": {metric: [{"year": 2020+i, metric: value} for i, value in enumerate(vals)] for metric, vals in base.items()}}]
    for table, rows in weights.items():
        for row in rows:
            row["weight"] = 1.0 if row["factor"] in ["ROE", "Operating Margin", "Spread"] else 0.0
    if scenario == "ranking":
        first = companies[0]
        first["name"], first["ticker"] = "Volatile loss", "VOLATILE"
        first["series"]["roe"] = [{"year": 2020+i, "roe": (-20+d*100)/100} for i,d in enumerate([-1,0,0,1])]
        for metric in ["margin", "spread_pct"]:
            first["series"][metric] = [{"year": 2020+i, metric: 0.0} for i in range(4)]
        second = copy.deepcopy(first)
        second["name"], second["ticker"] = "Stable loss", "STABLE"
        second["series"]["roe"] = [{"year": 2020+i, "roe": -.01} for i in range(4)]
        companies.append(second)

if st.session_state.get("drop_metric"):
    for company in companies:
        company["series"][st.session_state["drop_metric"]] = []
if st.session_state.get("zero_latest_accumulated_profit"):
    for company in companies:
        company["series"]["accumulated_profit"][-1]["accumulated_profit"] = 0.0
if st.session_state.get("gap_year"):
    for company in companies:
        for metric, rows in company["series"].items():
            company["series"][metric] = [r for r in rows if r["year"] != st.session_state["gap_year"]]
if st.session_state.get("zero_weights"):
    for rows in weights.values():
        for row in rows:
            row["weight"] = 0.0
if st.session_state.get("invalid_volatility"):
    companies[0]["series"]["roe"][0]["roe"] = float("inf")
if st.session_state.get("missing_interest"):
    for company in companies:
        for r in company["series"]["interest_expense"]:
            r["interest_expense"] = None
if st.session_state.get("zero_interest"):
    for company in companies:
        for r in company["series"]["interest_expense"]:
            r["interest_expense"] = 0.0

conn = sqlite3.connect(":memory:")
conn.executescript("""
CREATE TABLE companies(id INTEGER PRIMARY KEY, name TEXT, ticker TEXT);
CREATE TABLE company_groups(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE company_group_members(group_id INTEGER, company_id INTEGER);
CREATE TABLE industry_betas(user_industry_bucket TEXT, unlevered_beta REAL, cash_adjusted_beta REAL);
CREATE TABLE relative_valuation_categories(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE relative_valuation_subcategories(id INTEGER PRIMARY KEY, category_id INTEGER, name TEXT);
CREATE TABLE relative_valuation_company_assignments(company_id INTEGER, subcategory_id INTEGER);
INSERT INTO company_groups VALUES (1, 'Example Bucket');
INSERT INTO industry_betas VALUES ('Example Bucket', .8, .9);
INSERT INTO relative_valuation_subcategories VALUES (1, 1, 'Networks'), (2, 1, 'Software');
""")
conn.execute("INSERT INTO relative_valuation_categories VALUES (1, ?)", (companies[0]["category"],))
for table, rows in weights.items():
    conn.execute(f"CREATE TABLE {table}(factor TEXT, weight REAL)")
    conn.executemany(f"INSERT INTO {table} VALUES (?, ?)", [(r["factor"], r["weight"]) for r in rows])
for metric, (table, column) in COMBINED_DASHBOARD_METRICS.items():
    conn.execute(f"CREATE TABLE {table}(company_id INTEGER, fiscal_year INTEGER, {column} REAL)")
    for cid, company in enumerate(companies, 1):
        conn.executemany(f"INSERT INTO {table} VALUES (?, ?, ?)", [(cid, r["year"], r[metric]) for r in company["series"].get(metric, [])])
for cid, company in enumerate(companies, 1):
    conn.execute("INSERT INTO companies VALUES (?, ?, ?)", (cid, company["name"], company["ticker"]))
    conn.execute("INSERT INTO company_group_members VALUES (1, ?)", (cid,))
    conn.execute("INSERT INTO relative_valuation_company_assignments VALUES (?, ?)", (cid, min(cid, 2)))
conn.commit()
conn.execute("PRAGMA query_only=ON")

target = dashboard if kind == "combined" else importlib.import_module(kind)
real_scale = target.compute_scaled_volatility_score
calls = []
def capture_scale(g, v):
    result = g/(1+v) if st.session_state.get("legacy_scale") else real_scale(g, v)
    calls.append((g, v, result))
    return result
real_export = dashboard._combined_dashboard_excel_bytes
def capture_export(overall, breakup):
    st.session_state["overall_result"] = overall.copy()
    st.session_state["breakup_result"] = breakup.copy()
    result = real_export(overall, breakup)
    st.session_state["excel_result"] = result
    return result
overrides = {"get_db": lambda: conn, "compute_scaled_volatility_score": capture_scale}
if kind == "combined":
    overrides["_combined_dashboard_excel_bytes"] = capture_export
    render = target.render_combined_dashboard_tab
else:
    tree = ast.parse(Path(target.__file__).read_text(encoding="utf-8-sig"))
    node = next(n for n in ast.walk(tree) if isinstance(n, ast.With) and any(
        isinstance(item.context_expr, ast.Call) and isinstance(item.context_expr.func, ast.Attribute)
        and item.context_expr.func.attr == "expander" and item.context_expr.args
        and isinstance(item.context_expr.args[0], ast.Constant)
        and "Score Dashboard" in str(item.context_expr.args[0].value) for item in n.items))
    function = ast.FunctionDef(name="_issue2_test_render", args=ast.arguments(posonlyargs=[], args=[], kwonlyargs=[], kw_defaults=[], defaults=[]), body=[node], decorator_list=[])
    exec(ast.unparse(ast.fix_missing_locations(ast.Module(body=[function], type_ignores=[]))), target.__dict__)
    render = target._issue2_test_render
with patch.multiple(target, **overrides):
    render()
st.session_state["scaling_calls"] = calls
conn.close()
'''


class DashboardScaledScoreScenariosTests(unittest.TestCase):
    def app(self, mode="Company", scenario="snapshot", kind="combined", **state):
        source = APP_TEMPLATE.replace("__FIXTURE_PATH__", repr(str(FIXTURE)))
        app = AppTest.from_string(source).run()
        app.session_state["scenario"] = scenario
        app.session_state["kind"] = kind
        for key, value in state.items():
            app.session_state[key] = value
        app.run()
        self.assertFalse(app.exception)
        if kind != "combined":
            keys = {"bs_metrics": "bs_score_bucket_select", "pl_metrics": "pl_score_bucket_select", "cap_structure_cost": "cs_fcfe_spread_bucket_select"}
            app.multiselect(key=keys[kind]).set_value(["Example Bucket"]).run()
            if kind != "cap_structure_cost":
                app.button(key="compute_bs_scores" if kind == "bs_metrics" else "compute_pl_scores").click().run()
        else:
            app.radio(key="combined_score_mode").set_value(mode).run()
            if mode == "Company":
                count = 2 if scenario in ["snapshot", "ranking"] else 1
                app.multiselect(key="combined_score_company_select").set_value(list(range(1, count+1))).run()
            elif mode == "Industry Bucket":
                app.multiselect(key="combined_score_bucket_select").set_value(["Example Bucket"]).run()
            else:
                category = json.loads(FIXTURE.read_text())["companies"][0]["category"]
                app.multiselect(key="combined_score_categories_select").set_value([category]).run()
                # All plus an individual subcategory must not count a company twice.
                app.multiselect(key="combined_score_subcategory_select").set_value(["All - " + category, 1]).run()
            app.button(key="compute_overall_scores").click().run()
        self.assertFalse(app.exception)
        return app

    def assert_exports_match(self, app):
        overall, breakup = app.session_state["overall_result"], app.session_state["breakup_result"]
        pd.testing.assert_series_equal(overall.set_index("Ticker")[TOTAL], breakup.set_index("Ticker")[TOTAL])
        pd.testing.assert_series_equal(overall.set_index("Ticker")[DEBT_TOTAL], breakup.set_index("Ticker")[DEBT_TOTAL])
        self.assertEqual(len(app.dataframe), 3)
        book = pd.read_excel(io.BytesIO(app.session_state["excel_result"]), sheet_name=None)
        self.assertEqual(list(book), ["Overall Score Dashboard", "Overall Score Dashboard Breakup"])
        for sheet in book.values():
            for long_name, label in [(TOTAL, "Total Scaled Score"), (DEBT_TOTAL, "Debt-Adj. Total Score")]:
                expected = overall.set_index("Ticker")[long_name].sort_index()
                actual = sheet.set_index("Ticker")[label].sort_index()
                pd.testing.assert_series_equal(actual, expected, check_names=False, check_dtype=False, rtol=1e-12)

    def test_real_company_snapshots_in_all_three_selection_modes_and_excel_exports(self):
        fixture = json.loads(FIXTURE.read_text())
        previous = None
        for mode in ["Company", "Industry Bucket", "Category / Sub-Category"]:
            with self.subTest(mode=mode):
                app = self.app(mode)
                actual = app.session_state["overall_result"].set_index("Ticker")
                calls = app.session_state["scaling_calls"]
                self.assertEqual(len(calls), 6 * len(app.session_state["combined_score_results"]["range_windows"]))
                for ci, company in enumerate(fixture["companies"]):
                    expected_scores = []
                    for bi, block in enumerate(["BS", "PL", "FS"]):
                        audit = company["audit"][block]
                        g, v, score = calls[ci*3+bi]
                        self.assertAlmostEqual(g, audit["G"])
                        self.assertAlmostEqual(v, audit["V"])
                        expected = reference_score(audit["G"], audit["V"])
                        self.assertAlmostEqual(actual.loc[company["ticker"], SCALED_COLUMNS[bi]], expected)
                        expected_scores.append(expected)
                    self.assertAlmostEqual(actual.loc[company["ticker"], TOTAL], sum(expected_scores))
                    self.assertAlmostEqual(actual.loc[company["ticker"], DEBT_TOTAL], sum(expected_scores)-company["interest_penalty"])
                self.assertAlmostEqual(actual.loc["IDEA", DEBT_TOTAL], -34.48733143604379)
                self.assertAlmostEqual(actual.loc["STLTECH", DEBT_TOTAL], -46.12929332358358)
                self.assertFalse(app.warning)
                self.assert_exports_match(app)
                if previous is not None:
                    pd.testing.assert_frame_equal(actual, previous)
                previous = actual

    def test_each_negative_component_and_cumulative_score_decrease_when_volatility_rises(self):
        for bi, block in enumerate(["BS", "PL", "FS"]):
            for mode in ["Company", "Industry Bucket", "Category / Sub-Category"]:
                with self.subTest(block=block, mode=mode):
                    app = self.app(mode, scenario=block+"_negative")
                    before = app.session_state["overall_result"].iloc[0].copy()
                    g_before = app.session_state["scaling_calls"][bi][0]
                    app.session_state["volatility_multiplier"] = 3.0
                    app.button(key="compute_overall_scores").click().run()
                    self.assertFalse(app.exception)
                    after = app.session_state["overall_result"].iloc[0]
                    self.assertAlmostEqual(g_before, app.session_state["scaling_calls"][bi][0])
                    for col in [SCALED_COLUMNS[bi], TOTAL, DEBT_TOTAL]:
                        self.assertLess(after[col], before[col])
                    for other in set(SCALED_COLUMNS)-{SCALED_COLUMNS[bi]}:
                        self.assertEqual(after[other], before[other])
                    self.assert_exports_match(app)

    def test_only_affected_score_columns_change_for_real_companies_and_sparse_inputs(self):
        for scenario, extra in [
            ("snapshot", {}), ("all_negative", {}),
            ("snapshot", {"drop_metric": "net_income"}),
            ("snapshot", {"gap_year": 2022}),
            ("snapshot", {"zero_latest_accumulated_profit": True}),
        ]:
            with self.subTest(scenario=scenario, extra=extra):
                app = self.app(scenario=scenario, legacy_scale=True, **extra)
                before = app.session_state["overall_result"].set_index("Ticker").sort_index()
                before_breakup = app.session_state["breakup_result"].set_index("Ticker").sort_index()
                app.session_state["legacy_scale"] = False
                app.button(key="compute_overall_scores").click().run()
                self.assertFalse(app.exception)
                after = app.session_state["overall_result"].set_index("Ticker").sort_index()
                after_breakup = app.session_state["breakup_result"].set_index("Ticker").sort_index()
                pd.testing.assert_frame_equal(before.drop(columns=CHANGED_COLUMNS), after.drop(columns=CHANGED_COLUMNS))
                pd.testing.assert_frame_equal(before_breakup.drop(columns=[TOTAL, DEBT_TOTAL]), after_breakup.drop(columns=[TOTAL, DEBT_TOTAL]))
                self.assert_exports_match(app)

    def test_positive_outputs_zero_growth_and_zero_volatility(self):
        app = self.app(scenario="positive", legacy_scale=True)
        before = app.session_state["overall_result"].copy()
        app.session_state["legacy_scale"] = False
        app.button(key="compute_overall_scores").click().run()
        pd.testing.assert_frame_equal(before, app.session_state["overall_result"], check_exact=True)
        app = self.app(scenario="zero_growth")
        self.assertEqual(app.session_state["overall_result"][SCALED_COLUMNS[2]].iloc[0], 0)
        app = self.app(scenario="zero_volatility")
        row = app.session_state["overall_result"].iloc[0]
        self.assertEqual([row[c] for c in SCALED_COLUMNS], [-20, -20, -20])
        self.assertEqual(row[TOTAL], -60)
        self.assert_exports_match(app)

    def test_sample_population_singleton_and_reversed_range(self):
        app = self.app(scenario="all_negative")
        sample = app.session_state["overall_result"].iloc[0]
        app.radio(key="combined_stdev_mode").set_value("Population (ddof=0)").run()
        app.button(key="compute_overall_scores").click().run()
        population = app.session_state["overall_result"].iloc[0]
        self.assertGreater(population[TOTAL], sample[TOTAL])
        for bi in range(3):
            self.assertAlmostEqual(population[SCALED_COLUMNS[bi]], reference_score(-20, statistics.pstdev([-30, -20, -20, -10])))
        app.text_input(key="combined_year_range").set_value("2022-2021").run()
        app.button(key="compute_overall_scores").click().run()
        explicit = app.session_state["overall_result"].copy()
        self.assertEqual(explicit[TOTAL].iloc[0], -60)
        app.text_input(key="combined_year_range").set_value("2021-2022").run()
        app.button(key="compute_overall_scores").click().run()
        pd.testing.assert_frame_equal(explicit, app.session_state["overall_result"])
        app.text_input(key="combined_year_range").set_value("2021-2021").run()
        app.radio(key="combined_stdev_mode").set_value("Sample (ddof=1)").run()
        app.button(key="compute_overall_scores").click().run()
        self.assertFalse(app.exception)
        self.assertEqual(app.session_state["overall_result"][TOTAL].iloc[0], -60)
        self.assert_exports_match(app)

    def test_debt_strength_zero_interest_and_missing_interest_do_not_change_scaled_scores(self):
        app = self.app(scenario="all_negative")
        before = app.session_state["overall_result"].copy()
        app.number_input(key="combined_debt_penalty_lambda").set_value(2.0).run()
        app.button(key="compute_overall_scores").click().run()
        after = app.session_state["overall_result"]
        pd.testing.assert_frame_equal(before.drop(columns=[DEBT_BS, DEBT_TOTAL]), after.drop(columns=[DEBT_BS, DEBT_TOTAL]))
        penalty = statistics.median([1/11, 1/6, 3/13, 2/7])
        self.assertAlmostEqual(after[DEBT_TOTAL].iloc[0], after[TOTAL].iloc[0]-2*penalty)
        app.session_state["zero_interest"] = True
        app.button(key="compute_overall_scores").click().run()
        row = app.session_state["overall_result"].iloc[0]
        self.assertEqual(row[DEBT_TOTAL], row[TOTAL])
        app.session_state["missing_interest"] = True
        app.session_state["zero_interest"] = False
        app.button(key="compute_overall_scores").click().run()
        row = app.session_state["overall_result"].iloc[0]
        self.assertTrue(pd.isna(row[DEBT_TOTAL]))
        self.assertEqual(row[TOTAL], before[TOTAL].iloc[0])
        self.assert_exports_match(app)

    def test_missing_active_metric_and_nonfinite_volatility_keep_total_unavailable(self):
        for state in [{"drop_metric": "roe"}, {"invalid_volatility": True}]:
            with self.subTest(state=state):
                app = self.app(scenario="all_negative", **state)
                row = app.session_state["overall_result"].iloc[0]
                self.assertTrue(pd.isna(row[SCALED_COLUMNS[0]]))
                self.assertTrue(pd.isna(row[TOTAL]))
                self.assertTrue(pd.isna(row[DEBT_TOTAL]))
                self.assertTrue(all(math.isfinite(row[c]) for c in SCALED_COLUMNS[1:]))
                self.assert_exports_match(app)
        app = self.app(scenario="all_negative", zero_weights=True)
        self.assertFalse(app.exception)
        self.assertEqual(len(app.dataframe), 0)

    def test_ranking_changes_and_both_dashboards_keep_the_same_companies(self):
        app = self.app(scenario="ranking", legacy_scale=True)
        self.assertEqual(app.session_state["overall_result"]["Ticker"].tolist(), ["VOLATILE", "STABLE"])
        app.session_state["legacy_scale"] = False
        app.button(key="compute_overall_scores").click().run()
        self.assertEqual(app.session_state["overall_result"]["Ticker"].tolist(), ["STABLE", "VOLATILE"])
        self.assertEqual(app.session_state["breakup_result"]["Ticker"].tolist(), ["STABLE", "VOLATILE"])
        self.assert_exports_match(app)

    def test_standalone_component_dashboards_agree_with_combined_for_negative_growth(self):
        for bi, (kind, scenario) in enumerate([
            ("bs_metrics", "BS_negative"), ("pl_metrics", "PL_negative"), ("cap_structure_cost", "FS_negative"),
        ]):
            with self.subTest(kind=kind):
                combined = self.app(scenario=scenario)
                row = combined.session_state["overall_result"].iloc[0]
                standalone = self.app(scenario=scenario, kind=kind)
                self.assertFalse(standalone.warning)
                self.assertEqual(len(standalone.dataframe), 1)
                output = standalone.dataframe[0].value.iloc[0]
                self.assertAlmostEqual(float(output[SCALED_COLUMNS[bi]]), row[SCALED_COLUMNS[bi]], places=2)
                if kind == "bs_metrics":
                    self.assertAlmostEqual(float(output[DEBT_BS]), row[DEBT_BS], places=2)
                g, v, s = standalone.session_state["scaling_calls"][0]
                self.assertLess(g, 0)
                self.assertAlmostEqual(s, reference_score(g, v))

    def test_formula_documentation_describes_the_new_rule(self):
        import admin
        tree = ast.parse(Path(admin.__file__).read_text(encoding="utf-8-sig"))
        formula_tab = next(node for node in ast.walk(tree) if isinstance(node, ast.With) and any(
            isinstance(item.context_expr, ast.Name) and item.context_expr.id == "tab_formula" for item in node.items))
        source = "import streamlit as st\nimport pandas as pd\nfrom core_backend import normalize_score_weight_factors\nconn=None\ndef read_df(*a,**k):\n return pd.DataFrame({'factor':['ROE'],'weight':[20.]})\n" + ast.unparse(ast.Module(body=formula_tab.body, type_ignores=[]))
        app = AppTest.from_string(source).run()
        self.assertFalse(app.exception)
        rendered = "\n".join(x.value for x in app.markdown)
        self.assertIn("Scaled Score = G - ABS(G) * V / (1 + V)", rendered)
        self.assertIn("positive score approaches 0 and a negative score approaches 2 * G", rendered)
        self.assertIn("Total Scaled Volatility-Adjusted Score - Lambda * Median Annual Interest Penalty", rendered)


if __name__ == "__main__":
    unittest.main()
