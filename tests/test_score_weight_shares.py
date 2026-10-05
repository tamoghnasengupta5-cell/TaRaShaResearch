"""Percentage allocation validation and real Admin-to-dashboard regressions."""

import io
import json
import math
from pathlib import Path
import random
import sqlite3
import textwrap
import unittest
from unittest.mock import patch

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from streamlit.testing.v1 import AppTest

import db_models
from db_session import DbCompat
from core_backend import (
    SCORE_WEIGHT_FACTOR_ALIASES, SCORE_WEIGHT_TABLES,
    compute_score_weighted_average, get_score_weight_factors,
    normalize_score_weight_factors, read_df, save_score_weight_shares,
)
from test_scaled_volatility_score import APP_TEMPLATE, FIXTURE, TOTAL, DEBT_TOTAL


def fixture_rows():
    return json.loads(FIXTURE.read_text())["weights"]


def make_connection(orm=False):
    if orm:
        engine = create_engine("sqlite:///:memory:")
        for name in SCORE_WEIGHT_TABLES:
            db_models.metadata.tables[name].create(engine)
        conn = DbCompat(Session(engine))
    else:
        conn = sqlite3.connect(":memory:")
        for name in SCORE_WEIGHT_TABLES:
            conn.execute(f"CREATE TABLE {name}(id INTEGER PRIMARY KEY, factor TEXT, weight REAL)")
    for name, rows in fixture_rows().items():
        for index, row in enumerate(rows, 1):
            conn.execute(f"INSERT INTO {name}(id, factor, weight) VALUES (?, ?, ?)",
                         (index, row["factor"], row["weight"]))
    conn.commit()
    return conn


def allocation(frame, stddev=False):
    values = {factor: 10.0 for factor in SCORE_WEIGHT_FACTOR_ALIASES}
    values["Accumulated Equity Growth"] = values["ROCE"] = 5.0
    if stddev:
        values["FCFF Growth"], values["Spread"] = 5.0, 15.0
    return {int(row["id"]): values[row["factor"]] for _, row in frame.iterrows()}


class WeightShareMathTests(unittest.TestCase):
    def test_legacy_allocations_total_100_and_keep_relative_weights_without_writes(self):
        for orm in [False, True]:
            with self.subTest(orm=orm):
                conn = make_connection(orm)
                for table in SCORE_WEIGHT_TABLES:
                    before = read_df(f"SELECT * FROM {table} ORDER BY id", conn)
                    actual = get_score_weight_factors(conn, table, include_ids=True)
                    self.assertEqual(len(actual), 11)
                    self.assertEqual(set(actual["factor"]), set(SCORE_WEIGHT_FACTOR_ALIASES))
                    self.assertAlmostEqual(math.fsum(actual["weight"]), 100)
                    raw = before.set_index("id")["weight"]
                    ratios = [r["weight"]/raw[int(r["id"])] for _, r in actual.iterrows()]
                    self.assertTrue(all(math.isclose(ratios[0], value) for value in ratios))
                    pd.testing.assert_frame_equal(before, read_df(f"SELECT * FROM {table} ORDER BY id", conn))
                conn.close()

    def test_aliases_are_canonical_and_duplicates_do_not_consume_budget(self):
        frame = pd.DataFrame([
            {"id": 1, "factor": "Net Income  Growth", "weight": 99.0},
            {"id": 2, "factor": "Net Income Growth", "weight": 10.0},
            {"id": 3, "factor": "FCFE Growth", "weight": 30.0},
            {"id": 4, "factor": "Earnings Power Change %", "weight": 1000.0},
            {"id": 5, "factor": "Change in EP Delta", "weight": 1000.0},
        ])
        actual = normalize_score_weight_factors(frame).set_index("factor")
        self.assertEqual(actual.index.tolist(), ["Net Income Growth", "FCFF Growth"])
        self.assertEqual(actual["weight"].tolist(), [25, 75])
        self.assertEqual(actual["id"].tolist(), [2, 3])

    def test_empty_zero_and_invalid_weights_are_not_given_invented_allocations(self):
        empty = pd.DataFrame(columns=["factor", "weight"])
        self.assertTrue(normalize_score_weight_factors(empty).empty)
        zero = pd.DataFrame({"factor": ["ROE", "Spread"], "weight": [0, 0]})
        self.assertEqual(normalize_score_weight_factors(zero)["weight"].tolist(), [0, 0])
        for invalid in [None, "unknown", math.nan, math.inf, -math.inf, -1]:
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                normalize_score_weight_factors(pd.DataFrame({"factor": ["ROE"], "weight": [invalid]}))

    def test_normalization_is_scale_invariant_and_preserves_weighted_growth_and_volatility(self):
        rng = random.Random(20261005)
        names = list(SCORE_WEIGHT_FACTOR_ALIASES)
        for _ in range(200):
            weights = [rng.uniform(.01, 100) for _ in names]
            frame = pd.DataFrame({"factor": names, "weight": weights})
            normalized = normalize_score_weight_factors(frame)
            values = [rng.uniform(-1000, 1000) for _ in names]
            self.assertAlmostEqual(compute_score_weighted_average(list(zip(values, weights))),
                                   compute_score_weighted_average(list(zip(values, normalized["weight"]))), places=10)
            self.assertAlmostEqual(math.fsum(normalized["weight"]), 100)
        huge = pd.DataFrame({"factor": ["ROE", "Spread"], "weight": [1e308, 1e308]})
        self.assertEqual(normalize_score_weight_factors(huge)["weight"].tolist(), [50, 50])

    def test_normalization_does_not_introduce_weighted_average_overflow(self):
        self.assertEqual(compute_score_weighted_average([(1e308, .01), (0, .01)]), 5e307)
        self.assertEqual(compute_score_weighted_average([(1e308, 50), (0, 50)]), 5e307)
        self.assertEqual(compute_score_weighted_average([(1e308, 50), (-1e308, 50)]), 0)
        self.assertEqual(compute_score_weighted_average([(1e308, 50), (1e308, 50)]), 1e308)
        self.assertIsNone(compute_score_weighted_average([(1, 0), (None, 100)]))

    def test_valid_shares_save_implied_weights_on_sqlite_and_orm_without_touching_other_settings(self):
        for orm in [False, True]:
            with self.subTest(orm=orm):
                conn = make_connection(orm)
                before_stddev = read_df("SELECT * FROM stddev_weight_factors ORDER BY id", conn)
                original = read_df("SELECT * FROM growth_weight_factors ORDER BY id", conn)
                active = get_score_weight_factors(conn, "growth_weight_factors", include_ids=True)
                shares = allocation(active)
                implied = save_score_weight_shares(conn, "growth_weight_factors", shares)
                self.assertAlmostEqual(math.fsum(implied["weight"]), 100)
                actual = read_df("SELECT * FROM growth_weight_factors ORDER BY id", conn)
                for _, row in implied.iterrows():
                    self.assertAlmostEqual(actual.set_index("id").loc[int(row["id"]), "weight"], row["weight"])
                inactive = ~original["id"].isin(active["id"])
                pd.testing.assert_frame_equal(original[inactive], actual[inactive])
                pd.testing.assert_frame_equal(before_stddev, read_df("SELECT * FROM stddev_weight_factors ORDER BY id", conn))
                conn.close()

    def test_invalid_incomplete_or_non_100_allocations_never_partially_write(self):
        for orm in [False, True]:
            conn = make_connection(orm)
            active = get_score_weight_factors(conn, "growth_weight_factors", include_ids=True)
            valid = allocation(active)
            first = next(iter(valid))
            cases = [{}, {first: 100}, {**valid, 999: 0},
                     {**valid, first: 0}, {**valid, first: 101}]
            cases += [{**valid, first: value} for value in [-1, None, "unknown", math.nan, math.inf]]
            before = read_df("SELECT * FROM growth_weight_factors ORDER BY id", conn)
            for shares in cases:
                with self.subTest(orm=orm, shares=shares), self.assertRaises(ValueError):
                    save_score_weight_shares(conn, "growth_weight_factors", shares)
                pd.testing.assert_frame_equal(before, read_df("SELECT * FROM growth_weight_factors ORDER BY id", conn))
            with self.assertRaises(ValueError):
                get_score_weight_factors(conn, "growth_weight_factors; DROP TABLE companies")
            conn.close()

    def test_rounding_tolerance_and_zero_shares_preserve_an_exact_implied_budget(self):
        conn = make_connection()
        active = get_score_weight_factors(conn, "growth_weight_factors", include_ids=True)
        shares = {int(row_id): 0.0 for row_id in active["id"]}
        shares[next(iter(shares))] = 99.999999
        actual = save_score_weight_shares(conn, "growth_weight_factors", shares)
        self.assertEqual(actual["weight"].tolist(), [100.0] + [0.0]*10)
        conn.close()


def weight_flow_source():
    # Use the earlier full dashboard SQLite fixture, with persistent in-memory
    # weight tables so real Admin saves feed subsequent dashboard calculations.
    source = APP_TEMPLATE.replace("__FIXTURE_PATH__", repr(str(FIXTURE)))
    source = source.replace('CREATE TABLE {table}(factor TEXT, weight REAL)',
                            'CREATE TABLE {table}(id INTEGER PRIMARY KEY, factor TEXT, weight REAL)')
    source = source.replace('INSERT INTO {table} VALUES (?, ?)',
                            'INSERT INTO {table}(factor, weight) VALUES (?, ?)')
    start = source.index('conn = sqlite3.connect(":memory:")')
    end = source.index('target = dashboard', start)
    remaining = source[end:]
    initialize = source[start:end].replace('sqlite3.connect(":memory:")', 'sqlite3.connect(":memory:", check_same_thread=False)')
    initialize = initialize.replace('conn.execute("PRAGMA query_only=ON")', '')
    source = source[:start] + 'if "_weight_fixture_conn" not in st.session_state:\n' + textwrap.indent(initialize, '    ')
    source += '    st.session_state["_weight_fixture_conn"] = conn\nconn = st.session_state["_weight_fixture_conn"]\n' + remaining
    source = source.replace('with patch.multiple(target, **overrides):\n    render()',
        'from score_weights_ui import render_score_weights_admin\n'
        'with patch.multiple(target, **overrides):\n'
        '    if st.sidebar.radio("Page", ["Weights", "Combined Dashboard"], key="weight_test_page") == "Weights":\n'
        '        render_score_weights_admin(conn)\n'
        '    else:\n        render()')
    source = source.replace('conn.close()', '')
    source += '\nfrom core_backend import get_score_weight_factors, read_df\n'
    for prefix, table in [('growth', 'growth_weight_factors'), ('stddev', 'stddev_weight_factors')]:
        source += f'st.session_state["saved_{prefix}"] = get_score_weight_factors(conn, "{table}", include_ids=True)\n'
        source += f'st.session_state["raw_{prefix}"] = read_df("SELECT * FROM {table} ORDER BY id", conn)\n'
    return source


class WeightShareDashboardFlowTests(unittest.TestCase):
    def app(self):
        source = weight_flow_source()
        compile(source, 'weight_flow_app', 'exec')
        app = AppTest.from_string(source).run()
        self.assertFalse(app.exception)
        self.assertFalse(app.warning)
        return app

    def set_allocation(self, app, prefix, shares):
        for row_id, value in shares.items():
            app.number_input(key=f"{prefix}_share_{row_id}").set_value(value)
        app.run()
        self.assertFalse(app.exception)

    def test_admin_only_exposes_11_used_percentage_factors_and_read_only_implied_values_per_set(self):
        app = self.app()
        self.assertEqual(len(app.number_input), 22)
        self.assertEqual(len(app.text_input), 22)
        for prefix in ['growth', 'stddev']:
            saved = app.session_state[f'saved_{prefix}']
            self.assertAlmostEqual(saved['weight'].sum(), 100)
            for _, row in saved.iterrows():
                row_id = int(row['id'])
                editable = app.number_input(key=f'{prefix}_share_{row_id}')
                readonly = app.text_input(key=f'{prefix}_implied_{row_id}')
                self.assertFalse(editable.disabled)
                self.assertTrue(readonly.disabled)
                self.assertAlmostEqual(float(readonly.value), row['weight'], places=6)
                self.assertEqual(editable.max, 100)
        labels = [element.label for element in app.number_input]
        self.assertNotIn('Earnings Power Change %', labels)
        self.assertNotIn('Change in EP Delta', labels)

    def test_implied_weights_update_live_and_unsaved_or_invalid_edits_do_not_reach_scoring(self):
        app = self.app()
        before = app.session_state['raw_growth'].copy()
        row_id = int(app.session_state['saved_growth'].iloc[0]['id'])
        app.number_input(key=f'growth_share_{row_id}').set_value(0).run()
        self.assertEqual(float(app.text_input(key=f'growth_implied_{row_id}').value), 0)
        implied = [float(element.value) for element in app.text_input if element.key.startswith('growth_implied_')]
        self.assertAlmostEqual(sum(implied), 100, places=5)
        app.button(key='growth_weights_save').click().run()
        self.assertFalse(app.exception)
        self.assertTrue(app.error)
        pd.testing.assert_frame_equal(before, app.session_state['raw_growth'])

    def test_admin_save_flows_into_display_both_scores_and_excel_in_every_selection_mode(self):
        results = []
        for mode in ['Company', 'Industry Bucket', 'Category / Sub-Category']:
            with self.subTest(mode=mode):
                app = self.app()
                for prefix in ['growth', 'stddev']:
                    frame = app.session_state[f'saved_{prefix}']
                    shares = allocation(frame, stddev=prefix=='stddev')
                    self.set_allocation(app, prefix, shares)
                    app.button(key=f'{prefix}_weights_save').click().run()
                    self.assertFalse(app.error)
                    self.assertFalse(app.exception)
                    actual = app.session_state[f'saved_{prefix}'].set_index('id')['weight']
                    for row_id, value in shares.items():
                        self.assertAlmostEqual(actual.loc[row_id], value)
                app.radio(key='weight_test_page').set_value('Combined Dashboard').run()
                app.radio(key='combined_score_mode').set_value(mode).run()
                if mode=='Company':
                    app.multiselect(key='combined_score_company_select').set_value([1,2]).run()
                elif mode=='Industry Bucket':
                    app.multiselect(key='combined_score_bucket_select').set_value(['Example Bucket']).run()
                else:
                    category = json.loads(FIXTURE.read_text())['companies'][0]['category']
                    app.multiselect(key='combined_score_categories_select').set_value([category]).run()
                    app.multiselect(key='combined_score_subcategory_select').set_value(['All - '+category,1]).run()
                app.button(key='compute_overall_scores').click().run()
                self.assertFalse(app.exception)
                self.assertFalse(app.warning)
                overall = app.session_state['overall_result'].set_index('Ticker').sort_index()
                breakup = app.session_state['breakup_result'].set_index('Ticker').sort_index()
                pd.testing.assert_series_equal(overall[TOTAL],breakup[TOTAL])
                pd.testing.assert_series_equal(overall[DEBT_TOTAL],breakup[DEBT_TOTAL])
                self.assertEqual(len(app.dataframe),3)
                self.assertTrue(all(math.isfinite(v) for v in overall[TOTAL]))
                # Each company's real growth/SD calculations independently
                # use the persisted 100-point allocations.
                weights = {prefix: app.session_state[f'saved_{prefix}'].set_index('factor')['weight'].to_dict()
                           for prefix in ['growth','stddev']}
                raw_companies = json.loads(FIXTURE.read_text())['companies']
                import core_backend
                for index, company in enumerate(raw_companies):
                    roe = [r['roe']*100 for r in company['series']['roe']]
                    roce = [r['roce']*100 for r in company['series']['roce']]
                    equity = pd.DataFrame(company['series']['accumulated_profit'])
                    med, sd = core_backend.compute_growth_stats(equity,2026,2020,stdev_sample=True,value_col='accumulated_profit',abs_denom=True)
                    import statistics
                    gs = [med*100, statistics.median(roe), statistics.median(roce)]
                    vs = [sd*100, statistics.stdev(roe), statistics.stdev(roce)]
                    factors = ['Accumulated Equity Growth','ROE','ROCE']
                    g = sum(v*weights['growth'][f] for v,f in zip(gs,factors))/sum(weights['growth'][f] for f in factors)
                    v = sum(x*weights['stddev'][f] for x,f in zip(vs,factors))/sum(weights['stddev'][f] for f in factors)
                    call = app.session_state['scaling_calls'][index*3]
                    self.assertAlmostEqual(call[0],g)
                    self.assertAlmostEqual(call[1],v)
                    fcff = pd.DataFrame(company['series']['fcff'])
                    med_fcff, sd_fcff = core_backend.compute_growth_stats(fcff,2026,2020,stdev_sample=True,value_col='fcff',abs_denom=True)
                    spread = [r['spread_pct'] for r in company['series']['spread_pct']]
                    gs = [med_fcff*100,statistics.median(spread)]
                    vs = [sd_fcff*100,statistics.stdev(spread)]
                    factors = ['FCFF Growth','Spread']
                    g = sum(x*weights['growth'][f] for x,f in zip(gs,factors))/sum(weights['growth'][f] for f in factors)
                    v = sum(x*weights['stddev'][f] for x,f in zip(vs,factors))/sum(weights['stddev'][f] for f in factors)
                    call = app.session_state['scaling_calls'][index*3+2]
                    self.assertAlmostEqual(call[0],g)
                    self.assertAlmostEqual(call[1],v)
                book = pd.read_excel(io.BytesIO(app.session_state['excel_result']),sheet_name=None)
                self.assertEqual(len(book),2)
                for sheet in book.values():
                    actual = sheet.set_index('Ticker').sort_index()
                    for long_name,label in [(TOTAL,'Total Scaled Score'),(DEBT_TOTAL,'Debt-Adj. Total Score')]:
                        pd.testing.assert_series_equal(actual[label],overall[long_name],check_names=False,rtol=1e-12)
                results.append(overall)
        for frame in results[1:]:
            pd.testing.assert_frame_equal(results[0],frame)

    def test_weight_visual_is_read_only_uses_the_calculation_values_and_follows_lambda(self):
        import combined_dashboard
        import score_weights_ui
        import ast
        tree = ast.parse(Path(combined_dashboard.__file__).read_text(encoding='utf-8-sig'))
        calls = {n.func.id:n.lineno for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name)}
        self.assertGreater(calls['render_score_weight_summary'], next(n.lineno for n in ast.walk(tree)
            if isinstance(n,ast.Call) and any(isinstance(k.value,ast.Constant) and k.value.value=='combined_debt_penalty_lambda' for k in n.keywords)))
        app = self.app()
        growth, stddev = app.session_state['saved_growth'],app.session_state['saved_stddev']
        with patch.object(score_weights_ui.st,'html') as html:
            score_weights_ui.render_score_weight_summary(growth,stddev)
        rendered = html.call_args.args[0]
        self.assertEqual(rendered.count('100.00 / 100'),2)
        self.assertNotIn('Earnings Power Change',rendered)
        self.assertNotIn('<input',rendered)
        for frame in [growth,stddev]:
            for _, row in frame.iterrows():
                self.assertIn(f'<strong>{row["weight"]:.6f}</strong>',rendered)


if __name__ == '__main__':
    unittest.main()
