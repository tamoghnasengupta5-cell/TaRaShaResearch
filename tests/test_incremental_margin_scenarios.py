import unittest

from core_backend import (
    calculate_business_quarter_trend_details,
    calculate_scenario_incremental_operating_margin,
)


class IncrementalMarginScenarioTests(unittest.TestCase):
    def _result(self, current_revenue, current_oi, prior_revenue, prior_oi):
        return calculate_scenario_incremental_operating_margin(
            current_revenue,
            current_oi,
            prior_revenue,
            prior_oi,
        )

    def test_scenario_1_revenue_and_oi_increase(self):
        result = self._result(120.0, 16.0, 100.0, 10.0)
        self.assertAlmostEqual(result["value"], 0.30)
        self.assertEqual(result["scenario"], "expansion_oi_up")

    def test_scenario_2_revenue_increases_and_oi_decreases(self):
        result = self._result(120.0, 8.0, 100.0, 10.0)
        self.assertAlmostEqual(result["value"], -0.10)
        self.assertEqual(result["scenario"], "expansion_oi_down")

    def test_scenario_3_revenue_and_oi_decrease(self):
        result = self._result(80.0, 4.0, 100.0, 10.0)
        self.assertAlmostEqual(result["value"], -0.30)
        self.assertEqual(result["scenario"], "contraction_oi_down")

    def test_scenario_4_revenue_decreases_and_oi_increases(self):
        result = self._result(80.0, 12.0, 100.0, 10.0)
        self.assertAlmostEqual(result["value"], 0.10)
        self.assertEqual(result["scenario"], "contraction_oi_up")

    def test_scenario_5_near_flat_negative_result_is_capped(self):
        result = self._result(101.0, 0.0, 100.0, 10.0)
        self.assertAlmostEqual(result["value"], -0.20)
        self.assertEqual(result["scenario"], "near_flat_revenue")

    def test_scenario_6_unchanged_revenue_uses_operating_margin_change(self):
        result = self._result(100.0, 15.0, 100.0, 10.0)
        self.assertAlmostEqual(result["value"], 0.05)
        self.assertEqual(result["scenario"], "unchanged_revenue_operating_margin_proxy")

    def test_scenario_7_loss_to_profit_is_flagged(self):
        result = self._result(120.0, 5.0, 100.0, -5.0)
        self.assertAlmostEqual(result["value"], 0.50)
        self.assertTrue(result["turnaround"])

    def test_quarterly_weighted_median_uses_scenario_adjustments(self):
        current_periods = ["2025-12-31", "2025-09-30", "2025-06-30", "2025-03-31"]
        prior_periods = ["2024-12-31", "2024-09-30", "2024-06-30", "2024-03-31"]
        revenue = dict.fromkeys(prior_periods, 100.0)
        revenue.update(dict(zip(current_periods, [120.0, 120.0, 80.0, 80.0])))
        operating_income = dict.fromkeys(prior_periods, 10.0)
        operating_income.update(dict(zip(current_periods, [16.0, 8.0, 4.0, 12.0])))

        details = calculate_business_quarter_trend_details(
            {"revenue": revenue, "operating_income": operating_income},
            quarter_range=8,
        )

        # Scenario values are +30%, -10%, -30%, and +10%; their median is 0%.
        self.assertAlmostEqual(details["weighted_median_incremental_operating_margin"], 0.0)


if __name__ == "__main__":
    unittest.main()
