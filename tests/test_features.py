"""Synthetic edge cases for snapshot feature semantics."""

import math
import unittest

import pandas as pd

from tick_futures.features import build_features


def fixture():
    return pd.DataFrame({
        "datetime": [1_000_000_000, 1_500_000_000], "id": [10, 11],
        "symbol": ["TEST.x", "TEST.x"], "last_price": [101.0, 101.0],
        "volume": [100, 102], "amount": [100_000.0, 102_020.0],
        "open_interest": [1000, 999], "bid_price1": [100.0, 100.0],
        "ask_price1": [102.0, 102.0], "bid_volume1": [30, 30], "ask_volume1": [10, 10],
    })


class FeatureSemantics(unittest.TestCase):
    def test_first_row_has_no_increment_and_formula_uses_multiplier(self):
        result = build_features(fixture(), 10)
        self.assertTrue(math.isnan(result.loc[0, "volume_delta"]))
        self.assertTrue(math.isnan(result.loc[0, "interval_vwap"]))
        self.assertEqual(result.loc[1, "volume_delta"], 2)
        self.assertEqual(result.loc[1, "amount_delta"], 2020)
        self.assertEqual(result.loc[1, "interval_vwap"], 101)
        self.assertEqual(result.loc[1, "open_interest_delta"], -1)
        self.assertEqual(result.loc[1, "mid_price"], 101)
        self.assertEqual(result.loc[1, "spread"], 2)
        self.assertEqual(result.loc[1, "book_imbalance1"], 0.5)

    def test_negative_amount_is_retained_and_not_made_zero(self):
        data = fixture()
        data.loc[1, "amount"] = 99_000
        result = build_features(data, 10)
        self.assertEqual(result.loc[1, "amount_delta_raw"], -1000)
        self.assertTrue(result.loc[1, "amount_decreased"])
        self.assertTrue(math.isnan(result.loc[1, "amount_delta"]))
        self.assertTrue(math.isnan(result.loc[1, "interval_vwap"]))
        self.assertEqual(data.loc[1, "amount"], result.loc[1, "amount"])

    def test_zero_volume_or_zero_book_has_no_ratio(self):
        data = fixture()
        data.loc[1, "volume"] = 100
        data.loc[1, ["bid_volume1", "ask_volume1"]] = 0
        result = build_features(data, 10)
        self.assertTrue(math.isnan(result.loc[1, "interval_vwap"]))
        self.assertTrue(math.isnan(result.loc[1, "book_imbalance1"]))

    def test_crossed_quote_is_invalid(self):
        data = fixture()
        data.loc[1, "ask_price1"] = 99
        result = build_features(data, 10)
        self.assertFalse(result.loc[1, "quote_valid"])
        self.assertTrue(math.isnan(result.loc[1, "mid_price"]))

    def test_no_deltas_across_symbol_id_time_or_session_break(self):
        cases = (("symbol", "TEST.y"), ("id", 12),
                 ("datetime", 1_000_000_000), ("datetime", 500_000_000),
                 ("datetime", 4_000_000_000))
        for column, value in cases:
            with self.subTest(column=column, value=value):
                data = fixture()
                data.loc[1, column] = value
                result = build_features(data, 10)
                self.assertFalse(result.loc[1, "interval_valid"])
                self.assertTrue(math.isnan(result.loc[1, "volume_delta"]))
                self.assertTrue(math.isnan(result.loc[1, "interval_vwap"]))
                self.assertEqual(list(result["id"]), list(data["id"]))

    def test_volume_reset_is_not_a_trade_increment(self):
        data = fixture()
        data.loc[1, "volume"] = 1
        result = build_features(data, 10)
        self.assertTrue(result.loc[1, "volume_decreased"])
        self.assertTrue(math.isnan(result.loc[1, "volume_delta"]))
        self.assertTrue(math.isnan(result.loc[1, "interval_vwap"]))

    def test_invalid_multiplier_is_rejected(self):
        for value in (0, -1, float("nan"), float("inf")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                build_features(fixture(), value)


if __name__ == "__main__":
    unittest.main()
