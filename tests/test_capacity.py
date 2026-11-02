import json
from pathlib import Path
import tempfile
import unittest

from tick_futures.capacity import daily_quote_seconds, polling_interval
from tick_futures.codec import dumps
from tick_futures.main_contracts import discover_mains


class CapacityAndMainScope(unittest.TestCase):
    def test_day_night_none_and_2630_are_counted_once(self):
        rows = [{"instrument_id": "SHFE.au2612", "trading_time_day": [["09:00:00", "10:00:00"], ["09:30:00", "10:15:00"]],
                 "trading_time_night": [["21:00:00", "26:30:00"]]},
                {"instrument_id": "CFFEX.IF2612", "trading_time_day": [["09:30:00", "11:30:00"]], "trading_time_night": None}]
        result = daily_quote_seconds(rows)
        self.assertEqual(result["quote_seconds"], (1.25 + 5.5 + 2) * 3600)
        self.assertEqual(result["unknown_sessions"], [])

    def test_unknown_hours_are_not_silently_zero_and_missing_selection_fails(self):
        result = daily_quote_seconds([{"instrument_id": "DCE.m2701"}])
        self.assertEqual(result["quote_seconds"], 86400)
        self.assertEqual(result["unknown_sessions"], ["DCE.m2701"])
        with self.assertRaisesRegex(ValueError, "missing metadata"):
            daily_quote_seconds([], ["DCE.m2701"])

    def test_main_selection_reduces_scope_without_scaling_by_contract_count(self):
        rows = [{"instrument_id": "SHFE.au2612", "trading_time_day": [["09:00:00", "10:00:00"]], "trading_time_night": [["21:00:00", "26:30:00"]]},
                {"instrument_id": "GFEX.si2612", "trading_time_day": [["09:00:00", "10:00:00"]]}]
        result = daily_quote_seconds(rows, ["SHFE.au2612"])
        self.assertEqual(result["contracts"], 1)
        self.assertEqual(result["quote_seconds"], 6.5 * 3600)

    def test_tick_source_window_stays_10000_when_local_output_is_second_level(self):
        result = polling_interval(2)
        self.assertAlmostEqual(result["theoretical_history_seconds"], 5000)
        self.assertAlmostEqual(result["scenario_max_interval_seconds"], 2600)
        self.assertLess(result["scenario_max_interval_seconds"], 3600)
        self.assertFalse(result["source_peak_rate_certified"])
        self.assertEqual(polling_interval(100)["scenario_max_interval_seconds"], 0)
        for bad in (0, -1, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                polling_interval(bad)

    def test_main_query_validates_real_months_and_preserves_full_universe(self):
        class Api:
            def query_cont_quotes(self, exchange_id):
                return {"DCE": ["DCE.m2701"], "GFEX": ["GFEX.si2612"]}.get(exchange_id, [])
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            rows = [{"instrument_id": symbol, "ins_class": "FUTURE", "expired": False, "product_id": product}
                    for symbol, product in [("DCE.m2701", "m"), ("DCE.m2705", "m"), ("GFEX.si2612", "si")]]
            (root / "meta.tfrec").write_bytes(dumps(rows))
            universe = b'{"version":"catalog-v1","metadata_file":"meta.tfrec","symbols":["DCE.m2701","DCE.m2705","GFEX.si2612"]}'
            (root / "universe-latest.json").write_bytes(universe)
            result = discover_mains(Api(), root, root / "main")
            self.assertEqual(result["selected_contracts"], 2)
            self.assertEqual(result["status"], "verified_against_current_catalog")
            self.assertEqual((root / "universe-latest.json").read_bytes(), universe)
            self.assertFalse(result["production_universe_changed"])

    def test_stale_ambiguous_and_missing_main_are_visible(self):
        class Api:
            def query_cont_quotes(self, exchange_id):
                return {"DCE": ["DCE.m2701", "DCE.m2705", "DCE.m2001"]}.get(exchange_id, [])
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            rows = [{"instrument_id": symbol, "ins_class": "FUTURE", "expired": False, "product_id": product}
                    for symbol, product in [("DCE.m2701", "m"), ("DCE.m2705", "m"), ("GFEX.si2612", "si")]]
            (root / "meta.tfrec").write_bytes(dumps(rows))
            (root / "universe-latest.json").write_text(json.dumps({"version": "v1", "metadata_file": "meta.tfrec"}))
            result = discover_mains(Api(), root, root / "main")
            self.assertEqual(result["status"], "incomplete")
            self.assertEqual(len(result["ambiguous"]), 1)
            self.assertEqual(len(result["missing"]), 1)
            self.assertEqual(result["rejected"][0]["symbol"], "DCE.m2001")
            self.assertEqual(result["selected_contracts"], 0)
