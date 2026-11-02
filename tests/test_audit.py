import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tick_futures.audit import coverage,value_flags
from tick_futures.codec import dumps
from tick_futures.store import Store
from test_sessions import ns


class Coverage(unittest.TestCase):
    def test_missing_values_and_partial_depth_are_flagged_without_changing_bits(self):
        import numpy as np
        import pandas as pd
        row={"id":np.int64(2),"datetime":np.int64(1793600000000000001),
             "bid_price1":pd.NA,"ask_price1":None,"bid_volume1":np.int64(0),"ask_volume1":float("nan"),
             "extra":pd.NaT,"infinite":float("inf"),"signed_zero":np.float64(-0.0)}
        original=dumps(row)
        flags=value_flags(row)
        self.assertEqual(flags["bid_price1:pandas_na"],1)
        self.assertEqual(flags["ask_price1:null"],1)
        self.assertEqual(flags["ask_volume1:nan"],1)
        self.assertEqual(flags["extra:pandas_nat"],1)
        self.assertEqual(flags["infinite:infinite"],1)
        self.assertEqual(flags["depth_1:invalid_or_absent_values"],1)
        self.assertEqual(flags["depth_2:missing_columns"],1)
        self.assertEqual(dumps(row),original)

    def test_expected_open_session_with_no_quotes_remains_visible_and_unknown(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            store = Store(root, min_free_bytes=0)
            with store.db:
                store.db.execute("INSERT INTO contracts VALUES(?,?,?,?,?,?,?)",
                    ("SHFE.fixture2612", "fixture", ns("2026-11-02T09:00:00"), None, None, "pending", "{}"))
            store.close()
            discovery = root / "discovery"
            discovery.mkdir()
            (discovery / "universe-latest.json").write_text(json.dumps({"metadata_file": "metadata.tfrec"}))
            (discovery / "metadata.tfrec").write_bytes(dumps([{"instrument_id": "SHFE.fixture2612",
                "product_id": "fixture", "trading_time_day": [["09:00:00", "11:30:00"]],
                "trading_time_night": []}]))
            (discovery / "calendar-fixture.json").write_text(json.dumps({"days": [
                {"date": "2026-11-02", "trading": True}], "night_overrides": {}}))
            with patch("tick_futures.audit.time.time_ns", return_value=ns("2026-11-02T10:00:00")):
                report = coverage(root)
            session = next(r for r in report["ledger"] if r["session"] == "day-0")
            self.assertEqual(session["rows"], 0)
            self.assertEqual(session["collection_status"], "evidence_insufficient")
            self.assertEqual(session["target_end_ns"], ns("2026-11-02T10:00:00"))
            self.assertEqual(report["expected_contracts"], 1)
            self.assertEqual(report["persisted_contracts"], 0)
            self.assertFalse(report["upstream_zero_loss_proven"])
            metadata=[{"instrument_id":"SHFE.fixture2612","delivery_year":2027,"trading_time_day":[],"trading_time_night":[]}]
            (discovery/"metadata.tfrec").write_bytes(dumps(metadata))
            store=Store(root,min_free_bytes=0)
            store.ingest("SHFE.fixture2612",[{"id":1,"datetime":ns("2016-04-27T09:00:00")}],"fixture","initial_backfill")
            store.close()
            with patch("tick_futures.audit.time.time_ns",return_value=ns("2026-11-02T10:00:00")):
                report=coverage(root)
            self.assertEqual(report["persisted_rows"],1)
            warning=report["source_history_identity_warnings"][0]
            self.assertFalse(warning["earliest_for_current_contract_proven"])
            self.assertTrue(warning["original_rows_retained"])
