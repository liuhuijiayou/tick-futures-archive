import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tick_futures.audit import coverage
from tick_futures.codec import dumps
from tick_futures.store import Store
from test_sessions import ns


class Coverage(unittest.TestCase):
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
