import json
from pathlib import Path
import tempfile
import unittest

from tick_futures.overview import overview,local_time
from tick_futures.store import Store


class Overview(unittest.TestCase):
    def test_expected_persisted_live_and_archive_scopes_stay_distinct(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            source=Store(root,min_free_bytes=0)
            with source.db:
                for symbol in ["SHFE.a","SHFE.b","CFFEX.c"]:
                    source.db.execute("INSERT INTO contracts VALUES(?,?,?,?,?,?,?)",(symbol,"fixture",1793600000000000000,None,None,"pending","{}"))
                source.gap("SHFE.b","startup_window_after_target",{},status="gap_pending_replay")
            source.ingest("SHFE.a",[{"id":1,"datetime":1793600000000000001}],"r","initial_backfill")
            source.ingest("CFFEX.c",[{"id":1,"datetime":1793600000000000002}],"r","recovery_backfill")
            source.close()
            (root/"discovery").mkdir()
            (root/"discovery/universe-latest.json").write_text(json.dumps({"symbols":["SHFE.a","SHFE.b","CFFEX.c"]}))
            result=overview(root)
            self.assertEqual(result["target_contracts"],3)
            self.assertEqual(result["persisted_contracts"],2)
            self.assertEqual(result["persisted_rows"],2)
            self.assertEqual(result["contracts_with_unverified_prefix"],1)
            self.assertFalse(result["runtime"]["process_alive"])
            self.assertEqual(result["archives"]["completed_local_manifests"],0)
            self.assertFalse(result["upstream_zero_loss_proven"])
            self.assertTrue(local_time(1793600000000000001).endswith(".000000001+08:00"))
