import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tick_futures.operations import admit_requests,complete_requests,forced_symbols
from tick_futures.runtime import atomic_json
from tick_futures.store import Store


class BackfillOperations(unittest.TestCase):
    def test_malformed_request_is_preserved_without_stopping_reception(self):
        with tempfile.TemporaryDirectory() as folder:
            directory=Path(folder)/"requests"
            directory.mkdir()
            path=directory/"backfill-bad.json"
            raw=b'{"half written request"'
            path.write_bytes(raw)
            with patch("tick_futures.operations.alert"):
                self.assertEqual(admit_requests(folder,set(),[],set()),[])
            self.assertFalse(path.exists())
            self.assertEqual(next(directory.glob('*.invalid-*')).read_bytes(),raw)
            path.write_bytes(b'[]')
            with patch("tick_futures.operations.alert"):
                self.assertEqual(admit_requests(folder,set(),[],set()),[])
            self.assertEqual(len(list(directory.glob('*.invalid-*'))),2)

    def test_queued_request_survives_restart_and_needs_a_fresh_source(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/"requests/backfill-fixture.json"
            atomic_json(path,{"status":"pending","requested_ns":20,"symbols":["SHFE.fixture","UNKNOWN"]})
            seen=set()
            self.assertEqual(admit_requests(folder,{"SHFE.fixture"},[],seen),["SHFE.fixture"])
            self.assertEqual(admit_requests(folder,{"SHFE.fixture"},[],seen),[])
            self.assertEqual(admit_requests(folder,{"SHFE.fixture"},[],set()),["SHFE.fixture"])
            complete_requests(folder,"SHFE.fixture",19,{"written":0})
            self.assertEqual(json.loads(path.read_text())["status"],"queued")
            complete_requests(folder,"SHFE.fixture",21,{"written":0,"decision":"historical_content_replay_checked"})
            result=json.loads(path.read_text())
            self.assertEqual(result["status"],"completed")
            self.assertEqual(result["rejected"],["UNKNOWN"])
            self.assertEqual(forced_symbols(folder),set())

    def test_failure_is_visible_without_claiming_completion(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/"requests/backfill-fixture.json"
            atomic_json(path,{"status":"pending","requested_ns":1,"symbols":["SHFE.fixture"]})
            admit_requests(folder,{"SHFE.fixture"},[],set())
            complete_requests(folder,"SHFE.fixture",2,{"error":"source unavailable"})
            self.assertEqual(json.loads(path.read_text())["status"],"failed")

    def test_maximum_full_window_marks_unverifiable_old_interval_unrecoverable(self):
        def row(i):return {"id":i,"datetime":i*1000}
        with tempfile.TemporaryDirectory() as folder:
            store=Store(folder,min_free_bytes=0)
            store.ingest("TEST.a",[row(1)],"r","initial")
            store.ingest("TEST.a",[row(i) for i in range(10001,11001)],"r","reconnect",window_info={"configured_rows":1000})
            before=store.checkpoint("TEST.a")
            store.ingest("TEST.a",[row(i) for i in range(1001,11001)],"r","recovery_backfill",update_checkpoint=False,window_info={"configured_rows":10000})
            self.assertEqual(store.db.execute("SELECT status FROM gaps").fetchone()[0],"gap_unrecoverable")
            self.assertEqual(store.checkpoint("TEST.a"),before)
            self.assertEqual(store.db.execute("SELECT count(*) FROM records").fetchone()[0],10001)
            store.close()

    def test_short_maximum_window_leaves_unknown_instead_of_claiming_loss_or_completion(self):
        def row(i):return {"id":i,"datetime":i*1000}
        with tempfile.TemporaryDirectory() as folder:
            store=Store(folder,min_free_bytes=0)
            store.ingest("TEST.a",[row(1)],"r","initial")
            store.ingest("TEST.a",[row(i) for i in range(8,11)],"r","reconnect",window_info={"configured_rows":1000})
            store.ingest("TEST.a",[row(i) for i in range(2,11)],"r","recovery_backfill",update_checkpoint=False,window_info={"configured_rows":10000})
            self.assertEqual(store.db.execute("SELECT status FROM gaps").fetchone()[0],"evidence_insufficient")
            store.close()
