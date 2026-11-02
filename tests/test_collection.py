import json
import os
from pathlib import Path
import signal
import sqlite3
import struct
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd

from tick_futures.codec import dumps, loads, frame_rows
from tick_futures.collector import changed_rows
from tick_futures.probe_stream import ProbeStream
from tick_futures.runtime import InstanceLock
from tick_futures.store import DurableWriter, Store


def row(i, ns=None, **kw):
    return {"id": np.int64(i), "datetime": np.int64(ns or (1_793_600_000_000_000_000 + i)),
            "last_price": np.float64(101), "amount": np.float64(i), "volume": np.int64(i), **kw}


class Collection(unittest.TestCase):
    def test_exact_types_bits_and_large_timestamps(self):
        nan = struct.unpack(">d", bytes.fromhex("7ff8000000000042"))[0]
        values = [None, pd.NA, pd.NaT, nan, -0.0, float("inf"), float("-inf"),
                  np.float32(-0.0), np.float64(nan), np.int64(2**63-1), 2**80,
                  {"datetime": 1793600000000000001, "null": None, "nan": nan}]
        self.assertEqual(dumps(loads(dumps(values))), dumps(values))
        self.assertNotEqual(dumps(None), dumps(float("nan")))

    def test_snapshot_does_not_coerce_integer_to_float(self):
        frame = pd.DataFrame({"datetime": np.array([1793600000000000001], dtype="int64"),
                              "id": np.array([2], dtype="int64"), "p": [1.1]})
        copied = frame_rows(frame)
        frame.loc[0, "datetime"] = 1793600000000000002
        self.assertEqual(copied[0]["datetime"], 1793600000000000001)
        self.assertIsInstance(copied[0]["datetime"], np.int64)

    def test_new_nested_field_is_copied_stably_without_losing_null_or_float_bits(self):
        original=row(1,extra={"values":[None,np.float64(-0.0),np.float64(float("nan"))]})
        frame=pd.DataFrame([original])
        copied=frame_rows(frame)
        before=dumps(copied)
        frame.loc[0,"extra"]["values"][0]="mutated by SDK"
        self.assertEqual(dumps(copied),before)
        self.assertIsNone(copied[0]["extra"]["values"][0])
        self.assertTrue(changed_rows(frame,pd.DataFrame([row(1)]))[1])

    def test_initialization_and_coalesced_updates_are_all_saved(self):
        with tempfile.TemporaryDirectory() as folder:
            db = Store(folder, min_free_bytes=0)
            db.ingest("TEST.x", [row(1), row(2)], "r", "initial_backfill")
            db.ingest("TEST.x", [row(i) for i in range(1, 6)], "r", "realtime_update")
            self.assertEqual(db.db.execute("SELECT count(*) FROM records").fetchone()[0], 5)
            self.assertEqual(db.checkpoint("TEST.x")[1][-1]["id"], 5)
            db.close()

    def test_same_timestamp_different_rows_and_conflicts_are_retained(self):
        with tempfile.TemporaryDirectory() as folder:
            db = Store(folder, min_free_bytes=0)
            db.ingest("TEST.x", [row(1, 100), row(2, 100)], "r", "initial_backfill")
            db.ingest("TEST.x", [row(1, 100), row(2, 100, last_price=np.float64(102))], "r", "reconnect")
            self.assertEqual(db.db.execute("SELECT count(*) FROM records").fetchone()[0], 3)
            self.assertEqual(db.db.execute("SELECT count(*) FROM events WHERE kind='same_id_different_content'").fetchone()[0], 1)
            db.close()

    def test_restart_overlap_is_audited_without_duplicate_storage(self):
        with tempfile.TemporaryDirectory() as folder:
            db = Store(folder, min_free_bytes=0)
            db.ingest("TEST.x", [row(1), row(2)], "r1", "initial_backfill")
            db.close()
            db = Store(folder, min_free_bytes=0)
            result = db.ingest("TEST.x", [row(1), row(2), row(3)], "r2", "reconnect")
            self.assertEqual(result["written"], 1)
            self.assertEqual(db.db.execute("SELECT count(*) FROM observations").fetchone()[0], 2)
            db.close()

    def test_wider_replay_saves_older_history_and_revision_outside_checkpoint_tail(self):
        with tempfile.TemporaryDirectory() as folder:
            db=Store(folder,min_free_bytes=0)
            db.ingest("TEST.x",[row(i) for i in range(1,101)],"run","initial_backfill")
            replay=[row(i) for i in range(0,102)]
            replay[5]["last_price"]=np.float64(102)
            result=db.ingest("TEST.x",replay,"restart","recovery_backfill")
            self.assertEqual(result["written"],3)
            self.assertEqual(db.db.execute("SELECT count(*) FROM records").fetchone()[0],103)
            self.assertEqual(db.db.execute("SELECT count(*) FROM events WHERE kind='same_id_different_content'").fetchone()[0],1)
            observation=json.loads(db.db.execute("SELECT detail FROM observations ORDER BY seq DESC LIMIT 1").fetchone()[0])
            self.assertEqual(len(observation["input_fingerprints_in_order"]),102)
            db.close()

    def test_writer_ack_follows_commit_and_stable_copy_survives_mutation(self):
        with tempfile.TemporaryDirectory() as folder:
            frame=pd.DataFrame([row(1),row(2)])
            rows=frame_rows(frame)
            writer=DurableWriter(folder,min_free_bytes=0)
            writer.submit("token","TEST.x",rows,"run","initial_backfill")
            frame.loc[0,"last_price"]=999
            token,result=writer.acks.get(timeout=10)
            self.assertEqual(token,"token")
            reader=sqlite3.connect(Path(folder)/"hot.sqlite3")
            self.assertEqual(reader.execute("SELECT count(*) FROM records").fetchone()[0],2)
            raw=reader.execute("SELECT raw FROM records ORDER BY seq LIMIT 1").fetchone()[0]
            self.assertEqual(loads(raw)["last_price"],101)
            reader.close()
            writer.close()

    def test_historical_helper_never_moves_live_checkpoint_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            live=Store(root/"live",min_free_bytes=0)
            live.ingest("TEST.x",[row(i) for i in range(10,13)],"live","initial_backfill")
            before=live.checkpoint("TEST.x")
            helper=Store(root/"helper",min_free_bytes=0)
            helper.ingest("TEST.x",[row(i) for i in range(1,13)],"helper","initial_backfill")
            helper.close()
            result=live.ingest_journal(root/"helper/hot.sqlite3","TEST.x","live",{})
            self.assertEqual(result["written"],9)
            self.assertEqual(live.checkpoint("TEST.x")[0],before[0])
            self.assertEqual(dumps(live.checkpoint("TEST.x")[1]),dumps(before[1]))
            self.assertEqual(live.ingest_journal(root/"helper/hot.sqlite3","TEST.x","live",{})["written"],0)
            live.close()

    def test_corrupt_helper_ack_does_not_stop_the_live_writer(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            (root/"bad.sqlite3").write_bytes(b"half written SQLite")
            writer=DurableWriter(root/"live",min_free_bytes=0)
            writer.submit_journal("bad",root/"bad.sqlite3","TEST.x","live",{})
            token,result=writer.acks.get(timeout=10)
            self.assertEqual(token,"bad")
            self.assertIn("error",result)
            writer.submit("good","TEST.x",[row(1)],"live","live_update")
            self.assertEqual(writer.acks.get(timeout=10)[1]["written"],1)
            writer.close()

    def test_injected_disconnect_reconnect_replays_available_window(self):
        import asyncio
        import time
        from tick_futures.collector import collect
        from tick_futures.runtime import atomic_bytes
        symbol="SHFE.fixture2612"
        class FakeApi:
            def __init__(self,disconnect,rows):
                self.disconnect,self.calls=disconnect,0
                self.frame=pd.DataFrame(rows)
            def create_task(self,coro):
                asyncio.run(coro)
            def get_tick_serial(self,*args,**kwargs):
                return self.frame
            def is_serial_ready(self,frame):
                return True
            def is_changing(self,frame):
                return True
            def wait_update(self,**kwargs):
                self.calls+=1
                time.sleep(.003)
                if self.disconnect and self.calls==2:
                    raise ConnectionError("injected process-local transport failure")
            def close(self):
                pass
        apis=[FakeApi(True,[row(1),row(2)]),FakeApi(False,[row(1),row(2),row(3)])]
        def discover(api,path):
            path.mkdir(parents=True,exist_ok=True)
            atomic_bytes(path/"metadata.tfrec",dumps([{"instrument_id":symbol,"product_id":"fixture","trading_time_day":[],"trading_time_night":[]}]))
            return {"version":"fixture","symbols":[symbol],"metadata_file":"metadata.tfrec"}
        with tempfile.TemporaryDirectory() as folder,patch("tick_futures.collector.connect",side_effect=apis),patch("tick_futures.collector.discover",side_effect=discover):
            result=collect(folder,seconds=.5,min_free_bytes=0)
            db=sqlite3.connect(Path(folder)/"hot.sqlite3")
            self.assertEqual(db.execute("SELECT count(*) FROM records").fetchone()[0],3)
            self.assertEqual(db.execute("SELECT count(*) FROM events WHERE kind='connection_exception'").fetchone()[0],1)
            self.assertEqual(db.execute("SELECT count(*) FROM events WHERE kind='same_source_window_reconciled'").fetchone()[0],1)
            db.close()

    def test_false_update_does_not_rescan_stale_sdk_diff_and_coalesced_ticks_persist(self):
        import asyncio,time
        from tick_futures.collector import collect,changed_rows as compare
        from tick_futures.runtime import atomic_bytes
        symbol="SHFE.fixture2612"
        class Api:
            calls=0
            frame=pd.DataFrame([row(1),row(2)])
            def create_task(self,coro): asyncio.run(coro)
            def get_tick_serial(self,*a,**kw): return self.frame
            def is_serial_ready(self,frame): return True
            def is_changing(self,frame): return True  # cached prior diff, even after timeout
            def close(self): pass
            def wait_update(self,**kw):
                self.calls+=1
                time.sleep(.005)
                if self.calls==2:
                    self.frame.loc[2]=row(3)
                    self.frame.loc[3]=row(4)
                return self.calls<=2
        api=Api()
        def discover(api,path):
            path.mkdir(parents=True,exist_ok=True)
            atomic_bytes(path/"metadata.tfrec",dumps([{"instrument_id":symbol,"trading_time_day":[],"trading_time_night":[]}]))
            return {"version":"fixture","symbols":[symbol],"metadata_file":"metadata.tfrec"}
        with tempfile.TemporaryDirectory() as folder,patch("tick_futures.collector.connect",return_value=api),patch("tick_futures.collector.discover",side_effect=discover),patch("tick_futures.collector.changed_rows",wraps=compare) as comparisons:
            collect(folder,seconds=.3,min_free_bytes=0)
            db=sqlite3.connect(Path(folder)/"hot.sqlite3")
            self.assertEqual(db.execute("SELECT count(*) FROM records").fetchone()[0],4)
            self.assertLessEqual(comparisons.call_count,3)
            self.assertGreater(api.calls,5)
            db.close()

    def test_window_exceeded_and_id_reset_are_not_silently_deduplicated(self):
        with tempfile.TemporaryDirectory() as folder:
            db = Store(folder, min_free_bytes=0)
            db.ingest("TEST.x", [row(1), row(2)], "r1", "initial_backfill")
            db.ingest("TEST.x", [row(8), row(9)], "r2", "reconnect")
            db.ingest("TEST.x", [row(0, 999)], "r3", "reconnect")
            self.assertEqual(db.db.execute("SELECT count(*) FROM records").fetchone()[0], 5)
            self.assertEqual(db.db.execute("SELECT count(*) FROM gaps").fetchone()[0], 2)
            db.close()

    def test_disk_failure_and_transaction_failure_never_advance_checkpoint(self):
        with tempfile.TemporaryDirectory() as folder:
            db = Store(folder, min_free_bytes=0)
            db.ingest("TEST.x", [row(1)], "r", "initial_backfill")
            before = db.checkpoint("TEST.x")[0]
            db.min_free_bytes = 2**100
            with self.assertRaises(OSError):
                db.ingest("TEST.x", [row(2)], "r", "live")
            self.assertEqual(db.checkpoint("TEST.x")[0], before)
            db.min_free_bytes = 0
            db.db.execute("CREATE TRIGGER fail_second BEFORE INSERT ON records WHEN NEW.sdk_id=3 BEGIN SELECT RAISE(ABORT,'injected mid-transaction disk/SQL error'); END")
            db.db.commit()
            with self.assertRaises(sqlite3.IntegrityError):
                db.ingest("TEST.x", [row(2), row(3)], "r", "live", full_window=False)
            self.assertEqual(db.checkpoint("TEST.x")[0], before)
            self.assertEqual(db.db.execute("SELECT count(*) FROM records").fetchone()[0], 1)
            db.close()

    def test_kill_half_transaction_then_replay(self):
        with tempfile.TemporaryDirectory() as folder:
            db = Store(folder, min_free_bytes=0)
            db.ingest("TEST.x", [row(1)], "r", "initial_backfill")
            db.close()
            script = 'import sqlite3,sys,time; c=sqlite3.connect(sys.argv[1]); c.execute("BEGIN IMMEDIATE"); c.execute("INSERT INTO events(received_ns,kind) VALUES(1,\'half_write\')"); print("uncommitted",flush=True); time.sleep(60)'
            proc = subprocess.Popen([sys.executable, "-c", script, str(Path(folder)/"hot.sqlite3")], stdout=subprocess.PIPE, text=True)
            self.assertEqual(proc.stdout.readline().strip(), "uncommitted")
            proc.kill()
            proc.wait(timeout=5)
            proc.stdout.close()
            db = Store(folder, min_free_bytes=0)
            self.assertEqual(db.db.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertEqual(db.db.execute("SELECT count(*) FROM events WHERE kind='half_write'").fetchone()[0], 0)
            self.assertEqual(db.ingest("TEST.x", [row(1), row(2)], "restart", "replay")["written"], 1)
            db.close()

    def test_duplicate_schedule_lock(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/"lock"
            with InstanceLock(path):
                with self.assertRaisesRegex(RuntimeError, "already running"):
                    with InstanceLock(path):
                        pass

    def test_old_revisions_and_nan_payload_changes_are_selected(self):
        old = pd.DataFrame([row(i) for i in range(1, 4)])
        new = old.copy(deep=True)
        new.loc[0, "last_price"] = 102
        selected, full = changed_rows(new, old)
        self.assertFalse(full)
        self.assertEqual(list(selected["id"]), [1])
        reset = pd.DataFrame([row(9), row(10)])
        self.assertTrue(changed_rows(reset, old)[1])
        reused=pd.DataFrame([row(i,2_000_000_000+i) for i in range(1,4)])
        self.assertTrue(changed_rows(reused,old)[1])

    def test_probe_drains_during_subscription_initialization_and_immediately(self):
        with tempfile.TemporaryDirectory() as folder:
            first = pd.DataFrame([row(1), row(2)])
            serial = first.copy()
            path = Path(folder)/"ticks.csv"
            sink = ProbeStream(serial, first, path, lambda x: x.copy(), lambda: "now")
            # New rows arrive while another subscription initializes.
            serial.loc[2] = row(3)
            serial.loc[3] = row(4)
            sink.drain()
            self.assertEqual(list(pd.read_csv(path)["id"]), [3, 4])
            self.assertEqual(sink.cursor, 4)
            with patch.object(Path, "open", side_effect=OSError("disk failure")):
                serial.loc[4] = row(5)
                with self.assertRaises(OSError):
                    sink.drain()
            self.assertEqual(sink.cursor, 4)


if __name__ == "__main__":
    unittest.main()
