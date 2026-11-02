import sqlite3
import struct
from pathlib import Path
import tempfile
import unittest

import numpy as np
import pandas as pd

from tick_futures.codec import dumps
from tick_futures.preparation import encode_rows
from tick_futures.store import DurableWriter,Store


class ParallelPreparation(unittest.TestCase):
    def test_two_processes_keep_exact_bits_types_order_and_commit_ack(self):
        nan=struct.unpack(">d",bytes.fromhex("7ff8000000000042"))[0]
        first=[{"id":np.int64(i),"datetime":np.int64(1793600000000000001+i),
                "price":np.float64(nan),"zero":-0.0,"missing":pd.NA} for i in range(120)]
        last={"id":np.int64(1),"datetime":np.int64(1793600000000000201),"extra":None}
        with tempfile.TemporaryDirectory() as folder:
            writer=DurableWriter(folder,min_free_bytes=0,preparation_workers=2)
            writer.submit("first","TEST.a",first,"run","initial_backfill")
            writer.submit("last","TEST.b",[last],"run","initial_backfill")
            self.assertEqual(writer.acks.get(timeout=30)[0],"first")
            self.assertEqual(writer.acks.get(timeout=30)[0],"last")
            check=sqlite3.connect(Path(folder)/"hot.sqlite3")
            self.assertEqual([r[0] for r in check.execute("SELECT raw FROM records ORDER BY seq")],[dumps(r) for r in first]+[dumps(last)])
            self.assertEqual(check.execute("SELECT count(*) FROM checkpoints").fetchone()[0],2)
            check.close()
            writer.close()

    def test_corrupt_preparation_cannot_advance_checkpoint(self):
        row={"id":1,"datetime":1793600000000000001}
        with tempfile.TemporaryDirectory() as folder:
            store=Store(folder,min_free_bytes=0)
            raw,_=encode_rows([row])[0]
            with self.assertRaisesRegex(ValueError,"hash/count mismatch"):
                store.ingest("TEST.a",[row],"run","live",encoded_rows=[(raw,"wrong")])
            self.assertEqual(store.checkpoint("TEST.a"),(0,[]))
            self.assertEqual(store.db.execute("SELECT count(*) FROM records").fetchone()[0],0)
            store.close()
