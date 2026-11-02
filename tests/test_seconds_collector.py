"""Deterministic receiver integration with synthetic frames; no SDK connection."""

import asyncio
import json
from pathlib import Path
import signal
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd

from tick_futures.codec import loads
from tick_futures.runtime import atomic_json
from tick_futures.seconds_collector import collect_seconds, read_control_file


class SyntheticApi:
    def __init__(self, handlers, symbols):
        self.loop=asyncio.new_event_loop()
        self.handlers=handlers;self.symbols=symbols;self.frames={};self.steps=0;self.closed=False

    def create_task(self, coroutine):
        return self.loop.create_task(coroutine)

    def get_tick_serial(self, symbol, data_length):
        self.frames[symbol]=pd.DataFrame({'datetime':np.array([100_000_000_000],dtype='int64'),
            'id':np.array([1],dtype='int64'),'last_price':np.array([-0.0]),'symbol':[symbol]})
        return self.frames[symbol]

    def is_serial_ready(self, frame):
        symbol=frame.iloc[0]['symbol']
        return self.steps>=(1,30,45)[self.symbols.index(symbol)]

    def is_changing(self, frame):
        return True

    def wait_update(self, deadline):
        self.steps+=1
        self.loop.run_until_complete(asyncio.sleep(0))
        frame=self.frames.get(self.symbols[0])
        if frame is not None and self.steps==12:
            # Multiple delivered records in a single notification, including a
            # same-second replacement, while other subscriptions are unready.
            frame.loc[1]=[101_000_000_001,2,2.0,self.symbols[0]]
            frame.loc[2]=[101_500_000_001,3,3.0,self.symbols[0]]
        if frame is not None and self.steps==18:
            frame.loc[3]=[102_000_000_001,4,4.0,self.symbols[0]]
        if self.steps==80:
            self.handlers[signal.SIGTERM](signal.SIGTERM,None)
        time.sleep(.003)
        return True

    def close(self):
        self.loop.close();self.closed=True


class SecondsReceiver(unittest.TestCase):
    def test_malformed_control_file_is_preserved_and_isolated(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);path=root/'ready-invalid.json';raw=b'{"incomplete"'
            path.write_bytes(raw)
            with patch('tick_futures.seconds_collector.alert'):
                self.assertIsNone(read_control_file(root,path))
            self.assertEqual(next(root.glob('*.invalid-*')).read_bytes(),raw)

    def test_updates_are_durable_before_all_subscriptions_finish_initializing(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);symbols=['SHFE.au2612','DCE.m2701','CFFEX.IF2612']
            atomic_json(root/'discovery/calendar.json',{'days':[]})
            config={'target_start_ns':0,'prepared_trading_day':'2026-11-03','allow_replay':False,
                    'initialization_concurrency':3,'data_length':1000}
            handlers={}
            def register(sig, handler):
                old=handlers.get(sig,signal.SIG_DFL);handlers[sig]=handler;return old
            api=SyntheticApi(handlers,symbols)
            async def plan(*_):
                return {'symbols':symbols,'count':3,'version':'synthetic','excluded':[]},[
                    {'instrument_id':s,'trading_time_day':[],'trading_time_night':[]} for s in symbols]
            with patch('tick_futures.seconds_collector.query_main_plan',plan),patch('tick_futures.server.check_time_alignment'),patch('tick_futures.seconds_collector.signal.signal',side_effect=register):
                result=collect_seconds(root,config,api_factory=lambda:api)
            self.assertTrue(api.closed);self.assertEqual(result['ready'],3)
            self.assertEqual(result['status'],'stopped')
            db=sqlite3.connect(root/'seconds.sqlite3')
            rows=[loads(raw) for raw, in db.execute('SELECT raw FROM snapshots WHERE symbol=? ORDER BY second_ns',(symbols[0],))]
            self.assertEqual([int(r['id']) for r in rows],[1,3,4])
            self.assertEqual(int(rows[1]['datetime']),101_500_000_001)
            self.assertEqual(db.execute('SELECT count(*) FROM checkpoints').fetchone()[0],3)
            db.close()
            heartbeat=json.loads((root/'heartbeat.json').read_text())
            self.assertEqual(heartbeat['pending'],0)
            self.assertEqual(heartbeat['queue'],0)
