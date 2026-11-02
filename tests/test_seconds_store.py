import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

import numpy as np
import pandas as pd

from tick_futures.codec import dumps, loads, sha256
from tick_futures.runtime import atomic_json
from tick_futures.seconds_archive import seal_seconds, audit_seconds
from tick_futures.seconds_store import SecondStore, SecondWriter
from tick_futures.storage_budget import Budget, logical_bytes
from tick_futures.archive import restore, parent_chain
from tick_futures.seconds_collector import valid_source_frame, current_ready_count
from tick_futures.collector import changed_rows


def row(ns, identity, price=1.0):
    return {'datetime':np.int64(ns),'id':np.int64(identity),'last_price':np.float64(price),
            'symbol':'SHFE.au2612','amount':None,'bid_price5':np.float64('nan')}


class SecondsPersistence(unittest.TestCase):
    def test_old_ready_main_cannot_mask_unready_new_main(self):
        self.assertEqual(current_ready_count({'old':{'state':'ready'},'new':{'state':'initializing'}},
                                             {'symbols':['new'],'count':1}),0)

    def test_bad_recovery_file_does_not_kill_live_writer(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);folder=root/'imports';folder.mkdir()
            writer=SecondWriter(root,0)
            for kind in ('missing','checksum','wrong_symbol'):
                data=folder/(kind+'.tfrec')
                source=row(100_000_000_000,1)
                if kind=='wrong_symbol':source['symbol']='DCE.m2701'
                raw=dumps(source)
                if kind!='missing':data.write_bytes(raw)
                path=folder/('ready-'+kind+'.json')
                atomic_json(path,{'symbol':'SHFE.au2612','data_file':str(data),'raw_sha256':sha256(raw) if kind!='checksum' else 'bad'})
                writer.inbox.put((kind,'SHFE.au2612',None,{'import_path':str(path)}))
                token,result=writer.acks.get(timeout=15)
                self.assertEqual(token,kind);self.assertIn('error',result)
            writer.inbox.put(('live','SHFE.au2612',[row(101_000_000_000,2)],{}))
            token,result=writer.acks.get(timeout=15)
            self.assertEqual(token,'live');self.assertEqual(result['changed_seconds'],1)
            writer.close()
            db=sqlite3.connect(root/'seconds.sqlite3')
            self.assertEqual(db.execute('SELECT count(*) FROM snapshots').fetchone()[0],1)
            self.assertEqual(db.execute("SELECT count(*) FROM events WHERE kind='second_import_rejected'").fetchone()[0],3)
            db.close()

    def test_sdk_padding_cannot_force_repeated_full_window_imports(self):
        before=pd.DataFrame({'id':np.array([-1,-1,5,6],dtype='float64'),
            'datetime':np.array([0,0,100_000_000_000,100_500_000_000],dtype='float64'),
            'last_price':np.array([np.nan,np.nan,10,11],dtype='float64')})
        after=pd.DataFrame({'id':np.array([-1,5,6,7,8],dtype='float64'),
            'datetime':np.array([0,100_000_000_000,100_500_000_000,101_000_000_000,101_500_000_000],dtype='float64'),
            'last_price':np.array([np.nan,10,11,12,13],dtype='float64')})
        delta,full=changed_rows(valid_source_frame(after),valid_source_frame(before))
        self.assertFalse(full)
        self.assertEqual(list(delta['id']),[7,8])

    def store(self, root, start=0):
        return SecondStore(root,start,budget=Budget(root,paths=[root],maximum=10**12,pause_at=9*10**11,min_free=0))

    def test_same_second_last_snapshot_and_exact_special_bits(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);store=self.store(root)
            first=row(100_000_000_001,1)
            last=row(100_500_000_001,2,-0.0)
            store.ingest('SHFE.au2612',[first,last],received_ns=101_000_000_000)
            saved=store.db.execute('SELECT raw FROM snapshots').fetchall()
            self.assertEqual(len(saved),1)
            self.assertEqual(saved[0][0],dumps(last))
            self.assertTrue(np.signbit(loads(saved[0][0])['last_price']))
            store.close()

    def test_projection_audit_keeps_values_and_reports_zero_evidence_sessions(self):
        from datetime import datetime
        from tick_futures.common import CST
        from tick_futures.sessions import epoch_ns
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);(root/'discovery').mkdir()
            atomic_json(root/'service-config.json',{'target_start_ns':0,'prepared_trading_day':'2026-11-03'})
            atomic_json(root/'discovery/main-latest.json',{'symbols':['SHFE.au2612']})
            atomic_json(root/'discovery/calendar.json',{'days':[{'date':'2026-11-02','trading':True},{'date':'2026-11-03','trading':True}]})
            (root/'discovery/main-metadata-test.tfrec').write_bytes(dumps([{'instrument_id':'SHFE.au2612','product_id':'au',
                'trading_time_day':[['09:00:00','10:00:00']],'trading_time_night':[]}]))
            base=epoch_ns(datetime(2026,11,3,9,tzinfo=CST))
            first=row(base,2);first.update(amount=np.float64(10),volume=np.int64(4))
            last=row(base+1_000_000_000,1,-0.0);last.update(amount=np.float64(9),volume=np.int64(3),new_field=None)
            store=self.store(root);store.ingest('SHFE.au2612',[first,last]);store.close()
            report=audit_seconds(root,scan_values=True)
            self.assertEqual(report['projection_transition_flags']['amount:decreased'],1)
            self.assertEqual(report['projection_transition_flags']['id:decreased'],1)
            self.assertEqual(report['projection_transition_flags']['schema:added:new_field'],1)
            self.assertTrue(any(x['rows']==0 and x['session']=='day-0-preopen' for x in report['ledger']))
            self.assertFalse(report['upstream_zero_loss_proven'])
            store=self.store(root)
            self.assertEqual(store.db.execute('SELECT raw FROM snapshots ORDER BY second_ns DESC LIMIT 1').fetchone()[0],dumps(last))
            store.close()

    def test_initialization_coalesced_updates_same_timestamp_and_restart(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);store=self.store(root)
            data=[row(100_000_000_000,1),row(100_500_000_000,2),row(101_000_000_000,3),row(101_000_000_000,4,2.0)]
            store.ingest('SHFE.au2612',data)
            store.close();store=self.store(root)
            self.assertEqual(store.ingest('SHFE.au2612',data)['changed_seconds'],0)
            self.assertEqual(store.db.execute('SELECT count(*) FROM snapshots').fetchone()[0],2)
            self.assertEqual(loads(store.db.execute('SELECT raw FROM snapshots WHERE second_ns=101000000000').fetchone()[0])['id'],4)
            self.assertEqual(store.db.execute("SELECT count(*) FROM events WHERE kind='source_window_reconciled'").fetchone()[0],1)
            store.close()

    def test_target_cutoff_and_older_bootstrap_cannot_move_live_checkpoint(self):
        with tempfile.TemporaryDirectory() as temp:
            store=self.store(Path(temp),100_000_000_000)
            store.ingest('SHFE.au2612',[row(99_000_000_000,1),row(101_500_000_000,3)])
            cp=store.db.execute('SELECT event_ns,tail FROM checkpoints').fetchone()
            store.ingest('SHFE.au2612',[row(100_000_000_000,2),row(101_500_000_000,3,99)],tail=[],full_window=False,origin='legacy_cutover')
            self.assertEqual(store.db.execute('SELECT event_ns,tail FROM checkpoints').fetchone(),cp)
            self.assertEqual(store.db.execute('SELECT count(*) FROM snapshots').fetchone()[0],2)
            self.assertEqual(loads(store.db.execute('SELECT raw FROM snapshots WHERE second_ns=101000000000').fetchone()[0])['last_price'],1.0)
            store.close()

    def test_failed_transaction_rolls_back_candidate_and_checkpoint(self):
        with tempfile.TemporaryDirectory() as temp:
            store=self.store(Path(temp))
            store.db.execute("CREATE TRIGGER fail_checkpoint BEFORE INSERT ON checkpoints BEGIN SELECT RAISE(ABORT,'injected disk transaction failure'); END")
            with self.assertRaises(sqlite3.IntegrityError):
                store.ingest('SHFE.au2612',[row(100_000_000_000,1)])
            self.assertEqual(store.db.execute('SELECT count(*) FROM snapshots').fetchone()[0],0)
            self.assertEqual(store.db.execute('SELECT count(*) FROM checkpoints').fetchone()[0],0)
            store.close()

    def test_no_content_anchor_records_scope_not_a_false_continuity_pass(self):
        with tempfile.TemporaryDirectory() as temp:
            store=self.store(Path(temp))
            store.ingest('SHFE.au2612',[row(100_000_000_000,1)])
            store.ingest('SHFE.au2612',[row(200_000_000_000,1,5)])
            kind,detail=store.db.execute('SELECT kind,detail FROM events').fetchone()
            self.assertEqual(kind,'source_window_unverified')
            self.assertEqual(json.loads(detail)['status'],'evidence_insufficient')
            store.close()

    def test_quota_is_checked_before_candidate_or_checkpoint_advance(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);store=self.store(root)
            before=logical_bytes([root]);store.budget=Budget(root,paths=[root],maximum=before+10_000,pause_at=before+5_000,min_free=0)
            with self.assertRaisesRegex(OSError,'budget pause'):
                store.ingest('SHFE.au2612',[row(100_000_000_000,1)])
            self.assertEqual(store.db.execute('SELECT count(*) FROM checkpoints').fetchone()[0],0)
            self.assertEqual(store.db.execute('SELECT count(*) FROM snapshots').fetchone()[0],0)
            store.close()

    def test_cold_revision_parent_chain_and_empty_directory_restore(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)/'source';root.mkdir();(root/'discovery').mkdir()
            atomic_json(root/'service-config.json',{'target_start_ns':0,'prepared_trading_day':'2026-11-03'})
            atomic_json(root/'discovery/main-latest.json',{'symbols':['SHFE.au2612'],'version':'test'})
            store=self.store(root);old=row(100_000_000_000,1);new=row(100_500_000_000,2,3.0)
            store.ingest('SHFE.au2612',[old]);first=seal_seconds(root)
            store.ingest('SHFE.au2612',[new]);second=seal_seconds(root)
            index=json.loads((root/'seal-index.json').read_text());manifests={}
            for item in index['archives']:
                path=Path(item['path']);m=json.loads((path/'manifest.json').read_text());manifests[m['archive_id']]=m
                output=Path(temp)/item['archive_id'];restore(path,output)
                self.assertEqual((output/'seconds/snapshots.tfrec').read_bytes(),dumps(old if item['archive_id']==first['archive_id'] else new))
            self.assertEqual(len(parent_chain(manifests,second['archive_id'])),2)
            self.assertFalse(list((root/'staging').iterdir()))
            self.assertFalse(list((root/'verification').iterdir()))
            self.assertTrue(store.db.execute("SELECT count(*) FROM events WHERE kind='late_second_revision'").fetchone()[0])
            store.close()

    def test_async_ack_only_after_selected_snapshot_and_source_checkpoint_commit(self):
        with tempfile.TemporaryDirectory() as temp:
            writer=SecondWriter(Path(temp),0)
            writer.inbox.put(('one','SHFE.au2612',[row(100_000_000_000,1)],{}))
            token,result=writer.acks.get(timeout=15)
            db=sqlite3.connect(Path(temp)/'seconds.sqlite3')
            self.assertEqual(token,'one');self.assertEqual(result['changed_seconds'],1)
            self.assertEqual(db.execute('SELECT count(*) FROM snapshots').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT count(*) FROM checkpoints').fetchone()[0],1)
            db.close();writer.close()
