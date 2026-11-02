"""Durable last-source-event snapshot per second; subsecond data is intentionally reduced."""

from collections import defaultdict
from pathlib import Path
import json
import queue
import sqlite3
import struct
import threading
import time

from .codec import dumps, loads, sha256
from .runtime import atomic_json, redact_errors
from .storage_budget import Budget


DDL = """
CREATE TABLE IF NOT EXISTS snapshots(
 symbol TEXT, second_ns INTEGER, event_ns INTEGER, sdk_id INTEGER, raw BLOB,
 fingerprint TEXT, received_ns INTEGER, generation INTEGER, exported_generation INTEGER DEFAULT 0,
 origin TEXT DEFAULT 'source',
 PRIMARY KEY(symbol,second_ns));
CREATE INDEX IF NOT EXISTS snapshot_time ON snapshots(second_ns,symbol);
CREATE TABLE IF NOT EXISTS checkpoints(symbol TEXT PRIMARY KEY,event_ns INTEGER,tail TEXT,updated_ns INTEGER);
CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY,received_ns INTEGER,symbol TEXT,kind TEXT,detail TEXT);
CREATE TABLE IF NOT EXISTS source_stats(symbol TEXT PRIMARY KEY,windows INTEGER,source_rows INTEGER,selected_changes INTEGER,updated_ns INTEGER);
CREATE TABLE IF NOT EXISTS state(key TEXT PRIMARY KEY,value INTEGER);
CREATE TABLE IF NOT EXISTS health(seq INTEGER PRIMARY KEY,run TEXT,start_ns INTEGER,end_ns INTEGER,detail TEXT);
INSERT OR IGNORE INTO state VALUES('generation',0);
"""


class SecondStore:
    def __init__(self, root, target_start_ns, *, budget=None):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.start = int(target_start_ns)
        self.budget = budget or Budget(root)
        self.budget.check(force=True)
        self.db = sqlite3.connect(self.root / "seconds.sqlite3", timeout=30)
        for statement in ("PRAGMA journal_mode=WAL", "PRAGMA synchronous=FULL", "PRAGMA fullfsync=ON",
                          "PRAGMA checkpoint_fullfsync=ON", "PRAGMA cache_size=-32768"):
            self.db.execute(statement)
        self.db.executescript(DDL)
        if 'origin' not in {r[1] for r in self.db.execute('PRAGMA table_info(snapshots)')}:
            try:
                self.db.execute("ALTER TABLE snapshots ADD COLUMN origin TEXT DEFAULT 'before_origin_tracking'")
            except sqlite3.OperationalError as exc:
                if 'duplicate column' not in str(exc):
                    raise
        self.db.commit()

    def close(self):
        self.db.close()

    def event(self, symbol, kind, detail):
        cur=self.db.execute("INSERT INTO events(received_ns,symbol,kind,detail) VALUES(?,?,?,?)",
                            (time.time_ns(), symbol, kind, json.dumps(redact_errors(detail))))
        return cur.lastrowid

    def ingest(self, symbol, rows, *, tail=None, full_window=True, received_ns=None, origin="source"):
        received_ns = received_ns or time.time_ns()
        # Rank inside each source-event second, with stable source arrival as tie breaker.
        selected = {}
        for row in rows:
            event_ns = int(row["datetime"])
            if event_ns < self.start:
                continue
            second = event_ns // 1_000_000_000 * 1_000_000_000
            if second not in selected or event_ns >= int(selected[second]["datetime"]):
                selected[second] = row
        encoded = [(second, int(row["datetime"]), int(row["id"]), dumps(row)) for second, row in selected.items()]
        self.budget.check(sum(len(x[3]) for x in encoded) * 8 + 16*1024**2)
        prior = self.db.execute("SELECT event_ns,tail FROM checkpoints WHERE symbol=?", (symbol,)).fetchone()
        anchor_rows = list(tail if tail is not None else rows[-8:])
        anchors = [{"id": int(r["id"]), "time": int(r["datetime"]), "fp": sha256(dumps(r))} for r in anchor_rows[-8:]]
        detail = {"rows":len(rows), "origin":origin, "scope":"returned source window only"}
        with self.db:
            if full_window and prior and rows:
                old = json.loads(prior[1])
                needed = {x["fp"] for x in old}
                candidate_ids = {x['id'] for x in old}
                # IDs narrow the comparison only; exact content is still the anchor.
                overlap = any(sha256(dumps(r)) in needed for r in rows if int(r['id']) in candidate_ids)
                first = min(int(r["datetime"]) for r in rows)
                detail.update(content_anchor=overlap, first_source_ns=first,
                              last_source_ns=max(int(r["datetime"]) for r in rows))
                if not overlap and prior[0] >= self.start:
                    self.event(symbol,"source_window_unverified",{**detail,"previous_event_ns":prior[0],
                        'anchors':old,'start_ns':prior[0],'end_ns':max(prior[0],first),
                        "status":"gap_unrecoverable" if first>prior[0] and len(rows)>=10000 else "evidence_insufficient"})
                else:
                    self.event(symbol,"source_window_reconciled",detail)
            generation = self.db.execute("SELECT value FROM state WHERE key='generation'").fetchone()[0]
            changed = 0
            for second, event_ns, sdk_id, raw in encoded:
                fp = sha256(raw)
                previous = self.db.execute("SELECT event_ns,fingerprint,raw,exported_generation,generation FROM snapshots WHERE symbol=? AND second_ns=?",(symbol,second)).fetchone()
                if previous and (event_ns < previous[0] or fp == previous[1] or (origin=='legacy_cutover' and event_ns<=previous[0])):
                    continue
                if previous and second < received_ns//1_000_000_000*1_000_000_000-2_000_000_000:
                    self.event(symbol,"late_second_revision",{"second_ns":second,"previous_fingerprint":previous[1],
                        "new_fingerprint":fp,"previous_raw_hex":bytes(previous[2]).hex(),"previous_generation":previous[4]})
                generation += 1
                self.db.execute("INSERT INTO snapshots(symbol,second_ns,event_ns,sdk_id,raw,fingerprint,received_ns,generation,exported_generation,origin) VALUES(?,?,?,?,?,?,?,?,0,?) ON CONFLICT(symbol,second_ns) DO UPDATE SET event_ns=excluded.event_ns,sdk_id=excluded.sdk_id,raw=excluded.raw,fingerprint=excluded.fingerprint,received_ns=excluded.received_ns,generation=excluded.generation,origin=excluded.origin",
                                (symbol,second,event_ns,sdk_id,raw,fp,received_ns,generation,origin))
                changed += 1
            self.db.execute("UPDATE state SET value=? WHERE key='generation'",(generation,))
            if anchors:
                self.db.execute("INSERT INTO checkpoints VALUES(?,?,?,?) ON CONFLICT(symbol) DO UPDATE SET event_ns=excluded.event_ns,tail=excluded.tail,updated_ns=excluded.updated_ns",
                    (symbol,max(x['time'] for x in anchors),json.dumps(anchors),received_ns))
            self.db.execute("INSERT INTO source_stats VALUES(?,1,?,?,?) ON CONFLICT(symbol) DO UPDATE SET windows=windows+1,source_rows=source_rows+excluded.source_rows,selected_changes=selected_changes+excluded.selected_changes,updated_ns=excluded.updated_ns",
                            (symbol,len(rows),changed,received_ns))
        return {"changed_seconds":changed,"generation":generation,"source_rows":len(rows)}


class SecondWriter:
    def __init__(self, root, target, *, capacity=16):
        self.root, self.target = root, target
        self.inbox, self.acks = queue.Queue(capacity), queue.Queue()
        self.error = None
        self.thread = threading.Thread(target=self._work, daemon=True)
        self.thread.start()

    def _read_import(self, import_path, symbol):
        """Reject an isolated bad recovery file before it reaches a transaction."""
        try:
            folder=(Path(self.root)/'imports').resolve()
            path=Path(import_path)
            if path.is_symlink() or not path.resolve().is_relative_to(folder):
                raise ValueError('Unsafe second-snapshot ready path')
            info=json.loads(path.read_text())
            if info['symbol']!=symbol:
                raise ValueError('Recovery request symbol mismatch')
            data=Path(info['data_file'])
            if data.is_symlink() or not data.resolve().is_relative_to(folder):
                raise ValueError('Unsafe second-snapshot import path')
            if data.stat().st_size>64*1024**2:
                raise ValueError('Recovery chunk exceeds 64 MiB; split it before import')
            raw=data.read_bytes()
            if sha256(raw)!=info['raw_sha256']:
                raise ValueError('Second-snapshot import checksum mismatch')
            rows=[loads(line) for line in raw.splitlines(keepends=True)]
            if any(str(r['symbol'])!=symbol or int(r['id'])<0 or int(r['datetime'])<=0 for r in rows):
                raise ValueError('Recovery record symbol/time/ID mismatch')
            return info,rows
        except (OSError,KeyError,TypeError,UnicodeError,IndexError,AttributeError,OverflowError,struct.error) as exc:
            raise ValueError('Unreadable or invalid second-snapshot import: '+type(exc).__name__) from exc

    def _work(self):
        store = None
        try:
            store = SecondStore(self.root,self.target)
            while True:
                item = self.inbox.get()
                if item is None:
                    break
                token,symbol,rows,kwargs = item
                import_path=kwargs.pop('import_path',None)
                if import_path:
                    try:
                        info,rows=self._read_import(import_path,symbol)
                    except ValueError as exc:
                        result={'changed_seconds':0,'error':str(exc),'error_type':type(exc).__name__}
                        with store.db:
                            store.event(symbol,'second_import_rejected',{'import':Path(import_path).name,'error':str(exc)})
                    else:
                        result=store.ingest(symbol,rows,tail=[],full_window=False,origin=info.get('origin','legacy_cutover'))
                        with store.db:
                            store.event(symbol,'durable_second_import',{'import':Path(import_path).name,
                                'raw_sha256':info['raw_sha256'],'rows':len(rows),'changed_seconds':result['changed_seconds'],
                                'source_provenance':info.get('source_provenance')})
                            if info.get('replay_evidence'):
                                store.event(symbol,'gap_replay_result',{'gap_event_id':info.get('gap_event_id'),**info['replay_evidence']})
                        if info.get('replay_evidence'):
                            result['coverage']=info['replay_evidence']
                else:
                    result = store.ingest(symbol,rows,**kwargs)
                self.acks.put((token,result))  # only after FULL commit
        except BaseException as exc:
            self.error = exc
        finally:
            if store:
                store.close()

    def check(self):
        if self.error:
            raise RuntimeError(f"Second writer failed: {self.error}") from self.error

    def close(self):
        while self.thread.is_alive():
            self.check()
            try:
                self.inbox.put(None,timeout=.2)
                break
            except queue.Full:
                pass
        self.thread.join(30)
        self.check()
        if self.thread.is_alive():
            raise TimeoutError("Second writer did not drain; source overlap required")
