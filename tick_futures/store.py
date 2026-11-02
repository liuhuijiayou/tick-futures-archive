"""SQLite WAL/FULL is the append log; records and checkpoints commit together.

Rows are never UPDATEd or DELETEd. Overlap replay suppression is recorded as an
observation. IDs are scoped evidence, never a database uniqueness constraint.
"""

from collections import Counter
import json
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
from pathlib import Path
import queue
import shutil
import sqlite3
import threading
import time

from .codec import dumps, loads, sha256
from .runtime import redact_errors
from .preparation import encode_rows,prepare_journal,initialize_worker

DDL = """
CREATE TABLE IF NOT EXISTS records (
 seq INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL, run TEXT NOT NULL,
 received_ns INTEGER NOT NULL, event_ns INTEGER NOT NULL, sdk_id INTEGER NOT NULL,
 origin TEXT NOT NULL, fingerprint TEXT NOT NULL, raw BLOB NOT NULL);
CREATE INDEX IF NOT EXISTS records_symbol_id ON records(symbol,sdk_id,seq);
CREATE INDEX IF NOT EXISTS records_symbol_seq ON records(symbol,seq);
CREATE TABLE IF NOT EXISTS checkpoints (
 symbol TEXT PRIMARY KEY, seq INTEGER NOT NULL, tail BLOB NOT NULL, updated_ns INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS observations (
 seq INTEGER PRIMARY KEY, symbol TEXT, run TEXT, received_ns INTEGER, detail TEXT);
CREATE TABLE IF NOT EXISTS events (
 seq INTEGER PRIMARY KEY, received_ns INTEGER, symbol TEXT, kind TEXT, detail TEXT);
CREATE TABLE IF NOT EXISTS gaps (
 seq INTEGER PRIMARY KEY, symbol TEXT, detected_ns INTEGER, kind TEXT, status TEXT,
 start_ns INTEGER, end_ns INTEGER, detail TEXT);
CREATE TABLE IF NOT EXISTS runs (
 run TEXT PRIMARY KEY, started_ns INTEGER, ended_ns INTEGER, status TEXT, detail TEXT);
CREATE TABLE IF NOT EXISTS contracts (
 symbol TEXT PRIMARY KEY, version TEXT, target_start_ns INTEGER, earliest_ns INTEGER,
 live_enabled_ns INTEGER, status TEXT, detail TEXT);
"""


def fingerprints(rows):
    return [sha256(dumps(row)) for row in rows]


def overlap_plan(old_tail, rows, persisted_fingerprints=None, *, new_fingerprints=None):
    """Use exact content anchors, never latest-ID reset or timestamp equality.

    Revisions in the retained overlap are appended. If continuity is ambiguous,
    append the entire available window and explicitly leave an unknown gap.
    """
    if not old_tail:
        return rows, {"decision": "initial_window", "replayed": 0}
    new_fp = fingerprints(rows) if new_fingerprints is None else new_fingerprints
    old_fp = fingerprints(old_tail)
    # Longest suffix match is preferred; a changed last record can use an older anchor.
    for old_index in range(len(old_tail) - 1, -1, -1):
        matches = [i for i, fp in enumerate(new_fp) if fp == old_fp[old_index]]
        if not matches:
            continue
        if len(matches) != 1:
            return rows, {"decision": "ambiguous_overlap", "replayed": 0, "gap": True}
        anchor = matches[0]
        known = Counter(persisted_fingerprints if persisted_fingerprints is not None else old_fp)
        old_ids = {int(row["id"]) for row in old_tail}
        chosen = []
        for i, fp in enumerate(new_fp):
            if known[fp]:
                known[fp] -= 1
            else:
                chosen.append(i)
        revised = [i for i in chosen if i <= anchor and int(rows[i]["id"]) in old_ids]
        return [rows[i] for i in chosen], {"decision": "content_overlap", "replayed": len(rows) - len(chosen),
                    "anchor_fingerprint": new_fp[anchor], "anchor_id": int(rows[anchor]["id"]),
                    "retained_overlap_revisions": len(revised)}
    return rows, {"decision": "no_content_overlap", "replayed": 0, "gap": True}


class Store:
    def __init__(self, root, *, min_free_bytes=512 * 1024 * 1024):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.path = self.root / "hot.sqlite3"
        self.min_free_bytes = min_free_bytes
        self.db = sqlite3.connect(self.path, timeout=30)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA fullfsync=ON")
        self.db.execute("PRAGMA checkpoint_fullfsync=ON")
        self.db.execute("PRAGMA cache_size=-65536")  # bounded 64 MiB, allocated on demand
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.executescript(DDL)
        self.db.commit()

    def close(self):
        self.db.close()

    def event(self, symbol, kind, detail):
        self.db.execute("INSERT INTO events(received_ns,symbol,kind,detail) VALUES(?,?,?,?)",
                        (time.time_ns(), symbol, kind, json.dumps(redact_errors(detail), ensure_ascii=False)))

    def gap(self, symbol, kind, detail, *, status="evidence_insufficient", start_ns=None, end_ns=None):
        cur=self.db.execute("INSERT INTO gaps(symbol,detected_ns,kind,status,start_ns,end_ns,detail) VALUES(?,?,?,?,?,?,?)",
                        (symbol, time.time_ns(), kind, status, start_ns, end_ns, json.dumps(redact_errors(detail))))
        self.event(symbol,"gap_created",{"gap_id":cur.lastrowid,"kind":kind,"status":status,
                   "start_ns":start_ns,"end_ns":end_ns,"detail":detail})
        return cur.lastrowid

    def checkpoint(self, symbol):
        item = self.db.execute("SELECT seq,tail FROM checkpoints WHERE symbol=?", (symbol,)).fetchone()
        return (item[0], loads(item[1])) if item else (0, [])

    def ingest(self, symbol, rows, run, origin, *, received_ns=None, full_window=True, tail=None,
               window_info=None, update_checkpoint=True, encoded_rows=None):
        if not rows and tail is None:
            return {"written": 0}
        if shutil.disk_usage(self.root).free < self.min_free_bytes:
            raise OSError("Disk free space below collector reserve; checkpoint not advanced")
        encoded=encode_rows(rows) if encoded_rows is None else encoded_rows
        if len(encoded)!=len(rows) or any(sha256(raw)!=fp for raw,fp in encoded):
            raise ValueError("Prepared raw record hash/count mismatch; checkpoint not advanced")
        raw_by_identity={id(row):item for row,item in zip(rows,encoded)}
        input_fingerprints=[fp for raw,fp in encoded]
        received_ns = received_ns or time.time_ns()
        previous_seq, old_tail = self.checkpoint(symbol)
        full_first_ns=int(rows[0]["datetime"]) if rows else None
        full_last_ns=int(rows[-1]["datetime"]) if rows else None
        input_count = len(rows)
        current_tail = (tail if tail is not None else rows[-64:])
        if full_window:
            known = None
            if (old_tail or not update_checkpoint) and rows:
                lo, hi = min(int(r["id"]) for r in rows), max(int(r["id"]) for r in rows)
                time_lo,time_hi=min(int(r["datetime"]) for r in rows),max(int(r["datetime"]) for r in rows)
                known = [r[0] for r in self.db.execute("SELECT fingerprint FROM records WHERE symbol=? AND sdk_id BETWEEN ? AND ? AND event_ns BETWEEN ? AND ?", (symbol, lo, hi,time_lo,time_hi))]
            if not update_checkpoint:
                remaining=Counter(known or [])
                selected=[]
                for row,fp in zip(rows,input_fingerprints):
                    if remaining[fp]:
                        remaining[fp]-=1
                    else:
                        selected.append(row)
                detail={"decision":"historical_content_replay_checked","replayed":len(rows)-len(selected)}
            else:
                selected, detail = overlap_plan(old_tail, rows, known,new_fingerprints=input_fingerprints)
        else:
            detail = {"decision": "committed_window_diff", "replayed": 0}
            selected = rows
        # Preserve the application delivery order even for idempotent replays.
        # Each fingerprint resolves to an immutable canonical raw BLOB.
        detail["input_fingerprints_in_order"] = input_fingerprints
        rows = selected
        latest_by_id={}
        if rows:
            lo,hi=min(int(r["id"]) for r in rows),max(int(r["id"]) for r in rows)
            # One range read replaces one indexed SELECT per inserted Tick.
            # Later entries win exactly as ORDER BY seq DESC LIMIT 1 did.
            latest_by_id=dict(self.db.execute("SELECT sdk_id,fingerprint FROM records WHERE symbol=? AND sdk_id BETWEEN ? AND ? ORDER BY seq",(symbol,lo,hi)))
        detail.update({"input_rows": input_count, "window": window_info or {}, "origin": origin})
        last_seq = previous_seq
        first_written_seq=None
        previous = old_tail[-1] if old_tail and update_checkpoint else None
        with self.db:
            if detail.get("gap"):
                first = current_tail[0] if current_tail else None
                window_first = (window_info or {}).get("first_event_ns", int(first["datetime"]) if first else None)
                prior_ns = int(old_tail[-1]["datetime"])
                out_of_window = window_first is not None and window_first > prior_ns
                self.gap(symbol, "window_exceeded" if out_of_window else "id_reset_or_source_change",
                         {"previous_seq": previous_seq, "decision": detail["decision"],
                          "note": "No content anchor; all available rows retained; ID semantics not assumed"},
                         status=("gap_pending_replay" if (window_info or {}).get("configured_rows",10000)<10000 else "gap_unrecoverable") if out_of_window else "evidence_insufficient",
                         start_ns=prior_ns, end_ns=window_first)
            for row in rows:
                raw,fingerprint=raw_by_identity[id(row)]
                event_ns, sdk_id = int(row["datetime"]), int(row["id"])
                same_id = latest_by_id.get(sdk_id)
                if same_id and same_id != fingerprint:
                    self.event(symbol, "same_id_different_content", {"sdk_id": sdk_id, "previous_fingerprint": same_id,
                               "fingerprint": fingerprint, "meaning": "revision or reset; both retained"})
                latest_by_id[sdk_id]=fingerprint
                if previous is not None:
                    flags = audit_pair(previous, row)
                    if flags:
                        self.event(symbol, "quality", {"sdk_id": sdk_id, "flags": flags})
                cur = self.db.execute("INSERT INTO records(symbol,run,received_ns,event_ns,sdk_id,origin,fingerprint,raw) VALUES(?,?,?,?,?,?,?,?)",
                         (symbol, run, received_ns, event_ns, sdk_id, origin, fingerprint, raw))
                last_seq = cur.lastrowid
                if first_written_seq is None:
                    first_written_seq=last_seq
                previous = row
            detail["written"] = len(rows)
            detail["first_written_seq"]=first_written_seq
            detail["last_written_seq"]=last_seq
            self.db.execute("INSERT INTO observations(symbol,run,received_ns,detail) VALUES(?,?,?,?)",
                            (symbol, run, received_ns, json.dumps(detail)))
            if full_window and old_tail and detail["decision"] == "content_overlap":
                self.event(symbol, "same_source_window_reconciled", {"previous_checkpoint_seq": previous_seq,
                           "new_checkpoint_seq": last_seq, "anchor_fingerprint": detail["anchor_fingerprint"],
                           "scope": "returned SDK window only; upstream completeness not proven"})
            if update_checkpoint:
                self.db.execute("INSERT INTO checkpoints VALUES(?,?,?,?) ON CONFLICT(symbol) DO UPDATE SET seq=excluded.seq,tail=excluded.tail,updated_ns=excluded.updated_ns",
                                (symbol, last_seq, dumps(current_tail), received_ns))
            self.db.execute("UPDATE contracts SET earliest_ns=CASE WHEN earliest_ns IS NULL THEN ? ELSE min(earliest_ns,?) END,status='subscribed' WHERE symbol=?",
                            (min((int(r["datetime"]) for r in rows), default=received_ns),
                             min((int(r["datetime"]) for r in rows), default=received_ns), symbol))
            if full_window and (window_info or {}).get("configured_rows",0)>=10000 and (window_info or {}).get("source_journal_gap_count",0)==0 and (detail["decision"]=="content_overlap" or (detail["decision"]=="historical_content_replay_checked" and detail["replayed"]>0)):
                for gap_id,kind,start,end,gap_detail in self.db.execute("SELECT seq,kind,start_ns,end_ns,detail FROM gaps WHERE symbol=? AND status='gap_pending_replay'",(symbol,)).fetchall():
                    if kind not in ("startup_window_after_target","window_exceeded") or start is None or end is None:
                        continue
                    if full_first_ns is None or full_first_ns>start or full_last_ns<end:
                        continue
                    prior=json.loads(gap_detail).get("previous_seq")
                    if prior:
                        anchor=self.db.execute("SELECT fingerprint FROM records WHERE seq=?",(prior,)).fetchone()
                        if not anchor or anchor[0] not in detail["input_fingerprints_in_order"]:
                            continue
                    self.event(symbol,"gap_closed_by_source_window",{"gap_id":gap_id,"previous_status":"gap_pending_replay", "status":"window_verified",
                               "first_ns":full_first_ns,"last_ns":full_last_ns,"scope":"returned SDK window; upstream unknown"})
                    self.db.execute("UPDATE gaps SET status='window_verified' WHERE seq=?",(gap_id,))
        return {"written": len(rows), "seq": last_seq, "decision": detail["decision"]}

    def ingest_journal(self,path,symbol,run,info,*,prepared=None):
        """Cold helper output is reconciled by the single durable writer.

        Historical arrival can never roll the live checkpoint back or forward.
        """
        prepared=prepare_journal(path,symbol,info) if prepared is None else prepared
        return self.ingest(symbol,prepared["rows"],run,"recovery_backfill",received_ns=prepared["received_ns"],
            full_window=True,update_checkpoint=False,window_info=prepared["window_info"],encoded_rows=prepared["encoded"])


def audit_pair(previous, row):
    flags = []
    for key in ("volume", "amount"):
        if key in previous and key in row:
            try:
                if row[key] < previous[key]:
                    flags.append(key + "_decreased")
            except (TypeError, ValueError):
                flags.append(key + "_incomparable")
    if int(row["datetime"]) <= int(previous["datetime"]):
        flags.append("duplicate_timestamp" if row["datetime"] == previous["datetime"] else "reverse_timestamp")
    if int(row["id"]) <= int(previous["id"]):
        flags.append("id_reset_revision_or_reorder")
    elif int(row["id"]) != int(previous["id"]) + 1:
        flags.append("sdk_id_discontinuity_evidence_only")
    if list(previous) != list(row):
        flags.append("schema_changed")
    return flags


class DurableWriter:
    """Bounded one-writer queue. ACK is sent only after SQLite COMMIT succeeds."""
    def __init__(self, root, *, capacity=8, min_free_bytes=512 * 1024 * 1024,preparation_workers=0):
        self.root = root
        self.capacity = capacity
        self.min_free_bytes = min_free_bytes
        self.inbox = queue.Queue(capacity)
        self.acks = queue.Queue()
        self.error = None
        self.executor=ProcessPoolExecutor(max_workers=preparation_workers,mp_context=multiprocessing.get_context("spawn"),initializer=initialize_worker) if preparation_workers else None
        self.thread = threading.Thread(target=self._work, name="durable-ticks", daemon=True)
        self.thread.start()

    def _work(self):
        store = None
        try:
            store = Store(self.root, min_free_bytes=self.min_free_bytes)
            while True:
                message = self.inbox.get()
                if message is None:
                    self.inbox.task_done()
                    break
                token, operation, args, kwargs, prepared = message
                if operation=="journal":
                    try:
                        if prepared is not None:
                            kwargs["prepared"]=prepared.result()
                        result=store.ingest_journal(*args,**kwargs)
                    except (ValueError,sqlite3.DatabaseError) as exc:
                        result={"written":0,"error_type":type(exc).__name__,"error":str(exc)}
                else:
                    if prepared is not None:
                        kwargs["encoded_rows"]=prepared.result()
                    result = store.ingest(*args, **kwargs)
                self.acks.put((token, result))
                self.inbox.task_done()
        except BaseException as exc:
            self.error = exc
        finally:
            if store:
                store.close()
            if self.executor:
                self.executor.shutdown(wait=True,cancel_futures=True)

    def submit(self, token, *args, **kwargs):
        self.check()
        if self.inbox.full():
            raise queue.Full
        prepared=self.executor.submit(encode_rows,args[1]) if self.executor else None
        self.inbox.put_nowait((token, "ingest", args, kwargs,prepared))

    def submit_journal(self,token,path,symbol,run,info):
        self.check()
        if self.inbox.full():
            raise queue.Full
        prepared=self.executor.submit(prepare_journal,path,symbol,info) if self.executor else None
        self.inbox.put_nowait((token,"journal",(path,symbol,run,info),{},prepared))

    def check(self):
        if self.error:
            raise RuntimeError(f"Durable writer failed: {type(self.error).__name__}: {self.error}") from self.error

    def close(self):
        while self.thread.is_alive():
            self.check()
            try:
                self.inbox.put(None, timeout=0.25)
                break
            except queue.Full:
                continue
        self.thread.join(timeout=30)
        self.check()
        if self.thread.is_alive():
            raise TimeoutError("Writer did not drain; pending records require replay")
