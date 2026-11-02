"""Receive continuously; bounded stable copies -> WAL writer -> commit ACK.

Cold operations run in a separate maintenance process. A full source window is
reconciled on every process/connection rebuild. Within a connection we compare
all overlapping cells by their exact bit representation, retaining revisions.
"""

from collections import deque
from datetime import datetime
import json
import os
from pathlib import Path
import queue
import signal
import time
import uuid
import subprocess
import shutil
import sys

import numpy as np
import pandas as pd
import psutil

from .codec import dumps, frame_rows, loads, sha256
from .common import CST, timestamp
from .runtime import InstanceLock, alert, atomic_json
from .source import connect, discover, refresh_universe
from .store import DurableWriter, Store
from .sessions import classify


def changed_rows(current, committed):
    """Return new/changed rows in SDK order, including unchanged-price quotes.

    A duplicate ID or type/schema change makes alignment ambiguous: retain the
    complete window and let full content reconciliation record that ambiguity.
    """
    if committed is None or list(current) != list(committed):
        return current, True
    if not current["id"].is_unique or not committed["id"].is_unique:
        return current, True
    indices = pd.Index(committed["id"]).get_indexer(current["id"])
    changed = indices < 0
    shared = np.flatnonzero(~changed)
    if not len(shared) or committed["id"].iloc[-1] not in set(current["id"]):
        return current, True
    for name in current.columns:
        a = current[name].to_numpy(copy=False)
        b = committed[name].to_numpy(copy=False)
        if a.dtype != b.dtype:
            return current, True
        if a.dtype.hasobject:
            def identical(left,right):
                if type(left) is type(right) and type(left) in (str,int,bool,bytes,type(None)):
                    return left==right
                return dumps(left)==dumps(right)
            unequal = np.array([not identical(a[i],b[indices[i]]) for i in shared])
        else:
            left = np.ascontiguousarray(a[shared]).view(np.uint8).reshape(len(shared), a.dtype.itemsize)
            right = np.ascontiguousarray(b[indices[shared]]).view(np.uint8).reshape(len(shared), b.dtype.itemsize)
            unequal = np.any(left != right, axis=1)
        changed[shared] |= unequal
    if np.all(changed[shared]):
        # Reused IDs with no unchanged content anchor may be a day/session
        # reset, a complete revision, or a replaced source window.
        return current, True
    return current.loc[changed], False


def collect(root, *, symbols=None, data_length=10000, seconds=None, max_contracts=None,
            min_free_bytes=512 * 1024 * 1024, refresh_seconds=300,initial_only=False,
            preparation_workers=0,initial_concurrency=4):
    if not 0<=preparation_workers<=4 or not 1<=initial_concurrency<=16:
        raise ValueError("Preparation workers must be 0..4 and initialization concurrency 1..16")
    root = Path(root).resolve()
    with InstanceLock(root / "collector.lock"):
        return _collect(root, symbols=symbols, data_length=data_length, seconds=seconds,
                        max_contracts=max_contracts, min_free_bytes=min_free_bytes,
                        refresh_seconds=refresh_seconds,initial_only=initial_only,
                        preparation_workers=preparation_workers,initial_concurrency=initial_concurrency)


def _collect(root, *, symbols, data_length, seconds, max_contracts, min_free_bytes, refresh_seconds,initial_only,
             preparation_workers,initial_concurrency):
    os.umask(0o077)
    run = uuid.uuid4().hex
    target_start = time.time_ns()
    begun = time.monotonic()
    stop = False
    def request_stop(*_):
        nonlocal stop
        stop = True
    previous_handlers = {s: signal.signal(s, request_stop) for s in (signal.SIGTERM, signal.SIGINT)}
    store = Store(root, min_free_bytes=min_free_bytes)
    # A previous running row indicates SIGKILL/power loss; exact loss is not guessed.
    with store.db:
        for old_run, started in store.db.execute("SELECT run,started_ns FROM runs WHERE status='running'").fetchall():
            store.gap(None, "unclean_process_exit", {"run": old_run}, start_ns=started,
                      end_ns=target_start, status="gap_pending_replay")
            store.db.execute("UPDATE runs SET status='unclean_exit' WHERE run=?", (old_run,))
        source_bytes = b"".join(p.name.encode()+p.read_bytes() for p in sorted(Path(__file__).parent.glob("*.py")))
        try:
            code_commit = subprocess.check_output(["git","rev-parse","HEAD"],text=True,stderr=subprocess.DEVNULL).strip()
        except subprocess.CalledProcessError:
            code_commit = "uncommitted"
        store.db.execute("INSERT INTO runs VALUES(?,?,NULL,'running',?)", (run, target_start,
                         json.dumps({"source": "TqSdk", "account_mode": "TqSim", "requested_length": data_length,
                                     "source_sha256":sha256(source_bytes),"code_commit":code_commit})))
    writer = DurableWriter(root, capacity=8, min_free_bytes=min_free_bytes,preparation_workers=preparation_workers)
    pending = {}
    frames = {}
    committed = {}
    last_quote = {}
    last_unknown = {}
    subscriptions = {}
    total_written = 0
    realtime_written = 0
    failures = {}
    dirty = set()
    refresh_box = {}
    refresh_task = None
    pending_since = {}
    api = None
    reconnects = 0
    attempts = 0
    last_heartbeat = 0
    refreshed = 0
    metrics = {"max_queue": 0, "max_rss_bytes": 0, "max_receive_loop_seconds": 0.0,
               "uncommitted_risk": "SDK/memory/queue until SQLite FULL commit; restart replays available overlap"}
    process = psutil.Process()
    backfill_requested = set()
    backfill_todo = deque()
    upgrade_frames = {}
    backfill_child=None
    backfill_job=None
    backfill_tokens={}
    recovery_jobs={}
    def copy_helper_metadata(job):
        from .runtime import atomic_bytes
        for path in (Path(job)/"discovery").glob("*"):
            if path.name!="universe-latest.json" and path.is_file():
                destination=root/"discovery"/path.name
                if not destination.exists():
                    atomic_bytes(destination,path.read_bytes())
    for path in (root/"backfill-jobs").glob("*/completed.json") if (root/"backfill-jobs").exists() else []:
        recovery_jobs[str(path.parent)]=json.loads(path.read_text())
        copy_helper_metadata(path.parent)
    def acknowledge(token,result):
        nonlocal total_written,realtime_written
        symbol,frame,origin=pending.pop(token)
        pending_since.pop(token,None)
        if frame is not None:
            committed[symbol]=frame
        total_written+=result["written"]
        if origin=="realtime_update":
            realtime_written+=result["written"]
        if token in backfill_tokens:
            backfill_requested.discard(symbol)
            job=backfill_tokens.pop(token)
            receipt_path=Path(job)/"import-receipt.json"
            receipt=json.loads(receipt_path.read_text()) if receipt_path.exists() else {"symbols":{}}
            receipt["symbols"][symbol]=result
            atomic_json(receipt_path,receipt)
            with store.db:
                store.event(symbol,"backfill_import_failed" if "error" in result else "backfill_import_committed",{**result,"job":Path(job).name})
            if "error" in result:
                alert(root,"backfill_import_failed",{"symbol":symbol,**result})
    initial_io = process.io_counters() if hasattr(process, "io_counters") else None
    try:
        while not stop and (seconds is None or time.monotonic() - begun < seconds):
            writer.check()
            if api is None:
                try:
                    api = connect()
                    universe = discover(api, root / "discovery")
                    if universe.get("calendar_error"):
                        alert(root,"calendar_unavailable",universe["calendar_error"])
                    refreshed = time.monotonic()
                    all_symbols = universe["symbols"]
                    desired = all_symbols if symbols is None else list(symbols)
                    if any(s not in all_symbols for s in desired):
                        raise ValueError("Requested contract absent from current real FUTURE enumeration")
                    if max_contracts:
                        desired = desired[:max_contracts]
                    # Interleave markets without dropping a single target. Keep
                    # initialization admission small: unready SDK frames rebuild
                    # their full arrays on updates and can starve ready streams.
                    groups = {ex: deque(s for s in desired if s.startswith(ex + ".")) for ex in ("SHFE", "INE", "DCE", "CZCE", "CFFEX", "GFEX")}
                    todo = deque()
                    while any(groups.values()):
                        for group in groups.values():
                            if group:
                                todo.append(group.popleft())
                    subscriptions = {}
                    frames = {}
                    committed = {}
                    backfill_requested.clear()
                    upgrade_frames.clear()
                    raw_metadata = (root / "discovery" / universe["metadata_file"]).read_bytes()
                    meta = {str(r["instrument_id"]): r for r in loads(raw_metadata)}
                    calendars = sorted((root / "discovery").glob("calendar-*.json"))
                    calendar_info = json.loads(calendars[-1].read_text()) if calendars else {"days":[],"night_overrides":{}}
                    calendar = {r["date"]:r["trading"] for r in calendar_info["days"]}
                    with store.db:
                        for symbol in all_symbols:
                            store.db.execute("INSERT INTO contracts VALUES(?,?,?,NULL,NULL,?,?) ON CONFLICT(symbol) DO UPDATE SET version=excluded.version,status=excluded.status,detail=excluded.detail",
                                (symbol, universe["version"], target_start, "pending" if symbol in desired else "not_requested",
                                 json.dumps({"metadata_version": universe["version"], "backfill_limit_rows": data_length})))
                        for old_symbol, in store.db.execute("SELECT symbol FROM contracts").fetchall():
                            if old_symbol not in all_symbols:
                                store.db.execute("UPDATE contracts SET status='expired_or_removed' WHERE symbol=?", (old_symbol,))
                        store.event(None, "universe", {"version": universe["version"], "target": len(all_symbols),
                                    "requested": len(desired), "reconnect": reconnects})
                    attempts = 0
                except Exception as exc:
                    alert(root, "connect_discovery_failed", {"type": type(exc).__name__, "message": str(exc)})
                    if api:
                        api.close()
                    api = None
                    attempts += 1
                    # Interruptible backoff, never repeatedly purchase/probe history.
                    until = time.monotonic() + min(60, 2 ** min(attempts, 6))
                    while time.monotonic() < until and not stop:
                        time.sleep(0.2)
                    continue
            loop_started = time.monotonic()
            # ACK before selecting a new delta; one in-flight window per contract.
            while True:
                try:
                    token, result = writer.acks.get_nowait()
                except queue.Empty:
                    break
                symbol,frame,origin=pending[token]
                acknowledge(token,result)
                if result.get("decision") in ("no_content_overlap", "ambiguous_overlap") and data_length<10000 and symbol not in backfill_requested:
                    backfill_todo.append(symbol)
            # Create subscriptions from a public API coroutine to avoid the
            # synchronous get_tick_serial initialization starving existing ones.
            # Timed-out frames are still subscribed inside the SDK. They must
            # retain their admission slots until ready; ignoring them could
            # accumulate unbounded incomplete windows and starve ready quotes.
            initializing = sum(v["state"] == "initializing" for v in subscriptions.values())
            initializing += sum(not api.is_serial_ready(f) for f in upgrade_frames.values())
            memory_ok = process.memory_info().rss < 12*1024**3 and psutil.virtual_memory().available>4*1024**3
            metrics["initialization_paused_for_memory"] = not memory_ok
            if todo and memory_ok and initializing < initial_concurrency and writer.inbox.qsize() < 4 and len(pending) < 8:
                symbol = todo.popleft()
                subscriptions[symbol] = {"started": time.monotonic(), "state": "initializing"}
                async def subscribe(symbol=symbol):
                    try:
                        frames[symbol] = api.get_tick_serial(symbol, data_length=data_length)
                    except Exception as exc:
                        failures[symbol] = {"type": type(exc).__name__, "message": str(exc)}
                        subscriptions[symbol]["state"]="subscription_failed"
                        alert(root,"subscription_failed",{"symbol":symbol,**failures[symbol]})
                api.create_task(subscribe())
            if backfill_child is not None and backfill_child.poll() is not None:
                completed=backfill_job/"completed.json"
                if completed.exists():
                    recovery_jobs[str(backfill_job)]=json.loads(completed.read_text())
                    copy_helper_metadata(backfill_job)
                else:
                    alert(root,"isolated_backfill_failed",{"job":backfill_job.name,"exit_code":backfill_child.returncode})
                backfill_child=None
            if backfill_todo and backfill_child is None and len(recovery_jobs)<2 and not todo and memory_ok and not initial_only and shutil.disk_usage(root).free>20*1024**3:
                batch=[]
                while backfill_todo and len(batch)<8:
                    symbol=backfill_todo.popleft()
                    if symbol not in backfill_requested and symbol in frames:
                        batch.append(symbol)
                        backfill_requested.add(symbol)
                if batch:
                    backfill_job=root/"backfill-jobs"/uuid.uuid4().hex
                    backfill_job.mkdir(parents=True)
                    backfill_child=subprocess.Popen([sys.executable,"-m","tick_futures.backfill_worker",
                        "--parent-root",str(root),"--job-root",str(backfill_job),"--symbols",*batch],cwd=Path(__file__).resolve().parents[1],
                        stdout=subprocess.DEVNULL)  # errors still go to redacted project stderr
                    with store.db:
                        store.event(None,"isolated_backfill_started",{"job":backfill_job.name,"symbols":batch,"pid":backfill_child.pid,"account_mode":"TqSim"})
            try:
                updated=api.wait_update(deadline=time.time() + 0.2)
            except Exception as exc:
                with store.db:
                    store.event(None, "connection_exception", {"type": type(exc).__name__, "message": str(exc)})
                    store.gap(None, "connection_interruption", {"reconnect": reconnects}, status="gap_pending_replay")
                # Drain committed data before rebuilding, preserving ACK state.
                while pending:
                    writer.check()
                    try:
                        token, result = writer.acks.get(timeout=0.2)
                        acknowledge(token,result)
                    except queue.Empty:
                        continue
                api.close()
                api = None
                reconnects += 1
                continue
            pending_symbols = {v[0] for v in pending.values()}
            for symbol, larger in list(upgrade_frames.items()):
                # Existing live window continues until the wider window is ready.
                if symbol not in pending_symbols and api.is_serial_ready(larger):
                    frames[symbol] = larger
                    committed.pop(symbol,None)
                    dirty.add(symbol)
                    upgrade_frames.pop(symbol)
                    with store.db:
                        store.event(symbol,"backfill_window_expanded",{"rows":10000,"scope":"public recent buffer only"})
            for symbol, frame in list(frames.items()):
                sub = subscriptions[symbol]
                # SDK is_changing can expose the previous diff when a deadline
                # returns False. It must be gated by wait_update's result.
                if updated and api.is_changing(frame):
                    dirty.add(symbol)
                    last_quote[symbol] = time.monotonic()
                if not api.is_serial_ready(frame):
                    if time.monotonic() - sub["started"] > 180 and symbol not in failures:
                        failures[symbol] = {"type": "InitializationTimeout", "message": "No serial confirmation in 180s; still monitoring"}
                        alert(root,"subscription_initialization_timeout",{"symbol":symbol,**failures[symbol]})
                    continue
                if symbol in pending_symbols or writer.inbox.full():
                    continue
                first = symbol not in committed
                if not first and symbol not in dirty:
                    continue
                observed_ns = time.time_ns()
                stable = frame.copy(deep=True)
                selected, full = changed_rows(stable, committed.get(symbol))
                rows = frame_rows(selected)
                if not rows:
                    dirty.discard(symbol)
                    if first:
                        sub["state"] = "ready_empty"
                        with store.db:
                            store.db.execute("UPDATE contracts SET status='subscribed_empty',live_enabled_ns=coalesce(live_enabled_ns,?) WHERE symbol=?",(time.time_ns(),symbol))
                        committed[symbol]=stable  # no Tick exists in this confirmed empty window
                    continue
                tail = frame_rows(stable.tail(64))
                token = uuid.uuid4().hex
                origin = ("recovery_backfill" if symbol in backfill_requested else "reconnect_backfill" if reconnects else "initial_backfill") if first else "realtime_update"
                window = {"first_id": int(stable["id"].iloc[0]), "last_id": int(stable["id"].iloc[-1]),
                          "first_event_ns": int(stable.loc[stable["datetime"]>0,"datetime"].iloc[0]), "rows": len(stable),
                          "schema": {str(k): str(v) for k, v in stable.dtypes.items()},"configured_rows":len(stable),
                          "metadata_version":universe["version"],"sdk_connection_epoch":reconnects}
                writer.submit(token, symbol, rows, run, origin, full_window=full, tail=tail, window_info=window, received_ns=observed_ns)
                pending[token] = (symbol, stable, origin)
                pending_since[token] = time.monotonic()
                dirty.discard(symbol)
                sub["state"] = "subscribed"
                sub["live_enabled_ns"] = sub.get("live_enabled_ns", time.time_ns())
                # Updating unchanged contract metadata on every live snapshot
                # could wait on a historical writer transaction and block SDK
                # reception. Record activation only once per connection.
                if first:
                    with store.db:
                        store.db.execute("UPDATE contracts SET live_enabled_ns=coalesce(live_enabled_ns,?),status='subscribed' WHERE symbol=?",
                                         (sub["live_enabled_ns"], symbol))
                        contract_target,existing_earliest = store.db.execute("SELECT target_start_ns,earliest_ns FROM contracts WHERE symbol=?",(symbol,)).fetchone()
                        covered_earliest=min(int(rows[0]["datetime"]),existing_earliest) if existing_earliest is not None else int(rows[0]["datetime"])
                        if covered_earliest > contract_target:
                            store.gap(symbol,"startup_window_after_target",{"requested_rows":data_length,"maximum_documented_rows":10000},
                                      status="gap_pending_replay" if len(stable)<10000 else "gap_unrecoverable",
                                      start_ns=contract_target,end_ns=int(rows[0]["datetime"]))
                            if len(stable)<10000 and symbol not in backfill_requested:
                                backfill_todo.append(symbol)
                failures.pop(symbol, None)
            if time.monotonic() - last_heartbeat > 5:
                metrics["max_queue"] = max(metrics["max_queue"], writer.inbox.qsize())
                metrics["max_rss_bytes"] = max(metrics["max_rss_bytes"], process.memory_info().rss)
                cpu = process.cpu_times()
                metrics["cpu_seconds"] = cpu.user + cpu.system
                metrics["oldest_pending_seconds"] = max((time.monotonic()-v for v in pending_since.values()),default=0)
                status = {"run": run, "pid": os.getpid(), "heartbeat_ns": time.time_ns(), "started_ns": target_start,
                    "source": "TqSdk", "account_mode": "TqSim", "universe_count": len(all_symbols),
                    "requested_count": len(desired), "created_subscriptions": len(frames),
                    "ready_subscriptions": sum(v["state"] == "subscribed" for v in subscriptions.values()),
                    "confirmed_empty_subscriptions":sum(v["state"]=="ready_empty" for v in subscriptions.values()),
                    "remaining_subscriptions": len(todo), "failures": failures, "written_this_run": total_written,
                    "realtime_written_this_run": realtime_written, "queue": writer.inbox.qsize(), "pending": len(pending),
                    "reconnects": reconnects, "metrics": metrics, "status": "running",
                    "backfill_helper_pid":backfill_child.pid if backfill_child else None,"backfill_pending_contracts":len(backfill_todo),
                    "backfill_ready_jobs":len(recovery_jobs),
                    "backfill_waiting_import_contracts":sum(len(set(info["symbols"])-set(json.loads((Path(job)/"import-receipt.json").read_text())["symbols"] if (Path(job)/"import-receipt.json").exists() else [])) for job,info in recovery_jobs.items()),
                    "preparation_workers":preparation_workers,"initialization_concurrency":initial_concurrency,
                    "coverage_status": "PENDING full day and source replay verification"}
                atomic_json(root / "heartbeat.json", status)
                last_heartbeat = time.monotonic()
                now_ns = time.time_ns()
                for symbol, sub in subscriptions.items():
                    silence = time.monotonic()-last_quote.get(symbol,sub["started"])
                    if sub["state"] in ("subscribed","ready_empty") and silence>120 and time.monotonic()-last_unknown.get(symbol,0)>300:
                        session = classify(now_ns,meta.get(symbol,{}),calendar,calendar_info["night_overrides"])
                        if session["session"] != "outside_or_unknown" and session["certainty"]=="calendar_metadata":
                            with store.db:
                                store.gap(symbol,"active_session_silence_unknown",{"silence_seconds":silence,"session":session,
                                          "note":"No fixed Tick rate assumed; transport/normal quiet cannot be distinguished"},status="evidence_insufficient")
                            last_unknown[symbol] = time.monotonic()
                            alert(root,"active_session_silence_unknown",{"symbol":symbol,"silence_seconds":silence})
                requests = root/"requests"
                for request in requests.glob("backfill-*.json") if requests.exists() else []:
                    command = json.loads(request.read_text())
                    if command.get("status") != "pending":
                        continue
                    selected = command.get("symbols") or [s for s, in store.db.execute("SELECT DISTINCT symbol FROM gaps WHERE status='gap_pending_replay' AND symbol IS NOT NULL")]
                    rejected = [s for s in selected if s not in all_symbols]
                    accepted = [s for s in selected if s in all_symbols]
                    for s in accepted:
                        backfill_requested.discard(s)
                    backfill_todo.extend(accepted)
                    command.update({"status":"queued" if accepted else "no_pending_contract_gaps","symbols":accepted,"rejected":rejected})
                    atomic_json(request,command)
            # Bounded parallel CPU preparation, one ordered transactional writer.
            # Dispatch on every receive loop, not only the five-second heartbeat.
            # Keep cold work out of initial admission and give live work priority.
            for job,info in list(recovery_jobs.items()):
                cold_limit=max(1,min(2,preparation_workers))
                if todo or len(backfill_tokens)>=cold_limit or any(v[2]!="recovery_backfill" for v in pending.values()):
                    break
                receipt_path=Path(job)/"import-receipt.json"
                receipt=json.loads(receipt_path.read_text()) if receipt_path.exists() else {"symbols":{}}
                for symbol in info["symbols"]:
                    if symbol in receipt["symbols"] or symbol in {v[0] for v in pending.values()} or writer.inbox.full():
                        continue
                    token=uuid.uuid4().hex
                    writer.submit_journal(token,info["journal"],symbol,run,{"metadata_version":info["metadata_version"],"source_journal_sha256":info["journal_sha256"],"job":Path(job).name})
                    pending[token]=(symbol,None,"recovery_backfill")
                    pending_since[token]=time.monotonic()
                    backfill_tokens[token]=job
                    break
                if all(s in receipt["symbols"] for s in info["symbols"]):
                    recovery_jobs.pop(job,None)
            metrics["max_receive_loop_seconds"] = max(metrics["max_receive_loop_seconds"], time.monotonic() - loop_started)
            # Refresh on a slow cadence and before day/night preopen. Rebuild only
            # after draining; it is not a health-check restart.
            clock = datetime.now(CST)
            preopen_key = clock.strftime("%Y%m%d%H%M") if (clock.hour, clock.minute) in ((7,30),(8,40),(19,30),(20,30)) else None
            if time.monotonic() - refreshed > refresh_seconds or (preopen_key and preopen_key != locals().get("last_preopen")):
                if refresh_task is None:
                    async def refresh():
                        try:
                            refresh_box["value"] = await refresh_universe(api,root/"discovery",all_symbols)
                        except Exception as exc:
                            refresh_box["error"] = {"type":type(exc).__name__,"message":str(exc)}
                    refresh_task = api.create_task(refresh())
                    refreshed = time.monotonic()
                    last_preopen = preopen_key
            if refresh_task is not None and refresh_task.done():
                refresh_task = None
                if "error" in refresh_box:
                    alert(root,"universe_refresh_failed",refresh_box.pop("error"))
                elif "value" in refresh_box:
                    fresh, fresh_metadata = refresh_box.pop("value")
                    meta = {**meta,**{str(r["instrument_id"]):r for r in fresh_metadata}}
                    if fresh["symbols"] != all_symbols:
                        with store.db:
                            store.event(None,"universe_changed",{"previous":universe["version"],"new":fresh["version"]})
                            for s in fresh["symbols"]:
                                if s not in all_symbols:
                                    store.db.execute("INSERT OR IGNORE INTO contracts VALUES(?,?,?,NULL,NULL,'pending',?)",
                                        (s,fresh["version"],time.time_ns(),json.dumps({"backfill_limit_rows":data_length})))
                                    if symbols is None:
                                        todo.append(s)
                            for s in set(all_symbols)-set(fresh["symbols"]):
                                expired=meta.get(s,{}).get("expired")
                                confirmed=isinstance(expired,(bool,np.bool_)) and bool(expired)
                                store.db.execute("UPDATE contracts SET status=? WHERE symbol=?",("expired_confirmed" if confirmed else "vendor_missing_unresolved",s))
                                store.event(s,"expiry_confirmed" if confirmed else "vendor_list_missing_unresolved",{"metadata_version":fresh["version"],"kept_receiving":not confirmed})
                                if confirmed:
                                    frames.pop(s,None)
                                    subscriptions[s]["state"]="expired_confirmed"
                        retained=[s for s in all_symbols if s not in fresh["symbols"] and s in frames]
                        all_symbols = fresh["symbols"]+retained
                        if symbols is None and not max_contracts:
                            desired=list(all_symbols)
                    universe = fresh
            if initial_only and not todo and not pending and len(subscriptions)==len(desired) and all(v["state"] in ("subscribed","ready_empty") for v in subscriptions.values()):
                break
        writer.close()
        while not writer.acks.empty():
            token, result = writer.acks.get_nowait()
            acknowledge(token,result)
        with store.db:
            for symbol, failure in failures.items():
                store.db.execute("UPDATE contracts SET status='failed',detail=? WHERE symbol=?", (json.dumps(failure), symbol))
            store.db.execute("UPDATE runs SET ended_ns=?,status='stopped' WHERE run=?", (time.time_ns(), run))
        result = {"status": "stopped", "run": run, "heartbeat_ns": time.time_ns(), "pid": os.getpid(),
                  "written": total_written, "realtime_written": realtime_written,
                  "requested": len(subscriptions), "failures": failures, "metrics": metrics,
                  "elapsed_seconds": time.monotonic() - begun, "coverage_status": "PENDING full trading day"}
        result["initialization_complete"]=not todo and len(subscriptions)==len(desired) and all(v["state"] in ("subscribed","ready_empty") for v in subscriptions.values())
        atomic_json(root / f"run-{run}.json", result)
        atomic_json(root / "heartbeat.json", result)
        return result
    except BaseException as exc:
        with store.db:
            store.gap(None, "collector_failure", {"type": type(exc).__name__, "message": str(exc)}, status="gap_pending_replay")
            store.db.execute("UPDATE runs SET ended_ns=?,status='failed' WHERE run=?", (time.time_ns(), run))
        alert(root, "collector_failure", {"type": type(exc).__name__, "message": str(exc)})
        raise
    finally:
        if backfill_child is not None and backfill_child.poll() is None:
            backfill_child.terminate()
            try:
                backfill_child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                backfill_child.kill()
        if api:
            api.close()
        if writer.thread.is_alive():
            writer.close()
        store.close()
        for sig, handler in previous_handlers.items():
            signal.signal(sig, handler)
