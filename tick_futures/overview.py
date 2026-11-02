"""Read-only data inventory; capture evidence and archive state stay separate."""

from collections import Counter
from datetime import datetime
import json
from pathlib import Path
import shutil
import sqlite3
import time

import psutil

from .codec import loads
from .common import CST
from .runtime import atomic_json


def local_time(ns):
    if ns is None:
        return None
    seconds,nano=divmod(int(ns),1_000_000_000)
    return datetime.fromtimestamp(seconds,CST).strftime("%Y-%m-%dT%H:%M:%S")+f".{nano:09d}+08:00"


def overview(root,output=None):
    root=Path(root).resolve()
    started=time.time_ns()
    latest=root/"discovery/universe-latest.json"
    universe=json.loads(latest.read_text()) if latest.exists() else {"symbols":[]}
    metadata={}
    if universe.get("metadata_file"):
        metadata={str(r["instrument_id"]):r for r in loads((root/"discovery"/universe["metadata_file"]).read_bytes())}
    db=sqlite3.connect(f"file:{root/'hot.sqlite3'}?mode=ro",uri=True)
    db.execute("BEGIN")
    high=db.execute("SELECT coalesce(max(seq),0) FROM records").fetchone()[0]
    contracts={s:{"symbol":s,"target_start_ns":a,"earliest_source_event_ns":b,"first_activation_ns":c,"status":d}
               for s,a,b,c,d in db.execute("SELECT symbol,target_start_ns,earliest_ns,live_enabled_ns,status FROM contracts")}
    exchanges={e:{"exchange":e,"target_contracts":0,"persisted_contracts":0,"rows":0,"origins":{},
                    "earliest_source_event_ns":None,"latest_source_event_ns":None}
               for e in ("SHFE","INE","DCE","CZCE","CFFEX","GFEX")}
    for symbol in universe["symbols"]:
        exchanges[symbol.split('.')[0]]["target_contracts"]+=1
    persisted={e:set() for e in exchanges}
    origins=Counter()
    for symbol,origin,n,first,last in db.execute("SELECT symbol,origin,count(*),min(event_ns),max(event_ns) FROM records GROUP BY symbol,origin"):
        ex=symbol.split('.')[0]
        if ex not in exchanges:
            raise ValueError("Unexpected exchange in stored records: "+ex)
        item=exchanges[ex]
        persisted[ex].add(symbol)
        item["rows"]+=n
        item["origins"][origin]=item["origins"].get(origin,0)+n
        item["earliest_source_event_ns"]=min(first,item["earliest_source_event_ns"]) if item["earliest_source_event_ns"] is not None else first
        item["latest_source_event_ns"]=max(last,item["latest_source_event_ns"]) if item["latest_source_event_ns"] is not None else last
        origins[origin]+=n
    for ex,item in exchanges.items():
        item["persisted_contracts"]=len(persisted[ex])
        item["earliest_source_event_local"]=local_time(item["earliest_source_event_ns"])
        item["latest_source_event_local"]=local_time(item["latest_source_event_ns"])
    gaps=[{"kind":kind,"status":status,"versions":n,"contracts":distinct} for kind,status,n,distinct in
          db.execute("SELECT kind,status,count(*),count(distinct symbol) FROM gaps GROUP BY kind,status")]
    unverified=db.execute("SELECT count(distinct symbol) FROM gaps WHERE kind IN ('startup_window_after_target','startup_prefix_unverified') AND status!='window_verified'").fetchone()[0]
    fields={}
    for symbol in contracts:
        value=db.execute("SELECT raw FROM records WHERE symbol=? ORDER BY seq DESC LIMIT 1",(symbol,)).fetchone()
        if value:
            for name,value in loads(value[0]).items():
                dtype=str(value.dtype) if hasattr(value,"dtype") else type(value).__name__
                fields.setdefault(name,set()).add(dtype)
    observations=db.execute("SELECT count(*),coalesce(sum(json_extract(detail,'$.input_rows')),0),coalesce(sum(json_extract(detail,'$.replayed')),0) FROM observations").fetchone()
    db.close()
    heartbeat=root/"heartbeat.json"
    runtime=json.loads(heartbeat.read_text()) if heartbeat.exists() else {"status":"not_started"}
    runtime["heartbeat_age_seconds"]=(time.time_ns()-runtime.get("heartbeat_ns",0))/1e9 if heartbeat.exists() else None
    runtime["process_alive"]=False
    try:
        process=psutil.Process(runtime.get("pid",-1))
        args=process.cmdline()
        if "tick_futures" in args and str(root) in args:
            runtime["process_alive"]=process.is_running()
            children=[]
            for child in process.children(recursive=True):
                try:
                    children.append({"pid":child.pid,"rss_bytes":child.memory_info().rss})
                except psutil.NoSuchProcess:
                    pass
            runtime["observed_receiver_rss_bytes"]=process.memory_info().rss
            runtime["observed_child_processes"]=children
            runtime["observed_receiver_and_children_rss_bytes"]=runtime["observed_receiver_rss_bytes"]+sum(p["rss_bytes"] for p in children)
    except (psutil.NoSuchProcess,psutil.AccessDenied,ValueError):
        pass
    manifests=[json.loads(p.read_text()) for p in (root/"archives").glob("*/manifest.json")]
    archives={"completed_local_manifests":len(manifests),"canonical_rows_in_manifests":sum(m["scope"]["rows"] for m in manifests),
              "raw_file_bytes_including_preserved_sources":sum(m["raw_bytes"] for m in manifests),
              "compressed_part_bytes":sum(m["stored_part_bytes"] for m in manifests),
              "remote_receipts":sum((p.parent/"remote-receipt.json").exists() for p in (root/"archives").glob("*/manifest.json")),
              "note":"Manifest inventory only; independent decode/restore reports are required for recovery acceptance"}
    warnings=[]
    for symbol,c in contracts.items():
        try:
            year=int(metadata.get(symbol,{}).get("delivery_year"))
        except (ValueError,TypeError,OverflowError):
            continue
        first=c["earliest_source_event_ns"]
        if first is not None and year-datetime.fromtimestamp(first//1_000_000_000,CST).year>=5:
            warnings.append(symbol)
    report={"format":"tfoverview-1","database_snapshot_started_ns":started,"generated_ns":time.time_ns(),
        "source":"TqSdk snapshots","account_mode":"TqSim","universe_version":universe.get("version"),
        "target_contracts":len(universe["symbols"]),"persisted_contracts":sum(len(s) for s in persisted.values()),
        "persisted_rows":sum(origins.values()),"last_committed_arrival_seq":high,"origins":dict(origins),
        "exchanges":list(exchanges.values()),"runtime":runtime,"gap_versions":gaps,
        "contracts_with_unverified_prefix":unverified,"source_history_identity_warnings":warnings,
        "latest_record_field_types":{k:sorted(v) for k,v in fields.items()},
        "observations":{"windows":observations[0],"input_record_deliveries":observations[1],"audited_replayed_records":observations[2]},
        "archives":archives,"disk_free_bytes":shutil.disk_usage(root).free,
        "collection_integrity":"evidence_insufficient","complete_day_night_acceptance":"PENDING","upstream_zero_loss_proven":False,
        "interpretation":"Rows include historical replay/revisions. realtime_update means window differences, not a standalone live-completeness proof. Old source dates may refer to reused contract codes.",
        "contracts":list(contracts.values())}
    if output:
        atomic_json(output,report)
    return report
