#!/usr/bin/env python3
"""Repeatable same-input persistence or read-only subscription speed evidence."""

import argparse
import hashlib
import inspect
import json
from pathlib import Path
import resource
import sqlite3
import sys
import time


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode",choices=["import","collect"])
    parser.add_argument("--code-root",type=Path,required=True)
    parser.add_argument("--output-root",type=Path,required=True)
    parser.add_argument("--report",type=Path,required=True)
    parser.add_argument("--job",type=Path)
    parser.add_argument("--symbols",nargs="+")
    parser.add_argument("--workers",type=int,default=0)
    parser.add_argument("--initial-concurrency",type=int,default=4)
    args=parser.parse_args()
    if args.output_root.exists():
        parser.error("An empty, new benchmark root is required")
    sys.path.insert(0,str(args.code_root.resolve()))
    code_sha=hashlib.sha256(b"".join(p.name.encode()+p.read_bytes() for p in sorted((args.code_root/"tick_futures").glob("*.py")))).hexdigest()
    started=time.time_ns()
    if args.mode=="import":
        from tick_futures.store import Store,DurableWriter
        job=json.loads(args.job.read_text())
        info={"source_journal_sha256":job["journal_sha256"],"metadata_version":job["metadata_version"]}
        if args.workers:
            writer=DurableWriter(args.output_root,min_free_bytes=0,preparation_workers=args.workers)
            for i,symbol in enumerate(job["symbols"]):
                writer.submit_journal(str(i),job["journal"],symbol,"benchmark",info)
            result=[writer.acks.get(timeout=120)[1] for symbol in job["symbols"]]
            writer.close()
        else:
            store=Store(args.output_root,min_free_bytes=0)
            result=[store.ingest_journal(job["journal"],symbol,"benchmark",info) for symbol in job["symbols"]]
            store.close()
    else:
        from tick_futures.collector import collect
        kwargs={"symbols":args.symbols,"data_length":1000,"seconds":120,"initial_only":True,"min_free_bytes":0}
        if "preparation_workers" in inspect.signature(collect).parameters:
            kwargs.update(preparation_workers=args.workers,initial_concurrency=args.initial_concurrency)
        result=collect(args.output_root,**kwargs)
        if not result["initialization_complete"] or result["failures"]:
            raise RuntimeError("Subscription benchmark did not fully initialize")
    elapsed=(time.time_ns()-started)/1e9
    database=sqlite3.connect(f"file:{args.output_root.resolve()/'hot.sqlite3'}?mode=ro",uri=True)
    digest=hashlib.sha256()
    count=0
    by_contract={}
    for symbol,raw in database.execute("SELECT symbol,raw FROM records ORDER BY seq"):
        digest.update(raw)
        if symbol not in by_contract:
            by_contract[symbol]=[0,hashlib.sha256()]
        by_contract[symbol][0]+=1
        by_contract[symbol][1].update(raw)
        count+=1
    database.close()
    report={"mode":args.mode,"started_ns":started,"ended_ns":time.time_ns(),"elapsed_seconds":elapsed,
            "preparation_workers":args.workers,"initialization_concurrency":args.initial_concurrency,
            "code_root":str(args.code_root.resolve()),"output_root":str(args.output_root.resolve()),
            "source_tree_sha256":code_sha,
            "rows":count,"ordered_original_raw_sha256":digest.hexdigest(),"result":result,
            "contracts":{symbol:{"rows":n,"ordered_original_raw_sha256":h.hexdigest()} for symbol,(n,h) in by_contract.items()},
            "parent_peak_rss_bytes":resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            "note":"Shared-host measurement; historical buffers do not prove new live quote throughput"}
    args.report.parent.mkdir(parents=True,exist_ok=True)
    args.report.write_text(json.dumps(report,ensure_ascii=False,indent=2)+"\n")
    print(json.dumps({k:v for k,v in report.items() if k!="result"},ensure_ascii=False))


if __name__=="__main__":
    main()
