"""One bounded public TqSim helper; released after at most eight contracts.

The live receiver keeps its smaller windows. This process only creates an
immutable local journal; the live single writer later audits and imports it.
"""

import argparse
import json
import os
from pathlib import Path
import shutil
import time

from .archive import file_hash
from .collector import collect
from .runtime import InstanceLock,atomic_json


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--parent-root",type=Path,required=True)
    p.add_argument("--job-root",type=Path,required=True)
    p.add_argument("--symbols",nargs="+",required=True)
    a=p.parse_args()
    if len(a.symbols)>8:
        p.error("At most eight contracts per helper; use the same account, no limit bypass")
    os.nice(15)
    with InstanceLock(a.parent_root/"backfill-worker.lock"):
        if shutil.disk_usage(a.parent_root).free<20*1024**3:
            raise OSError("Backfill paused below 20 GiB; live receiver reserve takes priority")
        result=collect(a.job_root,symbols=a.symbols,data_length=10000,seconds=180,initial_only=True,
                       min_free_bytes=20*1024**3)
        journal=a.job_root/"hot.sqlite3"
        universe=json.loads((a.job_root/"discovery/universe-latest.json").read_text())
        report={"format":"tfbackfill-job-1","completed_ns":time.time_ns(),"symbols":a.symbols,
                "journal":str(journal.resolve()),"journal_sha256":file_hash(journal),
                "metadata_version":universe["version"],"collector_result":result,
                "initialization_complete":result["initialization_complete"],"account_mode":"TqSim"}
        atomic_json(a.job_root/"completed.json",report)
        print(json.dumps({k:v for k,v in report.items() if k!="collector_result"},ensure_ascii=False))


if __name__=="__main__":
    main()
