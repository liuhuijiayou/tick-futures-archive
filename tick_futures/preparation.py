"""CPU work for bounded processes; no SDK, authentication or database writes."""

import hashlib
import json
import os
from pathlib import Path
import sqlite3

from .codec import dumps,loads,sha256

_verified_journals={}


def initialize_worker():
    for key in ("TQ_USER","TQ_PASSWORD","TQ_AUTH_CONFIG"):
        os.environ.pop(key,None)
    if hasattr(os,"nice"):
        os.nice(5)


def encode_rows(rows):
    result=[]
    for row in rows:
        raw=dumps(row)
        result.append((raw,sha256(raw)))
    return result


def prepare_journal(path,symbol,info):
    path=Path(path).resolve()
    stat=path.stat()
    expected=info.get("source_journal_sha256")
    identity=(stat.st_size,stat.st_mtime_ns)
    if expected and _verified_journals.get(str(path))!=(identity,expected):
        with path.open("rb") as stream:
            actual=hashlib.file_digest(stream,"sha256").hexdigest()
        if actual!=expected:
            raise ValueError("Backfill journal file hash mismatch")
        _verified_journals[str(path)]=(identity,expected)
    source=sqlite3.connect(f"file:{path}?mode=ro",uri=True)
    try:
        rows,encoded=[],[]
        observed=None
        for fingerprint,raw,received in source.execute("SELECT fingerprint,raw,received_ns FROM records WHERE symbol=? ORDER BY seq",(symbol,)):
            row=loads(raw)
            if sha256(raw)!=fingerprint or dumps(row)!=raw:
                raise ValueError("Backfill journal raw hash/type/bit mismatch")
            rows.append(row)
            encoded.append((raw,fingerprint))
            observed=received
        gaps=source.execute("SELECT count(*) FROM gaps WHERE (symbol=? OR symbol IS NULL) AND status!='window_verified'",(symbol,)).fetchone()[0]
        runs=[{"run":r,"detail":json.loads(d)} for r,d in source.execute("SELECT run,detail FROM runs")]
        if (path.stat().st_size,path.stat().st_mtime_ns)!=identity:
            raise ValueError("Immutable backfill journal changed during preparation")
        return {"rows":rows,"encoded":encoded,"received_ns":observed,
                "window_info":{**info,"configured_rows":10000,"source_journal_gap_count":gaps,"source_runs":runs}}
    finally:
        source.close()
