"""Durable operator backfill requests, separate from collection completeness."""

import json
from pathlib import Path
import time
from collections import Counter

from .runtime import atomic_json,alert


def requests(root):
    directory=Path(root)/"requests"
    for path in directory.glob("backfill-*.json"):
        try:
            command=json.loads(path.read_text())
            if not isinstance(command,dict) or not isinstance(command.get("status"),str):
                raise ValueError("Invalid backfill request object/status")
            for field in ("symbols","remaining_symbols","requested_symbols"):
                value=command.get(field)
                if value is not None and (not isinstance(value,list) or not all(isinstance(s,str) for s in value)):
                    raise ValueError("Invalid backfill request symbol list")
            if type(command.get("requested_ns",0)) is not int:
                raise ValueError("Backfill request time must be an integer")
        except (ValueError,OSError) as exc:
            # Preserve the malformed bytes, but don't let an operational
            # request interrupt otherwise healthy market-data reception.
            try:
                path.rename(path.with_suffix(".invalid-"+str(time.time_ns())))
            except OSError:
                pass
            alert(root,"invalid_backfill_request",{"request":path.name,"error":str(exc)})
            continue
        yield path,command


def admit_requests(root,universe,default_symbols,seen):
    added=[]
    for path,command in requests(root):
        if command.get("status")=="pending":
            selected=command.get("symbols") or list(default_symbols)
            accepted=list(dict.fromkeys(s for s in selected if s in universe))
            command.update(status="queued" if accepted else "no_pending_contract_gaps",
                           requested_symbols=selected,remaining_symbols=accepted,
                           rejected=[s for s in selected if s not in universe])
            atomic_json(path,command)
        if command.get("status")=="queued":
            for symbol in command.get("remaining_symbols",command.get("symbols") or []):
                key=(str(path),symbol)
                if symbol in universe and key not in seen:
                    added.append(symbol)
                    seen.add(key)
    return added


def forced_symbols(root):
    return {s for path,command in requests(root) if command.get("status")=="queued"
            for s in command.get("remaining_symbols",command.get("symbols") or [])}


def request_states(root):
    return dict(Counter(command["status"] for path,command in requests(root)))


def complete_requests(root,symbol,source_completed_ns,result):
    for path,command in requests(root):
        remaining=command.get("remaining_symbols",command.get("symbols") or [])
        if command.get("status")!="queued" or symbol not in remaining or source_completed_ns<command.get("requested_ns",0):
            continue
        command.setdefault("results",{})[symbol]=result
        if "error" in result:
            command["status"]="failed"
        else:
            remaining=[s for s in remaining if s!=symbol]
            command["remaining_symbols"]=remaining
            command["status"]="completed" if not remaining else "queued"
        command["note"]="Source window operation state; market coverage is audited separately"
        atomic_json(path,command)
