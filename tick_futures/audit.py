"""Independent collection/restore ledgers. Counts never imply upstream coverage."""

from collections import Counter, defaultdict
import json
import math
from pathlib import Path
import sqlite3
import time
from datetime import datetime,date,timedelta

from .codec import dumps, loads, sha256
from .common import CST
from .runtime import atomic_json
from .sessions import classify,planned_sessions


def verify_db(path):
    db = sqlite3.connect(f"file:{Path(path).resolve()}?mode=ro", uri=True)
    try:
        check = db.execute("PRAGMA integrity_check").fetchone()[0]
        if check != "ok":
            raise ValueError("SQLite integrity failure: " + check)
        count = 0
        for seq, fingerprint, raw in db.execute("SELECT seq,fingerprint,raw FROM records ORDER BY seq"):
            if sha256(raw) != fingerprint or dumps(loads(raw)) != raw:
                raise ValueError(f"Record byte/type/bit roundtrip failure at seq {seq}")
            count += 1
        # The checkpoint must never point beyond a committed record for its symbol.
        for symbol, seq in db.execute("SELECT symbol,seq FROM checkpoints"):
            if seq and not db.execute("SELECT 1 FROM records WHERE seq=? AND symbol=?", (seq, symbol)).fetchone():
                # Incremental archives may refer to an earlier parent checkpoint.
                first = db.execute("SELECT min(seq) FROM records").fetchone()[0]
                if first is None or seq >= first:
                    raise ValueError(f"Checkpoint references a missing committed row: {symbol}")
        return {"status": "sqlite_and_all_record_bytes_types_bits_verified", "rows": count}
    finally:
        db.close()


def coverage(root, output=None, *, scan_values=False):
    root = Path(root)
    db = sqlite3.connect(f"file:{root / 'hot.sqlite3'}?mode=ro", uri=True)
    db.execute("BEGIN")
    contracts = {r[0]: {"symbol": r[0], "metadata_version": r[1], "target_start_ns": r[2],
                          "actual_earliest_ns": r[3], "live_enabled_ns": r[4], "subscription_status": r[5],
                          "boundary": json.loads(r[6])} for r in db.execute("SELECT * FROM contracts")}
    discovery = root / "discovery"
    meta = {}
    calendar, overrides = {}, {}
    if (discovery / "universe-latest.json").exists():
        universe = json.loads((discovery / "universe-latest.json").read_text())
        meta = {str(r["instrument_id"]): r for r in loads((discovery / universe["metadata_file"]).read_bytes())}
    calendars = sorted(discovery.glob("calendar-*.json"))
    if calendars:
        c = json.loads(calendars[-1].read_text())
        calendar = {r["date"]: r["trading"] for r in c["days"]}
        overrides = c["night_overrides"]
    ledger = {}
    field_flags = Counter()
    origin_counts = Counter()
    for symbol, event_ns, origin, raw in db.execute("SELECT symbol,event_ns,origin,raw FROM records ORDER BY seq"):
        interval = classify(event_ns, meta.get(symbol, {}), calendar, overrides)
        day = interval["trading_day"] or "UNKNOWN:" + interval["natural_day"]
        key = (symbol, day, interval["session"])
        if key not in ledger:
            ledger[key] = {"exchange": symbol.split(".")[0], "product": str(meta.get(symbol, {}).get("product_id", "UNKNOWN")),
                            "symbol": symbol, "trading_day": day, "session": interval["session"], "rows": 0,
                            "first_event_ns": event_ns, "last_event_ns": event_ns,
                            "calendar_status": interval["certainty"], "collection_status": "evidence_insufficient"}
        entry = ledger[key]
        entry["rows"] += 1
        entry["first_event_ns"] = min(entry["first_event_ns"], event_ns)
        entry["last_event_ns"] = max(entry["last_event_ns"], event_ns)
        origin_counts[origin] += 1
        if scan_values:
            row = loads(raw)
            for name, value in row.items():
                if value is None:
                    field_flags[name + ":null"] += 1
                elif isinstance(value, (float, __import__("numpy").floating)) and not math.isfinite(value):
                    field_flags[name + (":nan" if math.isnan(value) else ":infinite")] += 1
            for level in range(1, 6):
                names = [f"{side}_{field}{level}" for side in ("bid", "ask") for field in ("price", "volume")]
                if not all(name in row for name in names):
                    field_flags[f"depth_{level}:missing_columns"] += 1
                elif any(row[n] is None or not math.isfinite(row[n]) for n in names):
                    field_flags[f"depth_{level}:invalid_or_absent_values"] += 1
    # Explicit zero-evidence rows for every enumerated target; never filter failures.
    for symbol, contract in contracts.items():
        if not any(key[0] == symbol for key in ledger):
            ledger[(symbol, "UNKNOWN", "UNKNOWN")] = {"exchange": symbol.split(".")[0],
                "product": str(meta.get(symbol, {}).get("product_id", "UNKNOWN")), "symbol": symbol,
                "trading_day": "UNKNOWN", "session": "UNKNOWN", "rows": 0,
                "first_event_ns": None, "last_event_ns": None, "collection_status": "evidence_insufficient"}
    gaps = [{"id": r[0], "symbol": r[1], "detected_ns": r[2], "kind": r[3], "status": r[4],
             "start_ns": r[5], "end_ns": r[6], "detail": json.loads(r[7])} for r in db.execute("SELECT * FROM gaps")]
    for symbol,contract in contracts.items():
        first=contract["actual_earliest_ns"]
        target=contract["target_start_ns"]
        if first is not None and first>target and not any(g["symbol"]==symbol and g["kind"] in ("startup_window_after_target","startup_prefix_unverified") for g in gaps):
            gaps.append({"id":"audit:"+symbol,"symbol":symbol,"kind":"startup_prefix_unverified",
                         "status":"gap_pending_replay","start_ns":target,"end_ns":first,
                         "detail":{"evidence":"local earliest follows requested start; wider source replay needed",
                                   "missing_tick_count":"unknown; legitimate quiet is not excluded"}})
    asof=time.time_ns()
    if contracts:
        first_day=datetime.fromtimestamp(min(c["target_start_ns"] for c in contracts.values())//1_000_000_000,CST).date()
        last_day=datetime.fromtimestamp(asof//1_000_000_000,CST).date()+timedelta(days=31)
        days=[date.fromisoformat(d) for d,opened in calendar.items() if opened and first_day.isoformat()<=d<=last_day.isoformat()]
        for symbol,contract in contracts.items():
            for day in days:
                for scope in planned_sessions(day,meta.get(symbol,{}),calendar,overrides):
                    begin=max(scope["start_ns"],contract["target_start_ns"])
                    end=min(scope["end_ns"],asof)
                    if end<=begin:
                        continue
                    key=(symbol,scope["trading_day"],scope["session"])
                    entry=ledger.setdefault(key,{"exchange":symbol.split(".")[0],"product":str(meta.get(symbol,{}).get("product_id","UNKNOWN")),
                        "symbol":symbol,"trading_day":scope["trading_day"],"session":scope["session"],"rows":0,
                        "first_event_ns":None,"last_event_ns":None,"collection_status":"evidence_insufficient"})
                    entry.update({"target_start_ns":begin,"target_end_ns":end,"planned_status":scope["certainty"]})
                    related=[g for g in gaps if g["symbol"] in (None,symbol) and g.get("start_ns") is not None and g.get("end_ns") is not None
                             and g["start_ns"]<end and g["end_ns"]>begin]
                    states={g["status"] for g in related}
                    if "gap_unrecoverable" in states:
                        entry["collection_status"]="gap_unrecoverable"
                    elif "gap_pending_replay" in states:
                        entry["collection_status"]="gap_pending_replay"
    pair_flags = Counter()
    for detail, in db.execute("SELECT detail FROM events WHERE kind='quality'"):
        pair_flags.update(json.loads(detail)["flags"])
    summary = {"format": "tfcoverage-1", "generated_ns": time.time_ns(),
        "collection_integrity": "evidence_insufficient", "archive_integrity": "separate manifest/remote receipt required",
        "expected_contracts": len(contracts), "subscription_states": dict(Counter(c["subscription_status"] for c in contracts.values())),
        "persisted_contracts": len({r["symbol"] for r in ledger.values() if r["rows"]}),
        "persisted_rows": sum(r["rows"] for r in ledger.values()), "origins": dict(origin_counts),
        "gaps": gaps, "gap_states": dict(Counter(g["status"] for g in gaps)),
        "contracts_with_unverified_prefix":len({g["symbol"] for g in gaps if g["kind"] in ("startup_window_after_target","startup_prefix_unverified") and g["status"]!="window_verified"}),
        "pair_flags": dict(pair_flags), "field_flags": dict(field_flags),
        "upstream_zero_loss_proven": False, "complete_day_night_acceptance": "PENDING",
        "contracts": list(contracts.values()), "ledger": list(ledger.values())}
    db.close()
    if output:
        atomic_json(output, summary)
    return summary
