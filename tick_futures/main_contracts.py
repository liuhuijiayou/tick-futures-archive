"""Versioned provider main-contract selection; never rewrite the full universe."""

from collections import Counter, defaultdict
from pathlib import Path
import json
import time

from .codec import loads
from .runtime import atomic_json
from .source import EXCHANGES


def discover_mains(api, discovery, output):
    discovery, output = Path(discovery), Path(output)
    universe = json.loads((discovery / "universe-latest.json").read_text())
    metadata = loads((discovery / universe["metadata_file"]).read_bytes())
    eligible = {str(m["instrument_id"]): m for m in metadata
                if str(m.get("ins_class")) == "FUTURE" and not bool(m.get("expired"))
                and str(m["instrument_id"]).split(".")[0] in EXCHANGES}
    expected = {(s.split(".")[0], str(m["product_id"])) for s, m in eligible.items()}
    candidates, failures, rejected = defaultdict(list), [], []
    begun = time.monotonic()
    for exchange in EXCHANGES:
        try:
            symbols = list(api.query_cont_quotes(exchange_id=exchange))
        except Exception as exc:
            failures.append({"exchange": exchange, "error_type": type(exc).__name__, "error": str(exc)})
            continue
        for symbol in symbols:
            if symbol not in eligible or symbol.split(".")[0] != exchange:
                rejected.append({"symbol": symbol, "exchange": exchange,
                                 "reason": "not an unexpired real future in the versioned source catalog"})
                continue
            key = (exchange, str(eligible[symbol]["product_id"]))
            if symbol not in candidates[key]:
                candidates[key].append(symbol)
    mapping, missing, ambiguous = [], [], []
    for exchange, product in sorted(expected):
        contracts = candidates[(exchange, product)]
        entry = {"exchange": exchange, "product": product, "contracts": contracts}
        if not contracts:
            missing.append(entry)
        elif len(contracts) != 1:
            ambiguous.append(entry)
        else:
            mapping.append({"exchange": exchange, "product": product, "symbol": contracts[0]})
    version = str(time.time_ns())
    report = {"format": "tfmain-1", "version": version, "observed_ns": int(version),
              "source": "TqSdk.query_cont_quotes", "definition": "provider current main underlying, not an exchange universal standard",
              "account_mode": "TqSim", "universe_version": universe["version"],
              "expected_products": len(expected), "selected_contracts": len(mapping),
              "exchange_counts": dict(Counter(m["exchange"] for m in mapping)),
              "mapping": mapping, "missing": missing, "ambiguous": ambiguous,
              "rejected": rejected, "failures": failures,
              "status": "verified_against_current_catalog" if not any((missing, ambiguous, rejected, failures)) else "incomplete",
              "elapsed_seconds": time.monotonic() - begun,
              "production_universe_changed": False,
              "roll_policy_required": "save mappings before day/night open and on reconnect; overlap old/new months when mapping changes"}
    atomic_json(output / f"main-{version}.json", report)
    atomic_json(output / "main-latest.json", report)
    return report
