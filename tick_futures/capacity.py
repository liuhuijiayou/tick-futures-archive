"""Explicit capacity scenarios and row-window polling arithmetic, not guarantees."""

from pathlib import Path
import json
import math

from .codec import loads
from .sessions import clock_seconds


def daily_quote_seconds(metadata, symbols=None):
    selected = None if symbols is None else set(symbols)
    result, unknown = {}, []
    for row in metadata:
        symbol = str(row["instrument_id"])
        if selected is not None and symbol not in selected:
            continue
        intervals = []
        for key in ("trading_time_day", "trading_time_night"):
            for start, end in row.get(key) or []:
                lo, hi = clock_seconds(start), clock_seconds(end)
                if hi <= lo:
                    hi += 86400
                intervals.append((lo, hi))
        if not intervals:
            unknown.append(symbol)
            result[symbol] = 86400  # conservative unknown-day scenario, never zero
            continue
        merged = []
        for lo, hi in sorted(intervals):
            if merged and lo <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(merged[-1][1], hi))
            else:
                merged.append((lo, hi))
        result[symbol] = sum(hi - lo for lo, hi in merged)
    if selected is not None and selected - result.keys():
        raise ValueError("Selected contracts missing metadata: " + ",".join(sorted(selected - result.keys())))
    return {"contracts": len(result), "quote_seconds": sum(result.values()),
            "unknown_sessions": unknown, "per_contract_seconds": result,
            "basis": "one snapshot per configured trading second on an ordinary trading day; no fabricated quiet-second rows"}


def polling_interval(peak_rows_per_second, *, window_rows=10000, overlap_fraction=.3,
                     fetch_seconds=300, retry_seconds=600):
    values = (peak_rows_per_second, overlap_fraction, fetch_seconds, retry_seconds)
    if not all(math.isfinite(v) for v in values) or peak_rows_per_second <= 0:
        raise ValueError("Finite positive source row rate required")
    if not 0 <= overlap_fraction < 1 or not 1 <= window_rows <= 10000 or min(fetch_seconds, retry_seconds) < 0:
        raise ValueError("Invalid polling bounds")
    history = window_rows / peak_rows_per_second
    interval = max(0, (1 - overlap_fraction) * history - fetch_seconds - retry_seconds)
    return {"assumed_source_rows_per_second": peak_rows_per_second,
            "maximum_rows": window_rows, "theoretical_history_seconds": history,
            "overlap_fraction": overlap_fraction, "fetch_seconds": fetch_seconds,
            "retry_seconds": retry_seconds, "scenario_max_interval_seconds": interval,
            "source_peak_rate_certified": False,
            "note": "Saving one snapshot per second does not enlarge the source Tick row window"}


def capacity_plan(root, benchmark):
    root = Path(root)
    universe = json.loads((root / "discovery/universe-latest.json").read_text())
    metadata = loads((root / "discovery" / universe["metadata_file"]).read_bytes())
    bench = json.loads(Path(benchmark).read_text())
    mains_path = root / "main-discovery/main-latest.json"
    mains = json.loads(mains_path.read_text()) if mains_path.exists() else None
    scopes = {"all_listed": daily_quote_seconds(metadata)}
    if mains:
        scopes["provider_main"] = daily_quote_seconds(metadata, [m["symbol"] for m in mains["mapping"]])
    for name, scope in scopes.items():
        measured = bench["scopes"][name]
        if measured["selected_snapshots"] <= 0:
            raise ValueError("No measured snapshots for " + name)
        scope["measured_rows"] = measured["selected_snapshots"]
        scope["measured_raw_bytes"] = measured["raw_tfrec_bytes"]
        scope["measured_compressed_bytes"] = measured["compressed_bytes"]
        scope["bytes_per_measured_snapshot"] = measured["compressed_bytes"] / measured["selected_snapshots"]
        scope["projected_day_raw_bytes"] = math.ceil(scope["quote_seconds"] * measured["raw_tfrec_bytes"] / measured["selected_snapshots"])
        scope["projected_day_payload_bytes"] = math.ceil(scope["quote_seconds"] * scope["bytes_per_measured_snapshot"])
        scope["planning_day_bytes_at_3x_payload"] = scope["projected_day_payload_bytes"] * 3
        scope.pop("per_contract_seconds")
    return {"format": "tfcapacity-1", "universe_version": universe["version"],
            "main_mapping_version": mains.get("version") if mains else None,
            "main_mapping_status": mains.get("status") if mains else None,
            "sampling_policy": bench["sampling_policy"], "scopes": scopes,
            "local_total_budget_bytes": 100_000_000_000,
            "budget_is_per_day": False,
            "planning_multiplier_is_measured": False,
            "forecast_not_complete_open_day_measurement": True,
            "exclusions": "Daily payload forecast excludes retained old evidence/test copies, hot SQLite, staging, runtime and restoration copies. 3x is a planning scenario, not a guaranteed compression bound.",
            "polling_scenarios": [polling_interval(r) for r in (1, 2, 3, 5)],
            "production_schedule_or_retention_changed": False}
