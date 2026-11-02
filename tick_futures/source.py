"""Only the previously verified public TqSdk path; explicitly local TqSim."""

from collections import Counter
from datetime import date, datetime, timedelta
import importlib.metadata
import sys
import time

from .codec import frame_rows, dumps, pack, sha256
from .common import CST, RedactedStream, load_auth, timestamp
from .runtime import atomic_bytes, atomic_json

EXCHANGES = ("SHFE", "INE", "DCE", "CZCE", "CFFEX", "GFEX")


def connect():
    user, password, auth_source = load_auth()
    sys.stdout = RedactedStream(sys.stdout, (password, user))
    sys.stderr = RedactedStream(sys.stderr, (password, user))
    from tqsdk import TqApi, TqAuth, TqSim

    class QuoteOnlyApi(TqApi):
        def _send_pack(self, packet):
            if packet.get("aid") in ("insert_order", "cancel_order"):
                raise RuntimeError("Trading operations are disabled")
            self.request_counts[packet.get("aid", "unknown")] += 1
            return super()._send_pack(packet)

        def __init__(self):
            self.request_counts = Counter()
            super().__init__(account=TqSim(), auth=TqAuth(user, password),
                             debug=False, disable_print=True)

    api = QuoteOnlyApi()
    api.auth_source = auth_source
    return api


def discover(api, output, *, calendar=True):
    from pathlib import Path
    output = Path(output)
    begin = time.monotonic()
    symbols = list(api.query_quotes(ins_class="FUTURE", expired=False,
                                   exchange_id=list(EXCHANGES)))
    if len(symbols) != len(set(symbols)) or any(s.split(".")[0] not in EXCHANGES for s in symbols):
        raise ValueError("Unexpected duplicate/exchange in public enumeration")
    # Keep vendor order, no inferred CZCE year, no activity/main-contract filter.
    metadata = []
    failures = []
    for offset in range(0, len(symbols), 100):
        batch = symbols[offset:offset + 100]
        try:
            data = api.query_symbol_info(batch)
            # Metadata need not contain datetime/id; don't use tick filtering.
            columns = [(str(k), data[k].to_numpy(copy=True)) for k in data.columns]
            metadata.extend({k: values[i] for k, values in columns} for i in range(len(data)))
        except Exception as exc:
            failures.append({"symbols": batch, "error_type": type(exc).__name__, "error": str(exc)})
    raw = dumps(metadata)
    version = str(time.time_ns())
    atomic_bytes(output / f"metadata-{version}.tfrec", raw)
    info = {"version": version, "captured_at": timestamp(), "source": "TqSdk.query_quotes",
            "sdk_version": importlib.metadata.version("tqsdk"), "account_mode": "TqSim",
            "query": {"ins_class": "FUTURE", "expired": False, "exchange_id": list(EXCHANGES)},
            "symbols": symbols, "count": len(symbols), "exchange_counts": dict(Counter(s.split(".")[0] for s in symbols)),
            "metadata_file": f"metadata-{version}.tfrec", "metadata_sha256": sha256(raw),
            "metadata_failures": failures, "exchange_crosscheck": "PENDING", "elapsed_seconds": time.monotonic() - begin}
    atomic_json(output / f"universe-{version}.json", info)
    atomic_json(output / "universe-latest.json", info)
    if calendar:
        try:
            today = datetime.now(CST).date()
            # The installed SDK calendar has an explicit last supported year.
            cal = api.get_trading_calendar(date(2003,1,1), min(today + timedelta(days=60), date(today.year, 12, 31)))
            # Calendar dates are strings; this isn't an integer timestamp conversion.
            rows = [{"date": str(row.date)[:10], "trading": bool(row.trading)} for row in cal.itertuples()]
            atomic_json(output / f"calendar-{version}.json", {"version": version,
                        "source": "TqSdk.get_trading_calendar", "captured_at": timestamp(), "days": rows,
                        "night_overrides": {}, "night_override_status": "PENDING exchange holiday notices"})
        except Exception as exc:
            info["calendar_error"] = {"type": type(exc).__name__, "message": str(exc)}
            atomic_json(output / "universe-latest.json", info)
    return info


async def refresh_universe(api, output,previous_symbols=()):
    """Public awaitable metadata queries keep wait_update driving subscriptions."""
    from pathlib import Path
    output = Path(output)
    begin = time.monotonic()
    result=api.query_quotes(ins_class="FUTURE", expired=False, exchange_id=list(EXCHANGES))
    await result
    symbols=list(result)
    metadata, failures = [], []
    previous_missing=[s for s in previous_symbols if s not in symbols]
    metadata_targets=symbols+previous_missing
    for offset in range(0, len(metadata_targets), 100):
        batch = metadata_targets[offset:offset+100]
        try:
            data = api.query_symbol_info(batch)
            await data
            cols = [(str(k),data[k].to_numpy(copy=True)) for k in data.columns]
            metadata.extend({k:v[i] for k,v in cols} for i in range(len(data)))
        except Exception as exc:
            failures.append({"symbols":batch,"error_type":type(exc).__name__,"error":str(exc)})
    version = str(time.time_ns())
    raw = dumps(metadata)
    info = {"version":version,"captured_at":timestamp(),"source":"TqSdk.query_quotes",
            "sdk_version":importlib.metadata.version("tqsdk"),"account_mode":"TqSim",
            "query":{"ins_class":"FUTURE","expired":False,"exchange_id":list(EXCHANGES)},
            "symbols":symbols,"count":len(symbols),"exchange_counts":dict(Counter(s.split(".")[0] for s in symbols)),
            "metadata_file":f"metadata-{version}.tfrec","metadata_sha256":sha256(raw),
            "metadata_failures":failures,"exchange_crosscheck":"PENDING","elapsed_seconds":time.monotonic()-begin}
    info["previous_symbols_missing_from_vendor_list"]=previous_missing
    atomic_bytes(output/info["metadata_file"],raw)
    atomic_json(output/f"universe-{version}.json",info)
    atomic_json(output/"universe-latest.json",info)
    return info, metadata
