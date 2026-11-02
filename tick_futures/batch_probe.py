"""Probe multiple futures products in one read-only TqSdk session."""

import argparse
from collections import Counter
from datetime import datetime, timedelta
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import signal
import sys
import time

from .common import BASE, CST, RedactedStream, clean, load_auth, timestamp, write_json
from .probe_stream import ProbeStream

DEFAULT_PRODUCTS = ["SHFE.au", "SHFE.cu", "DCE.m", "DCE.i", "CZCE.MA",
                    "INE.sc", "GFEX.si", "CFFEX.IF"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--products", nargs="+", default=DEFAULT_PRODUCTS,
                        help="Exchange.product identifiers; actual contracts are resolved online")
    parser.add_argument("--tick-length", type=int, default=10000)
    parser.add_argument("--observe-seconds", type=float, default=15)
    parser.add_argument("--probe-historical-download", action="store_true",
                        help="Explicit opt-in only; current account is known to lack this permission")
    args = parser.parse_args()
    if not 2 <= args.tick_length <= 10000 or not 0 <= args.observe_seconds <= 300:
        parser.error("tick-length must be 2..10000; observe-seconds must be 0..300")
    if len(args.products) != len(set(args.products)):
        parser.error("products must be unique")
    user, password, source = load_auth()
    sys.stdout = RedactedStream(sys.stdout, (password, user))
    sys.stderr = RedactedStream(sys.stderr, (password, user))
    os.umask(0o077)
    root = BASE / "data/raw/futures_verification" / datetime.now(CST).strftime("%Y%m%d_%H%M%S_%f")
    root.mkdir(parents=True, mode=0o700)
    result = {"started_at": timestamp(), "status": "running", "auth_source": source,
              "account_mode": "TqSim", "broker_account_connected": False,
              "orders_submitted": 0, "requested_products": args.products,
              "output_directory": str(root.relative_to(BASE)), "products": []}
    api = None
    sent = Counter()
    streams = []

    def timeout_handler(signum, frame):
        raise TimeoutError("The read-only batch exceeded its process time budget")

    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(180 + 120 * len(args.products) + int(args.observe_seconds))
    try:
        import pandas as pd
        from tqsdk import TqApi, TqAuth, TqSim

        class ReadOnlyApi(TqApi):
            def wait_update(self, *args, **kwargs):
                result = super().wait_update(*args, **kwargs)
                for stream in streams:
                    stream.drain()
                return result

            def _send_pack(self, pack):
                aid = pack.get("aid", "unknown")
                if aid in ("insert_order", "cancel_order"):
                    raise RuntimeError("Order operations are disabled")
                sent[aid] += 1
                return super()._send_pack(pack)

        result["versions"] = {name: importlib.metadata.version(name)
                              for name in ("tqsdk", "pandas", "numpy")}
        result["python_version"] = sys.version.split()[0]
        auth = TqAuth(user, password)
        print(f"{timestamp()} Connecting one quote-only session", flush=True)
        api = ReadOnlyApi(account=TqSim(), auth=auth, debug=False, disable_print=True)
        result["connected_at"] = timestamp()
        result["account_features"] = {"futures_market_data": bool(auth._has_feature("futr")),
                                      "historical_download": bool(auth._has_feature("tq_dl"))}

        def snapshot(frame):
            data = frame.loc[(frame["datetime"] > 0) & (frame["id"] >= 0)].copy()
            data["datetime"] = data["datetime"].astype("int64")
            data["id"] = data["id"].astype("int64")
            data["time"] = pd.to_datetime(data["datetime"], unit="ns", utc=True).dt.tz_convert(CST).astype(str)
            return data

        price_keys = ("last_price", "highest", "lowest", "open", "close", "average",
                      "volume", "amount", "open_interest", "pre_open_interest", "settlement",
                      "pre_settlement", "pre_close", "upper_limit", "lower_limit", "price_tick",
                      "volume_multiple", "instrument_name", "instrument_id", "exchange_id", "product_id",
                      "ins_class", "expired", "expire_datetime")
        depth_keys = [f"{side}_{field}{level}" for level in range(1, 6)
                      for side in ("bid", "ask") for field in ("price", "volume")]
        key_fields = ["last_price", "volume", "amount", "open_interest",
                      "bid_price1", "bid_volume1", "ask_price1", "ask_volume1"]
        subscriptions = []
        for product in args.products:
            record = {"product": product, "started_at": timestamp(), "status": "running"}
            result["products"].append(record)
            folder = root / product.replace(".", "_")
            folder.mkdir(mode=0o700)
            record["directory"] = str(folder.relative_to(BASE))
            try:
                continuous = "KQ.m@" + product
                main_quote = api.get_quote(continuous)
                symbol = main_quote.underlying_symbol
                if not symbol:
                    raise RuntimeError("No current underlying contract was returned")
                quote = api.get_quote(symbol)
                if quote.expired or quote.ins_class != "FUTURE":
                    raise RuntimeError("Resolved symbol is not an unexpired futures contract")
                record.update({"continuous_symbol": continuous, "symbol": symbol,
                               "instrument_name": quote.instrument_name})
                quote_data = {key: getattr(quote, key, None) for key in (*price_keys, *depth_keys)}
                quote_data["datetime"] = quote.datetime
                quote_data["captured_at"] = timestamp()
                quote_data["trading_time"] = {key: list(value) for key, value in quote.trading_time.items()}
                write_json(folder / "quote.json", quote_data)
                record["quote"] = clean(quote_data)
                print(f"{timestamp()} {product} -> {symbol}: requesting ticks", flush=True)
                before = time.monotonic()
                serial = api.get_tick_serial(symbol, data_length=args.tick_length)
                if not api.is_serial_ready(serial):
                    raise RuntimeError("Tick serial did not finish initialization")
                ticks = snapshot(serial)
                if ticks.empty:
                    raise RuntimeError("No valid ticks were returned")
                ticks.to_csv(folder / "ticks.csv", index=False)
                # Persist the snapshot before using its ID as a committed cursor.
                with (folder / "ticks.csv").open("rb") as persisted:
                    os.fsync(persisted.fileno())
                sink = ProbeStream(serial, ticks, folder / "observed_new_ticks.csv", snapshot, timestamp)
                streams.append(sink)
                gaps = ticks["datetime"].diff().iloc[1:] / 1_000_000
                depth = {}
                for level in range(1, 6):
                    cols = [f"bid_price{level}", f"ask_price{level}",
                            f"bid_volume{level}", f"ask_volume{level}"]
                    if not all(col in ticks for col in cols):
                        depth[str(level)] = {"valid_bilateral_rows": 0, "columns_present": False}
                    else:
                        valid = (ticks[cols].notna().all(axis=1)
                                 & ticks[cols[0]].gt(0) & ticks[cols[1]].gt(0)
                                 & ticks[cols[2]].ge(0) & ticks[cols[3]].ge(0))
                        depth[str(level)] = {"valid_bilateral_rows": int(valid.sum()), "columns_present": True}
                sample_fields = ["id", "time", *key_fields]
                record["ticks"] = {
                    "rows": len(ticks), "requested_rows": args.tick_length,
                    "initialization_seconds": time.monotonic() - before,
                    "first_time": ticks["time"].iloc[0], "last_time": ticks["time"].iloc[-1],
                    "columns": list(ticks.columns), "dtypes": {key: str(value) for key, value in ticks.dtypes.items()},
                    "missing_key_fields": {key: int(ticks[key].isna().sum()) for key in key_fields},
                    "datetime_monotonic": bool(ticks["datetime"].is_monotonic_increasing),
                    "duplicate_sdk_ids": int(ticks["id"].duplicated().sum()),
                    "sdk_id_gap_count": int(ticks["id"].diff().iloc[1:].ne(1).sum()),
                    "cumulative_volume_decreases": int(ticks["volume"].diff().lt(0).sum()),
                    "cumulative_amount_decreases": int(ticks["amount"].diff().lt(0).sum()),
                    "intervals_ms": [{"milliseconds": float(gap), "count": int(count)}
                                     for gap, count in gaps.round(6).value_counts().head(8).items()],
                    "depth": depth,
                    "last_three_rows": json.loads(ticks[sample_fields].tail(3).to_json(orient="records", double_precision=15)),
                }
                record["klines"] = {}
                for period, length in ((1, 600), (5, 200), (60, 200), (86400, 100)):
                    bars = snapshot(api.get_kline_serial(symbol, duration_seconds=period, data_length=length))
                    bars.to_csv(folder / f"klines_{period}s.csv", index=False)
                    record["klines"][str(period)] = {"requested_rows": length, "rows": len(bars),
                        "first_time": bars["time"].iloc[0] if len(bars) else None,
                        "last_time": bars["time"].iloc[-1] if len(bars) else None,
                        "columns": list(bars.columns),
                        "missing_ohlc": {key: int(bars[key].isna().sum()) for key in ("open", "high", "low", "close")}}
                    if bars.empty:
                        raise RuntimeError(f"No valid {period}s bars were returned")
                record["status"] = "passed"
                subscriptions.append((record, sink, folder))
                print(f"{timestamp()} {symbol}: {len(ticks)} ticks, four K-line periods received", flush=True)
            except Exception as exc:
                record.update({"status": "failed", "error_type": type(exc).__name__, "error": str(exc)})
                print(f"{timestamp()} {product}: {type(exc).__name__}: {exc}", flush=True)
            record["finished_at"] = timestamp()
            write_json(folder / "summary.json", record)

        result["observation_started_at"] = timestamp()
        before = time.monotonic()
        while time.monotonic() - before < args.observe_seconds:
            remaining = args.observe_seconds - (time.monotonic() - before)
            api.wait_update(deadline=time.time() + min(1, max(0, remaining)))
        result["observation_ended_at"] = timestamp()
        result["observation_seconds"] = time.monotonic() - before
        for record, sink, folder in subscriptions:
            sink.drain()
            record["live_new_ticks"] = sink.rows
            record["live_updates_verified"] = sink.rows > 0
            write_json(folder / "summary.json", record)

        result["historical_download_probe"] = {"status": "skipped_known_permission_denied"}
        if subscriptions and args.probe_historical_download:
            from tqsdk.tools import DataDownloader
            first = subscriptions[0][0]
            end = pd.Timestamp(first["ticks"]["last_time"]).floor("min").to_pydatetime()
            start = end - timedelta(minutes=1)
            probe = {"symbol": first["symbol"], "start": start.isoformat(), "end": end.isoformat()}
            result["historical_download_probe"] = probe
            try:
                path = root / "historical_tick_1m.csv"
                task = DataDownloader(api, first["symbol"], 0, start, end, str(path))
                deadline = time.time() + 45
                while not task.is_finished():
                    if not api.wait_update(deadline=deadline):
                        raise TimeoutError("Historical download timed out")
                data = pd.read_csv(path)
                probe.update({"status": "passed" if len(data) else "empty", "rows": len(data)})
            except Exception as exc:
                probe.update({"status": "unavailable", "error_type": type(exc).__name__, "error": str(exc)})
        result["status"] = "passed" if all(record["status"] == "passed" for record in result["products"]) else "partial"
    except Exception as exc:
        result.update({"status": "failed", "error_type": type(exc).__name__, "error": str(exc)})
    finally:
        signal.alarm(15)
        try:
            if api is not None:
                api.close()
            result["closed_cleanly"] = True if api is not None else None
        except Exception as exc:
            result.update({"closed_cleanly": False, "close_error": str(exc)})
        signal.alarm(0)
        result["finished_at"] = timestamp()
        result["queued_request_counts"] = dict(sent)
        result["request_count_note"] = "SDK queue submissions; not a server QPS limit or websocket packet benchmark"
        result["files_sha256"] = {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
                                   for path in root.rglob("*") if path.is_file()}
        encoded = json.dumps(clean(result), ensure_ascii=False)
        for secret in (password, user):
            encoded = encoded.replace(secret, "[REDACTED]")
        result = json.loads(encoded)
        write_json(root / "summary.json", result)
        brief = {key: result.get(key) for key in ("status", "started_at", "finished_at", "output_directory", "closed_cleanly", "historical_download_probe")}
        brief["products"] = [{"product": item["product"], "symbol": item.get("symbol"),
                              "status": item["status"], "ticks": item.get("ticks", {}).get("rows"),
                              "klines": {key: value["rows"] for key, value in item.get("klines", {}).items()},
                              "depth": item.get("ticks", {}).get("depth"),
                              "live_new_ticks": item.get("live_new_ticks")}
                             for item in result["products"]]
        print(json.dumps(brief, ensure_ascii=False, indent=2), flush=True)
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
