"""Read-only single-contract quote probe, migrated from the verified futures project."""

import argparse
from datetime import datetime, timedelta
import hashlib
import importlib.metadata
import json
import os
import signal
import sys
import time

from .common import BASE, CST, RedactedStream, clean, load_auth, timestamp, write_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default="SHFE.rb2701")
    parser.add_argument("--observe-seconds", type=float, default=20)
    parser.add_argument("--data-length", type=int, default=10000)
    parser.add_argument("--probe-historical-download", action="store_true",
                        help="Opt in only; current account has already been denied this capability")
    args = parser.parse_args()
    if not 1 <= args.data_length <= 10000 or not 0 <= args.observe_seconds <= 300:
        parser.error("data-length must be 1..10000; observe-seconds must be 0..300")
    user, password, source = load_auth()
    sys.stdout = RedactedStream(sys.stdout, (password, user))
    sys.stderr = RedactedStream(sys.stderr, (password, user))
    os.umask(0o077)
    folder = BASE / "data/raw/tick_verification" / datetime.now(CST).strftime("%Y%m%d_%H%M%S_%f")
    folder.mkdir(parents=True, mode=0o700)
    result = {"status": "running", "started_at": timestamp(), "symbol": args.symbol,
              "auth_source": source, "account_mode": "TqSim",
              "broker_account_connected": False, "orders_submitted": 0,
              "output_directory": str(folder.relative_to(BASE))}
    api = None

    def timeout_handler(signum, frame):
        raise TimeoutError("Tick probe exceeded its process time budget")

    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(240 + int(args.observe_seconds))
    try:
        import pandas as pd
        from tqsdk import TqApi, TqAuth, TqSim

        class ReadOnlyApi(TqApi):
            def _send_pack(self, pack):
                if pack.get("aid") in ("insert_order", "cancel_order"):
                    raise RuntimeError("Order operations are disabled in this probe")
                return super()._send_pack(pack)

        result["versions"] = {name: importlib.metadata.version(name)
                              for name in ("tqsdk", "pandas", "numpy")}
        print(f"{timestamp()} Connecting to quote service with TqSim", flush=True)
        auth = TqAuth(user, password)
        api = ReadOnlyApi(account=TqSim(), auth=auth, debug=False, disable_print=True)
        result["connected_at"] = timestamp()
        quote = api.get_quote(args.symbol)
        if quote.expired:
            raise RuntimeError("The requested contract is expired")
        main_quote = api.get_quote("KQ.m@SHFE.rb") if args.symbol.startswith("SHFE.rb") else None
        result["contract"] = {"expired": bool(quote.expired),
                              "volume_multiple": float(quote.volume_multiple),
                              "trading_time": {key: list(value) for key, value in quote.trading_time.items()},
                              "current_main_contract": main_quote.underlying_symbol if main_quote else None}
        print(f"{timestamp()} Requesting {args.data_length} ticks for {args.symbol}", flush=True)
        serial = api.get_tick_serial(args.symbol, data_length=args.data_length)
        if not api.is_serial_ready(serial):
            raise RuntimeError("Tick serial did not finish initialization")

        def snapshot(frame):
            data = frame.loc[(frame["datetime"] > 0) & (frame["id"] >= 0)].copy()
            for name in ("datetime", "id"):
                data[name] = data[name].astype("int64")
            data["time"] = pd.to_datetime(data["datetime"], unit="ns", utc=True).dt.tz_convert("Asia/Shanghai").astype(str)
            return data

        ticks = snapshot(serial)
        if ticks.empty:
            raise RuntimeError("Tick serial contains no valid records")
        ticks.to_csv(folder / "recent_ticks.csv", index=False)
        gaps = ticks["datetime"].diff().iloc[1:] / 1_000_000
        counts = gaps.round(6).value_counts().head(10)
        key_fields = ["time", "id", "last_price", "volume", "amount", "open_interest",
                      "bid_price1", "bid_volume1", "ask_price1", "ask_volume1"]
        latest = pd.to_datetime(int(ticks["datetime"].iloc[-1]), unit="ns", utc=True)
        result["ticks"] = {
            "requested_rows": args.data_length, "valid_rows": len(ticks),
            "first_time": ticks["time"].iloc[0], "last_time": ticks["time"].iloc[-1],
            "latest_age_seconds_at_snapshot": time.time() - latest.timestamp(),
            "datetime_monotonic": bool(ticks["datetime"].is_monotonic_increasing),
            "duplicate_sdk_ids": int(ticks["id"].duplicated().sum()),
            "sdk_id_gap_count": int((ticks["id"].diff().iloc[1:] != 1).sum()),
            "same_timestamp_intervals": int((gaps == 0).sum()),
            "intervals_ms": [{"milliseconds": float(gap), "count": int(count)}
                             for gap, count in counts.items()],
            "interval_ms_min": float(gaps.min()), "interval_ms_max": float(gaps.max()),
            "cumulative_volume_decreases": int((ticks["volume"].diff() < 0).sum()),
            "cumulative_amount_decreases": int((ticks["amount"].diff() < 0).sum()),
            "missing_key_fields": {name: int(ticks[name].isna().sum()) for name in key_fields},
            "last_three_rows": json.loads(ticks[key_fields].tail(3).to_json(orient="records", double_precision=15)),
            "large_intervals": [{"from": ticks["time"].iloc[position - 1],
                                 "to": ticks["time"].iloc[position], "milliseconds": float(gaps.iloc[position - 1])}
                                for position in range(1, len(ticks)) if gaps.iloc[position - 1] > 2000][:10],
        }
        print(f"{timestamp()} Received {len(ticks)} valid ticks; latest {ticks['time'].iloc[-1]}", flush=True)

        bars = snapshot(api.get_kline_serial(args.symbol, duration_seconds=1, data_length=600))
        bars.to_csv(folder / "recent_1s_bars.csv", index=False)
        closed = bars.iloc[:-1].copy()
        closed["ma20"] = closed["close"].rolling(20).mean()
        closed.to_csv(folder / "closed_1s_features.csv", index=False)
        result["one_second_bars"] = {"valid_rows": len(bars), "conservatively_closed_rows": len(closed),
                                     "first_time": bars["time"].iloc[0] if len(bars) else None,
                                     "last_time": bars["time"].iloc[-1] if len(bars) else None,
                                     "last_three_closed_rows": json.loads(closed.tail(3).to_json(orient="records", double_precision=15))}

        last_id = int(ticks["id"].iloc[-1])
        live_chunks = []
        observation_start = time.monotonic()
        result["live_observation"] = {"started_at": timestamp(), "requested_seconds": args.observe_seconds}
        while time.monotonic() - observation_start < args.observe_seconds:
            remaining = args.observe_seconds - (time.monotonic() - observation_start)
            api.wait_update(deadline=time.time() + min(1, max(0, remaining)))
            fresh = serial.loc[(serial["id"] > last_id) & (serial["datetime"] > 0)]
            if len(fresh):
                batch = snapshot(fresh)
                batch["observed_at"] = timestamp()
                live_chunks.append(batch)
                last_id = int(batch["id"].iloc[-1])
        live = pd.concat(live_chunks, ignore_index=True) if live_chunks else ticks.iloc[:0].copy()
        live.to_csv(folder / "observed_new_ticks.csv", index=False)
        result["live_observation"].update({"ended_at": timestamp(),
                                           "elapsed_seconds": time.monotonic() - observation_start,
                                           "new_tick_count": len(live),
                                           "live_updates_verified": len(live) > 0})

        result["historical_tick_download"] = {"status": "skipped_known_permission_denied"}
        if args.probe_historical_download:
            # A one-minute historical probe never purchases or activates any entitlement.
            from tqsdk.tools import DataDownloader
            history_end = latest.tz_convert(CST).floor("min").to_pydatetime()
            history_start = history_end - timedelta(minutes=1)
            result["historical_tick_download"] = {"start": history_start.isoformat(), "end": history_end.isoformat()}
            try:
                history_path = folder / "historical_tick_1m.csv"
                task = DataDownloader(api, args.symbol, 0, history_start, history_end, str(history_path))
                deadline = time.time() + 45
                while not task.is_finished():
                    if not api.wait_update(deadline=deadline):
                        raise TimeoutError("Historical Tick download timed out")
                # is_finished alone does not prove successful data delivery.
                history = pd.read_csv(history_path)
                result["historical_tick_download"].update({"status": "passed" if len(history) else "empty", "rows": len(history)})
            except Exception as exc:
                result["historical_tick_download"].update({"status": "unavailable", "error_type": type(exc).__name__, "error": str(exc)})

        result["status"] = "passed_recent_tick_retrieval"
    except Exception as exc:
        result.update({"status": "failed", "error_type": type(exc).__name__, "error": str(exc)})
    finally:
        signal.alarm(15)
        try:
            if api is not None:
                api.close()
            result["closed_cleanly"] = True if api is not None else None
        except Exception as exc:
            result["closed_cleanly"] = False
            result["close_error"] = str(exc)
        signal.alarm(0)
        result["finished_at"] = timestamp()
        for secret in (password, user):
            result = json.loads(json.dumps(result, ensure_ascii=False).replace(secret, "[REDACTED]"))
        result["files_sha256"] = {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                                   for path in folder.glob("*.csv")}
        write_json(folder / "summary.json", result)
        print(json.dumps(clean(result), ensure_ascii=False, indent=2, allow_nan=False), flush=True)
    return 0 if result["status"] == "passed_recent_tick_retrieval" else 1


if __name__ == "__main__":
    raise SystemExit(main())
