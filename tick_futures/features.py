"""Compute causal snapshot features while retaining raw data and anomaly flags."""

import argparse
import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

from .common import timestamp, write_json

REQUIRED = ["datetime", "id", "symbol", "last_price", "volume", "amount", "open_interest",
            "bid_price1", "bid_volume1", "ask_price1", "ask_volume1"]


def build_features(ticks, multiplier, max_gap_seconds=2.0):
    """No sorting, deduplication, forward filling, or negative-delta replacement."""
    if not math.isfinite(multiplier) or multiplier <= 0:
        raise ValueError("multiplier must be finite and positive")
    if not math.isfinite(max_gap_seconds) or max_gap_seconds <= 0:
        raise ValueError("max_gap_seconds must be finite and positive")
    missing = set(REQUIRED) - set(ticks.columns)
    if missing:
        raise ValueError(f"Missing columns: {', '.join(sorted(missing))}")
    data = ticks.copy().reset_index(drop=True)
    same_symbol = data["symbol"].eq(data["symbol"].shift())
    has_previous = same_symbol & data["id"].shift().notna()
    gaps = data["datetime"].diff() / 1_000_000_000
    data["interval_seconds"] = gaps.where(has_previous)
    data["interval_valid"] = (has_previous & data["id"].diff().eq(1)
                              & gaps.gt(0) & gaps.le(max_gap_seconds))

    bid, ask = data["bid_price1"], data["ask_price1"]
    bid_qty, ask_qty = data["bid_volume1"], data["ask_volume1"]
    quote_valid = np.isfinite(bid) & np.isfinite(ask) & bid.gt(0) & ask.ge(bid)
    qty_valid = (np.isfinite(bid_qty) & np.isfinite(ask_qty)
                 & bid_qty.ge(0) & ask_qty.ge(0) & (bid_qty + ask_qty).gt(0))
    data["quote_valid"] = quote_valid
    data["mid_price"] = ((bid + ask) / 2).where(quote_valid)
    data["spread"] = (ask - bid).where(quote_valid)
    data["book_imbalance1"] = ((bid_qty - ask_qty) / (bid_qty + ask_qty)).where(quote_valid & qty_valid)

    volume_diff = data["volume"].diff().where(has_previous)
    amount_diff = data["amount"].diff().where(has_previous)
    data["volume_delta_raw"] = volume_diff
    data["amount_delta_raw"] = amount_diff
    data["volume_decreased"] = volume_diff.lt(0)
    data["amount_decreased"] = amount_diff.lt(0)
    valid = data["interval_valid"]
    data["volume_delta"] = volume_diff.where(valid & np.isfinite(volume_diff) & volume_diff.ge(0))
    data["amount_delta"] = amount_diff.where(valid & np.isfinite(amount_diff) & amount_diff.ge(0))
    data["open_interest_delta"] = data["open_interest"].diff().where(valid)
    mean_valid = data["volume_delta"].gt(0) & data["amount_delta"].gt(0)
    data["interval_vwap"] = (data["amount_delta"] / (data["volume_delta"] * multiplier)).where(mean_valid)
    return data


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True, help="Tick CSV or CSV.gz")
    parser.add_argument("--output", type=Path, required=True)
    metadata = parser.add_mutually_exclusive_group(required=True)
    metadata.add_argument("--quote", type=Path, help="Quote JSON containing volume_multiple")
    metadata.add_argument("--multiplier", type=float)
    parser.add_argument("--max-gap-seconds", type=float, default=2.0)
    args = parser.parse_args()
    if args.input.resolve() == args.output.resolve():
        parser.error("output must differ from input")
    multiplier = float(json.loads(args.quote.read_text())["volume_multiple"]) if args.quote else args.multiplier
    ticks = pd.read_csv(args.input, dtype={"datetime": "int64", "id": "int64"})
    result = build_features(ticks, multiplier, args.max_gap_seconds)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(args.output, index=False, compression="infer")
    summary = {"created_at": timestamp(), "input": str(args.input), "output": str(args.output),
               "input_sha256": hashlib.sha256(args.input.read_bytes()).hexdigest(),
               "rows": len(result), "volume_multiple": multiplier,
               "max_gap_seconds": args.max_gap_seconds,
               "invalid_quote_rows": int((~result["quote_valid"]).sum()),
               "unlinked_interval_rows": int((~result["interval_valid"]).sum()),
               "volume_decrease_rows": int(result["volume_decreased"].sum()),
               "amount_decrease_rows": int(result["amount_decreased"].sum()),
               "note": "Snapshot features only; intervals are not individual trades. Negative raw deltas are retained."}
    write_json(Path(str(args.output) + ".summary.json"), summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
