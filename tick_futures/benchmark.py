"""Byte-exact corpus benchmarks, each codec in a fresh measured subprocess."""

import argparse
import gzip
import importlib.metadata
import json
import os
from pathlib import Path
import resource
import sqlite3
import subprocess
import sys
import time

from .archive import encode, decode, file_hash
from .codec import sha256
from .common import BASE
from .runtime import atomic_json

CONFIGURATIONS = [("gzip",9), ("zstd",3), ("zstd",9), ("zstd",19),
                  ("xz",6), ("xz",9), ("parquet-zstd",9), ("parquet-zstd",19)]


def prepare(destination, live_root=None):
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    manifest = json.loads((BASE/"evidence/manifest.json").read_text())
    files = []
    for index, entry in enumerate(manifest["files"]):
        stored = (BASE/entry["destination"]).read_bytes()
        if sha256(stored) != entry["sha256_stored"]:
            raise ValueError("Original evidence changed")
        raw = gzip.decompress(stored) if entry["encoding"] == "gzip" else stored
        if sha256(raw) != entry["sha256_uncompressed"]:
            raise ValueError("Original evidence roundtrip failed")
        path = destination/f"legacy-{index:03d}.raw"
        path.write_bytes(raw)
        files.append({"path": path.name, "source": entry["destination"], "bytes": len(raw), "sha256": sha256(raw)})
    if live_root:
        db = sqlite3.connect(f"file:{Path(live_root).resolve()/'hot.sqlite3'}?mode=ro", uri=True)
        for symbol, in db.execute("SELECT DISTINCT symbol FROM records"):
            path = destination/("new-" + symbol + ".tfrec")
            with path.open("wb") as stream:
                for raw, in db.execute("SELECT raw FROM records WHERE symbol=? ORDER BY seq", (symbol,)):
                    stream.write(raw)
            files.append({"path": path.name, "source": "new_typed_records:"+symbol,
                          "bytes": path.stat().st_size, "sha256": file_hash(path)})
        db.close()
    lock=json.loads((BASE/"decoder/runtime-lock.json").read_text())
    result = {"files":files,"legacy_original_bytes":manifest["total_source_bytes"],
              "legacy_current_gzip_and_json_bytes":manifest["total_stored_bytes"],
              "decoder_source_bytes":sum((BASE/"tick_futures"/p).stat().st_size for p in ("codec.py","archive.py","runtime.py")),
              "dependency_declaration_bytes":(BASE/"pyproject.toml").stat().st_size+(BASE/"decoder/runtime-lock.json").stat().st_size,
              "core_runtime_wheel_bytes":lock["core_wheel_bytes"],
              "optional_parquet_wheel_bytes":lock["total_wheel_bytes"]-lock["core_wheel_bytes"],
              "new_live_day_night_coverage": "short day-session smoke only; full day/night PENDING"}
    atomic_json(destination/"corpus.json", result)
    return result


def worker(corpus, codec, level):
    os.nice(10)
    corpus = Path(corpus)
    spec = json.loads((corpus/"corpus.json").read_text())
    original = compressed = 0
    compression_seconds = decompression_seconds = 0.0
    entries = []
    for entry in spec["files"]:
        raw = (corpus/entry["path"]).read_bytes()
        start = time.perf_counter()
        encoded = encode(raw, codec, level)
        compression_seconds += time.perf_counter()-start
        start = time.perf_counter()
        restored = decode(encoded, codec, expected_size=len(raw))
        decompression_seconds += time.perf_counter()-start
        if restored != raw or sha256(restored) != entry["sha256"]:
            raise ValueError("Byte-exact benchmark roundtrip failed")
        original += len(raw)
        compressed += len(encoded)
        entries.append({"path":entry["path"],"original_bytes":len(raw),"stored_bytes":len(encoded),
                        "original_sha256":entry["sha256"],"stored_sha256":sha256(encoded)})
    decoder_bytes = spec["decoder_source_bytes"]
    metadata_bytes = len(json.dumps({"files":entries,"codec":codec,"level":level,"dictionary":None}).encode())
    dependency_bytes = spec["dependency_declaration_bytes"]
    payload_and_metadata = compressed + decoder_bytes + metadata_bytes + dependency_bytes
    runtime_bytes = spec["core_runtime_wheel_bytes"] + (spec["optional_parquet_wheel_bytes"] if codec=="parquet-zstd" else 0)
    total = payload_and_metadata + runtime_bytes
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform != "darwin":
        peak *= 1024
    return {"codec":codec,"level":level,"files":len(entries),"raw_bytes":original,
            "stored_payload_bytes":compressed,"metadata_decoder_declaration_bytes":payload_and_metadata-compressed,
            "shipped_runtime_wheel_bytes":runtime_bytes,"payload_code_metadata_bytes":payload_and_metadata,
            "final_total_bytes":total,"ratio":original/total,"compression_seconds":compression_seconds,
            "decompression_seconds":decompression_seconds,"peak_rss_bytes":peak,
            "roundtrip":"all_original_bytes_sha256_match","dictionary_bytes":0,"query_copy_bytes":0,
            "columnar_note":"Parquet raw binary line column, no numeric parsing; byte-exact envelope" if codec=="parquet-zstd" else None}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--corpus",type=Path,required=True)
    p.add_argument("--output",type=Path)
    p.add_argument("--live-root",type=Path)
    p.add_argument("--worker",nargs=2)
    a = p.parse_args()
    if a.worker:
        print(json.dumps(worker(a.corpus,a.worker[0],int(a.worker[1]))))
        return
    corpus = prepare(a.corpus,a.live_root)
    results = []
    for codec,level in CONFIGURATIONS:
        process = subprocess.run([sys.executable,"-m","tick_futures.benchmark","--corpus",str(a.corpus),
                                  "--worker",codec,str(level)],capture_output=True,text=True,check=True)
        result = json.loads(process.stdout)
        results.append(result)
        report = {"corpus":corpus,"results":results,"concurrent_collector":"running; times are observed shared-host measurements",
                  "decision":"PENDING" if len(results)<len(CONFIGURATIONS) else "zstd level 9: bounded memory, fast replay, independent checksum frames; not a maximal-ratio claim"}
        atomic_json(a.output,report)
        print(json.dumps(result),flush=True)


if __name__ == "__main__":
    main()
