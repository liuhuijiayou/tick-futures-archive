"""Verify migrated samples without authentication or network access."""

import csv
import gzip
import hashlib
import io
import json
import argparse
from pathlib import Path

from .common import BASE


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root",type=Path,default=BASE,help="Explicit independently restored sample root")
    args=parser.parse_args(argv)
    manifest = json.loads((args.root / "evidence/manifest.json").read_text())
    tick_rows = 0
    for entry in manifest["files"]:
        path = args.root / entry["destination"]
        stored = path.read_bytes()
        if hashlib.sha256(stored).hexdigest() != entry["sha256_stored"]:
            raise ValueError(f"Stored file hash mismatch: {entry['destination']}")
        raw = gzip.decompress(stored) if entry["encoding"] == "gzip" else stored
        if hashlib.sha256(raw).hexdigest() != entry["sha256_uncompressed"]:
            raise ValueError(f"Original content hash mismatch: {entry['destination']}")
        if path.name == "ticks.csv.gz":
            tick_rows += sum(1 for _ in csv.DictReader(io.StringIO(raw.decode("utf-8"))))
    print(json.dumps({"verified_files": len(manifest["files"]), "tick_rows": tick_rows,
                      "source_bytes": manifest["total_source_bytes"],
                      "stored_bytes": manifest["total_stored_bytes"],
                      "status": "all_original_content_hashes_match"}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
