"""Immutable independent standard-codec shards and byte-exact recovery."""

from contextlib import ExitStack
import gzip
import hashlib
import importlib.metadata
import json
import lzma
import os
from pathlib import Path, PurePosixPath
import shutil
import sqlite3
import subprocess
import tempfile
import time
import uuid

import zstandard as zstd

from .codec import sha256
from .runtime import InstanceLock, atomic_json
from .store import DDL

FORMAT = "tfarchive-1"
TARGET_SHARD_BYTES = 256 * 1024 * 1024
SAFE_ASSET_BYTES = 1024 * 1024 * 1024  # deliberately below GitHub's <2 GiB
SAFE_ASSETS_PER_RELEASE = 900  # including manifest + completion; hard limit 1000


def file_hash(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def encode(raw, codec, level):
    if codec == "zstd":
        return zstd.ZstdCompressor(level=level, write_checksum=True).compress(raw)
    if codec == "gzip":
        return gzip.compress(raw, compresslevel=level, mtime=0)
    if codec == "xz":
        return lzma.compress(raw, preset=level)
    if codec == "parquet-zstd":
        import pyarrow as pa
        import pyarrow.parquet as pq
        sink = pa.BufferOutputStream()
        # A byte-conserving columnar envelope. No CSV numeric parsing/rounding.
        table = pa.table({"raw_line": pa.array(raw.splitlines(keepends=True), type=pa.binary())})
        pq.write_table(table, sink, compression="zstd", compression_level=level, use_dictionary=True)
        return sink.getvalue().to_pybytes()
    raise ValueError(f"Unsupported codec: {codec}")


def decode(raw, codec, *, expected_size=None):
    if codec == "zstd":
        return zstd.ZstdDecompressor().decompress(raw, max_output_size=expected_size or TARGET_SHARD_BYTES)
    if codec == "gzip":
        return gzip.decompress(raw)
    if codec == "xz":
        return lzma.decompress(raw)
    if codec == "parquet-zstd":
        import pyarrow as pa
        import pyarrow.parquet as pq
        table = pq.read_table(pa.BufferReader(raw), columns=["raw_line"])
        return b"".join(table["raw_line"].to_pylist())
    raise ValueError(f"Unsupported codec: {codec}")


def safe_relative(value):
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or not path.parts or str(path) != value or "\\" in value:
        raise ValueError(f"Unsafe archive path: {value}")
    return path


def create(files, destination, *, codec="zstd", level=9, shard_bytes=TARGET_SHARD_BYTES,
           scope=None, data_kind="market", parent=None):
    destination = Path(destination)
    if destination.exists():
        raise FileExistsError("Immutable archive destination already exists")
    if not 1 <= shard_bytes <= TARGET_SHARD_BYTES:
        raise ValueError("Shard size must be 1..256 MiB of uncompressed source bytes")
    destination.mkdir(parents=True, mode=0o700)
    manifest = {"format": FORMAT, "archive_id": uuid.uuid4().hex, "created_ns": time.time_ns(),
                "data_kind": data_kind, "source": "TqSdk" if data_kind == "market" else "synthetic_fixture",
                "scope": scope or {}, "collection_integrity": (scope or {}).get("collection_integrity", "evidence_insufficient"),
                "archive_integrity": "local_roundtrip_verified", "record_format": "tfrec-1",
                "dictionary": None, "parent": parent, "codec": codec, "level": level,
                "dependencies": {k: importlib.metadata.version(k) for k in ("numpy", "zstandard")},
                "files": [], "part_order": []}
    from .common import BASE
    manifest["record_schema"] = json.loads((BASE / "schema/tfrec-1.json").read_text())
    manifest["dependencies"]["pandas"] = importlib.metadata.version("pandas")
    runtime=BASE/"decoder/runtime-lock.json"
    if runtime.exists():
        lock=json.loads(runtime.read_text())
        manifest["decoder_runtime"]={"tag":lock["tag"],"manifest_sha256":file_hash(runtime),
                                      "core_wheel_bytes":lock["core_wheel_bytes"],"platform":"macOS arm64 CPython 3.13"}
    try:
        manifest["code_commit"] = subprocess.check_output(["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL, text=True).strip()
    except subprocess.CalledProcessError:
        manifest["code_commit"] = "uncommitted"
    began = time.monotonic()
    for relative, source in files.items():
        safe_relative(relative)
        entry = {"path": relative, "raw_bytes": 0, "raw_sha256": None, "parts": []}
        digest = hashlib.sha256()
        with Path(source).open("rb") as stream:
            ordinal = 0
            while True:
                raw = stream.read(shard_bytes)
                if not raw and ordinal:
                    break
                stored = encode(raw, codec, level)
                raw_hash, stored_hash = sha256(raw), sha256(stored)
                if decode(stored, codec, expected_size=len(raw)) != raw:
                    raise ValueError("Strict archive round-trip failed")
                if len(stored) >= SAFE_ASSET_BYTES:
                    raise ValueError("Compressed shard exceeds safe asset threshold")
                name = f"part-{len(manifest['part_order']):06d}-{stored_hash[:24]}.{codec}"
                from .runtime import atomic_bytes
                atomic_bytes(destination / name, stored)
                entry["parts"].append({"ordinal": ordinal, "name": name, "raw_bytes": len(raw),
                    "stored_bytes": len(stored), "raw_sha256": raw_hash, "stored_sha256": stored_hash,
                    "codec": codec, "level": level, "dictionary": None})
                manifest["part_order"].append(name)
                entry["raw_bytes"] += len(raw)
                digest.update(raw)
                ordinal += 1
                if not raw:
                    break
        entry["raw_sha256"] = digest.hexdigest()
        manifest["files"].append(entry)
    manifest["raw_bytes"] = sum(f["raw_bytes"] for f in manifest["files"])
    manifest["stored_part_bytes"] = sum(p["stored_bytes"] for f in manifest["files"] for p in f["parts"])
    manifest["elapsed_seconds"] = time.monotonic() - began
    atomic_json(destination / "manifest.json", manifest)
    return manifest


def validate_manifest(manifest):
    if manifest.get("format") != FORMAT or manifest.get("record_format") != "tfrec-1":
        raise ValueError("Incompatible archive/schema version")
    if manifest.get("dictionary") is not None:
        raise ValueError("Unexpected/missing dictionary; this decoder uses independent dictionary-free shards")
    if manifest.get("record_schema",{}).get("format")!="tfrec-1":
        raise ValueError("Incompatible embedded record schema")
    paths, names = [], []
    for entry in manifest["files"]:
        safe_relative(entry["path"])
        paths.append(entry["path"])
        for index, part in enumerate(entry["parts"]):
            safe_relative(part["name"])
            if part["ordinal"] != index:
                raise ValueError(f"Wrong shard order: {entry['path']}")
            if part.get("dictionary") is not None:
                raise ValueError("Wrong or unsupported dictionary")
            if not 0 <= part["raw_bytes"] <= TARGET_SHARD_BYTES or part["stored_bytes"] >= SAFE_ASSET_BYTES:
                raise ValueError("Shard exceeds decoder safety limits")
            names.append(part["name"])
        if not entry["parts"]:
            raise ValueError("File has no shard")
    if len(paths) != len(set(paths)) or len(names) != len(set(names)):
        raise ValueError("Duplicate file/shard in manifest")
    if manifest["part_order"] != names:
        raise ValueError("Missing shard or wrong manifest shard order")
    return names


def parent_chain(manifests,leaf):
    chain=[]
    seen=set()
    current=leaf
    while current is not None:
        if current in seen:
            raise ValueError("Archive parent cycle")
        if current not in manifests:
            raise ValueError("Missing parent archive: "+current)
        seen.add(current)
        manifest=manifests[current]
        validate_manifest(manifest)
        safe_relative(current)
        chain.append(manifest)
        current=manifest.get("parent")
    chain.reverse()
    for previous,current in zip(chain,chain[1:]):
        before,after=previous.get("scope",{}),current.get("scope",{})
        if not isinstance(before.get("last_seq"),int) or not isinstance(after.get("first_seq"),int) or after["first_seq"]!=before["last_seq"]+1:
            raise ValueError("Missing or overlapping arrival-sequence range in parent chain")
    return chain


def restore_dataset(directory,output,*,leaf=None):
    directory,output=Path(directory),Path(output)
    paths=list(directory.glob("*/manifest.json"))
    if (directory/"manifest.json").exists():
        paths.append(directory/"manifest.json")
    manifests={}
    locations={}
    for path in paths:
        manifest=json.loads(path.read_text())
        key=manifest["archive_id"]
        if key in manifests and manifests[key]!=manifest:
            raise ValueError("Conflicting immutable archive ID")
        manifests[key]=manifest
        locations[key]=path.parent
    if leaf is None:
        leaves=set(manifests)-{m["parent"] for m in manifests.values() if m.get("parent")}
        if len(leaves)!=1:
            raise ValueError("Select a leaf archive; no unique complete dataset chain")
        leaf=leaves.pop()
    chain=parent_chain(manifests,leaf)
    if output.is_symlink() or (output.exists() and any(output.iterdir())):
        raise FileExistsError("Dataset recovery requires an empty real destination")
    output=output.resolve()
    output.mkdir(parents=True,exist_ok=True)
    reports=[]
    for manifest in chain:
        key=manifest["archive_id"]
        reports.append(restore(locations[key],output/"archives"/key))
    report={"status":"all_parent_archives_and_original_bytes_verified","archive_order":[m["archive_id"] for m in chain],
            "rows":sum(m.get("scope",{}).get("rows",0) for m in chain),"archives":reports,
            "collection_integrity":[m["collection_integrity"] for m in chain],"remote_restore":False}
    atomic_json(output/"dataset-restore-report.json",report)
    return report


def restore(archive, output):
    archive, output = Path(archive), Path(output)
    if output.is_symlink():
        raise ValueError("Symlink recovery destination")
    # macOS /var and /tmp are system symlinks; normalize the trusted root first.
    archive, output = archive.resolve(), output.resolve()
    manifest = json.loads((archive / "manifest.json").read_text())
    names = validate_manifest(manifest)
    if output.exists() and any(output.iterdir()):
        raise FileExistsError("Recovery requires an empty destination")
    output.mkdir(parents=True, exist_ok=True)
    # No production-directory or cache fallback exists.
    for name in names:
        if not (archive / name).is_file() or (archive / name).is_symlink():
            raise ValueError(f"Missing/unsafe shard: {name}")
    recovered = []
    for entry in manifest["files"]:
        target = output / entry["path"]
        if any(p.is_symlink() for p in [output, *target.parents]):
            raise ValueError("Symlink in recovery destination")
        target.parent.mkdir(parents=True, exist_ok=True)
        temp = target.with_name(target.name + ".partial")
        digest = hashlib.sha256()
        size = 0
        with temp.open("xb") as stream:
            for part in entry["parts"]:
                path = archive / part["name"]
                if path.stat().st_size != part["stored_bytes"] or file_hash(path) != part["stored_sha256"]:
                    raise ValueError(f"Corrupt compressed shard: {part['name']}")
                raw = decode(path.read_bytes(), part["codec"], expected_size=part["raw_bytes"])
                if len(raw) != part["raw_bytes"] or sha256(raw) != part["raw_sha256"]:
                    raise ValueError(f"Wrong decoded content: {part['name']}")
                stream.write(raw)
                digest.update(raw)
                size += len(raw)
            stream.flush()
            os.fsync(stream.fileno())
        if size != entry["raw_bytes"] or digest.hexdigest() != entry["raw_sha256"]:
            raise ValueError(f"Restored original hash mismatch: {entry['path']}")
        os.replace(temp, target)
        recovered.append({"path": entry["path"], "bytes": size, "sha256": digest.hexdigest()})
    report = {"archive_id": manifest["archive_id"], "status": "all_original_bytes_match",
              "files": recovered, "collection_integrity": manifest["collection_integrity"],
              "remote_restore": False}
    atomic_json(output / "restore-report.json", report)
    return report


def seal(root, destination, *, level=9, shard_bytes=TARGET_SHARD_BYTES,data_kind="market"):
    """Export a consistent incremental SQLite journal; never modify hot records.

    The DB contains canonical typed raw BLOBs, original global arrival seq, receive
    times, revisions, observations and gaps. Its physical bytes are also hashed.
    """
    root, destination = Path(root), Path(destination)
    with InstanceLock(root / "seal.lock"):
        index_path = root / "seal-index.json"
        index = json.loads(index_path.read_text()) if index_path.exists() else {"last_seq": 0, "archives": []}
        original_sources={}
        source_files=dict(index.get("backfill_source_files",{}))
        for completed in (root/"backfill-jobs").glob("*/completed.json"):
            job=json.loads(completed.read_text())
            journal=completed.parent/"hot.sqlite3"
            if journal.is_symlink() or not journal.resolve().is_relative_to((root/"backfill-jobs").resolve()) or Path(job["journal"]).resolve()!=journal.resolve():
                raise ValueError("Backfill source must be the immutable journal inside its job directory")
            name=completed.parent.name
            previous=source_files.get(name)
            if previous and previous!=job["journal_sha256"]:
                raise ValueError("Immutable backfill source identity changed: "+name)
            if not previous:
                if file_hash(journal)!=job["journal_sha256"]:
                    raise ValueError("Backfill source hash mismatch before sealing: "+name)
                original_sources["backfill-jobs/"+name+"/journal.sqlite3"]=journal
                original_sources["backfill-jobs/"+name+"/completed.json"]=completed
                source_files[name]=job["journal_sha256"]
        source = sqlite3.connect(f"file:{root / 'hot.sqlite3'}?mode=ro", uri=True)
        source.execute("BEGIN")
        high = source.execute("SELECT coalesce(max(seq),0) FROM records").fetchone()[0]
        watermarks={table:source.execute(f"SELECT coalesce(max(seq),0) FROM {table}").fetchone()[0]
                    for table in ("observations","events")}
        latest=root/"discovery/universe-latest.json"
        metadata_changed=latest.exists() and file_hash(latest)!=index.get("metadata_files",{}).get(latest.name)
        audit_changed=any(value!=index.get("watermarks",{}).get(table,0) for table,value in watermarks.items())
        if high <= index["last_seq"] and not audit_changed and not metadata_changed and not original_sources:
            source.close()
            return {"status": "no_new_records", "last_seq": high}
        staging = Path(tempfile.mkdtemp(prefix="seal-", dir=root))
        snapshot = staging / "journal.sqlite3"
        target = sqlite3.connect(snapshot)
        target.executescript(DDL)
        target.execute("CREATE TABLE archive_context(key TEXT PRIMARY KEY,value TEXT)")
        target.execute("INSERT INTO archive_context VALUES('parent_last_seq',?)",(str(index["last_seq"]),))
        # Batched copies are outside the receive process. No pandas/float conversions.
        cursor = source.execute("SELECT * FROM records WHERE seq>? AND seq<=? ORDER BY seq", (index["last_seq"], high))
        while rows := cursor.fetchmany(2000):
            for row in rows:
                if sha256(row[8])!=row[7]:
                    target.close()
                    source.close()
                    raise ValueError(f"Hot journal raw record hash mismatch at seq {row[0]}; archive not completed")
            target.executemany("INSERT INTO records VALUES(?,?,?,?,?,?,?,?,?)", rows)
            target.commit()
        for table in ("checkpoints", "observations", "events", "gaps", "runs", "contracts"):
            if table in ("observations","events"):
                cursor=source.execute(f"SELECT * FROM {table} WHERE seq>?",(index.get("watermarks",{}).get(table,0),))
            else:
                # Gap statuses can change with an audit event. Preserve their
                # complete current snapshot, just like contracts/checkpoints.
                cursor = source.execute(f"SELECT * FROM {table}")
            columns = len(cursor.description)
            while rows := cursor.fetchmany(2000):
                target.executemany(f"INSERT INTO {table} VALUES({','.join('?' for _ in range(columns))})", rows)
        counts = target.execute("SELECT symbol,count(*),min(event_ns),max(event_ns) FROM records GROUP BY symbol").fetchall()
        gap_counts = dict(source.execute("SELECT status,count(*) FROM gaps GROUP BY status").fetchall())
        target.commit()
        target.close()
        source.close()
        files = {"journal/journal.sqlite3": snapshot,**original_sources}
        metadata_files=dict(index.get("metadata_files",{}))
        for path in (root / "discovery").glob("*"):
            if path.is_file():
                digest=file_hash(path)
                if metadata_files.get(path.name)!=digest:
                    files["discovery/" + path.name] = path
                    metadata_files[path.name]=digest
        scope = {"first_seq": index["last_seq"] + 1, "last_seq": high,
                 "rows": sum(c[1] for c in counts), "contracts": [{"symbol": s, "rows": n, "first_ns": a, "last_ns": b} for s,n,a,b in counts],
                 "gaps": gap_counts, "collection_integrity": "evidence_insufficient",
                 "original_source_journals_added":len(original_sources)//2,
                 "source_semantics": "TqSdk snapshots; no exchange upstream completeness claim"}
        result = create(files, destination, level=level, shard_bytes=shard_bytes, scope=scope,data_kind=data_kind,
                        parent=index["archives"][-1]["archive_id"] if index["archives"] else None)
        # Keep staging as a local independent raw copy. Cleanup is never automatic.
        index["last_seq"] = high
        index["watermarks"]=watermarks
        index["metadata_files"]=metadata_files
        index["backfill_source_files"]=source_files
        index["archives"].append({"archive_id": result["archive_id"], "path": str(destination.resolve()),
                                  "first_seq": scope["first_seq"], "last_seq": high, "raw_staging": str(staging)})
        atomic_json(index_path, index)
        return result
