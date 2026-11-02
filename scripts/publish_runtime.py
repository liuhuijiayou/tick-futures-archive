#!/usr/bin/env python3
"""Publish verified public decoder wheels to the owner's private project repo."""

import argparse
import json
from pathlib import Path
import subprocess
import tempfile

from tick_futures.archive import file_hash
from tick_futures.github import api, asset_list, command, create_release, download, release_info, verify_owner_private
from tick_futures.runtime import atomic_json


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("repo")
    p.add_argument("wheels",type=Path)
    p.add_argument("--report",type=Path,required=True)
    a=p.parse_args()
    lock_path=Path("decoder/runtime-lock.json")
    lock=json.loads(lock_path.read_text())
    verify_owner_private(a.repo)
    try:
        release=release_info(a.repo,lock["tag"])
    except subprocess.CalledProcessError as exc:
        if b"404" not in (exc.stderr or b""):
            raise
        release=create_release(a.repo,lock["tag"],"Offline decoder runtime",
                 "Public-package wheels and hashes for isolated recovery; no market data or credentials.")
    assets={x["name"]:x for x in asset_list(a.repo,release)}
    verified=[]
    with tempfile.TemporaryDirectory(prefix="decoder-verify-") as folder:
        for wheel in lock["wheels"]:
            name=wheel["name"]
            source=a.wheels/name
            if source.stat().st_size!=wheel["bytes"] or file_hash(source)!=wheel["sha256"]:
                raise ValueError("Local runtime wheel differs from lock: "+name)
            if name not in assets:
                command(["release","upload",lock["tag"],str(source),"--repo",a.repo])
                assets={x["name"]:x for x in asset_list(a.repo,release)}
            target=Path(folder)/name
            download(a.repo,assets[name],target)
            if file_hash(target)!=wheel["sha256"]:
                raise ValueError("Remote runtime hash conflict: "+name)
            verified.append({"name":name,"id":assets[name]["id"],"bytes":wheel["bytes"],"sha256":wheel["sha256"]})
        if "runtime-lock.json" not in assets:
            command(["release","upload",lock["tag"],str(lock_path),"--repo",a.repo])
            assets={x["name"]:x for x in asset_list(a.repo,release)}
        target=Path(folder)/"runtime-lock.json"
        download(a.repo,assets["runtime-lock.json"],target)
        if target.read_bytes()!=lock_path.read_bytes():
            raise ValueError("Remote runtime manifest conflict")
    if release["draft"]:
        command(["release","edit",lock["tag"],"--repo",a.repo,"--draft=false"])
    report={"status":"all_runtime_assets_redownloaded_sha256_verified","release":f"https://github.com/{a.repo}/releases/tag/{lock['tag']}",
            "assets":verified,"total_asset_bytes":sum(w["bytes"] for w in verified)+lock_path.stat().st_size,
            "market_data_assets":0}
    atomic_json(a.report,report)
    print(json.dumps({k:v for k,v in report.items() if k!="assets"},indent=2))


if __name__=="__main__":
    main()
