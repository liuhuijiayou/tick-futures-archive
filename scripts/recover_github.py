#!/usr/bin/env python3
"""Bootstrap recovery from GitHub only, into a fresh offline-installed venv.

Uses Python stdlib and the user's normal gh authentication. Reads no collector
root, source CSV, checkpoint, production venv site-packages, or pip cache.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile


def gh_json(path):
    return json.loads(subprocess.check_output(["gh","api",path]))


def download(repo, asset, destination):
    with Path(destination).open("wb") as stream:
        subprocess.run(["gh","api",f"repos/{repo}/releases/assets/{asset['id']}",
                        "-H","Accept: application/octet-stream"],stdout=stream,check=True)


def digest(path):
    d = hashlib.sha256()
    with Path(path).open("rb") as stream:
        while chunk := stream.read(1024*1024):
            d.update(chunk)
    return d.hexdigest()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("repo")
    p.add_argument("tag")
    p.add_argument("output",type=Path)
    p.add_argument("--archive-id")
    p.add_argument("--with-parquet",action="store_true")
    a = p.parse_args()
    code = Path(__file__).resolve().parents[1]
    lock_name="runtime-linux-x86_64-py313.json" if platform.system()=="Linux" and platform.machine()=="x86_64" else "runtime-lock.json"
    lock_path = code/"decoder"/lock_name
    lock = json.loads(lock_path.read_text())
    if platform.system()!=lock["system"] or platform.machine()!=lock["machine"] or list(sys.version_info[:2])!=lock["python"]:
        p.error("Use a matching bundled runtime: macOS arm64 or Linux x86_64, CPython 3.13")
    owner = gh_json("user")["login"]
    repo = gh_json("repos/"+a.repo)
    if not repo["private"] or repo["owner"]["login"].lower()!=owner.lower():
        p.error("Recovery target must be the authenticated owner's private repository")
    release = gh_json(f"repos/{a.repo}/releases/tags/{lock['tag']}")
    pages = json.loads(subprocess.check_output(["gh","api","--paginate","--slurp",f"repos/{a.repo}/releases/{release['id']}/assets?per_page=100"]))
    assets = {item["name"]:item for page in pages for item in page}
    selected = [w for w in lock["wheels"] if w["role"]=="core" or a.with_parquet]
    with tempfile.TemporaryDirectory(prefix="tick-github-isolated-") as folder:
        folder = Path(folder)
        download(a.repo,assets["runtime-lock.json"],folder/"runtime-lock.json")
        if (folder/"runtime-lock.json").read_bytes()!=lock_path.read_bytes():
            raise ValueError("Remote runtime manifest differs from GitHub code manifest")
        wheels = folder/"wheels"
        wheels.mkdir()
        for wheel in selected:
            if wheel["name"] not in assets:
                raise ValueError("Missing GitHub decoder dependency: "+wheel["name"])
            target = wheels/wheel["name"]
            download(a.repo,assets[wheel["name"]],target)
            if target.stat().st_size!=wheel["bytes"] or digest(target)!=wheel["sha256"]:
                raise ValueError("Corrupt GitHub decoder dependency: "+wheel["name"])
        environment = folder/"environment"
        env = dict(os.environ)
        for key in ("PYTHONPATH","PYTHONHOME","TQ_AUTH_CONFIG","TQ_USER","TQ_PASSWORD"):
            env.pop(key,None)
        env["PYTHONNOUSERSITE"] = "1"
        env["PIP_NO_INDEX"] = "1"
        env["PIP_NO_CACHE_DIR"] = "1"
        subprocess.run([sys.executable,"-m","venv",str(environment)],env=env,check=True)
        python = environment/"bin/python"
        subprocess.run([str(python),"-m","pip","install","--no-index","--no-deps","--no-cache-dir",
                        *[str(wheels/w["name"]) for w in selected]],env=env,check=True)
        command = [str(python),"-m","tick_futures","restore-github",a.repo,a.tag,str(a.output.resolve())]
        if a.archive_id:
            command.extend(["--archive-id",a.archive_id])
        subprocess.run(command,env=env,cwd=code,check=True)
        journals=list(a.output.resolve().glob("archives/*/journal/journal.sqlite3"))
        if (a.output.resolve()/"journal/journal.sqlite3").exists():
            journals.append(a.output.resolve()/"journal/journal.sqlite3")
        results=[json.loads(subprocess.check_output([str(python),"-m","tick_futures","verify-db",str(journal)],env=env,cwd=code)) for journal in journals]
        source_journals=list(a.output.resolve().glob("archives/*/backfill-jobs/*/journal.sqlite3"))
        source_journals.extend(a.output.resolve().glob("backfill-jobs/*/journal.sqlite3"))
        source_results=[json.loads(subprocess.check_output([str(python),"-m","tick_futures","verify-db",str(journal)],env=env,cwd=code)) for journal in source_journals]
        result={"status":"all_journals_verified","journals":len(results),"rows":sum(r["rows"] for r in results)} if results else {"status":"no_journal_in_archive"}
        result.update({"original_source_journals":len(source_results),"preserved_source_rows":sum(r["rows"] for r in source_results),
                       "note":"Source rows are preserved delivery evidence, not additional unique market Ticks"})
        second_files=list(a.output.resolve().rglob("seconds/snapshots.tfrec"))
        second_results=[json.loads(subprocess.check_output([str(python),"-m","tick_futures","verify-seconds-file",str(path)],env=env,cwd=code)) for path in second_files]
        observation_files=list(a.output.resolve().rglob("seconds/observations.tfrec"))
        for path in observation_files:
            subprocess.run([str(python),"-m","tick_futures","verify-seconds-file",str(path)],env=env,cwd=code,check=True,stdout=subprocess.DEVNULL)
        result.update({"second_snapshot_files":len(second_results),"second_snapshot_export_rows":sum(r['rows'] for r in second_results),
                       "second_observation_files_verified":len(observation_files)})
        report = {"status":"github_only_isolated_recovery_passed","runtime_tag":lock["tag"],
                  "wheel_bytes":sum(w["bytes"] for w in selected),"wheels_downloaded":len(selected),
                  "pip_network_or_cache_used":False,"production_directory_or_environment_used":False,
                  "journal_verification":result}
        (a.output.resolve()/"isolated-runtime-report.json").write_text(json.dumps(report,indent=2)+"\n")
        print(json.dumps(report,indent=2))


if __name__=="__main__":
    main()
