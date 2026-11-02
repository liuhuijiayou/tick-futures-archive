"""Unified collection, audit, archive, recovery and scheduler entry point."""

import argparse
import json
from pathlib import Path
import sys


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", type=Path, default=Path("data/derived/market"))
    sub = p.add_subparsers(dest="command", required=True)
    sub.add_parser("discover")
    for name in ("collect", "backfill"):
        parser = sub.add_parser(name)
        parser.add_argument("--symbols", nargs="+")
        parser.add_argument("--data-length", type=int, default=10000)
        parser.add_argument("--seconds", type=float, default=30 if name == "backfill" else None)
        parser.add_argument("--max-contracts", type=int)
        parser.add_argument("--min-free-gib", type=float, default=5)
        parser.add_argument("--prepare-workers",type=int,default=2,choices=range(5))
        parser.add_argument("--init-concurrency",type=int,default=8,choices=range(1,17))
        parser.add_argument("--initial-only",action="store_true",help="Stop after the complete initial window is committed")
    sub.add_parser("status")
    parser = sub.add_parser("overview")
    parser.add_argument("--output",type=Path)
    parser = sub.add_parser("audit")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--scan-values", action="store_true")
    parser = sub.add_parser("verify-db")
    parser.add_argument("path", nargs="?", type=Path)
    parser = sub.add_parser("seal")
    parser.add_argument("destination", type=Path)
    parser.add_argument("--level", type=int, default=9)
    parser = sub.add_parser("verify-archive")
    parser.add_argument("archive", type=Path)
    parser = sub.add_parser("restore")
    parser.add_argument("archive", type=Path)
    parser.add_argument("output", type=Path)
    parser = sub.add_parser("restore-dataset")
    parser.add_argument("directory",type=Path)
    parser.add_argument("output",type=Path)
    parser.add_argument("--leaf")
    parser = sub.add_parser("upload")
    parser.add_argument("archive", type=Path)
    parser.add_argument("repo")
    parser.add_argument("tag")
    parser.add_argument("--authorization", type=Path)
    parser.add_argument("--fault-after-parts", type=int, help="Acceptance fault injection; leaves an incomplete batch")
    parser = sub.add_parser("restore-github")
    parser.add_argument("repo")
    parser.add_argument("tag")
    parser.add_argument("output", type=Path)
    parser.add_argument("--archive-id")
    parser = sub.add_parser("schedule")
    parser.add_argument("action", choices=["install", "enable", "status", "disable", "uninstall"])
    parser.add_argument("--auth-config", type=Path)
    parser = sub.add_parser("service")
    parser.add_argument("role", choices=["collector", "maintenance"])
    args = p.parse_args(argv)
    try:
        if args.command == "discover":
            from .source import connect, discover
            api = connect()
            try:
                result = discover(api, args.root / "discovery")
            finally:
                api.close()
        elif args.command in ("collect", "backfill"):
            if not 1 <= args.data_length <= 10000 or (args.seconds is not None and args.seconds <= 0):
                p.error("data-length must be 1..10000 and seconds must be positive")
            from .collector import collect
            from .runtime import InstanceLock,atomic_json
            active = False
            if args.command == "backfill":
                try:
                    with InstanceLock(args.root/"collector.lock"):
                        pass
                except RuntimeError:
                    active = True
            if active:
                import time,uuid
                request = {"status":"pending","requested_ns":time.time_ns(),"symbols":args.symbols,"rows":10000}
                path = args.root/"requests"/("backfill-"+uuid.uuid4().hex+".json")
                atomic_json(path,request)
                result = {**request,"request_file":str(path)}
            else:
                result = collect(args.root, symbols=args.symbols, data_length=args.data_length,
                                 seconds=args.seconds, max_contracts=args.max_contracts,
                                 min_free_bytes=int(args.min_free_gib * 1024 ** 3),initial_only=args.initial_only,
                                 preparation_workers=args.prepare_workers,initial_concurrency=args.init_concurrency)
        elif args.command == "status":
            import time
            path = args.root / "heartbeat.json"
            result = json.loads(path.read_text()) if path.exists() else {"status": "not_started"}
            if "heartbeat_ns" in result:
                result["heartbeat_age_seconds"] = (time.time_ns() - result["heartbeat_ns"]) / 1e9
        elif args.command == "audit":
            from .audit import coverage
            report = coverage(args.root, args.output, scan_values=args.scan_values)
            result = {k: v for k,v in report.items() if k not in ("contracts", "ledger", "gaps")}
        elif args.command == "overview":
            from .overview import overview
            report=overview(args.root,args.output)
            result={k:v for k,v in report.items() if k!="contracts"}
        elif args.command == "verify-db":
            from .audit import verify_db
            result = verify_db(args.path or args.root / "hot.sqlite3")
        elif args.command == "seal":
            from .archive import seal
            result = seal(args.root, args.destination, level=args.level)
        elif args.command == "restore":
            from .archive import restore
            result = restore(args.archive, args.output)
        elif args.command == "restore-dataset":
            from .archive import restore_dataset
            result=restore_dataset(args.directory,args.output,leaf=args.leaf)
        elif args.command == "verify-archive":
            import tempfile
            from .archive import restore
            with tempfile.TemporaryDirectory(prefix="tick-verify-") as folder:
                result = restore(args.archive, folder)
        elif args.command == "upload":
            from .github import upload
            result = upload(args.archive, args.repo, args.tag, authorization=args.authorization,fault_after_parts=args.fault_after_parts)
        elif args.command == "restore-github":
            from .github import restore_github
            result = restore_github(args.repo, args.tag, args.output, archive_id=args.archive_id)
        elif args.command == "schedule":
            from .schedule import manage
            result = manage(args.action, args.root, auth_config=args.auth_config)
        elif args.command == "service":
            from .schedule import service
            result = service(args.role, args.root)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        print(json.dumps({"status": "failed", "error_type": type(exc).__name__, "message": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
