"""Atomic local state, advisory instance locks, and bounded operational alerts."""

from contextlib import AbstractContextManager
import fcntl
import json
import os
from pathlib import Path
import tempfile
import time
import re
import sys


def atomic_bytes(path, raw):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, name = tempfile.mkstemp(prefix="." + path.name + ".", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def atomic_json(path, obj):
    atomic_bytes(path, (json.dumps(redact_errors(obj), ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode())


def redact_errors(value):
    """Sanitize diagnostic strings; never touch raw data, IDs or content hashes."""
    secrets={os.environ.get(k) for k in ("TQ_USER","TQ_PASSWORD")}
    for stream in (sys.stdout,sys.stderr):
        secrets.update(getattr(stream,"secrets",()))
    secrets.discard(None)
    secrets.discard("")
    def scrub(text):
        for secret in sorted(secrets,key=len,reverse=True):
            text=text.replace(secret,"[REDACTED]")
        return re.sub(r"eyJ[\w-]+\.[\w-]+\.[\w-]+","[REDACTED_TOKEN]",text)
    if isinstance(value,dict):
        return {k:scrub(v) if k in ("error","message","close_error","last_error") and isinstance(v,str) else redact_errors(v) for k,v in value.items()}
    if isinstance(value,list):
        return [redact_errors(v) for v in value]
    if isinstance(value,tuple):
        return tuple(redact_errors(v) for v in value)
    return value


class InstanceLock(AbstractContextManager):
    def __init__(self, path):
        self.path = Path(path)

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.stream = self.path.open("a+")
        try:
            fcntl.flock(self.stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.stream.close()
            raise RuntimeError(f"Instance already running: {self.path.name}") from None
        self.stream.seek(0)
        self.stream.truncate()
        self.stream.write(str(os.getpid()))
        self.stream.flush()
        return self

    def __exit__(self, *args):
        fcntl.flock(self.stream, fcntl.LOCK_UN)
        self.stream.close()


def alert(root, kind, detail):
    obj = redact_errors({"received_ns": time.time_ns(), "kind": kind, "detail": detail})
    # launchd captures stderr. No emails/webhooks or other messages are sent.
    print(json.dumps(obj, ensure_ascii=False), file=sys.stderr, flush=True)
    try:
        atomic_json(Path(root) / "last_alert.json", obj)
    except OSError as exc:
        # A full/read-only disk must not suppress the stderr failure signal or
        # replace the original collector/maintenance exception.
        print(json.dumps({"kind":"alert_file_write_failed","error_type":type(exc).__name__}),file=sys.stderr,flush=True)
