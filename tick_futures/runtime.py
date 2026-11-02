"""Atomic local state, advisory instance locks, and bounded operational alerts."""

from contextlib import AbstractContextManager
import fcntl
import json
import os
from pathlib import Path
import tempfile
import time


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
    atomic_bytes(path, (json.dumps(obj, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode())


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
    obj = {"received_ns": time.time_ns(), "kind": kind, "detail": detail}
    atomic_json(Path(root) / "last_alert.json", obj)
    # launchd captures stderr. No emails/webhooks or other messages are sent.
    print(json.dumps(obj, ensure_ascii=False), file=__import__("sys").stderr, flush=True)
