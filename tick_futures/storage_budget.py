"""Conservative application data budget; never automatically deletes history."""

import json
import os
from pathlib import Path
import shutil
import time

from .common import BASE


def logical_bytes(paths):
    total = 0
    roots = []
    for value in paths:
        path = Path(value).resolve()
        if any(path == p or path.is_relative_to(p) for p in roots):
            continue
        roots = [p for p in roots if not p.is_relative_to(path)] + [path]
    for root in roots:
        if root.is_file():
            total += root.stat().st_size
            continue
        for directory, dirs, files in os.walk(root, followlinks=False):
            dirs[:] = [d for d in dirs if d != ".git"]
            for name in files:
                try:
                    total += (Path(directory) / name).lstat().st_size
                except FileNotFoundError:
                    pass
    return total


class Budget:
    def __init__(self, root, *, paths=None, maximum=100_000_000_000,
                 pause_at=90_000_000_000, min_free=5 * 1024**3):
        self.root = Path(root)
        self.paths = paths or [BASE / "data", BASE / "evidence", BASE / "reports", self.root]
        self.maximum, self.pause_at, self.min_free = maximum, pause_at, min_free
        self.measured = 0
        self.checked = 0

    def check(self, extra=0, *, force=False):
        if extra < 0:
            raise ValueError("Negative budget reservation")
        if force or time.monotonic() - self.checked > 1:
            self.measured = logical_bytes(self.paths)
            self.checked = time.monotonic()
        free = shutil.disk_usage(self.root).free
        if self.measured + extra >= self.pause_at or free - extra < self.min_free:
            raise OSError(f"Data budget pause: used={self.measured}, requested_growth={extra}, pause_at={self.pause_at}, maximum={self.maximum}; no automatic deletion")
        return {"logical_data_bytes": self.measured, "maximum_bytes": self.maximum,
                "pause_at_bytes": self.pause_at, "safety_reserve_bytes": self.maximum-self.pause_at,
                "disk_free_bytes": free, "automatic_history_deletion": False}
