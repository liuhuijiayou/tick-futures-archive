"""Project paths and explicit, read-only authentication configuration."""

import ast
from datetime import datetime
import json
import math
import os
from pathlib import Path
import re
from zoneinfo import ZoneInfo

BASE = Path(__file__).resolve().parents[1]
CST = ZoneInfo("Asia/Shanghai")


def load_auth():
    """Use environment credentials or an explicitly supplied AST-only config."""
    user, password = os.getenv("TQ_USER"), os.getenv("TQ_PASSWORD")
    if user and password:
        return user, password, "environment"
    if user or password:
        raise ValueError("TQ_USER and TQ_PASSWORD must both be set")
    config = os.getenv("TQ_AUTH_CONFIG")
    if not config:
        raise ValueError("Set TQ_USER/TQ_PASSWORD, or explicitly set TQ_AUTH_CONFIG")
    path = Path(config).expanduser().resolve()
    tree = ast.parse(path.read_text(encoding="utf-8"))
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
             and isinstance(node.func, ast.Name) and node.func.id == "TqAuth"]
    if len(calls) != 1:
        raise ValueError("Expected one literal TqAuth configuration")
    values = []
    for index, name in enumerate(("user_name", "password")):
        node = calls[0].args[index] if len(calls[0].args) > index else next(
            item.value for item in calls[0].keywords if item.arg == name)
        value = ast.literal_eval(node)
        if not isinstance(value, str) or not value:
            raise ValueError("Authentication values must be nonempty string literals")
        values.append(value)
    return *values, f"{path.name}:TqAuth (AST only)"


class RedactedStream:
    def __init__(self, stream, secrets):
        self.stream, self.secrets = stream, secrets

    def write(self, value):
        for secret in self.secrets:
            value = value.replace(secret, "[REDACTED]")
        value = re.sub(r"eyJ[\w-]+\.[\w-]+\.[\w-]+", "[REDACTED_TOKEN]", value)
        return self.stream.write(value)

    def flush(self):
        self.stream.flush()

    def __getattr__(self, name):
        return getattr(self.stream, name)


def timestamp():
    return datetime.now(CST).isoformat(timespec="milliseconds")


def clean(value):
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {key: clean(item) for key, item in value.items()}
    if isinstance(value, list):
        return [clean(item) for item in value]
    return value


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(clean(value), ensure_ascii=False, indent=2,
                               allow_nan=False) + "\n", encoding="utf-8")
    path.chmod(0o600)
