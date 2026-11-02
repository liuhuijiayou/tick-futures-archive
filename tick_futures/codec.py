"""TFREC v1: ordered typed records; float/NumPy values retain their exact bits.

This is a serialization envelope, not a new compression algorithm. Unsupported
types fail closed. In particular, no pandas row coercion or JSON float occurs.
"""

import base64
import hashlib
import json
import struct

import numpy as np

VERSION = "tfrec-1"


def pack(value):
    if isinstance(value, np.generic):
        if value.dtype.hasobject or value.dtype.fields:
            raise TypeError("Object numpy scalars cannot be serialized")
        return ["numpy", value.dtype.str, value.tobytes().hex()]
    if value is None:
        return ["null"]
    if isinstance(value, bool):
        return ["bool", value]
    if isinstance(value, int):
        return ["int", str(value)]
    if isinstance(value, float):
        return ["float64", struct.pack(">d", value).hex()]
    if isinstance(value, str):
        return ["str", value]
    if isinstance(value, bytes):
        return ["bytes", base64.b64encode(value).decode("ascii")]
    if isinstance(value, dict):
        return ["map", [[pack(k), pack(v)] for k, v in value.items()]]
    if isinstance(value, (list, tuple)):
        return ["tuple" if isinstance(value, tuple) else "list", [pack(v) for v in value]]
    # Missing extension scalars must stay distinct from None and IEEE NaN.
    import pandas as pd
    if value is pd.NA:
        return ["pd.NA"]
    if value is pd.NaT:
        return ["pd.NaT"]
    raise TypeError(f"Unsupported raw type: {type(value).__name__}")


def unpack(value):
    tag, *args = value
    if tag == "numpy":
        dtype = np.dtype(args[0])
        if dtype.hasobject or len(bytes.fromhex(args[1])) != dtype.itemsize:
            raise ValueError("Invalid numpy scalar")
        return np.frombuffer(bytes.fromhex(args[1]), dtype=dtype)[0]
    if tag == "null":
        return None
    if tag == "bool":
        return args[0]
    if tag == "int":
        return int(args[0])
    if tag == "float64":
        return struct.unpack(">d", bytes.fromhex(args[0]))[0]
    if tag == "str":
        return args[0]
    if tag == "bytes":
        return base64.b64decode(args[0], validate=True)
    if tag == "map":
        return {unpack(k): unpack(v) for k, v in args[0]}
    if tag in ("tuple", "list"):
        result = [unpack(v) for v in args[0]]
        return tuple(result) if tag == "tuple" else result
    if tag in ("pd.NA", "pd.NaT"):
        import pandas as pd
        return pd.NA if tag == "pd.NA" else pd.NaT
    raise ValueError(f"Unknown scalar tag: {tag}")


def dumps(value):
    return json.dumps({"format": VERSION, "value": pack(value)}, ensure_ascii=True,
                      separators=(",", ":"), allow_nan=False).encode("ascii") + b"\n"


def loads(raw):
    obj = json.loads(raw)
    if obj.get("format") != VERSION:
        raise ValueError("Incompatible record version")
    return unpack(obj["value"])


def sha256(raw):
    return hashlib.sha256(raw).hexdigest()


def frame_rows(frame):
    """Copy by column before crossing threads; iterrows would coerce int64."""
    columns = [(str(k), frame[k].to_numpy(copy=True)) for k in frame.columns]
    result = []
    for index in range(len(frame)):
        row = {key: loads(dumps(values[index])) if isinstance(values[index],(dict,list,tuple)) else values[index]
               for key, values in columns}
        if row.get("datetime", 0) > 0 and row.get("id", -1) >= 0:
            result.append(row)
    return result
