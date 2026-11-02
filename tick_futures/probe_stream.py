"""Small legacy-probe CSV sink: fsync before advancing the initial cursor."""

import os


class ProbeStream:
    def __init__(self, serial, initial, path, snapshot, timestamp):
        self.serial, self.path, self.snapshot = serial, path, snapshot
        self.timestamp = timestamp
        # This is the snapshot already written, not the serial's later latest ID.
        self.cursor = int(initial["id"].iloc[-1])
        self.rows = 0
        empty = initial.iloc[:0].copy()
        empty["observed_at"] = ""
        with path.open("w") as stream:
            empty.to_csv(stream, index=False)
            stream.flush()
            os.fsync(stream.fileno())

    def drain(self):
        valid = self.serial.loc[self.serial["datetime"] > 0]
        if valid.empty:
            return
        if int(valid["id"].iloc[-1]) < self.cursor:
            raise RuntimeError("SDK ID reset during legacy probe; use durable collector for reconciliation")
        if int(valid["id"].iloc[0]) > self.cursor + 1:
            raise RuntimeError("Legacy probe window exceeded before persistence")
        new = valid.loc[valid["id"] > self.cursor]
        if new.empty:
            return
        data = self.snapshot(new)
        data["observed_at"] = self.timestamp()
        with self.path.open("a") as stream:
            data.to_csv(stream, index=False, header=False)
            stream.flush()
            os.fsync(stream.fileno())
        self.cursor = int(data["id"].iloc[-1])
        self.rows += len(data)
