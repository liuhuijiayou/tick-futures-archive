"""Run reproducible synthetic tests while forbidding IP network connections.

Use a real file entry point so macOS multiprocessing spawn can import it.
The guard also loads in spawned children; Unix sockets for local IPC remain usable.
"""

import argparse
import contextlib
import hashlib
import json
from pathlib import Path
import socket
import sys
import time
import unittest


BASE=Path(__file__).resolve().parents[1]
if str(BASE) not in sys.path:
    sys.path.insert(0,str(BASE))

_connect=socket.socket.connect
_connect_ex=socket.socket.connect_ex
_blocked=[]


def offline_connect(sock, address):
    if sock.family in (socket.AF_INET,socket.AF_INET6):
        _blocked.append(True)
        raise AssertionError('IP network connection forbidden during offline validation')
    return _connect(sock,address)


def offline_connect_ex(sock, address):
    if sock.family in (socket.AF_INET,socket.AF_INET6):
        _blocked.append(True)
        raise AssertionError('IP network connection forbidden during offline validation')
    return _connect_ex(sock,address)


socket.socket.connect=offline_connect
socket.socket.connect_ex=offline_connect_ex


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,default=BASE/'reports/local/offline-validation')
    args=parser.parse_args()
    output=args.output.resolve();output.mkdir(parents=True,exist_ok=True)
    with contextlib.chdir(BASE):
        files=sorted([*Path('tick_futures').glob('*.py'),*Path('tests').glob('*.py'),
                      *Path('scripts').glob('*.py'),Path('pyproject.toml')])
        digest=hashlib.sha256(b''.join(str(p).encode()+b'\0'+p.read_bytes()+b'\0' for p in files)).hexdigest()
        suite=unittest.defaultTestLoader.discover('tests')
        start=time.monotonic();started_ns=time.time_ns()
        with (output/'unittest.log').open('w') as stream:
            result=unittest.TextTestRunner(stream=stream,verbosity=2).run(suite)
        report={'tests':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),
            'skipped':len(result.skipped),'success':result.wasSuccessful(),
            'started_ns':started_ns,'duration_seconds':time.monotonic()-start,
            'source_sha256':digest,'source_files':len(files),'ip_network_guard':'enabled in parent and spawned children',
            'blocked_network_attempts_in_parent':len(_blocked),'real_quote_downloads':0,'data_uploads':0}
        from tick_futures.runtime import atomic_json
        atomic_json(output/'report.json',report)
        print(json.dumps(report))
        if not result.wasSuccessful():
            print((output/'unittest.log').read_text()[-16000:])
        return 0 if result.wasSuccessful() else 1


if __name__=='__main__':
    raise SystemExit(main())
