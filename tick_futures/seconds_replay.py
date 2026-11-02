"""A single public 10,000-row helper publishes stable second-snapshot imports."""

import argparse
import json
import os
from pathlib import Path
import signal
import time
import uuid

from .codec import frame_rows, dumps, sha256
from .runtime import InstanceLock, atomic_bytes, atomic_json
from .source import connect
from .storage_budget import Budget


def replay_evidence(rows, request):
    first=min((int(r['datetime']) for r in rows),default=None)
    last=max((int(r['datetime']) for r in rows),default=None)
    anchors=request.get('anchors',[])
    ids={a['id'] for a in anchors};fingerprints={a['fp'] for a in anchors}
    matched=any(sha256(dumps(r)) in fingerprints for r in rows if int(r['id']) in ids)
    start=request.get('start_ns');end=request.get('end_ns',start)
    if first is None:
        status='evidence_insufficient'
    elif start is not None and first>start:
        status='gap_unrecoverable' if len(rows)>=10000 else 'evidence_insufficient'
    elif matched and end is not None and last>=end:
        status='window_verified'
    else:
        status='evidence_insufficient'
    return {'status':status,'returned_rows':len(rows),'requested_rows':10000,'first_source_ns':first,
            'last_source_ns':last,'content_anchor_matched':matched,'requested_start_ns':start,
            'requested_end_ns':end,'missing_tick_count':'unknown','proof_scope':'returned same-source window only'}


def fetch(root,request_path):
    root,request_path=Path(root),Path(request_path)
    config=json.loads((root/'service-config.json').read_text())
    os.environ['TQ_AUTH_CONFIG']=config['auth_config']
    request=json.loads(request_path.read_text())
    with InstanceLock(root/'replay-worker.lock'):
        Budget(root,min_free=20*1024**3).check(64*1024**2,force=True)
        signal.alarm(180)
        api=connect()
        try:
            frame=api.get_tick_serial(request['symbol'],data_length=10000)
            rows=frame_rows(frame)
        finally:
            api.close();signal.alarm(0)
        evidence=replay_evidence(rows,request)
        selected={}
        for row in rows:
            ns=int(row['datetime'])
            if ns<config['target_start_ns']:continue
            sec=ns//1_000_000_000
            if sec not in selected or ns>=int(selected[sec]['datetime']):selected[sec]=row
        raw=b''.join(dumps(row) for row in selected.values())
        if len(raw)>64*1024**2:
            raise ValueError('Recovery output exceeds bounded chunk size')
        Budget(root,min_free=20*1024**3).check(len(raw)*2,force=True)
        identity=uuid.uuid4().hex;folder=root/'imports';folder.mkdir(exist_ok=True)
        data=folder/(identity+'.tfrec');atomic_bytes(data,raw)
        info={'status':'pending','symbol':request['symbol'],'origin':'recovery_backfill',
              'data_file':str(data.resolve()),'raw_sha256':sha256(raw),'rows':len(selected),
              'source_completed_ns':time.time_ns(),'replay_request_file':str(request_path.resolve()),
              'replay_evidence':evidence,'gap_event_id':request.get('gap_event_id'),
              'source_provenance':{'method':'TqSim public get_tick_serial 10000','source_window':evidence}}
        atomic_json(folder/('ready-'+identity+'.json'),info)
        request.update(status='waiting_import',source_completed_ns=info['source_completed_ns'],evidence=evidence)
        atomic_json(request_path,request)
        return evidence


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',type=Path,required=True);p.add_argument('--request',type=Path,required=True)
    a=p.parse_args();os.nice(15);print(json.dumps(fetch(a.root,a.request)))


if __name__=='__main__':main()
