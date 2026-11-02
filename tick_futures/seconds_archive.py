"""Independent second-snapshot archives; audit integrity separately from market coverage."""

from collections import Counter
from datetime import date
import json
import math
import os
from pathlib import Path
import shutil
import sqlite3
import time
import uuid

from .archive import create, restore
from .audit import value_flags
from .codec import dumps, loads, sha256
from .runtime import InstanceLock, alert, atomic_json
from .seconds_store import SecondStore
from .storage_budget import Budget
from .common import BASE
from .sessions import classify, planned_sessions


def seconds_status(root):
    root=Path(root)
    h=json.loads((root/'heartbeat.json').read_text()) if (root/'heartbeat.json').exists() else {}
    config=json.loads((root/'service-config.json').read_text()) if (root/'service-config.json').exists() else {}
    result={'storage_mode':'main-second','prepared_trading_day':config.get('prepared_trading_day'),
        'target_start_ns':config.get('target_start_ns'),'runtime':h,'maximum_bytes':100_000_000_000,
        'pause_at_bytes':90_000_000_000,'upstream_zero_loss_proven':False,'complete_day_acceptance':'PENDING'}
    if h:
        result['heartbeat_age_seconds']=(time.time_ns()-h['heartbeat_ns'])/1e9
    if not (root/'seconds.sqlite3').exists():
        return {**result,'status':'prepared' if config else 'not_started','persisted_seconds':0,'contracts':[]}
    db=sqlite3.connect(f"file:{root/'seconds.sqlite3'}?mode=ro",uri=True)
    result['contracts']=[{'symbol':s,'seconds':n,'first_second_ns':a,'last_second_ns':z}
        for s,n,a,z in db.execute('SELECT symbol,count(*),min(second_ns),max(second_ns) FROM snapshots GROUP BY symbol')]
    result['persisted_seconds']=sum(r['seconds'] for r in result['contracts'])
    result['events']=dict(db.execute('SELECT kind,count(*) FROM events GROUP BY kind'))
    result['source_windows']=db.execute('SELECT sum(windows),sum(source_rows) FROM source_stats').fetchone()
    result['health_intervals']=db.execute('SELECT count(*) FROM health').fetchone()[0]
    db.close()
    idx=root/'seal-index.json'
    result['local_archives']=len(json.loads(idx.read_text()).get('archives',[])) if idx.exists() else 0
    result['source_semantics']='No new quote in a second creates no fabricated Tick; silent active scopes remain unknown unless independently justified'
    return result


def projection_transition_flags(previous, row):
    """Flags are about selected source values, never an estimate of lost Ticks."""
    flags=Counter()
    if previous is None:
        return flags
    for field in ('amount','volume','datetime','id'):
        try:
            a,b=previous[field],row[field]
            if math.isfinite(a) and math.isfinite(b) and b<a:
                flags[field+':decreased']+=1
        except (KeyError,TypeError,ValueError,OverflowError):
            pass
    for field in row.keys()-previous.keys():flags['schema:added:'+field]+=1
    for field in previous.keys()-row.keys():flags['schema:absent:'+field]+=1
    return flags


def audit_seconds(root, *, scan_values=False):
    root=Path(root)
    report=seconds_status(root)
    report.update(value_scan_performed=scan_values,ledger=[],gaps=[],field_flags={},projection_transition_flags={})
    if not (root/'seconds.sqlite3').exists():
        return report
    metadata={}
    for path in sorted((root/'discovery').glob('main-metadata-*.tfrec')):
        metadata.update({str(m['instrument_id']):m for m in loads(path.read_bytes())})
    plan=json.loads((root/'discovery/main-latest.json').read_text())
    calfile=root/'discovery/calendar.json'
    cal=json.loads(calfile.read_text()) if calfile.exists() else {'days':[]}
    calendar={x['date']:x['trading'] for x in cal['days']}
    overrides=cal.get('night_overrides',{})
    ledger,previous={},{}
    fields,transitions=Counter(),Counter()
    db=sqlite3.connect(f"file:{root/'seconds.sqlite3'}?mode=ro",uri=True)
    try:
        db.execute('BEGIN')
        report['contracts']=[{'symbol':s,'seconds':n,'first_second_ns':a,'last_second_ns':z}
            for s,n,a,z in db.execute('SELECT symbol,count(*),min(second_ns),max(second_ns) FROM snapshots GROUP BY symbol')]
        report['persisted_seconds']=sum(c['seconds'] for c in report['contracts'])
        for symbol,event_ns,raw in db.execute('SELECT symbol,event_ns,raw FROM snapshots ORDER BY symbol,second_ns'):
            scope=classify(event_ns,metadata.get(symbol,{}),calendar,overrides)
            day=scope['trading_day'] or 'UNKNOWN:'+scope['natural_day']
            key=(symbol,day,scope['session'])
            item=ledger.setdefault(key,{'exchange':symbol.split('.')[0],'product':str(metadata.get(symbol,{}).get('product_id','UNKNOWN')),
                'symbol':symbol,'trading_day':day,'session':scope['session'],'rows':0,'first_event_ns':event_ns,
                'last_event_ns':event_ns,'calendar_status':scope['certainty'],'collection_status':'evidence_insufficient'})
            item['rows']+=1;item['first_event_ns']=min(item['first_event_ns'],event_ns);item['last_event_ns']=max(item['last_event_ns'],event_ns)
            if scan_values:
                obj=loads(raw);fields.update(value_flags(obj));transitions.update(projection_transition_flags(previous.get(symbol),obj));previous[symbol]=obj
        day=report.get('prepared_trading_day')
        if day:
            for symbol in plan['symbols']:
                scopes=planned_sessions(date.fromisoformat(day),metadata.get(symbol,{}),calendar,overrides)
                for scope in scopes:
                    key=(symbol,day,scope['session'])
                    ledger.setdefault(key,{'exchange':symbol.split('.')[0],'product':str(metadata.get(symbol,{}).get('product_id','UNKNOWN')),
                        'symbol':symbol,'trading_day':day,'session':scope['session'],'rows':0,'first_event_ns':None,'last_event_ns':None,
                        'calendar_status':scope['certainty'],'collection_status':'evidence_insufficient',
                        'expected_start_ns':scope['start_ns'],'expected_end_ns':scope['end_ns']})
        events=[{'seq':s,'received_ns':t,'symbol':sy,'kind':kind,'detail':json.loads(d)} for s,t,sy,kind,d in db.execute('SELECT * FROM events ORDER BY seq')]
        replay={e['detail']['gap_event_id']:e['detail'] for e in events if e['kind']=='gap_replay_result' and e['detail'].get('gap_event_id') is not None}
        for event in events:
            if event['kind'] in ('source_window_unverified','active_session_silence_unknown','connection_interruption','collector_failure','restart_requires_source_overlap'):
                result=replay.get(event['seq'])
                report['gaps'].append({**event,'current_status':result['status'] if result else event['detail'].get('status','evidence_insufficient'),
                    'replay_result':result,'missing_tick_count':'unknown'})
        report.update(ledger=list(ledger.values()),field_flags=dict(fields),projection_transition_flags=dict(transitions),
            gap_status_counts=dict(Counter(x['current_status'] for x in report['gaps'])),events=events,
            planned_contracts=len(plan['symbols']),subscription_failures=report['runtime'].get('failures',{}),
            quality_scope='selected one-second projection only; sorting by event second cannot prove original source arrival order or all subsecond events')
        return report
    finally:
        db.close()


def seal_seconds(root, *, force=False):
    root=Path(root)
    config=json.loads((root/'service-config.json').read_text())
    with InstanceLock(root/'seal.lock'):
        store=SecondStore(root,config['target_start_ns'])
        try:
            index_path=root/'seal-index.json'
            index=json.loads(index_path.read_text()) if index_path.exists() else {'last_seq':0,'archives':[]}
            store.db.execute('BEGIN')
            cutoff=time.time_ns()//1_000_000_000*1_000_000_000-2_000_000_000
            size=store.db.execute('SELECT coalesce(sum(length(raw)),0) FROM snapshots WHERE generation!=exported_generation AND second_ns<?',(cutoff,)).fetchone()[0]
            audit_high=store.db.execute('SELECT coalesce(max(seq),0) FROM events').fetchone()[0]
            if not size and not force and audit_high<=index.get('last_event_seq',0):
                store.db.rollback()
                return {'status':'no_new_closed_seconds'}
            store.budget.check(size*3+32*1024**2,force=True)
            batch=uuid.uuid4().hex
            staging=root/'staging'/batch
            staging.mkdir(parents=True)
            keys,counts=[],Counter()
            fields,transitions,previous=Counter(),Counter(),{}
            bounds={}
            days=Counter()
            latest=json.loads((root/'discovery/main-latest.json').read_text())
            metadata={}
            if latest.get('metadata_file'):
                metadata={str(m['instrument_id']):m for m in loads((root/'discovery'/latest['metadata_file']).read_bytes())}
            calendar_info=json.loads((root/'discovery/calendar.json').read_text()) if (root/'discovery/calendar.json').exists() else {'days':[]}
            calendar={x['date']:x['trading'] for x in calendar_info['days']}
            data=staging/'snapshots.tfrec'
            observations=staging/'observations.tfrec'
            with data.open('xb') as out,observations.open('xb') as obs:
                for symbol,second,generation,fp,raw,received_ns,origin in store.db.execute('SELECT symbol,second_ns,generation,fingerprint,raw,received_ns,origin FROM snapshots WHERE generation!=exported_generation AND second_ns<? ORDER BY symbol,second_ns',(cutoff,)):
                    decoded=loads(raw)
                    if sha256(raw)!=fp or dumps(decoded)!=raw:
                        raise ValueError('Second snapshot hash/type/bit mismatch')
                    fields.update(value_flags(decoded));transitions.update(projection_transition_flags(previous.get(symbol),decoded));previous[symbol]=decoded
                    out.write(raw);keys.append((symbol,second,generation));counts[symbol]+=1
                    event_ns=int(decoded['datetime'])
                    if symbol not in bounds:bounds[symbol]=[event_ns,event_ns]
                    bounds[symbol]=[min(bounds[symbol][0],event_ns),max(bounds[symbol][1],event_ns)]
                    scope=classify(event_ns,metadata.get(symbol,{}),calendar,calendar_info.get('night_overrides',{}))
                    days[scope['trading_day'] or 'UNKNOWN:'+scope['natural_day']]+=1
                    obs.write(dumps({'symbol':symbol,'second_ns':second,'generation':generation,'received_ns':received_ns,
                        'origin':origin,'receive_time_semantics':'application window observation; historical/replay rows are not network-latency samples'}))
                if not index.get('observation_metadata_v1'):
                    for symbol,second,generation,received_ns,origin in store.db.execute('SELECT symbol,second_ns,generation,received_ns,origin FROM snapshots WHERE generation=exported_generation AND second_ns<? ORDER BY symbol,second_ns',(cutoff,)):
                        obs.write(dumps({'symbol':symbol,'second_ns':second,'generation':generation,'received_ns':received_ns,
                            'origin':origin,'receive_time_semantics':'metadata baseline for previous immutable exports; not a latency claim'}))
                out.flush();os.fsync(out.fileno())
                obs.flush();os.fsync(obs.fileno())
            events=[{'seq':s,'received_ns':t,'symbol':sy,'kind':k,'detail':json.loads(d)}
                for s,t,sy,k,d in store.db.execute('SELECT * FROM events WHERE seq>? AND seq<=?',(index.get('last_event_seq',0),audit_high))]
            health_high=store.db.execute('SELECT coalesce(max(seq),0) FROM health').fetchone()[0]
            health=staging/'health.tfrec'
            with health.open('xb') as out:
                for seq,run,start,end,detail in store.db.execute('SELECT * FROM health WHERE seq>? AND seq<=?',(index.get('last_health_seq',0),health_high)):
                    out.write(dumps({'seq':seq,'run':run,'start_ns':start,'end_ns':end,'detail':json.loads(detail)}))
                out.flush();os.fsync(out.fileno())
            store.db.commit()
            atomic_json(staging/'quality.json',{'selected_rows':len(keys),'raw_hash_type_bit_verification':'passed',
                'source_event_second_last':True,'all_subsecond_ticks_retained':False,'events':events,
                'field_flags':dict(fields),'projection_transition_flags':dict(transitions),
                'value_audit_scope':'selected records in this immutable export only; cumulative resets are flags, not proof of corruption',
                'collection_integrity':'evidence_insufficient','upstream_zero_loss_proven':False})
            atomic_json(staging/'plan.json',json.loads((root/'discovery/main-latest.json').read_text()))
            discovery={f'discovery/{p.name}':p for p in (root/'discovery').glob('*')
                       if p.is_file() and p.name not in index.get('discovery_files',[]) and p.name!='main-latest.json'}
            parent=index['archives'][-1]['archive_id'] if index['archives'] else None
            first=index['last_seq']+1
            destination=root/'archives'/batch
            m=create({'seconds/snapshots.tfrec':data,'seconds/observations.tfrec':observations,
                      'quality/health.tfrec':health,'quality/report.json':staging/'quality.json',
                      'selection/plan.json':staging/'plan.json',
                      'schema/second-snapshot-1.json':BASE/'schema/second-snapshot-1.json',**discovery},destination,codec='zstd',level=9,
                shard_bytes=64*1024**2,parent=parent,scope={'first_seq':first,'last_seq':first+len(keys)-1,
                'rows':len(keys),'contracts':[{'symbol':s,'rows':n,'first_event_ns':bounds[s][0],'last_event_ns':bounds[s][1]} for s,n in counts.items()],
                'trading_day_rows':dict(days),'observation_metadata_baseline':not index.get('observation_metadata_v1'),
                'selection':'last source-event snapshot/second; later revisions are incremental versions',
                'sequence_semantics':'export record ordinal, not original Tick arrival order',
                'collection_integrity':'evidence_insufficient','prepared_trading_day':config['prepared_trading_day']})
            verified=root/'verification'/batch
            report=restore(destination,verified)
            atomic_json(root/'verification-reports'/f'{batch}.json',report)
            # Publish the index before ACK. A crash can repeat an export; it cannot lose it.
            index['archives'].append({'archive_id':m['archive_id'],'path':str(destination.resolve()),
                'first_seq':first,'last_seq':first+len(keys)-1,'rows':len(keys),'locally_restored':True})
            index['last_seq']=first+len(keys)-1
            index['observation_metadata_v1']=True
            index['last_event_seq']=audit_high
            index['last_health_seq']=health_high
            index['discovery_files']=index.get('discovery_files',[])+[Path(n).name for n in discovery]
            atomic_json(index_path,index)
            with store.db:
                store.db.executemany('UPDATE snapshots SET exported_generation=? WHERE symbol=? AND second_ns=? AND generation=?',
                    [(generation,symbol,second,generation) for symbol,second,generation in keys])
                store.event(None,'seconds_archive_verified',{'archive_id':m['archive_id'],'rows':len(keys),
                    'raw_bytes':m['raw_bytes'],'compressed_bytes':m['stored_part_bytes'],'remote_restore':False})
            # These are verified transient duplicates; the selected source and cold archive remain.
            shutil.rmtree(verified)
            shutil.rmtree(staging)
            return {'status':'local_archive_and_empty_directory_restore_verified','archive_id':m['archive_id'],
                'rows':len(keys),'compressed_bytes':m['stored_part_bytes'],'remote_restore':False}
        finally:
            store.close()


def seconds_maintenance(root,config):
    root=Path(root)
    budget=Budget(root).check(force=True)
    if budget['logical_data_bytes']>=80_000_000_000 or budget['disk_free_bytes']<20*1024**3:
        alert(root,'seconds_storage_warning',budget)
    heartbeat=root/'heartbeat.json'
    h=json.loads(heartbeat.read_text()) if heartbeat.exists() else {}
    age=(time.time_ns()-h.get('heartbeat_ns',0))/1e9
    if age>30:
        alert(root,'seconds_heartbeat_stale',{'seconds':age,'status':h.get('status')})
    prior_path=root/'maintenance.json'
    prior=json.loads(prior_path.read_text()) if prior_path.exists() else {}
    result={'checked_ns':time.time_ns(),'heartbeat_age_seconds':age,'budget':budget,
        'rights_status':config.get('backup_rights_status','PENDING'),'last_seal_ns':prior.get('last_seal_ns',0)}
    if __import__('sys').platform.startswith('linux'):
        from .server import check_time_alignment
        result['time_alignment']=check_time_alignment(require_ntp=True)
    if budget['disk_free_bytes']>=20*1024**3 and (root/'seconds.sqlite3').exists() and time.time_ns()-result['last_seal_ns']>3600*1e9:
        result['seal']=seal_seconds(root)
        result['last_seal_ns']=time.time_ns()
    atomic_json(prior_path,result)
    return result


def verify_tfrec(path):
    rows=0
    with Path(path).open('rb') as stream:
        for raw in stream:
            if dumps(loads(raw))!=raw:
                raise ValueError(f'TFREC record types/bits mismatch at row {rows+1}')
            rows+=1
    return {'status':'all_record_bytes_types_bits_verified','rows':rows}


def queue_replay(root,symbols=None):
    root=Path(root)
    plan=json.loads((root/'discovery/main-latest.json').read_text())
    db=sqlite3.connect(f"file:{root/'seconds.sqlite3'}?mode=ro",uri=True)
    if symbols is None:
        finished={json.loads(d).get('gap_event_id') for d, in db.execute("SELECT detail FROM events WHERE kind='gap_replay_result'")}
        selected=list(dict.fromkeys(s for seq,s in db.execute("SELECT seq,symbol FROM events WHERE kind='source_window_unverified'") if seq not in finished))
    else:
        selected=symbols
    if any(s not in plan['symbols'] for s in selected):
        raise ValueError('Replay target absent from current main mapping')
    queued=[]
    for symbol in selected:
        cp=db.execute('SELECT event_ns,tail FROM checkpoints WHERE symbol=?',(symbol,)).fetchone()
        identity=uuid.uuid4().hex
        path=root/'replay-requests'/f'manual-{identity}.json'
        atomic_json(path,{'status':'pending','symbol':symbol,'start_ns':cp[0] if cp else None,
            'end_ns':cp[0] if cp else None,'anchors':json.loads(cp[1]) if cp else [],'created_ns':time.time_ns(),
            'request_scope':'fresh maximum same-source recent window; operation success and coverage separate'})
        queued.append(str(path))
    db.close()
    return {'status':'queued','source_rows_requested':10000,'requests':queued,'live_checkpoint_unchanged':True}


def verify_seconds_db(path):
    db=sqlite3.connect(f'file:{Path(path).resolve()}?mode=ro',uri=True)
    try:
        if db.execute('PRAGMA integrity_check').fetchone()[0]!='ok':
            raise ValueError('Second-snapshot SQLite integrity failure')
        rows=0
        for symbol,second,event_ns,raw,fp in db.execute('SELECT symbol,second_ns,event_ns,raw,fingerprint FROM snapshots'):
            obj=loads(raw)
            if sha256(raw)!=fp or dumps(obj)!=raw or int(obj['datetime'])!=event_ns or event_ns//1_000_000_000*1_000_000_000!=second or str(obj['symbol'])!=symbol:
                raise ValueError(f'Second record/hash/key/type failure at {symbol}/{second}')
            rows+=1
        return {'status':'sqlite_and_all_selected_record_bytes_types_bits_verified','rows':rows}
    finally:
        db.close()
