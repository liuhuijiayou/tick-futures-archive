"""Continuous main-month receiver; durable source overlap and one selected snapshot/second."""

from collections import deque
from datetime import datetime
import json
from pathlib import Path
import queue
import signal
import time
import uuid
import sys
import subprocess

import psutil

from .codec import dumps, loads, frame_rows
from .collector import changed_rows
from .common import CST
from .runtime import InstanceLock, alert, atomic_bytes, atomic_json
from .seconds_store import SecondStore, SecondWriter
from .sessions import classify
from .source import connect, EXCHANGES


def valid_source_frame(frame):
    # SDK padding has repeated id=-1; it is not a delivered Tick and must not
    # turn every later update into a full-window revision/replay.
    return frame.loc[(frame['id']>=0) & (frame['datetime']>0)].copy(deep=True)


def current_ready_count(subscriptions, plan):
    return sum(subscriptions.get(symbol,{}).get('state')=='ready' for symbol in plan.get('symbols',[]))


def read_control_file(root, path):
    try:
        info=json.loads(path.read_text())
        if not isinstance(info,dict) or not isinstance(info.get('symbol'),str) or not isinstance(info.get('status'),str):
            raise ValueError('Control file requires a symbol and status')
        return info
    except (ValueError,UnicodeError) as exc:
        path.rename(path.with_suffix('.invalid-'+str(time.time_ns())))
        alert(root,'invalid_second_control_file',{'file':path.name,'error_type':type(exc).__name__})
        return None


async def query_main_plan(api, root, config):
    candidates = []
    for exchange in EXCHANGES:
        result = api.query_cont_quotes(exchange_id=exchange)
        await result
        candidates.extend(list(result))
    symbols = list(dict.fromkeys(candidates))
    metadata = []
    for start in range(0,len(symbols),100):
        result = api.query_symbol_info(symbols[start:start+100])
        await result
        columns = [(str(k), result[k].to_numpy(copy=True)) for k in result.columns]
        metadata.extend({k:v[i] for k,v in columns} for i in range(len(result)))
    selected, excluded, products = [], [], set()
    for row in metadata:
        symbol = str(row['instrument_id'])
        key = (symbol.split('.')[0],str(row.get('product_id')))
        if str(row.get('ins_class'))!='FUTURE' or bool(row.get('expired')) or key[0] not in EXCHANGES:
            excluded.append({'symbol':symbol,'reason':'expired/non-future/outside-six-markets'})
            continue
        if key in products:
            raise ValueError('Ambiguous main contract for '+str(key))
        products.add(key)
        selected.append(row)
    if len(selected)<config.get('minimum_main_contracts',87):
        raise ValueError('Main enumeration below expected product count; previous subscriptions retained')
    expected=set(config.get('expected_products',[]))
    missing=expected-{exchange+'.'+product for exchange,product in products}
    if missing:
        raise ValueError('Expected main products missing: '+','.join(sorted(missing)))
    version = str(time.time_ns())
    raw = dumps(selected)
    atomic_bytes(root/'discovery'/f'main-metadata-{version}.tfrec',raw)
    plan = {'version':version,'captured_ns':int(version),'source':'TqSdk.query_cont_quotes/query_symbol_info',
            'account_mode':'TqSim','trading_day_prepared':config['prepared_trading_day'],
            'target_start_ns':config['target_start_ns'],'count':len(selected),
            'symbols':[str(r['instrument_id']) for r in selected],
            'mapping':[{'exchange':str(r['exchange_id']),'product':str(r['product_id']),
                        'symbol':str(r['instrument_id'])} for r in selected],
            'excluded':excluded,'metadata_file':f'main-metadata-{version}.tfrec',
            'sampling':'last source-event snapshot/second; selected raw fields unchanged; no quiet-second fills'}
    atomic_json(root/'discovery'/f'main-{version}.json',plan)
    atomic_json(root/'discovery'/'main-latest.json',plan)
    return plan,selected


def collect_seconds(root, config, *, api_factory=connect, seconds=None):
    from .server import check_time_alignment
    check_time_alignment(require_ntp=sys.platform.startswith('linux'))
    root = Path(root)
    root.mkdir(parents=True,exist_ok=True,mode=0o700)
    with InstanceLock(root/'collector.lock'):
        return _collect(root,config,api_factory,seconds)


def _collect(root,config,api_factory,seconds):
    target = int(config['target_start_ns'])
    run = uuid.uuid4().hex
    begun = time.monotonic()
    run_started_ns = time.time_ns()
    stop = False
    def halt(*_):
        nonlocal stop
        stop = True
    handlers = {s:signal.signal(s,halt) for s in (signal.SIGTERM,signal.SIGINT)}
    writer = SecondWriter(root,target)
    control = SecondStore(root,target)
    api = None
    frames,committed,pending,subs,meta = {},{},{},{},{}
    dirty,todo,failures = set(),deque(),{}
    last_quote,last_unknown = {},{}
    refresh_box = {}
    refresh_task = None
    last_refresh,last_heartbeat = 0,0
    reconnects = 0
    changed_seconds = 0
    max_queue = 0
    max_loop = 0
    status = 'starting'
    calendar_info=json.loads((root/'discovery'/'calendar.json').read_text())
    calendar={r['date']:r['trading'] for r in calendar_info['days']}
    latest_plan={}
    import_tokens={}
    last_import=0
    replay_child=None
    replay_path=None
    last_replay_check=0
    def ack(token,result):
        nonlocal changed_seconds
        symbol,frame = pending.pop(token)
        if frame is not None:
            committed[symbol]=frame
        changed_seconds+=result['changed_seconds']
        if token in import_tokens:
            path=import_tokens.pop(token)
            info=json.loads(path.read_text());info.update(status='failed' if result.get('error') else 'completed',result=result,completed_ns=time.time_ns());atomic_json(path,info)
            if info.get('replay_request_file'):
                request=Path(info['replay_request_file']);r=json.loads(request.read_text())
                r.update(status='failed' if result.get('error') else 'completed',result=result,completed_ns=time.time_ns());atomic_json(request,r)
    def receive_acks():
        while True:
            try:
                token,result=writer.acks.get_nowait()
            except queue.Empty:
                break
            ack(token,result)
    try:
        with control.db:
            prior=root/'heartbeat.json'
            if prior.exists() and json.loads(prior.read_text()).get('status')=='running':
                control.event(None,'restart_requires_source_overlap',{'status':'evidence_insufficient','previous_run':json.loads(prior.read_text()).get('run')})
            control.event(None,'run_started',{'run':run,'target_start_ns':target,'sampling':'source-event-second-last'})
        while not stop and (seconds is None or time.monotonic()-begun<seconds):
            loop = time.monotonic()
            writer.check()
            receive_acks()
            if replay_child is not None and replay_child.poll() is not None:
                request=json.loads(replay_path.read_text())
                if replay_child.returncode!=0:
                    request.update(status='failed',exit_code=replay_child.returncode)
                    atomic_json(replay_path,request)
                    alert(root,'second_window_replay_failed',{'symbol':request['symbol'],'exit_code':replay_child.returncode})
                replay_child=None
            if time.monotonic()-last_replay_check>=10:
                folder=root/'replay-requests';folder.mkdir(exist_ok=True)
                resolved={json.loads(d).get('gap_event_id') for d, in control.db.execute("SELECT detail FROM events WHERE kind='gap_replay_result'")}
                for seq,symbol,detail in control.db.execute("SELECT seq,symbol,detail FROM events WHERE kind='source_window_unverified'"):
                    path=folder/f'gap-{seq}.json'
                    if seq in resolved or path.exists():continue
                    d=json.loads(detail);atomic_json(path,{'status':'pending','symbol':symbol,'gap_event_id':seq,
                        'start_ns':d.get('start_ns',d.get('previous_event_ns')),'end_ns':d.get('end_ns',d.get('first_source_ns')),
                        'anchors':d.get('anchors',[]),'created_ns':time.time_ns()})
                if replay_child is None and config.get('allow_replay',True) and latest_plan and current_ready_count(subs,latest_plan)==latest_plan['count']:
                    for path in folder.glob('*.json'):
                        d=read_control_file(root,path)
                        if d is None:continue
                        if d.get('status')=='running':
                            d['status']='pending';atomic_json(path,d)
                        if d.get('status')!='pending':continue
                        d.update(status='running',started_ns=time.time_ns());atomic_json(path,d)
                        replay_child=subprocess.Popen([sys.executable,'-m','tick_futures.seconds_replay','--root',str(root),'--request',str(path)],stdout=subprocess.DEVNULL)
                        replay_path=path;break
                last_replay_check=time.monotonic()
            if not import_tokens and len(pending)<4 and not writer.inbox.full() and time.monotonic()-last_import>=1:
                for path in (root/'imports').glob('ready-*.json'):
                    info=read_control_file(root,path)
                    if info is None:continue
                    if info.get('status')!='pending':
                        continue
                    token=uuid.uuid4().hex;symbol=info['symbol']
                    writer.inbox.put_nowait((token,symbol,None,{'import_path':str(path)}))
                    pending[token]=(symbol,None);import_tokens[token]=path;last_import=time.monotonic()
                    break
            if api is None:
                try:
                    api=api_factory()
                    refresh_box={}
                    async def refresh():
                        try:
                            refresh_box['result']=await query_main_plan(api,root,config)
                        except Exception as exc:
                            refresh_box['error']={'type':type(exc).__name__,'message':str(exc)}
                    refresh_task=api.create_task(refresh())
                    frames,committed,subs={}, {}, {}
                    dirty.clear()
                    todo=deque()
                    status='initializing'
                except Exception as exc:
                    alert(root,'seconds_connect_failed',{'type':type(exc).__name__,'message':str(exc)})
                    until=time.monotonic()+min(60,2**min(reconnects+1,6))
                    while time.monotonic()<until and not stop:
                        time.sleep(.2)
                    reconnects+=1
                    continue
            if refresh_box.get('result'):
                latest_plan,records=refresh_box.pop('result')
                meta.update({str(r['instrument_id']):r for r in records})
                desired=latest_plan['symbols']
                # Add the new month first, then retain one minute of overlap.
                for symbol,subscription in subs.items():
                    subscription.pop('retire_after',None)
                    if symbol in desired and subscription['state']=='retired':
                        subscription.update(state='initializing',started=time.monotonic())
                todo=deque(s for s in todo if s in desired)
                todo.extend(s for s in desired if s not in frames and s not in todo and s not in subs)
                last_refresh=time.monotonic()
                refresh_task=None
                with control.db:
                    control.event(None,'main_mapping',{'version':latest_plan['version'],'symbols':desired,'excluded':latest_plan['excluded']})
            if refresh_box.get('error'):
                error=refresh_box.pop('error')
                alert(root,'seconds_main_refresh_failed',error)
                with control.db:
                    control.event(None,'main_mapping_unverified',error)
                refresh_task=None
                last_refresh=time.monotonic()-240
            initializing=sum(s['state']=='initializing' for s in subs.values())
            if todo and initializing<config.get('initialization_concurrency',4) and len(pending)<8:
                symbol=todo.popleft()
                subs[symbol]={'state':'initializing','started':time.monotonic()}
                async def subscribe(symbol=symbol):
                    try:
                        frames[symbol]=api.get_tick_serial(symbol,data_length=config.get('data_length',1000))
                    except Exception as exc:
                        failures[symbol]={'type':type(exc).__name__,'message':str(exc)}
                        subs[symbol]['state']='failed'
                        alert(root,'seconds_subscription_failed',{'symbol':symbol,**failures[symbol]})
                api.create_task(subscribe())
            if not refresh_task and time.monotonic()-last_refresh>300:
                refresh_box={}
                async def refresh():
                    try:
                        refresh_box['result']=await query_main_plan(api,root,config)
                    except Exception as exc:
                        refresh_box['error']={'type':type(exc).__name__,'message':str(exc)}
                refresh_task=api.create_task(refresh())
            try:
                updated=api.wait_update(deadline=time.time()+.2)
            except Exception as exc:
                with control.db:
                    control.event(None,'connection_interruption',{'status':'gap_pending_replay','message':str(exc),'reconnects':reconnects})
                while pending:
                    writer.check();receive_acks();time.sleep(.05)
                api.close();api=None;reconnects+=1
                continue
            busy={s for s,_ in pending.values()}
            for symbol,frame in list(frames.items()):
                if symbol not in latest_plan.get('symbols',[]) and current_ready_count(subs,latest_plan)==latest_plan.get('count'):
                    if subs[symbol]['state']=='retired':
                        continue
                    retire_after=subs[symbol].setdefault('retire_after',time.monotonic()+60)
                    if time.monotonic()>=retire_after and symbol not in busy:
                        subs[symbol]['state']='retired';committed.pop(symbol,None);dirty.discard(symbol)
                        with control.db:
                            control.event(symbol,'old_main_overlap_retired',{'overlap_seconds':60,'mapping_version':latest_plan['version'],
                                'sdk_object_retained_until_reconnect':True})
                        continue
                if updated and api.is_changing(frame):
                    dirty.add(symbol);last_quote[symbol]=time.monotonic()
                if not api.is_serial_ready(frame):
                    if time.monotonic()-subs[symbol]['started']>180 and symbol not in failures:
                        failures[symbol]={'message':'initialization timeout; slot retained'}
                        alert(root,'seconds_initialization_timeout',{'symbol':symbol})
                    continue
                if subs[symbol]['state']=='initializing':
                    subs[symbol]['state']='ready';dirty.add(symbol);failures.pop(symbol,None)
                if symbol in busy or (symbol not in dirty and symbol in committed):
                    continue
                if writer.inbox.full() or len(pending)>=8:
                    dirty.add(symbol);continue
                current=valid_source_frame(frame)
                delta,full=changed_rows(current,committed.get(symbol))
                # The source window is copied stably before reaching the writer.
                rows=frame_rows(delta.loc[delta['datetime']>=target])
                tail=frame_rows(current.iloc[-8:])
                token=uuid.uuid4().hex
                writer.inbox.put_nowait((token,symbol,rows,{'tail':tail,'full_window':full,
                    'received_ns':time.time_ns(),'origin':'initial_or_replay' if symbol not in committed else 'live_update'}))
                pending[token]=(symbol,current);dirty.discard(symbol)
            if time.monotonic()-last_heartbeat>=5:
                now=time.time_ns()
                coverage={}
                for symbol in latest_plan.get('symbols',[]):
                    scope=classify(now,meta.get(symbol,{}),calendar,calendar_info.get('night_overrides',{}))
                    active=scope['session']!='outside_or_unknown' and not scope['session'].endswith(('-preopen','-tail'))
                    age=time.monotonic()-last_quote.get(symbol,begun)
                    state='source_live' if age<120 else 'quiet_or_unknown'
                    coverage[symbol]={'source_notification_age_seconds':age,'active_session':active,'state':state}
                    if active and subs.get(symbol,{}).get('state')=='ready' and age>120 and time.monotonic()-last_unknown.get(symbol,0)>120:
                        with control.db:
                            control.event(symbol,'active_session_silence_unknown',{'status':'evidence_insufficient','seconds':age})
                        alert(root,'seconds_active_session_silence_unknown',{'symbol':symbol,'seconds':age})
                        last_unknown[symbol]=time.monotonic()
                ready=current_ready_count(subs,latest_plan)
                status='running' if latest_plan and ready>=latest_plan['count'] else 'initializing'
                max_queue=max(max_queue,writer.inbox.qsize())
                with control.db:
                    control.db.execute('INSERT INTO health(run,start_ns,end_ns,detail) VALUES(?,?,?,?)',
                        (run,now-5_000_000_000,now,json.dumps({'api_present':True,'ready':ready,
                            'targets':latest_plan.get('count',0),'queue':writer.inbox.qsize(),'pending':len(pending),
                            'note':'process/source-subscription evidence, not exchange upstream completeness'})))
                atomic_json(root/'heartbeat.json',{'run':run,'pid':__import__('os').getpid(),'heartbeat_ns':now,
                    'started_ns':run_started_ns,'target_start_ns':target,'status':status,'storage_mode':'main-second','prepared_trading_day':config['prepared_trading_day'],
                    'source':'TqSdk','account_mode':'TqSim','main_contracts':latest_plan.get('count',0),
                    'ready_subscriptions':ready,'requested_count':len(subs)+len(todo),'queue':writer.inbox.qsize(),
                    'retained_old_main_sdk_objects':sum(s not in latest_plan.get('symbols',[]) for s in subs),
                    'pending':len(pending),'dirty_deferred':len(dirty),'failures':failures,'reconnects':reconnects,
                    'changed_seconds_this_run':changed_seconds,'max_queue':max_queue,'max_loop_seconds':max_loop,
                    'receiver_rss_bytes':psutil.Process().memory_info().rss,'mapping_version':latest_plan.get('version'),
                    'source_window_rows':config.get('data_length',1000),'sampling':'last source-event snapshot/second; no fabricated quiet seconds',
                    'replay_worker_pid':replay_child.pid if replay_child is not None else None,
                    'coverage':coverage,'full_day_acceptance':'PENDING','upstream_zero_loss_proven':False,
                    'uncommitted_risk':'stable memory queue until FULL commit; durable second candidate and source overlap on restart'})
                last_heartbeat=time.monotonic()
            max_loop=max(max_loop,time.monotonic()-loop)
        writer.close()
        receive_acks()
        status='stopped'
        return {'status':status,'run':run,'changed_seconds':changed_seconds,'ready':current_ready_count(subs,latest_plan)}
    except BaseException as exc:
        status='failed'
        alert(root,'seconds_collector_failed',{'type':type(exc).__name__,'message':str(exc)})
        with control.db:
            control.event(None,'collector_failure',{'status':'evidence_insufficient','message':str(exc)})
        raise
    finally:
        try:
            if writer.thread.is_alive():writer.close()
        finally:
            if replay_child is not None and replay_child.poll() is None:
                replay_child.terminate()
            if api:api.close()
            control.close()
            for sig,handler in handlers.items():signal.signal(sig,handler)
            if (root/'heartbeat.json').exists():
                h=json.loads((root/'heartbeat.json').read_text());h.update(status=status,stopped_ns=time.time_ns(),
                    queue=writer.inbox.qsize(),pending=len(pending),changed_seconds_this_run=changed_seconds);atomic_json(root/'heartbeat.json',h)
