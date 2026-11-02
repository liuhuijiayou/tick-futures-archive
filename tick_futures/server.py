"""Server preparation and explicit China-time deployment units; no credentials in units."""

from datetime import date, datetime
import getpass
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time

from .codec import loads
from .common import BASE, CST
from .main_contracts import discover_mains
from .runtime import atomic_json, atomic_bytes, InstanceLock
from .sessions import planned_sessions
from .source import connect, discover


def check_time_alignment(*, require_ntp=False):
    process_offset=datetime.now().astimezone().utcoffset().total_seconds()
    host_zone=None
    ntp=None
    if sys.platform.startswith('linux'):
        try:
            r=subprocess.run(['timedatectl','show','--property=Timezone','--property=NTPSynchronized'],
                capture_output=True,text=True,timeout=5)
            values=dict(line.split('=',1) for line in r.stdout.splitlines() if '=' in line)
            host_zone=values.get('Timezone');ntp=values.get('NTPSynchronized')
        except (OSError,subprocess.TimeoutExpired):
            pass
        if not host_zone:
            resolved=str(Path('/etc/localtime').resolve())
            host_zone='Asia/Shanghai' if resolved.endswith('/Asia/Shanghai') else 'PRC' if resolved.endswith('/PRC') else resolved
    else:
        resolved=str(Path('/etc/localtime').resolve())
        host_zone='Asia/Shanghai' if resolved.endswith('/Asia/Shanghai') else 'PRC' if resolved.endswith('/PRC') else resolved
    if process_offset!=28800 or host_zone not in ('Asia/Shanghai','PRC'):
        raise RuntimeError(f'China-time preflight failed: host={host_zone}, process_offset={process_offset}; set host timezone Asia/Shanghai and TZ=Asia/Shanghai before starting')
    if require_ntp and sys.platform.startswith('linux') and ntp!='yes':
        raise RuntimeError('Clock not confirmed synchronized: timedatectl NTPSynchronized must be yes')
    return {'timezone':'Asia/Shanghai','host_timezone':host_zone,'process_offset_seconds':process_offset,
            'ntp_synchronized':ntp,'checked_ns':time.time_ns(),'raw_source_timestamps_shifted':False}


def prepare_seconds(root, trading_day, auth_config):
    check_time_alignment(require_ntp=sys.platform.startswith('linux'))
    root=Path(root).resolve();root.mkdir(parents=True,exist_ok=True,mode=0o700)
    auth_config=Path(auth_config).expanduser().resolve(strict=True)
    os.environ['TQ_AUTH_CONFIG']=str(auth_config)
    with InstanceLock(root/'collector.lock'):
        api=connect()
        try:
            u=discover(api,root/'discovery/full',calendar=True)
            if u.get('calendar_error'):
                raise RuntimeError('Versioned trading calendar unavailable: '+str(u['calendar_error']))
            main=discover_mains(api,root/'discovery/full',root/'main-discovery')
            if main['missing'] or main['ambiguous'] or main['failures']:
                raise RuntimeError('Main contract scope incomplete; see main-discovery report')
            rejected=[]
            for item in main['rejected']:
                info=api.query_symbol_info([item['symbol']])
                if info.empty or not bool(info.iloc[0]['expired']):
                    raise RuntimeError('Unresolved main mapping rejection: '+item['symbol'])
                rejected.append({'symbol':item['symbol'],'expired':True})
        finally:
            api.close()
        metadata=loads((root/'discovery/full'/u['metadata_file']).read_bytes())
        chosen={m['symbol'] for m in main['mapping']}
        selected=[m for m in metadata if str(m['instrument_id']) in chosen]
        calfile=sorted((root/'discovery/full').glob('calendar-*.json'))[-1]
        cal=json.loads(calfile.read_text());calendar={r['date']:r['trading'] for r in cal['days']}
        day=date.fromisoformat(trading_day)
        if calendar.get(trading_day) is not True:
            raise ValueError('Requested trading day is closed/unknown in the versioned calendar')
        intervals=[scope for m in selected for scope in planned_sessions(day,m,calendar,cal.get('night_overrides',{}))]
        if not intervals or any(x['certainty']=='evidence_insufficient' for x in intervals):
            raise ValueError('Night/day session calendar insufficient; explicit exchange holiday notice required')
        start=min(x['start_ns'] for x in intervals)
        atomic_bytes(root/'discovery/calendar.json',calfile.read_bytes())
        config={'auth_config':str(auth_config),'storage_mode':'main-second','prepared_trading_day':trading_day,
                'target_start_ns':start,'minimum_main_contracts':len(main['mapping']),
                'expected_products':[m['exchange']+'.'+m['product'] for m in main['mapping']],
                'initialization_concurrency':4,'data_length':1000,'archive_level':9,'min_free_gib':5,
                'storage_budget_bytes':100_000_000_000,'storage_pause_bytes':90_000_000_000,
                'backup_rights_status':'PENDING','data_upload_enabled':False,'github_repo':'liuhuijiayou/tick-futures-archive',
                'timezone':'Asia/Shanghai','prepared_ns':time.time_ns(),'rejected_expired_mains':rejected}
        prior=root/'service-config.json'
        if prior.exists():
            previous=json.loads(prior.read_text())
            config['target_start_ns']=min(previous.get('target_start_ns',start),start)
        atomic_json(prior,config)
        result={k:v for k,v in config.items() if k!='auth_config'}
        result['target_start_local']=datetime.fromtimestamp(config['target_start_ns']//10**9,CST).isoformat()
        atomic_json(root/'preflight/next-day.json',result)
        return result


def server_preflight(root):
    report=check_time_alignment(require_ntp=True)
    root=Path(root)
    config=json.loads((root/'service-config.json').read_text())
    if config.get('storage_mode')!='main-second':
        raise ValueError('Run prepare-seconds first')
    if not Path(config['auth_config']).is_file():
        raise FileNotFoundError('Explicit AST credential configuration missing')
    from .storage_budget import Budget
    report['budget']=Budget(root).check(force=True)
    report['python']=platform.python_version()
    report['architecture']=platform.machine()
    report['prepared_trading_day']=config['prepared_trading_day']
    atomic_json(root/'preflight/server.json',report)
    return report


def systemd_units(root, output, user=None):
    root=Path(root).resolve();output=Path(output);output.mkdir(parents=True,exist_ok=True)
    user=user or getpass.getuser()
    if not user or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-' for c in user):
        raise ValueError('Invalid systemd User')
    def quote(value):
        return '"'+str(value).replace('\\','\\\\').replace('"','\\"').replace('%','%%')+'"'
    command=f'{quote(sys.executable)} -m tick_futures --root {quote(root)}'
    label='tick-futures'
    common=f'User={user}\nWorkingDirectory={quote(BASE)}\nEnvironment=TZ=Asia/Shanghai\nEnvironment=PYTHONUNBUFFERED=1\nUMask=0077\n'
    collector=f'''[Unit]
Description=Main futures second snapshots (Asia/Shanghai)
Wants=network-online.target time-sync.target
After=network-online.target time-sync.target
StartLimitIntervalSec=0
[Service]
Type=simple
{common}ExecStartPre={command} server-preflight
ExecStart={command} service collector
Restart=on-failure
RestartSec=30
TimeoutStopSec=60
KillMode=control-group
[Install]
WantedBy=multi-user.target
'''
    maintenance=f'''[Unit]
Description=Futures disk/health/archive maintenance
After=network-online.target
[Service]
Type=oneshot
{common}Nice=15
ExecStart={command} service maintenance
'''
    timer='''[Unit]
Description=Futures minute maintenance
[Timer]
OnBootSec=60
OnCalendar=*-*-* *:*:00 Asia/Shanghai
Persistent=true
AccuracySec=1
Unit=tick-futures-maintenance.service
[Install]
WantedBy=timers.target
'''
    preopen='''[Unit]
Description=Futures pre-open start fallback, China time
[Timer]
OnCalendar=*-*-* 07:30:00 Asia/Shanghai
OnCalendar=*-*-* 19:30:00 Asia/Shanghai
Persistent=true
AccuracySec=1
Unit=tick-futures-collector.service
[Install]
WantedBy=timers.target
'''
    files={label+'-collector.service':collector,label+'-maintenance.service':maintenance,
           label+'-maintenance.timer':timer,label+'-preopen.timer':preopen}
    for name,content in files.items():
        atomic_bytes(output/name,content.encode())
    return {'status':'unit_files_generated_not_installed','output':str(output.resolve()),'files':list(files),
            'timezone':'Asia/Shanghai','system_units':True,'user':user,'server_execution_verified':False}
