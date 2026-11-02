"""Project-only macOS LaunchAgents and separate receiver/maintenance processes."""

import json
import os
from pathlib import Path
import plistlib
import shutil
import subprocess
import sys
import time

from .common import BASE, load_auth
from .runtime import InstanceLock, alert, atomic_bytes, atomic_json

LABEL = "com.liuhui.tick-futures"


def config_path(root):
    return Path(root)/"service-config.json"


def definition(role, root):
    label = LABEL + "." + role
    logs = Path(root)/"logs"
    result = {"Label":label,"ProgramArguments":[sys.executable,"-m","tick_futures","--root",str(Path(root).resolve()),"service",role],
              "WorkingDirectory":str(BASE),"RunAtLoad":True,"ProcessType":"Background",
              "StandardOutPath":str(logs/(role+".stdout.log")),"StandardErrorPath":str(logs/(role+".stderr.log")),
              "EnvironmentVariables":{"PATH":"/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin","PYTHONUNBUFFERED":"1"}}
    if role == "collector":
        result.update({"KeepAlive":True,"ThrottleInterval":30,"ExitTimeOut":45,
                       "StartCalendarInterval":[{"Hour":7,"Minute":30},{"Hour":19,"Minute":30}]})
    else:
        result.update({"StartInterval":300,"Nice":15})
    return result


def manage(action, root, auth_config=None):
    if sys.platform != "darwin":
        raise RuntimeError("This deployment is macOS; Linux systemd deployment is not yet implemented")
    root = Path(root).resolve()
    agents = Path.home()/"Library/LaunchAgents"
    domain = f"gui/{os.getuid()}"
    def run(args):
        r = subprocess.run(["launchctl",*args],capture_output=True,text=True)
        # launchctl print includes unrelated inherited environment; don't expose it.
        stdout = r.stdout
        if args[0] == "print":
            import re
            stdout = "\n".join(line.strip() for line in stdout.splitlines()
                              if re.match(r"\s*(state|pid|runs|last exit code|path|program|working directory|throttle interval) =",line))
        return {"returncode":r.returncode,"stdout":stdout,"stderr":r.stderr}
    def wait_absent(label):
        deadline=time.monotonic()+50
        while run(["print",domain+"/"+label])["returncode"]==0:
            if time.monotonic()>=deadline:
                raise TimeoutError("launchd has not unloaded "+label)
            time.sleep(.2)
    if action == "install":
        if not auth_config:
            raise ValueError("Install requires explicit --auth-config path; no credential values are saved")
        auth_config = auth_config.expanduser().resolve(strict=True)
        os.environ["TQ_AUTH_CONFIG"] = str(auth_config)
        load_auth()  # AST only, never executes config.
        root.mkdir(parents=True,exist_ok=True,mode=0o700)
        (root/"logs").mkdir(exist_ok=True,mode=0o700)
        settings={"auth_config":str(auth_config),"data_length":1000,
                    "min_free_gib":5,"archive_level":9,"github_repo":"liuhuijiayou/tick-futures-archive",
                    "backup_rights_status":"PENDING","installed_ns":time.time_ns()}
        if config_path(root).exists():
            settings.update(json.loads(config_path(root).read_text()))
        settings["auth_config"]=str(auth_config)
        atomic_json(config_path(root),settings)
        agents.mkdir(parents=True,exist_ok=True)
        for role in ("collector","maintenance"):
            atomic_bytes(agents/(LABEL+"."+role+".plist"),plistlib.dumps(definition(role,root)))
        return {"status":"installed","enabled":False,"domain":domain,"root":str(root)}
    results = {}
    for role in ("collector","maintenance"):
        label = LABEL+"."+role
        path = agents/(label+".plist")
        if action == "enable":
            if not path.exists():
                raise FileNotFoundError("Run schedule install first")
            run(["enable",domain+"/"+label])
            loaded = run(["bootstrap",domain,str(path)])
            # Already loaded is handled by checking the actual job state.
            result = run(["print",domain+"/"+label])
            if "state = SIGTERMed" in result["stdout"]:
                # bootout returns before a receiver finishes its WAL drain.
                # An immediate enable must not mistake that dying job for a
                # newly bootstrapped receiver.
                wait_absent(label)
                loaded=run(["bootstrap",domain,str(path)])
                result=run(["print",domain+"/"+label])
            if result["returncode"]:
                raise RuntimeError(f"launchd could not enable {label}: {loaded['stderr']}")
            results[role] = result
        elif action == "status":
            results[role] = run(["print",domain+"/"+label])
        elif action in ("disable","uninstall"):
            results[role] = run(["bootout",domain+"/"+label])
            run(["disable",domain+"/"+label])
            wait_absent(label)
            if action == "uninstall" and path.exists():
                path.unlink()  # explicit project-only uninstall, no data deletion
    return {"action":action,"jobs":results,"user_login_required":True}


def maintenance(root, config):
    root = Path(root)
    with InstanceLock(root/"maintenance.lock"):
        heartbeat = root/"heartbeat.json"
        h = json.loads(heartbeat.read_text()) if heartbeat.exists() else {}
        age = (time.time_ns()-h.get("heartbeat_ns",0))/1e9
        free = shutil.disk_usage(root).free
        archives_root = root/"archives"
        manifests = list(archives_root.glob("*/manifest.json"))
        backlog = [p for p in manifests if not (p.parent/"remote-receipt.json").exists()]
        report = {"checked_ns":time.time_ns(),"heartbeat_age_seconds":age,"disk_free_bytes":free,
                  "upload_backlog_archives":len(backlog),"rights_status":config["backup_rights_status"]}
        if age>30 or (h and h.get("status")!="running"):
            alert(root,"collector_heartbeat_stale",{"age_seconds":age,"status":h.get("status")})
        if free<20*1024**3:
            alert(root,"disk_low",{"free_bytes":free,"automatic_deletion":False})
        prior_path = root/"maintenance.json"
        prior = json.loads(prior_path.read_text()) if prior_path.exists() else {}
        last_seal = prior.get("last_seal_ns",0)
        if free>=20*1024**3 and time.time_ns()-last_seal>3600*1e9 and (root/"hot.sqlite3").exists():
            from .archive import seal
            destination = archives_root/str(time.time_ns())
            report["seal"] = {k:v for k,v in seal(root,destination,level=config["archive_level"]).items()
                              if k in ("archive_id","raw_bytes","stored_part_bytes","status","scope")}
            report["last_seal_ns"] = time.time_ns()
        else:
            report["last_seal_ns"] = last_seal
        # Off-host market upload stays blocked until documented authorization.
        if config["backup_rights_status"] == "approved":
            from .github import upload
            for path in backlog:
                m = json.loads(path.read_text())
                upload(path.parent,config["github_repo"],"archive-"+m["archive_id"],authorization=config.get("authorization_file"))
        atomic_json(prior_path,report)
        # Bounded local diagnostics; only this project's logs are rotated.
        for log in (root/"logs").glob("*.log"):
            if log.stat().st_size>10*1024**2:
                old = log.with_suffix(".previous")
                if not old.exists():
                    log.rename(old)
        return report


def service(role, root):
    root = Path(root)
    config = json.loads(config_path(root).read_text())
    os.environ["TQ_AUTH_CONFIG"] = config["auth_config"]
    if role == "maintenance":
        try:
            return maintenance(root,config)
        except Exception as exc:
            alert(root,"maintenance_failed",{"type":type(exc).__name__,"message":str(exc)})
            raise
    from .collector import collect
    # launchd restarts on unexpected exit. Keep one receiver alive through
    # lunch/midnight/weekends; calendar is for auditing, never for cutting data.
    return collect(root,data_length=config["data_length"],min_free_bytes=int(config["min_free_gib"]*1024**3))
