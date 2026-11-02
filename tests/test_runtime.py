import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tick_futures.runtime import alert
from tick_futures.schedule import service


class OperationalFailures(unittest.TestCase):
    def test_disk_failure_still_emits_redacted_stderr_alert(self):
        stream=io.StringIO()
        with patch.dict("os.environ",{"TQ_PASSWORD":"fixture-secret"}), patch("sys.stderr",stream), patch("tick_futures.runtime.atomic_json",side_effect=OSError("disk full")):
            alert("unused","disk_failure",{"message":"fixture-secret"})
        self.assertIn("alert_file_write_failed",stream.getvalue())
        self.assertIn("disk_failure",stream.getvalue())
        self.assertNotIn("fixture-secret",stream.getvalue())

    def test_maintenance_failure_preserves_exception_and_local_alert(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            (root/"service-config.json").write_text(json.dumps({"auth_config":"/fixture/auth.py"}))
            with patch("tick_futures.schedule.maintenance",side_effect=OSError("fixture archive failure")), patch("sys.stderr",io.StringIO()), patch.dict("os.environ",{},clear=False):
                with self.assertRaisesRegex(OSError,"fixture archive failure"):
                    service("maintenance",root)
            self.assertEqual(json.loads((root/"last_alert.json").read_text())["kind"],"maintenance_failed")

    def test_enable_waits_for_an_old_terminating_launchagent(self):
        from tick_futures.schedule import manage,LABEL
        from subprocess import CompletedProcess
        with tempfile.TemporaryDirectory() as folder:
            home=Path(folder)
            agents=home/"Library/LaunchAgents"
            agents.mkdir(parents=True)
            for role in ("collector","maintenance"):
                (agents/(LABEL+"."+role+".plist")).write_bytes(b"fixture")
            seen={}
            def command(args,**kw):
                if args[1]=="print":
                    seen[args[2]]=seen.get(args[2],0)+1
                    n=seen[args[2]]
                    return CompletedProcess(args,113 if n==2 else 0,"state = SIGTERMed" if n==1 else "state = running","")
                return CompletedProcess(args,0,"","")
            with patch("tick_futures.schedule.sys.platform","darwin"),patch("tick_futures.schedule.Path.home",return_value=home),patch("tick_futures.schedule.subprocess.run",side_effect=command),patch("tick_futures.schedule.time.sleep"):
                result=manage("enable",home/"root")
            self.assertTrue(all(r["stdout"]=="state = running" for r in result["jobs"].values()))
