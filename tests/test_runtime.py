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
