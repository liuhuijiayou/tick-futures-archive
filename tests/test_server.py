from pathlib import Path
import contextlib
import io
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch, MagicMock

from tick_futures.server import check_time_alignment,systemd_units
from tick_futures.seconds_replay import replay_evidence
from tick_futures.codec import dumps,sha256


class ServerPreflight(unittest.TestCase):
    def test_default_collect_without_preparation_cannot_start_legacy_receiver(self):
        from tick_futures.__main__ import main
        with tempfile.TemporaryDirectory() as temp,contextlib.chdir(temp),patch('tick_futures.collector.collect',side_effect=AssertionError('network must not start')),patch('sys.stderr',io.StringIO()) as err:
            self.assertEqual(main(['collect']),1)
            self.assertIn('prepare-seconds',err.getvalue())

    def clock(self,host,offset,ntp,required=True):
        dt=MagicMock()
        dt.now.return_value.astimezone.return_value.utcoffset.return_value.total_seconds.return_value=offset
        with patch('tick_futures.server.sys.platform','linux'),patch('tick_futures.server.datetime',dt),patch('tick_futures.server.subprocess.run',return_value=SimpleNamespace(stdout=f'Timezone={host}\nNTPSynchronized={ntp}\n',returncode=0)):
            return check_time_alignment(require_ntp=required)

    def test_process_china_tz_cannot_hide_host_utc(self):
        with self.assertRaisesRegex(RuntimeError,'China-time'):
            self.clock('UTC',28800,'yes')

    def test_host_china_with_wrong_process_zone_fails(self):
        with self.assertRaisesRegex(RuntimeError,'China-time'):
            self.clock('Asia/Shanghai',0,'yes')

    def test_unsynchronized_clock_fails_and_aligned_clock_does_not_shift_source(self):
        with self.assertRaisesRegex(RuntimeError,'synchronized'):
            self.clock('Asia/Shanghai',28800,'no')
        result=self.clock('Asia/Shanghai',28800,'yes')
        self.assertFalse(result['raw_source_timestamps_shifted'])
        self.assertEqual(result['timezone'],'Asia/Shanghai')

    def test_system_units_have_explicit_china_calendars_user_and_preflight(self):
        with tempfile.TemporaryDirectory() as temp:
            out=Path(temp)/'units';systemd_units(Path(temp)/'data',out,'tickdata')
            text=(out/'tick-futures-collector.service').read_text()
            self.assertIn('User=tickdata',text)
            self.assertIn('Environment=TZ=Asia/Shanghai',text)
            self.assertIn('server-preflight',text)
            self.assertIn('RestartSec=30',text)
            self.assertNotIn('TQ_PASSWORD',text)
            timer=(out/'tick-futures-preopen.timer').read_text()
            self.assertIn('07:30:00 Asia/Shanghai',timer)
            self.assertIn('19:30:00 Asia/Shanghai',timer)
            self.assertIn('*:*:00 Asia/Shanghai',(out/'tick-futures-maintenance.timer').read_text())

    def test_maximum_source_replay_does_not_call_unknown_history_verified(self):
        rows=[{'id':i,'datetime':200_000_000_000+i*1_000_000_000} for i in range(10000)]
        result=replay_evidence(rows,{'start_ns':100_000_000_000,'end_ns':200_000_000_000,'anchors':[]})
        self.assertEqual(result['status'],'gap_unrecoverable')
        self.assertEqual(replay_evidence(rows[:10],{'start_ns':100_000_000_000,'anchors':[]})['status'],'evidence_insufficient')
        anchor=rows[2]
        result=replay_evidence(rows,{'start_ns':202_000_000_000,'end_ns':203_000_000_000,'anchors':[{'id':2,'fp':sha256(dumps(anchor))}]})
        self.assertEqual(result['status'],'window_verified')
