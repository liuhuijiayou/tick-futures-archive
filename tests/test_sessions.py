from datetime import date, datetime, timedelta
import unittest

from tick_futures.common import CST
from tick_futures.sessions import classify, clock_seconds


def ns(text):
    dt = datetime.fromisoformat(text).replace(tzinfo=CST)
    epoch = datetime(1970, 1, 1, tzinfo=__import__("datetime").timezone.utc)
    delta = dt - epoch
    return (delta.days*86400 + delta.seconds)*1_000_000_000


class Sessions(unittest.TestCase):
    def setUp(self):
        self.meta = {"trading_time_day": [["09:00:00","11:30:00"],["13:30:00","15:00:00"]],
                     "trading_time_night": [["21:00:00","26:30:00"]]}
        start = date(2026, 11, 1)
        self.calendar = {(d:=start+timedelta(days=i)).isoformat():d.weekday()<5 for i in range(20)}

    def test_friday_night_and_saturday_midnight_map_to_monday(self):
        for text in ("2026-11-06T21:00:00", "2026-11-07T00:30:00", "2026-11-07T02:30:00"):
            value = classify(ns(text), self.meta, self.calendar)
            self.assertEqual(value["trading_day"], "2026-11-09")
            self.assertEqual(value["certainty"], "calendar_metadata")

    def test_holiday_night_unknown_until_explicit_notice(self):
        for d in ("2026-11-09","2026-11-10"):
            self.calendar[d] = False
        text = ns("2026-11-06T21:30:00")
        self.assertEqual(classify(text,self.meta,self.calendar)["certainty"], "evidence_insufficient")
        self.assertEqual(classify(text,self.meta,self.calendar,{"2026-11-06":False})["session"], "outside_or_unknown")
        self.assertEqual(classify(text,self.meta,self.calendar,{"2026-11-06":True})["trading_day"], "2026-11-11")

    def test_preopen_lunch_weekend_and_unknown_calendar(self):
        self.assertEqual(classify(ns("2026-11-02T08:55:00"),self.meta,self.calendar)["session"], "day-0-preopen")
        self.assertEqual(classify(ns("2026-11-02T12:00:00"),self.meta,self.calendar)["session"], "outside_or_unknown")
        self.assertEqual(classify(ns("2026-11-08T10:00:00"),self.meta,self.calendar)["certainty"], "evidence_insufficient")
        self.assertEqual(classify(ns("2027-01-04T10:00:00"),self.meta,self.calendar)["trading_day"], None)
        self.assertEqual(clock_seconds("26:30:00"), 95400)


if __name__ == "__main__":
    unittest.main()
