"""Versioned calendar/session interpretation; never controls whether we receive.

Keeping the connection open all day also covers auctions and uncertain holiday
sessions. Calendar uncertainty is carried into the coverage ledger explicitly.
"""

from datetime import datetime, timedelta, timezone

from .common import CST


def clock_seconds(value):
    h, m, s = map(int, value.split(":"))
    if not (0 <= h < 48 and 0 <= m < 60 and 0 <= s < 60):
        raise ValueError(f"Invalid session time: {value}")
    return h * 3600 + m * 60 + s


def next_trading_day(day, calendar):
    for step in range(1, 32):
        candidate = day + timedelta(days=step)
        known = calendar.get(candidate.isoformat())
        if known is None:
            return None
        if known:
            return candidate
    return None


def epoch_ns(value):
    delta=value.astimezone(timezone.utc)-datetime(1970,1,1,tzinfo=timezone.utc)
    return (delta.days*86400+delta.seconds)*1_000_000_000+delta.microseconds*1000


def planned_sessions(trading_day,metadata,calendar,night_overrides=None):
    if not calendar.get(trading_day.isoformat()):
        return []
    previous=None
    for distance in range(1,32):
        candidate=trading_day-timedelta(days=distance)
        if candidate.isoformat() not in calendar:
            break
        if calendar[candidate.isoformat()]:
            previous=candidate
            break
    result=[]
    for kind,field,anchor_day in (("day","trading_time_day",trading_day),("night","trading_time_night",previous)):
        intervals=metadata.get(field)
        if anchor_day is None or not isinstance(intervals,(list,tuple)):
            continue
        override=(night_overrides or {}).get(anchor_day.isoformat())
        if kind=="night" and override is False:
            continue
        certain=kind=="day" or override is True or (trading_day-anchor_day).days<=3
        anchor=datetime.combine(anchor_day,datetime.min.time(),tzinfo=CST)
        for index,(start,end) in enumerate(intervals):
            a,b=clock_seconds(start),clock_seconds(end)
            if b<=a:
                b+=86400
            begin,finish=epoch_ns(anchor+timedelta(seconds=a)),epoch_ns(anchor+timedelta(seconds=b))
            for suffix,lo,hi,certainty in (("-preopen",begin-600_000_000_000,begin,"capture_buffer"),
                    ("",begin,finish,"calendar_metadata" if certain else "evidence_insufficient"),
                    ("-tail",finish,finish+180_000_000_000,"capture_buffer")):
                result.append({"trading_day":trading_day.isoformat(),"session":f"{kind}-{index}"+suffix,
                               "start_ns":lo,"end_ns":hi,"certainty":certainty})
    return result


def classify(event_ns, metadata, calendar, night_overrides=None, preopen_seconds=600):
    sec, nano = divmod(int(event_ns), 1_000_000_000)
    dt = datetime.fromtimestamp(sec, CST)
    natural = dt.date()
    midnight = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    info = {"natural_day": natural.isoformat(), "trading_day": None, "session": "outside_or_unknown",
            "certainty": "evidence_insufficient"}
    for kind, field in (("day", "trading_time_day"), ("night", "trading_time_night")):
        intervals=metadata.get(field)
        if not isinstance(intervals,(list,tuple)):
            continue
        for index, (start, end) in enumerate(intervals):
            begin_seconds, end_seconds = clock_seconds(start), clock_seconds(end)
            if end_seconds <= begin_seconds:
                end_seconds += 86400
            for anchor_offset in (0, -1):
                anchor = midnight + timedelta(days=anchor_offset)
                begin = anchor + timedelta(seconds=begin_seconds)
                finish = anchor + timedelta(seconds=end_seconds)
                begin_ns,finish_ns=epoch_ns(begin),epoch_ns(finish)
                if not (begin_ns-preopen_seconds*1_000_000_000 <= event_ns <= finish_ns+180_000_000_000):
                    continue
                anchor_day = anchor.date()
                if kind == "day":
                    trade_day = anchor_day if calendar.get(anchor_day.isoformat()) else None
                    certain = trade_day is not None
                else:
                    trade_day = next_trading_day(anchor_day, calendar)
                    override = (night_overrides or {}).get(anchor_day.isoformat())
                    # Missing holiday eve night notices are never guessed.
                    certain = bool(calendar.get(anchor_day.isoformat()) and trade_day
                                   and (trade_day - anchor_day).days <= 3)
                    if override is False:
                        continue
                    if override is True:
                        certain = trade_day is not None
                info.update({"trading_day": trade_day.isoformat() if trade_day else None,
                             "session": f"{kind}-{index}" + ("-preopen" if event_ns<begin_ns else "-tail" if event_ns>finish_ns else ""),
                             "certainty": "capture_buffer" if event_ns<begin_ns or event_ns>finish_ns else "calendar_metadata" if certain else "evidence_insufficient"})
                return info
    return info
