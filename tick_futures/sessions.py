"""Versioned calendar/session interpretation; never controls whether we receive.

Keeping the connection open all day also covers auctions and uncertain holiday
sessions. Calendar uncertainty is carried into the coverage ledger explicitly.
"""

from datetime import datetime, timedelta

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


def classify(event_ns, metadata, calendar, night_overrides=None, preopen_seconds=600):
    sec, nano = divmod(int(event_ns), 1_000_000_000)
    dt = datetime.fromtimestamp(sec, CST)
    natural = dt.date()
    midnight = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    info = {"natural_day": natural.isoformat(), "trading_day": None, "session": "outside_or_unknown",
            "certainty": "evidence_insufficient"}
    for kind, field in (("day", "trading_time_day"), ("night", "trading_time_night")):
        for index, (start, end) in enumerate(metadata.get(field) or []):
            begin_seconds, end_seconds = clock_seconds(start), clock_seconds(end)
            if end_seconds <= begin_seconds:
                end_seconds += 86400
            for anchor_offset in (0, -1):
                anchor = midnight + timedelta(days=anchor_offset)
                begin = anchor + timedelta(seconds=begin_seconds)
                finish = anchor + timedelta(seconds=end_seconds)
                if not (begin - timedelta(seconds=preopen_seconds) <= dt <= finish):
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
                             "session": f"{kind}-{index}" + ("-preopen" if dt < begin else ""),
                             "certainty": "calendar_metadata" if certain else "evidence_insufficient"})
                return info
    return info
