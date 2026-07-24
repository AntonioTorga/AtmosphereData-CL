"""Time-interval parsing and period chunking.

The interval parsing here is lifted from ClimateGraph's ``utils/general_utils.py``
(functions only — the original module imports cartopy at top level for unrelated
enums). The period-chunking helpers are new: sources need to split a requested
range into per-request buckets, which ClimateGraph never had to do.
"""

import datetime
import logging
import re

import pandas as pd
from dateutil import parser

log = logging.getLogger(__name__)

def _parses_as_single(token: str) -> bool:
    try:
        parser.parse(token, dayfirst=True)
        return True
    except (ValueError, OverflowError):
        return False


def _split_interval(time_interval: str) -> tuple[str, str]:
    """Split ``"start - end"`` / ``"start to end"`` into endpoints, or a single
    date to itself.

    A bare hyphen is ambiguous — ISO dates contain them (``"2026-07-20"``) — so it
    is only treated as a range separator when the whole string does *not* parse as
    a single date. An explicit ``to`` or a spaced `` - `` is always a range.
    """
    if (m := re.match(r"^(.+?)\s+to\s+(.+)$", time_interval, re.IGNORECASE)):
        return m.group(1), m.group(2)
    if (m := re.match(r"^(.+?)\s+-\s+(.+)$", time_interval)):
        return m.group(1), m.group(2)
    if _parses_as_single(time_interval):
        return time_interval, time_interval
    if "-" in time_interval:
        head, tail = time_interval.split("-", 1)
        return head.strip(), tail.strip()
    return time_interval, time_interval

# Coarsest-to-finest. Only resolutions coarser than "hour" expand to a full bucket.
_RESOLUTION_ORDER = ("year", "month", "day", "hour", "minute", "second")
COARSE_OFFSETS = {
    "day": pd.Timedelta(days=1),
    "month": pd.DateOffset(months=1),
    "year": pd.DateOffset(years=1),
}


def _parse_with_resolution(token: str) -> tuple[pd.Timestamp, str]:
    """Parse a date token and detect the resolution it was written at.

    The trick: parse twice with two wildly different defaults. Any field that
    comes out equal must have been specified in the token itself, since a
    default would have produced two different values. So "9/2022" is detected
    as month-resolution while "9/9/2022" is day-resolution.
    """
    floored = parser.parse(token, dayfirst=True, default=datetime.datetime(1999, 1, 1))
    probe = parser.parse(
        token, dayfirst=True, default=datetime.datetime(2002, 7, 8, 9, 10, 11)
    )

    resolution = "year"
    for field in _RESOLUTION_ORDER:
        if getattr(floored, field) == getattr(probe, field):
            resolution = field
    return pd.Timestamp(floored), resolution


def _bucket_end(value: pd.Timestamp, resolution: str) -> pd.Timestamp:
    """End of the bucket ``value`` falls in, for its resolution.

    For coarse resolutions (day/month/year) returns the last representable
    instant of the bucket (start of the next bucket minus 1 ns), so an inclusive
    slice covers the whole day/month/year without spilling into the next. For
    hour-or-finer the value is an exact point and is returned unchanged.
    """
    offset = COARSE_OFFSETS.get(resolution)
    if offset is None:
        return value
    return value + offset - pd.Timedelta(1, "ns")


def manage_time_interval(time_interval: str | None) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    """Turn a time-interval string into ``(start, end)``.

    Accepts either a single date or a "start - end" / "start to end" range. Each
    endpoint is treated as an interval covering its own resolution when that
    resolution is coarser than hourly, so ``"9/2022"`` spans all of September.
    """
    if time_interval is None:
        return None, None
    time_interval = time_interval.strip()

    start_str, end_str = _split_interval(time_interval)

    start_val, start_res = _parse_with_resolution(start_str)
    end_val, end_res = _parse_with_resolution(end_str)

    if start_res != end_res:
        log.warning(
            "time_interval %r endpoints have different temporal resolutions "
            "(%s vs %s); expanding each on its own bucket.",
            time_interval,
            start_res,
            end_res,
        )

    return start_val, _bucket_end(end_val, end_res)


def normalize_time(time: str | list[str] | None) -> list[str | None]:
    """Coerce a ``time`` field into a list of entries to iterate over."""
    if isinstance(time, list):
        return time
    return [time]


def as_interval(period) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Coerce the many things callers pass as a period into ``(start, end)``.

    Accepts an interval string ("9/2022", "1/9/2022 to 30/9/2022"), a
    ``(start, end)`` pair, or a single datetime-like.
    """
    if isinstance(period, str):
        start, end = manage_time_interval(period)
    elif isinstance(period, (tuple, list)) and len(period) == 2:
        start, end = pd.Timestamp(period[0]), pd.Timestamp(period[1])
    else:
        start = end = pd.Timestamp(period)
    if start is None or end is None:
        raise ValueError(f"Could not resolve a time interval from {period!r}")
    return start, end


# Chunking. Sources differ in what a single request can cover: SINCA takes an
# arbitrary from/to range in one call, Vipnet takes one instant. `chunk_period`
# expresses that difference as data rather than as per-source loop code.
_CHUNK_FREQ = {
    "hour": "h",
    "day": "D",
    "month": "MS",
    "year": "YS",
}


_CHUNK_OFFSET = {
    "hour": pd.Timedelta(hours=1),
    "day": pd.Timedelta(days=1),
    "month": pd.DateOffset(months=1),
    "year": pd.DateOffset(years=1),
}


def _floor_to_grain(ts: pd.Timestamp, grain: str) -> pd.Timestamp:
    """Floor ``ts`` to the start of the bucket of ``grain`` that contains it."""
    if grain == "hour":
        return ts.floor("h")
    if grain == "day":
        return ts.floor("D")
    if grain == "month":
        return ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0, nanosecond=0)
    if grain == "year":
        return ts.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0, nanosecond=0)
    raise ValueError(f"Unknown chunk grain {grain!r}; expected one of {sorted(_CHUNK_FREQ)}")


def chunk_period(start: pd.Timestamp, end: pd.Timestamp, grain: str | None) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    """Split ``[start, end]`` into consecutive buckets of ``grain``.

    ``grain=None`` means the range is not chunked at all — one bucket covering
    everything, which is what a source that accepts a from/to range wants.
    Buckets are inclusive of ``end`` and clipped to the requested range, so the
    first and last bucket may be partial. A single instant (``start == end``)
    yields exactly one bucket.
    """
    if grain is None:
        return [(start, end)]
    if grain not in _CHUNK_FREQ:
        raise ValueError(f"Unknown chunk grain {grain!r}; expected one of {sorted(_CHUNK_FREQ)}")

    freq = _CHUNK_FREQ[grain]
    offset = _CHUNK_OFFSET[grain]
    # Walk bucket starts from the boundary containing `start` — flooring to the
    # grain's *own* boundary (not the day), so a mid-bucket start still yields
    # whole buckets without emitting spurious pre-start ones.
    starts = pd.date_range(start=_floor_to_grain(start, grain), end=end, freq=freq)
    if len(starts) == 0:
        starts = pd.DatetimeIndex([_floor_to_grain(start, grain)])

    buckets = []
    for bucket_start in starts:
        lo = max(bucket_start, start)
        hi = min(bucket_start + offset - pd.Timedelta(1, "ns"), end)
        if lo > hi:
            continue  # bucket lies entirely outside [start, end]
        buckets.append((lo, hi))
    return buckets
