# hockey/workers/hockey_live_common.py
from __future__ import annotations

import os
import datetime as dt
from typing import List


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def parse_int_csv(env_value: str) -> List[int]:
    out: List[int] = []
    for part in (env_value or "").split(","):
        s = part.strip()
        if not s:
            continue
        try:
            out.append(int(s))
        except Exception:
            continue
    return out


def hockey_live_leagues() -> List[int]:
    return parse_int_csv(os.getenv("HOCKEY_LIVE_LEAGUES", ""))


def interval_sec(default: float = 25.0) -> float:
    v = (os.getenv("HOCKEY_WORKER_INTERVAL_SEC") or "").strip()
    if not v:
        return default
    try:
        return float(v)
    except Exception:
        return default


def notify_interval_sec(default: float = 6.0) -> float:
    v = (os.getenv("HOCKEY_NOTIFY_INTERVAL_SEC") or "").strip()
    if not v:
        return default
    try:
        return float(v)
    except Exception:
        return default
