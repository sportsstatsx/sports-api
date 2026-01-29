# hockey/scripts/hockey_orphan_live_fill.py
from __future__ import annotations

import time
import argparse
import datetime as dt
import logging
from typing import Any, Dict, List, Optional

from hockey.hockey_db import hockey_fetch_all
from hockey.workers.hockey_live_status_worker import (
    tick_once_windowed,
    _int_env,
    _float_env,
    _int_set_env,
    hockey_live_leagues,
)

log = logging.getLogger("hockey_orphan_live_fill")
logging.basicConfig(level=logging.INFO)

LIVE_STATUSES = ("P1", "P2", "P3", "BT", "OT", "SO", "LIVE", "HT", "INT")


def _kst_date_to_utc_start(d: dt.date) -> dt.datetime:
    # KST d 00:00:00 -> UTC
    kst = dt.timezone(dt.timedelta(hours=9))
    return dt.datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=kst).astimezone(dt.timezone.utc)


def _kst_date_to_utc_end_exclusive(d: dt.date) -> dt.datetime:
    # KST (d+1) 00:00:00 -> UTC  (end-exclusive)
    return _kst_date_to_utc_start(d + dt.timedelta(days=1))


def _parse_kst_date(s: str) -> dt.date:
    return dt.date.fromisoformat(s.strip())


def load_orphan_rows(
    *,
    leagues: List[int],
    since_utc: dt.datetime,
    until_utc: dt.datetime,
    stale_min: int,
    statuses: List[str],
    max_games: int,
) -> List[Dict[str, Any]]:
    """
    ✅ 고아 정의:
    - league in leagues
    - status in statuses
    - game_date in [since_utc, until_utc)
    - (poll_state 없음) OR (poll_state.updated_at 오래됨) OR (games.updated_at 오래됨)
    """
    rows = hockey_fetch_all(
        """
        SELECT
          g.id, g.league_id, g.season, g.status, g.game_date
        FROM hockey_games g
        LEFT JOIN hockey_live_poll_state ps
          ON ps.game_id = g.id
        WHERE g.league_id = ANY(%s)
          AND g.status = ANY(%s)
          AND g.game_date >= %s
          AND g.game_date < %s
          AND (
            ps.game_id IS NULL
            OR ps.updated_at IS NULL
            OR ps.updated_at < NOW() - (%s || ' minutes')::interval
            OR g.updated_at < NOW() - (%s || ' minutes')::interval
          )
        ORDER BY g.game_date DESC
        """,
        (leagues, statuses, since_utc, until_utc, int(stale_min), int(stale_min)),
    )
    out = [dict(r) for r in (rows or [])]
    return out[:max_games]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-kst", required=True, help="KST start date YYYY-MM-DD (inclusive)")
    ap.add_argument("--to-kst", required=True, help="KST end date YYYY-MM-DD (inclusive)")
    ap.add_argument(
        "--leagues",
        default="",
        help="comma-separated league_ids. empty => use HOCKEY_LIVE_LEAGUES",
    )
    ap.add_argument(
        "--stale-min",
        type=int,
        default=10,
        help="consider orphan if poll_state/games updated_at older than this minutes (default 10)",
    )
    ap.add_argument(
        "--max-games",
        type=int,
        default=300,
        help="safety cap (default 300)",
    )
    ap.add_argument(
        "--loops",
        type=int,
        default=1,
        help="how many passes to run (default 1)",
    )
    ap.add_argument(
        "--sleep",
        type=float,
        default=2.0,
        help="sleep seconds between loops (default 2.0)",
    )
    ap.add_argument(
        "--statuses",
        default=",".join(LIVE_STATUSES),
        help="comma-separated statuses to include (default live-ish set)",
    )

    args = ap.parse_args()

    from_kst = _parse_kst_date(args.from_kst)
    to_kst = _parse_kst_date(args.to_kst)
    if to_kst < from_kst:
        raise SystemExit("to-kst must be >= from-kst")

    since_utc = _kst_date_to_utc_start(from_kst)
    until_utc = _kst_date_to_utc_end_exclusive(to_kst)

    # leagues
    if args.leagues.strip():
        leagues = [int(x.strip()) for x in args.leagues.split(",") if x.strip()]
    else:
        leagues = hockey_live_leagues()

    if not leagues:
        raise SystemExit("no leagues resolved: provide --leagues or set HOCKEY_LIVE_LEAGUES")

    statuses = [x.strip() for x in args.statuses.split(",") if x.strip()]

    # intervals (live worker와 동일 env 사용)
    super_fast_leagues = _int_set_env("HOCKEY_LIVE_SUPER_FAST_LEAGUES")
    super_fast_interval = _float_env("HOCKEY_LIVE_SUPER_FAST_INTERVAL_SEC", 5.0)

    fast_leagues = _int_set_env("HOCKEY_LIVE_FAST_LEAGUES")
    fast_interval = _float_env("HOCKEY_LIVE_FAST_INTERVAL_SEC", 10.0)

    slow_interval = _float_env("HOCKEY_LIVE_SLOW_INTERVAL_SEC", 20.0)

    pre_min = _int_env("HOCKEY_LIVE_PRESTART_MIN", 60)
    post_min = _int_env("HOCKEY_LIVE_POSTEND_MIN", 30)

    log.info(
        "orphan fill start: kst=%s..%s(inc) utc=[%s..%s) leagues=%s stale_min=%s statuses=%s loops=%s",
        from_kst.isoformat(),
        to_kst.isoformat(),
        since_utc.isoformat(),
        until_utc.isoformat(),
        leagues,
        args.stale_min,
        statuses,
        args.loops,
    )

    for i in range(int(args.loops)):
        orphan_rows = load_orphan_rows(
            leagues=leagues,
            since_utc=since_utc,
            until_utc=until_utc,
            stale_min=int(args.stale_min),
            statuses=statuses,
            max_games=int(args.max_games),
        )

        if not orphan_rows:
            log.info("no orphan rows. done.")
            return

        log.info("loop=%s/%s orphan_games=%s (cap=%s)", i + 1, args.loops, len(orphan_rows), args.max_games)

        g_up, e_up, cand = tick_once_windowed(
            orphan_rows,
            super_fast_leagues=super_fast_leagues,
            fast_leagues=fast_leagues,
            super_fast_interval=super_fast_interval,
            fast_interval=fast_interval,
            slow_interval=slow_interval,
            pre_min=pre_min,
            post_min=post_min,
        )

        log.info("orphan tick done: candidates=%s games_upserted=%s events_upserted=%s", cand, g_up, e_up)

        if i < int(args.loops) - 1:
            time.sleep(float(args.sleep))


if __name__ == "__main__":
    main()
