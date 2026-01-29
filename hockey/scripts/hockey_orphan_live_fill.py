# hockey/scripts/hockey_orphan_live_fill.py
from __future__ import annotations

import os
import time
import datetime as dt
import logging
from typing import Any, Dict, List

from hockey.hockey_db import hockey_fetch_all
from hockey.workers.hockey_live_status_worker import (
    tick_once_windowed,
    _int_env,
    _float_env,
    _int_set_env,
    hockey_live_leagues,   # ✅ live leagues env 파서
)

log = logging.getLogger("hockey_orphan_live_fill")
logging.basicConfig(level=logging.INFO)


LIVE_STATUSES = ("P1","P2","P3","BT","OT","SO","LIVE","HT","INT")


def _since_utc_yesterday_kst() -> dt.datetime:
    # 어제 00:00 KST → UTC timestamptz
    # (date_trunc('day', now() AT TIME ZONE 'Asia/Seoul') - interval '1 day') AT TIME ZONE 'Asia/Seoul'
    kst = dt.timezone(dt.timedelta(hours=9))
    now_kst = dt.datetime.now(dt.timezone.utc).astimezone(kst)
    yday_kst = (now_kst.replace(hour=0, minute=0, second=0, microsecond=0) - dt.timedelta(days=1))
    return yday_kst.astimezone(dt.timezone.utc)


def load_orphan_rows(*, stale_min: int) -> List[Dict[str, Any]]:
    """
    ✅ 고아 정의:
    - 어제 00:00 KST 이후
    - LIVE류 status
    - (poll_state 없음) OR (poll_state.updated_at 오래됨) OR (games.updated_at 오래됨)
    - 리그는 HOCKEY_LIVE_LEAGUES 안에 있는 것만
    """
    leagues = hockey_live_leagues()
    if not leagues:
        raise RuntimeError("HOCKEY_LIVE_LEAGUES is empty")

    since_utc = _since_utc_yesterday_kst()

    rows = hockey_fetch_all(
        f"""
        SELECT
          g.id, g.league_id, g.season, g.status, g.game_date
        FROM hockey_games g
        LEFT JOIN hockey_live_poll_state ps
          ON ps.game_id = g.id
        WHERE g.league_id = ANY(%s)
          AND g.status = ANY(%s)
          AND g.game_date >= %s
          AND (
            ps.game_id IS NULL
            OR ps.updated_at IS NULL
            OR ps.updated_at < NOW() - (%s || ' minutes')::interval
            OR g.updated_at < NOW() - (%s || ' minutes')::interval
          )
        ORDER BY g.game_date DESC
        """,
        (leagues, list(LIVE_STATUSES), since_utc, int(stale_min), int(stale_min)),
    )
    return [dict(r) for r in (rows or [])]


def main() -> None:
    stale_min = _int_env("HOCKEY_ORPHAN_STALE_MIN", 10)  # 기본 10분 이상 멈춘 걸 고아로
    max_games = _int_env("HOCKEY_ORPHAN_MAX_GAMES", 200) # 안전장치
    loops = _int_env("HOCKEY_ORPHAN_LOOPS", 1)          # 기본 1회만 돌림(원샷)

    super_fast_leagues = _int_set_env("HOCKEY_LIVE_SUPER_FAST_LEAGUES")
    super_fast_interval = _float_env("HOCKEY_LIVE_SUPER_FAST_INTERVAL_SEC", 5.0)

    fast_leagues = _int_set_env("HOCKEY_LIVE_FAST_LEAGUES")
    fast_interval = _float_env("HOCKEY_LIVE_FAST_INTERVAL_SEC", 10.0)

    slow_interval = _float_env("HOCKEY_LIVE_SLOW_INTERVAL_SEC", 20.0)

    pre_min = _int_env("HOCKEY_LIVE_PRESTART_MIN", 60)
    post_min = _int_env("HOCKEY_LIVE_POSTEND_MIN", 30)

    for i in range(loops):
        orphan_rows = load_orphan_rows(stale_min=stale_min)
        if not orphan_rows:
            log.info("no orphan rows (stale_min=%s). done.", stale_min)
            return

        orphan_rows = orphan_rows[:max_games]
        log.info("orphan fill loop=%s/%s stale_min=%s orphan_games=%s", i+1, loops, stale_min, len(orphan_rows))

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

        # 여러 루프 돌릴 때만 약간 쉼
        if i < loops - 1:
            time.sleep(2.0)


if __name__ == "__main__":
    main()
