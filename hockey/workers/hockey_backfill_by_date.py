# hockey/workers/hockey_backfill_by_date.py
from __future__ import annotations

import os
import sys
import datetime as dt
from typing import Any, Dict, List, Optional, Set, Tuple

from hockey.hockey_db import hockey_execute, hockey_fetch_all
from hockey.workers.hockey_live_status_worker import (
    _get,
    upsert_game,
    upsert_events,
    ensure_event_key_migration,
    hockey_live_leagues,
)

# ------------------------------------------------------------
# Clean backfill by date (DELETE -> REINSERT)
#
# Usage:
#   python -m hockey.workers.hockey_backfill_by_date --date 2026-01-29
#
# Options:
#   --tz Asia/Seoul          (default: Asia/Seoul)
#   --all-leagues            (ignore HOCKEY_LIVE_LEAGUES filter)
#   --no-events              (skip events backfill)
# ------------------------------------------------------------


def _parse_args(argv: List[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "date": None,              # YYYY-MM-DD
        "tz": "Asia/Seoul",
        "all_leagues": False,
        "fetch_events": True,
    }

    i = 0
    while i < len(argv):
        a = argv[i].strip()

        if a == "--date":
            if i + 1 >= len(argv):
                raise SystemExit("missing value for --date (ex: --date 2026-01-29)")
            out["date"] = argv[i + 1].strip()
            i += 2
            continue

        if a == "--tz":
            if i + 1 >= len(argv):
                raise SystemExit("missing value for --tz (ex: --tz Asia/Seoul)")
            out["tz"] = argv[i + 1].strip()
            i += 2
            continue

        if a == "--all-leagues":
            out["all_leagues"] = True
            i += 1
            continue

        if a == "--no-events":
            out["fetch_events"] = False
            i += 1
            continue

        raise SystemExit(f"unknown arg: {a}")

    if not out["date"]:
        raise SystemExit("required: --date YYYY-MM-DD")

    # date validation
    try:
        dt.date.fromisoformat(out["date"])
    except Exception:
        raise SystemExit(f"invalid --date: {out['date']} (expected YYYY-MM-DD)")

    return out


def _kst_like_tz_to_offset(tz: str) -> dt.tzinfo:
    """
    외부 라이브러리 없이, 최소한 'Asia/Seoul' 같은 고정 오프셋은 처리.
    - Render 환경에 zoneinfo가 있을 수도 있으니 우선 사용 시도.
    """
    tz = (tz or "").strip()
    if not tz:
        return dt.timezone.utc

    # Python 3.9+ zoneinfo (있으면 사용)
    try:
        from zoneinfo import ZoneInfo  # type: ignore
        return ZoneInfo(tz)
    except Exception:
        pass

    # fallback: Asia/Seoul = UTC+9
    if tz in ("Asia/Seoul", "KST", "UTC+9", "UTC+09:00"):
        return dt.timezone(dt.timedelta(hours=9))

    # 마지막 fallback: UTC
    return dt.timezone.utc


def _local_day_utc_range(date_str: str, tz_name: str) -> Tuple[dt.datetime, dt.datetime]:
    """
    로컬 타임존 기준 날짜(00:00~24:00)를 UTC 구간으로 변환.
    """
    tzinfo = _kst_like_tz_to_offset(tz_name)
    d = dt.date.fromisoformat(date_str)
    start_local = dt.datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=tzinfo)
    end_local = start_local + dt.timedelta(days=1)
    start_utc = start_local.astimezone(dt.timezone.utc)
    end_utc = end_local.astimezone(dt.timezone.utc)
    return start_utc, end_utc


def _pick_leagues(all_leagues: bool) -> Set[int]:
    if all_leagues:
        return set()
    return set(hockey_live_leagues() or [])


def _find_existing_game_ids_for_day(
    *,
    start_utc: dt.datetime,
    end_utc: dt.datetime,
    leagues_filter: Set[int],
) -> List[int]:
    """
    해당 UTC 구간에 속하는 game_date들의 게임 id 목록을 찾는다.
    (리그 필터가 있으면 league_id도 제한)
    """
    if leagues_filter:
        rows = hockey_fetch_all(
            """
            SELECT id
            FROM hockey_games
            WHERE game_date >= %s AND game_date < %s
              AND league_id = ANY(%s)
            """,
            (start_utc, end_utc, list(leagues_filter)),
        )
    else:
        rows = hockey_fetch_all(
            """
            SELECT id
            FROM hockey_games
            WHERE game_date >= %s AND game_date < %s
            """,
            (start_utc, end_utc),
        )

    out: List[int] = []
    for r in rows or []:
        try:
            out.append(int(r["id"]))
        except Exception:
            pass
    return out


def _delete_games_and_related(game_ids: List[int]) -> None:
    """
    game_ids에 해당하는 이벤트/폴링상태/게임을 "깨끗하게" 삭제
    """
    if not game_ids:
        return

    # events
    hockey_execute(
        "DELETE FROM hockey_game_events WHERE game_id = ANY(%s)",
        (game_ids,),
    )

    # poll state
    hockey_execute(
        "DELETE FROM hockey_live_poll_state WHERE game_id = ANY(%s)",
        (game_ids,),
    )

    # games
    hockey_execute(
        "DELETE FROM hockey_games WHERE id = ANY(%s)",
        (game_ids,),
    )


def _backfill_from_api(
    *,
    date_str: str,
    tz_name: str,
    leagues_filter: Set[int],
    fetch_events: bool,
) -> Tuple[int, int, int]:
    """
    API에서 해당 날짜 게임을 다시 받아 삽입(+옵션 events)
    returns: (games_seen, games_inserted, events_inserted)
    """
    payload = _get("/games", {"date": date_str, "timezone": tz_name})
    resp = payload.get("response") if isinstance(payload, dict) else None
    if not isinstance(resp, list):
        raise RuntimeError(f"unexpected /games response shape: {type(resp).__name__}")

    games_seen = 0
    games_ins = 0
    events_ins = 0

    for item in resp:
        if not isinstance(item, dict):
            continue

        league = item.get("league") if isinstance(item.get("league"), dict) else {}
        lid = int(league.get("id") or 0)
        if leagues_filter and lid not in leagues_filter:
            continue

        season = int(league.get("season") or 0)

        gid = upsert_game(item, lid, season)
        if not gid:
            continue

        games_seen += 1
        games_ins += 1

        if not fetch_events:
            continue

        ev_payload = _get("/games/events", {"game": gid})
        ev_resp = ev_payload.get("response") if isinstance(ev_payload, dict) else None
        if isinstance(ev_resp, list) and ev_resp:
            ev_list = [x for x in ev_resp if isinstance(x, dict)]
            if ev_list:
                upsert_events(gid, ev_list)
                events_ins += len(ev_list)

    return games_seen, games_ins, events_ins


def main() -> None:
    args = _parse_args(sys.argv[1:])

    date_str: str = args["date"]
    tz_name: str = args["tz"]
    all_leagues: bool = args["all_leagues"]
    fetch_events: bool = args["fetch_events"]

    leagues_filter = _pick_leagues(all_leagues)

    start_utc, end_utc = _local_day_utc_range(date_str, tz_name)

    print(
        f"[clean-backfill] date={date_str} tz={tz_name} "
        f"utc_range=[{start_utc.isoformat()} ~ {end_utc.isoformat()}) "
        f"leagues_filter={'ALL' if not leagues_filter else sorted(leagues_filter)} "
        f"fetch_events={fetch_events}"
    )

    # event_key + unique index 보장 (events 재삽입 시 충돌 방지)
    ensure_event_key_migration()
    print("[ok] ensure_event_key_migration")

    # 1) 기존 데이터 찾고 삭제
    existing_ids = _find_existing_game_ids_for_day(
        start_utc=start_utc,
        end_utc=end_utc,
        leagues_filter=leagues_filter,
    )
    print(f"[step1] existing games in db: {len(existing_ids)}")

    if existing_ids:
        _delete_games_and_related(existing_ids)
        print(f"[step2] deleted games+events+poll_state: {len(existing_ids)}")
    else:
        print("[step2] nothing to delete")

    # 2) API로 재백필
    seen, ins, evs = _backfill_from_api(
        date_str=date_str,
        tz_name=tz_name,
        leagues_filter=leagues_filter,
        fetch_events=fetch_events,
    )
    print(f"[step3] api games inserted: seen={seen} inserted={ins} events_inserted={evs}")
    print("[done] clean backfill completed")


if __name__ == "__main__":
    main()
