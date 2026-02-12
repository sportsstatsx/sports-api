# src/football/workers/schedule_sync.py
#
# 역할(가벼운 스케줄 동기화):
# - 매일 KST 09:00에 실행하는 것을 전제로
# - KST 기준 "전날 09:00 ~ +7일" 구간에 해당하는 날짜들을 스캔
# - fixtures/matches/match_fixtures_raw 를 upsert해서
#   "미래 경기 일정이 DB에 항상 존재"하도록 보장
# - 팀 메타(teams) 누락도 자동 백필(⚠️ /teams는 단건 호출)

import os
import sys
import datetime as dt
from typing import Any, Dict, List, Optional, Set

# 기존 postmatch_backfill의 함수 재사용
from football.workers.postmatch_backfill import (
    fetch_fixtures_from_api,
    fetch_fixture_by_id,
    _extract_fixture_basic,
    upsert_match_fixtures_raw,
    upsert_fixture_row,
    upsert_match_row,
    now_utc,
    _safe_get,
    _upsert_team_from_api,
    parse_live_leagues,
)

# 레이스 방지 정책 공유
os.environ.setdefault("LIVE_WORKER_ROLE", "backfill")

KST = dt.timezone(dt.timedelta(hours=9))


def _kst_now() -> dt.datetime:
    return now_utc().astimezone(KST)


def _date_range_kst(start_dt: dt.datetime, end_dt: dt.datetime) -> List[str]:
    """
    start_dt~end_dt 구간에 걸친 KST 날짜 리스트(YYYY-MM-DD).
    """
    s = start_dt.astimezone(KST).date()
    e = end_dt.astimezone(KST).date()
    out = []
    cur = s
    while cur <= e:
        out.append(cur.strftime("%Y-%m-%d"))
        cur += dt.timedelta(days=1)
    return out


def _get_schedule_leagues() -> List[int]:
    """
    우선순위:
    - SCHEDULE_LEAGUES (없으면)
    - LIVE_LEAGUES
    """
    s = (os.environ.get("SCHEDULE_LEAGUES") or "").strip()
    if s:
        return parse_live_leagues(s)
    return parse_live_leagues(os.environ.get("LIVE_LEAGUES", ""))


def _backfill_missing_teams_single(ids: Set[int]) -> None:
    """
    teams 메타 누락을 /teams?id= 단건 호출로 채운다.
    (/teams 콤마 다건 호출은 API가 에러 내는 케이스가 있어 단건이 안전)
    """
    if not ids:
        return

    # DB에 이미 있는 팀은 제외
    from db import fetch_all

    id_list = sorted({int(x) for x in ids if x is not None})
    rows = fetch_all("SELECT id FROM teams WHERE id = ANY(%s)", (id_list,))
    existing = {int(r["id"]) for r in (rows or []) if r and r.get("id") is not None}
    missing = [x for x in id_list if x not in existing]

    if not missing:
        return

    print(f"[schedule_sync][meta] missing teams={len(missing)}")

    ok = 0
    fail = 0
    for tid in missing:
        try:
            data = _safe_get("/teams", params={"id": int(tid)})
            resp = data.get("response") or []
            if resp and isinstance(resp, list):
                for r in resp:
                    if isinstance(r, dict):
                        _upsert_team_from_api(r)
                        ok += 1
                        break
            else:
                fail += 1
        except Exception as e:
            fail += 1
            print(f"[schedule_sync][meta] team id={tid} failed: {e}", file=sys.stderr)

    print(f"[schedule_sync][meta] done ok={ok} fail={fail}")


def main() -> None:
    leagues = _get_schedule_leagues()
    if not leagues:
        print("[schedule_sync] no leagues (set SCHEDULE_LEAGUES or LIVE_LEAGUES)", file=sys.stderr)
        return

    # 실행 기준 시간(보통 KST 09:00 크론)
    now_kst = _kst_now()

    # 전날 09:00 ~ +7일 09:00
    start_kst = now_kst.replace(hour=9, minute=0, second=0, microsecond=0) - dt.timedelta(days=1)
    end_kst = now_kst.replace(hour=9, minute=0, second=0, microsecond=0) + dt.timedelta(days=7)

    dates = _date_range_kst(start_kst, end_kst)
    print(f"[schedule_sync] leagues={leagues}")
    print(f"[schedule_sync] window_kst={start_kst} ~ {end_kst} dates={dates[0]}..{dates[-1]} ({len(dates)}d)")

    seen_team_ids: Set[int] = set()
    total_fixtures = 0
    total_upserts = 0

    for dstr in dates:
        for lid in leagues:
            try:
                # 날짜별 fixtures 수집(가벼운 호출)
                fixtures = fetch_fixtures_from_api(int(lid), dstr, season=None)
                if not fixtures:
                    continue

                total_fixtures += len(fixtures)
                # fixture_id 목록
                ids = []
                for fx in fixtures:
                    b = _extract_fixture_basic(fx)
                    if not b:
                        continue
                    ids.append(int(b["fixture_id"]))

                # fixture_id별로 full 조회 후 DB 업서트(정확도/일관성)
                for fid in sorted(set(ids)):
                    fx_full = fetch_fixture_by_id(int(fid))
                    if not fx_full:
                        continue

                    b = _extract_fixture_basic(fx_full)
                    if not b:
                        continue

                    league_id = b.get("league_id")
                    season = b.get("season")
                    if league_id is None or season is None:
                        continue

                    # teams meta 후보 수집
                    if b.get("home_id") is not None:
                        seen_team_ids.add(int(b["home_id"]))
                    if b.get("away_id") is not None:
                        seen_team_ids.add(int(b["away_id"]))

                    # DB upsert (상태 무관: UPCOMING/INPLAY/FINISHED 모두)
                    ts = now_utc()
                    upsert_match_fixtures_raw(int(fid), fx_full, ts)
                    upsert_fixture_row(fx_full, int(league_id), int(season))
                    upsert_match_row(fx_full, int(league_id), int(season))
                    total_upserts += 1

            except Exception as e:
                print(f"[schedule_sync] date={dstr} league={lid} failed: {e}", file=sys.stderr)
                continue

    # 팀 메타 자동 채우기 (단건 /teams)
    try:
        _backfill_missing_teams_single(seen_team_ids)
    except Exception as e:
        print(f"[schedule_sync][meta] failed: {e}", file=sys.stderr)

    print(f"[schedule_sync] done total_fixtures_seen={total_fixtures} total_upserts={total_upserts}")

    # ✅ psycopg_pool 종료 경고 방지 (크론/짧은 프로세스에서 join 에러 방지)
    try:
        from db import close_pool
        close_pool()
    except Exception:
        pass



if __name__ == "__main__":
    main()
