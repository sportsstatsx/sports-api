# postmatch_backfill.py
#
# 역할:
# - 특정 날짜(date=YYYY-MM-DD)의 FINISHED 경기들에 대해 "한 번만" 무거운 데이터 전체 백필
#   * /fixtures          → fixtures/matches upsert + match_fixtures_raw 저장
#   * /fixtures/events   → match_events / match_events_raw
#   * /fixtures/lineups  → match_lineups
#   * /fixtures/statistics → match_team_stats
#   * /fixtures/players  → match_player_stats
#
# 특징:
# - 이미 백필된 경기(match_events에 row 존재)는 스킵
# - LIVE_LEAGUES env 에 포함된 리그만 대상
# - 스키마 변경 없음

import os
import sys
import json
import time
import datetime as dt
from typing import Any, Dict, List, Optional

import requests

from db import fetch_one, fetch_all, execute

BASE_URL = "https://v3.football.api-sports.io"


# ─────────────────────────────────────
#  ENV / 유틸
# ─────────────────────────────────────

def _get_api_key() -> str:
    key = (
        os.environ.get("APIFOOTBALL_KEY")
        or os.environ.get("API_KEY")
        or ""
    )
    if not key:
        raise RuntimeError("API key missing: set APIFOOTBALL_KEY (or API_FOOTBALL_KEY / API_KEY)")
    return key


def _get_headers() -> Dict[str, str]:
    return {"x-apisports-key": _get_api_key()}


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def parse_live_leagues(s: str) -> List[int]:
    out: List[int] = []
    for tok in (s or "").split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            out.append(int(tok))
        except ValueError:
            print(f"[WARN] LIVE_LEAGUES token invalid: {tok!r}", file=sys.stderr)
    return sorted(set(out))


def get_target_date() -> str:
    # 우선순위: ENV TARGET_DATE > CLI arg1 > 오늘(UTC)
    env_date = (os.environ.get("TARGET_DATE") or "").strip()
    if env_date:
        return env_date
    if len(sys.argv) >= 2 and sys.argv[1].strip():
        return sys.argv[1].strip()
    return now_utc().strftime("%Y-%m-%d")

def get_target_dates() -> List[str]:
    # 우선순위: ENV TARGET_DATE > CLI arg1 > 최근 N일(기본 7일)
    env_date = (os.environ.get("TARGET_DATE") or "").strip()
    if env_date:
        return [env_date]
    if len(sys.argv) >= 2 and sys.argv[1].strip():
        return [sys.argv[1].strip()]

    days = int((os.environ.get("BACKFILL_DAYS") or "7").strip())
    if days <= 0:
        days = 7

    end = now_utc().date()
    start = end - dt.timedelta(days=days - 1)
    out: List[str] = []
    cur = start
    while cur <= end:
        out.append(cur.strftime("%Y-%m-%d"))
        cur += dt.timedelta(days=1)
    return out



def _safe_get(path: str, *, params: Dict[str, Any], timeout: int = 25, max_retry: int = 4) -> Dict[str, Any]:
    url = f"{BASE_URL}{path}"
    last_err: Optional[Exception] = None
    for i in range(max_retry):
        try:
            resp = requests.get(url, headers=_get_headers(), params=params, timeout=timeout)
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(0.7 * (i + 1))
                continue
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, dict):
                raise RuntimeError("API response is not a dict")
            return data
        except Exception as e:
            last_err = e
            time.sleep(0.7 * (i + 1))
            continue
    raise RuntimeError(f"API request failed after retries: {last_err}")


def _status_group_from_short(short: Optional[str]) -> str:
    s = (short or "").upper()
    if s in ("FT", "AET", "PEN"):
        return "FINISHED"
    if s in ("NS", "TBD"):
        return "SCHEDULED"
    if s in ("PST", "CANC", "ABD", "AWD", "WO"):
        return "CANCELLED"
    return "LIVE"


def _extract_fixture_basic(fx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    fixture = fx.get("fixture") or {}
    league = fx.get("league") or {}
    teams = fx.get("teams") or {}

    fid = fixture.get("id")
    if fid is None:
        return None

    status = fixture.get("status") or {}
    status_short = status.get("short")
    status_group = _status_group_from_short(status_short)

    season = league.get("season")
    league_id = league.get("id")

    home_id = (teams.get("home") or {}).get("id")
    away_id = (teams.get("away") or {}).get("id")

    return {
        "fixture_id": int(fid),
        "league_id": int(league_id) if league_id is not None else None,
        "season": int(season) if season is not None else None,
        "status_short": status_short,
        "status_group": status_group,
        "home_id": int(home_id) if home_id is not None else None,
        "away_id": int(away_id) if away_id is not None else None,
    }


# ─────────────────────────────────────
#  API fetchers
# ─────────────────────────────────────

def fetch_league_seasons(league_id: int) -> List[Dict[str, Any]]:
    data = _safe_get("/leagues", params={"id": league_id})
    resp = data.get("response") or []
    if not resp or not isinstance(resp, list) or not isinstance(resp[0], dict):
        return []
    seasons = (resp[0].get("seasons") or [])
    return [s for s in seasons if isinstance(s, dict)]


def pick_season_for_date(league_id: int, date_str: str) -> Optional[int]:
    # date_str: "YYYY-MM-DD"
    seasons = fetch_league_seasons(league_id)
    if not seasons:
        return None

    # 1) start <= date <= end 범위 매칭 우선
    for s in seasons:
        start = s.get("start")
        end = s.get("end")
        year = s.get("year")
        if start and end and year is not None and start <= date_str <= end:
            try:
                return int(year)
            except Exception:
                return None

    # 2) 범위 매칭 실패 시 current=True 시즌으로 fallback
    for s in seasons:
        if s.get("current") is True and s.get("year") is not None:
            try:
                return int(s["year"])
            except Exception:
                return None

    return None


def fetch_fixtures_from_api(league_id: int, date_str: str, season: Optional[int] = None) -> List[Dict[str, Any]]:
    if season is None:
        season = pick_season_for_date(league_id, date_str)

    params: Dict[str, Any] = {"league": league_id, "date": date_str}
    if season is not None:
        params["season"] = int(season)

    data = _safe_get("/fixtures", params=params)
    rows = data.get("response", []) or []
    return [r for r in rows if isinstance(r, dict)]



def fetch_fixture_by_id(fixture_id: int) -> Optional[Dict[str, Any]]:
    data = _safe_get("/fixtures", params={"id": fixture_id})
    rows = data.get("response", []) or []
    for r in rows:
        if isinstance(r, dict):
            return r
    return None


def fetch_events_from_api(fixture_id: int) -> List[Dict[str, Any]]:
    data = _safe_get("/fixtures/events", params={"fixture": fixture_id})
    rows = data.get("response", []) or []
    return [r for r in rows if isinstance(r, dict)]


def fetch_lineups_from_api(fixture_id: int) -> List[Dict[str, Any]]:
    data = _safe_get("/fixtures/lineups", params={"fixture": fixture_id})
    rows = data.get("response", []) or []
    return [r for r in rows if isinstance(r, dict)]


def fetch_team_stats_from_api(fixture_id: int) -> List[Dict[str, Any]]:
    data = _safe_get("/fixtures/statistics", params={"fixture": fixture_id})
    rows = data.get("response", []) or []
    return [r for r in rows if isinstance(r, dict)]


def fetch_player_stats_from_api(fixture_id: int) -> List[Dict[str, Any]]:
    data = _safe_get("/fixtures/players", params={"fixture": fixture_id})
    rows = data.get("response", []) or []
    return [r for r in rows if isinstance(r, dict)]


# ─────────────────────────────────────
#  DB upserts (스키마 그대로)
# ─────────────────────────────────────

def upsert_match_fixtures_raw(fixture_id: int, fixture_obj: Dict[str, Any]) -> None:
    raw = json.dumps(fixture_obj, ensure_ascii=False)
    execute(
        """
        INSERT INTO match_fixtures_raw (fixture_id, data_json, fetched_at, updated_at)
        VALUES (%s, %s, now(), now())
        ON CONFLICT (fixture_id) DO UPDATE
        SET data_json = EXCLUDED.data_json,
            fetched_at = now(),
            updated_at = now()
        """,
        (fixture_id, raw),
    )


def upsert_fixture_row(fx: Dict[str, Any], league_id: int, season: int) -> None:
    fixture_block = fx.get("fixture") or {}
    fid = fixture_block.get("id")
    if fid is None:
        return

    date_utc = fixture_block.get("date")
    status_short = (fixture_block.get("status") or {}).get("short")
    status_group = _status_group_from_short(status_short)

    execute(
        """
        INSERT INTO fixtures (fixture_id, league_id, season, date_utc, status, status_group)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (fixture_id) DO UPDATE SET
            league_id     = EXCLUDED.league_id,
            season        = EXCLUDED.season,
            date_utc      = EXCLUDED.date_utc,
            status        = EXCLUDED.status,
            status_group  = EXCLUDED.status_group
        """,
        (int(fid), int(league_id), int(season), date_utc, status_short, status_group),
    )


def upsert_match_row(fx: Dict[str, Any], league_id: int, season: int) -> None:
    fixture_block = fx.get("fixture") or {}
    teams_block = fx.get("teams") or {}
    goals_block = fx.get("goals") or {}
    score_block = fx.get("score") or {}

    fid = fixture_block.get("id")
    if fid is None:
        return

    date_utc = fixture_block.get("date")
    st = fixture_block.get("status") or {}
    status_short = st.get("short")
    status_long = st.get("long")
    status_elapsed = st.get("elapsed")
    status_extra = st.get("extra")
    status_group = _status_group_from_short(status_short)

    home_id = (teams_block.get("home") or {}).get("id")
    away_id = (teams_block.get("away") or {}).get("id")
    if home_id is None or away_id is None:
        return

    home_ft = goals_block.get("home")
    away_ft = goals_block.get("away")

    ht = score_block.get("halftime") or {}
    home_ht = ht.get("home")
    away_ht = ht.get("away")

    elapsed = status_elapsed

    referee = fixture_block.get("referee")
    fixture_timezone = fixture_block.get("timezone")
    fixture_timestamp = fixture_block.get("timestamp")
    venue = fixture_block.get("venue") or {}
    venue_id = venue.get("id")
    venue_name = venue.get("name")
    venue_city = venue.get("city")

    league_round = (fx.get("league") or {}).get("round")

    execute(
        """
        INSERT INTO matches (
            fixture_id, league_id, season, date_utc, status, status_group,
            home_id, away_id,
            home_ft, away_ft, elapsed,
            home_ht, away_ht,
            referee, fixture_timezone, fixture_timestamp,
            status_short, status_long, status_elapsed, status_extra,
            venue_id, venue_name, venue_city, league_round
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (fixture_id) DO UPDATE SET
            league_id = EXCLUDED.league_id,
            season = EXCLUDED.season,
            date_utc = EXCLUDED.date_utc,
            status = EXCLUDED.status,
            status_group = EXCLUDED.status_group,
            home_id = EXCLUDED.home_id,
            away_id = EXCLUDED.away_id,
            home_ft = EXCLUDED.home_ft,
            away_ft = EXCLUDED.away_ft,
            elapsed = EXCLUDED.elapsed,
            home_ht = EXCLUDED.home_ht,
            away_ht = EXCLUDED.away_ht,
            referee = EXCLUDED.referee,
            fixture_timezone = EXCLUDED.fixture_timezone,
            fixture_timestamp = EXCLUDED.fixture_timestamp,
            status_short = EXCLUDED.status_short,
            status_long = EXCLUDED.status_long,
            status_elapsed = EXCLUDED.status_elapsed,
            status_extra = EXCLUDED.status_extra,
            venue_id = EXCLUDED.venue_id,
            venue_name = EXCLUDED.venue_name,
            venue_city = EXCLUDED.venue_city,
            league_round = EXCLUDED.league_round
        """,
        (
            int(fid), int(league_id), int(season), date_utc, status_short, status_group,
            int(home_id), int(away_id),
            home_ft, away_ft, elapsed,
            home_ht, away_ht,
            referee, fixture_timezone, fixture_timestamp,
            status_short, status_long, status_elapsed, status_extra,
            venue_id, venue_name, venue_city, league_round,
        ),
    )


def upsert_match_events_raw(fixture_id: int, events: List[Dict[str, Any]]) -> None:
    execute(
        """
        INSERT INTO match_events_raw (fixture_id, data_json)
        VALUES (%s, %s)
        ON CONFLICT (fixture_id) DO UPDATE SET
            data_json = EXCLUDED.data_json
        """,
        (fixture_id, json.dumps(events, ensure_ascii=False)),
    )


def upsert_match_events(fixture_id: int, events: List[Dict[str, Any]]) -> None:
    # postmatch는 최종본 목적 → fixture 단위 통째 덮어쓰기
    execute("DELETE FROM match_events WHERE fixture_id = %s", (fixture_id,))

    for ev in events:
        if not isinstance(ev, dict):
            continue

        team_id = (ev.get("team") or {}).get("id")
        player_id = (ev.get("player") or {}).get("id")
        assist_id = (ev.get("assist") or {}).get("id")
        assist_name = (ev.get("assist") or {}).get("name")

        type_ = ev.get("type") or ""
        if not type_:
            continue

        detail = ev.get("detail")
        time_block = ev.get("time") or {}
        minute = time_block.get("elapsed")
        extra = time_block.get("extra")

        # minute NOT NULL
        if minute is None:
            continue

        # Subst: 들어온 선수는 assist쪽에 실리는 경우가 많음
        player_in_id = None
        player_in_name = None
        if str(type_).lower() == "subst":
            player_in_id = assist_id
            player_in_name = assist_name

        execute(
            """
            INSERT INTO match_events (
                fixture_id,
                team_id,
                player_id,
                type,
                detail,
                minute,
                extra,
                assist_player_id,
                assist_name,
                player_in_id,
                player_in_name
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                fixture_id,
                team_id,
                player_id,
                type_,
                detail,
                int(minute),
                extra,
                assist_id,
                assist_name,
                player_in_id,
                player_in_name,
            ),
        )


def upsert_match_lineups(fixture_id: int, lineups: List[Dict[str, Any]]) -> None:
    execute("DELETE FROM match_lineups WHERE fixture_id = %s", (fixture_id,))
    updated_utc = now_utc().isoformat()

    for row in lineups:
        if not isinstance(row, dict):
            continue
        team_id = (row.get("team") or {}).get("id")
        if team_id is None:
            continue
        execute(
            """
            INSERT INTO match_lineups (fixture_id, team_id, data_json, updated_utc)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (fixture_id, team_id) DO UPDATE SET
                data_json = EXCLUDED.data_json,
                updated_utc = EXCLUDED.updated_utc
            """,
            (fixture_id, int(team_id), json.dumps(row, ensure_ascii=False), updated_utc),
        )


def upsert_match_team_stats(fixture_id: int, stats: List[Dict[str, Any]]) -> None:
    execute("DELETE FROM match_team_stats WHERE fixture_id = %s", (fixture_id,))

    for row in stats:
        if not isinstance(row, dict):
            continue
        team_id = (row.get("team") or {}).get("id")
        if team_id is None:
            continue
        stat_list = row.get("statistics") or []
        for s in stat_list:
            if not isinstance(s, dict):
                continue
            name = s.get("type")
            if not name:
                continue
            value = s.get("value")
            value_str = None if value is None else str(value)

            execute(
                """
                INSERT INTO match_team_stats (fixture_id, team_id, name, value)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (fixture_id, team_id, name) DO UPDATE SET
                    value = EXCLUDED.value
                """,
                (fixture_id, int(team_id), str(name), value_str),
            )


def upsert_match_player_stats(fixture_id: int, players_stats: List[Dict[str, Any]]) -> None:
    execute("DELETE FROM match_player_stats WHERE fixture_id = %s", (fixture_id,))

    for team_block in players_stats:
        if not isinstance(team_block, dict):
            continue
        players_list = team_block.get("players") or []
        for p in players_list:
            if not isinstance(p, dict):
                continue
            player_id = (p.get("player") or {}).get("id")
            if player_id is None:
                continue
            execute(
                """
                INSERT INTO match_player_stats (fixture_id, player_id, data_json)
                VALUES (%s, %s, %s)
                ON CONFLICT (fixture_id, player_id) DO UPDATE SET
                    data_json = EXCLUDED.data_json
                """,
                (fixture_id, int(player_id), json.dumps(p, ensure_ascii=False)),
            )


# ─────────────────────────────────────
#  이미 백필된 경기인지 체크
# ─────────────────────────────────────

def _has_any_row(table: str, fixture_id: int) -> bool:
    row = fetch_one(
        f"""
        SELECT 1
        FROM {table}
        WHERE fixture_id = %s
        LIMIT 1
        """,
        (fixture_id,),
    )
    return row is not None


def has_match_events(fixture_id: int) -> bool:
    return _has_any_row("match_events", fixture_id)


def has_lineups(fixture_id: int) -> bool:
    return _has_any_row("match_lineups", fixture_id)


def has_team_stats(fixture_id: int) -> bool:
    return _has_any_row("match_team_stats", fixture_id)


def has_player_stats(fixture_id: int) -> bool:
    return _has_any_row("match_player_stats", fixture_id)



# ─────────────────────────────────────
#  한 경기 상세 백필
# ─────────────────────────────────────

def backfill_postmatch_for_fixture(
    fixture_id: int,
    *,
    do_events: bool,
    do_lineups: bool,
    do_team_stats: bool,
    do_player_stats: bool,
) -> None:
    if do_events:
        try:
            events = fetch_events_from_api(fixture_id)
        except Exception as e:
            print(f"    ! fixture {fixture_id}: events 호출 에러: {e}", file=sys.stderr)
            events = []
        if events:
            upsert_match_events(fixture_id, events)
            upsert_match_events_raw(fixture_id, events)

    if do_lineups:
        try:
            lineups = fetch_lineups_from_api(fixture_id)
        except Exception as e:
            print(f"    ! fixture {fixture_id}: lineups 호출 에러: {e}", file=sys.stderr)
            lineups = []
        if lineups:
            upsert_match_lineups(fixture_id, lineups)

    if do_team_stats:
        try:
            stats = fetch_team_stats_from_api(fixture_id)
        except Exception as e:
            print(f"    ! fixture {fixture_id}: statistics 호출 에러: {e}", file=sys.stderr)
            stats = []
        if stats:
            upsert_match_team_stats(fixture_id, stats)

    if do_player_stats:
        try:
            players_stats = fetch_player_stats_from_api(fixture_id)
        except Exception as e:
            print(f"    ! fixture {fixture_id}: players 호출 에러: {e}", file=sys.stderr)
            players_stats = []
        if players_stats:
            upsert_match_player_stats(fixture_id, players_stats)



# ─────────────────────────────────────
#  엔트리
# ─────────────────────────────────────

def main() -> None:
    target_dates = get_target_dates()
    live_leagues = parse_live_leagues(os.environ.get("LIVE_LEAGUES", ""))

    if not live_leagues:
        print("[postmatch_backfill] LIVE_LEAGUES env 가 비어있습니다. 종료.", file=sys.stderr)
        return

    force = (os.environ.get("FORCE_BACKFILL") or "").strip().lower() in ("1", "true", "yes", "y")
    today_str = now_utc().strftime("%Y-%m-%d")
    print(f"[postmatch_backfill] dates={target_dates}, today={today_str}, leagues={live_leagues}, force={force}")

    total_new = 0
    total_skipped = 0

    for target_date in target_dates:
        for lid in live_leagues:
            try:
                season_guess = pick_season_for_date(lid, target_date)
                fixtures = fetch_fixtures_from_api(lid, target_date, season_guess)
                print(f"  - date={target_date} league {lid}: season={season_guess} fixtures={len(fixtures)}")

                for fx in fixtures:
                    basic = _extract_fixture_basic(fx)
                    if basic is None:
                        continue
                    if basic.get("status_group") != "FINISHED":
                        continue

                    fixture_id = basic["fixture_id"]

                    season = basic.get("season") or season_guess
                    if season is None:
                        continue

                    fx_full = fetch_fixture_by_id(fixture_id) or fx

                    try:
                        upsert_match_fixtures_raw(fixture_id, fx_full)
                    except Exception as raw_e:
                        print(f"    ! fixture {fixture_id}: match_fixtures_raw 저장 실패: {raw_e}", file=sys.stderr)

                    upsert_fixture_row(fx_full, lid, int(season))
                    upsert_match_row(fx_full, lid, int(season))

                    need_events = force or (not has_match_events(fixture_id))
                    need_lineups = force or (not has_lineups(fixture_id))
                    need_team_stats = force or (not has_team_stats(fixture_id))
                    need_player_stats = force or (not has_player_stats(fixture_id))

                    if not (need_events or need_lineups or need_team_stats or need_player_stats):
                        total_skipped += 1
                        continue

                    todo = []
                    if need_events:
                        todo.append("events")
                    if need_lineups:
                        todo.append("lineups")
                    if need_team_stats:
                        todo.append("team_stats")
                    if need_player_stats:
                        todo.append("player_stats")

                    print(f"    * fixture {fixture_id}: backfill={'+'.join(todo)}")
                    backfill_postmatch_for_fixture(
                        fixture_id,
                        do_events=need_events,
                        do_lineups=need_lineups,
                        do_team_stats=need_team_stats,
                        do_player_stats=need_player_stats,
                    )
                    total_new += 1

            except Exception as e:
                print(f"  ! date={target_date} league {lid} 처리 중 에러: {e}", file=sys.stderr)

    print(f"[postmatch_backfill] 완료. 신규={total_new}, 스킵={total_skipped}")



if __name__ == "__main__":
    main()
