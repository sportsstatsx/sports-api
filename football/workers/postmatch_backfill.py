# postmatch_backfill.py
#
# 단독 실행 버전 (외부 live_fixtures_* 의존 제거)
# - FINISHED 경기 대상: /fixtures → fixtures/matches upsert + match_fixtures_raw 저장
# - /fixtures/events   → match_events + match_events_raw
# - /fixtures/lineups  → match_lineups
# - /fixtures/statistics → match_team_stats
# - /fixtures/players  → match_player_stats
#
# 환경변수:
#   - APIFOOTBALL_KEY (또는 API_FOOTBALL_KEY / API_KEY)  : Api-Football 키
#   - LIVE_LEAGUES    : 예) "39,140,135"
#   - TARGET_DATE (옵션) : "YYYY-MM-DD" (없으면 UTC 오늘)
#
# 주의:
# - match_events.id 가 bigserial(기본값 자동 생성)이어야 함 (기존 코드들도 동일 전제)

import os
import sys
import json
import datetime as dt
from typing import Any, Dict, List, Optional

import requests

from db import fetch_one, execute


BASE_URL = "https://v3.football.api-sports.io"


def _api_key() -> str:
    return (
        os.environ.get("APIFOOTBALL_KEY")
        or os.environ.get("API_FOOTBALL_KEY")
        or os.environ.get("API_KEY")
        or ""
    )


def _headers() -> Dict[str, str]:
    key = _api_key()
    if not key:
        raise RuntimeError("APIFOOTBALL_KEY(또는 API_FOOTBALL_KEY/API_KEY) 환경변수가 비어있습니다.")
    return {"x-apisports-key": key}


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def get_target_date() -> str:
    d = (os.environ.get("TARGET_DATE") or "").strip()
    if d:
        return d
    return now_utc().strftime("%Y-%m-%d")


def parse_live_leagues(env_value: str) -> List[int]:
    tokens: List[str] = []
    for part in (env_value or "").replace("\n", ",").replace(" ", ",").split(","):
        p = part.strip()
        if p:
            tokens.append(p)
    out: List[int] = []
    for t in tokens:
        try:
            out.append(int(t))
        except ValueError:
            continue
    return sorted(set(out))


def _status_group(status_short: str, status_long: str = "") -> str:
    s = (status_short or "").upper()
    l = (status_long or "").lower()

    if s in ("FT", "AET", "PEN"):
        return "FINISHED"
    if s in ("NS", "TBD"):
        return "UPCOMING"
    if s in ("1H", "HT", "2H", "ET", "BT", "P", "LIVE"):
        return "INPLAY"
    if s in ("PST", "CANC", "ABD", "SUSP", "INT"):
        # 필요하면 별도 그룹을 만들 수도 있지만, 기존 스키마 유지 차원에서 UNKNOWN 처리
        return "UNKNOWN"
    if "finished" in l:
        return "FINISHED"
    if "not started" in l:
        return "UPCOMING"
    if "in play" in l or "live" in l:
        return "INPLAY"
    return "UNKNOWN"


def _extract_fixture_basic(fx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(fx, dict):
        return None

    fixture_block = fx.get("fixture") or {}
    league_block = fx.get("league") or {}
    status_block = fixture_block.get("status") or {}

    fixture_id = fixture_block.get("id")
    if fixture_id is None:
        return None

    league_id = league_block.get("id")
    season = league_block.get("season")

    date_utc = fixture_block.get("date")  # ISO 문자열 (API가 주는 값 그대로 TEXT 저장)
    status_short = status_block.get("short") or ""
    status_long = status_block.get("long") or ""
    elapsed = status_block.get("elapsed")
    extra = status_block.get("extra")

    return {
        "fixture_id": int(fixture_id),
        "league_id": int(league_id) if league_id is not None else None,
        "season": int(season) if season is not None else None,
        "date_utc": date_utc,
        "status": status_short,
        "status_short": status_short,
        "status_long": status_long,
        "status_elapsed": elapsed,
        "status_extra": extra,
        "status_group": _status_group(status_short, status_long),
        "elapsed": elapsed,
    }


# ─────────────────────────────────────
#  API fetchers
# ─────────────────────────────────────

def fetch_fixtures_from_api(league_id: int, date_str: str) -> List[Dict[str, Any]]:
    url = f"{BASE_URL}/fixtures"
    params = {"league": league_id, "date": date_str}
    resp = requests.get(url, headers=_headers(), params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    return data.get("response") or []


def fetch_events_from_api(fixture_id: int) -> List[Dict[str, Any]]:
    url = f"{BASE_URL}/fixtures/events"
    params = {"fixture": fixture_id}
    resp = requests.get(url, headers=_headers(), params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    return data.get("response") or []


def fetch_lineups_from_api(fixture_id: int) -> List[Dict[str, Any]]:
    url = f"{BASE_URL}/fixtures/lineups"
    params = {"fixture": fixture_id}
    resp = requests.get(url, headers=_headers(), params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    return data.get("response") or []


def fetch_team_stats_from_api(fixture_id: int) -> List[Dict[str, Any]]:
    url = f"{BASE_URL}/fixtures/statistics"
    params = {"fixture": fixture_id}
    resp = requests.get(url, headers=_headers(), params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    return data.get("response") or []


def fetch_player_stats_from_api(fixture_id: int) -> List[Dict[str, Any]]:
    url = f"{BASE_URL}/fixtures/players"
    params = {"fixture": fixture_id}
    resp = requests.get(url, headers=_headers(), params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    return data.get("response") or []


# ─────────────────────────────────────
#  DB upserts (스키마 유지)
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
    basic = _extract_fixture_basic(fx)
    if basic is None:
        return
    fixture_id = basic["fixture_id"]
    date_utc = basic.get("date_utc")
    status = basic.get("status")
    status_group = basic.get("status_group")

    execute(
        """
        INSERT INTO fixtures (fixture_id, league_id, season, date_utc, status, status_group)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (fixture_id) DO UPDATE SET
            league_id     = EXCLUDED.league_id,
            season        = EXCLUDED.season,
            date_utc      = EXCLUDED.date_utc,
            status        = EXCLUDED.status,
            status_group  = EXCLUDED.status_group
        """,
        (fixture_id, league_id, season, date_utc, status, status_group),
    )


def upsert_match_row(fx: Dict[str, Any], league_id: int, season: int) -> None:
    basic = _extract_fixture_basic(fx)
    if basic is None:
        return

    fixture_id = basic["fixture_id"]
    date_utc = basic.get("date_utc")
    status_short = basic.get("status_short") or ""
    status_group = basic.get("status_group") or "UNKNOWN"
    elapsed = basic.get("elapsed")

    teams_block = fx.get("teams") or {}
    home_team = teams_block.get("home") or {}
    away_team = teams_block.get("away") or {}
    home_id = home_team.get("id")
    away_id = away_team.get("id")
    if home_id is None or away_id is None:
        return

    goals_block = fx.get("goals") or {}
    home_ft = goals_block.get("home")
    away_ft = goals_block.get("away")

    fixture_block = fx.get("fixture") or {}
    league_block = fx.get("league") or {}
    status_block = fixture_block.get("status") or {}
    venue_block = fixture_block.get("venue") or {}
    score_block = fx.get("score") or {}
    ht_block = score_block.get("halftime") or {}

    referee = fixture_block.get("referee")
    fixture_timezone = fixture_block.get("timezone")
    fixture_timestamp = fixture_block.get("timestamp")

    status_long = status_block.get("long")
    status_elapsed = status_block.get("elapsed")
    status_extra = status_block.get("extra")

    home_ht = ht_block.get("home")
    away_ht = ht_block.get("away")

    venue_id = venue_block.get("id")
    venue_name = venue_block.get("name")
    venue_city = venue_block.get("city")

    league_round = league_block.get("round")

    # matches 스키마: 기존 컬럼들 그대로 유지 (너가 뽑아준 matches 컬럼 목록 기준)
    execute(
        """
        INSERT INTO matches (
            fixture_id,
            league_id,
            season,
            date_utc,
            status,
            status_group,
            home_id,
            away_id,
            home_ft,
            away_ft,
            elapsed,
            home_ht,
            away_ht,
            referee,
            fixture_timezone,
            fixture_timestamp,
            status_short,
            status_long,
            status_elapsed,
            status_extra,
            venue_id,
            venue_name,
            venue_city,
            league_round
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
            fixture_id,
            league_id,
            season,
            date_utc,
            status_short,
            status_group,
            int(home_id),
            int(away_id),
            home_ft,
            away_ft,
            elapsed,
            home_ht,
            away_ht,
            referee,
            fixture_timezone,
            fixture_timestamp,
            status_short,
            status_long,
            status_elapsed,
            status_extra,
            venue_id,
            venue_name,
            venue_city,
            league_round,
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
    # postmatch 백필은 "최종본"이 목적이므로, 한 경기 이벤트는 통째로 덮어쓰기
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

        # 스키마: minute NOT NULL
        if minute is None:
            continue

        # 교체(Subst)면, 들어온 선수는 assist 쪽에 실려오는 경우가 많아서 미러링
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
    # 기존 구현과 동일: fixture 단위로 통째로 갱신(DELETE 후 PK별 INSERT/UPSERT)
    # (schema: fixture_id + player_id PK) :contentReference[oaicite:3]{index=3}
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

def is_fixture_already_backfilled(fixture_id: int) -> bool:
    row = fetch_one(
        """
        SELECT 1
        FROM match_events
        WHERE fixture_id = %s
        LIMIT 1
        """,
        (fixture_id,),
    )
    return row is not None


def backfill_postmatch_for_fixture(fixture_id: int) -> None:
    # events
    try:
        events = fetch_events_from_api(fixture_id)
    except Exception as e:
        print(f"    ! fixture {fixture_id}: events 호출 에러: {e}", file=sys.stderr)
        events = []
    if events:
        upsert_match_events(fixture_id, events)
        upsert_match_events_raw(fixture_id, events)

    # lineups
    try:
        lineups = fetch_lineups_from_api(fixture_id)
    except Exception as e:
        print(f"    ! fixture {fixture_id}: lineups 호출 에러: {e}", file=sys.stderr)
        lineups = []
    if lineups:
        upsert_match_lineups(fixture_id, lineups)

    # team stats
    try:
        stats = fetch_team_stats_from_api(fixture_id)
    except Exception as e:
        print(f"    ! fixture {fixture_id}: statistics 호출 에러: {e}", file=sys.stderr)
        stats = []
    if stats:
        upsert_match_team_stats(fixture_id, stats)

    # player stats
    try:
        players_stats = fetch_player_stats_from_api(fixture_id)
    except Exception as e:
        print(f"    ! fixture {fixture_id}: players 호출 에러: {e}", file=sys.stderr)
        players_stats = []
    if players_stats:
        upsert_match_player_stats(fixture_id, players_stats)


def main() -> None:
    target_date = get_target_date()
    live_leagues = parse_live_leagues(os.environ.get("LIVE_LEAGUES", ""))

    if not live_leagues:
        print("[postmatch_backfill] LIVE_LEAGUES env 가 비어있습니다. 종료.", file=sys.stderr)
        return

    today_str = now_utc().strftime("%Y-%m-%d")
    print(f"[postmatch_backfill] date={target_date}, today={today_str}, leagues={live_leagues}")

    total_new = 0
    total_skipped = 0

    for lid in live_leagues:
        try:
            fixtures = fetch_fixtures_from_api(lid, target_date)
            print(f"  - league {lid}: fixtures={len(fixtures)}")

            for fx in fixtures:
                basic = _extract_fixture_basic(fx)
                if basic is None:
                    continue

                if basic.get("status_group") != "FINISHED":
                    continue

                fixture_id = basic["fixture_id"]
                league_id = lid
                season = basic.get("season")
                if season is None:
                    # season이 없으면 스키마상 matches.season NOT NULL이라 저장 불가
                    continue

                # /fixtures 원본 저장 (match_fixtures_raw)
                try:
                    upsert_match_fixtures_raw(fixture_id, fx)
                except Exception as raw_e:
                    print(f"    ! fixture {fixture_id}: match_fixtures_raw 저장 실패: {raw_e}", file=sys.stderr)

                # 기본 정보/스코어 최신화는 항상 수행
                upsert_fixture_row(fx, league_id, int(season))
                upsert_match_row(fx, league_id, int(season))

                # 이미 match_events 있으면 postmatch는 스킵
                if is_fixture_already_backfilled(fixture_id):
                    total_skipped += 1
                    continue

                print(f"    * fixture {fixture_id}: FINISHED → 상세 데이터 첫 백필")
                backfill_postmatch_for_fixture(fixture_id)
                total_new += 1

        except Exception as e:
            print(f"  ! league {lid} 처리 중 에러: {e}", file=sys.stderr)

    print(f"[postmatch_backfill] 완료. 신규={total_new}, 스킵={total_skipped}")


if __name__ == "__main__":
    main()
