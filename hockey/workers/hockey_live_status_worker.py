# hockey/workers/hockey_live_status_worker.py
from __future__ import annotations

import os
import time
import json
import zlib
import logging
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import requests

from hockey.hockey_db import hockey_execute, hockey_fetch_all, hockey_fetch_one
from hockey.workers.hockey_live_common import now_utc, hockey_live_leagues

log = logging.getLogger("hockey_live_status_worker")
logging.basicConfig(level=logging.INFO)

BASE_URL = "https://v1.hockey.api-sports.io"

def ensure_event_key_migration() -> None:
    """
    live worker가 먼저 뜨는 환경에서도 ON CONFLICT (game_id, event_key)가 안전하게 동작하도록
    DB에 event_key 컬럼 + 유니크 인덱스를 보장한다.
    """
    # 1) event_key 생성 컬럼
    hockey_execute(
        """
        ALTER TABLE hockey_game_events
        ADD COLUMN IF NOT EXISTS event_key TEXT
        GENERATED ALWAYS AS (
          lower(coalesce(type,'')) || '|' ||
          coalesce(period,'') || '|' ||
          coalesce(minute::text,'') || '|' ||
          coalesce(team_id::text,'') || '|' ||
          lower(coalesce(comment,'')) || '|' ||
          lower(coalesce(array_to_string(players,','),'')) || '|' ||
          lower(coalesce(array_to_string(assists,','),''))
        ) STORED;
        """
    )

    # 2) 유니크 인덱스
    hockey_execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_hockey_game_events_game_event_key
        ON hockey_game_events (game_id, event_key);
        """
    )



def _headers() -> Dict[str, str]:
    key = (os.getenv("APISPORTS_KEY") or os.getenv("API_SPORTS_KEY") or "").strip()
    if not key:
        raise RuntimeError("APISPORTS_KEY (or API_SPORTS_KEY) is not set")
    return {"x-apisports-key": key}


def _get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.get(
        f"{BASE_URL}{path}",
        headers=_headers(),
        params=params,
        timeout=45,
    )
    r.raise_for_status()
    return r.json()


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None or v == "":
            return None
        return int(v)
    except Exception:
        return None


def _safe_text(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _jdump(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)

def _int_env(name: str, default: int) -> int:
    v = (os.getenv(name) or "").strip()
    if not v:
        return default
    try:
        return int(v)
    except Exception:
        return default


def _float_env(name: str, default: float) -> float:
    v = (os.getenv(name) or "").strip()
    if not v:
        return default
    try:
        return float(v)
    except Exception:
        return default

def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def _select_latest_league_season_pairs(leagues: List[int]) -> List[Tuple[int, int]]:
    """
    standings 갱신 대상 (league_id, season) 추출:
    - hockey_games에 존재하는 시즌 중 league별 최신 season 1개
    """
    if not leagues:
        return []

    rows = hockey_fetch_all(
        """
        SELECT league_id, season, max(updated_at) AS last_u
        FROM hockey_games
        WHERE league_id = ANY(%s)
        GROUP BY league_id, season
        ORDER BY league_id ASC, season DESC
        """,
        (leagues,),
    )

    latest_by_league: Dict[int, int] = {}
    for r in rows:
        lid = int(r.get("league_id") or 0)
        season = int(r.get("season") or 0)
        if lid <= 0 or season <= 0:
            continue
        if lid not in latest_by_league:
            latest_by_league[lid] = season  # season DESC 정렬이므로 첫값이 최신

    return [(lid, latest_by_league[lid]) for lid in sorted(latest_by_league.keys())]


def _api_get_league_meta_by_id(league_id: int) -> Optional[Dict[str, Any]]:
    payload = _get("/leagues", {"id": league_id})
    resp = payload.get("response") if isinstance(payload, dict) else None
    if isinstance(resp, list) and resp and isinstance(resp[0], dict):
        return resp[0]
    return None


def _api_get_standings_payload(league_id: int, season: int) -> Dict[str, Any]:
    return _get("/standings", {"league": league_id, "season": season})


def _upsert_country(country: Dict[str, Any]) -> None:
    # schema: hockey_countries(id, name, code, flag, updated_at...)
    cid = _safe_int(country.get("id")) or 0
    if cid <= 0:
        return
    hockey_execute(
        """
        INSERT INTO hockey_countries (id, name, code, flag)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
          name = EXCLUDED.name,
          code = EXCLUDED.code,
          flag = EXCLUDED.flag
        """,
        (
            cid,
            _safe_text(country.get("name")),
            _safe_text(country.get("code")),
            _safe_text(country.get("flag")),
        ),
    )


def _upsert_league(league: Dict[str, Any], country_id: Optional[int]) -> None:
    # schema: hockey_leagues(id, name, type, logo, country_id, updated_at...)
    lid = _safe_int(league.get("id")) or 0
    if lid <= 0:
        return
    hockey_execute(
        """
        INSERT INTO hockey_leagues (id, name, type, logo, country_id)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
          name = EXCLUDED.name,
          type = EXCLUDED.type,
          logo = EXCLUDED.logo,
          country_id = EXCLUDED.country_id
        """,
        (
            lid,
            _safe_text(league.get("name")),
            _safe_text(league.get("type")),
            _safe_text(league.get("logo")),
            country_id,
        ),
    )

# ✅ hockey_league_seasons 컬럼명 자동 감지(스키마 차이 대응)
_LEAGUE_SEASONS_COLMAP: Optional[Dict[str, Optional[str]]] = None

def _detect_league_seasons_colmap() -> Dict[str, Optional[str]]:
    """
    DB의 hockey_league_seasons 컬럼명을 조회해서
    start/end/current 컬럼이 어떤 이름인지 매핑해준다.

    지원:
      - start_date or start
      - end_date or end
      - is_current or current
    """
    global _LEAGUE_SEASONS_COLMAP
    if _LEAGUE_SEASONS_COLMAP is not None:
        return _LEAGUE_SEASONS_COLMAP

    cols = set()
    try:
        rows = hockey_fetch_all(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public'
              AND table_name='hockey_league_seasons'
            """,
            (),
        )
        for r in rows:
            cn = (r.get("column_name") or "").strip()
            if cn:
                cols.add(cn)
    except Exception:
        # 조회 실패 시에는 코드 기본값을 우선 시도
        cols = {"start_date", "end_date", "is_current", "coverage_json"}

    start_col = "start_date" if "start_date" in cols else ("start" if "start" in cols else None)
    end_col = "end_date" if "end_date" in cols else ("end" if "end" in cols else None)
    current_col = "is_current" if "is_current" in cols else ("current" if "current" in cols else None)

    _LEAGUE_SEASONS_COLMAP = {
        "start": start_col,
        "end": end_col,
        "current": current_col,
    }
    return _LEAGUE_SEASONS_COLMAP



def _upsert_league_season(league_id: int, season_item: Dict[str, Any]) -> None:
    # schema: hockey_league_seasons(league_id, season, start/start_date, end/end_date, current/is_current, coverage_json, updated_at...)
    season = _safe_int(season_item.get("season")) or 0
    if league_id <= 0 or season <= 0:
        return

    colmap = _detect_league_seasons_colmap()
    start_col = colmap.get("start")
    end_col = colmap.get("end")
    current_col = colmap.get("current")

    start_val = _safe_text(season_item.get("start"))
    end_val = _safe_text(season_item.get("end"))
    current_val = (
        bool(season_item.get("current"))
        if season_item.get("current") is not None
        else None
    )
    coverage_json = _jdump(season_item.get("coverage") or {})

    # start/end 컬럼이 아예 없으면 최소 upsert만 하고 종료
    # (coverage_json만이라도 넣게)
    cols = ["league_id", "season"]
    vals = ["%s", "%s"]
    upd = []

    params: List[Any] = [league_id, season]

    if start_col:
        cols.append(start_col)
        vals.append("%s")
        upd.append(f"{start_col} = EXCLUDED.{start_col}")
        params.append(start_val)

    if end_col:
        cols.append(end_col)
        vals.append("%s")
        upd.append(f"{end_col} = EXCLUDED.{end_col}")
        params.append(end_val)

    if current_col:
        cols.append(current_col)
        vals.append("%s")
        upd.append(f"{current_col} = EXCLUDED.{current_col}")
        params.append(current_val)

    cols.append("coverage_json")
    vals.append("%s::jsonb")
    upd.append("coverage_json = EXCLUDED.coverage_json")
    params.append(coverage_json)

    sql = f"""
    INSERT INTO hockey_league_seasons ({", ".join(cols)})
    VALUES ({", ".join(vals)})
    ON CONFLICT (league_id, season) DO UPDATE SET
      {", ".join(upd)}
    """

    hockey_execute(sql, tuple(params))



def _upsert_team(team: Dict[str, Any]) -> None:
    # schema: hockey_teams(id, name, logo, updated_at...)
    tid = _safe_int(team.get("id")) or 0
    if tid <= 0:
        return
    hockey_execute(
        """
        INSERT INTO hockey_teams (id, name, logo)
        VALUES (%s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
          name = EXCLUDED.name,
          logo = EXCLUDED.logo
        """,
        (
            tid,
            _safe_text(team.get("name")),
            _safe_text(team.get("logo")),
        ),
    )


def _iter_standing_rows(obj: Any):
    # standings 응답이 list/list-of-list/dict 섞여서 오는 경우 방어적으로 flatten
    if isinstance(obj, list):
        for x in obj:
            yield from _iter_standing_rows(x)
    elif isinstance(obj, dict):
        if "standings" in obj:
            yield from _iter_standing_rows(obj.get("standings"))
        elif "team" in obj:
            yield obj


def _upsert_standings(league_id: int, season: int, payload: Dict[str, Any]) -> int:
    resp = payload.get("response") if isinstance(payload, dict) else None
    if not isinstance(resp, list) or not resp:
        return 0

    saved = 0

    # API는 response[0]에 league+standings가 들어있는 형태가 흔함
    for item in resp:
        if not isinstance(item, dict):
            continue

        league_obj = item.get("league") if isinstance(item.get("league"), dict) else {}
        stage = _safe_text(league_obj.get("stage")) or "REG"

        standings_obj = item.get("standings")
        for row in _iter_standing_rows(standings_obj):
            team = row.get("team") if isinstance(row.get("team"), dict) else {}
            _upsert_team(team)

            group_name = _safe_text(row.get("group")) or _safe_text(row.get("group_name")) or "overall"

            games = row.get("games") if isinstance(row.get("games"), dict) else {}
            win = row.get("win") if isinstance(row.get("win"), dict) else {}
            lose = row.get("lose") if isinstance(row.get("lose"), dict) else {}
            goals = row.get("goals") if isinstance(row.get("goals"), dict) else {}

            hockey_execute(
                """
                INSERT INTO hockey_standings (
                  league_id, season, stage, group_name,
                  position, team_id, team_name,
                  games_played,
                  win_total, win_pct, win_ot_total, win_ot_pct,
                  lose_total, lose_pct, lose_ot_total, lose_ot_pct,
                  goals_for, goals_against,
                  points, form, description,
                  raw_json
                )
                VALUES (
                  %s,%s,%s,%s,
                  %s,%s,%s,
                  %s,
                  %s,%s,%s,%s,
                  %s,%s,%s,%s,
                  %s,%s,
                  %s,%s,%s,
                  %s::jsonb
                )
                ON CONFLICT (league_id, season, stage, group_name, team_id)
                DO UPDATE SET
                  position = EXCLUDED.position,
                  team_name = EXCLUDED.team_name,
                  games_played = EXCLUDED.games_played,
                  win_total = EXCLUDED.win_total,
                  win_pct = EXCLUDED.win_pct,
                  win_ot_total = EXCLUDED.win_ot_total,
                  win_ot_pct = EXCLUDED.win_ot_pct,
                  lose_total = EXCLUDED.lose_total,
                  lose_pct = EXCLUDED.lose_pct,
                  lose_ot_total = EXCLUDED.lose_ot_total,
                  lose_ot_pct = EXCLUDED.lose_ot_pct,
                  goals_for = EXCLUDED.goals_for,
                  goals_against = EXCLUDED.goals_against,
                  points = EXCLUDED.points,
                  form = EXCLUDED.form,
                  description = EXCLUDED.description,
                  raw_json = EXCLUDED.raw_json
                """,
                (
                    league_id,
                    season,
                    stage,
                    group_name,

                    _safe_int(row.get("position")) or 0,
                    _safe_int(team.get("id")) or 0,
                    _safe_text(team.get("name")),

                    _safe_int(games.get("played")),

                    _safe_int(win.get("total")),
                    _safe_float(win.get("percentage")),
                    _safe_int(win.get("overtime")),
                    _safe_float(win.get("overtime_percentage")),

                    _safe_int(lose.get("total")),
                    _safe_float(lose.get("percentage")),
                    _safe_int(lose.get("overtime")),
                    _safe_float(lose.get("overtime_percentage")),

                    _safe_int(goals.get("for")),
                    _safe_int(goals.get("against")),

                    _safe_int(row.get("points")),
                    _safe_text(row.get("form")),
                    _safe_text(row.get("description")),
                    _jdump(row),
                ),
            )
            saved += 1

    return saved


def _refresh_meta_for_leagues(leagues: List[int], sleep_sec: float) -> None:
    for lid in leagues:
        try:
            item = _api_get_league_meta_by_id(int(lid))
            if not item:
                continue

            country = item.get("country") if isinstance(item.get("country"), dict) else {}
            league = item.get("league") if isinstance(item.get("league"), dict) else {}
            seasons = item.get("seasons") if isinstance(item.get("seasons"), list) else []

            # country -> league -> seasons
            _upsert_country(country)
            cid = _safe_int(country.get("id"))
            _upsert_league(league, cid)

            league_id = _safe_int(league.get("id")) or int(lid)
            for s in seasons:
                if isinstance(s, dict):
                    _upsert_league_season(league_id, s)

        except Exception as e:
            log.warning("meta refresh failed: league=%s err=%s", lid, e)

        if sleep_sec > 0:
            time.sleep(float(sleep_sec))


def _refresh_standings_for_latest_seasons(leagues: List[int], sleep_sec: float) -> None:
    pairs = _select_latest_league_season_pairs(leagues)
    if not pairs:
        return

    for (lid, season) in pairs:
        try:
            payload = _api_get_standings_payload(lid, season)
            saved = _upsert_standings(lid, season, payload)
            log.info("standings refreshed: league=%s season=%s rows=%s", lid, season, saved)
        except Exception as e:
            log.warning("standings refresh failed: league=%s season=%s err=%s", lid, season, e)

        if sleep_sec > 0:
            time.sleep(float(sleep_sec))



def _int_set_env(name: str) -> set[int]:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return set()
    out: set[int] = set()
    for p in raw.split(","):
        p = p.strip()
        if not p:
            continue
        try:
            out.add(int(p))
        except Exception:
            pass
    return out

def _league_interval_sec(
    league_id: int,
    *,
    super_fast_leagues: set[int],
    fast_leagues: set[int],
    super_fast_interval: float,
    fast_interval: float,
    slow_interval: float,
) -> float:
    """
    리그별 폴링 주기 결정 우선순위:
    SUPER_FAST > FAST > SLOW(기본)
    """
    if league_id in super_fast_leagues:
        return super_fast_interval
    if league_id in fast_leagues:
        return fast_interval
    return slow_interval



def _utc_now() -> dt.datetime:
    return now_utc()



def _load_live_window_game_rows() -> List[Dict[str, Any]]:
    """
    정석 구조(개선):
    - 시작 전(pre): now ~ now+pre_min
    - 진행중(in-play): game_date가 now - inplay_max_min 이후이고, status가 '종료'가 아닌 경기

    ✅ 추가 보정(중요):
    - start_called_at(=킥오프 감지)이 찍힌 경기는,
      DB status가 NS/TBD로 남아있더라도 in-play 후보에서 절대 빠지지 않게 유지한다.
      (API가 NS를 오래 주는 케이스에서 윈도우 탈락 → 영구 NS 고착 방지)

    env:
      HOCKEY_LIVE_PRESTART_MIN        (default 60)
      HOCKEY_LIVE_INPLAY_MAX_MIN      (default 240)
      HOCKEY_LIVE_NS_GRACE_MIN        (default 20)
      HOCKEY_LIVE_FUTURE_GRACE_MIN    (default 2)
      HOCKEY_LIVE_BATCH_LIMIT         (default 120)
    """
    leagues = hockey_live_leagues()
    if not leagues:
        return []

    pre_min = _int_env("HOCKEY_LIVE_PRESTART_MIN", 60)
    inplay_max_min = _int_env("HOCKEY_LIVE_INPLAY_MAX_MIN", 240)
    ns_grace_min = _int_env("HOCKEY_LIVE_NS_GRACE_MIN", 20)
    future_grace_min = _int_env("HOCKEY_LIVE_FUTURE_GRACE_MIN", 2)
    batch_limit = _int_env("HOCKEY_LIVE_BATCH_LIMIT", 120)

    now = _utc_now()
    upcoming_end = now + dt.timedelta(minutes=pre_min)

    inplay_start = now - dt.timedelta(minutes=inplay_max_min)
    inplay_end = now + dt.timedelta(minutes=future_grace_min)

    ns_grace_start = now - dt.timedelta(minutes=ns_grace_min)

    rows = hockey_fetch_all(
        """
        SELECT
          g.id, g.league_id, g.season, g.status, g.game_date
        FROM hockey_games g
        LEFT JOIN hockey_live_poll_state ps
          ON ps.game_id = g.id
        WHERE g.league_id = ANY(%s)
          AND (
            -- (1) 시작 전(pre) 경기: now ~ now+pre
            (g.game_date >= %s AND g.game_date <= %s)

            OR

            -- (2) 진행중(in-play) 경기: 최근 N분 이내에 "시작했거나 막 시작한" 경기 + 종료 아님
            (
              g.game_date >= %s
              AND g.game_date <= %s
              AND COALESCE(g.status, '') NOT IN (
                'FT','AET','PEN','FIN','ENDED','END',
                'ABD','AW','CANC','POST','WO'
              )
              AND (
                -- ✅ 보통 진행중 상태 (NS/TBD 제외)
                COALESCE(g.status, '') NOT IN ('NS','TBD')

                OR

                -- ✅ 시작 직후 ns_grace_min 동안만 NS/TBD 허용
                (COALESCE(g.status, '') IN ('NS','TBD') AND g.game_date >= %s)

                OR

                -- ✅ 핵심: "킥오프 감지(start_called_at)"가 찍힌 경기는
                --        NS/TBD로 남아도 윈도우에서 절대 탈락시키지 않는다.
                (COALESCE(g.status, '') IN ('NS','TBD')
                 AND ps.start_called_at IS NOT NULL
                 AND ps.finished_at IS NULL)
              )
            )
          )
        ORDER BY g.game_date ASC
        LIMIT %s
        """,
        (
            leagues,
            now, upcoming_end,
            inplay_start, inplay_end,
            ns_grace_start,
            batch_limit,
        ),
    )
    return [dict(r) for r in rows]



def _is_finished_status(s: str, game_date: Optional[dt.datetime]) -> bool:
    """
    ✅ 워커 관점 '종료' 판정(중요):
    - 명시적 종료 상태는 즉시 종료로 본다.
    - 과거 경기인데 NS/TBD/SUSP/INT/DELAYED 같은 상태로 남아있으면
      라이브로 다시 바뀔 가능성이 사실상 없으므로 '종료'로 본다(시간 기반 종료).
    """
    x = (s or "").upper().strip()

    # 1) 명시적 종료/확정 상태
    if x in {
        "FT", "AET", "PEN", "FIN", "END", "ENDED",
        "ABD", "AW", "CANC", "POST", "WO",
    }:
        return True

    # 2) 시간 기반 종료: 과거 경기인데 미시작/중단류 상태로 남아있는 경우
    #    (여기서 6시간은 너가 쿼리에서 쓰던 기준과 동일하게 맞춤)
    if isinstance(game_date, dt.datetime):
        try:
            age = _utc_now() - game_date
            if age > dt.timedelta(hours=6):
                if x in {"NS", "TBD", "SUSP", "INT", "DELAYED"}:
                    return True
        except Exception:
            # game_date 비교 실패 시에는 보수적으로 False
            pass

    return False




def _is_not_started_status(s: str) -> bool:
    x = (s or "").upper().strip()
    return x in {"NS", "TBD"}


def _should_poll_events(db_status: str, game_date: Optional[dt.datetime]) -> bool:
    """
    events 폴링 조건:
    - 윈도우 후보로 들어온 경기들만 여기까지 오고,
    - '종료'로 판정되면 스킵
    """
    if _is_finished_status(db_status, game_date):
        return False
    if _is_not_started_status(db_status):
        # 시작 전이라도 윈도우 안이면 line-up/상태변경 가능성은 있지만,
        # events는 보통 시작 후 의미가 크므로 기본은 스킵.
        return False
    return True


def _poll_state_get_or_create(game_id: int) -> Dict[str, Any]:
    row = hockey_fetch_one(
        "SELECT * FROM hockey_live_poll_state WHERE game_id=%s",
        (game_id,),
    )
    if row:
        return dict(row)

    hockey_execute(
        "INSERT INTO hockey_live_poll_state (game_id) VALUES (%s) ON CONFLICT DO NOTHING",
        (game_id,),
    )
    row2 = hockey_fetch_one(
        "SELECT * FROM hockey_live_poll_state WHERE game_id=%s",
        (game_id,),
    )
    return dict(row2) if row2 else {"game_id": game_id}


def _poll_state_update(game_id: int, **cols: Any) -> None:
    if not cols:
        return
    keys = list(cols.keys())
    sets = ", ".join([f"{k}=%s" for k in keys])
    values = [cols[k] for k in keys]
    hockey_execute(
        f"UPDATE hockey_live_poll_state SET {sets}, updated_at=now() WHERE game_id=%s",
        tuple(values + [game_id]),
    )




def _extract_team_ids(item: Dict[str, Any]) -> Tuple[Optional[int], Optional[int]]:
    teams = item.get("teams")
    if not isinstance(teams, dict):
        return None, None

    home = teams.get("home")
    away = teams.get("away")
    home_id = _safe_int(home.get("id")) if isinstance(home, dict) else None
    away_id = _safe_int(away.get("id")) if isinstance(away, dict) else None
    return home_id, away_id


def upsert_game(item: Dict[str, Any], league_id_fallback: int, season_fallback: int) -> Optional[int]:
    gid = _safe_int(item.get("id"))
    if gid is None:
        return None

    league_obj = item.get("league") if isinstance(item.get("league"), dict) else {}
    league_id = _safe_int(league_obj.get("id")) or league_id_fallback
    season = _safe_int(league_obj.get("season")) or season_fallback
    stage = _safe_text(league_obj.get("stage")) or _safe_text(item.get("stage"))
    group_name = _safe_text(league_obj.get("group")) or _safe_text(item.get("group"))

    home_team_id, away_team_id = _extract_team_ids(item)

    date_str = item.get("date")
    game_date = None
    if isinstance(date_str, str) and date_str:
        try:
            game_date = dt.datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except Exception:
            game_date = None

    status_obj = item.get("status") if isinstance(item.get("status"), dict) else {}
    status = _safe_text(status_obj.get("short"))
    status_long = _safe_text(status_obj.get("long"))

    # ✅ API-Sports: timer (예: "18" 또는 "18:34")
    live_timer = _safe_text(item.get("timer"))

    tz = _safe_text(item.get("timezone"))
    scores = item.get("scores") if isinstance(item.get("scores"), dict) else {}

    hockey_execute(
        """
        INSERT INTO hockey_games (
          id, league_id, season,
          stage, group_name,
          home_team_id, away_team_id,
          game_date, status, status_long, live_timer, timezone,
          score_json, raw_json
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb)
        ON CONFLICT (id) DO UPDATE SET
          league_id = EXCLUDED.league_id,
          season = EXCLUDED.season,
          stage = EXCLUDED.stage,
          group_name = EXCLUDED.group_name,
          home_team_id = EXCLUDED.home_team_id,
          away_team_id = EXCLUDED.away_team_id,
          game_date = EXCLUDED.game_date,
          status = EXCLUDED.status,
          status_long = EXCLUDED.status_long,
          live_timer = EXCLUDED.live_timer,
          timezone = EXCLUDED.timezone,
          score_json = EXCLUDED.score_json,
          raw_json = EXCLUDED.raw_json
        """,
        (
            gid,
            league_id,
            season,
            stage,
            group_name,
            home_team_id,
            away_team_id,
            game_date,
            status,
            status_long,
            live_timer,
            tz,
            _jdump(scores),
            _jdump(item),
        ),
    )


    return gid


def _norm_text(x: Optional[str]) -> str:
    return (x or "").strip().lower()


def _stable_event_order(
    period: str,
    minute: Optional[int],
    team_id: Optional[int],
    etype: str,
    comment: Optional[str],
    players_arr: List[str],
) -> int:
    """
    라이브 수집에서 '순서(idx)' 때문에 중복이 쌓이지 않도록,
    이벤트의 의미 기반 fingerprint로 event_order를 생성한다.

    - assists는 fingerprint에서 제외 (동일 골의 assists가 늦게 채워지는 케이스를 UPDATE로 흡수)
    - 같은 분에 같은 타입 골이 2개라도 players/ comment가 다르면 다른 fingerprint → 둘 다 저장됨
    """
    sig = "|".join(
        [
            _norm_text(period),
            str(minute if minute is not None else -1),
            str(team_id if team_id is not None else -1),
            _norm_text(etype),
            _norm_text(comment),
            ",".join([_norm_text(p) for p in (players_arr or [])]),
        ]
    )
    return zlib.crc32(sig.encode("utf-8")) & 0x7FFFFFFF


def upsert_events(game_id: int, ev_list: List[Dict[str, Any]]) -> None:
    """
    API-Sports events는 고유 id가 없고 minute/assists 등이 라이브 중 정정될 수 있다.
    따라서 '증분 누적'이 아니라 '스냅샷 동기화'가 정석이다.

    - 이번 스냅샷에 존재하는 event_key 목록을 만든다.
    - 스냅샷 이벤트를 upsert 한다.
    - DB에 남아있는 goal/penalty 중, 이번 스냅샷에 없는 event_key는 HARD DELETE 한다.
    """
    snapshot_event_keys: List[str] = []

    for ev in ev_list:
        if not isinstance(ev, dict):
            continue

        period = _safe_text(ev.get("period")) or "UNK"
        minute = _safe_int(ev.get("minute"))

        team = ev.get("team") if isinstance(ev.get("team"), dict) else {}
        team_id = _safe_int(team.get("id")) if isinstance(team, dict) else None
        if team_id == 0:
            team_id = None

        etype = _safe_text(ev.get("type")) or "unknown"
        comment = _safe_text(ev.get("comment")) or _safe_text(ev.get("detail"))

        players = ev.get("players")
        assists = ev.get("assists")
        if not isinstance(players, list):
            players = []
        if not isinstance(assists, list):
            assists = []

        players_arr = [str(x).strip() for x in players if str(x).strip()]
        assists_arr = [str(x).strip() for x in assists if str(x).strip()]

        # (DB 트리거 hockey_game_events_set_event_key() 와 동일한 규칙으로 event_key 계산)
        # lower(type)||'|'||period||'|'||minute||'|'||team_id||'|'||lower(comment)||'|'||lower(players_csv)||'|'||lower(assists_csv)
        event_key = (
            (etype or "").strip().lower()
            + "|"
            + (period or "")
            + "|"
            + ("" if minute is None else str(minute))
            + "|"
            + ("" if team_id is None else str(team_id))
            + "|"
            + ((comment or "").strip().lower())
            + "|"
            + (",".join(players_arr).strip().lower())
            + "|"
            + (",".join(assists_arr).strip().lower())
        )

        # 스냅샷 기준은 goal/penalty만 (현재 API 응답도 이 2종 위주)
        if etype in ("goal", "penalty"):
            snapshot_event_keys.append(event_key)

        event_order = _stable_event_order(period, minute, team_id, etype, comment, players_arr)

        hockey_execute(
            """
            INSERT INTO hockey_game_events (
              game_id, period, minute, team_id,
              type, comment, players, assists,
              event_order, raw_json
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
            ON CONFLICT (game_id, event_key)
            DO UPDATE SET
              comment = COALESCE(EXCLUDED.comment, hockey_game_events.comment),
              players = CASE
                WHEN COALESCE(array_length(EXCLUDED.players, 1), 0) >= COALESCE(array_length(hockey_game_events.players, 1), 0)
                THEN EXCLUDED.players
                ELSE hockey_game_events.players
              END,
              assists = CASE
                WHEN COALESCE(array_length(EXCLUDED.assists, 1), 0) >= COALESCE(array_length(hockey_game_events.assists, 1), 0)
                THEN EXCLUDED.assists
                ELSE hockey_game_events.assists
              END,
              raw_json = EXCLUDED.raw_json
            """,
            (
                game_id,
                period,
                minute,
                team_id,
                etype,
                comment,
                players_arr,
                assists_arr,
                event_order,
                _jdump(ev),
            ),
        )

    # ─────────────────────────────────────────
    # 스냅샷 HARD DELETE 동기화 (근본 해결)
    # - 이번 스냅샷에 없는 goal/penalty 이벤트는 DB에서 제거
    # - 이렇게 해야 minute 정정/삭제된 "찌꺼기 이벤트"가 남지 않음
    # ─────────────────────────────────────────
    hockey_execute(
        """
        DELETE FROM hockey_game_events
        WHERE game_id = %s
          AND type IN ('goal','penalty')
          AND (event_key IS NOT NULL AND event_key <> '')
          AND NOT (event_key = ANY(%s))
        """,
        (game_id, snapshot_event_keys),
    )




def _api_get_game_by_id(game_id: int) -> Optional[Dict[str, Any]]:
    payload = _get("/games", {"id": game_id})
    resp = payload.get("response") if isinstance(payload, dict) else None
    if isinstance(resp, list) and resp and isinstance(resp[0], dict):
        return resp[0]
    return None


def tick_once_windowed(
    rows: List[Dict[str, Any]],
    *,
    super_fast_leagues: set[int],
    fast_leagues: set[int],
    super_fast_interval: float,
    fast_interval: float,
    slow_interval: float,
    pre_min: int,
    post_min: int,
) -> Tuple[int, int, int]:
    """
    ✅ 게임별 1회 호출 규칙 + 라이브 중 주기 규칙을 DB 상태(hockey_live_poll_state)로 보장한다.

    게임 1개 기준 호출 구조:
      - 시작 1시간 전 1회 (pre_called_at)
      - 시작 감지 1회 (start_called_at)
      - 라이브 중 next_live_poll_at 도달 시만 주기 호출
      - 종료 감지 1회 (end_called_at + finished_at)
      - 종료 30분 후 1회 (post_called_at)

    returns: (games_upserted, events_upserted, candidates)
    """
    if not rows:
        return (0, 0, 0)

    games_upserted = 0
    events_upserted = 0
    now = _utc_now()
    ns_grace_min = _int_env("HOCKEY_LIVE_NS_GRACE_MIN", 20)

    for r in rows:
        gid = int(r["id"])
        league_id = int(r.get("league_id") or 0)
        season = int(r.get("season") or 0)
        db_status = (r.get("status") or "").strip()
        db_date = r.get("game_date")

        # poll state 로드/생성
        st = _poll_state_get_or_create(gid)
        pre_called_at = st.get("pre_called_at")
        start_called_at = st.get("start_called_at")
        end_called_at = st.get("end_called_at")
        post_called_at = st.get("post_called_at")
        finished_at = st.get("finished_at")
        next_live_poll_at = st.get("next_live_poll_at")

        # ─────────────────────────────────────────
        # (A) 시작 1시간 전 1회
        # ─────────────────────────────────────────
        if (
            pre_called_at is None
            and isinstance(db_date, dt.datetime)
            and (db_date - dt.timedelta(minutes=pre_min)) <= now < db_date
        ):
            try:
                api_item = _api_get_game_by_id(gid)
                if isinstance(api_item, dict):
                    upsert_game(api_item, league_id, season)
                    games_upserted += 1
                    _poll_state_update(gid, pre_called_at=now)
            except Exception as e:
                log.warning("pre-call games(id) fetch failed: game=%s err=%s", gid, e)
            continue

        # ─────────────────────────────────────────
        # (B) 시작 시점 1회 (워커가 처음 '시작 이후'를 감지했을 때)
        #   - 시작 직후 NS/TBD가 잠깐 남는 케이스가 있으니
        #     now >= game_date면 1회 호출로 스냅샷 갱신해준다.
        # ─────────────────────────────────────────
        if (
            start_called_at is None
            and isinstance(db_date, dt.datetime)
            and now >= db_date
            and not _is_finished_status(db_status, db_date)
        ):
            try:
                api_item = _api_get_game_by_id(gid)
                if isinstance(api_item, dict):
                    upsert_game(api_item, league_id, season)
                    games_upserted += 1
                    _poll_state_update(gid, start_called_at=now)

                    # 최신 status/game_date로 재판정(라이브 전환을 놓치지 않기 위함)
                    cur = hockey_fetch_one(
                        "SELECT status, game_date FROM hockey_games WHERE id=%s",
                        (gid,),
                    )
                    if cur:
                        db_status = (cur.get("status") or db_status).strip()
                        db_date = cur.get("game_date") or db_date
            except Exception as e:
                log.warning("start-call games(id) fetch failed: game=%s err=%s", gid, e)

                # ─────────────────────────────────────────
        # (B2) 킥오프 이후 NS/TBD 재확인(상태 전환 지연 흡수)
        #   - start_called_at은 찍혔는데 status가 계속 NS/TBD이면,
        #     ns_grace_min 동안 next_live_poll_at 기준으로 /games를 재호출한다.
        #   - status가 LIVE로 바뀌면 같은 틱에서 (E) 라이브 주기 호출로 자연스럽게 넘어간다.
        # ─────────────────────────────────────────
        if (
            isinstance(db_date, dt.datetime)
            and start_called_at is not None
            and db_status in ("NS", "TBD")
            and now >= db_date
            and now <= (db_date + dt.timedelta(minutes=ns_grace_min))
            and not _is_finished_status(db_status, db_date)
        ):
            # due 판단: next_live_poll_at이 없으면 즉시, 있으면 그 시각 이후에만
            due = False
            if next_live_poll_at is None:
                due = True
            else:
                try:
                    due = now >= next_live_poll_at
                except Exception:
                    due = True

            if due:
                interval = _league_interval_sec(
                    league_id,
                    super_fast_leagues=super_fast_leagues,
                    fast_leagues=fast_leagues,
                    super_fast_interval=super_fast_interval,
                    fast_interval=fast_interval,
                    slow_interval=slow_interval,
                )

                try:
                    # /games 스냅샷 재확인
                    api_item = _api_get_game_by_id(gid)
                    if isinstance(api_item, dict):
                        upsert_game(api_item, league_id, season)
                        games_upserted += 1

                        # 최신 status/game_date로 재판정
                        cur = hockey_fetch_one(
                            "SELECT status, game_date FROM hockey_games WHERE id=%s",
                            (gid,),
                        )
                        if cur:
                            db_status = (cur.get("status") or db_status).strip()
                            db_date = cur.get("game_date") or db_date

                except Exception as e:
                    log.warning("ns-grace games(id) recheck failed: game=%s err=%s", gid, e)

                # 다음 재확인 시각 저장(여기서부터 폴링이 "살아남")
                _poll_state_update(
                    gid,
                    next_live_poll_at=now + dt.timedelta(seconds=float(interval)),
                )

            # 아직도 NS/TBD면 (E)로 못 가니 여기서 다음 게임으로
            if db_status in ("NS", "TBD"):
                continue


        # ─────────────────────────────────────────
        # (C) 종료 감지 1회
        # ─────────────────────────────────────────
        if _is_finished_status(db_status, db_date) and end_called_at is None:
            try:
                api_item = _api_get_game_by_id(gid)
                if isinstance(api_item, dict):
                    upsert_game(api_item, league_id, season)
                    games_upserted += 1
                    _poll_state_update(gid, end_called_at=now, finished_at=now)
            except Exception as e:
                log.warning("end-call games(id) fetch failed: game=%s err=%s", gid, e)
            continue

        # ─────────────────────────────────────────
        # (D) 종료 30분 후 1회
        #   - finished_at이 없으면(이전 루프에서 종료를 아직 못 봤으면) 실행 안 함
        # ─────────────────────────────────────────
        if (
            finished_at is not None
            and post_called_at is None
            and now >= (finished_at + dt.timedelta(minutes=post_min))
        ):
            try:
                api_item = _api_get_game_by_id(gid)
                if isinstance(api_item, dict):
                    upsert_game(api_item, league_id, season)
                    games_upserted += 1
                    _poll_state_update(gid, post_called_at=now)
            except Exception as e:
                log.warning("post-call games(id) fetch failed: game=%s err=%s", gid, e)
            continue

        # ─────────────────────────────────────────
        # (D2) LIVE 판정 보정
        #   - 킥오프 이후인데 API가 계속 NS/TBD를 주는 경우
        #   - timer / score / status_short 중 하나라도 LIVE 징후면 LIVE로 간주
        # ─────────────────────────────────────────
        if (
            isinstance(db_date, dt.datetime)
            and now >= db_date
            and db_status in ("NS", "TBD")
        ):
            try:
                api_item = _api_get_game_by_id(gid)
                if isinstance(api_item, dict):
                    status_obj = api_item.get("status") if isinstance(api_item.get("status"), dict) else {}
                    api_status = (status_obj.get("short") or "").strip()
                    timer = api_item.get("timer")

                    # ✅ hockey API-sports scores는 {home:int, away:int} 형태
                    scores = api_item.get("scores")
                    home_score = None
                    away_score = None
                    if isinstance(scores, dict):
                        h = scores.get("home")
                        a = scores.get("away")
                        if isinstance(h, int):
                            home_score = h
                        if isinstance(a, int):
                            away_score = a

                    has_score = (
                        isinstance(home_score, int)
                        and isinstance(away_score, int)
                        and (home_score + away_score) > 0
                    )
                    has_timer = bool(timer)

                    # LIVE 징후가 있으면 강제로 스냅샷 반영
                    if has_timer or has_score or api_status not in ("NS", "TBD", ""):
                        upsert_game(api_item, league_id, season)
                        games_upserted += 1

                        # 최신 상태 다시 로드
                        cur = hockey_fetch_one(
                            "SELECT status, game_date FROM hockey_games WHERE id=%s",
                            (gid,),
                        )
                        if cur:
                            db_status = (cur.get("status") or db_status).strip()
                            db_date = cur.get("game_date") or db_date
            except Exception as e:
                log.warning("live-force check failed: game=%s err=%s", gid, e)


                # ─────────────────────────────────────────
        # (D3) 킥오프 이후에도 NS/TBD로 남는 케이스 강제 LIVE 처리
        #   - /games 가 NS를 계속 주는 경우가 있음(하키에서 실제로 발생)
        #   - 이때 /games/events 에 이벤트가 1개라도 오면 "이미 진행중"으로 보고
        #     DB status를 LIVE로 강제 전환해서 (E) 폴링을 태운다.
        # ─────────────────────────────────────────
        if (
            isinstance(db_date, dt.datetime)
            and now >= db_date
            and db_status in ("NS", "TBD")
            and not _is_finished_status(db_status, db_date)
        ):
            # next_live_poll_at 기준으로만 재시도(너무 자주 치지 않게)
            due = False
            if next_live_poll_at is None:
                due = True
            else:
                try:
                    due = now >= next_live_poll_at
                except Exception:
                    due = True

            if due:
                interval = _league_interval_sec(
                    league_id,
                    super_fast_leagues=super_fast_leagues,
                    fast_leagues=fast_leagues,
                    super_fast_interval=super_fast_interval,
                    fast_interval=fast_interval,
                    slow_interval=slow_interval,
                )

                # 1) events 먼저 확인 (NS라도 이벤트가 오면 진행중으로 간주)
                ev_list: List[Dict[str, Any]] = []
                try:
                    ev_payload = _get("/games/events", {"game": gid})
                    ev_resp = ev_payload.get("response") if isinstance(ev_payload, dict) else None
                    if isinstance(ev_resp, list):
                        ev_list = [x for x in ev_resp if isinstance(x, dict)]
                except Exception as e:
                    log.warning("ns-livecheck events fetch failed: game=%s err=%s", gid, e)

                if ev_list:
                    # events가 있으면 → LIVE로 강제 전환 + 이벤트 저장
                    try:
                        upsert_events(gid, ev_list)
                        events_upserted += len(ev_list)
                    except Exception as e:
                        log.warning("ns-livecheck upsert_events failed: game=%s err=%s", gid, e)

                    # ✅ status를 강제로 LIVE로 변경 (upsert_game으로는 NS로 다시 덮일 수 있음)
                    hockey_execute(
                        """
                        UPDATE hockey_games
                        SET status=%s,
                            status_long=%s,
                            updated_at=now()
                        WHERE id=%s
                        """,
                        ("LIVE", "Live (forced by events)", gid),
                    )
                    db_status = "LIVE"  # 이 틱에서 바로 (E)로 진입 가능

                # 2) 다음 폴링 예약 (NS든 LIVE든 다음 확인 시각은 필요)
                _poll_state_update(
                    gid,
                    next_live_poll_at=now + dt.timedelta(seconds=float(interval)),
                )

                # 아직도 NS/TBD면 (E) 못 타니까 다음 게임으로
                if db_status in ("NS", "TBD"):
                    continue



        # ─────────────────────────────────────────
        # (E) 라이브 중 주기 호출 (게임별 next_live_poll_at 기준)
        #
        # ✅ 핵심 수정(데드락 방지):
        # - start_called_at이 찍힌 이후에는 DB status가 NS/TBD로 남아있더라도
        #   /games 스냅샷을 주기적으로 다시 호출해야 상태(BT/P1/P2/P3/...)로 전환된다.
        #
        # - 기존: if _should_poll_events(db_status, db_date):
        #         → db_status가 NS/TBD면 영원히 False라서 (E) 자체가 못 타는 데드락 발생
        #
        # - 변경: "종료가 아니고, start_called_at이 존재(=킥오프 이후 감지됨)"면 (E) 진입 허용
        #
        # - events 폴링은 기존 정책(_should_poll_events) 그대로 유지:
        #   즉, DB status가 아직 NS/TBD면 events는 스킵(불필요 호출 방지)
        # ─────────────────────────────────────────
        if (start_called_at is not None) and (not _is_finished_status(db_status, db_date)):
            due = False
            if next_live_poll_at is None:
                due = True
            else:
                try:
                    due = now >= next_live_poll_at
                except Exception:
                    due = True

            if due:
                interval = _league_interval_sec(
                    league_id,
                    super_fast_leagues=super_fast_leagues,
                    fast_leagues=fast_leagues,
                    super_fast_interval=super_fast_interval,
                    fast_interval=fast_interval,
                    slow_interval=slow_interval,
                )

                # 1) /games 스냅샷 (status 전환을 위해 NS/TBD여도 반드시 수행)
                try:
                    api_item = _api_get_game_by_id(gid)
                    if isinstance(api_item, dict):
                        upsert_game(api_item, league_id, season)
                        games_upserted += 1

                        # upsert 이후 최신 status/game_date 로 재판정
                        cur = hockey_fetch_one(
                            "SELECT status, game_date FROM hockey_games WHERE id=%s",
                            (gid,),
                        )
                        if cur:
                            db_status = (cur.get("status") or db_status).strip()
                            db_date = cur.get("game_date") or db_date
                except Exception as e:
                    log.warning("live-call games(id) fetch failed: game=%s err=%s", gid, e)
                    # games 실패해도 next_live_poll_at은 너무 촘촘히 다시 치지 않게 약하게 밀어줌
                    _poll_state_update(
                        gid,
                        next_live_poll_at=now + dt.timedelta(seconds=max(5.0, float(interval))),
                    )
                    continue

                # 2) /games/events (진행중일 때만)  ← 여기 조건은 그대로 유지
                if _should_poll_events(db_status, db_date):
                    try:
                        ev_payload = _get("/games/events", {"game": gid})
                        ev_resp = ev_payload.get("response") if isinstance(ev_payload, dict) else None
                        if isinstance(ev_resp, list):
                            ev_list = [x for x in ev_resp if isinstance(x, dict)]
                            if ev_list:
                                upsert_events(gid, ev_list)
                                events_upserted += len(ev_list)
                    except Exception as e:
                        log.warning("events fetch failed: game=%s err=%s", gid, e)

                # 다음 라이브 폴링 시각 저장
                _poll_state_update(
                    gid,
                    next_live_poll_at=now + dt.timedelta(seconds=float(interval)),
                )


    return (games_upserted, events_upserted, len(rows))





def main() -> None:
    leagues = hockey_live_leagues()
    if not leagues:
        raise RuntimeError("HOCKEY_LIVE_LEAGUES is empty. ex) 57,58")

    ensure_event_key_migration()
    log.info("ensure_event_key_migration: OK")

    # 정석 구조에서는 season을 굳이 고정할 필요가 없다.
    # DB에서 window로 뽑힌 경기 row에 season이 이미 들어있기 때문.
    # (HOCKEY_SEASON 환경변수도 더 이상 강제하지 않음)

    super_fast_leagues = _int_set_env("HOCKEY_LIVE_SUPER_FAST_LEAGUES")
    super_fast_interval = _float_env("HOCKEY_LIVE_SUPER_FAST_INTERVAL_SEC", 2.0)  # super fast

    fast_leagues = _int_set_env("HOCKEY_LIVE_FAST_LEAGUES")
    fast_interval = _float_env("HOCKEY_LIVE_FAST_INTERVAL_SEC", 5.0)   # fast

    slow_interval = _float_env("HOCKEY_LIVE_SLOW_INTERVAL_SEC", 15.0)  # slow(기본)
    idle_interval = _float_env("HOCKEY_LIVE_IDLE_INTERVAL_SEC", 180.0) # 후보 없을 때


    pre_min = _int_env("HOCKEY_LIVE_PRESTART_MIN", 60)
    post_min = _int_env("HOCKEY_LIVE_POSTEND_MIN", 30)

    log.info(
        "🏒 hockey live worker(start windowed): leagues=%s pre=%sm post=%sm super_fast_leagues=%s super_fast=%.1fs fast_leagues=%s fast=%.1fs slow=%.1fs idle=%.1fs",
        leagues,
        pre_min,
        post_min,
        sorted(list(super_fast_leagues)),
        super_fast_interval,
        sorted(list(fast_leagues)),
        fast_interval,
        slow_interval,
        idle_interval,
    )



    super_fast_leagues = _int_set_env("HOCKEY_LIVE_SUPER_FAST_LEAGUES")
    super_fast_interval = _float_env("HOCKEY_LIVE_SUPER_FAST_INTERVAL_SEC", 2.0)

    log.info(
        "🏒 hockey live worker(interval tiers): super_fast_leagues=%s super_fast=%.1fs fast_leagues=%s fast=%.1fs slow=%.1fs idle=%.1fs",
        sorted(list(super_fast_leagues)), super_fast_interval,
        sorted(list(fast_leagues)), fast_interval,
        slow_interval, idle_interval
    )

    # 리그별 다음 실행 시각(UTC timestamp)
    next_run_by_league: Dict[int, float] = {}

    # ✅ meta/standings 저빈도 갱신용 타이머
    meta_refresh_sec = _float_env("HOCKEY_META_REFRESH_SEC", 86400.0)           # 기본 24h
    standings_refresh_sec = _float_env("HOCKEY_STANDINGS_REFRESH_SEC", 21600.0) # 기본 6h
    meta_sleep = _float_env("HOCKEY_META_REFRESH_SLEEP_SEC", 0.5)
    standings_sleep = _float_env("HOCKEY_STANDINGS_REFRESH_SLEEP_SEC", 0.5)

    next_meta_refresh_ts = 0.0
    next_standings_refresh_ts = 0.0

    while True:
        try:
            now0 = time.time()

            # ✅ (A) 메타 테이블 갱신: countries/leagues/league_seasons
            if now0 >= next_meta_refresh_ts:
                try:
                    _refresh_meta_for_leagues(leagues, sleep_sec=meta_sleep)
                except Exception as e:
                    log.exception("meta refresh tick failed: %s", e)
                next_meta_refresh_ts = now0 + float(meta_refresh_sec)

            # ✅ (B) 스탠딩+팀 갱신: standings + teams
            if now0 >= next_standings_refresh_ts:
                try:
                    _refresh_standings_for_latest_seasons(leagues, sleep_sec=standings_sleep)
                except Exception as e:
                    log.exception("standings refresh tick failed: %s", e)
                next_standings_refresh_ts = now0 + float(standings_refresh_sec)

            # 1) 윈도우 후보 한 번만 로드
            all_rows = _load_live_window_game_rows()
            if not all_rows:
                time.sleep(idle_interval)
                continue

            # 2) 리그별로 rows 그룹핑
            rows_by_league: Dict[int, List[Dict[str, Any]]] = {}
            for r in all_rows:
                lid = int(r.get("league_id") or 0)
                if lid <= 0:
                    continue
                rows_by_league.setdefault(lid, []).append(r)

            if not rows_by_league:
                time.sleep(idle_interval)
                continue

            now_ts = time.time()

            # 3) due 된 리그만 처리
            total_games_upserted = 0
            total_events_upserted = 0
            total_candidates = 0
            processed_leagues: List[int] = []

            for lid, rows in rows_by_league.items():
                interval = _league_interval_sec(
                    lid,
                    super_fast_leagues=super_fast_leagues,
                    fast_leagues=fast_leagues,
                    super_fast_interval=super_fast_interval,
                    fast_interval=fast_interval,
                    slow_interval=slow_interval,
                )

                nxt = next_run_by_league.get(lid, 0.0)
                if now_ts < nxt:
                    continue

                g_up, e_up, cand = tick_once_windowed(
                    rows,
                    super_fast_leagues=super_fast_leagues,
                    fast_leagues=fast_leagues,
                    super_fast_interval=super_fast_interval,
                    fast_interval=fast_interval,
                    slow_interval=slow_interval,
                    pre_min=pre_min,
                    post_min=post_min,
                )
                total_games_upserted += g_up
                total_events_upserted += e_up
                total_candidates += cand
                processed_leagues.append(lid)

                next_run_by_league[lid] = now_ts + max(1.0, float(interval))

            log.info(
                "tick done(per-league): leagues_processed=%s total_candidates=%s games_upserted=%s events_upserted=%s",
                processed_leagues, total_candidates, total_games_upserted, total_events_upserted
            )

            # 4) 다음 sleep 계산
            soonest = None
            for lid, tnext in next_run_by_league.items():
                if lid in rows_by_league:
                    if soonest is None or tnext < soonest:
                        soonest = tnext

            if soonest is None:
                time.sleep(1.0)
            else:
                wait = max(0.0, soonest - time.time())
                wait = min(1.0, max(0.2, wait))
                time.sleep(wait)

        except Exception as e:
            log.exception("tick failed: %s", e)
            time.sleep(idle_interval)






if __name__ == "__main__":
    main()
