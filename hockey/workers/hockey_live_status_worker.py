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


def _utc_now() -> dt.datetime:
    return now_utc()



def _load_live_window_game_rows() -> List[Dict[str, Any]]:
    """
    정석 구조(개선):
    - 시작 전(pre): now ~ now+pre_min
    - 진행중(in-play): game_date가 now - inplay_max_min 이후이고, status가 '종료'가 아닌 경기

    ✅ 추가 보강(중요):
    - 시작 직후 API status가 잠깐 NS/TBD로 남는 케이스가 있어
      game_date가 now보다 과거가 되는 순간 pre에서 빠지고,
      in-play에서 NS/TBD 제외로 빠지면 "영원히 후보에서 탈락"하는 구멍이 생긴다.
      → 시작 후 ns_grace_min 동안은 NS/TBD도 in-play 후보로 포함한다.

    env:
      HOCKEY_LIVE_PRESTART_MIN      (default 60)
      HOCKEY_LIVE_INPLAY_MAX_MIN    (default 240)
      HOCKEY_LIVE_NS_GRACE_MIN      (default 20)   # ✅ 시작 후 NS/TBD 유예
      HOCKEY_LIVE_BATCH_LIMIT       (default 120)
    """
    leagues = hockey_live_leagues()
    if not leagues:
        return []

    pre_min = _int_env("HOCKEY_LIVE_PRESTART_MIN", 60)
    inplay_max_min = _int_env("HOCKEY_LIVE_INPLAY_MAX_MIN", 240)
    ns_grace_min = _int_env("HOCKEY_LIVE_NS_GRACE_MIN", 20)
    batch_limit = _int_env("HOCKEY_LIVE_BATCH_LIMIT", 120)

    now = _utc_now()
    upcoming_end = now + dt.timedelta(minutes=pre_min)
    inplay_start = now - dt.timedelta(minutes=inplay_max_min)
    ns_grace_start = now - dt.timedelta(minutes=ns_grace_min)

    rows = hockey_fetch_all(
        """
        SELECT
          id, league_id, season, status, game_date
        FROM hockey_games
        WHERE league_id = ANY(%s)
          AND (
            -- (1) 시작 전(pre) 경기: now ~ now+pre
            (game_date >= %s AND game_date <= %s)

            OR

            -- (2) 진행중(in-play) 경기: 시작시간이 최근 N분 이내 + 종료 아님
            (
              game_date >= %s
              AND COALESCE(status, '') NOT IN ('FT','AET','PEN','FIN','ENDED','END')
              AND (
                -- ✅ 보통 진행중 상태
                COALESCE(status, '') NOT IN ('NS','TBD')
                OR
                -- ✅ 시작 후 ns_grace_min 동안은 NS/TBD도 후보로 포함(시작 상태 전환을 놓치지 않기 위함)
                (COALESCE(status, '') IN ('NS','TBD') AND game_date >= %s)
              )
            )
          )
        ORDER BY game_date ASC
        LIMIT %s
        """,
        (leagues, now, upcoming_end, inplay_start, ns_grace_start, batch_limit),
    )
    return [dict(r) for r in rows]




def _is_finished_status(s: str) -> bool:
    x = (s or "").upper().strip()
    return x in {"FT", "AET", "PEN", "FIN", "ENDED", "END"}


def _is_not_started_status(s: str) -> bool:
    x = (s or "").upper().strip()
    return x in {"NS", "TBD"}


def _should_poll_events(db_status: str, game_date: Optional[dt.datetime]) -> bool:
    """
    events 폴링 조건:
    - 윈도우 목록에 들어온 경기들만 여기까지 오고,
    - status가 완전 종료면 스킵(단, 종료 직후 정정이 필요하면 윈도우 안이므로 /games?id 업데이트는 해도 됨)
    """
    if _is_finished_status(db_status):
        return False
    if _is_not_started_status(db_status):
        # 시작 전이라도 윈도우 안이면 line-up/상태변경 가능성은 있지만,
        # events는 보통 시작 후 의미가 크므로 기본은 스킵.
        # 필요하면 여기 True로 바꾸면 됨.
        return False
    return True



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



def _api_get_game_by_id(game_id: int) -> Optional[Dict[str, Any]]:
    payload = _get("/games", {"id": game_id})
    resp = payload.get("response") if isinstance(payload, dict) else None
    if isinstance(resp, list) and resp and isinstance(resp[0], dict):
        return resp[0]
    return None


def tick_once_windowed() -> Tuple[int, int, int]:
    """
    정석 구조 tick:
    - DB에서 윈도우 내 경기만 로드
    - 각 경기:
        1) /games?id 로 최신 상태 스냅샷 반영(upsert)
        2) (진행중일 때만) /games/events 호출 + upsert
    returns: (games_upserted, events_upserted, candidates)
    """
    rows = _load_live_window_game_rows()
    if not rows:
        return (0, 0, 0)

    games_upserted = 0
    events_upserted = 0

    for r in rows:
        gid = int(r["id"])
        league_id = int(r.get("league_id") or 0)
        season = int(r.get("season") or 0)
        db_status = (r.get("status") or "").strip()
        db_date = r.get("game_date")

        # 1) 게임 스냅샷 갱신
        try:
            api_item = _api_get_game_by_id(gid)
            if isinstance(api_item, dict):
                new_id = upsert_game(api_item, league_id, season)
                if new_id:
                    games_upserted += 1

                    # upsert 이후 최신 status를 다시 읽어 events 판단
                    cur = hockey_fetch_one("SELECT status, game_date FROM hockey_games WHERE id=%s", (gid,))
                    if cur:
                        db_status = (cur.get("status") or db_status).strip()
                        db_date = cur.get("game_date") or db_date
        except Exception as e:
            log.warning("api games(id) fetch failed: game=%s err=%s", gid, e)
            continue

        # 2) events는 "진행중일 때만" 폴링
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

    fast_leagues = _int_set_env("HOCKEY_LIVE_FAST_LEAGUES")
    fast_interval = _float_env("HOCKEY_LIVE_FAST_INTERVAL_SEC", 5.0)   # 1부(빠른 리그)
    slow_interval = _float_env("HOCKEY_LIVE_SLOW_INTERVAL_SEC", 15.0)  # 나머지(기본)
    idle_interval = _float_env("HOCKEY_LIVE_IDLE_INTERVAL_SEC", 180.0) # 대상 경기 없을 때(3분)

    pre_min = _int_env("HOCKEY_LIVE_PRESTART_MIN", 60)
    post_min = _int_env("HOCKEY_LIVE_POSTEND_MIN", 30)

    log.info(
        "🏒 hockey live worker(start windowed): leagues=%s pre=%sm post=%sm fast_leagues=%s fast=%.1fs slow=%.1fs idle=%.1fs",
        leagues, pre_min, post_min, sorted(list(fast_leagues)), fast_interval, slow_interval, idle_interval
    )


    while True:
        sleep_sec = idle_interval
        try:
            games_upserted, events_upserted, candidates = tick_once_windowed()
            log.info(
                "tick done(windowed): candidates=%s games_upserted=%s events_upserted=%s",
                candidates, games_upserted, events_upserted
            )

            if candidates > 0:
                # 이번 윈도우에 fast league 경기가 하나라도 있으면 fast_interval
                has_fast = False
                rows_check = _load_live_window_game_rows()
                for rr in rows_check:
                    lid = int(rr.get("league_id") or 0)
                    if lid in fast_leagues:
                        has_fast = True
                        break

                sleep_sec = fast_interval if has_fast else slow_interval
            else:
                sleep_sec = idle_interval

        except Exception as e:
            log.exception("tick failed: %s", e)
            sleep_sec = idle_interval

        time.sleep(sleep_sec)




if __name__ == "__main__":
    main()
