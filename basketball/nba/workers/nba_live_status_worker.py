# basketball/nba/workers/nba_live_status_worker.py
from __future__ import annotations

import os
import time
import json
import logging
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg

from basketball.nba.bootstrap_nba import ingest_game_stats

log = logging.getLogger("nba_live_status_worker")
logging.basicConfig(level=logging.INFO)

BASE_URL = os.getenv("NBA_BASE", "https://v2.nba.api-sports.io").rstrip("/")

# game_id별 stats 마지막 호출 시각(UTC timestamp)
_last_stats_ts_by_game: Dict[int, float] = {}


# ─────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────

def _dsn() -> str:
    dsn = (os.getenv("NBA_DATABASE_URL") or os.getenv("DATABASE_URL") or "").strip()
    if not dsn:
        raise RuntimeError("NBA_DATABASE_URL (or DATABASE_URL) is not set")
    return dsn


def _db_fetch_one(sql: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
    with psycopg.connect(_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            if not row:
                return None
            cols = [d.name for d in cur.description]
            return {cols[i]: row[i] for i in range(len(cols))}


def _db_fetch_all(sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
    with psycopg.connect(_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            cols = [d.name for d in cur.description]
            return [{cols[i]: r[i] for i in range(len(cols))} for r in rows]


def _db_execute(sql: str, params: tuple = ()) -> None:
    with psycopg.connect(_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()


# ─────────────────────────────────────────
# API helpers
# ─────────────────────────────────────────

def _headers() -> Dict[str, str]:
    key = (os.getenv("API_KEY") or os.getenv("APISPORTS_KEY") or os.getenv("API_SPORTS_KEY") or "").strip()
    if not key:
        raise RuntimeError("API_KEY (or APISPORTS_KEY/API_SPORTS_KEY) is not set")
    return {"x-apisports-key": key}


def _get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.get(
        f"{BASE_URL}{path}",
        headers=_headers(),
        params=params,
        timeout=45,
    )
    r.raise_for_status()
    data = r.json()

    # ✅ API-Sports는 HTTP 200이어도 errors로 실패할 수 있음
    errs = data.get("errors") if isinstance(data, dict) else None
    if isinstance(errs, dict) and errs:
        raise RuntimeError(f"API-Sports error: {errs}")

    return data



def _jdump(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)


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


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


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


# ─────────────────────────────────────────
# NBA status helpers
# ─────────────────────────────────────────
# API-Sports NBA에서 status.long이 예: "Scheduled", "In Play", "Live", "Halftime", "Finished"
LIVE_STATUS_LONG = {"In Play", "Live", "Halftime"}
FINISHED_STATUS_LONG = {"Finished"}
NOT_STARTED_STATUS_LONG = {"Scheduled"}


def _is_finished_status(status_long: str, start_utc: Optional[dt.datetime]) -> bool:
    x = (status_long or "").strip()
    if x in FINISHED_STATUS_LONG:
        return True

    # 시간 기반 fallback: 시작시간이 오래 전인데도 Scheduled로 남아있는 경우
    if isinstance(start_utc, dt.datetime):
        try:
            age = _utc_now() - start_utc
            if age > dt.timedelta(hours=6) and x in NOT_STARTED_STATUS_LONG:
                return True
        except Exception:
            pass
    return False


def _is_not_started(status_long: str) -> bool:
    return (status_long or "").strip() in NOT_STARTED_STATUS_LONG


# ─────────────────────────────────────────
# poll_state
# ─────────────────────────────────────────

def _poll_state_get_or_create(game_id: int) -> Dict[str, Any]:
    row = _db_fetch_one("SELECT * FROM nba_live_poll_state WHERE game_id=%s", (game_id,))
    if row:
        return dict(row)

    _db_execute(
        "INSERT INTO nba_live_poll_state (game_id) VALUES (%s) ON CONFLICT DO NOTHING",
        (game_id,),
    )
    row2 = _db_fetch_one("SELECT * FROM nba_live_poll_state WHERE game_id=%s", (game_id,))
    return dict(row2) if row2 else {"game_id": game_id}


def _poll_state_update(game_id: int, **cols: Any) -> None:
    if not cols:
        return
    keys = list(cols.keys())
    sets = ", ".join([f"{k}=%s" for k in keys])
    values = [cols[k] for k in keys]
    _db_execute(
        f"UPDATE nba_live_poll_state SET {sets}, updated_at=now() WHERE game_id=%s",
        tuple(values + [game_id]),
    )


# ─────────────────────────────────────────
# candidates window loader
# ─────────────────────────────────────────

def _load_live_window_game_rows() -> List[Dict[str, Any]]:
    """
    NBA는 league_id 개념 대신 league='standard' 중심.
    후보:
      (1) pre: now ~ now+pre_min
      (2) in-play: now - inplay_max_min ~ now + grace_min, 그리고 Finished 제외
    """
    pre_min = _int_env("NBA_LIVE_PRESTART_MIN", 60)
    inplay_max_min = _int_env("NBA_LIVE_INPLAY_MAX_MIN", 240)
    grace_min = _int_env("NBA_LIVE_FUTURE_GRACE_MIN", 2)
    batch_limit = _int_env("NBA_LIVE_BATCH_LIMIT", 120)

    now = _utc_now()
    upcoming_end = now + dt.timedelta(minutes=pre_min)

    inplay_start = now - dt.timedelta(minutes=inplay_max_min)
    inplay_end = now + dt.timedelta(minutes=grace_min)

    rows = _db_fetch_all(
        """
        SELECT
          g.id,
          g.league,
          g.season,
          g.date_start_utc,
          g.status_long
        FROM nba_games g
        LEFT JOIN nba_live_poll_state ps
          ON ps.game_id = g.id
        WHERE g.league = 'standard'
          AND (
            -- (1) 프리 윈도우: now ~ now+pre_min
            (g.date_start_utc >= %s AND g.date_start_utc <= %s)
            OR
            -- (2) 라이브/진행 윈도우: now-inplay_max ~ now+grace (Finished만 제외)
            (
              g.date_start_utc >= %s
              AND g.date_start_utc <= %s
              AND COALESCE(g.status_long,'') <> 'Finished'
            )
          )
        ORDER BY g.date_start_utc ASC
        LIMIT %s
        """,
        (
            now, upcoming_end,
            inplay_start, inplay_end,
            batch_limit,
        ),
    )

    return rows


# ─────────────────────────────────────────
# upsert game snapshot
# ─────────────────────────────────────────

def _api_get_game_by_id(game_id: int) -> Optional[Dict[str, Any]]:
    payload = _get("/games", {"id": int(game_id)})
    resp = payload.get("response") if isinstance(payload, dict) else None
    if isinstance(resp, list) and resp and isinstance(resp[0], dict):
        return resp[0]
    return None


def upsert_game(api_item: Dict[str, Any]) -> Optional[int]:
    """
    nba_games에 스냅샷 반영 (✅ 너 DB 스키마에 100% 맞춤)

    nba_games columns:
      id, league, season, stage,
      status_long, status_short,
      date_start_utc,
      home_team_id, visitor_team_id,
      arena_name, arena_city, arena_state,
      raw_json, updated_utc
    """
    gid = _safe_int(api_item.get("id"))
    if gid is None:
        return None

    # league/season/stage
    league = _safe_text(api_item.get("league")) or "standard"
    season = _safe_int(api_item.get("season"))
    stage = _safe_int(api_item.get("stage"))

    # date.start
    date_obj = api_item.get("date") if isinstance(api_item.get("date"), dict) else {}
    start_str = date_obj.get("start")
    start_utc: Optional[dt.datetime] = None
    if isinstance(start_str, str) and start_str:
        try:
            start_utc = dt.datetime.fromisoformat(start_str.replace("Z", "+00:00"))
        except Exception:
            start_utc = None

    # status
    status_obj = api_item.get("status") if isinstance(api_item.get("status"), dict) else {}
    status_long = _safe_text(status_obj.get("long"))
    if not status_long:
        # fallback: 혹시 다른 키로 오거나 비정상 케이스 방어
        status_long = _safe_text(api_item.get("status_long")) or _safe_text(api_item.get("status"))


    

    # 너 컬럼 status_short = integer
    status_short = _safe_int(status_obj.get("short"))

    # teams (API-Sports NBA: teams.home / teams.visitors)
    teams = api_item.get("teams") if isinstance(api_item.get("teams"), dict) else {}
    home = teams.get("home") if isinstance(teams.get("home"), dict) else {}
    visitors = teams.get("visitors") if isinstance(teams.get("visitors"), dict) else {}

    home_team_id = _safe_int(home.get("id"))
    visitor_team_id = _safe_int(visitors.get("id"))

    # arena
    arena = api_item.get("arena") if isinstance(api_item.get("arena"), dict) else {}
    arena_name = _safe_text(arena.get("name"))
    arena_city = _safe_text(arena.get("city"))
    arena_state = _safe_text(arena.get("state"))

    # updated_utc: text 컬럼이므로 ISO 문자열로
    updated_utc = _utc_now().isoformat()

    _db_execute(
        """
        INSERT INTO nba_games (
          id,
          league, season, stage,
          status_long, status_short,
          date_start_utc,
          home_team_id, visitor_team_id,
          arena_name, arena_city, arena_state,
          raw_json,
          updated_utc
        )
        VALUES (
          %s,
          %s,%s,%s,
          %s,%s,
          %s,
          %s,%s,
          %s,%s,%s,
          %s::jsonb,
          %s
        )
        ON CONFLICT (id) DO UPDATE SET
          league = EXCLUDED.league,
          season = EXCLUDED.season,
          stage = EXCLUDED.stage,
          status_long = EXCLUDED.status_long,
          status_short = EXCLUDED.status_short,
          date_start_utc = EXCLUDED.date_start_utc,
          home_team_id = EXCLUDED.home_team_id,
          visitor_team_id = EXCLUDED.visitor_team_id,
          arena_name = EXCLUDED.arena_name,
          arena_city = EXCLUDED.arena_city,
          arena_state = EXCLUDED.arena_state,
          raw_json = EXCLUDED.raw_json,
          updated_utc = EXCLUDED.updated_utc
        """,
        (
            gid,
            league, season, stage,
            status_long, status_short,
            start_utc,
            home_team_id, visitor_team_id,
            arena_name, arena_city, arena_state,
            _jdump(api_item),
            updated_utc,
        ),
    )
    return gid



def _try_ingest_game_stats(game_id: int) -> None:
    """
    ✅ 너가 이미 갖고 있는 ingest_game_stats 재사용.
    - live 중에도 호출해도 됨(네 DB/요금 상황에 따라 빈도 조절)
    """
    try:
        ingest_game_stats(game_id=game_id)
    except Exception as e:
        log.info("ingest_game_stats skipped/failed: game=%s err=%s", game_id, e)


# ─────────────────────────────────────────
# tick core (windowed)
# ─────────────────────────────────────────

def tick_once_windowed(
    rows: List[Dict[str, Any]],
    *,
    pre_min: int,
    post_min: int,
    live_interval_sec: float,
    stats_interval_sec: float,
) -> Tuple[int, int, int]:
    """
    하키 tick 구조를 NBA로 이식:
      - pre 1회
      - start 1회
      - live 주기 (games snapshot)
      - (옵션) stats 주기 (ingest_game_stats)
      - end 1회
      - post 1회
    """
    if not rows:
        return (0, 0, 0)

    games_upserted = 0
    stats_called = 0
    now = _utc_now()

    for r in rows:
        gid = int(r["id"])
        db_status_long = (r.get("status_long") or "").strip()
        db_start = r.get("date_start_utc")
        if isinstance(db_start, str):
            # 혹시 text로 저장된 환경 방어
            try:
                db_start = dt.datetime.fromisoformat(db_start.replace("Z", "+00:00"))
            except Exception:
                db_start = None

        st = _poll_state_get_or_create(gid)
        pre_called_at = st.get("pre_called_at")
        start_called_at = st.get("start_called_at")
        end_called_at = st.get("end_called_at")
        post_called_at = st.get("post_called_at")
        finished_at = st.get("finished_at")
        next_live_poll_at = st.get("next_live_poll_at")

        # (A) pre 1회
        if (
            pre_called_at is None
            and isinstance(db_start, dt.datetime)
            and (db_start - dt.timedelta(minutes=pre_min)) <= now < db_start
        ):
            try:
                api_item = _api_get_game_by_id(gid)
                if isinstance(api_item, dict):
                    upsert_game(api_item)
                    games_upserted += 1
                    _poll_state_update(gid, pre_called_at=now)
            except Exception as e:
                log.warning("pre-call /games?id failed: game=%s err=%s", gid, e)
            continue

        # (B) start 1회 (now>=start & not finished)
        if (
            start_called_at is None
            and isinstance(db_start, dt.datetime)
            and now >= db_start
            and not _is_finished_status(db_status_long, db_start)
        ):
            try:
                api_item = _api_get_game_by_id(gid)
                if isinstance(api_item, dict):
                    upsert_game(api_item)
                    games_upserted += 1
                    _poll_state_update(gid, start_called_at=now)

                    cur = _db_fetch_one("SELECT status_long, date_start_utc FROM nba_games WHERE id=%s", (gid,))
                    if cur:
                        db_status_long = (cur.get("status_long") or db_status_long).strip()
                        db_start = cur.get("date_start_utc") or db_start
            except Exception as e:
                log.warning("start-call /games?id failed: game=%s err=%s", gid, e)

        # (C) end 1회
        if _is_finished_status(db_status_long, db_start) and end_called_at is None:
            try:
                api_item = _api_get_game_by_id(gid)
                if isinstance(api_item, dict):
                    upsert_game(api_item)
                    games_upserted += 1
                    _poll_state_update(gid, end_called_at=now, finished_at=now)
            except Exception as e:
                log.warning("end-call /games?id failed: game=%s err=%s", gid, e)
            continue

        # (D) post 1회 (finished + post_min)
        if (
            finished_at is not None
            and post_called_at is None
            and isinstance(finished_at, dt.datetime)
            and now >= (finished_at + dt.timedelta(minutes=post_min))
        ):
            try:
                api_item = _api_get_game_by_id(gid)
                if isinstance(api_item, dict):
                    upsert_game(api_item)
                    games_upserted += 1
                    _poll_state_update(gid, post_called_at=now)
            except Exception as e:
                log.warning("post-call /games?id failed: game=%s err=%s", gid, e)
            continue

        # (E) live periodic
        # ✅ start_called_at 이후에는 status_long이 Scheduled로 남아도(전환 지연) /games는 계속 폴링
        if (start_called_at is not None) and (not _is_finished_status(db_status_long, db_start)):
            due = False
            if next_live_poll_at is None:
                due = True
            else:
                try:
                    due = now >= next_live_poll_at
                except Exception:
                    due = True

            if due:
                # 1) /games snapshot
                try:
                    api_item = _api_get_game_by_id(gid)
                    if isinstance(api_item, dict):
                        upsert_game(api_item)
                        games_upserted += 1

                        cur = _db_fetch_one("SELECT status_long, date_start_utc FROM nba_games WHERE id=%s", (gid,))
                        if cur:
                            db_status_long = (cur.get("status_long") or db_status_long).strip()
                            db_start = cur.get("date_start_utc") or db_start
                except Exception as e:
                    log.warning("live /games?id failed: game=%s err=%s", gid, e)
                    _poll_state_update(gid, next_live_poll_at=now + dt.timedelta(seconds=max(5.0, float(live_interval_sec))))
                    continue

                # 2) stats (너 비용/부하 고려해서 더 느리게)
                #    - 라이브/하프타임일 때만 호출 권장
                if db_status_long in LIVE_STATUS_LONG:
                    now_ts = time.time()
                    last_ts = _last_stats_ts_by_game.get(gid, 0.0)

                    # ✅ stats_interval_sec마다만 호출
                    if (now_ts - last_ts) >= float(stats_interval_sec):
                        try:
                            _try_ingest_game_stats(gid)
                            stats_called += 1
                            _last_stats_ts_by_game[gid] = now_ts
                        except Exception:
                            # 실패해도 last_ts 갱신 안 해서 다음에 재시도됨
                            pass


                _poll_state_update(gid, next_live_poll_at=now + dt.timedelta(seconds=float(live_interval_sec)))

    return (games_upserted, stats_called, len(rows))


# ─────────────────────────────────────────
# main loop
# ─────────────────────────────────────────

def main() -> None:
    # intervals
    pre_min = _int_env("NBA_LIVE_PRESTART_MIN", 60)
    post_min = _int_env("NBA_LIVE_POSTEND_MIN", 30)

    live_interval_sec = _float_env("NBA_LIVE_INTERVAL_SEC", 10.0)
    idle_interval_sec = _float_env("NBA_LIVE_IDLE_INTERVAL_SEC", 180.0)

    # stats는 더 느리게(지금은 live_interval과 같이 호출되지만,
    # 정말 분리하려면 poll_state에 next_stats_poll_at 추가 추천)
    stats_interval_sec = _float_env("NBA_STATS_INTERVAL_SEC", 30.0)

    log.info(
        "🏀 nba live worker(start): pre=%sm post=%sm live=%.1fs idle=%.1fs stats_hint=%.1fs base=%s",
        pre_min, post_min, live_interval_sec, idle_interval_sec, stats_interval_sec, BASE_URL
    )

    # (선택) poll_state 테이블 존재 보장 (원하면 여기서 create)
    _db_execute(
        """
        CREATE TABLE IF NOT EXISTS nba_live_poll_state (
          game_id           INTEGER PRIMARY KEY,
          pre_called_at     TIMESTAMPTZ,
          start_called_at   TIMESTAMPTZ,
          end_called_at     TIMESTAMPTZ,
          post_called_at    TIMESTAMPTZ,
          finished_at       TIMESTAMPTZ,
          next_live_poll_at TIMESTAMPTZ,
          updated_at        TIMESTAMPTZ DEFAULT now()
        );
        """
    )


    while True:
        try:
            rows = _load_live_window_game_rows()
            if not rows:
                time.sleep(idle_interval_sec)
                continue

            g_up, s_up, cand = tick_once_windowed(
                rows,
                pre_min=pre_min,
                post_min=post_min,
                live_interval_sec=live_interval_sec,
                stats_interval_sec=stats_interval_sec,
            )
            log.info("tick done: candidates=%s games_upserted=%s stats_called=%s", cand, g_up, s_up)

            # 너무 빡세게 돌지 않게 약간 sleep (per-league 분리 안 했으니 단순)
            time.sleep(min(1.0, max(0.2, float(live_interval_sec) / 5.0)))

        except Exception as e:
            log.exception("tick failed: %s", e)
            time.sleep(idle_interval_sec)


if __name__ == "__main__":
    main()
