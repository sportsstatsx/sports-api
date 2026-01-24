from __future__ import annotations

import os
import time
import json
import zlib
import logging
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple, Iterable

import requests

from hockey.hockey_db import hockey_execute, hockey_fetch_all, hockey_fetch_one
from hockey.workers.hockey_live_common import now_utc

log = logging.getLogger("hockey_postmatch_backfill")
logging.basicConfig(level=logging.INFO)

BASE_URL = "https://v1.hockey.api-sports.io"


# ─────────────────────────────────────────
# Utilities (라이브워커와 동일 규칙)
# ─────────────────────────────────────────

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


def _parse_date(s: str) -> dt.date:
    return dt.date.fromisoformat(s.strip())


def _daterange(d1: dt.date, d2: dt.date) -> Iterable[dt.date]:
    cur = d1
    while cur <= d2:
        yield cur
        cur += dt.timedelta(days=1)


# ─────────────────────────────────────────
# event_key migration (라이브워커 동일)
# ─────────────────────────────────────────

def ensure_event_key_migration() -> None:
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
    hockey_execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_hockey_game_events_game_event_key
        ON hockey_game_events (game_id, event_key);
        """
    )


# ─────────────────────────────────────────
# Game / Events upsert (라이브워커 핵심 그대로)
# ─────────────────────────────────────────

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


def _is_finished_status(s: str, game_date: Optional[dt.datetime]) -> bool:
    x = (s or "").upper().strip()

    # ✅ API-Sports hockey 종료/확정 상태 보강
    if x in {
        "FT", "AET", "PEN", "FIN", "END", "ENDED",
        "AP",   # After Penalties  ✅ 핵심
        "AOT",  # After Overtime   (케이스 대비)
        "ABD", "AW", "CANC", "POST", "WO",
    }:
        return True

    if isinstance(game_date, dt.datetime):
        try:
            age = _utc_now() - game_date
            if age > dt.timedelta(hours=6):
                if x in {"NS", "TBD", "SUSP", "INT", "DELAYED"}:
                    return True
        except Exception:
            pass

    return False




# ─────────────────────────────────────────
# A안: API에서 날짜별 game 목록을 받아 백필
# ─────────────────────────────────────────

def _pick_leagues() -> List[int]:
    leagues = sorted(list(_int_set_env("HOCKEY_BACKFILL_LEAGUES")))
    if not leagues:
        raise RuntimeError("HOCKEY_BACKFILL_LEAGUES is empty. ex) 57,58")
    return leagues

def _resolve_backfill_season_by_league(leagues: List[int]) -> Dict[int, int]:
    """
    백필용 season 결정.

    우선순위:
    1) ENV HOCKEY_BACKFILL_SEASON 이 있으면 모든 league에 동일 적용 (가장 안정/권장)
    2) 없으면 DB(hockey_games)에서 league별 MAX(season)으로 추정
    3) 그래도 없으면 에러 → ENV로 지정 권장
    """
    out: Dict[int, int] = {}

    # (1) ENV 강제
    forced = (os.getenv("HOCKEY_BACKFILL_SEASON") or "").strip()
    if forced:
        try:
            ss = int(forced)
            for lid in leagues:
                out[int(lid)] = ss
            return out
        except Exception:
            raise RuntimeError(f"HOCKEY_BACKFILL_SEASON is invalid int: {forced}")

    # (2) DB에서 league별 MAX(season)
    try:
        rows = hockey_fetch_all(
            """
            SELECT league_id, MAX(season) AS season
            FROM hockey_games
            WHERE league_id = ANY(%s)
            GROUP BY league_id
            """,
            (leagues,),
        )
        for r in rows or []:
            lid = r.get("league_id")
            ss = r.get("season")
            if lid is None or ss is None:
                continue
            out[int(lid)] = int(ss)
    except Exception as e:
        log.warning("resolve backfill season(DB max) failed: %s", e)

    missing = [int(x) for x in leagues if int(x) not in out]
    if missing:
        raise RuntimeError(
            "Backfill season unresolved for leagues=%s. "
            "Set ENV HOCKEY_BACKFILL_SEASON=2025 (recommended)."
            % missing
        )

    return out



def _api_fetch_games(date_yyyy_mm_dd: str, league_id: int, season: int) -> List[Dict[str, Any]]:
    # ✅ API-sports hockey: /games?date=YYYY-MM-DD&league=ID&season=YYYY
    payload = _get(
        "/games",
        {"date": date_yyyy_mm_dd, "league": int(league_id), "season": int(season)},
    )
    resp = payload.get("response") if isinstance(payload, dict) else None
    if not isinstance(resp, list):
        return []
    return [x for x in resp if isinstance(x, dict)]



def _api_fetch_events(game_id: int) -> List[Dict[str, Any]]:
    payload = _get("/games/events", {"game": int(game_id)})
    resp = payload.get("response") if isinstance(payload, dict) else None
    if not isinstance(resp, list):
        return []
    return [x for x in resp if isinstance(x, dict)]


def run_backfill() -> None:
    start_s = (os.getenv("HOCKEY_BACKFILL_START_DATE") or "").strip()
    end_s = (os.getenv("HOCKEY_BACKFILL_END_DATE") or "").strip()
    if not start_s or not end_s:
        raise RuntimeError("HOCKEY_BACKFILL_START_DATE and HOCKEY_BACKFILL_END_DATE are required (YYYY-MM-DD)")

    start_d = _parse_date(start_s)
    end_d = _parse_date(end_s)
    if end_d < start_d:
        raise RuntimeError("end date < start date")

    leagues = _pick_leagues()

    # ✅ 핵심: backfill에서 /games는 season 없으면 0개가 나오는 리그가 많음
    season_by_league = _resolve_backfill_season_by_league(leagues)

    sleep_sec = _float_env("HOCKEY_BACKFILL_SLEEP_SEC", 0.35)
    only_finished = _int_env("HOCKEY_BACKFILL_ONLY_FINISHED", 1) == 1

    meta_on = _int_env("HOCKEY_BACKFILL_META", 0) == 1
    standings_on = _int_env("HOCKEY_BACKFILL_STANDINGS", 0) == 1

    # 로그에 season도 같이 찍어서 다음에 0개 삽질 방지
    log.info(
        "🏒 hockey backfill start: dates=%s..%s leagues=%s seasons=%s only_finished=%s sleep=%.2fs meta=%s standings=%s",
        start_d.isoformat(),
        end_d.isoformat(),
        leagues,
        {lid: season_by_league.get(int(lid)) for lid in leagues},
        only_finished,
        sleep_sec,
        meta_on,
        standings_on,
    )

    # 이벤트 유니크키 보장
    ensure_event_key_migration()
    log.info("ensure_event_key_migration: OK")

    # (옵션) meta/standings도 같이 (필요할 때만)
    if meta_on or standings_on:
        try:
            from hockey.workers.hockey_live_status_worker import (
                _meta_refresh_leagues_and_seasons,
                _meta_refresh_countries,
                _meta_refresh_teams_for_leagues,
                _refresh_standings_for_leagues,
            )
            if meta_on:
                log.info("meta refresh(start)")
                _meta_refresh_leagues_and_seasons(leagues)
                _meta_refresh_countries()
                _meta_refresh_teams_for_leagues(leagues)
                log.info("meta refresh(done)")
            if standings_on:
                log.info("standings refresh(start)")
                _refresh_standings_for_leagues(leagues)
                log.info("standings refresh(done)")
        except Exception as e:
            log.warning("meta/standings optional refresh failed: %s", e)

    total_games = 0
    total_events = 0
    total_days = 0

    for d in _daterange(start_d, end_d):
        total_days += 1
        day_str = d.isoformat()

        for lid in leagues:
            season = int(season_by_league.get(int(lid)) or 0)
            if season <= 0:
                # 여기 오면 시즌결정 실패인데, 위에서 보통 raise 되므로 보험
                log.warning("skip league(no season): date=%s league=%s", day_str, lid)
                continue

            try:
                games = _api_fetch_games(day_str, int(lid), season)
            except Exception as e:
                log.warning("fetch games failed: date=%s league=%s season=%s err=%s", day_str, lid, season, e)
                continue

            if not games:
                continue

            for item in games:
                gid = None
                try:
                    # fallback season도 우리가 쓴 season으로 고정
                    gid = upsert_game(item, int(lid), int(season))
                    if gid is not None:
                        total_games += 1
                except Exception as e:
                    log.warning("upsert_game failed: date=%s league=%s season=%s err=%s", day_str, lid, season, e)

                # events는 "종료 경기만" 기본
                if gid is None:
                    if sleep_sec > 0:
                        time.sleep(float(sleep_sec))
                    continue

                if only_finished:
                    row = hockey_fetch_one("SELECT status, game_date FROM hockey_games WHERE id=%s", (gid,))
                    db_status = (row.get("status") or "") if row else ""
                    db_date = row.get("game_date") if row else None
                    if not _is_finished_status(db_status, db_date):
                        if sleep_sec > 0:
                            time.sleep(float(sleep_sec))
                        continue

                try:
                    ev_list = _api_fetch_events(gid)
                    if ev_list:
                        upsert_events(gid, ev_list)
                        total_events += len(ev_list)
                except Exception as e:
                    log.warning("events backfill failed: game=%s date=%s league=%s season=%s err=%s", gid, day_str, lid, season, e)

                if sleep_sec > 0:
                    time.sleep(float(sleep_sec))

        log.info("day done: date=%s cumulative_games=%s cumulative_events=%s", day_str, total_games, total_events)

    log.info("✅ hockey backfill done: days=%s games_upserted=%s events_upserted=%s", total_days, total_games, total_events)



def main() -> None:
    run_backfill()


if __name__ == "__main__":
    main()
