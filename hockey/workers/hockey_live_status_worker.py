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

from hockey.hockey_db import hockey_execute
from hockey.workers.hockey_live_common import now_utc, hockey_live_leagues, interval_sec

log = logging.getLogger("hockey_live_status_worker")
logging.basicConfig(level=logging.INFO)

BASE_URL = "https://v1.hockey.api-sports.io"


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

    tz = _safe_text(item.get("timezone"))
    scores = item.get("scores") if isinstance(item.get("scores"), dict) else {}

    hockey_execute(
        """
        INSERT INTO hockey_games (
          id, league_id, season,
          stage, group_name,
          home_team_id, away_team_id,
          game_date, status, status_long, timezone,
          score_json, raw_json
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb)
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



def tick_once_for_date(league_id: int, season: int, target_date: dt.date) -> int:
    payload = _get("/games", {"league": league_id, "season": season, "date": target_date.isoformat()})
    resp = payload.get("response") if isinstance(payload, dict) else None
    if not isinstance(resp, list) or not resp:
        return 0

    updated = 0
    for item in resp:
        if not isinstance(item, dict):
            continue
        gid = upsert_game(item, league_id, season)
        if not gid:
            continue
        updated += 1

        # events 갱신
        try:
            ev_payload = _get("/games/events", {"game": gid})
            ev_resp = ev_payload.get("response") if isinstance(ev_payload, dict) else None
            if isinstance(ev_resp, list):
                ev_list = [x for x in ev_resp if isinstance(x, dict)]
                upsert_events(gid, ev_list)
        except Exception as e:
            log.warning("events fetch failed: game=%s err=%s", gid, e)

    return updated


def main() -> None:
    leagues = hockey_live_leagues()
    if not leagues:
        raise RuntimeError("HOCKEY_LIVE_LEAGUES is empty. ex) 57,58")

    season_env = (os.getenv("HOCKEY_SEASON") or "").strip()
    if season_env:
        try:
            season = int(season_env)
        except Exception:
            raise RuntimeError("HOCKEY_SEASON must be int")
    else:
        # 비워두면 현재 연도 기준 (너가 시즌을 2025로 쓴다면 env로 지정 추천)
        season = now_utc().year

    sleep_sec = interval_sec(25.0)
    log.info("🏒 hockey live worker start: leagues=%s season=%s interval=%.1fs", leagues, season, sleep_sec)

    while True:
        try:
            now = now_utc()
            dates = [
                (now - dt.timedelta(days=1)).date(),
                now.date(),
                (now + dt.timedelta(days=1)).date(),
            ]
            total = 0
            for lid in leagues:
                for d in dates:
                    total += tick_once_for_date(lid, season, d)

            log.info("tick done. games_upserted=%s dates=%s", total, [x.isoformat() for x in dates])
        except Exception as e:
            log.exception("tick failed: %s", e)

        time.sleep(sleep_sec)


if __name__ == "__main__":
    main()
