# hockey/workers/hockey_reconcile_worker.py
from __future__ import annotations

import os
import time
import json
import logging
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import requests

from hockey.hockey_db import hockey_fetch_all, hockey_fetch_one, hockey_execute

log = logging.getLogger("hockey_reconcile_worker")
logging.basicConfig(level=logging.INFO)

BASE_URL = "https://v1.hockey.api-sports.io"

def ensure_event_key_migration() -> None:
    """
    reconcile worker가 ON CONFLICT (game_id, event_key)로 upsert할 때
    필요한 DB 요소(event_key 컬럼/유니크 인덱스/트리거)를 보장한다.
    """
    # 1) event_key 컬럼 보장 (이미 있으면 스킵)
    hockey_execute(
        """
        ALTER TABLE hockey_game_events
        ADD COLUMN IF NOT EXISTS event_key TEXT;
        """
    )

    # 2) (game_id, event_key) 유니크 인덱스 보장
    hockey_execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_hockey_game_events_game_event_key
        ON hockey_game_events (game_id, event_key);
        """
    )

    # 3) event_key 자동 계산 트리거 보장 (이미 있으면 교체)
    hockey_execute(
        """
        CREATE OR REPLACE FUNCTION hockey_game_events_set_event_key()
        RETURNS trigger AS $$
        BEGIN
          NEW.event_key :=
            lower(coalesce(NEW.type,'')) || '|' ||
            coalesce(NEW.period,'') || '|' ||
            coalesce(NEW.minute::text,'') || '|' ||
            coalesce(NEW.team_id::text,'') || '|' ||
            lower(coalesce(NEW.comment,'')) || '|' ||
            lower(coalesce(array_to_string(NEW.players,','),'')) || '|' ||
            lower(coalesce(array_to_string(NEW.assists,','),''));
          RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )

    hockey_execute(
        """
        DROP TRIGGER IF EXISTS trg_hockey_game_events_set_event_key ON hockey_game_events;

        CREATE TRIGGER trg_hockey_game_events_set_event_key
        BEFORE INSERT OR UPDATE ON hockey_game_events
        FOR EACH ROW
        EXECUTE FUNCTION hockey_game_events_set_event_key();
        """
    )

    # 4) event_key 백필 (NULL/빈값만)
    hockey_execute(
        """
        UPDATE hockey_game_events
        SET event_key =
          lower(coalesce(type,'')) || '|' ||
          coalesce(period,'') || '|' ||
          coalesce(minute::text,'') || '|' ||
          coalesce(team_id::text,'') || '|' ||
          lower(coalesce(comment,'')) || '|' ||
          lower(coalesce(array_to_string(players,','),'')) || '|' ||
          lower(coalesce(array_to_string(assists,','),''))
        WHERE event_key IS NULL OR event_key = '';
        """
    )



# ----------------------------
# env helpers
# ----------------------------
def _get_headers() -> Dict[str, str]:
    key = (os.getenv("APISPORTS_KEY") or os.getenv("API_SPORTS_KEY") or "").strip()
    if not key:
        raise RuntimeError("APISPORTS_KEY (or API_SPORTS_KEY) is not set")
    return {"x-apisports-key": key}


def _float_env(name: str, default: float) -> float:
    v = (os.getenv(name) or "").strip()
    if not v:
        return default
    try:
        return float(v)
    except Exception:
        return default


def _int_env(name: str, default: int) -> int:
    v = (os.getenv(name) or "").strip()
    if not v:
        return default
    try:
        return int(v)
    except Exception:
        return default


def _parse_int_csv(v: str) -> List[int]:
    out: List[int] = []
    for p in (v or "").split(","):
        s = p.strip()
        if not s:
            continue
        try:
            out.append(int(s))
        except Exception:
            continue
    return out


def _live_leagues() -> List[int]:
    # live worker와 동일하게 HOCKEY_LIVE_LEAGUES 사용
    return _parse_int_csv(os.getenv("HOCKEY_LIVE_LEAGUES", ""))


def _now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


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


# ----------------------------
# API
# ----------------------------
def _api_get_games_by_id(game_id: int) -> Optional[Dict[str, Any]]:
    """
    /games?id=XXXX 로 단일 게임 상세 조회.
    (API-Sports hockey v1 기준)
    """
    r = requests.get(
        f"{BASE_URL}/games",
        headers=_get_headers(),
        params={"id": game_id},
        timeout=45,
    )
    r.raise_for_status()
    data = r.json()
    resp = data.get("response") if isinstance(data, dict) else None
    if isinstance(resp, list) and resp and isinstance(resp[0], dict):
        return resp[0]
    return None


def _api_get_events(game_id: int) -> List[Dict[str, Any]]:
    r = requests.get(
        f"{BASE_URL}/games/events",
        headers=_get_headers(),
        params={"game": game_id},
        timeout=45,
    )
    r.raise_for_status()
    data = r.json()
    resp = data.get("response") if isinstance(data, dict) else None
    if not isinstance(resp, list):
        return []
    return [x for x in resp if isinstance(x, dict)]


# ----------------------------
# DB upsert (게임/이벤트)
# ----------------------------
def _extract_team_ids(item: Dict[str, Any]) -> Tuple[Optional[int], Optional[int]]:
    teams = item.get("teams")
    if not isinstance(teams, dict):
        return None, None

    home = teams.get("home")
    away = teams.get("away")
    home_id = _safe_int(home.get("id")) if isinstance(home, dict) else None
    away_id = _safe_int(away.get("id")) if isinstance(away, dict) else None
    return home_id, away_id


def upsert_game_from_api_item(item: Dict[str, Any]) -> Optional[int]:
    if not isinstance(item, dict):
        return None

    gid = _safe_int(item.get("id"))
    if gid is None:
        return None

    league_obj = item.get("league") if isinstance(item.get("league"), dict) else {}
    league_id = _safe_int(league_obj.get("id"))
    season = _safe_int(league_obj.get("season"))

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

    # league_id/season은 NOT NULL이므로, DB에 있는 값으로 fallback
    if league_id is None or season is None:
        row = hockey_fetch_one("SELECT league_id, season FROM hockey_games WHERE id=%s", (gid,))
        if row:
            league_id = league_id or _safe_int(row.get("league_id"))
            season = season or _safe_int(row.get("season"))

    if league_id is None or season is None:
        # 그래도 없으면 저장 불가
        log.warning("skip upsert (missing league/season): game_id=%s", gid)
        return None

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


def upsert_events(game_id: int, ev_list: List[Dict[str, Any]]) -> int:
    """
    Reconcile 목적:
    - 최신 API 스냅샷 기준으로 이벤트를 "교체(replace)"해야 정정/취소가 반영된다.
    - 기존 방식(UPSERT only)은 API에서 이벤트 정렬/내용이 바뀌면 과거 row가 남아서
      타임라인/period score가 꼬일 수 있다.
    """
    if not ev_list:
        return 0

    # 1) 정규화된 이벤트 레코드 만들기
    norm_rows: List[Tuple[str, Optional[int], Optional[int], str, Optional[str], List[str], List[str], Dict[str, Any]]] = []
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

        norm_rows.append((period, minute, team_id, etype, comment, players_arr, assists_arr, ev))

    if not norm_rows:
        return 0

    # 2) event_order를 "안정적으로" 재부여
    #    - 같은 (period, minute, team_id, type) 안에서
    #      (comment, players, assists, raw_json 문자열)로 정렬 → 0..N-1 부여
    grouped: Dict[Tuple[str, Optional[int], Optional[int], str], List[Tuple[Optional[str], List[str], List[str], Dict[str, Any]]]] = {}
    for period, minute, team_id, etype, comment, players_arr, assists_arr, raw in norm_rows:
        k = (period, minute, team_id, etype)
        grouped.setdefault(k, []).append((comment, players_arr, assists_arr, raw))

    # 3) DB에서 해당 게임 이벤트를 "전부 삭제" 후 최신 스냅샷으로 재삽입
    hockey_execute("DELETE FROM hockey_game_events WHERE game_id=%s", (game_id,))

    saved = 0
    for (period, minute, team_id, etype), items in grouped.items():
        items_sorted = sorted(
            items,
            key=lambda x: (
                (x[0] or ""),                # comment
                ",".join(x[1] or []),        # players
                ",".join(x[2] or []),        # assists
                _jdump(x[3])                 # raw_json
            ),
        )

        for event_order, (comment, players_arr, assists_arr, raw) in enumerate(items_sorted):
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
                  period = EXCLUDED.period,
                  minute = EXCLUDED.minute,
                  team_id = EXCLUDED.team_id,
                  type = EXCLUDED.type,
                  comment = EXCLUDED.comment,
                  players = EXCLUDED.players,
                  assists = EXCLUDED.assists,
                  event_order = EXCLUDED.event_order,
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
                    _jdump(raw),
                ),
            )
            saved += 1

    return saved



# ----------------------------
# reconcile targets
# ----------------------------
FINISHED_STATUSES = {"FT", "AET", "PEN", "FIN", "ENDED", "END"}
NOT_STARTED_STATUSES = {"NS", "TBD"}


def load_candidate_game_ids() -> List[int]:
    """
    리컨실 대상:
    - 지난 N일 ~ 앞으로 M일 범위의 경기 중
      (1) status가 FINISHED가 아닌 것
      (2) 또는 date가 바뀌었을 가능성이 큰 범위(근처 경기) 전체
    """
    leagues = _live_leagues()
    if not leagues:
        raise RuntimeError("HOCKEY_LIVE_LEAGUES is empty. ex) 57,58")

    past_days = _int_env("HOCKEY_RECONCILE_PAST_DAYS", 7)
    future_days = _int_env("HOCKEY_RECONCILE_FUTURE_DAYS", 3)

    rows = hockey_fetch_all(
        """
        SELECT id
        FROM hockey_games
        WHERE league_id = ANY(%s)
          AND game_date >= NOW() - (%s || ' days')::interval
          AND game_date <= NOW() + (%s || ' days')::interval
        ORDER BY game_date ASC
        """,
        (leagues, past_days, future_days),
    )
    return [int(r["id"]) for r in rows if r.get("id") is not None]


def should_refresh_events(status: str, game_date: Optional[dt.datetime]) -> bool:
    """
    이벤트는 너무 과거 경기까지 매번 긁지 않게 제한:
    - status가 NS/TBD면 스킵
    - game_date가 현재 기준 너무 오래 전이면 스킵(기본 3일)
    """
    s = (status or "").upper().strip()
    if s in NOT_STARTED_STATUSES:
        return False

    max_days = _int_env("HOCKEY_RECONCILE_EVENTS_MAX_AGE_DAYS", 3)
    if game_date is None:
        return True

    now = _now_utc()
    try:
        if (now - game_date).total_seconds() > (max_days * 86400):
            return False
    except Exception:
        return True

    return True


# ----------------------------
# main loop
# ----------------------------
def main() -> None:
    sleep_sec = _float_env("HOCKEY_RECONCILE_INTERVAL_SEC", 900.0)  # 기본 15분
    batch_limit = _int_env("HOCKEY_RECONCILE_BATCH_LIMIT", 300)

    log.info(
        "🧹 hockey reconcile worker start: interval=%.1fs leagues=%s",
        sleep_sec,
        _live_leagues(),
    )

    ensure_event_key_migration()
    log.info("ensure_event_key_migration: OK")

    while True:

        try:
            ids = load_candidate_game_ids()
            if batch_limit > 0:
                ids = ids[:batch_limit]

            log.info("reconcile tick: candidates=%s", len(ids))

            updated_games = 0
            updated_events = 0
            skipped = 0
            missing = 0

            for gid in ids:
                # DB 현재값(비교/로그용)
                db_row = hockey_fetch_one(
                    "SELECT id, status, game_date FROM hockey_games WHERE id=%s",
                    (gid,),
                )
                db_status = (db_row.get("status") if db_row else None) or ""
                db_date = db_row.get("game_date") if db_row else None

                api_item = None
                try:
                    api_item = _api_get_games_by_id(gid)
                except Exception as e:
                    log.warning("api games fetch failed: game_id=%s err=%s", gid, e)
                    continue

                if not api_item:
                    # API에서 사라진 경기: 일단 삭제는 위험(리그/시즌 전체 리컨실이 아니므로)
                    # → 로그만 남기고 스킵
                    missing += 1
                    continue

                new_id = upsert_game_from_api_item(api_item)
                if not new_id:
                    skipped += 1
                    continue
                updated_games += 1

                # 이벤트도 필요시 갱신
                # (최근 경기 + 시작된 경기만)
                # upsert_game 한 뒤 최신 status/game_date 다시 읽어 이벤트 판단
                cur = hockey_fetch_one("SELECT status, game_date FROM hockey_games WHERE id=%s", (gid,))
                cur_status = (cur.get("status") if cur else None) or db_status
                cur_date = cur.get("game_date") if cur else db_date

                if should_refresh_events(cur_status, cur_date):
                    try:
                        ev_list = _api_get_events(gid)
                        if ev_list:
                            saved = upsert_events(gid, ev_list)
                            updated_events += saved
                    except Exception as e:
                        log.warning("events reconcile failed: game_id=%s err=%s", gid, e)

            log.info(
                "reconcile done: games_upserted=%s events_upserted=%s skipped=%s missing_api=%s",
                updated_games,
                updated_events,
                skipped,
                missing,
            )

        except Exception as e:
            log.exception("reconcile tick failed: %s", e)

        time.sleep(sleep_sec)


if __name__ == "__main__":
    main()
