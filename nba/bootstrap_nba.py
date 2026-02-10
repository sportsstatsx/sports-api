# src/nba/bootstrap_nba.py
from __future__ import annotations

import os
import sys
import json
import time
import argparse
import datetime as dt
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

# DB driver: psycopg (v3) 우선, 없으면 psycopg2
try:
    import psycopg  # type: ignore
    _PSYCOPG_V3 = True
except Exception:
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
    _PSYCOPG_V3 = False


# -----------------------------
# HTTP
# -----------------------------
def _http_get_json(base: str, path: str, api_key: str, timeout: int = 30, retries: int = 4) -> Dict[str, Any]:
    """
    API-Sports NBA v2 호출 공통.
    - gzip/br 자동 처리: requests가 Accept-Encoding 처리
    - 429/5xx 재시도
    - 응답이 200인데 errors가 있는 경우: 그대로 리턴(상위에서 판단)
    """
    url = f"{base.rstrip('/')}/{path.lstrip('/')}"
    headers = {"x-apisports-key": api_key, "Accept": "application/json"}

    backoff = 1.0
    last_err: Optional[str] = None
    for i in range(retries + 1):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            if r.status_code == 429 or (500 <= r.status_code <= 599):
                # rate limit or server error
                last_err = f"HTTP {r.status_code}"
                time.sleep(backoff)
                backoff = min(backoff * 2.0, 10.0)
                continue

            # Cloudflare 등에서 HTML/빈 응답이 오면 json 파싱에서 터지므로 보호
            ct = (r.headers.get("content-type") or "").lower()
            if "application/json" not in ct:
                last_err = f"Non-JSON content-type={ct} status={r.status_code}"
                time.sleep(backoff)
                backoff = min(backoff * 2.0, 10.0)
                continue

            return r.json()
        except Exception as e:
            last_err = str(e)
            time.sleep(backoff)
            backoff = min(backoff * 2.0, 10.0)

    raise RuntimeError(f"GET failed after retries: {url} last_err={last_err}")


def _iso_now_utc() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _parse_iso_z(s: Optional[str]) -> Optional[dt.datetime]:
    if not s:
        return None
    # "2025-10-02T16:00:00.000Z" 형태
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))


# -----------------------------
# DB
# -----------------------------
def _conn():
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL is required")
    if _PSYCOPG_V3:
        return psycopg.connect(dsn)
    return psycopg2.connect(dsn)


def _exec(conn, sql: str, params: Optional[Tuple[Any, ...]] = None) -> None:
    if _PSYCOPG_V3:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
    else:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())


def _fetchall(conn, sql: str, params: Optional[Tuple[Any, ...]] = None) -> List[Dict[str, Any]]:
    if _PSYCOPG_V3:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql, params or ())
            return list(cur.fetchall())
    else:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or ())
            return list(cur.fetchall())


def _create_schema(conn) -> None:
    # JSONB raw 저장 + 최소 정규화(검색/조인용 키만)
    _exec(
        conn,
        """
        CREATE TABLE IF NOT EXISTS nba_fetch_state (
          id              TEXT PRIMARY KEY,              -- 예: "season:2025:league:standard"
          league          TEXT NOT NULL,
          season          INTEGER NOT NULL,
          stage           TEXT NOT NULL,                 -- meta/teams/players/games/standings/stats_done
          last_game_index INTEGER NOT NULL DEFAULT 0,    -- finished_game_ids 인덱스
          updated_utc     TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS nba_leagues (
          id          TEXT PRIMARY KEY,   -- ✅ "standard" 같은 league code
          raw_json    JSONB,
          updated_utc TEXT NOT NULL
        );


        CREATE TABLE IF NOT EXISTS nba_seasons (
          season      INTEGER PRIMARY KEY,
          raw_json    JSONB,
          updated_utc TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS nba_teams (
          id          INTEGER PRIMARY KEY,
          name        TEXT,
          nickname    TEXT,
          code        TEXT,
          city        TEXT,
          logo        TEXT,
          raw_json    JSONB,
          updated_utc TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS nba_players (
          id          INTEGER PRIMARY KEY,
          firstname   TEXT,
          lastname    TEXT,
          birth       TEXT,
          country     TEXT,
          height      TEXT,
          weight      TEXT,
          nba_start   INTEGER,
          affiliation TEXT,
          raw_json    JSONB,
          updated_utc TEXT NOT NULL
        );

        -- 팀-시즌 로스터 매핑 (이게 핵심)
        CREATE TABLE IF NOT EXISTS nba_team_rosters (
          team_id     INTEGER NOT NULL REFERENCES nba_teams(id),
          season      INTEGER NOT NULL,
          player_id   INTEGER NOT NULL REFERENCES nba_players(id),
          raw_json    JSONB,
          updated_utc TEXT NOT NULL,
          PRIMARY KEY(team_id, season, player_id)
        );

        CREATE TABLE IF NOT EXISTS nba_games (
          id            INTEGER PRIMARY KEY,
          league        TEXT,
          season        INTEGER,
          stage         INTEGER,
          status_long   TEXT,
          status_short  INTEGER,
          date_start_utc TIMESTAMPTZ,
          home_team_id  INTEGER,
          visitor_team_id INTEGER,
          arena_name    TEXT,
          arena_city    TEXT,
          arena_state   TEXT,
          raw_json      JSONB,
          updated_utc   TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_nba_games_season ON nba_games(season);
        CREATE INDEX IF NOT EXISTS idx_nba_games_date ON nba_games(date_start_utc);

        CREATE TABLE IF NOT EXISTS nba_standings (
          league      TEXT NOT NULL,
          season      INTEGER NOT NULL,
          team_id     INTEGER NOT NULL REFERENCES nba_teams(id),
          conference_name TEXT,
          conference_rank INTEGER,
          division_name TEXT,
          division_rank INTEGER,
          win         INTEGER,
          loss        INTEGER,
          streak      INTEGER,
          raw_json    JSONB,
          updated_utc TEXT NOT NULL,
          PRIMARY KEY(league, season, team_id)
        );

        -- 경기 팀 스탯 (games/statistics)
        CREATE TABLE IF NOT EXISTS nba_game_team_stats (
          game_id     INTEGER NOT NULL REFERENCES nba_games(id),
          team_id     INTEGER NOT NULL REFERENCES nba_teams(id),
          raw_json    JSONB,
          updated_utc TEXT NOT NULL,
          PRIMARY KEY(game_id, team_id)
        );

        -- 경기 선수 스탯 (players/statistics?game=)
        CREATE TABLE IF NOT EXISTS nba_game_player_stats (
          game_id     INTEGER NOT NULL REFERENCES nba_games(id),
          player_id   INTEGER NOT NULL REFERENCES nba_players(id),
          team_id     INTEGER,
          raw_json    JSONB,
          updated_utc TEXT NOT NULL,
          PRIMARY KEY(game_id, player_id)
        );

        CREATE INDEX IF NOT EXISTS idx_nba_gps_team ON nba_game_player_stats(team_id);
        """,
    )
    conn.commit()


def _upsert_json_by_id(conn, table: str, row_id: int, payload: Dict[str, Any], columns: Dict[str, Any]) -> None:
    """
    id PK 테이블 upsert 공통.
    columns에는 id 제외한 컬럼들(원하는 만큼) 넣으면 됨.
    raw_json/updated_utc는 내부에서 강제 세팅.
    """
    cols = ["id"] + list(columns.keys()) + ["raw_json", "updated_utc"]
    vals = [row_id] + list(columns.values()) + [json.dumps(payload), _iso_now_utc()]
    placeholders = ", ".join(["%s"] * len(cols))
    set_clause = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols[1:]])

    _exec(
        conn,
        f"""
        INSERT INTO {table} ({", ".join(cols)})
        VALUES ({placeholders})
        ON CONFLICT (id) DO UPDATE SET {set_clause}
        """,
        tuple(vals),
    )


def _get_state_id(league: str, season: int) -> str:
    return f"season:{season}:league:{league}"


def _load_state(conn, league: str, season: int) -> Dict[str, Any]:
    sid = _get_state_id(league, season)
    rows = _fetchall(conn, "SELECT * FROM nba_fetch_state WHERE id=%s", (sid,))
    if rows:
        return dict(rows[0])
    # default
    return {
        "id": sid,
        "league": league,
        "season": season,
        "stage": "init",
        "last_game_index": 0,
    }


def _save_state(conn, league: str, season: int, stage: str, last_game_index: int) -> None:
    sid = _get_state_id(league, season)
    _exec(
        conn,
        """
        INSERT INTO nba_fetch_state (id, league, season, stage, last_game_index, updated_utc)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (id) DO UPDATE SET
          stage=EXCLUDED.stage,
          last_game_index=EXCLUDED.last_game_index,
          updated_utc=EXCLUDED.updated_utc
        """,
        (sid, league, season, stage, int(last_game_index), _iso_now_utc()),
    )
    conn.commit()


# -----------------------------
# ETL
# -----------------------------
def ingest_meta(conn, base: str, api_key: str) -> None:
    leagues = _http_get_json(base, "leagues", api_key)
    seasons = _http_get_json(base, "seasons", api_key)

    # leagues: response = ["standard", ...] (list[str])
    lresp = leagues.get("response") or []
    for code in lresp:
        if not isinstance(code, str):
            raise RuntimeError(f"Unexpected leagues item type: {type(code)} value={code}")
        payload = {"code": code}
        _exec(
            conn,
            """
            INSERT INTO nba_leagues (id, raw_json, updated_utc)
            VALUES (%s,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
              raw_json=EXCLUDED.raw_json,
              updated_utc=EXCLUDED.updated_utc
            """,
            (code, json.dumps(payload), _iso_now_utc()),
        )

    # seasons: response = [2015,2016,...] (list[int])
    sresp = seasons.get("response") or []
    for s in sresp:
        if isinstance(s, int):
            season_val = s
            payload = {"season": season_val}
        else:
            # 혹시 dict로 오는 케이스 방어(근데 지금은 int임)
            season_val = int(s.get("season"))
            payload = dict(s)

        _exec(
            conn,
            """
            INSERT INTO nba_seasons (season, raw_json, updated_utc)
            VALUES (%s,%s,%s)
            ON CONFLICT (season) DO UPDATE SET
              raw_json=EXCLUDED.raw_json,
              updated_utc=EXCLUDED.updated_utc
            """,
            (season_val, json.dumps(payload), _iso_now_utc()),
        )

    conn.commit()



def ingest_teams(conn, base: str, api_key: str) -> List[int]:
    d = _http_get_json(base, "teams", api_key)
    teams = d.get("response") or []
    team_ids: List[int] = []
    for t in teams:
        tid = int(t.get("id"))
        team_ids.append(tid)
        cols = {
            "name": t.get("name"),
            "nickname": t.get("nickname"),
            "code": t.get("code"),
            "city": t.get("city"),
            "logo": t.get("logo"),
        }
        _upsert_json_by_id(conn, "nba_teams", tid, t, cols)
    conn.commit()
    return team_ids


def ingest_rosters(conn, base: str, api_key: str, season: int, team_ids: List[int]) -> None:
    for tid in team_ids:
        d = _http_get_json(base, f"players?team={tid}&season={season}", api_key)
        errs = d.get("errors") or []
        if errs:
            # 어떤 팀은 시즌 로스터가 없을 수도 있음(예: G리그/기타). 일단 skip.
            print(f"[roster] team={tid} season={season} errors={errs}")
            continue

        players = d.get("response") or []
        for p in players:
            pid = int(p.get("id"))
            cols = {
                "firstname": p.get("firstname"),
                "lastname": p.get("lastname"),
                "birth": (p.get("birth") or {}).get("date") if isinstance(p.get("birth"), dict) else p.get("birth"),
                "country": p.get("country"),
                "height": p.get("height"),
                "weight": p.get("weight"),
                "nba_start": (p.get("nba") or {}).get("start") if isinstance(p.get("nba"), dict) else None,
                "affiliation": p.get("affiliation"),
            }
            _upsert_json_by_id(conn, "nba_players", pid, p, cols)

            _exec(
                conn,
                """
                INSERT INTO nba_team_rosters (team_id, season, player_id, raw_json, updated_utc)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (team_id, season, player_id) DO UPDATE SET
                  raw_json=EXCLUDED.raw_json,
                  updated_utc=EXCLUDED.updated_utc
                """,
                (tid, season, pid, json.dumps(p), _iso_now_utc()),
            )

        conn.commit()
        time.sleep(0.15)  # 호출 과속 방지(너 key limit 300/분 느낌이라 안전하게)


def ingest_games(conn, base: str, api_key: str, league: str, season: int) -> List[int]:
    d = _http_get_json(base, f"games?league={league}&season={season}", api_key)
    games = d.get("response") or []
    finished_ids: List[int] = []

    for g in games:
        gid = int(g.get("id"))
        status = g.get("status") or {}
        status_long = status.get("long")
        status_short = status.get("short")
        if status_long == "Finished":
            finished_ids.append(gid)

        date_start = _parse_iso_z(((g.get("date") or {}).get("start")))

        teams = g.get("teams") or {}
        v_id = ((teams.get("visitors") or {}).get("id"))
        h_id = ((teams.get("home") or {}).get("id"))

        arena = g.get("arena") or {}

        _exec(
            conn,
            """
            INSERT INTO nba_games (
              id, league, season, stage, status_long, status_short, date_start_utc,
              home_team_id, visitor_team_id,
              arena_name, arena_city, arena_state,
              raw_json, updated_utc
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
              league=EXCLUDED.league,
              season=EXCLUDED.season,
              stage=EXCLUDED.stage,
              status_long=EXCLUDED.status_long,
              status_short=EXCLUDED.status_short,
              date_start_utc=EXCLUDED.date_start_utc,
              home_team_id=EXCLUDED.home_team_id,
              visitor_team_id=EXCLUDED.visitor_team_id,
              arena_name=EXCLUDED.arena_name,
              arena_city=EXCLUDED.arena_city,
              arena_state=EXCLUDED.arena_state,
              raw_json=EXCLUDED.raw_json,
              updated_utc=EXCLUDED.updated_utc
            """,
            (
                gid,
                league,
                season,
                int(g.get("stage") or 0),
                status_long,
                int(status_short) if status_short is not None else None,
                date_start,
                int(h_id) if h_id is not None else None,
                int(v_id) if v_id is not None else None,
                arena.get("name"),
                arena.get("city"),
                arena.get("state"),
                json.dumps(g),
                _iso_now_utc(),
            ),
        )

    conn.commit()
    return finished_ids


def ingest_standings(conn, base: str, api_key: str, league: str, season: int) -> None:
    d = _http_get_json(base, f"standings?league={league}&season={season}", api_key)
    rows = d.get("response") or []

    for r in rows:
        team = r.get("team") or {}
        tid = int(team.get("id"))
        conf = r.get("conference") or {}
        div = r.get("division") or {}

        _exec(
            conn,
            """
            INSERT INTO nba_standings (
              league, season, team_id,
              conference_name, conference_rank,
              division_name, division_rank,
              win, loss, streak,
              raw_json, updated_utc
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (league, season, team_id) DO UPDATE SET
              conference_name=EXCLUDED.conference_name,
              conference_rank=EXCLUDED.conference_rank,
              division_name=EXCLUDED.division_name,
              division_rank=EXCLUDED.division_rank,
              win=EXCLUDED.win,
              loss=EXCLUDED.loss,
              streak=EXCLUDED.streak,
              raw_json=EXCLUDED.raw_json,
              updated_utc=EXCLUDED.updated_utc
            """,
            (
                league,
                season,
                tid,
                conf.get("name"),
                int(conf.get("rank")) if conf.get("rank") is not None else None,
                div.get("name"),
                int(div.get("rank")) if div.get("rank") is not None else None,
                int((r.get("win") or {}).get("total")) if isinstance(r.get("win"), dict) and (r.get("win") or {}).get("total") is not None else None,
                int((r.get("loss") or {}).get("total")) if isinstance(r.get("loss"), dict) and (r.get("loss") or {}).get("total") is not None else None,
                int(r.get("streak")) if r.get("streak") is not None else None,
                json.dumps(r),
                _iso_now_utc(),
            ),
        )

    conn.commit()


def ingest_game_stats(conn, base: str, api_key: str, game_id: int) -> None:
    # 팀 스탯
    d = _http_get_json(base, f"games/statistics?id={game_id}", api_key)
    for trow in (d.get("response") or []):
        team = trow.get("team") or {}
        tid = team.get("id")
        if tid is None:
            continue
        tid = int(tid)
        _exec(
            conn,
            """
            INSERT INTO nba_game_team_stats (game_id, team_id, raw_json, updated_utc)
            VALUES (%s,%s,%s,%s)
            ON CONFLICT (game_id, team_id) DO UPDATE SET
              raw_json=EXCLUDED.raw_json,
              updated_utc=EXCLUDED.updated_utc
            """,
            (int(game_id), tid, json.dumps(trow), _iso_now_utc()),
        )

    # 선수 스탯
    p = _http_get_json(base, f"players/statistics?game={game_id}", api_key)
    for prow in (p.get("response") or []):
        player = prow.get("player") or {}
        pid = player.get("id")
        if pid is None:
            continue
        pid = int(pid)
        team = prow.get("team") or {}
        tid = team.get("id")
        tid_i = int(tid) if tid is not None else None

        # 선수 기본정보 upsert(있으면 좋고 없어도 됨)
        cols = {
            "firstname": player.get("firstname"),
            "lastname": player.get("lastname"),
            "birth": None,
            "country": None,
            "height": None,
            "weight": None,
            "nba_start": None,
            "affiliation": None,
        }
        _upsert_json_by_id(conn, "nba_players", pid, {"player": player}, cols)

        _exec(
            conn,
            """
            INSERT INTO nba_game_player_stats (game_id, player_id, team_id, raw_json, updated_utc)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (game_id, player_id) DO UPDATE SET
              team_id=EXCLUDED.team_id,
              raw_json=EXCLUDED.raw_json,
              updated_utc=EXCLUDED.updated_utc
            """,
            (int(game_id), pid, tid_i, json.dumps(prow), _iso_now_utc()),
        )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", default="standard")
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--include-stage1", action="store_true", help="preseason(stage=1) finished도 stats까지 백필")
    ap.add_argument("--stats", action="store_true", help="finished game stats까지 수행")
    args = ap.parse_args()

    base = os.environ.get("NBA_BASE", "https://v2.nba.api-sports.io")
    api_key = os.environ.get("API_KEY")
    if not api_key:
        raise RuntimeError("API_KEY is required")

    league = args.league
    season = int(args.season)

    with _conn() as conn:
        _create_schema(conn)

        state = _load_state(conn, league, season)
        stage = state.get("stage") or "init"
        last_game_index = int(state.get("last_game_index") or 0)

        # 1) meta
        if stage in ("init", "meta"):
            print("[1/6] meta leagues/seasons")
            ingest_meta(conn, base, api_key)
            _save_state(conn, league, season, "teams", last_game_index)
            stage = "teams"

        # 2) teams
        if stage in ("teams", "init", "meta"):
            print("[2/6] teams")
            team_ids = ingest_teams(conn, base, api_key)
            _save_state(conn, league, season, "players", last_game_index)
            stage = "players"
        else:
            team_ids = [int(r["id"]) for r in _fetchall(conn, "SELECT id FROM nba_teams ORDER BY id")]

        # 3) rosters (팀별 시즌 로스터)
        if stage in ("players", "init", "meta", "teams"):
            print(f"[3/6] rosters season={season} teams={len(team_ids)}")
            ingest_rosters(conn, base, api_key, season, team_ids)
            _save_state(conn, league, season, "games", last_game_index)
            stage = "games"

        # 4) games
        if stage in ("games", "init", "meta", "teams", "players"):
            print(f"[4/6] games league={league} season={season}")
            finished_ids = ingest_games(conn, base, api_key, league, season)
            _save_state(conn, league, season, "standings", last_game_index)
            stage = "standings"
        else:
            finished_ids = [int(r["id"]) for r in _fetchall(conn, "SELECT id FROM nba_games WHERE season=%s AND status_long='Finished' ORDER BY id", (season,))]

        # stage 필터(프리시즌 stats 포함 여부)
        if not args.include_stage1:
            stage2_ids = set(
                int(r["id"])
                for r in _fetchall(
                    conn,
                    "SELECT id FROM nba_games WHERE season=%s AND stage=2 AND status_long='Finished'",
                    (season,),
                )
            )
            finished_ids = [gid for gid in finished_ids if gid in stage2_ids]

        # 5) standings  ✅ 이제 첫 실행에서도 무조건 탐
        if stage in ("standings", "init", "meta", "teams", "players", "games"):
            print(f"[5/6] standings league={league} season={season}")
            ingest_standings(conn, base, api_key, league, season)
            _save_state(conn, league, season, "stats", last_game_index)
            stage = "stats"


        # 6) stats (옵션)
        if args.stats:
            print(f"[6/6] stats finished_games={len(finished_ids)} resume_index={last_game_index}")
            for idx in range(last_game_index, len(finished_ids)):
                gid = finished_ids[idx]
                ingest_game_stats(conn, base, api_key, gid)
                conn.commit()

                # 진행상태 저장(재시작 안전)
                if idx % 10 == 0:
                    _save_state(conn, league, season, "stats", idx)

                # 과속 방지
                time.sleep(0.12)

            _save_state(conn, league, season, "stats_done", len(finished_ids))
            print("[DONE] stats_done")
        else:
            print("[SKIP] stats not requested (use --stats)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
