from __future__ import annotations

import os
import time
import argparse
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from db import execute, fetch_all  # 너 프로젝트에서 이미 쓰는 db 헬퍼

log = logging.getLogger("hockey_backfill")
logging.basicConfig(level=logging.INFO)


# =========================
# API-Sports Hockey Client
# =========================
class HockeyApi:
    def __init__(self, api_key: str, base_url: str = "https://v1.hockey.api-sports.io"):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        headers = {
            "x-apisports-key": self.api_key,
        }
        r = requests.get(url, headers=headers, params=params or {}, timeout=30)
        r.raise_for_status()
        return r.json()

    def leagues(self) -> Dict[str, Any]:
        return self.get("/leagues")

    def games(self, league_id: int, season: int, page: int = 1) -> Dict[str, Any]:
        # API-Sports는 보통 page/pagination 구조가 있음
        return self.get("/games", {"league": league_id, "season": season, "page": page})

    def standings(self, league_id: int, season: int) -> Dict[str, Any]:
        return self.get("/standings", {"league": league_id, "season": season})


# =========================
# DB UPSERTS
# =========================
def upsert_country(country: Dict[str, Any]) -> None:
    # country: {id, name, code, flag}
    sql = """
    INSERT INTO hockey_countries (id, name, code, flag)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE SET
      name = EXCLUDED.name,
      code = EXCLUDED.code,
      flag = EXCLUDED.flag;
    """
    execute(sql, (country.get("id"), country.get("name"), country.get("code"), country.get("flag")))


def upsert_league(league: Dict[str, Any], country_id: Optional[int]) -> None:
    # league: {id, name, type, logo}
    sql = """
    INSERT INTO hockey_leagues (id, name, type, logo, country_id)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE SET
      name = EXCLUDED.name,
      type = EXCLUDED.type,
      logo = EXCLUDED.logo,
      country_id = EXCLUDED.country_id;
    """
    execute(sql, (league.get("id"), league.get("name"), league.get("type"), league.get("logo"), country_id))


def upsert_league_season(league_id: int, s: Dict[str, Any]) -> None:
    # seasons[]: {season, current, start, end}
    sql = """
    INSERT INTO hockey_league_seasons (league_id, season, current, start_date, end_date)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (league_id, season) DO UPDATE SET
      current = EXCLUDED.current,
      start_date = EXCLUDED.start_date,
      end_date = EXCLUDED.end_date;
    """
    execute(sql, (league_id, s.get("season"), bool(s.get("current", False)), s.get("start"), s.get("end")))


def upsert_team(team: Dict[str, Any], country_id: Optional[int]) -> None:
    # team: {id, name, logo}
    sql = """
    INSERT INTO hockey_teams (id, name, logo, country_id)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE SET
      name = EXCLUDED.name,
      logo = EXCLUDED.logo,
      country_id = EXCLUDED.country_id;
    """
    execute(sql, (team.get("id"), team.get("name"), team.get("logo"), country_id))


def upsert_game(row: Dict[str, Any]) -> None:
    """
    API-Sports hockey /games 응답은 보통 row 안에
    game(혹은 fixture), league, teams, scores/status 등이 들어있음.
    우리는 유연하게 raw_json/score_json에 넣어버리고 핵심만 뽑아 저장.
    """
    game_obj = row.get("game") or row.get("fixture") or {}
    league_obj = row.get("league") or {}
    teams_obj = row.get("teams") or {}
    scores_obj = row.get("scores") or row.get("score") or {}

    game_id = game_obj.get("id")
    league_id = league_obj.get("id")
    season = league_obj.get("season")

    # teams
    home = teams_obj.get("home") or {}
    away = teams_obj.get("away") or {}
    home_id = home.get("id")
    away_id = away.get("id")

    # date/status/timezone
    game_date = game_obj.get("date") or row.get("date")
    status_obj = game_obj.get("status") or {}
    status = status_obj.get("short") or status_obj.get("status") or row.get("status")
    status_long = status_obj.get("long") or status_obj.get("description")
    tz = game_obj.get("timezone") or row.get("timezone")

    # stage/group (있으면)
    stage = league_obj.get("stage")
    group_name = league_obj.get("group")

    sql = """
    INSERT INTO hockey_games (
      id, league_id, season, stage, group_name,
      home_team_id, away_team_id,
      game_date, status, status_long, timezone,
      score_json, raw_json
    )
    VALUES (
      %s, %s, %s, %s, %s,
      %s, %s,
      %s, %s, %s, %s,
      %s::jsonb, %s::jsonb
    )
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
      raw_json = EXCLUDED.raw_json;
    """
    execute(
        sql,
        (
            game_id,
            league_id,
            season,
            stage,
            group_name,
            home_id,
            away_id,
            game_date,
            status,
            status_long,
            tz,
            json_dumps(scores_obj),
            json_dumps(row),
        ),
    )


def upsert_standings(league_id: int, season: int, standings_resp: Dict[str, Any]) -> int:
    """
    standings 응답이 [[ {...}, {...} ]] 형태(2중 배열)인 경우가 많아서
    flatten해서 upsert.
    """
    response = standings_resp.get("response") or []
    rows: List[Dict[str, Any]] = []

    # response가 [ [..] ] 또는 [ .. ] 둘 다 대비
    if len(response) > 0 and isinstance(response[0], list):
        for inner in response:
            rows.extend(inner)
    else:
        rows = response

    count = 0
    for r in rows:
        team = r.get("team") or {}
        group = (r.get("group") or {})
        stage = r.get("stage")
        group_name = group.get("name") if isinstance(group, dict) else r.get("group")

        sql = """
        INSERT INTO hockey_standings (
          league_id, season, stage, group_name,
          team_id, position,
          games_played,
          win_total, win_pct, win_ot_total, win_ot_pct,
          lose_total, lose_pct, lose_ot_total, lose_ot_pct,
          goals_for, goals_against, points,
          form, description,
          raw_json
        )
        VALUES (
          %s, %s, %s, %s,
          %s, %s,
          %s,
          %s, %s, %s, %s,
          %s, %s, %s, %s,
          %s, %s, %s,
          %s, %s,
          %s::jsonb
        )
        ON CONFLICT (league_id, season, stage, group_name, team_id) DO UPDATE SET
          position = EXCLUDED.position,
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
          raw_json = EXCLUDED.raw_json;
        """

        games = r.get("games") or {}
        win = r.get("win") or {}
        lose = r.get("lose") or {}
        win_ot = r.get("win_overtime") or {}
        lose_ot = r.get("lose_overtime") or {}
        goals = r.get("goals") or {}

        execute(
            sql,
            (
                league_id,
                season,
                stage,
                group_name,
                team.get("id"),
                r.get("position"),
                games.get("played"),

                win.get("total"),
                num_or_none(win.get("percentage")),
                win_ot.get("total"),
                num_or_none(win_ot.get("percentage")),

                lose.get("total"),
                num_or_none(lose.get("percentage")),
                lose_ot.get("total"),
                num_or_none(lose_ot.get("percentage")),

                goals.get("for"),
                goals.get("against"),
                r.get("points"),

                r.get("form"),
                r.get("description"),
                json_dumps(r),
            ),
        )
        count += 1

    return count


# =========================
# utils
# =========================
def json_dumps(obj: Any) -> str:
    import json
    return json.dumps(obj or {}, ensure_ascii=False, separators=(",", ":"))

def num_or_none(x: Any):
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


# =========================
# main flow
# =========================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seasons", required=True, help="예: 2024,2025  (2024-25 / 2025-26 목적)")
    ap.add_argument("--league-ids", default="", help="특정 리그만: 예 57,58,59  (비우면 전체 leagues에서 필터)")
    ap.add_argument("--sleep", type=float, default=0.25, help="API 호출간 sleep")
    args = ap.parse_args()

    api_key = os.environ.get("APISPORTS_KEY") or os.environ.get("API_SPORTS_KEY")
    if not api_key:
        raise SystemExit("APISPORTS_KEY 환경변수 필요")

    seasons = [int(x.strip()) for x in args.seasons.split(",") if x.strip()]
    league_ids_filter = set(int(x.strip()) for x in args.league_ids.split(",") if x.strip())

    api = HockeyApi(api_key=api_key)

    log.info("1) leagues + seasons 백필 시작")
    data = api.leagues()
    leagues_resp = data.get("response") or []

    # leagues 응답 구조: { league:{...}, country:{...}, seasons:[...] }
    target_leagues: List[Dict[str, Any]] = []
    for item in leagues_resp:
        league = item.get("league") or {}
        if not league:
            continue
        lid = league.get("id")
        if league_ids_filter and lid not in league_ids_filter:
            continue

        # 우리가 원하는 시즌이 하나라도 들어있으면 대상
        seasons_arr = item.get("seasons") or []
        seasons_in_item = {s.get("season") for s in seasons_arr if isinstance(s, dict)}
        if not any(s in seasons_in_item for s in seasons):
            continue

        target_leagues.append(item)

    log.info("대상 리그 수: %d", len(target_leagues))

    # upsert leagues/countries/seasons
    for item in target_leagues:
        country = item.get("country") or {}
        league = item.get("league") or {}
        seasons_arr = item.get("seasons") or []

        country_id = country.get("id")
        if country_id is not None:
            upsert_country(country)

        upsert_league(league, country_id)

        for s in seasons_arr:
            if not isinstance(s, dict):
                continue
            if s.get("season") in seasons:
                upsert_league_season(league.get("id"), s)

        time.sleep(args.sleep)

    log.info("2) games + teams 백필 시작")
    for item in target_leagues:
        league = item.get("league") or {}
        lid = league.get("id")
        seasons_arr = item.get("seasons") or []
        seasons_in_item = [s.get("season") for s in seasons_arr if isinstance(s, dict) and s.get("season") in seasons]

        for season in seasons_in_item:
            log.info("league=%s season=%s games fetch...", lid, season)

            page = 1
            total_upserted = 0
            while True:
                g = api.games(league_id=lid, season=season, page=page)
                resp = g.get("response") or []
                if not resp:
                    break

                for row in resp:
                    # teams upsert 먼저
                    league_obj = row.get("league") or {}
                    country_obj = league_obj.get("country") or {}  # 응답에 없을 수도
                    country_id = country_obj.get("id")

                    teams = row.get("teams") or {}
                    home = teams.get("home") or {}
                    away = teams.get("away") or {}

                    if home.get("id"):
                        upsert_team(home, country_id)
                    if away.get("id"):
                        upsert_team(away, country_id)

                    # game upsert
                    upsert_game(row)
                    total_upserted += 1

                time.sleep(args.sleep)

                # pagination 힌트가 있으면 사용
                paging = g.get("paging") or {}
                total_pages = paging.get("total")
                if total_pages and page >= int(total_pages):
                    break

                page += 1

            log.info("league=%s season=%s games upserted=%d", lid, season, total_upserted)

    log.info("3) standings 백필 시작")
    for item in target_leagues:
        league = item.get("league") or {}
        lid = league.get("id")
        seasons_arr = item.get("seasons") or []
        seasons_in_item = [s.get("season") for s in seasons_arr if isinstance(s, dict) and s.get("season") in seasons]

        for season in seasons_in_item:
            try:
                st = api.standings(league_id=lid, season=season)
                cnt = upsert_standings(lid, season, st)
                log.info("league=%s season=%s standings rows=%d", lid, season, cnt)
            except requests.HTTPError as e:
                # standings 없는 리그도 많음
                log.warning("standings skip league=%s season=%s (%s)", lid, season, str(e))
            time.sleep(args.sleep)

    log.info("✅ 백필 완료")


if __name__ == "__main__":
    main()
