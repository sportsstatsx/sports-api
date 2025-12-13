#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EPL(league=39) 시즌 데이터를 API-Football(API-SPORTS v3)에서 받아
PostgreSQL(dev) 스키마(leagues/teams/matches/fixtures)에 upsert 저장.

필요 ENV:
- DATABASE_URL : dev DB url
- APISPORTS_KEY 또는 API_FOOTBALL_KEY : API 키 (x-apisports-key 헤더로 사용)

실행 예:
  PYTHONPATH=. python football/scripts/epl_seed_dev.py --season 2025
  PYTHONPATH=. python football/scripts/epl_seed_dev.py --season 2025 --chunk-monthly
  PYTHONPATH=. python football/scripts/epl_seed_dev.py --season 2025 --from 2025-08-01 --to 2025-08-31
"""

import os
import sys
import time
import argparse
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg


DEFAULT_LEAGUE_ID = 39  # EPL
BASE_URL = "https://v3.football.api-sports.io"


def env_key() -> str:
    key = (
        os.environ.get("APISPORTS_KEY")
        or os.environ.get("API_FOOTBALL_KEY")
        or os.environ.get("X_APISPORTS_KEY")
    )
    if not key:
        raise RuntimeError("Missing API key env. Set APISPORTS_KEY (or API_FOOTBALL_KEY).")
    return key


def db_url() -> str:
    url = os.environ.get("DATABASE_URL") or os.environ.get("database_url") or os.environ.get("DATABASE_URL".upper())
    if not url:
        raise RuntimeError("Missing DATABASE_URL env for dev DB.")
    return url


def status_group_from_short(short: Optional[str]) -> str:
    s = (short or "").upper()

    scheduled = {"NS", "TBD"}
    live = {"1H", "2H", "HT", "ET", "P", "LIVE", "BT", "INT"}
    finished = {"FT", "AET", "PEN"}
    other = {"PST", "CANC", "ABD", "SUSP", "AWD", "WO"}

    if s in scheduled:
        return "SCHEDULED"
    if s in live:
        return "LIVE"
    if s in finished:
        return "FINISHED"
    if s in other:
        return "OTHER"
    return "UNKNOWN"


def api_get(path: str, params: Dict[str, Any], timeout: int = 30) -> Dict[str, Any]:
    headers = {"x-apisports-key": env_key()}
    url = f"{BASE_URL}{path}"
    r = requests.get(url, headers=headers, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def month_ranges_for_season(season_year: int) -> List[Tuple[date, date]]:
    """
    API-Football season=2025 (2025/26) 같은 경우를 고려해
    2025-07-01 ~ 2026-06-30 범위를 월 단위로 나눈다.
    """
    start = date(season_year, 7, 1)
    end = date(season_year + 1, 6, 30)

    ranges: List[Tuple[date, date]] = []
    cur = start
    while cur <= end:
        nxt_month = (cur.replace(day=1) + timedelta(days=32)).replace(day=1)
        last_day = nxt_month - timedelta(days=1)
        a = cur
        b = min(last_day, end)
        ranges.append((a, b))
        cur = nxt_month
    return ranges


def upsert_league(cur, league_id: int, name: str, country: Optional[str], logo: Optional[str]) -> None:
    cur.execute(
        """
        INSERT INTO leagues (id, name, country, logo)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
          name = EXCLUDED.name,
          country = EXCLUDED.country,
          logo = EXCLUDED.logo
        """,
        (league_id, name, country, logo),
    )


def upsert_team(cur, team_id: int, name: str, logo: Optional[str], country: Optional[str] = None) -> None:
    cur.execute(
        """
        INSERT INTO teams (id, name, country, logo)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
          name = EXCLUDED.name,
          country = COALESCE(EXCLUDED.country, teams.country),
          logo = EXCLUDED.logo
        """,
        (team_id, name, country, logo),
    )


def upsert_match_and_fixture(
    cur,
    fixture_id: int,
    league_id: int,
    season: int,
    dt_utc: datetime,
    status_short: str,
    status_group: str,
    elapsed: Optional[int],
    home_id: int,
    away_id: int,
    home_ft: Optional[int],
    away_ft: Optional[int],
) -> None:
    # matches
    cur.execute(
        """
        INSERT INTO matches (
          fixture_id, league_id, season, date_utc, status, status_group, elapsed,
          home_id, away_id, home_ft, away_ft
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (fixture_id) DO UPDATE SET
          league_id = EXCLUDED.league_id,
          season = EXCLUDED.season,
          date_utc = EXCLUDED.date_utc,
          status = EXCLUDED.status,
          status_group = EXCLUDED.status_group,
          elapsed = EXCLUDED.elapsed,
          home_id = EXCLUDED.home_id,
          away_id = EXCLUDED.away_id,
          home_ft = EXCLUDED.home_ft,
          away_ft = EXCLUDED.away_ft
        """,
        (
            fixture_id,
            league_id,
            season,
            dt_utc,
            status_short,
            status_group,
            elapsed,
            home_id,
            away_id,
            home_ft,
            away_ft,
        ),
    )

    # fixtures (간단한 상태 테이블)
    cur.execute(
        """
        INSERT INTO fixtures (
          fixture_id, league_id, season, date_utc, status, status_group
        )
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (fixture_id) DO UPDATE SET
          league_id = EXCLUDED.league_id,
          season = EXCLUDED.season,
          date_utc = EXCLUDED.date_utc,
          status = EXCLUDED.status,
          status_group = EXCLUDED.status_group
        """,
        (fixture_id, league_id, season, dt_utc, status_short, status_group),
    )


def parse_fixture_row(row: Dict[str, Any]) -> Dict[str, Any]:
    fx = row.get("fixture") or {}
    lg = row.get("league") or {}
    tm = row.get("teams") or {}
    goals = row.get("goals") or {}

    fixture_id = int(fx.get("id"))
    # API는 ISO8601 문자열(UTC offset 포함) 제공
    dt_str = fx.get("date")
    dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))

    status = (fx.get("status") or {}).get("short") or "NS"
    elapsed = (fx.get("status") or {}).get("elapsed")

    league_id = int(lg.get("id"))
    season = int(lg.get("season"))

    home = tm.get("home") or {}
    away = tm.get("away") or {}
    home_id = int(home.get("id"))
    away_id = int(away.get("id"))

    home_name = home.get("name") or ""
    away_name = away.get("name") or ""
    home_logo = home.get("logo")
    away_logo = away.get("logo")

    league_name = lg.get("name") or ""
    league_country = lg.get("country")
    league_logo = lg.get("logo")

    home_ft = goals.get("home")
    away_ft = goals.get("away")

    return {
        "fixture_id": fixture_id,
        "dt_utc": dt,
        "status": status,
        "elapsed": elapsed,
        "league_id": league_id,
        "season": season,
        "home_id": home_id,
        "away_id": away_id,
        "home_ft": home_ft,
        "away_ft": away_ft,
        "home_name": home_name,
        "away_name": away_name,
        "home_logo": home_logo,
        "away_logo": away_logo,
        "league_name": league_name,
        "league_country": league_country,
        "league_logo": league_logo,
    }


def fetch_fixtures(league_id: int, season: int, from_date: Optional[str], to_date: Optional[str]) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {"league": league_id, "season": season}
    if from_date and to_date:
        params["from"] = from_date
        params["to"] = to_date

    data = api_get("/fixtures", params=params)
    resp = data.get("response") or []
    return resp


def main() -> int:
    ap = argparse.ArgumentParser(add_help=True)
    ap.add_argument("--league-id", type=int, default=DEFAULT_LEAGUE_ID)
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--from", dest="from_date", type=str, default=None, help="YYYY-MM-DD")
    ap.add_argument("--to", dest="to_date", type=str, default=None, help="YYYY-MM-DD")
    ap.add_argument("--chunk-monthly", action="store_true", help="season 범위를 월별로 쪼개서 호출")
    ap.add_argument("--sleep", type=float, default=0.2, help="API 호출 간 sleep(초)")
    args = ap.parse_args()

    league_id = args.league_id
    season = args.season

    if (args.from_date and not args.to_date) or (args.to_date and not args.from_date):
        raise SystemExit("--from 과 --to 는 같이 써야 합니다.")

    # 호출 범위 결정
    ranges: List[Tuple[Optional[str], Optional[str]]]
    if args.from_date and args.to_date:
        ranges = [(args.from_date, args.to_date)]
    elif args.chunk_monthly:
        ranges = [(a.isoformat(), b.isoformat()) for a, b in month_ranges_for_season(season)]
    else:
        ranges = [(None, None)]  # API에서 시즌 전체 한번에

    url = db_url()
    total_rows = 0

    with psycopg.connect(url, autocommit=False) as conn:
        with conn.cursor() as cur:
            for (a, b) in ranges:
                if a and b:
                    print(f"[EPL seed] fetching fixtures league={league_id} season={season} from={a} to={b}")
                else:
                    print(f"[EPL seed] fetching fixtures league={league_id} season={season} (full season)")

                rows = fetch_fixtures(league_id, season, a, b)
                print(f"[EPL seed] api response rows={len(rows)}")

                for r in rows:
                    d = parse_fixture_row(r)

                    # leagues/teams upsert
                    upsert_league(cur, d["league_id"], d["league_name"], d["league_country"], d["league_logo"])
                    upsert_team(cur, d["home_id"], d["home_name"], d["home_logo"])
                    upsert_team(cur, d["away_id"], d["away_name"], d["away_logo"])

                    # matches/fixtures upsert
                    sg = status_group_from_short(d["status"])
                    upsert_match_and_fixture(
                        cur,
                        fixture_id=d["fixture_id"],
                        league_id=d["league_id"],
                        season=d["season"],
                        dt_utc=d["dt_utc"],
                        status_short=d["status"],
                        status_group=sg,
                        elapsed=d["elapsed"],
                        home_id=d["home_id"],
                        away_id=d["away_id"],
                        home_ft=d["home_ft"],
                        away_ft=d["away_ft"],
                    )
                    total_rows += 1

                conn.commit()
                if args.sleep:
                    time.sleep(args.sleep)

    print(f"[EPL seed] done. upserted fixtures={total_rows}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
