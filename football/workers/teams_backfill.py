# football/workers/teams_backfill.py
from __future__ import annotations

import os
import sys
import time
from typing import Any, Dict, List, Optional

import requests

# ✅ 중요: 실행 위치에 따라 import 경로가 깨질 수 있으니,
# project/src 기준에서 실행하거나 PYTHONPATH를 잡아준다.
# (Render shell에서 /opt/render/project/src 위치에서 실행 권장)
from db import fetch_all, execute  # 프로젝트 공통 DB 헬퍼

BASE_URL = "https://v3.football.api-sports.io"


def _get_api_key() -> str:
    key = (
        os.environ.get("APIFOOTBALL_KEY")
        or os.environ.get("API_FOOTBALL_KEY")
        or os.environ.get("API_KEY")
        or ""
    )
    if not key:
        raise RuntimeError("API key missing: set APIFOOTBALL_KEY (or API_FOOTBALL_KEY / API_KEY)")
    return key


def _headers() -> Dict[str, str]:
    return {"x-apisports-key": _get_api_key()}


def _safe_get(path: str, params: Dict[str, Any], timeout: int = 25, max_retry: int = 4) -> Dict[str, Any]:
    url = f"{BASE_URL}{path}"
    last_err: Optional[Exception] = None
    for i in range(max_retry):
        try:
            r = requests.get(url, headers=_headers(), params=params, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(0.8 * (i + 1))
                continue
            r.raise_for_status()
            data = r.json()
            if not isinstance(data, dict):
                raise RuntimeError("API response is not a dict")

            errs = data.get("errors")
            if isinstance(errs, dict) and errs:
                raise RuntimeError(f"API errors: {errs}")
            if isinstance(errs, list) and len(errs) > 0:
                raise RuntimeError(f"API errors: {errs}")

            return data
        except Exception as e:
            last_err = e
            time.sleep(0.8 * (i + 1))
    raise RuntimeError(f"API request failed after retries: {last_err}")


def get_needed_team_ids(league_ids: List[int], seasons: List[int]) -> List[int]:
    rows = fetch_all(
        """
        WITH t AS (
          SELECT DISTINCT home_id AS team_id
          FROM matches
          WHERE league_id = ANY(%s)
            AND season = ANY(%s)
            AND home_id IS NOT NULL
          UNION
          SELECT DISTINCT away_id
          FROM matches
          WHERE league_id = ANY(%s)
            AND season = ANY(%s)
            AND away_id IS NOT NULL
        )
        SELECT t.team_id
        FROM t
        LEFT JOIN teams tm ON tm.id = t.team_id
        WHERE tm.id IS NULL
        ORDER BY t.team_id
        """,
        (league_ids, seasons, league_ids, seasons),
    )
    out: List[int] = []
    for r in rows or []:
        tid = r.get("team_id")
        if tid is not None:
            try:
                out.append(int(tid))
            except Exception:
                pass
    return out


def upsert_team(team: Dict[str, Any]) -> None:
    """
    teams 테이블 스키마(확인 완료):
      id integer PK
      name text
      country text
      logo text
    """
    team_id = team.get("id")
    if team_id is None:
        return
    execute(
        """
        INSERT INTO teams (id, name, country, logo)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
          name = EXCLUDED.name,
          country = EXCLUDED.country,
          logo = EXCLUDED.logo
        """,
        (
            int(team_id),
            team.get("name"),
            team.get("country"),
            team.get("logo"),
        ),
    )


def fetch_team_by_id(team_id: int) -> Optional[Dict[str, Any]]:
    data = _safe_get("/teams", params={"id": int(team_id)})
    resp = data.get("response") or []
    if not isinstance(resp, list) or not resp:
        return None
    # API-Sports /teams 응답은 보통 resp[0].team
    item = resp[0]
    if not isinstance(item, dict):
        return None
    t = item.get("team")
    if isinstance(t, dict):
        return t
    # 예외 케이스 fallback
    return item if "id" in item else None


def main() -> None:
    # 사용 예:
    # export TARGET_LEAGUES="218,219,179,180,345,346,106,107,169"
    # export TARGET_SEASONS="2024,2025"
    leagues_s = (os.environ.get("TARGET_LEAGUES") or "").strip()
    seasons_s = (os.environ.get("TARGET_SEASONS") or "").strip()

    if not leagues_s or not seasons_s:
        print("Missing env. Set TARGET_LEAGUES and TARGET_SEASONS", file=sys.stderr)
        sys.exit(1)

    league_ids = [int(x.strip()) for x in leagues_s.split(",") if x.strip()]
    seasons = [int(x.strip()) for x in seasons_s.split(",") if x.strip()]

    needed = get_needed_team_ids(league_ids, seasons)
    print(f"[teams_backfill] missing teams: {len(needed)}")

    ok = 0
    fail = 0
    for i, tid in enumerate(needed, 1):
        try:
            t = fetch_team_by_id(tid)
            if not t:
                print(f"  ! {tid}: empty response")
                fail += 1
                continue
            upsert_team(t)
            ok += 1
            if i % 20 == 0:
                print(f"  ... {i}/{len(needed)} done (ok={ok}, fail={fail})")
        except Exception as e:
            print(f"  ! {tid}: error {e}", file=sys.stderr)
            fail += 1

    print(f"[teams_backfill] done. ok={ok}, fail={fail}")


if __name__ == "__main__":
    main()
