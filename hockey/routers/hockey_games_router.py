from typing import Optional, List, Any

from flask import Blueprint, request, jsonify

from hockey.hockey_db import hockey_fetch_all


hockey_games_bp = Blueprint("hockey_games", __name__, url_prefix="/api/hockey")


@hockey_games_bp.route("/games")
def route_hockey_games():
    """
    하키 경기 목록 (DB 연결/수집 상태 확인용 - 경량)
    - SELECT * 제거 (raw_json/score_json 등 대형 컬럼으로 응답 비대해지는 것 방지)

    Query:
      - ids: str (선택)   예) "419206,419207"  → 해당 id들만 반환 (즐겨찾기 최신화용)
      - season: int (선택)
      - league_id: int (선택)
      - limit: int (선택, 기본 50, 최대 500)
      - live: int (선택, 1이면 진행중(P1/P2/P3/OT/SO)만 반환)
    """
    ids_raw: Optional[str] = request.args.get("ids", type=str)
    season: Optional[int] = request.args.get("season", type=int)
    league_id: Optional[int] = request.args.get("league_id", type=int)
    limit: int = request.args.get("limit", type=int) or 50

    # ✅ live 파라미터는 앱/클라에서 "1" 말고 "true" 등으로 올 수 있어서 문자열도 허용한다.
    live_raw = request.args.get("live", default="0")
    live_s = str(live_raw).strip().lower()
    live: int = 1 if live_s in ("1", "true", "t", "yes", "y", "on", "live") else 0


    # ✅ ids 파싱 ("1,2,3" → [1,2,3])
    ids: List[int] = []
    if ids_raw:
        parts = [p.strip() for p in str(ids_raw).split(",")]
        for p in parts:
            if not p:
                continue
            try:
                ids.append(int(p))
            except Exception:
                # 숫자가 아닌 값은 무시(에러로 막고 싶으면 여기서 400 반환으로 바꿔도 됨)
                continue
        # 중복 제거 + 최대 500개 제한(서버 보호)
        ids = list(dict.fromkeys(ids))[:500]

    # ✅ ids가 있으면: 요청된 ids만 반환해야 하므로 limit을 ids 개수로 맞춘다(최대 500)
    if ids:
        limit = min(len(ids), 500)

    if limit < 1:
        limit = 1
    if limit > 500:
        limit = 500


    where: List[str] = []
    params: List[Any] = []

    # ✅ ids가 있으면 해당 경기들만 필터링
    if ids:
        placeholders = ",".join(["%s"] * len(ids))
        where.append(f"id IN ({placeholders})")
        params.extend(ids)

    if season is not None:
        where.append("season = %s")
        params.append(season)

    if league_id is not None:
        where.append("league_id = %s")
        params.append(league_id)


    # ✅ 진행중만 보고 싶을 때
    # BT(Break Time, 인터미션)도 "진행중"으로 취급해야 라이브 목록에서 사라지지 않는다.
    if live == 1:
        where.append("status IN ('P1','P2','P3','BT','OT','SO')")


    where_sql = ""
    if where:
        where_sql = "WHERE " + " AND ".join(where)

    # ✅ live=1이면 경기시간 기준으로 정렬, 아니면 기존대로 id DESC
    order_sql = "ORDER BY game_date DESC" if live == 1 else "ORDER BY id DESC"

    # ✅ 디버그/확인용으로 필요한 최소 컬럼만 반환 (live_timer 포함)
    sql = f"""
        SELECT
            id,
            league_id,
            season,
            stage,
            group_name,
            game_date,
            status,
            status_long,
            live_timer,
            home_team_id,
            away_team_id,
            timezone,

            -- ✅ score_json 평면/중첩 구조 모두 방어
            CASE
                -- (1) 현재 DB 실데이터: {"home":1,"away":4}
                WHEN (score_json->>'home') ~ '^[0-9]+$' THEN (score_json->>'home')::int
                -- (2) 혹시 중첩 구조가 올 경우 대비
                WHEN (score_json #>> '{{home,total}}') ~ '^[0-9]+$' THEN (score_json #>> '{{home,total}}')::int
                WHEN (score_json #>> '{{home,goals}}') ~ '^[0-9]+$' THEN (score_json #>> '{{home,goals}}')::int
                WHEN (score_json #>> '{{home,score}}') ~ '^[0-9]+$' THEN (score_json #>> '{{home,score}}')::int
                WHEN (score_json #>> '{{home,ft}}') ~ '^[0-9]+$' THEN (score_json #>> '{{home,ft}}')::int
                ELSE NULL
            END AS home_score,

            CASE
                WHEN (score_json->>'away') ~ '^[0-9]+$' THEN (score_json->>'away')::int
                WHEN (score_json #>> '{{away,total}}') ~ '^[0-9]+$' THEN (score_json #>> '{{away,total}}')::int
                WHEN (score_json #>> '{{away,goals}}') ~ '^[0-9]+$' THEN (score_json #>> '{{away,goals}}')::int
                WHEN (score_json #>> '{{away,score}}') ~ '^[0-9]+$' THEN (score_json #>> '{{away,score}}')::int
                WHEN (score_json #>> '{{away,ft}}') ~ '^[0-9]+$' THEN (score_json #>> '{{away,ft}}')::int
                ELSE NULL
            END AS away_score

        FROM hockey_games
        {where_sql}
        {order_sql}
        LIMIT %s
    """

    params.append(limit)


    rows = hockey_fetch_all(sql, tuple(params))
    # ✅ live=1일 때: status_long에 live_timer를 붙여 "Live {Period} {MM:SS}"로 가공
    if live == 1 and rows:
        for r in rows:
            status = (r.get("status") or "").strip().upper()
            status_long = (r.get("status_long") or "").strip()
            live_timer = r.get("live_timer")

            live_timer_s = ""
            if live_timer is None:
                live_timer_s = ""
            else:
                live_timer_s = str(live_timer).strip()

            clock_text = ""
            if live_timer_s:
                # "7" -> "07:00", "18:34" -> 그대로
                if ":" in live_timer_s:
                    clock_text = live_timer_s
                else:
                    try:
                        clock_text = f"{int(live_timer_s):02d}:00"
                    except Exception:
                        clock_text = live_timer_s

            if status in ("P1", "P2", "P3", "OT", "SO"):
                if clock_text:
                    r["status_long"] = f"Live {status_long} {clock_text}"
                else:
                    r["status_long"] = f"Live {status_long}"

    return jsonify({"ok": True, "count": len(rows), "rows": rows})
