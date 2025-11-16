from typing import Optional, Dict, Any, List
import json

from db import fetch_all
from services.insights.insights_overall_shooting_efficency import (
    insights_overall_shooting_efficency,
)
from services.insights.insights_overall_outcome_totals import (
    insights_overall_outcome_totals,
)
from services.insights.insights_overall_resultscombos_draw import (
    insights_overall_resultscombos_draw,
)
from services.insights.insights_overall_timing import insights_overall_timing
from services.insights.insights_overall_firstgoal_momentum import (
    insights_overall_firstgoal_momentum,
)
from services.insights.insights_overall_discipline_setpieces import (
    insights_overall_discipline_setpieces,
)
from services.insights.insights_overall_goalsbytime import (
    insights_overall_goalsbytime,
)


def get_team_season_stats(team_id: int, league_id: int) -> Optional[Dict[str, Any]]:
    """
    team_season_stats 테이블에서 (league_id, team_id)에 해당하는
    가장 최신 season 한 줄을 가져오고, 거기에 insights_overall.* 지표를
    추가/보정해서 반환한다.
    """

    rows = fetch_all(
        """
        SELECT
            league_id,
            season,
            team_id,
            name,
            value
        FROM team_season_stats
        WHERE league_id = %s
          AND team_id   = %s
        ORDER BY season DESC
        LIMIT 1
        """,
        (league_id, team_id),
    )
    if not rows:
        return None

    row = rows[0]

    raw_value = row.get("value")
    if isinstance(raw_value, str):
        try:
            stats = json.loads(raw_value)
        except Exception:
            stats = {}
    elif isinstance(raw_value, dict):
        stats = raw_value
    else:
        stats = {}

    if not isinstance(stats, dict):
        stats = {}

    insights = stats.get("insights_overall")
    if not isinstance(insights, dict):
        insights = {}
        stats["insights_overall"] = insights

    fixtures = stats.get("fixtures") or {}
    played = fixtures.get("played") or {}
    matches_total_api = played.get("total") or 0

    season = row.get("season")
    try:
        season_int = int(season)
    except (TypeError, ValueError):
        season_int = None

    # ─────────────────────────────
    # 섹션별 계산 함수 호출
    # ─────────────────────────────

    # Shooting & Efficiency
    insights_overall_shooting_efficency(
        stats=stats,
        insights=insights,
        team_id=team_id,
        league_id=league_id,
        season_int=season_int,
        matches_total_api=matches_total_api,
    )

    # Outcome & Totals (+ Result Combos 관련 지표까지 같이 계산)
    insights_overall_outcome_totals(
        stats=stats,
        insights=insights,
        team_id=team_id,
        league_id=league_id,
        season_int=season_int,
    )

    # Results Combos & Draw – 현재는 별도 계산 없음(Outcome에서 같이 처리)
    insights_overall_resultscombos_draw(
        stats=stats,
        insights=insights,
        team_id=team_id,
        league_id=league_id,
        season_int=season_int,
    )

    # Timing + First Goal + Momentum
    insights_overall_timing(
        stats=stats,
        insights=insights,
        team_id=team_id,
        league_id=league_id,
        season_int=season_int,
    )

    # First Goal / Momentum – 현재 계산은 Timing 모듈에서 같이 수행
    insights_overall_firstgoal_momentum(
        stats=stats,
        insights=insights,
        team_id=team_id,
        league_id=league_id,
        season_int=season_int,
    )

    # Discipline & Set Pieces (자리만, 추후 구현)
    insights_overall_discipline_setpieces(
        stats=stats,
        insights=insights,
        team_id=team_id,
        league_id=league_id,
        season_int=season_int,
    )

    # Goals by Time (자리만, 추후 구현)
    insights_overall_goalsbytime(
        stats=stats,
        insights=insights,
        team_id=team_id,
        league_id=league_id,
        season_int=season_int,
    )

    return {
        "league_id": row["league_id"],
        "season": row["season"],
        "team_id": row["team_id"],
        "name": row.get("name"),
        "value": stats,
    }
