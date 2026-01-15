# backfill_team_season_stats.py

## 역할(한 줄)
DB의 `(league_id, season)` 조합을 기준으로 **team_season_stats를 시즌 단위로 다시 채움(재계산/재생성)**.

## 언제 쓰나
- team_season_stats가 누락/불일치/스키마 변경 등으로 “다시 채워야” 할 때
- 리그/시즌 누적 통계를 일괄로 정리하고 싶을 때

## 실행 방법
```bash
python tools/football_backfill/backfill_team_season_stats.py
```

## 주의
- 이 스크립트는 “경기 단건 상세 백필(이벤트/라인업/통계/선수)”이 아닙니다.
- Render Cron에 연결되어 있지 않은 **수동 도구**입니다.
