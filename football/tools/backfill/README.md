# tools/football_backfill

축구(football) 데이터 **수동 백필 도구 모음**입니다.  
Render Cron에서 자동으로 도는 워커가 아니라, **문제 복구/일괄 재계산이 필요할 때만 사람이 직접 실행**합니다.

## 1) 특정 경기만 fixtures/raw 복구
- 스크립트: `backfill_match_fixtures_raw_by_ids.py`
- 한 줄 요약: **fixture_id 목록(ids.txt)에 있는 경기만** `/fixtures?id=...`를 다시 받아 `fixtures_raw` 저장 + `matches` 갱신

실행 예시:
```bash
python tools/football_backfill/backfill_match_fixtures_raw_by_ids.py ids.txt
# 일부만 실행(예: 100번째부터)
python tools/football_backfill/backfill_match_fixtures_raw_by_ids.py ids.txt 100
```

## 2) 팀 시즌 통계(team_season_stats) 일괄 재생성
- 스크립트: `backfill_team_season_stats.py`
- 한 줄 요약: DB의 `(league_id, season)` 조합을 기준으로 **team_season_stats를 시즌 단위로 다시 채움**

실행 예시:
```bash
python tools/football_backfill/backfill_team_season_stats.py
```
