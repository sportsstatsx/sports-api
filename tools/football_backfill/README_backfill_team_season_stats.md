# backfill_team_season_stats.py

## 이 파일이 하는 일 (정확히)
DB의 `matches` 테이블에서 DISTINCT `(league_id, season)` 조합을 뽑아서,  
각 조합에 대해 **`team_season_stats`를 생성/재계산** 합니다.

➡️ 한마디로: **“리그/시즌 단위로 팀 시즌 누적 통계를 한 번에 다시 만드는 도구”**

## postmatch 백필과 차이
- `postmatch_backfill.py` : **경기 단위**(FT 후) 이벤트/라인업/통계 등 “경기 상세” 채움
- 이 스크립트 : **시즌 누적 통계**(팀 단위) `team_season_stats` 재생성

## 언제 쓰나
- 특정 리그/시즌의 `team_season_stats`가 비어있거나 값이 이상한 경우
- fixtures/matches를 대량 수정(리컨실/백필)한 뒤 누적 통계를 다시 맞춰야 하는 경우
- 새 리그/시즌을 추가했는데 팀 시즌 통계 테이블이 아직 없는 경우

## 실행 명령어 예시
```bash
# 1) DB에 있는 모든 (league_id, season) 대상으로 재계산
python tools/football_backfill/backfill_team_season_stats.py

# 2) 특정 시즌만 (여러 시즌도 가능)
python tools/football_backfill/backfill_team_season_stats.py 2024
python tools/football_backfill/backfill_team_season_stats.py 2024,2025
python tools/football_backfill/backfill_team_season_stats.py 2024 2025
```

## 필요 환경
- DB 연결 환경변수: 프로젝트에서 쓰는 `db.py` 설정(예: `DATABASE_URL` 등)
- (내부 구현에 따라) API 호출이 포함될 수 있으니 API 키도 운영과 동일하게 준비

## 주의
- `(league_id, season)` 조합이 많으면 오래 걸립니다.
- DB에 즉시 반영됩니다. dev에서 샘플 시즌으로 먼저 검증 후 prod 실행 권장
