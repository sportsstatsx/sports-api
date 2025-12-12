-- hockey/db/migrate/002_add_hockey_odds_meta.sql
-- Add odds meta tables (markets, bookmakers) + link columns in hockey_odds

BEGIN;

-- 1) Markets (예: "Match Winner", "Total Goals", etc.)
CREATE TABLE IF NOT EXISTS hockey_odds_markets (
  id           INTEGER PRIMARY KEY,     -- API: market.id (있으면) / 없으면 우리가 채움(아래 참고)
  name         TEXT NOT NULL,           -- API: market.name
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DROP TRIGGER IF EXISTS trg_hockey_odds_markets_updated_at ON hockey_odds_markets;
CREATE TRIGGER trg_hockey_odds_markets_updated_at
BEFORE UPDATE ON hockey_odds_markets
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE UNIQUE INDEX IF NOT EXISTS uq_hockey_odds_markets_name
ON hockey_odds_markets(name);


-- 2) Bookmakers (예: "1xBet", "Bet365", etc.)
CREATE TABLE IF NOT EXISTS hockey_odds_bookmakers (
  id           INTEGER PRIMARY KEY,     -- API: bookmaker.id (있으면) / 없으면 우리가 채움(아래 참고)
  name         TEXT NOT NULL,           -- API: bookmaker.name
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DROP TRIGGER IF EXISTS trg_hockey_odds_bookmakers_updated_at ON hockey_odds_bookmakers;
CREATE TRIGGER trg_hockey_odds_bookmakers_updated_at
BEFORE UPDATE ON hockey_odds_bookmakers
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE UNIQUE INDEX IF NOT EXISTS uq_hockey_odds_bookmakers_name
ON hockey_odds_bookmakers(name);


-- 3) hockey_odds 테이블에 market_id/bookmaker_id 연결 컬럼 추가 (기존 구조 유지 + 확장)
ALTER TABLE hockey_odds
  ADD COLUMN IF NOT EXISTS market_id INTEGER REFERENCES hockey_odds_markets(id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS bookmaker_id INTEGER REFERENCES hockey_odds_bookmakers(id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS last_update TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_hockey_odds_market_id ON hockey_odds(market_id);
CREATE INDEX IF NOT EXISTS idx_hockey_odds_bookmaker_id ON hockey_odds(bookmaker_id);
CREATE INDEX IF NOT EXISTS idx_hockey_odds_game_market_book ON hockey_odds(game_id, market_id, bookmaker_id);

-- 4) (선택) 중복 방지 유니크 키: 같은 게임/마켓/북메이커/선택 조합은 1개로 유지
CREATE UNIQUE INDEX IF NOT EXISTS uq_hockey_odds_dedupe2
ON hockey_odds (game_id, market_id, bookmaker_id, selection);

-- 5) migrations 기록
INSERT INTO hockey_schema_migrations(version)
VALUES ('002_add_hockey_odds_meta')
ON CONFLICT (version) DO NOTHING;

COMMIT;
