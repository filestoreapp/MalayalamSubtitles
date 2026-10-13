-- migrate.sql - idempotent, run in Render Postgres shell after deploying schema changes.
-- Do NOT run the unique index until duplicates are cleaned (Animals 1275, Odyssey loser).

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Movie indexes
CREATE INDEX IF NOT EXISTS ix_movie_title ON movie (title);
CREATE INDEX IF NOT EXISTS ix_movie_imdb_media ON movie (imdb_id, media_type);
CREATE INDEX IF NOT EXISTS ix_movie_media_created ON movie (media_type, created_at);
CREATE INDEX IF NOT EXISTS ix_movie_created ON movie (created_at);
CREATE INDEX IF NOT EXISTS ix_movie_title_trgm ON movie USING gin (title gin_trgm_ops);
CREATE INDEX IF NOT EXISTS ix_movie_category_trgm ON movie USING gin (category gin_trgm_ops);

-- TranslationJob
CREATE INDEX IF NOT EXISTS ix_job_status_priority_queued ON translation_job (status, priority, queued_at);
CREATE INDEX IF NOT EXISTS ix_job_movie_lang ON translation_job (movie_id, language);

-- SearchLog
ALTER TABLE search_log ADD COLUMN IF NOT EXISTS source VARCHAR(20) DEFAULT 'home';
CREATE INDEX IF NOT EXISTS ix_searchlog_searched ON search_log (searched_at);
CREATE INDEX IF NOT EXISTS ix_searchlog_query_searched ON search_log (search_query, searched_at);

-- DownloadLog
CREATE INDEX IF NOT EXISTS ix_downloadlog_downloaded ON download_log (downloaded_at);
CREATE INDEX IF NOT EXISTS ix_downloadlog_movie_downloaded ON download_log (movie_id, downloaded_at);

-- Others
CREATE INDEX IF NOT EXISTS ix_uploadplan_status_date ON upload_plan (status, scheduled_date);
CREATE INDEX IF NOT EXISTS ix_rating_movie_lang ON subtitle_rating (movie_id, language);
CREATE INDEX IF NOT EXISTS ix_fav_session_movie ON favorite (session_id, movie_id);

-- is_silent migration (replaces ", SilentMode" category hack)
ALTER TABLE movie ADD COLUMN IF NOT EXISTS is_silent BOOLEAN DEFAULT FALSE;
UPDATE movie SET is_silent = TRUE WHERE category LIKE '%SilentMode%';
UPDATE movie SET category = REPLACE(REPLACE(category, ', SilentMode', ''), 'SilentMode', '') WHERE category LIKE '%SilentMode%';

-- SiteStat R2 cache
ALTER TABLE site_stat ADD COLUMN IF NOT EXISTS r2_size_bytes BIGINT DEFAULT 0;
ALTER TABLE site_stat ADD COLUMN IF NOT EXISTS r2_file_count INTEGER DEFAULT 0;
ALTER TABLE site_stat ADD COLUMN IF NOT EXISTS r2_checked_at TIMESTAMP NULL;

-- Canonicalise legacy job status
UPDATE translation_job SET status = 'Completed' WHERE status = 'Success';

-- Unique duplicate guard - ONLY after de-duping:
-- CREATE UNIQUE INDEX IF NOT EXISTS uq_movie_imdb_media_season_episode
--   ON movie (imdb_id, media_type, season, episode)
--   WHERE imdb_id IS NOT NULL AND imdb_id <> '';
