-- 00_CREATE_ENGINE_SCHEMA.sql
-- Canon tables, FULL grants, and COMMIT-ONLY triggers (no per-row heavy work).
-- Triggers call your functions by name and fire ONLY after the whole transaction commits.

BEGIN;

-- [0] SCHEMA
CREATE SCHEMA IF NOT EXISTS engine;
GRANT USAGE ON SCHEMA engine TO PUBLIC;

-- [1] CORE TABLES (exact column names)
CREATE TABLE IF NOT EXISTS engine.staging_historical (
  forecast_id   uuid NOT NULL,
  forecast_name text,
  "DATE"        date,
  "VALUE"       double precision,
  "SES-M"       double precision, "SES-Q" double precision,
  "HWES-M"      double precision, "HWES-Q" double precision,
  "ARIMA-M"     double precision, "ARIMA-Q" double precision,
  uploaded_at   timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS engine.forecast_registry (
  forecast_id         uuid PRIMARY KEY,
  forecast_name       text NOT NULL,
  source_csv_filename text NOT NULL,
  parameters          jsonb DEFAULT '{}'::jsonb,
  status              text  DEFAULT 'pending',
  created_at          timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS engine.instance_historical (
  forecast_id uuid NOT NULL,
  "date"      date NOT NULL,
  value       double precision,
  qmv         double precision,
  mmv         double precision,
  lqm1  double precision, lqm5  double precision, lqm10 double precision,
  lqm15 double precision, lqm30 double precision,
  lmm1  double precision, lmm5  double precision, lmm10 double precision,
  lmm15 double precision, lmm30 double precision,
  arima_q double precision, ses_q  double precision, hwes_q double precision,
  arima_m double precision, ses_m  double precision, hwes_m double precision,
  created_at timestamptz DEFAULT now(),
  PRIMARY KEY (forecast_id, "date")
);

-- [2] INDEXES
CREATE INDEX IF NOT EXISTS ix_staging_hist_fid_date
  ON engine.staging_historical (forecast_id, "DATE");
CREATE INDEX IF NOT EXISTS ix_hist_fid_date
  ON engine.instance_historical (forecast_id, "date");
CREATE INDEX IF NOT EXISTS ix_hist_created_at
  ON engine.instance_historical (created_at);

-- [3] GLOBAL GRANTS (cover ALL existing tables; broad on purpose)
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA engine TO PUBLIC;
GRANT DELETE ON engine.staging_historical TO PUBLIC;         -- purge needs this
GRANT INSERT, UPDATE ON engine.instance_historical TO PUBLIC; -- explicit
GRANT INSERT, UPDATE ON engine.forecast_registry   TO PUBLIC; -- explicit
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA engine TO PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA engine TO PUBLIC;

-- Ensure FUTURE objects also get usable privileges (for SR tables created later)
ALTER DEFAULT PRIVILEGES IN SCHEMA engine
  GRANT SELECT, INSERT, UPDATE ON TABLES TO PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA engine
  GRANT EXECUTE ON FUNCTIONS TO PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA engine
  GRANT USAGE, SELECT ON SEQUENCES TO PUBLIC;

-- [4] CLEAN OUT ANY PRIOR HELPERS/TRIGGERS (idempotent)
DROP TRIGGER  IF EXISTS trg_collect_ids_stmt   ON engine.staging_historical;
DROP TRIGGER  IF EXISTS trg_on_staging_commit  ON engine._tx_sentinel_staging;
DROP FUNCTION IF EXISTS engine.trg_collect_staging_stmt()    CASCADE;
DROP FUNCTION IF EXISTS engine.trg_finalize_staging_commit() CASCADE;

DROP TRIGGER  IF EXISTS trg_collect_hist_stmt  ON engine.instance_historical;
DROP TRIGGER  IF EXISTS trg_on_hist_commit     ON engine._tx_sentinel_hist;
DROP FUNCTION IF EXISTS engine.trg_collect_hist_stmt()       CASCADE;
DROP FUNCTION IF EXISTS engine.trg_finalize_hist_commit()    CASCADE;

DROP TABLE IF EXISTS engine._tx_sentinel_staging;
DROP TABLE IF EXISTS engine._tx_sentinel_hist;

-- [5] SENTINEL TABLES (ensure one fire at COMMIT)
CREATE TABLE engine._tx_sentinel_staging (txid bigint PRIMARY KEY);
CREATE TABLE engine._tx_sentinel_hist    (txid bigint PRIMARY KEY);

-- Explicit grants for sentinels (created after the ALL TABLES grant)
GRANT SELECT, INSERT, UPDATE, DELETE ON engine._tx_sentinel_staging TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON engine._tx_sentinel_hist    TO PUBLIC;

-- [6] STAGING FLOW — collect per statement; finalize ON COMMIT only

-- 6a) Statement-level collector: gathers all forecast_ids touched in this tx and sets the sentinel
CREATE OR REPLACE FUNCTION engine.trg_collect_staging_stmt()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  CREATE TEMP TABLE IF NOT EXISTS pg_temp.pending_finalize(
    forecast_id uuid PRIMARY KEY
  ) ON COMMIT PRESERVE ROWS;

  INSERT INTO pg_temp.pending_finalize(forecast_id)
  SELECT DISTINCT forecast_id
  FROM new_batch
  WHERE forecast_id IS NOT NULL
  ON CONFLICT DO NOTHING;

  INSERT INTO engine._tx_sentinel_staging(txid)
  VALUES (txid_current())
  ON CONFLICT DO NOTHING;

  RETURN NULL;
END;
$$;

CREATE TRIGGER trg_collect_ids_stmt
AFTER INSERT ON engine.staging_historical
REFERENCING NEW TABLE AS new_batch
FOR EACH STATEMENT
EXECUTE FUNCTION engine.trg_collect_staging_stmt();

-- 6b) COMMIT-time finalizer (deferred constraint trigger on sentinel; runs ONCE)
CREATE OR REPLACE FUNCTION engine.trg_finalize_staging_commit()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE r record;
BEGIN
  IF to_regclass('pg_temp.pending_finalize') IS NOT NULL THEN
    FOR r IN SELECT DISTINCT forecast_id FROM pg_temp.pending_finalize LOOP
      PERFORM engine.build_instance_historical(r.forecast_id);
      PERFORM engine.update_forecast_registry(r.forecast_id);
    END LOOP;
    TRUNCATE pg_temp.pending_finalize;
  END IF;

  DELETE FROM engine._tx_sentinel_staging WHERE txid = NEW.txid;
  RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER trg_on_staging_commit
AFTER INSERT ON engine._tx_sentinel_staging
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION engine.trg_finalize_staging_commit();

-- [7] HISTORICAL FLOW — same pattern (collect per statement; finalize ON COMMIT)

CREATE OR REPLACE FUNCTION engine.trg_collect_hist_stmt()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  CREATE TEMP TABLE IF NOT EXISTS pg_temp.pending_hist_finalize(
    forecast_id uuid PRIMARY KEY
  ) ON COMMIT PRESERVE ROWS;

  INSERT INTO pg_temp.pending_hist_finalize(forecast_id)
  SELECT DISTINCT forecast_id
  FROM new_hist
  WHERE forecast_id IS NOT NULL
  ON CONFLICT DO NOTHING;

  INSERT INTO engine._tx_sentinel_hist(txid)
  VALUES (txid_current())
  ON CONFLICT DO NOTHING;

  RETURN NULL;
END;
$$;

CREATE TRIGGER trg_collect_hist_stmt
AFTER INSERT ON engine.instance_historical
REFERENCING NEW TABLE AS new_hist
FOR EACH STATEMENT
EXECUTE FUNCTION engine.trg_collect_hist_stmt();

CREATE OR REPLACE FUNCTION engine.trg_finalize_hist_commit()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE r record;
BEGIN
  IF to_regclass('pg_temp.pending_hist_finalize') IS NOT NULL THEN
    FOR r IN SELECT DISTINCT forecast_id FROM pg_temp.pending_hist_finalize LOOP
      PERFORM engine.delete_staging_for(r.forecast_id);
      PERFORM engine.build_sr_series_s(r.forecast_id);
      PERFORM engine.build_sr_series_sq(r.forecast_id);
      PERFORM engine.build_sr_series_sqm(r.forecast_id);
    END LOOP;
    TRUNCATE pg_temp.pending_hist_finalize;
  END IF;

  DELETE FROM engine._tx_sentinel_hist WHERE txid = NEW.txid;
  RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER trg_on_hist_commit
AFTER INSERT ON engine._tx_sentinel_hist
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION engine.trg_finalize_hist_commit();

COMMIT;
