-- V4_00_Create_Engine_Roles_And_Schema.sql
-- One-shot, from-scratch initializer for the TSF Engine (Milestone 2).
-- Part A: Create required roles if missing (cluster-wide) — safe to run multiple times.
-- Part B: Create engine schema, core tables, grants, and a lightweight staging trigger.
-- VC V4.0 (2025-09-17)

/* ==============================
   PART A — ROLES (cluster-wide)
   ============================== */
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'matrix_owner') THEN
    CREATE ROLE matrix_owner NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'matrix_reader') THEN
    CREATE ROLE matrix_reader NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tsf_engine_app') THEN
    CREATE ROLE tsf_engine_app NOLOGIN INHERIT;
  END IF;
END $$;

-- Optional: make current login manage objects easily
-- GRANT matrix_owner TO CURRENT_USER;

/* ========================================
   PART B — ENGINE SCHEMA & CORE STRUCTURE
   ======================================== */
BEGIN;

-- [0] SCHEMA
CREATE SCHEMA IF NOT EXISTS engine;
REVOKE ALL ON SCHEMA engine FROM PUBLIC;

GRANT USAGE ON SCHEMA engine TO matrix_owner;
GRANT USAGE ON SCHEMA engine TO matrix_reader;

-- [1] CORE TABLES

-- 1a) STAGING (landing for uploaded classical forecast rows)
CREATE TABLE IF NOT EXISTS engine.staging_historical (
  forecast_id   uuid NOT NULL,
  forecast_name text,                      -- CSV filename goes here (front-end writes it)
  "DATE"        date,
  "VALUE"       double precision,
  "SES-M"       double precision, "SES-Q" double precision,
  "HWES-M"      double precision, "HWES-Q" double precision,
  "ARIMA-M"     double precision, "ARIMA-Q" double precision,
  uploaded_at   timestamptz DEFAULT now(),
  created_at    timestamptz DEFAULT now()
);

-- 1b) FORECAST REGISTRY
CREATE TABLE IF NOT EXISTS engine.forecast_registry (
  forecast_id          uuid PRIMARY KEY,
  forecast_name        text NOT NULL,
  source_csv_filename  text,
  classical_source_uri text,
  parameters           jsonb DEFAULT '{}'::jsonb,
  status               text  DEFAULT 'created',   -- created|staged|historical_ready|sr_running|sr_done|fi_running|complete|failed
  sr_total             int   DEFAULT 54,
  sr_completed         int   DEFAULT 0,
  fi_total             int   DEFAULT 54,
  fi_completed         int   DEFAULT 0,
  error_text           text,
  created_at           timestamptz DEFAULT now(),
  updated_at           timestamptz DEFAULT now()
);

-- 1c) INSTANCE HISTORICAL
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

-- 1d) RUN AUDIT
CREATE TABLE IF NOT EXISTS engine.instance_runs (
  run_id      uuid PRIMARY KEY,
  forecast_id uuid NOT NULL,
  phase       text NOT NULL,               -- historical | sr | forecast_instance
  model       text,                        -- <table> when applicable
  series      text,                        -- s | sq | sqm
  status      text NOT NULL,               -- queued | running | succeeded | failed
  started_at  timestamptz,
  finished_at timestamptz,
  rowcount    bigint,
  error_text  text
);

-- [2] INDEXES
CREATE INDEX IF NOT EXISTS ix_staging_hist_fid_date
  ON engine.staging_historical (forecast_id, "DATE");

CREATE INDEX IF NOT EXISTS ix_hist_fid_date
  ON engine.instance_historical (forecast_id, "date");

CREATE INDEX IF NOT EXISTS ix_hist_created_at
  ON engine.instance_historical (created_at);

CREATE INDEX IF NOT EXISTS ix_registry_status
  ON engine.forecast_registry (status);

CREATE INDEX IF NOT EXISTS ix_registry_updated_at
  ON engine.forecast_registry (updated_at);

CREATE INDEX IF NOT EXISTS ix_runs_forecast_id
  ON engine.instance_runs (forecast_id);

CREATE INDEX IF NOT EXISTS ix_runs_phase_status
  ON engine.instance_runs (phase, status);

-- [3] GRANTS
REVOKE ALL     ON ALL TABLES    IN SCHEMA engine FROM PUBLIC;
REVOKE ALL     ON ALL SEQUENCES IN SCHEMA engine FROM PUBLIC;
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA engine FROM PUBLIC;

GRANT ALL ON ALL TABLES    IN SCHEMA engine TO matrix_owner;
GRANT ALL ON ALL SEQUENCES IN SCHEMA engine TO matrix_owner;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA engine TO matrix_owner;

GRANT SELECT ON ALL TABLES    IN SCHEMA engine TO matrix_reader;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA engine TO matrix_reader;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA engine TO matrix_reader;

GRANT matrix_reader TO tsf_engine_app;

ALTER DEFAULT PRIVILEGES IN SCHEMA engine
  REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;

ALTER DEFAULT PRIVILEGES IN SCHEMA engine
  GRANT ALL ON TABLES    TO matrix_owner;
ALTER DEFAULT PRIVILEGES IN SCHEMA engine
  GRANT ALL ON SEQUENCES TO matrix_owner;
ALTER DEFAULT PRIVILEGES IN SCHEMA engine
  GRANT EXECUTE ON FUNCTIONS TO matrix_owner;

ALTER DEFAULT PRIVILEGES IN SCHEMA engine
  GRANT SELECT ON TABLES TO matrix_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA engine
  GRANT USAGE, SELECT ON SEQUENCES TO matrix_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA engine
  GRANT EXECUTE ON FUNCTIONS TO matrix_reader;

-- [4] HELPERS & TRIGGER (lightweight)

-- registry_touch: SAFE against NOT NULL on forecast_name.
-- It derives a name from existing data or falls back to the UUID text.
CREATE OR REPLACE FUNCTION engine.registry_touch(
  p_forecast_id uuid,
  p_status      text DEFAULT NULL,
  p_error_text  text DEFAULT NULL
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  _name text;
BEGIN
  -- Prefer existing name
  SELECT forecast_name INTO _name
  FROM engine.forecast_registry
  WHERE forecast_id = p_forecast_id;

  -- If missing, try staging batch
  IF _name IS NULL THEN
    SELECT MAX(forecast_name) INTO _name
    FROM engine.staging_historical
    WHERE forecast_id = p_forecast_id;
  END IF;

  -- Last resort: use UUID text (never NULL)
  _name := COALESCE(_name, p_forecast_id::text);

  INSERT INTO engine.forecast_registry (
    forecast_id, forecast_name, source_csv_filename, status, error_text, created_at, updated_at
  )
  VALUES (
    p_forecast_id, _name, _name, COALESCE(p_status,'created'), p_error_text, now(), now()
  )
  ON CONFLICT (forecast_id) DO UPDATE
    SET status               = COALESCE(EXCLUDED.status, engine.forecast_registry.status),
        error_text           = COALESCE(EXCLUDED.error_text, engine.forecast_registry.error_text),
        -- Never overwrite an existing name with NULL; otherwise keep current name
        forecast_name        = COALESCE(engine.forecast_registry.forecast_name, EXCLUDED.forecast_name),
        source_csv_filename  = COALESCE(engine.forecast_registry.source_csv_filename, EXCLUDED.source_csv_filename),
        updated_at           = now();
END;
$$;

-- Fresh, statement-level trigger: copies filename from CSV into registry and marks 'staged'.
DROP TRIGGER IF EXISTS trg_registry_on_staging_stmt ON engine.staging_historical;
DROP FUNCTION IF EXISTS engine.trg_registry_on_staging_stmt() CASCADE;

CREATE OR REPLACE FUNCTION engine.trg_registry_on_staging_stmt()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO engine.forecast_registry (
      forecast_id, forecast_name, source_csv_filename, status, created_at, updated_at
  )
  SELECT
      nb.forecast_id,
      COALESCE(MAX(nb.forecast_name), nb.forecast_id::text) AS forecast_name,
      COALESCE(MAX(nb.forecast_name), nb.forecast_id::text) AS source_csv_filename,
      'staged'::text, now(), now()
  FROM new_batch nb
  WHERE nb.forecast_id IS NOT NULL
  GROUP BY nb.forecast_id
  ON CONFLICT (forecast_id) DO UPDATE
    SET status               = 'staged',
        forecast_name        = COALESCE(engine.forecast_registry.forecast_name, EXCLUDED.forecast_name),
        source_csv_filename  = COALESCE(engine.forecast_registry.source_csv_filename, EXCLUDED.source_csv_filename),
        updated_at           = now();

  RETURN NULL;
END;
$$;

CREATE TRIGGER trg_registry_on_staging_stmt
AFTER INSERT ON engine.staging_historical
REFERENCING NEW TABLE AS new_batch
FOR EACH STATEMENT
EXECUTE FUNCTION engine.trg_registry_on_staging_stmt();

COMMIT;
