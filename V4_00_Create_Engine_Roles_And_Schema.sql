-- V4_00_Create_Engine_Roles_And_Schema.sql
-- One-shot, from-scratch initializer for the TSF Engine (Milestone 2).
-- Part A: Create required roles if missing (cluster-wide) — safe to run multiple times.
-- Part B: Create engine schema, core tables, grants, and lightweight registry trigger.
-- VC V4.0 (2025-09-17)

/* ==============================
   PART A — ROLES (cluster-wide)
   ============================== */
-- NOTE: Roles in Postgres are cluster-wide. You run this once on the branch;
--       GRANTs to objects are per-database and are applied in Part B.
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

-- (Optional) make your current login a member of matrix_owner so you can manage objects easily.
-- Uncomment if desired:
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

-- [1] CORE TABLES (additive only)

-- 1a) STAGING (landing for uploaded classical forecast file)
CREATE TABLE IF NOT EXISTS engine.staging_historical (
  forecast_id   uuid NOT NULL,
  forecast_name text,
  "DATE"        date,
  "VALUE"       double precision,
  "SES-M"       double precision, "SES-Q" double precision,
  "HWES-M"      double precision, "HWES-Q" double precision,
  "ARIMA-M"     double precision, "ARIMA-Q" double precision,
  uploaded_at   timestamptz DEFAULT now(),
  created_at    timestamptz DEFAULT now()   -- explicit created_at for consistency
);

-- 1b) FORECAST REGISTRY (expanded for batch status + counters)
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

-- 1c) INSTANCE HISTORICAL (per-forecast snapshot; populated elsewhere)
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

-- 1d) JOB/RUN AUDIT (per-attempt visibility)
CREATE TABLE IF NOT EXISTS engine.instance_runs (
  run_id      uuid PRIMARY KEY,
  forecast_id uuid NOT NULL,
  phase       text NOT NULL,               -- historical | sr | forecast_instance
  model       text,                        -- <table> value when applicable
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

-- [3] GRANTS (object-level; PUBLIC removed)
REVOKE ALL     ON ALL TABLES    IN SCHEMA engine FROM PUBLIC;
REVOKE ALL     ON ALL SEQUENCES IN SCHEMA engine FROM PUBLIC;
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA engine FROM PUBLIC;

-- matrix_owner: full control
GRANT ALL ON ALL TABLES    IN SCHEMA engine TO matrix_owner;
GRANT ALL ON ALL SEQUENCES IN SCHEMA engine TO matrix_owner;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA engine TO matrix_owner;

-- matrix_reader: read-only
GRANT SELECT ON ALL TABLES    IN SCHEMA engine TO matrix_reader;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA engine TO matrix_reader;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA engine TO matrix_reader;

-- App role inherits reader
GRANT matrix_reader TO tsf_engine_app;

-- Default privileges for future objects
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

-- [4] LIGHTWEIGHT REGISTRY HELPER & TRIGGER (no heavy compute)

-- Helper: minimal, idempotent registry updater
CREATE OR REPLACE FUNCTION engine.registry_touch(
  p_forecast_id uuid,
  p_status      text DEFAULT NULL,
  p_error_text  text DEFAULT NULL
) RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO engine.forecast_registry (forecast_id, status, created_at, updated_at)
  VALUES (p_forecast_id, COALESCE(p_status, 'created'), now(), now())
  ON CONFLICT (forecast_id) DO UPDATE
  SET status    = COALESCE(EXCLUDED.status, engine.forecast_registry.status),
      error_text= COALESCE(p_error_text, engine.forecast_registry.error_text),
      updated_at= now();
END;
$$;

-- Statement-level trigger: mark batches as 'staged' when rows land in staging
DROP TRIGGER IF EXISTS trg_registry_on_staging_stmt ON engine.staging_historical;
DROP FUNCTION IF EXISTS engine.trg_registry_on_staging_stmt() CASCADE;

CREATE OR REPLACE FUNCTION engine.trg_registry_on_staging_stmt()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  -- 1) Ensure a registry row exists and flip status to 'staged' for each forecast_id in this batch
  PERFORM engine.registry_touch(n.forecast_id, 'staged', NULL)
  FROM (SELECT DISTINCT forecast_id FROM new_batch WHERE forecast_id IS NOT NULL) AS n;

  -- 2) If the CSV carried a forecast_name, copy it into registry.forecast_name AND source_csv_filename
  --    (do not overwrite existing values with NULLs)
  UPDATE engine.forecast_registry r
  SET forecast_name       = COALESCE(s.forecast_name, r.forecast_name),
      source_csv_filename = COALESCE(s.forecast_name, r.source_csv_filename),
      updated_at          = now()
  FROM (
    SELECT forecast_id, MAX(forecast_name) AS forecast_name
    FROM new_batch
    WHERE forecast_name IS NOT NULL
    GROUP BY forecast_id
  ) AS s
  WHERE r.forecast_id = s.forecast_id;

  RETURN NULL;
END;
$$;

CREATE TRIGGER trg_registry_on_staging_stmt
AFTER INSERT ON engine.staging_historical
REFERENCING NEW TABLE AS new_batch
FOR EACH STATEMENT
EXECUTE FUNCTION engine.trg_registry_on_staging_stmt();

COMMIT;
