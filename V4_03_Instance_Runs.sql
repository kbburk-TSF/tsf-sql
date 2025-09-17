-- V4_03_Instance_Runs.sql
-- Per-job audit table for forecast workflow (Milestone 2)
-- VC V4.0 (2025-09-17): New table + indexes + grants; no views, no triggers. Idempotent and safe to run repeatedly.

BEGIN;

-- Ensure target schema exists
CREATE SCHEMA IF NOT EXISTS engine;

-- Per-job audit (one row per job attempt). Orchestrator writes/updates these rows.
CREATE TABLE IF NOT EXISTS engine.instance_runs (
  run_id      uuid PRIMARY KEY,                              -- unique per attempt
  forecast_id uuid NOT NULL,                                 -- batch identifier
  phase       text NOT NULL,                                 -- 'historical' | 'sr' | 'forecast_instance'
  model       text,                                          -- <table> name when applicable
  series      text,                                          -- 's' | 'sq' | 'sqm' (lowercase)
  status      text NOT NULL,                                 -- 'queued' | 'running' | 'succeeded' | 'failed'
  started_at  timestamptz DEFAULT now(),
  finished_at timestamptz,
  rowcount    bigint,
  error_text  text,
  -- lightweight guards (keep text for flexibility; enforce allowed values)
  CONSTRAINT instance_runs_phase_chk  CHECK (phase IN ('historical','sr','forecast_instance')),
  CONSTRAINT instance_runs_series_chk CHECK (series IS NULL OR lower(series) IN ('s','sq','sqm')),
  CONSTRAINT instance_runs_status_chk CHECK (status IN ('queued','running','succeeded','failed'))
);

-- Helpful indexes for status pages & troubleshooting
CREATE INDEX IF NOT EXISTS ix_instance_runs_forecast_id
  ON engine.instance_runs (forecast_id);

CREATE INDEX IF NOT EXISTS ix_instance_runs_phase_status
  ON engine.instance_runs (phase, status);

CREATE INDEX IF NOT EXISTS ix_instance_runs_started_at
  ON engine.instance_runs (started_at DESC);

-- Optional fast lookup for active work
CREATE INDEX IF NOT EXISTS ix_instance_runs_active
  ON engine.instance_runs (status)
  WHERE status IN ('queued','running');

-- Visibility & access
GRANT SELECT ON TABLE engine.instance_runs TO matrix_reader;
GRANT SELECT, INSERT, UPDATE ON TABLE engine.instance_runs TO tsf_engine_app;

COMMIT;
