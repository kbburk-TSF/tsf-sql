-- V8_00_Create_Engine_Schema_And_Workers_AllCore.sql
-- ==============================================================================
-- VERSION: V7.01.0  (2025-09-18)
-- PURPOSE:
--   Fresh, parallel-safe engine schema that *includes all V6 core tables*
--   (staging_historical, forecast_registry, instance_historical, instance_runs,
--    runtime_settings, batch_queue, job_queue) *plus* the new lock-avoidant
--   worker architecture (atomic UPDATE ... RETURNING claim).
--
--   This file does NOT define SR core functions; you must already have:
--     engine.build_sr_series_s_core(uuid, text)
--     engine.build_sr_series_sq_core(uuid, text)
--     engine.build_sr_series_sqm_core(uuid, text)
-- ==============================================================================

BEGIN;

-- ----------------------------------------------------------------------------
-- SCHEMA
-- ----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS engine;
REVOKE ALL ON SCHEMA engine FROM PUBLIC;

-- ----------------------------------------------------------------------------
-- CORE TABLES (carried forward from V6)
-- ----------------------------------------------------------------------------

-- 1) STAGING (landing for uploaded historical rows)
CREATE TABLE IF NOT EXISTS engine.staging_historical (
  forecast_id   uuid NOT NULL,
  forecast_name text,
  "DATE"        date,
  "VALUE"       double precision,
  "SES-M"       double precision, "SES-Q" double precision,
  "HWES-M"      double precision, "HWES-Q" double precision,
  "ARIMA-M"     double precision, "ARIMA-Q" double precision,
  uploaded_at   timestamptz DEFAULT now(),
  created_at    timestamptz DEFAULT now()
);

-- 2) FORECAST REGISTRY
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

-- 3) INSTANCE HISTORICAL (normalized)
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

-- 4) RUN AUDIT / METRICS
CREATE TABLE IF NOT EXISTS engine.instance_runs (
  run_id      uuid PRIMARY KEY,
  forecast_id uuid NOT NULL,
  phase       text NOT NULL,               -- historical | sr | forecast_instance
  model       text,
  series      text,                        -- s | sq | sqm
  status      text NOT NULL,               -- queued | running | succeeded | failed
  started_at  timestamptz,
  finished_at timestamptz,
  rowcount    bigint,
  error_text  text
);

-- ----------------------------------------------------------------------------
-- PARALLEL PIPELINE SUPPORT (batch+job queues, runtime settings)
-- ----------------------------------------------------------------------------

-- BATCH QUEUE — one row per forecast batch; one ACTIVE at a time
CREATE TABLE IF NOT EXISTS engine.batch_queue (
  forecast_id uuid PRIMARY KEY,
  enqueued_at timestamptz NOT NULL DEFAULT now(),
  status text NOT NULL CHECK (status IN ('queued','active','done','failed')),
  error_text text,
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- JOB QUEUE — individual runnable jobs (SR focus here)
CREATE TABLE IF NOT EXISTS engine.job_queue (
  job_id       bigserial PRIMARY KEY,
  forecast_id  uuid NOT NULL REFERENCES engine.batch_queue(forecast_id) ON DELETE CASCADE,
  phase        text NOT NULL CHECK (phase IN ('historical','sr','fi')),
  model_name   text NOT NULL,                             -- base model name only
  series       text NOT NULL CHECK (series IN ('S','SQ','SQM')),
  status       text NOT NULL DEFAULT 'queued' CHECK (status IN ('queued','running','done','failed')),
  started_at   timestamptz,
  finished_at  timestamptz,
  error_text   text,
  created_at   timestamptz NOT NULL DEFAULT now(),
  updated_at   timestamptz NOT NULL DEFAULT now()
);

-- Uniqueness: only one SR job per (forecast, model, series)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE c.contype='u'
      AND n.nspname='engine'
      AND t.relname='job_queue'
      AND c.conname='uq_job_unique'
  ) THEN
    ALTER TABLE engine.job_queue
      ADD CONSTRAINT uq_job_unique UNIQUE (forecast_id, phase, model_name, series);
  END IF;
END $$;

-- RUNTIME SETTINGS
CREATE TABLE IF NOT EXISTS engine.runtime_settings (
  id int PRIMARY KEY DEFAULT 1,
  max_concurrency int NOT NULL DEFAULT 6
);
INSERT INTO engine.runtime_settings(id) VALUES (1) ON CONFLICT (id) DO NOTHING;

-- ----------------------------------------------------------------------------
-- INDEXES
-- ----------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS ix_staging_hist_fid_date ON engine.staging_historical (forecast_id, "DATE");
CREATE INDEX IF NOT EXISTS ix_hist_fid_date         ON engine.instance_historical (forecast_id, "date");
CREATE INDEX IF NOT EXISTS ix_hist_created_at       ON engine.instance_historical (created_at);
CREATE INDEX IF NOT EXISTS ix_registry_status       ON engine.forecast_registry (status);
CREATE INDEX IF NOT EXISTS ix_registry_updated_at   ON engine.forecast_registry (updated_at);
CREATE INDEX IF NOT EXISTS ix_runs_forecast_id      ON engine.instance_runs (forecast_id);
CREATE INDEX IF NOT EXISTS ix_runs_phase_status     ON engine.instance_runs (phase, status);

-- Job system indexes
CREATE INDEX IF NOT EXISTS job_queue_status_idx        ON engine.job_queue(status);
CREATE INDEX IF NOT EXISTS job_queue_batch_status_idx  ON engine.job_queue(forecast_id,status);
CREATE INDEX IF NOT EXISTS job_queue_phase_idx         ON engine.job_queue(phase);
CREATE INDEX IF NOT EXISTS job_queue_phase_status_fk   ON engine.job_queue(phase, forecast_id, status);

-- ----------------------------------------------------------------------------
-- TRIGGERS (updated_at touch)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION engine._touch_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS t_job_queue_touch   ON engine.job_queue;
CREATE TRIGGER t_job_queue_touch
BEFORE UPDATE ON engine.job_queue
FOR EACH ROW EXECUTE FUNCTION engine._touch_updated_at();

DROP TRIGGER IF EXISTS t_batch_queue_touch ON engine.batch_queue;
CREATE TRIGGER t_batch_queue_touch
BEFORE UPDATE ON engine.batch_queue
FOR EACH ROW EXECUTE FUNCTION engine._touch_updated_at();

-- ----------------------------------------------------------------------------
-- ENQUEUE HELPER (idempotent) — seeds 18 SR jobs per forecast, activates batch
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION engine.enqueue_sr_jobs(p_forecast_id uuid)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  m text;
  seasonal_models text[] := ARRAY['me_mr10','me_s_mr30','me_std','mo_s','mo_sf','wd_md'];
BEGIN
  IF p_forecast_id IS NULL THEN
    RAISE EXCEPTION 'engine.enqueue_sr_jobs: forecast_id is NULL';
  END IF;

  INSERT INTO engine.batch_queue(forecast_id, status)
  VALUES (p_forecast_id, 'active')
  ON CONFLICT (forecast_id) DO UPDATE SET status='active', updated_at=now();

  FOREACH m IN ARRAY seasonal_models LOOP
    INSERT INTO engine.job_queue(forecast_id, phase, model_name, series, status)
    VALUES (p_forecast_id, 'sr', m, 'S',   'queued')
    ON CONFLICT (forecast_id, phase, model_name, series) DO NOTHING;

    INSERT INTO engine.job_queue(forecast_id, phase, model_name, series, status)
    VALUES (p_forecast_id, 'sr', m, 'SQ',  'queued')
    ON CONFLICT (forecast_id, phase, model_name, series) DO NOTHING;

    INSERT INTO engine.job_queue(forecast_id, phase, model_name, series, status)
    VALUES (p_forecast_id, 'sr', m, 'SQM', 'queued')
    ON CONFLICT (forecast_id, phase, model_name, series) DO NOTHING;
  END LOOP;
END;
$$;

-- ----------------------------------------------------------------------------
-- WORKER PRIMITIVES (new architecture: atomic claim, no long-held locks)
-- ----------------------------------------------------------------------------

-- Dispatches a single job_id to the correct SR core; records done/failed
CREATE OR REPLACE FUNCTION engine.run_job(p_job_id bigint)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  j engine.job_queue%ROWTYPE;
BEGIN
  SELECT * INTO j FROM engine.job_queue WHERE job_id = p_job_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'engine.run_job: job % not found', p_job_id;
  END IF;

  IF j.phase <> 'sr' THEN
    RAISE EXCEPTION 'engine.run_job: unsupported phase %', j.phase;
  END IF;

  BEGIN
    IF j.series = 'S' THEN
      PERFORM engine.build_sr_series_s_core(j.forecast_id, j.model_name);
    ELSIF j.series = 'SQ' THEN
      PERFORM engine.build_sr_series_sq_core(j.forecast_id, j.model_name);
    ELSIF j.series = 'SQM' THEN
      PERFORM engine.build_sr_series_sqm_core(j.forecast_id, j.model_name);
    ELSE
      RAISE EXCEPTION 'engine.run_job: unsupported series %', j.series;
    END IF;

    UPDATE engine.job_queue
       SET status='done', finished_at=now(), error_text=NULL
     WHERE job_id = p_job_id;

  EXCEPTION WHEN OTHERS THEN
    UPDATE engine.job_queue
       SET status='failed', finished_at=now(), error_text=LEFT(SQLERRM, 1000)
     WHERE job_id = p_job_id;
    RAISE;
  END;
END;
$$;

-- Atomic claim of the next queued SR job for the ACTIVE batch (no FOR UPDATE)
CREATE OR REPLACE FUNCTION engine.worker_step()
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
  _job_id bigint;
  _fid    uuid;
BEGIN
  SELECT forecast_id INTO _fid
  FROM engine.batch_queue
  WHERE status='active'
  ORDER BY updated_at DESC
  LIMIT 1;

  IF _fid IS NULL THEN
    RETURN NULL;
  END IF;

  WITH next_job AS (
    SELECT j.job_id
    FROM engine.job_queue j
    WHERE j.status='queued'
      AND j.phase='sr'
      AND j.forecast_id=_fid
    ORDER BY j.job_id
    LIMIT 1
  ), claim AS (
    UPDATE engine.job_queue j
       SET status='running', started_at=COALESCE(started_at, now()), error_text=NULL
     WHERE j.job_id = (SELECT job_id FROM next_job)
       AND j.status='queued'
     RETURNING j.job_id
  )
  SELECT job_id INTO _job_id FROM claim;

  IF _job_id IS NULL THEN
    PERFORM engine.maybe_finish_active_batch();
    RETURN NULL;
  END IF;

  PERFORM set_config('statement_timeout','30min', true);
  PERFORM engine.run_job(_job_id);
  PERFORM engine.maybe_finish_active_batch();

  RETURN _job_id;
END;
$$;

-- Mark ACTIVE batch done when all SR jobs finished
CREATE OR REPLACE FUNCTION engine.maybe_finish_active_batch()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  _fid uuid;
  _left int;
BEGIN
  SELECT forecast_id INTO _fid
  FROM engine.batch_queue
  WHERE status='active'
  ORDER BY updated_at DESC
  LIMIT 1;

  IF _fid IS NULL THEN
    RETURN;
  END IF;

  SELECT COUNT(*) INTO _left
  FROM engine.job_queue
  WHERE forecast_id=_fid
    AND phase='sr'
    AND status IN ('queued','running');

  IF _left = 0 THEN
    UPDATE engine.batch_queue
       SET status='done', updated_at=now()
     WHERE forecast_id=_fid AND status='active';
  END IF;
END;
$$;

-- ----------------------------------------------------------------------------
-- GRANTS (minimal; extend as needed)
-- ----------------------------------------------------------------------------
REVOKE ALL     ON ALL TABLES    IN SCHEMA engine FROM PUBLIC;
REVOKE ALL     ON ALL SEQUENCES IN SCHEMA engine FROM PUBLIC;
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA engine FROM PUBLIC;

GRANT SELECT ON ALL TABLES    IN SCHEMA engine TO matrix_reader;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA engine TO matrix_reader;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA engine TO matrix_reader;

COMMIT;

-- ==============================================================================
-- USAGE:
--   -- Seed & activate a batch (example):
--   -- SELECT engine.enqueue_sr_jobs('<forecast-uuid>'::uuid);
--   -- Then open 6 windows and run:
--   -- DO $$ BEGIN FOR i IN 1..100000 LOOP PERFORM engine.worker_step(); PERFORM pg_sleep(1); END LOOP; END $$;
--   -- Monitor:
--   -- SELECT job_id, model_name, series, status, started_at, finished_at, error_text FROM engine.job_queue ORDER BY job_id;
-- ==============================================================================
