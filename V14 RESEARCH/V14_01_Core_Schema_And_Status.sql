-- V14_01_Core_Schema_And_Status.sql
-- Generated: 2025-10-04
-- Change: Add 'demo' column to engine.forecast_registry; no other changes.
-- =====================================================================

-- V11_00_Core_Schema_And_Status.sql
-- Generated: 2025-09-24
-- Change: Removed all CREATE VIEW statements only; no other changes.
-- =====================================================================
-- V10_01_Core_Schema_And_Status.sql
-- Generated: 2025-09-22T14:43:58Z
-- Purpose: Core 'engine' schema + UPDATED status tracking for the new 4-step pipeline.
-- Change from V10_00: FIX nested dollar-quoting in DO blocks (use $ddl$ for EXECUTE).
-- =============================================================================

BEGIN;

CREATE SCHEMA IF NOT EXISTS engine;

-- 1) staging_historical (unchanged)
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

-- 2) forecast_registry (new tracking model)
CREATE TABLE IF NOT EXISTS engine.forecast_registry (
  forecast_id          uuid PRIMARY KEY,
  forecast_name        text NOT NULL,
  source_csv_filename  text,
  classical_source_uri text,
  parameters           jsonb DEFAULT '{}'::jsonb,
  pipeline_status      text NOT NULL DEFAULT 'historical_ready'
                       CHECK (pipeline_status IN ('historical_ready','sr_complete')),
  ms_complete          text CHECK (ms_complete  IN ('running','complete')),
  msq_complete         text CHECK (msq_complete IN ('running','complete')),
  msqm_complete        text CHECK (msqm_complete IN ('running','complete')),
  overall_error        text,
  demo boolean,
  created_at           timestamptz DEFAULT now(),
  updated_at           timestamptz DEFAULT now());

-- Backward-safe ALTERs with correct quoting
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='engine' AND table_name='forecast_registry' AND column_name='pipeline_status'
  ) THEN
    EXECUTE $ddl$ALTER TABLE engine.forecast_registry
             ADD COLUMN pipeline_status text NOT NULL DEFAULT 'historical_ready'
             CHECK (pipeline_status IN ('historical_ready','sr_complete'))$ddl$;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='engine' AND table_name='forecast_registry' AND column_name='ms_complete'
  ) THEN
    EXECUTE $ddl$ALTER TABLE engine.forecast_registry
             ADD COLUMN ms_complete text CHECK (ms_complete IN ('running','complete'))$ddl$;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='engine' AND table_name='forecast_registry' AND column_name='msq_complete'
  ) THEN
    EXECUTE $ddl$ALTER TABLE engine.forecast_registry
             ADD COLUMN msq_complete text CHECK (msq_complete IN ('running','complete'))$ddl$;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='engine' AND table_name='forecast_registry' AND column_name='msqm_complete'
  ) THEN
    EXECUTE $ddl$ALTER TABLE engine.forecast_registry
             ADD COLUMN msqm_complete text CHECK (msqm_complete IN ('running','complete'))$ddl$;
  END IF;

  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema='engine' AND table_name='forecast_registry' AND column_name='status') THEN
    EXECUTE $ddl$ALTER TABLE engine.forecast_registry DROP COLUMN status$ddl$;
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema='engine' AND table_name='forecast_registry' AND column_name='sr_total') THEN
    EXECUTE $ddl$ALTER TABLE engine.forecast_registry DROP COLUMN sr_total$ddl$;
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema='engine' AND table_name='forecast_registry' AND column_name='sr_completed') THEN
    EXECUTE $ddl$ALTER TABLE engine.forecast_registry DROP COLUMN sr_completed$ddl$;
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema='engine' AND table_name='forecast_registry' AND column_name='fi_total') THEN
    EXECUTE $ddl$ALTER TABLE engine.forecast_registry DROP COLUMN fi_total$ddl$;
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema='engine' AND table_name='forecast_registry' AND column_name='fi_completed') THEN
    EXECUTE $ddl$ALTER TABLE engine.forecast_registry DROP COLUMN fi_completed$ddl$;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='engine' AND table_name='forecast_registry' AND column_name='demo'
  ) THEN
    EXECUTE $ddl$ALTER TABLE engine.forecast_registry
             ADD COLUMN demo boolean$ddl$;
  END IF;

END$$;

CREATE INDEX IF NOT EXISTS ix_registry_pipeline_status ON engine.forecast_registry (pipeline_status);
CREATE INDEX IF NOT EXISTS ix_registry_ms_done   ON engine.forecast_registry (ms_complete);
CREATE INDEX IF NOT EXISTS ix_registry_msq_done  ON engine.forecast_registry (msq_complete);
CREATE INDEX IF NOT EXISTS ix_registry_msqm_done ON engine.forecast_registry (msqm_complete);
CREATE INDEX IF NOT EXISTS ix_registry_updated_at ON engine.forecast_registry (updated_at);

-- 3) instance_historical (unchanged structure)
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

-- 4) instance_runs (headers)
CREATE TABLE IF NOT EXISTS engine.instance_runs (
  run_id        uuid PRIMARY KEY,
  forecast_id   uuid NOT NULL,
  status        text NOT NULL DEFAULT 'queued', -- queued|running|done|error
  created_at    timestamptz DEFAULT now(),
  started_at    timestamptz,
  finished_at   timestamptz,
  overall_error text
);

-- 5) instance_run_phases (4-phase pipeline + legacy allowed)
DO $$
BEGIN
  -- Ensure table exists first
  EXECUTE $ddl$CREATE TABLE IF NOT EXISTS engine.instance_run_phases (
    run_id       uuid        NOT NULL,
    forecast_id  uuid        NOT NULL,
    phase        text        NOT NULL CHECK (phase IN (
                     'sr_all','fc_ms_all','fc_msq_all','fc_msqm_all',
                     'sr_s','sr_sq','sr_sqm','fc_ms','fc_msq','fc_msqm'
                   )),
    status       text        NOT NULL CHECK (status IN ('queued','running','done','error')),
    started_at   timestamptz,
    finished_at  timestamptz,
    rows_written bigint,
    message      text,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT pk_instance_run_phases PRIMARY KEY (run_id, phase),
    CONSTRAINT fk_instance_run_phases_run
      FOREIGN KEY (run_id) REFERENCES engine.instance_runs(run_id) ON DELETE CASCADE
  )$ddl$;

  -- If a stricter CHECK exists, attempt to drop it (best-effort)
  BEGIN
    ALTER TABLE engine.instance_run_phases DROP CONSTRAINT IF EXISTS instance_run_phases_phase_check;
  EXCEPTION WHEN undefined_table THEN
    NULL;
  END;
END$$;

CREATE INDEX IF NOT EXISTS ix_irp_forecast_id_created ON engine.instance_run_phases (forecast_id, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_irp_status              ON engine.instance_run_phases (status);

-- Utility trigger
CREATE OR REPLACE FUNCTION engine._touch_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS t_instance_run_phases_touch ON engine.instance_run_phases;
CREATE TRIGGER t_instance_run_phases_touch
BEFORE UPDATE ON engine.instance_run_phases
FOR EACH ROW EXECUTE FUNCTION engine._touch_updated_at();

-- Helpers (seed 4 phases)
CREATE OR REPLACE FUNCTION engine.seed_instance_run_phases(p_run_id uuid, p_forecast_id uuid)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  phases text[] := ARRAY['sr_all','fc_ms_all','fc_msq_all','fc_msqm_all'];
  ph text;
BEGIN
  FOREACH ph IN ARRAY phases LOOP
    INSERT INTO engine.instance_run_phases(run_id, forecast_id, phase, status)
    VALUES (p_run_id, p_forecast_id, ph, 'queued')
    ON CONFLICT (run_id, phase) DO NOTHING;
  END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION engine.start_instance_run(p_forecast_id uuid, p_run_id uuid DEFAULT NULL)
RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE
  v_run_id uuid := p_run_id;
  v_new_id uuid;
  v_can_gen bool;
BEGIN
  IF v_run_id IS NULL THEN
    v_can_gen := EXISTS (
      SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
      WHERE n.nspname IN ('public','pgcrypto') AND p.proname = 'gen_random_uuid'
    );
    IF v_can_gen THEN
      SELECT gen_random_uuid() INTO v_new_id;
    ELSE
      v_can_gen := EXISTS (
        SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname IN ('public') AND p.proname = 'uuid_generate_v4'
      );
      IF v_can_gen THEN
        SELECT uuid_generate_v4() INTO v_new_id;
      ELSE
        RAISE EXCEPTION 'No UUID generator found. Provide run_id or enable pgcrypto/uuid-ossp.';
      END IF;
    END IF;
    v_run_id := v_new_id;
  END IF;

  INSERT INTO engine.instance_runs (run_id, forecast_id, status, created_at, started_at)
  VALUES (v_run_id, p_forecast_id, 'queued', now(), NULL)
  ON CONFLICT (run_id) DO NOTHING;

  PERFORM engine.seed_instance_run_phases(v_run_id, p_forecast_id);
  RETURN v_run_id;
END;
$$;

CREATE OR REPLACE FUNCTION engine.set_phase_status(
  p_run_id uuid,
  p_phase text,
  p_new_status text,
  p_message text DEFAULT NULL,
  p_rows_written bigint DEFAULT NULL
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  v_now timestamptz := now();
BEGIN
  IF p_new_status NOT IN ('queued','running','done','error') THEN
    RAISE EXCEPTION 'Invalid status: %', p_new_status;
  END IF;

  UPDATE engine.instance_run_phases rp
  SET status = p_new_status,
      message = COALESCE(p_message, rp.message),
      rows_written = COALESCE(p_rows_written, rp.rows_written),
      started_at = CASE WHEN p_new_status = 'running' AND rp.started_at IS NULL THEN v_now ELSE rp.started_at END,
      finished_at = CASE WHEN p_new_status IN ('done','error') THEN v_now
                         WHEN p_new_status IN ('queued','running') THEN NULL
                         ELSE rp.finished_at END
  WHERE rp.run_id = p_run_id AND rp.phase = p_phase;

  IF p_new_status = 'running' THEN
    UPDATE engine.instance_runs SET status='running', started_at=COALESCE(started_at, v_now)
    WHERE run_id = p_run_id;
  ELSIF p_new_status = 'error' THEN
    UPDATE engine.instance_runs SET status='error', overall_error=COALESCE(p_message, overall_error), finished_at=v_now
    WHERE run_id = p_run_id;
  ELSIF p_new_status = 'done' THEN
    UPDATE engine.instance_runs ir
    SET status='done', finished_at=v_now
    WHERE ir.run_id = p_run_id
      AND NOT EXISTS (
        SELECT 1 FROM engine.instance_run_phases rp2
        WHERE rp2.run_id = ir.run_id AND rp2.status IS DISTINCT FROM 'done'
      );
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION engine.retry_from_phase(p_run_id uuid, p_phase text)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  phases text[] := ARRAY['sr_all','fc_ms_all','fc_msq_all','fc_msqm_all'];
  i int; idx int := NULL; ph text;
BEGIN
  FOR i IN 1..array_length(phases,1) LOOP
    IF phases[i] = p_phase THEN idx := i; EXIT; END IF;
  END LOOP;
  IF idx IS NULL THEN RAISE EXCEPTION 'Unknown phase: %', p_phase; END IF;

  FOR i IN idx..array_length(phases,1) LOOP
    ph := phases[i];
    UPDATE engine.instance_run_phases
      SET status='queued', started_at=NULL, finished_at=NULL, message=NULL, rows_written=NULL
    WHERE run_id = p_run_id AND phase = ph;
  END LOOP;

  UPDATE engine.instance_runs ir
  SET status = CASE WHEN EXISTS (
                      SELECT 1 FROM engine.instance_run_phases rp
                      WHERE rp.run_id = ir.run_id AND rp.status='done')
                    THEN 'running' ELSE 'queued' END,
      finished_at = NULL,
      overall_error = NULL
  WHERE ir.run_id = p_run_id;
END;
$$;

-- Views
-- [V11_00] View removed.

-- [V11_00] View removed.

-- [V11_00] View removed.

-- [V11_00] View removed.

-- Grants
GRANT ALL ON SCHEMA engine TO aq_engine_owner;
GRANT ALL PRIVILEGES ON ALL TABLES     IN SCHEMA engine TO aq_engine_owner;
GRANT ALL PRIVILEGES ON ALL SEQUENCES  IN SCHEMA engine TO aq_engine_owner;
GRANT EXECUTE         ON ALL FUNCTIONS IN SCHEMA engine TO aq_engine_owner;
ALTER DEFAULT PRIVILEGES IN SCHEMA engine GRANT ALL PRIVILEGES ON TABLES     TO aq_engine_owner;
ALTER DEFAULT PRIVILEGES IN SCHEMA engine GRANT ALL PRIVILEGES ON SEQUENCES  TO aq_engine_owner;
ALTER DEFAULT PRIVILEGES IN SCHEMA engine GRANT EXECUTE         ON FUNCTIONS  TO aq_engine_owner;
ALTER DEFAULT PRIVILEGES IN SCHEMA engine GRANT USAGE           ON TYPES      TO aq_engine_owner;

COMMIT;