-- V10_13_MSQM_All.sql
-- Regenerated: 2025-09-22
-- Wrapper: MSQM runner with OLDEST-ELIGIBLE selection, IMMEDIATE non-blocking claim,
--          per-forecast advisory lock, and END-ONLY completion flip.
-- Notes: No schema or forecast-core logic here; wrapper only.
-- Behavior:
--   • Oldest-eligible: pick oldest created_at where SR complete AND msqm_complete IS NULL
--     (V10: pipeline_status='sr_complete'; legacy: status='sr_complete').
--   • Immediate claim: one CTE + UPDATE sets msqm_complete='running' right away (SKIP LOCKED).
--   • Non-blocking: FOR UPDATE SKIP LOCKED and short lock_timeout.
--   • Per-forecast advisory lock (per series) prevents duplicate MSQM work on same id.
--   • End-only completion flip: 'complete' on success; on error reset to NULL and append to overall_error.

BEGIN;

CREATE OR REPLACE FUNCTION engine.run_msqm_all()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  use_v10     boolean;
  has_msqmcol boolean;
  rec_id      uuid;
BEGIN
  -- Guardrails
  PERFORM set_config('statement_timeout','3600000', true);
  PERFORM set_config('lock_timeout','250ms', true);

  -- Detect V10 registry and presence of msqm_complete
  SELECT EXISTS (
           SELECT 1 FROM information_schema.columns
           WHERE table_schema='engine' AND table_name='forecast_registry' AND column_name='pipeline_status'
         )
    INTO use_v10;

  SELECT EXISTS (
           SELECT 1 FROM information_schema.columns
           WHERE table_schema='engine' AND table_name='forecast_registry' AND column_name='msqm_complete'
         )
    INTO has_msqmcol;

  IF NOT has_msqmcol THEN
    RAISE EXCEPTION 'engine.forecast_registry.msqm_complete is required for run_msqm_all()';
  END IF;

  -- Process until queue empty
  LOOP
    -- 1) Oldest-eligible + IMMEDIATE CLAIM (non-blocking via SKIP LOCKED)
    IF use_v10 THEN
      WITH candidate AS (
        SELECT fr.forecast_id
          FROM engine.forecast_registry fr
         WHERE fr.pipeline_status = 'sr_complete'
           AND fr.msqm_complete IS NULL
         ORDER BY fr.created_at ASC NULLS LAST
         FOR UPDATE SKIP LOCKED
         LIMIT 1
      )
      UPDATE engine.forecast_registry fr
         SET msqm_complete='running', updated_at=now()
        FROM candidate
       WHERE fr.forecast_id = candidate.forecast_id
       RETURNING fr.forecast_id INTO rec_id;
    ELSE
      WITH candidate AS (
        SELECT fr.forecast_id
          FROM engine.forecast_registry fr
         WHERE fr.status = 'sr_complete'
           AND fr.msqm_complete IS NULL
         ORDER BY fr.created_at ASC NULLS LAST
         FOR UPDATE SKIP LOCKED
         LIMIT 1
      )
      UPDATE engine.forecast_registry fr
         SET msqm_complete='running', updated_at=now()
        FROM candidate
       WHERE fr.forecast_id = candidate.forecast_id
       RETURNING fr.forecast_id INTO rec_id;
    END IF;

    -- None left
    IF rec_id IS NULL THEN
      EXIT;
    END IF;

    -- 2) Per-forecast advisory lock (per series)
    PERFORM pg_advisory_xact_lock(
      (hashtext(rec_id::text)::bigint # hashtext('engine.run_msqm_all')::bigint)
    );

    -- 3) Execute core for THIS id
    BEGIN
      PERFORM engine.build_forecast_msqm_core(rec_id);
    EXCEPTION WHEN undefined_function THEN
      PERFORM engine.build_forecast_msqm_core();
    END;

    -- 4) Mark complete (fast flip)
    UPDATE engine.forecast_registry
       SET msqm_complete='complete', updated_at=now(), overall_error=NULL
     WHERE forecast_id = rec_id;

    -- Next
    rec_id := NULL;
  END LOOP;

EXCEPTION WHEN OTHERS THEN
  -- Best-effort reset & log for current id
  BEGIN
    IF rec_id IS NOT NULL THEN
      UPDATE engine.forecast_registry
         SET msqm_complete=NULL, updated_at=now(),
             overall_error = COALESCE(overall_error,'') || CASE WHEN overall_error IS NULL THEN '' ELSE E'\n' END
                             || '['||to_char(now(),'YYYY-MM-DD"T"HH24:MI:SSOF')||'] MSQM-all failed: '||SQLERRM
       WHERE forecast_id = rec_id;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    NULL;
  END;
  RAISE;
END;
$$;

GRANT EXECUTE ON FUNCTION engine.run_msqm_all() TO aq_engine_owner;

COMMIT;
