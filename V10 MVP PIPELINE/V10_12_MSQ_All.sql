-- V10_12_MSQ_All.sql
-- Regenerated: 2025-09-22
-- Purpose: Orchestrator for MSQ that guarantees:
--   • Oldest-eligible selection (created_at ASC) where msq_complete IS NULL and SR is complete
--     (V10: pipeline_status='sr_complete'; legacy: status='sr_complete').
--   • Immediate, non-blocking claim using FOR UPDATE SKIP LOCKED + short lock_timeout,
--     setting msq_complete='running' before work begins.
--   • Per-forecast advisory lock (per series) prevents duplicate MSQ runs for the same forecast_id.
--   • End-only completion flip to 'complete'; on error, reset to NULL and append to overall_error.
-- Notes: No DDL in hot path. Compatible with engine.build_forecast_msq_core(uuid) and no-arg fallback.

BEGIN;

CREATE OR REPLACE FUNCTION engine.run_msq_all()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  use_v10    boolean;
  has_msqcol boolean;
  rec        RECORD;
BEGIN
  -- Guardrails
  PERFORM set_config('statement_timeout','3600000', true);
  PERFORM set_config('lock_timeout','250ms', true);

  -- Detect V10 registry and presence of msq_complete
  SELECT EXISTS (
           SELECT 1 FROM information_schema.columns
           WHERE table_schema='engine' AND table_name='forecast_registry' AND column_name='pipeline_status'
         )
    INTO use_v10;

  SELECT EXISTS (
           SELECT 1 FROM information_schema.columns
           WHERE table_schema='engine' AND table_name='forecast_registry' AND column_name='msq_complete'
         )
    INTO has_msqcol;

  -- Process until queue empty
  LOOP
    -- 1) Claim the OLDEST eligible forecast_id without blocking anyone else
    IF use_v10 THEN
      WITH cte AS (
        SELECT fr.forecast_id
          FROM engine.forecast_registry fr
         WHERE fr.pipeline_status = 'sr_complete'
           AND (CASE WHEN has_msqcol THEN fr.msq_complete IS NULL ELSE TRUE END)
         ORDER BY fr.created_at ASC NULLS LAST
         FOR UPDATE SKIP LOCKED
         LIMIT 1
      )
      UPDATE engine.forecast_registry fr
         SET msq_complete = CASE WHEN has_msqcol THEN 'running' ELSE fr.msq_complete END,
             updated_at   = now()
        FROM cte
       WHERE fr.forecast_id = cte.forecast_id
       RETURNING fr.forecast_id INTO rec;
    ELSE
      WITH cte AS (
        SELECT fr.forecast_id
          FROM engine.forecast_registry fr
         WHERE fr.status = 'sr_complete'
           AND (CASE WHEN has_msqcol THEN fr.msq_complete IS NULL ELSE TRUE END)
         ORDER BY fr.created_at ASC NULLS LAST
         FOR UPDATE SKIP LOCKED
         LIMIT 1
      )
      UPDATE engine.forecast_registry fr
         SET msq_complete = CASE WHEN has_msqcol THEN 'running' ELSE fr.msq_complete END,
             updated_at   = now()
        FROM cte
       WHERE fr.forecast_id = cte.forecast_id
       RETURNING fr.forecast_id INTO rec;
    END IF;

    -- None left
    IF rec.forecast_id IS NULL THEN
      EXIT;
    END IF;

    -- 2) Per-forecast advisory lock (per series)
    PERFORM pg_advisory_xact_lock(
      (hashtext(rec.forecast_id::text)::bigint # hashtext('engine.run_msq_all')::bigint)
    );

    -- 3) Execute core for this forecast_id
    BEGIN
      PERFORM engine.build_forecast_msq_core(rec.forecast_id);
    EXCEPTION WHEN undefined_function THEN
      PERFORM engine.build_forecast_msq_core();
    END;

    -- 4) Mark complete (fast flip)
    IF has_msqcol THEN
      UPDATE engine.forecast_registry
         SET msq_complete='complete', updated_at=now(), overall_error=NULL
       WHERE forecast_id = rec.forecast_id;
    ELSE
      UPDATE engine.forecast_registry
         SET updated_at=now(), overall_error=NULL
       WHERE forecast_id = rec.forecast_id;
    END IF;

    -- next loop
  END LOOP;

EXCEPTION WHEN OTHERS THEN
  -- Best-effort reset & log for the current id
  BEGIN
    IF rec.forecast_id IS NOT NULL AND has_msqcol THEN
      UPDATE engine.forecast_registry
         SET msq_complete=NULL, updated_at=now(),
             overall_error = COALESCE(overall_error,'') || CASE WHEN overall_error IS NULL THEN '' ELSE E'\n' END
                             || '['||to_char(now(),'YYYY-MM-DD"T"HH24:MI:SSOF')||'] MSQ-all failed: '||SQLERRM
       WHERE forecast_id = rec.forecast_id;
    ELSIF rec.forecast_id IS NOT NULL THEN
      UPDATE engine.forecast_registry
         SET updated_at=now(),
             overall_error = COALESCE(overall_error,'') || CASE WHEN overall_error IS NULL THEN '' ELSE E'\n' END
                             || '['||to_char(now(),'YYYY-MM-DD"T"HH24:MI:SSOF')||'] MSQ-all failed: '||SQLERRM
       WHERE forecast_id = rec.forecast_id;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    NULL;
  END;
  RAISE;
END;
$$;

GRANT EXECUTE ON FUNCTION engine.run_msq_all() TO aq_engine_owner;

COMMIT;
