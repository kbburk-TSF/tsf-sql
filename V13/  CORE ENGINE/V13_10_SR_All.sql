-- V10_10_SR_All.sql
-- 2025-09-27: Clean wrapper — calls only *_core(forecast_id); no preflight, no table touching outside cores.
-- Regenerated: 2025-09-22T15:23:54Z
-- Purpose: Manual, NO-ARG trigger to create ALL SR tables and run ALL SR series (S, SQ, SQM)
--          for every forecast_id marked 'historical_ready' in the registry.
-- Notes:
--   * Preflight: calls zero-arg SR wrappers once (they create missing destination tables).
--   * Main loop: for each applicable forecast_id, runs S -> SQ -> SQM core builders.
--   * Compatibility: works with either legacy 'status' or new 'pipeline_status' columns.
--   * Guardrail: 30m statement_timeout (LOCAL).
--   * Error handling: per-forecast try/catch appends to overall_error and continues.

BEGIN;

CREATE OR REPLACE FUNCTION engine.run_sr_all()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  rec RECORD;
  v_has_pipeline boolean;
BEGIN
  -- Timeout for the wrapper (LOCAL)
  PERFORM set_config('statement_timeout','1800000', true);

  -- Determine which registry status column exists
  SELECT EXISTS (
           SELECT 1 FROM information_schema.columns
           WHERE table_schema='engine' AND table_name='forecast_registry' AND column_name='pipeline_status'
         )
    INTO v_has_pipeline;

  -- ---------- Preflight: ensure SR destination tables exist (wrappers create schema only) ----------
  -- These wrappers also run the core for the latest forecast_id; that's harmless and ensures tables.



  -- ---------- Select targets ----------
  IF v_has_pipeline THEN
    FOR rec IN
      SELECT fr.forecast_id
      FROM engine.forecast_registry fr
      WHERE fr.pipeline_status = 'historical_ready'
    LOOP
      BEGIN
        PERFORM engine.build_sr_series_s_core(rec.forecast_id);
        PERFORM engine.build_sr_series_sq_core(rec.forecast_id);
        PERFORM engine.build_sr_series_sqm_core(rec.forecast_id);

        UPDATE engine.forecast_registry
           SET pipeline_status = 'sr_complete', updated_at = now()
         WHERE forecast_id = rec.forecast_id;

        -- Clear stale errors (best effort)
        UPDATE engine.forecast_registry SET overall_error = NULL
         WHERE forecast_id = rec.forecast_id AND overall_error IS NOT NULL;

      EXCEPTION WHEN OTHERS THEN
        UPDATE engine.forecast_registry
           SET overall_error = COALESCE(overall_error,'') || CASE WHEN overall_error IS NULL THEN '' ELSE E'\n' END
                               || '['||to_char(now(),'YYYY-MM-DD"T"HH24:MI:SSOF')||'] SR-all failed: '||SQLERRM,
               updated_at    = now()
         WHERE forecast_id = rec.forecast_id;
      END;
    END LOOP;
  ELSE
    -- Legacy registry (uses 'status')
    FOR rec IN
      SELECT fr.forecast_id
      FROM engine.forecast_registry fr
      WHERE fr.status = 'historical_ready'
    LOOP
      BEGIN
        PERFORM engine.build_sr_series_s_core(rec.forecast_id);
        PERFORM engine.build_sr_series_sq_core(rec.forecast_id);
        PERFORM engine.build_sr_series_sqm_core(rec.forecast_id);

        UPDATE engine.forecast_registry
           SET status = 'sr_complete', updated_at = now()
         WHERE forecast_id = rec.forecast_id;

        UPDATE engine.forecast_registry SET overall_error = NULL
         WHERE forecast_id = rec.forecast_id AND overall_error IS NOT NULL;

      EXCEPTION WHEN OTHERS THEN
        UPDATE engine.forecast_registry
           SET overall_error = COALESCE(overall_error,'') || CASE WHEN overall_error IS NULL THEN '' ELSE E'\n' END
                               || '['||to_char(now(),'YYYY-MM-DD"T"HH24:MI:SSOF')||'] SR-all failed: '||SQLERRM,
               updated_at    = now()
         WHERE forecast_id = rec.forecast_id;
      END;
    END LOOP;
  END IF;
END;
$$;

GRANT EXECUTE ON FUNCTION engine.run_sr_all() TO aq_engine_owner;

COMMIT;