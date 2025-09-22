-- V10_11_MS_All.sql
-- Regenerated: 2025-09-22
-- Wrapper: MS runner with IMMEDIATE claim + oldest-first + parallel-safe
--   • Oldest-eligible: oldest created_at where SR complete AND ms_complete IS NULL
--   • Immediate claim: single CTE + UPDATE sets ms_complete='running' right away
--   • Non-blocking: FOR UPDATE SKIP LOCKED and short lock_timeout
--   • Per-forecast advisory lock (per series)
--   • End-only completion flip; errors reset to NULL and append to overall_error
--   • No schema or core-forecast logic here

BEGIN;

CREATE OR REPLACE FUNCTION engine.run_ms_all()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  use_v10   boolean;
  has_mscol boolean;
  rec_id    uuid;
BEGIN
  PERFORM set_config('statement_timeout','3600000', true);
  PERFORM set_config('lock_timeout','250ms', true);

  SELECT EXISTS (
           SELECT 1 FROM information_schema.columns
           WHERE table_schema='engine' AND table_name='forecast_registry' AND column_name='pipeline_status'
         )
    INTO use_v10;

  SELECT EXISTS (
           SELECT 1 FROM information_schema.columns
           WHERE table_schema='engine' AND table_name='forecast_registry' AND column_name='ms_complete'
         )
    INTO has_mscol;

  IF NOT has_mscol THEN
    RAISE EXCEPTION 'engine.forecast_registry.ms_complete is required for run_ms_all()';
  END IF;

  LOOP
    -- 1) Oldest-eligible + IMMEDIATE CLAIM in the SAME statement (non-blocking)
    IF use_v10 THEN
      WITH candidate AS (
        SELECT fr.forecast_id
        FROM engine.forecast_registry fr
        WHERE fr.pipeline_status = 'sr_complete'
          AND fr.ms_complete IS NULL
        ORDER BY fr.created_at ASC NULLS LAST
        FOR UPDATE SKIP LOCKED
        LIMIT 1
      )
      UPDATE engine.forecast_registry fr
         SET ms_complete='running', updated_at=now()
        FROM candidate
       WHERE fr.forecast_id = candidate.forecast_id
       RETURNING fr.forecast_id INTO rec_id;
    ELSE
      WITH candidate AS (
        SELECT fr.forecast_id
        FROM engine.forecast_registry fr
        WHERE fr.status = 'sr_complete'
          AND fr.ms_complete IS NULL
        ORDER BY fr.created_at ASC NULLS LAST
        FOR UPDATE SKIP LOCKED
        LIMIT 1
      )
      UPDATE engine.forecast_registry fr
         SET ms_complete='running', updated_at=now()
        FROM candidate
       WHERE fr.forecast_id = candidate.forecast_id
       RETURNING fr.forecast_id INTO rec_id;
    END IF;

    -- None left to process
    IF rec_id IS NULL THEN
      EXIT;
    END IF;

    -- 2) Per-forecast advisory lock (per series)
    PERFORM pg_advisory_xact_lock( (hashtext(rec_id::text)::bigint # hashtext('engine.run_ms_all')::bigint) );

    -- 3) Run the MS core for THIS id (try uuid, fallback no-arg)
    BEGIN
      PERFORM engine.build_forecast_ms_core(rec_id);
    EXCEPTION WHEN undefined_function THEN
      PERFORM engine.build_forecast_ms_core();
    END;

    -- 4) End-only completion flip
    UPDATE engine.forecast_registry
       SET ms_complete='complete', updated_at=now(), overall_error=NULL
     WHERE forecast_id = rec_id;

    -- Loop to next candidate
    rec_id := NULL;
  END LOOP;

EXCEPTION WHEN OTHERS THEN
  -- Best-effort reset + error append for the current id
  BEGIN
    IF rec_id IS NOT NULL THEN
      UPDATE engine.forecast_registry
         SET ms_complete=NULL,
             updated_at=now(),
             overall_error = COALESCE(overall_error,'') || CASE WHEN overall_error IS NULL THEN '' ELSE E'\n' END
                             || '['||to_char(now(),'YYYY-MM-DD"T"HH24:MI:SSOF')||'] MS-all failed: '||SQLERRM
       WHERE forecast_id = rec_id;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    -- ignore
    NULL;
  END;
  RAISE;
END;
$$;

GRANT EXECUTE ON FUNCTION engine.run_ms_all() TO aq_engine_owner;

COMMIT;
