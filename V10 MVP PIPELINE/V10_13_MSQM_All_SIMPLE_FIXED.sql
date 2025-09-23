-- V10_13_MSQM_All_SIMPLE_FIXED.sql
-- Generated: 2025-09-23 00:49
-- FIX: After calling build_forecast_msqm_core, verify rows were written for the claimed forecast_id
--      in any engine.*_instance_forecast_msqm table before setting msqm_complete='complete'.
--      If zero rows were written, reset to NULL and append an error line.
--      No changes to forecast logic; wrapper only.

BEGIN;

CREATE OR REPLACE FUNCTION engine.run_msqm_all_fixed()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  use_v10 boolean;
  rec_id  uuid;
  wrote_rows bigint := 0;
  t record;
BEGIN
  PERFORM set_config('lock_timeout','250ms', true);

  -- Detect V10 (pipeline_status) vs legacy (status)
  SELECT EXISTS (
           SELECT 1 FROM information_schema.columns
           WHERE table_schema='engine' AND table_name='forecast_registry' AND column_name='pipeline_status'
         )
    INTO use_v10;

  LOOP
    -- Oldest eligible + immediate non-blocking claim
    IF use_v10 THEN
      WITH c AS (
        SELECT forecast_id
          FROM engine.forecast_registry
         WHERE pipeline_status='sr_complete'
           AND msqm_complete IS NULL
         ORDER BY created_at ASC NULLS LAST
         FOR UPDATE SKIP LOCKED
         LIMIT 1
      )
      UPDATE engine.forecast_registry fr
         SET msqm_complete='running', updated_at=now()
        FROM c
       WHERE fr.forecast_id=c.forecast_id
       RETURNING fr.forecast_id INTO rec_id;
    ELSE
      WITH c AS (
        SELECT forecast_id
          FROM engine.forecast_registry
         WHERE status='sr_complete'
           AND msqm_complete IS NULL
         ORDER BY created_at ASC NULLS LAST
         FOR UPDATE SKIP LOCKED
         LIMIT 1
      )
      UPDATE engine.forecast_registry fr
         SET msqm_complete='running', updated_at=now()
        FROM c
       WHERE fr.forecast_id=c.forecast_id
       RETURNING fr.forecast_id INTO rec_id;
    END IF;

    IF rec_id IS NULL THEN
      EXIT; -- nothing to do
    END IF;

    -- Run core for this id (uuid signature if available, else no-arg)
    BEGIN
      PERFORM engine.build_forecast_msqm_core(rec_id);
    EXCEPTION WHEN undefined_function THEN
      PERFORM engine.build_forecast_msqm_core();
    END;

    -- Verify: rows were written for this forecast_id in any *_instance_forecast_msqm table
    wrote_rows := 0;
    FOR t IN
      SELECT format('%I.%I','engine',tablename) AS qname
      FROM pg_catalog.pg_tables
      WHERE schemaname='engine' AND tablename LIKE '%\_instance\_forecast\_msqm' ESCAPE '\'
    LOOP
      EXECUTE format('SELECT COUNT(*) FROM %s WHERE forecast_id = $1', t.qname)
        USING rec_id INTO wrote_rows;
      IF wrote_rows > 0 THEN
        EXIT;
      END IF;
    END LOOP;

    IF wrote_rows > 0 THEN
      UPDATE engine.forecast_registry
         SET msqm_complete='complete', updated_at=now(), overall_error=NULL
       WHERE forecast_id=rec_id;
    ELSE
      UPDATE engine.forecast_registry
         SET msqm_complete=NULL,
             updated_at=now(),
             overall_error = COALESCE(overall_error,'') || CASE WHEN overall_error IS NULL THEN '' ELSE E'\n' END
                             || '['||to_char(now(),'YYYY-MM-DD"T"HH24:MI:SSOF')||'] Wrapper: no msqm rows written'
       WHERE forecast_id=rec_id;
    END IF;

    rec_id := NULL; -- continue loop
  END LOOP;

EXCEPTION WHEN OTHERS THEN
  -- Best-effort reset for the id we claimed
  BEGIN
    IF rec_id IS NOT NULL THEN
      UPDATE engine.forecast_registry
         SET msqm_complete=NULL,
             updated_at=now(),
             overall_error = COALESCE(overall_error,'') || CASE WHEN overall_error IS NULL THEN '' ELSE E'\n' END
                             || '['||to_char(now(),'YYYY-MM-DD"T"HH24:MI:SSOF')||'] MSQM failed: '||SQLERRM
       WHERE forecast_id=rec_id;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    NULL;
  END;
  RAISE;
END;
$$;

GRANT EXECUTE ON FUNCTION engine.run_msqm_all_fixed() TO aq_engine_owner;

COMMIT;
