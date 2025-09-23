-- V10_12_MSQ_All_SIMPLE_FIXED.sql
-- Generated: 2025-09-23 00:44
-- FIX: After calling build_forecast_msq_core, verify rows were written for the claimed forecast_id
--      before flipping msq_complete='complete'. If zero rows found, reset to NULL and append error.
--      No changes to forecast logic; wrapper only.
BEGIN;

CREATE OR REPLACE FUNCTION engine.V10_12_MSQ_All_SIMPLE_FIXED()
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

  SELECT EXISTS (
           SELECT 1 FROM information_schema.columns
           WHERE table_schema='engine' AND table_name='forecast_registry' AND column_name='pipeline_status'
         )
    INTO use_v10;

  LOOP
    IF use_v10 THEN
      WITH c AS (
        SELECT forecast_id
          FROM engine.forecast_registry
         WHERE pipeline_status='sr_complete'
           AND msq_complete IS NULL
         ORDER BY created_at ASC NULLS LAST
         FOR UPDATE SKIP LOCKED
         LIMIT 1
      )
      UPDATE engine.forecast_registry fr
         SET msq_complete='running', updated_at=now()
        FROM c
       WHERE fr.forecast_id=c.forecast_id
       RETURNING fr.forecast_id INTO rec_id;
    ELSE
      WITH c AS (
        SELECT forecast_id
          FROM engine.forecast_registry
         WHERE status='sr_complete'
           AND msq_complete IS NULL
         ORDER BY created_at ASC NULLS LAST
         FOR UPDATE SKIP LOCKED
         LIMIT 1
      )
      UPDATE engine.forecast_registry fr
         SET msq_complete='running', updated_at=now()
        FROM c
       WHERE fr.forecast_id=c.forecast_id
       RETURNING fr.forecast_id INTO rec_id;
    END IF;

    IF rec_id IS NULL THEN
      EXIT; -- nothing to do
    END IF;

    -- Run core for this id (uuid signature if available, else no-arg)
    BEGIN
      PERFORM engine.build_forecast_msq_core(rec_id);
    EXCEPTION WHEN undefined_function THEN
      PERFORM engine.build_forecast_msq_core();
    END;

    -- Verify: rows written for this forecast_id in any msq table
    FOR t IN
      SELECT format('%I.%I','engine',tablename) AS qname
      FROM pg_catalog.pg_tables
      WHERE schemaname='engine' AND tablename LIKE '%\_instance\_forecast\_msq' ESCAPE '\'
    LOOP
      EXECUTE format('SELECT COUNT(*) FROM %s WHERE forecast_id = $1', t.qname) USING rec_id INTO STRICT wrote_rows;
      IF wrote_rows > 0 THEN EXIT; END IF;
    END LOOP;

    IF wrote_rows > 0 THEN
      UPDATE engine.forecast_registry
         SET msq_complete='complete', updated_at=now(), overall_error=NULL
       WHERE forecast_id=rec_id;
    ELSE
      UPDATE engine.forecast_registry
         SET msq_complete=NULL,
             updated_at=now(),
             overall_error = COALESCE(overall_error,'') || CASE WHEN overall_error IS NULL THEN '' ELSE E'\n' END
                             || '['||to_char(now(),'YYYY-MM-DD"T"HH24:MI:SSOF')||'] Wrapper: no msq rows written'
       WHERE forecast_id=rec_id;
    END IF;

    rec_id := NULL; -- loop onward
  END LOOP;

EXCEPTION WHEN OTHERS THEN
  BEGIN
    IF rec_id IS NOT NULL THEN
      UPDATE engine.forecast_registry
         SET msq_complete=NULL,
             updated_at=now(),
             overall_error = COALESCE(overall_error,'') || CASE WHEN overall_error IS NULL THEN '' ELSE E'\n' END
                             || '['||to_char(now(),'YYYY-MM-DD"T"HH24:MI:SSOF')||'] Wrapper failed: '||SQLERRM
       WHERE forecast_id=rec_id;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    NULL;
  END;
  RAISE;
END;
$$;

GRANT EXECUTE ON FUNCTION engine.V10_12_MSQ_All_SIMPLE_FIXED() TO aq_engine_owner;

COMMIT;
