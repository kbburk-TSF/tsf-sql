-- V4_05_DeleteStaging.sql
-- Zero-arg staging cleanup (no manual UUID). Original logic preserved.
-- VC V4.0 (2025-09-17): Renamed core to engine.delete_staging_core(uuid); added engine.delete_staging().

BEGIN;

CREATE SCHEMA IF NOT EXISTS engine;

-- Remove prior function to avoid overload ambiguity
DROP FUNCTION IF EXISTS engine.delete_staging_for(uuid) CASCADE;

-- Core (verbatim behavior, renamed)
CREATE OR REPLACE FUNCTION engine.delete_staging_core(p_forecast_id uuid)
RETURNS void
LANGUAGE plpgsql
SECURITY INVOKER
AS $$
BEGIN
  -- best-effort session tuning (ignore errors if not allowed)
  BEGIN
    PERFORM set_config('client_min_messages','NOTICE',true);
    PERFORM set_config('jit','off',true);
    PERFORM set_config('synchronous_commit','off',true);
  EXCEPTION WHEN OTHERS THEN
    -- do nothing
  END;

  RAISE NOTICE '[%] delete_staging_core start', clock_timestamp();

  DELETE FROM engine.staging_historical
  WHERE forecast_id = p_forecast_id;

  RAISE NOTICE '[%] delete_staging_core done', clock_timestamp();
END;
$$;

-- Zero-arg wrapper: picks newest forecast_id from staging (fallback to latest in registry)
CREATE OR REPLACE FUNCTION engine.delete_staging()
RETURNS void
LANGUAGE plpgsql
SECURITY INVOKER
AS $$
DECLARE
  _fid uuid;
BEGIN
  -- Prefer the freshest id present in staging
  SELECT sh.forecast_id
  INTO _fid
  FROM engine.staging_historical sh
  WHERE sh.forecast_id IS NOT NULL
  ORDER BY sh.uploaded_at DESC NULLS LAST, sh.created_at DESC NULLS LAST
  LIMIT 1;

  -- Fallback: most recently updated registry row
  IF _fid IS NULL THEN
    SELECT fr.forecast_id
    INTO _fid
    FROM engine.forecast_registry fr
    ORDER BY fr.updated_at DESC, fr.created_at DESC
    LIMIT 1;
  END IF;

  IF _fid IS NULL THEN
    RAISE EXCEPTION 'No forecast_id found to clear from staging.';
  END IF;

  PERFORM engine.delete_staging_core(_fid);
END;
$$;

-- Execution privileges
GRANT EXECUTE ON FUNCTION engine.delete_staging() TO matrix_reader, tsf_engine_app;
GRANT EXECUTE ON FUNCTION engine.delete_staging_core(uuid) TO matrix_reader, tsf_engine_app;

COMMIT;
